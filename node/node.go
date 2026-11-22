package main

import (
	pb "RTVMandatoryActivity5/gRPC"
	"bufio"
	"context"
	"io"
	"log"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Length of the auction in seconds. The clock starts when the first bid is accepted.
const auctionDuration int64 = 100

// The network is assumed to deliver messages to non-crashed nodes within a known time limit,
// so if a call takes longer than these limits, the node that was called has crashed.
// forwardTimeout must be longer than replicateTimeout, because the primary can spend
// up to replicateTimeout on a crashed backup before it answers a forwarded request.
const (
	replicateTimeout = 2 * time.Second
	forwardTimeout   = 5 * time.Second
)

type Server struct {
	pb.UnimplementedAuctionHouseServer
	id string // This node's ID. Not protected by mu or deadMu, since it doesn't change after initial startup.

	// The auction state that this node contains. seq == 0 means that no bid has been placed yet.
	// Protected by mu, which is also the serialization point of the whole system when this node is the primary.
	highestBidId     string
	highestBidAmount int32
	endTimestamp     int64
	seq              int64
	mu               sync.Mutex

	// The failure detector state. nodeIds contains all nodes (including this one) in ascending order.
	// The primary is always the lexicographically lowest ID in nodeIds that isn't in the dead map.
	// Protected by deadMu, which is a separate mutex from mu, so the primary can mark nodes as dead while it holds the primary mutex mu.
	nodeIds []string        // Slice of the known node IDs
	dead    map[string]bool // Map of node IDs to booleans. If not in the map, a node at a specific ID is assumed alive.
	deadMu  sync.Mutex

	// Only written at startup, so it can be read without a lock afterwards.
	otherNodeClients map[string]pb.AuctionHouseClient
}

func main() {
	// LOGGING
	os.MkdirAll("logs", 0755)

	var myID string = os.Args[1]
	var myPort string
	var logPath string = "logs/node" + myID + ".log"

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}

	writer := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(writer)
	log.SetPrefix("[SERVER " + myID + "] ")

	defer logFile.Close()

	server := Server{
		id:               myID,
		nodeIds:          []string{myID},
		dead:             make(map[string]bool),
		otherNodeClients: make(map[string]pb.AuctionHouseClient),
	}

	// READING FROM ID MAP FILE
	idFile, err := os.Open("idMap.txt")
	if err != nil {
		log.Fatal(err)
	}

	defer idFile.Close()

	// Set own port, remember the address of every other node and fill the list of all node IDs
	otherAddresses := make(map[string]string)

	reader := bufio.NewScanner(idFile)
	for reader.Scan() {
		parts := strings.Fields(reader.Text())
		if len(parts) < 2 {
			continue
		}
		if parts[0] == myID {
			myPort = parts[1]
		} else {
			otherAddresses[parts[0]] = "localhost:" + parts[1]
			server.nodeIds = append(server.nodeIds, parts[0])
		}
	}

	if myPort == "" {
		log.Fatalf("ID %s was not found in idMap.txt", myID) // If the user accidentally types a node ID that doesn't exist in idMap.txt, the program crashes
	}

	// Sort the IDs in the nodeId slice, so the lowest-ID live node can be found as the primary,
	// and so replication and primary selection always happens in ascending ID order
	sort.Strings(server.nodeIds)

	// Listen and serve own server
	listener, _ := net.Listen("tcp", "localhost:"+myPort)

	grpcServer := grpc.NewServer()

	pb.RegisterAuctionHouseServer(grpcServer, &server)

	// Fill otherNodeClients map with new AuctionHouseClients based on the adresses and IDs of other nodes in the otherAddresses map
	for id, port := range otherAddresses {
		conn, err := grpc.NewClient(port, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if (err) != nil {
			log.Fatal(err)
		}
		defer conn.Close()
		server.otherNodeClients[id] = pb.NewAuctionHouseClient(conn)
	}

	// Start server after knowing who the other nodes are,
	// so clients can't bid while there's only one known node
	go grpcServer.Serve(listener)

	log.Println("[STARTUP] Server has started")

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() { // Manually stop this node, so a crash can be manually simulated.
		if strings.ToLower(scanner.Text()) == "exit" {
			break
		}
	}

	grpcServer.Stop()

	log.Println("[SHUTDOWN] Server has shutdown.")
}

// Method for handling client bid requests
func (s *Server) Bid(ctx context.Context, bidMessage *pb.Amount) (*pb.Ack, error) {

	for {
		primaryId := s.currentPrimary()

		// If I'm the primary node, handle the bid myself.
		if primaryId == s.id {
			return s.handleBidAsPrimary(bidMessage), nil
		}

		// If not, forward the message to the primary node, so it can handle the bid and
		// the replication in a proper order. No lock is held here, so the primary can replicate to this node meanwhile.
		forwardContext, cancel := context.WithTimeout(ctx, forwardTimeout)
		response, err := s.otherNodeClients[primaryId].Bid(forwardContext, bidMessage)
		cancel()

		if err == nil {
			return response, nil
		}

		// If the client itself has given up, the error says nothing about the primary.
		if ctx.Err() != nil {
			return &pb.Ack{Outcome: "exception"}, nil
		}

		// The network is reliable, so a failed or timed out call means the primary has crashed.
		// Mark it as dead and go around the loop, which gives the next lowest ID as the new primary (which may be this node).
		s.markDead(primaryId)
	}
}

// Method for handling a bid on the primary node.
// The lock is held during the whole method, including replication, so all operations on the primary are serialized.
func (s *Server) handleBidAsPrimary(bidMessage *pb.Amount) *pb.Ack {

	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().Unix()

	// A client could possibly retry its bid if it never got an answer, for example because the old primary crashed after applying it.
	// If the bid is already the highest bid, treat the retry as a success instead of failing it for not being higher.
	// The state is replicated again first, because the old primary may have crashed before reaching every backup.
	if s.seq > 0 && s.highestBidId == bidMessage.Id && s.highestBidAmount == bidMessage.BidAmount {
		s.replicateAsPrimary(s.snapshot()) // Send state to other nodes

		log.Printf("Bid of %d from %s was already registered, treating it as a retry", bidMessage.BidAmount, bidMessage.Id)

		return &pb.Ack{Outcome: "success"}
	}

	// The first bid made by a client starts the auction. Later bids are only accepted before the auction has ended.
	newEndTimestamp := s.endTimestamp

	if s.seq == 0 {
		newEndTimestamp = now + auctionDuration // Set the timestamp of the auction
	} else if now >= s.endTimestamp {
		log.Printf("Bid of %d from %s rejected, the auction is over", bidMessage.BidAmount, bidMessage.Id)

		return &pb.Ack{Outcome: "fail"}
	}

	// A bid has to be higher than the highest bid so far.
	// That also makes it higher than all earlier bids from the same bidder.
	// Before the first bid, the highest bid is 0, so the first bid has to be positive.
	if bidMessage.BidAmount <= s.highestBidAmount {
		log.Printf("Bid of %d from %s rejected, highest bid is %d", bidMessage.BidAmount, bidMessage.Id, s.highestBidAmount)

		return &pb.Ack{Outcome: "fail"}
	}

	newState := &pb.State{
		HighestBidId:     bidMessage.Id,
		HighestBidAmount: bidMessage.BidAmount,
		EndTimestamp:     newEndTimestamp,
		Seq:              s.seq + 1,
	}

	// Send the new state to the backups before answering the client,
	// so an acknowledged bid is never lost when the primary crashes.
	s.replicateAsPrimary(newState)
	s.apply(newState)

	log.Printf("Bid of %d from %s accepted", bidMessage.BidAmount, bidMessage.Id)

	return &pb.Ack{Outcome: "success"}
}

// Method for handling client result requests
func (s *Server) Result(ctx context.Context, emptyMessage *pb.Nothing) (*pb.State, error) {

	for {
		primaryId := s.currentPrimary()

		// Results are also answered by the primary, never from the local state of a backup,
		// so a client can't read a state that is older than a bid that already got acknowledged.
		if primaryId == s.id {
			return s.handleResultAsPrimary(), nil
		}

		forwardContext, cancel := context.WithTimeout(ctx, forwardTimeout)
		response, err := s.otherNodeClients[primaryId].Result(forwardContext, emptyMessage)
		cancel()

		if err == nil {
			return response, nil
		}

		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		s.markDead(primaryId)
	}
}

// Method for answering a result request on the primary node.
// Takes the same lock as bids, so it waits for any bid that's currently being replicated.
func (s *Server) handleResultAsPrimary() *pb.State {

	s.mu.Lock()
	defer s.mu.Unlock()

	state := s.snapshot()

	// The auction is over when a bid has started it, and the end timestamp has passed.
	state.Over = s.seq > 0 && time.Now().Unix() >= s.endTimestamp

	return state
}

// Method for a backup node receiving the full auction state from the primary node.
func (s *Server) Replicate(ctx context.Context, state *pb.State) (*pb.Ack, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	// Only move the sequence number forward, so if somehow and old state is received, it's not applied.
	// Also, the state contains everything, so a node that missed an earlier update is brought
	// up to date by this one, and an old or repeated update can't overwrite a newer state.
	if state.Seq > s.seq {
		s.apply(state)
		log.Printf("Replicated state with sequence number %d", state.Seq)
	}

	return &pb.Ack{Outcome: "success"}, nil
}

// Method for sending a state to all backups, in ascending ID order.
// The order matters: if the primary somehow crashes halfway through replication, the backup that becomes the new primary
// (the lowest alphabetical ID) is the first one that got the state, so it's at least as new as all other backups.
// Must be called while holding mu.
func (s *Server) replicateAsPrimary(state *pb.State) {

	for _, nodeId := range s.nodeIds {

		if nodeId == s.id || s.isDead(nodeId) {
			continue
		}

		// A fresh context is used instead of the request's context. If the request was cancelled,
		// it would make a perfectly healthy backup look like it had crashed.
		timeOutContext, cancel := context.WithTimeout(context.Background(), replicateTimeout)

		acknowledgement, err := s.otherNodeClients[nodeId].Replicate(timeOutContext, state)

		cancel()

		// It's assummed that failure detection is perfect in this "network", so a node that doesn't answer is simply
		// crashed/manually stopped, and it's dropped from the set of nodes that the primary replicates to.
		if err != nil {
			log.Printf("Node %s failed when replicating, marking it as crashed: %v", nodeId, err)
			s.markDead(nodeId)
			continue
		}

		// Log the acknowledgement, to show successful replications in the logs.
		log.Printf("Node %s replied to sequence number %d with: %s", nodeId, state.Seq, acknowledgement.Outcome)

		log.Printf("Node %s successfully replicated sequence number %d", nodeId, state.Seq)
	}
}

// Method for overwriting the local auction state with the given state.
// Must be called while holding mu.
func (s *Server) apply(state *pb.State) {
	s.highestBidId = state.HighestBidId
	s.highestBidAmount = state.HighestBidAmount
	s.endTimestamp = state.EndTimestamp
	s.seq = state.Seq
}

// Method for copying the local auction state into a message.
// Must be called while holding mu.
func (s *Server) snapshot() *pb.State {
	return &pb.State{
		HighestBidId:     s.highestBidId,
		HighestBidAmount: s.highestBidAmount,
		EndTimestamp:     s.endTimestamp,
		Seq:              s.seq,
	}
}

// Method for finding the primary, which is the lowest ID that hasn't been marked as dead.
// The node this runs on is never marked as dead (because if it's running it, it can't be dead),
// so there's always a primary, and every node reaches the same
// answer without any election messages once they agree on which nodes have crashed.
func (s *Server) currentPrimary() string {

	s.deadMu.Lock()
	defer s.deadMu.Unlock()

	for _, nodeId := range s.nodeIds {
		if !s.dead[nodeId] {
			return nodeId
		}
	}

	return s.id
}

// Method for checking if a node has been marked as crashed.
func (s *Server) isDead(nodeId string) bool {

	s.deadMu.Lock()
	defer s.deadMu.Unlock()

	return s.dead[nodeId]
}

// Method for marking a node as crashed. A crashed node never comes back, since failures are full crashes.
func (s *Server) markDead(nodeId string) {

	s.deadMu.Lock()
	defer s.deadMu.Unlock()

	if !s.dead[nodeId] {
		s.dead[nodeId] = true
		log.Printf("Node %s has crashed, new primary is now chosen without it", nodeId)
	}
}
