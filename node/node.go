package main

import (
	pb "RTVMandatoryActivity5/gRPC"
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	pb.UnimplementedAuctionHouseServer
	id                      string
	primaryId               string
	term                    int64
	currentAuctionTimestamp int64
	currentHighestBid       int32
	currentHighestBidder    string
	committedSequenceNo     int64
	pendingAuction          *pb.AuctionInformation
	knownNodesMap           map[string]string
	otherNodeClients        map[string]pb.AuctionHouseClient
	mu                      sync.Mutex
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
		id:                      myID,
		primaryId:               "a",
		knownNodesMap:           make(map[string]string),
		otherNodeClients:        make(map[string]pb.AuctionHouseClient),
		currentAuctionTimestamp: 0,
		currentHighestBid:       0,
		currentHighestBidder:    "none",
	}

	// READING FROM ID MAP FILE
	idFile, err := os.Open("idMap.txt")
	if err != nil {
		log.Fatal(err)
	}

	defer idFile.Close()

	// Set own ID and fill address map
	reader := bufio.NewScanner(idFile)
	for reader.Scan() {
		parts := strings.Fields(reader.Text())
		if parts[0] == myID {
			myPort = parts[1]
		} else {
			server.knownNodesMap[parts[0]] = "localhost:" + parts[1]
		}
	}

	// Listen and serve own server
	listener, _ := net.Listen("tcp", "localhost:"+myPort)

	grpcServer := grpc.NewServer()

	pb.RegisterAuctionHouseServer(grpcServer, &server)

	log.Println("[STARTUP] Server has started")

	//Fill nodes map with new AuctionHouseClients based on the knownNodesMap
	for k, v := range server.knownNodesMap {
		conn, err := grpc.NewClient(v, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if (err) != nil {
			log.Fatal(err)
		}
		defer conn.Close()
		server.otherNodeClients[k] = pb.NewAuctionHouseClient(conn)
	}

	// Start server after knowing who the other nodes are,
	// so clients can't bid while there's only one known node
	go grpcServer.Serve(listener)

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		if strings.ToLower(scanner.Text()) == "exit" {
			break
		}
	}

	grpcServer.Stop()

	log.Println("[SHUTDOWN] Server has shutdown.")
}

// Method for handling client bid requests
func (s *Server) Bid(ctx context.Context, bidMessage *pb.Amount) (*pb.ClientAcknowledgement, error) {

	// Check if I'm the primary node, if not, forward the message to the primary node,
	// so it can handle auction information replication in a proper order.
	if s.id != s.primaryId {
		primaryNodeClient := s.otherNodeClients[s.primaryId]

		response, err := primaryNodeClient.Bid(ctx, bidMessage)

		if err != nil {

			// Try to recover auction information
			// Elect a new leader and make sure the information is copied and committed
			// Retry bidding if the recovery succeeded
		}

		return response, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// The if-statement below handles creation of the first auction.
	if s.currentHighestBidder == "none" {

		localCurrentAuctionTimestamp := time.Now().Unix() + 100
		localCurrentHighestBid := bidMessage.BidAmount
		localCurrentHighestBidder := bidMessage.Id

		copyMessage := &pb.AuctionInformation{EndTimestamp: localCurrentAuctionTimestamp,
			HighestBidAmount: localCurrentHighestBid,
			HighestBidId:     localCurrentHighestBidder,
			SequenceNumber:   s.committedSequenceNo + 1,
		}

		if !s.replicateAuctionInformation(ctx, copyMessage) {
			return &pb.ClientAcknowledgement{
				BidOutcome: "Exception: Not enough nodes acknowledged the bid",
			}, nil
		}

		s.currentAuctionTimestamp = copyMessage.EndTimestamp
		s.currentHighestBid = copyMessage.HighestBidAmount
		s.currentHighestBidder = copyMessage.HighestBidId
		s.committedSequenceNo = copyMessage.SequenceNumber

		commitRequest := &pb.CommitRequest{
			SequenceNumber: copyMessage.SequenceNumber,
		}

		successfulCommits := s.commitLoopMethod(ctx, commitRequest)

		log.Printf(
			"Commit completed on %d backup nodes",
			successfulCommits,
		)

		return &pb.ClientAcknowledgement{BidOutcome: "This was the first auction, created a new auction with your bid"}, nil

	}

	// Checks if the current auction is still ongoing.
	// If yes, then checks if the incoming bid is higher than the current highest bid
	// If no, creates a new auction with the bid.
	if s.currentAuctionTimestamp > time.Now().Unix() {

		log.Println("Internal highest bid " + strconv.Itoa(int(s.currentHighestBid)))
		log.Println("Incoming bid " + strconv.Itoa(int(bidMessage.BidAmount)))
		if s.currentHighestBid < bidMessage.BidAmount {

			localCurrentHighestBid := bidMessage.BidAmount
			localCurrentHighestBidder := bidMessage.Id

			copyMessage := &pb.AuctionInformation{EndTimestamp: s.currentAuctionTimestamp,
				HighestBidAmount: localCurrentHighestBid,
				HighestBidId:     localCurrentHighestBidder,
				SequenceNumber:   s.committedSequenceNo + 1,
			}

			if !s.replicateAuctionInformation(ctx, copyMessage) {
				return &pb.ClientAcknowledgement{
					BidOutcome: "Exception: Not enough nodes acknowledged the bid",
				}, nil
			}

			s.currentHighestBid = copyMessage.HighestBidAmount
			s.currentHighestBidder = copyMessage.HighestBidId
			s.committedSequenceNo = copyMessage.SequenceNumber

			commitRequest := &pb.CommitRequest{
				SequenceNumber: copyMessage.SequenceNumber,
			}

			successfulCommits := s.commitLoopMethod(ctx, commitRequest)

			log.Printf(
				"Commit completed on %d backup nodes",
				successfulCommits,
			)

			return &pb.ClientAcknowledgement{BidOutcome: "Your bid was higher than the previously highest bid, and was accepted"}, nil

		} else {

			return &pb.ClientAcknowledgement{BidOutcome: "Your bid was lower than the highest bid, and was rejected"}, nil

		}
	} else {

		localCurrentAuctionTimestamp := time.Now().Unix() + 100
		localCurrentHighestBid := bidMessage.BidAmount
		localCurrentHighestBidder := bidMessage.Id

		copyMessage := &pb.AuctionInformation{EndTimestamp: localCurrentAuctionTimestamp,
			HighestBidAmount: localCurrentHighestBid,
			HighestBidId:     localCurrentHighestBidder,
			SequenceNumber:   s.committedSequenceNo + 1,
		}

		if !s.replicateAuctionInformation(ctx, copyMessage) {
			return &pb.ClientAcknowledgement{
				BidOutcome: "Exception: Not enough nodes acknowledged the bid",
			}, nil
		}

		s.currentAuctionTimestamp = copyMessage.EndTimestamp
		s.currentHighestBid = copyMessage.HighestBidAmount
		s.currentHighestBidder = copyMessage.HighestBidId
		s.committedSequenceNo = copyMessage.SequenceNumber

		commitRequest := &pb.CommitRequest{
			SequenceNumber: copyMessage.SequenceNumber,
		}

		successfulCommits := s.commitLoopMethod(ctx, commitRequest)

		log.Printf(
			"Commit completed on %d backup nodes",
			successfulCommits,
		)

		return &pb.ClientAcknowledgement{BidOutcome: "Old auction has ended, created a new auction with your bid"}, nil
	}

}

// Method for handling client result requests
func (s *Server) Result(ctx context.Context, emptyMessage *pb.Nothing) (*pb.Outcome, error) {
	s.mu.Lock()
	outcomeMessage := &pb.Outcome{HighestBidId: s.currentHighestBidder, HighestBidAmount: s.currentHighestBid, EndTimestamp: s.currentAuctionTimestamp}
	s.mu.Unlock()

	return outcomeMessage, nil

}

// Method for copying auction information from another node
func (s *Server) CopyAuctionInformation(ctx context.Context, information *pb.AuctionInformation) (*pb.NodeAcknowledgement, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if the sequence number of the incoming information is one higher as expected.
	// If not, return an acknowledgement that indicates that copying failed
	if information.SequenceNumber != s.committedSequenceNo+1 {
		return &pb.NodeAcknowledgement{Success: false, CopyOutcome: "Didn't receive the expected sequence number"}, nil
	}

	s.pendingAuction = information

	return &pb.NodeAcknowledgement{Success: true, CopyOutcome: "Successfully copied the sent information"}, nil
}

// Method for handling the replication loop, to avoid redundancy
// Also checks if there's a majority of nodes that have successfully copied the new information
func (s *Server) replicateAuctionInformation(
	ctx context.Context,
	information *pb.AuctionInformation,
) bool {

	// Count itself as one successful replication
	successfulCopies := 1

	// Calculate the majority required to
	totalNodes := len(s.knownNodesMap) + 1
	majority := totalNodes/2 + 1

	for nodeId, nodeClient := range s.otherNodeClients {

		timeOutContext, cancel := context.WithTimeout(ctx, 2*time.Second)

		acknowledgement, err := nodeClient.CopyAuctionInformation(
			timeOutContext, information,
		)

		cancel()

		if err != nil {
			log.Printf(
				"Node %s failed when copying auction information: %v",
				nodeId, err,
			)
			continue
		}

		if acknowledgement.Success {
			successfulCopies++
		}
		log.Println(acknowledgement.CopyOutcome)
	}

	return successfulCopies >= majority
}

// Method for committing the previously copied information,
// so the auction information that's returned to the client is only committed information.
func (s *Server) CommitAuctionInformation(ctx context.Context, request *pb.CommitRequest) (*pb.NodeAcknowledgement, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.pendingAuction == nil || s.pendingAuction.SequenceNumber != request.SequenceNumber {
		return &pb.NodeAcknowledgement{Success: false, CopyOutcome: "The request doesn't match the pending update!"}, nil
	}

	s.currentAuctionTimestamp = s.pendingAuction.EndTimestamp
	s.currentHighestBid = s.pendingAuction.HighestBidAmount
	s.currentHighestBidder = s.pendingAuction.HighestBidId

	s.committedSequenceNo = request.SequenceNumber

	s.pendingAuction = nil

	return &pb.NodeAcknowledgement{Success: true, CopyOutcome: "Successfully committed the pending auction."}, nil
}

// Sends the commit request to all backup nodes.
// Returns the number of backups that successfully committed the update.
func (s *Server) commitLoopMethod(
	ctx context.Context,
	request *pb.CommitRequest,
) int {

	successfulCommits := 0

	for nodeId, nodeClient := range s.otherNodeClients {

		timeOutContext, cancel := context.WithTimeout(
			ctx,
			2*time.Second,
		)

		acknowledgement, err := nodeClient.CommitAuctionInformation(
			timeOutContext,
			request,
		)

		cancel()

		if err != nil {
			log.Printf(
				"Node %s failed when committing auction information: %v",
				nodeId, err,
			)
			continue
		}

		if !acknowledgement.Success {
			log.Printf(
				"Node %s rejected the commit: %s",
				nodeId,
				acknowledgement.CopyOutcome,
			)
			continue
		}

		successfulCommits++
		log.Printf(
			"Node %s successfully committed auction information",
			nodeId,
		)
	}

	return successfulCommits
}

// Method for returning the latest information for recovery when the primary has crashed.
func (s *Server) GetRecoveryInformation(
	ctx context.Context,
	request *pb.Nothing,
) (*pb.RecoveryInformation, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	committedAuction := &pb.AuctionInformation{
		HighestBidId:     s.currentHighestBidder,
		HighestBidAmount: s.currentHighestBid,
		EndTimestamp:     s.currentAuctionTimestamp,
		SequenceNumber:   s.committedSequenceNo,
	}

	return &pb.RecoveryInformation{
		CommittedAuction:        committedAuction,
		PendingAuction:          s.pendingAuction,
		CommittedSequenceNumber: s.committedSequenceNo,
		PrimaryId:               s.primaryId,
		Term:                    s.term,
	}, nil
}

// Method for actually electing a new leader when the primary has crashed.
func (s *Server) recoverPrimary(ctx context.Context) error {

	recoveryInfo := make(map[string]*pb.RecoveryInformation) // Make a map of the recovery information so it can be checked later

	localInfo, err := s.GetRecoveryInformation(ctx, &pb.Nothing{}) // Get local information
	if err != nil {
		return err
	}

	recoveryInfo[s.id] = localInfo // Save local information so it's included in the map

	successes := 1        // Like in the copy and commit methods, count self as success.
	acknowledgements := 1 // Count later acknowledgements

	totalNodes := len(s.knownNodesMap) + 1
	majority := totalNodes/2 + 1

	for nodeId, nodeClient := range s.otherNodeClients {

		timeoutContext, cancel := context.WithTimeout(ctx, 2*time.Second)
		response, err := nodeClient.GetRecoveryInformation(timeoutContext, &pb.Nothing{})

		cancel()

		if err != nil {
			log.Printf(
				"Node %s could not be contacted for recovery information: %v",
				nodeId,
				err,
			)
			continue
		}

		successes++

		recoveryInfo[nodeId] = response
	}

	if successes < majority { // Check if a majority responded
		return fmt.Errorf(
			"Recovery failed: only %d of %d nodes responded",
			successes,
			totalNodes,
		)
	}

	// Find the lowest-ID available node as the primary candidate.
	newPrimaryId := ""

	for nodeId := range recoveryInfo {
		if newPrimaryId == "" || nodeId < newPrimaryId {
			newPrimaryId = nodeId
		}
	}

	log.Printf("Selected node %s as primary candidate", newPrimaryId) // Locally log the selected new primary

	newTerm := int64(0)

	for _, info := range recoveryInfo { // Select the newest highest term from the recovered information, and increment by 1
		if info.Term >= newTerm {
			newTerm = info.Term + 1
		}
	}

	for nodeId, nodeClient := range s.otherNodeClients {

		timeoutContext, cancel := context.WithTimeout(ctx, 2*time.Second)
		response, err := nodeClient.GetPrimaryAcknowledgement(timeoutContext, &pb.NewPrimary{PrimaryId: newPrimaryId, Term: newTerm})

		cancel()

		if err != nil {
			log.Printf(
				"Node %s could not be contacted for primary acknowledgement: %v",
				nodeId,
				err,
			)
			continue
		}

		if response.Acknowledged {
			acknowledgements++
		}

	}

	if acknowledgements < majority {
		return fmt.Errorf(
			"Election failed: only %d of %d nodes acknowledged",
			acknowledgements,
			totalNodes,
		)
	}

	s.mu.Lock()
	s.primaryId = newPrimaryId
	s.term = newTerm
	s.mu.Unlock()

	return nil
}

// Method for confirming that this node has the same new primary and term no as the requester.
func (s *Server) GetPrimaryAcknowledgement(ctx context.Context, request *pb.NewPrimary) (*pb.PrimaryAcknowledgement, error) {

	s.mu.Lock()
	defer s.mu.Unlock()

	// Reject elections from older terms.
	if request.Term < s.term {
		return &pb.PrimaryAcknowledgement{
			Acknowledged: false,
		}, nil
	}

	// Don't accept two different primaries in the same term.
	if request.Term == s.term && request.PrimaryId != s.primaryId {
		return &pb.PrimaryAcknowledgement{
			Acknowledged: false,
		}, nil
	}

	// Accept the proposed primary and term.
	s.term = request.Term
	s.primaryId = request.PrimaryId

	return &pb.PrimaryAcknowledgement{
		Acknowledged: true,
	}, nil
}
