package main

import (
	pb "RTVMandatoryActivity5/gRPC"
	"bufio"
	"context"
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
	currentAuctionTimestamp int64
	currentHighestBid       int32
	currentHighestBidder    string
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

	go grpcServer.Serve(listener)

	//Fill nodes map with new AuctionHouseClients based on the knownNodesMap
	for k, v := range server.knownNodesMap {
		conn, err := grpc.NewClient(v, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if (err) != nil {
			log.Fatal(err)
		}
		defer conn.Close()
		server.otherNodeClients[k] = pb.NewAuctionHouseClient(conn)
	}

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

	s.mu.Lock()

	// The if-statement below handles creation of the first auction.
	if s.currentHighestBidder == "none" {

		localCurrentAuctionTimestamp := time.Now().Unix() + 100
		localCurrentHighestBid := bidMessage.BidAmount
		localCurrentHighestBidder := bidMessage.Id

		copyMessage := &pb.AuctionInformation{EndTimestamp: localCurrentAuctionTimestamp,
			HighestBidAmount: localCurrentHighestBid,
			HighestBidId:     localCurrentHighestBidder,
		}

		for nodeId, nodeClient := range s.otherNodeClients {
			acknowledgement, err := nodeClient.CopyAuctionInformation(context.Background(), copyMessage)

			if err != nil {
				log.Fatalf("Node " + nodeId + " failed when copying auction information")
			}

			log.Println(acknowledgement.CopyOutcome)
		}

		s.currentAuctionTimestamp = localCurrentAuctionTimestamp
		s.currentHighestBid = localCurrentHighestBid
		s.currentHighestBidder = localCurrentHighestBidder

		s.mu.Unlock()

		return &pb.ClientAcknowledgement{BidOutcome: "This was the first auction, created a new auction with your bid"}, nil

	}

	if s.currentAuctionTimestamp > time.Now().Unix() {

		log.Println("Internal highest bid " + strconv.Itoa(int(s.currentHighestBid)))
		log.Println("Incoming bid " + strconv.Itoa(int(bidMessage.BidAmount)))
		if s.currentHighestBid < bidMessage.BidAmount {

			localCurrentHighestBid := bidMessage.BidAmount
			localCurrentHighestBidder := bidMessage.Id

			copyMessage := &pb.AuctionInformation{EndTimestamp: s.currentAuctionTimestamp,
				HighestBidAmount: localCurrentHighestBid,
				HighestBidId:     localCurrentHighestBidder,
			}

			for nodeId, nodeClient := range s.otherNodeClients {
				acknowledgement, err := nodeClient.CopyAuctionInformation(context.Background(), copyMessage)

				if err != nil {
					log.Fatalf("Node " + nodeId + " failed when copying auction information")
				}

				log.Println(acknowledgement.CopyOutcome)
			}

			s.currentHighestBid = localCurrentHighestBid
			s.currentHighestBidder = localCurrentHighestBidder

			s.mu.Unlock()

			return &pb.ClientAcknowledgement{BidOutcome: "Your bid was higher than the previously highest bid, and was accepted"}, nil

		} else {

			s.mu.Unlock()

			return &pb.ClientAcknowledgement{BidOutcome: "Your bid was lower than the highest bid, and was rejected"}, nil

		}
	} else {

		localCurrentAuctionTimestamp := time.Now().Unix() + 100
		localCurrentHighestBid := bidMessage.BidAmount
		localCurrentHighestBidder := bidMessage.Id

		copyMessage := &pb.AuctionInformation{EndTimestamp: localCurrentAuctionTimestamp,
			HighestBidAmount: localCurrentHighestBid,
			HighestBidId:     localCurrentHighestBidder,
		}

		for nodeId, nodeClient := range s.otherNodeClients {
			acknowledgement, err := nodeClient.CopyAuctionInformation(context.Background(), copyMessage)

			if err != nil {
				log.Fatalf("Node " + nodeId + " failed when copying auction information")
			}

			log.Println(acknowledgement.CopyOutcome)
		}

		s.currentAuctionTimestamp = localCurrentAuctionTimestamp
		s.currentHighestBid = localCurrentHighestBid
		s.currentHighestBidder = localCurrentHighestBidder

		s.mu.Unlock()

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
	s.currentAuctionTimestamp = information.EndTimestamp
	s.currentHighestBid = information.HighestBidAmount
	s.currentHighestBidder = information.HighestBidId

	return &pb.NodeAcknowledgement{CopyOutcome: "Successfully copied the sent information"}, nil
}
