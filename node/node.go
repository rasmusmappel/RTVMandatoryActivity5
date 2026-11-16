package main

import (
	pb "RTVMandatoryActivity5/gRPC"
	"bufio"
	"context"
	"io"
	"log"
	"net"
	"os"
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

	for {

	}

	grpcServer.Stop()

	log.Println("[SHUTDOWN] Server has shutdown.")
}

// Method for handling client bid requests
func (s *Server) Bid(ctx context.Context, bidMessage *pb.Amount) (*pb.ClientAcknowledgement, error) {

	if s.currentHighestBidder == "none" {

		s.currentAuctionTimestamp = time.Now().Unix() + 100
		s.currentHighestBid = bidMessage.BidAmount
		s.currentHighestBidder = bidMessage.Id

	}

	if s.currentAuctionTimestamp < time.Now().Unix() {

	} else {
		return &pb.ClientAcknowledgement{BidOutcome: "Old auction has ended, created a new auction with your bid"}, nil
	}

}

// Method for handling client result requests
func (s *Server) Result(ctx context.Context, emptyMessage *pb.Nothing) (*pb.Outcome, error) {

}

// Method for copying auction information from another node
func (s *Server) CopyAuctionInformation(ctx context.Context, information *pb.AuctionInformation) (*pb.NodeAcknowledgement, error) {

}
