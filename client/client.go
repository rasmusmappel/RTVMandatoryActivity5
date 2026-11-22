package main

import (
	pb "RTVMandatoryActivity5/gRPC"
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// How long the client waits for a node before trying the next one.
// Longer than the servers' forward timeout, so a node that's still working for the client isn't given up on.
const requestTimeout = 10 * time.Second

type Client struct {
	id        string                  // My own client ID
	nodes     []pb.AuctionHouseClient // Map of the actual clients to connect to "serverside" nodes
	addresses []string                // Map of the adresses of known nodes, so the client can reconnect to another node if the node it's connected to crashes/becomes unavailable
	current   int                     // Index of the node that answered last, which is tried first next time this client tries to make a bid or read results
}

func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage syntax: go run client.go <client id> <port> [<port> ...]") // Guide for the user trying to run the client
		os.Exit(1)
	}

	id := os.Args[1]

	os.MkdirAll("logs", 0755)

	logFileName := "logs/client" + id + ".log"

	file, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}

	log.SetOutput(file)
	logPrefix := "[CLIENT " + id + "] "
	log.SetPrefix(logPrefix)

	defer file.Close()

	client := Client{id: id}

	// Every port after the ID is a node that the client knows. The first one is tried first,
	// and the others are used if it doesn't answer. The reason the idMap isn't used is because I think realistically,
	// not every client knows every "serverside" node. Incorrect ports written by the user will crash the program
	for _, port := range os.Args[2:] {
		conn, err := grpc.NewClient("localhost:"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))

		if err != nil {
			log.Fatalf("[ERROR] Could not create a connection to port %s: %v", port, err)
		}

		defer conn.Close()

		client.addresses = append(client.addresses, "localhost:"+port)
		client.nodes = append(client.nodes, pb.NewAuctionHouseClient(conn))
	}

	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		inputArray := strings.Fields(scanner.Text()) // Use the Fields function to immediately split the command written by the user

		if len(inputArray) == 0 {
			continue
		}

		// Use a switch to check the command written by the user. If the first word is bid, check the number that follows,
		// if the number that follows is valid, it calls client.makeBid with the number that was written
		// If the user instead writes "result" as the first word, it returns the result of the auction.
		// If the first word is neither "bid" or result, ignores the command, and prints instructions
		switch strings.ToLower(inputArray[0]) {

		case "bid":
			if len(inputArray) < 2 {
				fmt.Println("Usage: bid <amount>")
				continue
			}

			amount, err := strconv.Atoi(inputArray[1])
			if err != nil {
				fmt.Println("The amount must be a whole number")
				continue
			}

			client.makeBid(amount)

		case "result":
			client.readOutcome()

		default:
			fmt.Println("Unknown command, use 'bid <amount>' or 'result'")
		}
	}
}

// Method for sending a bid. If a node doesn't answer, the same bid is sent to the next known node.
// Sending the same bid again is safe, because the servers treat a repeat of the highest bid as a success.
func (c *Client) makeBid(amount int) {

	bidMessage := &pb.Amount{Id: c.id, BidAmount: int32(amount)} // Construct the bid message

	for attempt := 0; attempt < len(c.nodes); attempt++ { // Iterate over the nodes that this client knows

		index := (c.current + attempt) % len(c.nodes) // Remember the index of the current port this client is trying

		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		acknowledgement, err := c.nodes[index].Bid(ctx, bidMessage)
		cancel()

		if err != nil {
			log.Printf("[WARNING] Node at %s didn't answer the bid, trying the next node: %v", c.addresses[index], err)
			continue
		}

		c.current = index

		fmt.Println("Bid outcome: " + acknowledgement.Outcome)

		return
	}

	// Every known node failed to answer, so the outcome of the bid is unknown.
	log.Println("[ERROR] No known node could be reached for the bid")
	fmt.Println("Bid outcome: exception, none of the known nodes responded")
}

// Method for requesting the state of the auction, with the same failover as makeBid.
func (c *Client) readOutcome() {

	for attempt := 0; attempt < len(c.nodes); attempt++ {

		index := (c.current + attempt) % len(c.nodes)

		ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
		state, err := c.nodes[index].Result(ctx, &pb.Nothing{})
		cancel()

		if err != nil {
			log.Printf("[WARNING] Node at %s didn't answer the result request, trying the next node: %v", c.addresses[index], err)
			continue
		}

		c.current = index

		printOutcome(state)

		return
	}

	log.Println("[ERROR] No known node could be reached for the result")
	fmt.Println("Result: exception, none of the known nodes responded")
}

// Method for printing the auction state that was returned by a node.
func printOutcome(state *pb.State) {

	// A sequence number of 0 means that no bid has been accepted yet.
	if state.Seq == 0 {
		fmt.Println("No bids have been placed yet, so the auction has not started")
		return
	}

	if state.Over {
		fmt.Println("The auction is over")
		fmt.Println("Winner: " + state.HighestBidId)
		fmt.Println("Winning bid: " + strconv.Itoa(int(state.HighestBidAmount)))
	} else {
		fmt.Println("The auction is still ongoing")
		fmt.Println("Current highest bidder: " + state.HighestBidId)
		fmt.Println("Current highest bid: " + strconv.Itoa(int(state.HighestBidAmount)))
	}

	timestamp := time.Unix(state.EndTimestamp, 0)
	fmt.Println("End timestamp for the auction: " + timestamp.UTC().String())
}
