package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "RTVMandatoryActivity5/gRPC"
)

func main() {
	id := os.Args[1]
	logFileName := "logs/client" + id + ".log"

	file, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}

	log.SetOutput(file)
	logPrefix := "[CLIENT " + id + "] "
	log.SetPrefix(logPrefix)

	defer file.Close()

	conn, err := grpc.NewClient("localhost:"+os.Args[2], grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Fatalf("[ERROR] Server not found!")
	}

	client := pb.NewAuctionHouseClient(conn)

	defer conn.Close()

	var input string
	scanner := bufio.NewScanner(os.Stdin)

	for scanner.Scan() {
		input = scanner.Text()

		if strings.Contains("bid", strings.ToLower(input)) {

			amount, _ := strconv.Atoi(input)

			makeBid(client, amount, id)

		} else {

			readOutcome(client)

		}
	}
}

func makeBid(client pb.AuctionHouseClient, amount int, id string) {

	bidMessage := &pb.Amount{Id: id, BidAmount: int32(amount)}

	acknowledgement, err := client.Bid(context.Background(), bidMessage)

	if err != nil {
		log.Fatalf("[ERROR] Bid caused a server side error!")
	}

	fmt.Println(acknowledgement)

}

func readOutcome(client pb.AuctionHouseClient) {

	outcomeRequest := &pb.Nothing{}

	outcomeResult, err := client.Result(context.Background(), outcomeRequest)

	if err != nil {
		log.Fatalf("[ERROR] Outcome request returned an error!")
	}

	fmt.Println(outcomeResult)
}
