package main

import (
	proto "assignment-3/grpc"
	"bufio"
	//"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"unicode/utf8"
	"strconv"


	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var leaving bool;


func main() {
	var clientLogicalClock int64 = 0

	// Open (or create) file for writing logs.
	f, err := os.OpenFile("client.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666) 
	if err != nil {
		log.Fatalf("Error opening file: %v", err) 
	}
	defer f.Close()


	conn, err := grpc.NewClient("localhost:5050", grpc.WithTransportCredentials(insecure.NewCredentials()))

	client := proto.NewChitChatClient(conn)
	
	reader := bufio.NewReader(os.Stdin)
    

	streamMessage, err := client.PostMessage(context.Background())
	if err != nil {
			log.Fatalf("failed to call PostMessage: %v", err) 
	}

	header, err := streamMessage.Header()
	if err != nil {
		log.Fatalf("failed to get header from stream: %v", err) 
	}
	idString := header["client-id"][0] //handle error here??? see here:https://github.com/grpc/grpc-go/blob/master/examples/features/metadata/client/main.go
	id, err := strconv.ParseInt(idString, 10, 64)

	// New creates a new Logger with log data destination, prefix, logging properties.
	clientLogger := log.New(f, "[Client #" + idString + "] ", log.Ldate|log.Ltime) // skift log.Ldate... til 0 når vi vil have logical time ind

	
	// --- [Event: Client connected] ---
	clientLogicalClock++ // Inkrementerer fordi vi er connected
	clientLogger.Println("[CONNECTED] at logical timestamp: " + strconv.Itoa(int(clientLogicalClock))) // X Client connected

	waitc := make(chan struct{})
	
	
	if err != nil {
		return;
	}


	// --- [Event: Client joins chat] ---
	fmt.Print("Enter your name: ")

	participant, _:= reader.ReadString('\n')
	participant = strings.TrimSpace(participant)

	clientLogicalClock++ 
	clientLogger.Println("[JOINED MESSAGE SENT TO SERVER] at logical timestamp: " + strconv.Itoa(int(clientLogicalClock)))

	joinMessage := participant + " has joined the chat at client clock: " + strconv.Itoa(int(clientLogicalClock)) + "\n"
	streamMessage.Send(&proto.Message{
		Id: id,
		Participant: participant,
		Message: joinMessage,
		Clock: clientLogicalClock,

	})


	// Receiving goroutine
    go func() {
		defer close(waitc)
        for {
			// --- [Event: Incoming message] ---
            msg, err := streamMessage.Recv()
            if err != nil {
				if(!leaving){
                	fmt.Println("Server shut down")
				}
                break
            }

			// --- Sync with other "nodes" clock and increment
			if msg.GetClock() > clientLogicalClock {
				clientLogicalClock = msg.GetClock()
			}
			// --- [Event: Message received] ---
			clientLogicalClock++
			clientLogger.Printf("[MESSAGE '" + strings.TrimSpace(msg.Message) + "' RECEIVED] at logical timestamp: " + strconv.Itoa(int(clientLogicalClock)))

            fmt.Print(msg.Message)
        }
    }()
	
    // Sending loop
    for {
        text, _ := reader.ReadString('\n')

		if(utf8.RuneCountInString(text) > 128){
			fmt.Println("Message is too long")
		
		} else { 

		// --- [Event: Client disconnected] ---
		if(text == "/leave\n"){
			leaving = true;
			clientLogicalClock++ // increment clock because leaving event
			clientLogger.Println("[DISCONNECTED] at logical timestamp: " + strconv.Itoa(int(clientLogicalClock)))
			streamMessage.CloseSend()
			<-waitc
			return;
		}

		// --- [Event: Message sent] ---
		clientLogicalClock++ 
		clientLogger.Println("[MESSAGE SENT TO SERVER] at logical timestamp: " + strconv.Itoa(int(clientLogicalClock)))
        streamMessage.Send(&proto.Message{
            Id: id,
			Participant:      participant,
            Message:   text,
            Clock: clientLogicalClock,
        })

		
	}
    }
	
}
