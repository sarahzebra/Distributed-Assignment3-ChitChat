package main

import (
	proto "assignment-3/grpc"
	"bufio"
	//"bytes"
	"context"
	"fmt"
	"io"
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
	var logicalClock int64 = 0

	// Open (or create) file for writing logs.
	f, err := os.OpenFile("client.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("Error opening file: %v", err) // Kan lowkey ikke logge her fordi vi først opretter den længere nede - what to do:(?? 
	}
	defer f.Close()


	// Set output of logs to file.
	// log.SetOutput(f)

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

	id := header["client-id"][0] //handle error here??? see here:https://github.com/grpc/grpc-go/blob/master/examples/features/metadata/client/main.go
	
	// New creates a new Logger with log data destination, prefix, logging properties.
	clientLogger := log.New(f, "[Client #" + id + "] ", log.Ldate|log.Ltime)


	waitc := make(chan struct{})
	
	
	if err == io.EOF {
		return;
	}

	fmt.Print("Enter your name: ")

	participant, _:= reader.ReadString('\n')
	participant = strings.TrimSpace(participant)

	joinMessage := " has joined the chat at " + strconv.Itoa(int(logicalClock)) + "\n"
	streamMessage.Send(&proto.Message{
		Id: id,
		Participant: participant,
		Message: joinMessage,
		Clock: logicalClock,

	})
	clientLogger.Println("[JOINED]")

	// Receiving goroutine
	
    go func() {
		defer close(waitc)
        for {
            msg, err := streamMessage.Recv()
            if err != nil {
				if(!leaving){
                	log.Printf("Receive error: %v", err)
				}
                break
            }
            fmt.Print(msg.Participant, msg.Message)
        }
    }()
	
    // Sending loop
    for {
        text, _ := reader.ReadString('\n')

		if(utf8.RuneCountInString(text) > 128){
			fmt.Println("Message is too long")
			
		} else {
		if(text == "/leave\n"){
			leaving = true;
			streamMessage.CloseSend()
			clientLogger.Println("[DISCONNECTED]")
			<-waitc
			return;

		}

		logicalClock++ // Increment clock on event: Sent Message
        streamMessage.Send(&proto.Message{
            Id: id,
			Participant:      participant,
            Message:   ": " + text,
            Clock: logicalClock,
        })
		clientLogger.Printf("[MESSAGE SENT]: " + text)
	}
    }
	
}

