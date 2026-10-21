package main

import (
	proto "assignment-3/grpc"
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"unicode/utf8"
	

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var leaving bool;


func main() {
	// Open file for writing logs.
	f, err := os.OpenFile("ChitChat.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer f.Close()

	// Set output of logs to file.
	log.SetOutput(f)

	conn, err := grpc.NewClient("localhost:5050", grpc.WithTransportCredentials(insecure.NewCredentials()))


	client := proto.NewChitChatClient(conn)
	

	reader := bufio.NewReader(os.Stdin)
    

	streamMessage, err := client.PostMessage(context.Background())
	header, err := streamMessage.Header()
		if err != nil {
			log.Fatalf("failed to get header from stream: %v", err)
		}
	id := header["client-id"][0] //handle error here??? see here:https://github.com/grpc/grpc-go/blob/master/examples/features/metadata/client/main.go

	waitc := make(chan struct{})
	
	
	if err == io.EOF {
		return;
	}

	if err != nil {
		log.Fatalf("Not working")
	}

	fmt.Print("Enter your name: ")

	participant, _:= reader.ReadString('\n')
	participant = strings.TrimSpace(participant)

	timeStamp := timestamppb.Now()
	joinMessage := " has joined the chat at " + timeStamp.AsTime().Local().Format("2006-01-02 15:04:05") + "\n"
	streamMessage.Send(&proto.Message{
		Id: id,
		Participant: participant,
		Message: joinMessage,
		LogicalTimestamp: timeStamp,

	})

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
			streamMessage.CloseSend()
			leaving = true;
			<-waitc
			return;

		}
        streamMessage.Send(&proto.Message{
            Id: id,
			Participant:      participant,
            Message:   ": " + text,
            LogicalTimestamp: timestamppb.Now(),
        })
	}
    }
	
}

