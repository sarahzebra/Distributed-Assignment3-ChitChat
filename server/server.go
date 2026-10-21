package main

import (
	proto "assignment-3/grpc"
	//"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"

	//"time"

	//"context"


	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var clientId int;

type ChitChatServer struct {
	proto.UnimplementedChitChatServer
	mu      sync.Mutex
	clients map[string]proto.ChitChat_PostMessageServer
}


func (s *ChitChatServer) PostMessage(stream grpc.BidiStreamingServer[proto.Message, proto.Message]) error {
	// Log needs: Component name (with identifier fx. client ID), event type, timestamp
	// Events needed to be logged: Server startup/shutdown, client connection/disconnection, join/leave messages, message delivery


	const append = os.O_APPEND; // adss new logs to the end of the file
	const create = os.O_CREATE; // creates the file if it doesn't exist
	const wronly = os.O_WRONLY; // opens the file for writing only
	
	f, err := os.OpenFile("ChitChat.log", append|create|wronly, 0666)
	if err != nil {
		log.Fatalf("Error opening file: %v", err)
	}
	defer f.Close()
	
	// Redirects and sets all standard log outputs to file.
	log.SetOutput(f)

	log.SetPrefix("[Server] ")
	log.SetFlags(log.Ldate|log.Ltime)
	
	log.Println("Server connected")  

	var participant string;
	
	clientId++
	
	header := metadata.New(map[string]string{"client-id":strconv.Itoa(clientId)})

	stream.SendHeader(header)

	

	for {
    	message, err := stream.Recv()

    	if err == io.EOF {
				ExitTimeStamp := timestamppb.Now()
				ExitTime := ExitTimeStamp.AsTime().Local().Format("2006-01-02 15:04:05")
				ExitMessage := " has left the chat at " + ExitTime + "\n"
			for p, st := range s.clients {
            if p != participant {
                _ = st.Send(&proto.Message{
            		Participant:      participant,
            		Message:   ExitMessage,
            		LogicalTimestamp: ExitTimeStamp,
        			})
				}
			
			}
			defer delete(s.clients,participant)
      		return nil
    	}

    
		if err != nil {
    	  return err
    	}
	
	id := message.GetId()
	log.Println(id) //have to log here, just so it doesnt brokker sig
	participant = message.GetParticipant();
	text := message.GetMessage();
	timestamp := message.GetLogicalTimestamp()





	//log.Println(participant, ": ", message.GetMessage())
    s.mu.Lock()
	s.clients[participant] = stream


	for p, st := range s.clients {
            if p != participant {
                _ = st.Send(&proto.Message{
            		Participant:      participant,
            		Message:   text,
            		LogicalTimestamp: timestamp,
        			}) 
			}
        }
        s.mu.Unlock()

  }
}

/*func (s *ChitChatServer) Join(ctx context.Context, message *proto.Message) error {
	var participant string = message.Participant

	for p, st := range s.clients {
            if p != participant {
                _ = st.Send(message.Message) // ignore error for simplicity
            }
        }
        s.mu.Unlock()

  }*/



func main() {
	server := NewServer()

	server.start_server()
	
}

func (s *ChitChatServer) start_server() {
	grpcServer := grpc.NewServer()
	listener, err := net.Listen("tcp", ":5050")
	if err != nil {
		log.Fatalf("Did not work" + err.Error())
	}

	proto.RegisterChitChatServer(grpcServer, s)

	err = grpcServer.Serve(listener)

	if err != nil {
		log.Fatalf("Did not work")
	}

}

func NewServer() *ChitChatServer {
    return &ChitChatServer{
        clients: make(map[string]proto.ChitChat_PostMessageServer),
    }
}

