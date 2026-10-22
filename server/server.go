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
)

var clientId int;

type ChitChatServer struct {
	proto.UnimplementedChitChatServer
	mu      sync.Mutex
	clients map[string]proto.ChitChat_PostMessageServer
	serverLogicalClock int64
}


func (s *ChitChatServer) PostMessage(stream grpc.BidiStreamingServer[proto.Message, proto.Message]) error {
	
	// --- Opens or creates a new server log
	const append = os.O_APPEND; // adss new logs to the end of the file
	const create = os.O_CREATE; // creates the file if it doesn't exist
	const wronly = os.O_WRONLY; // opens the file for writing only
	
	f, err := os.OpenFile("server.log", append|create|wronly, 0666)
	if err != nil {
		log.Fatalf("Error opening file: %v", err) 
	}
	defer f.Close()
	
	// New creates a new Logger with log data destination, prefix, logging properties.
	serverLogger := log.New(f, "[Server] ", log.Ldate|log.Ltime)



	// --- [Event: Client connection] --- 
	var participant string;
	clientId++
	s.serverLogicalClock++
	serverLogger.Println("[CLIENT #" + strconv.Itoa(clientId) + " HAS BEEN CONNECTED]")
	
	header := metadata.New(map[string]string{"client-id":strconv.Itoa(clientId)})
	stream.SendHeader(header)

	for {
    	message, err := stream.Recv()

		// --- Sync with other "nodes" clock and increment
		if message.GetClock() > s.serverLogicalClock { // Skal det være lige før leaving event eller broadcasting besked eller må det godt ske lige når beskeden modtages?
			s.serverLogicalClock = message.GetClock()
		}
		s.serverLogicalClock++


    	if err == io.EOF {
			s.mu.Lock() // We're deleting from s.clients so we should lock 

			// --- Leaving event ---
			ExitMessage := " has left the chat at " + strconv.Itoa(int(s.serverLogicalClock)) + "\n"
			
			for p, st := range s.clients {
            if p != participant {
                _ = st.Send(&proto.Message{
            		Participant:      participant,
            		Message:   ExitMessage,
            		Clock: s.serverLogicalClock,
        			})
				}
				
			}
			serverLogger.Printf("[CLIENT DISCONNECTED]: Client %s", participant) // Missing the client ID here...
			defer delete(s.clients,participant)
			s.mu.Unlock()

      		return nil
    	}

    
		if err != nil {
    	  return err
    	}

		// --- [Event: Broadcasting join message] --- 
		id := message.GetId()
		participant = message.GetParticipant();
		text := message.GetMessage();

		s.mu.Lock()
		s.clients[participant] = stream

		for p, st := range s.clients {
				if p != participant || p == participant { // Skal lige fixes??? Message should be sent to all clients, including the one that sent it.
					_ = st.Send(&proto.Message{
						Participant:      participant,
						Message:   text,
						Clock: s.serverLogicalClock + 1, //increment 
						}) 
						serverLogger.Println("[JOIN MESSAGE HAS BEEN BROADCASTED] to client#" + id)
				}
			}
			s.mu.Unlock()
	}
}

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
