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

	//"google.golang.org/genproto/googleapis/cloud/notebooks/logging/v1"
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

	serverLogger := serverLogger()
	
	// --- [Event: Client connection] --- 
	var participant string;
	clientId++

	s.serverLogicalClock++
	serverLogger.Println("[CLIENT #" + strconv.Itoa(clientId) + " HAS BEEN CONNECTED at server clock: " + strconv.Itoa(int(s.serverLogicalClock)))
	
	header := metadata.New(map[string]string{"client-id":strconv.Itoa(clientId)})
	stream.SendHeader(header)
	//Evt. synkroniser af clock

	for {
    	message, err := stream.Recv()
		
		// --- [Event: Client connection] --- 
		// --- Sync with other "nodes" clock and increment
		serverLogger.Printf("Tjek for sync ServerClock: " + strconv.Itoa(int(s.serverLogicalClock)) + " med client #" +  message.Id + " med ClientClock: " + strconv.Itoa(int(message.GetClock())))
		if message.GetClock() > s.serverLogicalClock { // Skal det være lige før leaving event eller broadcasting besked eller må det godt ske lige når beskeden modtages?
			s.serverLogicalClock = message.GetClock()
		}
		s.serverLogicalClock++
		serverLogger.Printf("ServerClock efter sync: " + strconv.Itoa(int(s.serverLogicalClock)))


    	if err == io.EOF {
			s.mu.Lock() // We're deleting from s.clients so we should lock 

			if len(s.clients) <= 0 {
				//stream.GracefulStop();
			}

			// --- [Event: leaving event] --- 
			ExitMessage := " has left the chat at server clock: " + strconv.Itoa(int(s.serverLogicalClock)) + "\n"

			for p, st := range s.clients {
            if p != participant {
				s.serverLogicalClock++;
				serverLogger.Println("[EXIT MESSAGE HAS BEEN BROADCASTED] to client#" + message.GetId() + " at logical timestamp: " + strconv.Itoa(int(s.serverLogicalClock)) + " with amount of clients: " + strconv.Itoa(int(len(s.clients))))
                _ = st.Send(&proto.Message{
            		Participant:      participant,
            		Message:   ExitMessage,
            		Clock: s.serverLogicalClock,
        			})
				}
				
			}
			serverLogger.Printf("[CLIENT DISCONNECTED]: Client " + participant +  " at server clock: " + strconv.Itoa(int(s.serverLogicalClock))) // Missing the client ID here...
			defer delete(s.clients,participant)
			s.mu.Unlock()

      		return nil
    	}

    
		if err != nil {
    	  return err
    	}

		// --- [Event: Broadcasting message] --- 
		// id := message.GetId()
		participant = message.GetParticipant();
		text := message.GetMessage();

		s.mu.Lock()
		s.clients[participant] = stream

		for p, st := range s.clients {
				if p != participant { //Join Message should be sent to all clients, including the one that sent it.
					s.serverLogicalClock++;
					serverLogger.Println("[MESSAGE: '" + text + "' HAS BEEN BROADCASTED] to client#" + p + " at logical timestamp: " + strconv.Itoa(int(s.serverLogicalClock)) + " with amount of clients: " + strconv.Itoa(int(len(s.clients))))
					_ = st.Send(&proto.Message{
						Participant:      participant,
						Message:   text,
						Clock: s.serverLogicalClock, //increment 
						}) 
						//Join skal rykkes (message gets logged multiple times)
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
	
	// --- [Event: Server Startup] --- 
	serverLogger := serverLogger()
	serverLogger.Printf("Server startup at: " + strconv.Itoa(int(s.serverLogicalClock)))

	proto.RegisterChitChatServer(grpcServer, s)

	err = grpcServer.Serve(listener)

	// --- [Event: Server Shutdown] --- 
	serverLogger.Printf("Server shutdown")

	if err != nil {
		log.Fatalf("Did not work")
	}

}

func serverLogger() *log.Logger {
	
	// --- Opens or creates a new server log
	const append = os.O_APPEND; // adss new logs to the end of the file
	const create = os.O_CREATE; // creates the file if it doesn't exist
	const wronly = os.O_WRONLY; // opens the file for writing only
	
	f, err := os.OpenFile("server.log", append|create|wronly, 0666)
	if err != nil {
		log.Fatalf("Error opening file: %v", err) 
	}
	
	// New creates a new Logger with log data destination, prefix, logging properties.
	return log.New(f, "[Server] ", log.Ldate|log.Ltime)
}

func NewServer() *ChitChatServer {
    return &ChitChatServer{
        clients: make(map[string]proto.ChitChat_PostMessageServer),
    }
}