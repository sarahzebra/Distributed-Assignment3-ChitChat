package main

import (
	proto "assignment-3/grpc"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"


	//"time"
	//"context"

	//"google.golang.org/genproto/googleapis/cloud/notebooks/logging/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var clientId int;
var shutdown bool = false;
var grpcServer *grpc.Server;
	

type ChitChatServer struct {
	proto.UnimplementedChitChatServer
	mu      sync.Mutex
	clients map[int64]proto.ChitChat_PostMessageServer
	serverLogicalClock int64
}

func (s *ChitChatServer) PostMessage(stream grpc.BidiStreamingServer[proto.Message, proto.Message]) error {

	serverLogger := serverLogger()
	
	// --- [Event: Client connection] --- 
	var clientIdMessage int64;
	var participant string;
	clientId++

	s.serverLogicalClock++
	serverLogger.Println("[CLIENT #" + strconv.Itoa(clientId) + " HAS BEEN CONNECTED] at logical timestamp: " + strconv.Itoa(int(s.serverLogicalClock)))
	
	header := metadata.New(map[string]string{"client-id":strconv.Itoa(clientId)})
	stream.SendHeader(header)
	//Evt. synkroniser af clock

	for {
    	message, err := stream.Recv()
		
		// --- [Event: Client connection] --- 
		// --- Sync with other "nodes" clock and increment
		
		if message.GetClock() > s.serverLogicalClock { // Skal det være lige før leaving event eller broadcasting besked eller må det godt ske lige når beskeden modtages?
			s.serverLogicalClock = message.GetClock()
		}
		s.serverLogicalClock++
	


    	if err == io.EOF {
			s.mu.Lock() // We're deleting from s.clients so we should lock 
			
		

			// --- [Event: leaving event] --- 
			ExitMessage := participant + " has left the chat at server clock: " + strconv.Itoa(int(s.serverLogicalClock)) + "\n"

			for id, st := range s.clients {
            if id != clientIdMessage {
				s.serverLogicalClock++;
				serverLogger.Println("[EXIT MESSAGE HAS BEEN BROADCASTED] to client#" + strconv.Itoa(int(id)) + " at logical timestamp: " + strconv.Itoa(int(s.serverLogicalClock)) + " with amount of clients: " + strconv.Itoa(int(len(s.clients))))
                _ = st.Send(&proto.Message{
            		Participant:      participant,
            		Message:   ExitMessage,
            		Clock: s.serverLogicalClock,
        			})
				}
				
			}
			serverLogger.Printf("[CLIENT DISCONNECTED]: Client#" + strconv.Itoa(int(clientIdMessage)) +  " at logical timestamp: " + strconv.Itoa(int(s.serverLogicalClock))) // Missing the client ID here...
			delete(s.clients,clientIdMessage)
			s.mu.Unlock()

			if len(s.clients) <= 0 {
				// --- [Event: server shutdown] ---
				s.serverLogicalClock++
				grpcServer.Stop()
				serverLogger.Printf("[SERVER SHUTDOWN] at logical timestamp: " + strconv.Itoa(int(s.serverLogicalClock)))
			}
			
				

      		return nil
    	}

    
		if err != nil {
    	  return err
    	}

		// --- [Event: Broadcasting message] --- 
		clientIdMessage = message.GetId();
		participant = message.GetParticipant();
		text := message.GetMessage();

		s.mu.Lock()
		s.clients[clientIdMessage] = stream

		for id, st := range s.clients {
				
				if(strings.Contains(text, "has joined the chat at ")){
					if true{
						s.serverLogicalClock++;
						serverLogger.Println("[MESSAGE: '" + strings.TrimSpace(text) + "' HAS BEEN BROADCASTED] to client#" + strconv.Itoa(int(id)) + " at logical timestamp: " + strconv.Itoa(int(s.serverLogicalClock)) + " with amount of clients: " + strconv.Itoa(int(len(s.clients))))
						_ = st.Send(&proto.Message{
							Participant:      participant,
							Message:   text,
							Clock: s.serverLogicalClock, //increment 
							}) 
					}
				} else {
					if id != clientIdMessage { //Join Message should be sent to all clients, including the one that sent it.

						s.serverLogicalClock++;
						serverLogger.Println("[MESSAGE: '" + strings.TrimSpace(text) + "' HAS BEEN BROADCASTED] to client#" + strconv.Itoa(int(id)) + " at logical timestamp: " + strconv.Itoa(int(s.serverLogicalClock)) + " with amount of clients: " + strconv.Itoa(int(len(s.clients))))
						_ = st.Send(&proto.Message{
							Participant:      participant,
							Message:  participant + ": " + text,
							Clock: s.serverLogicalClock, //increment 
							}) 
							//Join skal rykkes (message gets logged multiple times)
							
				}
			}

				

			}
			s.mu.Unlock()
	}
}

func main() {
	grpcServer := NewServer()
	
	grpcServer.start_server()

	
}

func (s *ChitChatServer) start_server() {
	grpcServer = grpc.NewServer()
	listener, err := net.Listen("tcp", ":5050")
	if err != nil {
		log.Fatalf("Did not work" + err.Error())
	}
	
	// --- [Event: Server Startup] --- 
	serverLogger := serverLogger()
	s.serverLogicalClock++
	serverLogger.Printf("[SERVER STARTUP] at logical timestamp: " + strconv.Itoa(int(s.serverLogicalClock)))

	proto.RegisterChitChatServer(grpcServer, s)

	err = grpcServer.Serve(listener)


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
        clients: make(map[int64]proto.ChitChat_PostMessageServer),
    }
}