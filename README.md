# Chit Chat Service

This is an implementation of mandatory activity 3 in Distributed Systems 2026. The implmentation uses gRPC to make the 
chit chat service.

## Usage
Make sure you are in the folder 'Distributed-Assignment3-Chitchat':
```bash
cd Distributed-Assignment3-Chitchat
```

### Run the server
Run the server using this command
```bash
go run server/server.go
```

### Run the clients
In another terminal run a client using this command
```bash
go run client/client.go
```
If you want to run multiple clients, you have to open up a new terminal every time.

When running the client, you have to enter your name after the message 'Enter your name:' pops up in the terminal.

### Shutdown client
If you want to make a client leave the chat write this command in the terminal
```bash
/leave
```

### Shutdown server
The server automatically shuts down when there are no active clients left.
