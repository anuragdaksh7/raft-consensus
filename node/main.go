package node

import (
	"fmt"
	"log"
	"net"
	"raft/transport"
)

type NodeLog struct {
	Index   int
	Term    int
	Command string
}

type Node struct {
	Peers     []string
	Term      int
	Logs      []NodeLog
	Timeoutms int
	IsLeader  bool
}

func (n *Node) Start(address string) {
	listner, err := transport.StartServer(address)
	if err != nil {
		return
	}

	defer listner.Close()

	for {
		conn, err := listner.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	message, err := transport.ParseMessage(conn)
	if err != nil {
		log.Println("Error reading request: ", err)
		return
	}

	switch message {
	case string(transport.LeaderPingMessage):
		fmt.Println("leader ping recieve")
	default:
		fmt.Println("unknown message " + message)
	}
}
