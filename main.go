package main

import (
	"math/rand/v2"
	"os"
	"raft/node"
)

func main() {

	var port string

	if len(os.Args) > 1 {
		port = os.Args[1]
	}

	timeMin := 1000
	timeMax := 2000

	allNodes := []string{"5001", "5002", "5003", "5004", "5005"}

	var peers []string

	for _, val := range allNodes {
		if val != port {
			peers = append(peers, val)
		}
	}

	var logs []node.NodeLog

	node := node.Node{
		ID:        port,
		Peers:     peers,
		Term:      1,
		Logs:      logs,
		Timeoutms: rand.IntN(timeMax-timeMin) + timeMin,
		IsLeader:  false,
	}

	node.Start(port)
}
