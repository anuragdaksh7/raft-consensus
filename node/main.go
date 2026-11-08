package node

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"raft/transport"
	"sync"
	"time"
)

type NodeLog struct {
	Index   uint64
	Term    uint64
	Command string
}

type NodeState struct {
	Value int
}

type NodeRole string

const (
	Leader    NodeRole = "LEADER"
	Follower  NodeRole = "FOLLOWER"
	Candidate NodeRole = "CANDIDATE"
)

type Node struct {
	ID        string
	Peers     []string
	Term      int
	Logs      []NodeLog
	Timeoutms int
	IsLeader  bool
	Leader    *net.Conn
	Votes     int
	State     NodeState
	VotedFor  string
	Role      NodeRole

	mu sync.Mutex
}

func (n *Node) Start(address string) {
	listner, err := transport.StartServer(address)
	if err != nil {
		return
	}

	defer listner.Close()

	go n.leaderPollingJob()

	for {
		conn, err := listner.Accept()
		if err != nil {
			log.Println("Error accepting connection: ", err)
			continue
		}

		go n.handleConnection(conn)
	}
}

func (n *Node) handleConnection(conn net.Conn) {
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(3 * time.Second))

	message, err := transport.ParseMessage(conn)
	if err != nil {
		log.Println("Error reading request: ", err)
		return
	}

	switch message.Type {
	case "request_vote":
		var req transport.RequestVoteRequest

		if err := json.Unmarshal(message.Payload, &req); err != nil {
			log.Println("invalid vote request: ", err)
			return
		}

		response := n.HandleRequestVote(req)

		if err := transport.SendMessage(conn, "request_vote_response", response); err != nil {
			log.Println("error sending vote response: ", err)
		}
	case "leader_ping":
		fmt.Println("leader ping recieved")
		if err := transport.SendMessage(conn, "leader_ping_response", struct{}{}); err != nil {
			log.Println("heartbeat response error: ", err)
		}
	default:
		fmt.Println("unknown message:", message.Type)
	}
}

func (n *Node) leaderPollingJob() {
	interval := time.Duration(n.Timeoutms) * time.Millisecond
	leader := n.Leader

	for range time.Tick(interval) {
		if leader == nil || *leader == nil {
			log.Println("leader not found !start election!")
			n.election()
			continue
		}
		err := transport.SendMessage(*leader, "leader_ping", struct{}{})
		if err != nil {
			log.Println("leader lost connection !reset leader!")
			n.Leader = nil
		}
		log.Println("leader alive")
	}
}

func (n *Node) election() {
	// numNodes := len(n.Peers)
	// majority := numNodes/2 + 1

	n.Votes = 1
	n.Term += 1

}

func (n *Node) RequestVote(peerAddress string, req transport.RequestVoteRequest) (
	transport.RequestVoteResponse, error,
) {
	conn, err := net.DialTimeout(
		"tcp",
		peerAddress,
		2*time.Second,
	)
	if err != nil {
		return transport.RequestVoteResponse{}, err
	}

	defer conn.Close()

	if err := conn.SetDeadline(
		time.Now().Add(3 * time.Second),
	); err != nil {
		return transport.RequestVoteResponse{}, err
	}

	if err := transport.SendMessage(
		conn,
		"request_vote",
		req,
	); err != nil {
		return transport.RequestVoteResponse{}, err
	}

	message, err := transport.ParseMessage(conn)
	if err != nil {
		return transport.RequestVoteResponse{}, err
	}

	if message.Type != "request_vote_response" {
		return transport.RequestVoteResponse{}, fmt.Errorf("unexpected response: %s", message.Type)
	}

	var response transport.RequestVoteResponse

	if err := json.Unmarshal(message.Payload, &response); err != nil {
		return transport.RequestVoteResponse{}, err
	}

	return response, nil
}

func (n *Node) HandleRequestVote(req transport.RequestVoteRequest) transport.RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	// check election term
	if n.Term > req.Term {
		return transport.RequestVoteResponse{
			Term:        n.Term,
			VoteGranted: false,
		}
	} else if n.Term < req.Term {
		n.Term = req.Term
		n.VotedFor = ""
	}

	// check if already voted
	if n.VotedFor != "" && n.VotedFor != req.CandidateID {
		return transport.RequestVoteResponse{
			Term:        n.Term,
			VoteGranted: false,
		}
	}

	followerLastLogIndex := -1
	followerLastLogTerm := -1
	if len(n.Logs) > 0 {
		followerLastLogIndex = int(n.Logs[len(n.Logs)-1].Index)
		followerLastLogTerm = int(n.Logs[len(n.Logs)-1].Term)
	}

	// check last log term
	if followerLastLogTerm > req.LastLogTerm {
		return transport.RequestVoteResponse{
			Term:        n.Term,
			VoteGranted: false,
		}
	} else if followerLastLogTerm == req.LastLogTerm {
		if followerLastLogIndex > req.LastLogIndex {
			return transport.RequestVoteResponse{
				Term:        n.Term,
				VoteGranted: false,
			}
		}
	}

	n.VotedFor = req.CandidateID

	return transport.RequestVoteResponse{
		Term:        n.Term,
		VoteGranted: true,
	}
}
