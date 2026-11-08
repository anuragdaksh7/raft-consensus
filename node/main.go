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

	mu         sync.Mutex
	electionMu sync.Mutex
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

	for range time.Tick(interval) {
		n.mu.Lock()
		leader := n.Leader
		n.mu.Unlock()

		if leader == nil || *leader == nil {
			log.Println("leader not found !start election!")
			n.election()
			continue
		}
		err := transport.SendMessage(*leader, "leader_ping", struct{}{})
		if err != nil {
			log.Println("leader lost connection !reset leader!")
			n.mu.Lock()
			n.Leader = nil
			n.mu.Unlock()
		}
		log.Println("leader alive")
	}
}

func (n *Node) election() {
	n.electionMu.Lock()
	defer n.electionMu.Unlock()

	numNodes := len(n.Peers) + 1
	majority := numNodes/2 + 1

	n.mu.Lock()
	n.VotedFor = n.ID
	n.Term += 1
	n.Role = Candidate
	electionTerm := n.Term
	lastLogIndex := -1
	lastLogTerm := -1
	if len(n.Logs) > 0 {
		lastLogIndex = int(n.Logs[len(n.Logs)-1].Index)
		lastLogTerm = int(n.Logs[len(n.Logs)-1].Term)
	}
	n.Votes = 1
	n.mu.Unlock()

	req := transport.RequestVoteRequest{
		Term:         electionTerm,
		CandidateID:  n.ID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	type voteResult struct {
		peer string
		res  transport.RequestVoteResponse
		err  error
	}

	results := make(chan voteResult, len(n.Peers))
	for _, peer := range n.Peers {
		go func(peer string) {
			res, err := n.RequestVote(peer, req)
			results <- voteResult{peer: peer, res: res, err: err}
		}(peer)
	}

	for range n.Peers {
		result := <-results
		if result.err != nil {
			log.Printf("error requesting vote from %s: %v", result.peer, result.err)
			continue
		}

		n.mu.Lock()
		switch {
		case result.res.Term > n.Term:
			n.Term = result.res.Term
			n.Role = Follower
			n.VotedFor = ""
			n.Votes = 0
		case result.res.Term == electionTerm &&
			n.Term == electionTerm &&
			n.Role == Candidate &&
			result.res.VoteGranted:
			n.Votes++
			fmt.Printf("%s voted for %s\n", result.peer, n.ID)
		case result.res.Term == electionTerm && !result.res.VoteGranted:
			fmt.Printf("%s did not vote for %s\n", result.peer, n.ID)
		}
		n.mu.Unlock()
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.Role == Candidate && n.Term == electionTerm && n.Votes >= majority {
		n.Role = Leader
		// TODO: broadcast leader
	}
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
		n.Role = Follower
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
