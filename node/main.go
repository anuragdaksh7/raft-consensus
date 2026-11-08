package node

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"raft/transport"
	"sync"
	"time"
)

type NodeLog = transport.NodeLog

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
	StateFile string

	mu              sync.Mutex
	electionMu      sync.Mutex
	timerMu         sync.Mutex
	electionResetCh chan struct{}
}

func (n *Node) Start(address string) {
	n.ensureElectionTimer()
	n.loadPersistentState()

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

func (n *Node) ensureElectionTimer() {
	n.timerMu.Lock()
	defer n.timerMu.Unlock()
	if n.electionResetCh == nil {
		n.electionResetCh = make(chan struct{}, 1)
	}
}

func (n *Node) resetElectionTimer() {
	n.ensureElectionTimer()
	n.timerMu.Lock()
	resetCh := n.electionResetCh
	n.timerMu.Unlock()
	select {
	case resetCh <- struct{}{}:
	default:
	}
}

type persistentState struct {
	Term     int    `json:"term"`
	VotedFor string `json:"voted_for"`
}

func (n *Node) statePath() string {
	if n.StateFile != "" {
		return n.StateFile
	}
	return "raft-state-" + n.ID + ".json"
}

func (n *Node) loadPersistentState() {
	data, err := os.ReadFile(n.statePath())
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		log.Printf("error reading persistent state: %v", err)
		return
	}

	var state persistentState
	if err := json.Unmarshal(data, &state); err != nil {
		log.Printf("error decoding persistent state: %v", err)
		return
	}

	n.mu.Lock()
	n.Term = state.Term
	n.VotedFor = state.VotedFor
	n.mu.Unlock()
}

func (n *Node) persistLocked() {
	data, err := json.Marshal(persistentState{Term: n.Term, VotedFor: n.VotedFor})
	if err != nil {
		log.Printf("error encoding persistent state: %v", err)
		return
	}
	if err := os.WriteFile(n.statePath(), data, 0600); err != nil {
		log.Printf("error writing persistent state: %v", err)
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
	case "append_entries":
		var req transport.AppendEntriesRequest
		if err := json.Unmarshal(message.Payload, &req); err != nil {
			log.Println("invalid append entries request: ", err)
			return
		}

		response := n.HandleAppendEntries(req)
		if err := transport.SendMessage(conn, "append_entries_response", response); err != nil {
			log.Println("error sending append entries response: ", err)
		}
	default:
		fmt.Println("unknown message:", message.Type)
	}
}

func (n *Node) leaderPollingJob() {
	n.ensureElectionTimer()
	for {
		timeout := n.electionTimeout()
		timer := time.NewTimer(timeout)
		select {
		case <-timer.C:
			n.mu.Lock()
			isLeader := n.Role == Leader
			n.mu.Unlock()
			if !isLeader {
				n.election()
			}
		case <-n.electionResetCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
	}
}

func (n *Node) electionTimeout() time.Duration {
	base := n.Timeoutms
	if base <= 0 {
		base = 1000
	}
	return time.Duration(base+rand.IntN(base)) * time.Millisecond
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
	n.IsLeader = false
	electionTerm := n.Term
	lastLogIndex := -1
	lastLogTerm := -1
	if len(n.Logs) > 0 {
		lastLogIndex = int(n.Logs[len(n.Logs)-1].Index)
		lastLogTerm = int(n.Logs[len(n.Logs)-1].Term)
	}
	n.Votes = 1
	n.persistLocked()
	n.mu.Unlock()
	n.resetElectionTimer()
	log.Printf("starting election for term %d", electionTerm)

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
			n.IsLeader = false
			n.VotedFor = ""
			n.Votes = 0
			n.persistLocked()
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

		n.mu.Lock()
		won := n.Role == Candidate && n.Term == electionTerm && n.Votes >= majority
		if won {
			n.Role = Leader
			n.IsLeader = true
		}
		n.mu.Unlock()
		if won {
			go n.heartbeatLoop(electionTerm)
			log.Printf("became leader for term %d with %d votes", electionTerm, n.Votes)
			return
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.Role == Candidate && n.Term == electionTerm && n.Votes >= majority {
		n.Role = Leader
		n.IsLeader = true
		go n.heartbeatLoop(electionTerm)
		log.Printf("became leader for term %d with %d votes", electionTerm, n.Votes)
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
		n.IsLeader = false
		n.persistLocked()
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
	n.persistLocked()
	n.resetElectionTimer()

	return transport.RequestVoteResponse{
		Term:        n.Term,
		VoteGranted: true,
	}
}

func (n *Node) HandleAppendEntries(req transport.AppendEntriesRequest) transport.AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.Term {
		return transport.AppendEntriesResponse{Term: n.Term, Success: false}
	}
	if req.Term > n.Term {
		n.Term = req.Term
		n.VotedFor = ""
		n.persistLocked()
	}
	previousRole := n.Role
	n.Role = Follower
	n.IsLeader = false
	n.resetElectionTimer()
	if previousRole != Follower {
		log.Printf("accepted leader %s for term %d", req.LeaderID, req.Term)
	}

	if req.PrevLogIndex >= 0 {
		if req.PrevLogIndex >= len(n.Logs) ||
			int(n.Logs[req.PrevLogIndex].Term) != req.PrevLogTerm {
			return transport.AppendEntriesResponse{Term: n.Term, Success: false}
		}
	}

	for _, entry := range req.Entries {
		localIndex := int(entry.Index)
		if localIndex < len(n.Logs) {
			if n.Logs[localIndex].Term != entry.Term {
				n.Logs = n.Logs[:localIndex]
				n.Logs = append(n.Logs, NodeLog{
					Index: entry.Index, Term: entry.Term, Command: entry.Command,
				})
			}
			continue
		}
		if localIndex == len(n.Logs) {
			n.Logs = append(n.Logs, NodeLog{
				Index: entry.Index, Term: entry.Term, Command: entry.Command,
			})
		}
	}
	return transport.AppendEntriesResponse{Term: n.Term, Success: true}
}

func (n *Node) heartbeatLoop(term int) {
	n.sendHeartbeats(term)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		n.mu.Lock()
		active := n.Role == Leader && n.Term == term
		n.mu.Unlock()
		if !active {
			return
		}
		n.sendHeartbeats(term)
	}
}

func (n *Node) sendHeartbeats(term int) {
	n.mu.Lock()
	if n.Role != Leader || n.Term != term {
		n.mu.Unlock()
		return
	}
	prevIndex := len(n.Logs) - 1
	prevTerm := -1
	if prevIndex >= 0 {
		prevTerm = int(n.Logs[prevIndex].Term)
	}
	req := transport.AppendEntriesRequest{
		Term:         n.Term,
		LeaderID:     n.ID,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		LeaderCommit: -1,
	}
	peers := append([]string(nil), n.Peers...)
	n.mu.Unlock()

	for _, peer := range peers {
		go func(peer string) {
			res, err := n.AppendEntries(peer, req)
			if err != nil {
				log.Printf("heartbeat to %s failed: %v", peer, err)
				return
			}
			if res.Term > term {
				n.mu.Lock()
				if res.Term > n.Term {
					n.Term = res.Term
					n.Role = Follower
					n.IsLeader = false
					n.VotedFor = ""
					n.persistLocked()
				}
				n.mu.Unlock()
			}
		}(peer)
	}
}

func (n *Node) AppendEntries(peerAddress string, req transport.AppendEntriesRequest) (
	transport.AppendEntriesResponse, error,
) {
	conn, err := net.DialTimeout("tcp", peerAddress, 2*time.Second)
	if err != nil {
		return transport.AppendEntriesResponse{}, err
	}
	defer conn.Close()

	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return transport.AppendEntriesResponse{}, err
	}
	if err := transport.SendMessage(conn, "append_entries", req); err != nil {
		return transport.AppendEntriesResponse{}, err
	}

	message, err := transport.ParseMessage(conn)
	if err != nil {
		return transport.AppendEntriesResponse{}, err
	}
	if message.Type != "append_entries_response" {
		return transport.AppendEntriesResponse{}, fmt.Errorf("unexpected response: %s", message.Type)
	}

	var response transport.AppendEntriesResponse
	if err := json.Unmarshal(message.Payload, &response); err != nil {
		return transport.AppendEntriesResponse{}, err
	}
	return response, nil
}
