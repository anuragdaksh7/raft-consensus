package transport

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
)

type FixMessage string

const (
	LeaderPingMessage FixMessage = "AWAKE?"
	LeaderResponse    FixMessage = "YES!"
)

type Message struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type RequestVoteRequest struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int    `json:"last_log_index"`
	LastLogTerm  int    `json:"last_log_term"`
}

type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

func StartServer(address string) (net.Listener, error) {
	listner, err := net.Listen("tcp", ":"+address)
	if err != nil {
		log.Println("Error listening:", err)
		return nil, err
	}

	return listner, nil
}

// generic for parsing tcp message
func ParseMessage(conn net.Conn) (Message, error) {
	reader := bufio.NewReader(conn)

	data, err := reader.ReadBytes('\n')
	if err != nil {
		return Message{}, err
	}

	if len(data) > 1024*1024 {
		return Message{}, fmt.Errorf("message too large")
	}

	var message Message

	err = json.Unmarshal(data, &message)
	if err != nil {
		return Message{}, err
	}

	return message, nil
}

func SendMessage(
	conn net.Conn,
	messageType string,
	payload any,
) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	message := Message{
		Type:    messageType,
		Payload: data,
	}

	encoded, err := json.Marshal(message)
	if err != nil {
		return err
	}

	encoded = append(encoded, '\n')

	for len(encoded) > 0 {
		n, err := conn.Write(encoded)
		if err != nil {
			return err
		}

		if n == 0 {
			return io.ErrShortWrite
		}

		encoded = encoded[n:]
	}

	return nil
}
