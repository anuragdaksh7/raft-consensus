package transport

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

type FixMessage string

const (
	LeaderPingMessage FixMessage = "AWAKE?"
	LeaderResponse    FixMessage = "YES!"
)

func main() {
	listner, err := net.Listen("tcp", ":8090")
	if err != nil {
		log.Fatal("Error listening:", err)
	}

	defer listner.Close()

	for {

		conn, err := listner.Accept()
		if err != nil {
			log.Println("Error accepting conn:", err)
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	message, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Read error: %v", err)
		return
	}

	ackMsg := strings.ToUpper(strings.TrimSpace(message))
	response := fmt.Sprintf("ACK: %s\n", ackMsg)
	_, err = conn.Write([]byte(response))
	if err != nil {
		log.Printf("Server write error: %v", err)
	}
}

func StartServer(address string) (net.Listener, error) {
	listner, err := net.Listen("tcp", ":"+address)
	if err != nil {
		log.Println("Error listening:", err)
		return nil, err
	}

	return listner, nil
}

// Ping for leader active check
func PingLeader(conn net.Conn) error {
	defer conn.Close()

	conn.SetDeadline(time.Now().Add(3 * time.Second))

	_, err := conn.Write([]byte(LeaderPingMessage + "\n"))
	if err != nil {
		return err
	}

	_, err = bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return err
	}

	return nil
}

// generic for parsing tcp message
func ParseMessage(conn net.Conn) (string, error) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	message, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("Read error: %v", err)
		return "", err
	}

	return message, nil
}

// leader reply for ping check
func LeaderPingReply(conn net.Conn) error {
	_, err := conn.Write([]byte(LeaderResponse + "\n"))
	if err != nil {
		return err
	}
	return nil
}
