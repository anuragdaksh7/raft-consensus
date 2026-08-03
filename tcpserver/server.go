package tcpserver

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"strings"
)

func serve(port string) {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatal("Error listening: ", err)
	}

	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Error accepting: ", err)
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
		log.Println("Error reading: ", err)
		return
	}

	ackMsg := strings.ToUpper(strings.TrimSpace(message))
	response := fmt.Sprintf("ACK: %s\n", ackMsg)
	_, err = conn.Write([]byte(response))
	if err != nil {
		log.Println("Error writing: ", err)
	}
}
