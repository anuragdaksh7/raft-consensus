package tcp

import (
	"net"
	"raft/transport"
)

type TCPTransport struct {
	addr     string
	listener net.Listener
	handler  func(message transport.Message)
}

func New(addr string) *TCPTransport {
	return &TCPTransport{
		addr: addr,
	}
}

func (t *TCPTransport) Send() error {}

func (t *TCPTransport) RegisterHandler() {}

func (t *TCPTransport) Start() error {}

func (t *TCPTransport) Stop() error {}
