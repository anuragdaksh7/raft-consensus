package transport

type MessageType uint8

const (
	RequestVote MessageType = iota
	RequestVoteResponse
	AppendEntries
	AppendEntriesResponse
	InstallSnapshot
)

type Message struct {
	From string
	To   string
	Type MessageType
	Body []byte
}

type Transport interface {
	Send(peerID string, msg Message) error
	RegisterHandler(func(Message))
	Start() error
	Stop() error
}
