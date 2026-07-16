package protocol

import "encoding/json"

const (
	Req MessageType = iota
	Resp
	Err
	// Notify is a fire-and-forget message: the receiver dispatches it to the
	// registered handler for side effects only and never sends a Resp back,
	// regardless of what the handler returns. Unlike Req, a nil handler
	// return does not close the connection. Intended for streaming use cases
	// (e.g. terminal I/O chunks) where a Req/Resp round trip per chunk would
	// be unnecessary overhead.
	Notify
)

type MessageType int

type Message struct {
	Id     string          `json:"id"`
	Type   MessageType     `json:"type"`
	Action string          `json:"action"`
	Data   json.RawMessage `json:"data"`
}

func ToMessage(message []byte) (*Message, error) {
	var msg Message

	err := json.Unmarshal(message, &msg)
	if err != nil {
		return nil, err
	}

	return &msg, nil
}

func (m *Message) ToBytes() ([]byte, error) {
	return json.Marshal(m)
}
