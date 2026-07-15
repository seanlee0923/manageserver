package protocol

import "encoding/json"

const (
	Req MessageType = iota
	Resp
	Err
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
