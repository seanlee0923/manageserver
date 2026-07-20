package manageserver

import (
	"encoding/json"
	"fmt"

	"github.com/seanlee0923/manageserver/protocol"
)

const (
	protocolCodeUnknownAction = "unknown_action"
	protocolCodeHandlerPanic  = "handler_panic"
)

// DispatchError is reported through the configured error hook. Its fields
// let callers write structured logs without parsing an error string.
type DispatchError struct {
	Code      string
	Side      string
	SessionID string
	MessageID string
	Action    string
	Cause     any
}

func (e *DispatchError) Error() string {
	return fmt.Sprintf("manageserver: protocol dispatch failed code=%s side=%s session_id=%s message_id=%s action=%s cause=%v",
		e.Code, e.Side, e.SessionID, e.MessageID, e.Action, e.Cause)
}

// RemoteProtocolError is returned by Send when the remote peer replies with
// protocol.Err instead of a normal response.
type RemoteProtocolError struct {
	Code    string
	Message string
}

func (e *RemoteProtocolError) Error() string {
	return fmt.Sprintf("manageserver: remote protocol error code=%s message=%s", e.Code, e.Message)
}

func responseProtocolError(msg *protocol.Message) error {
	if msg.Type != protocol.Err {
		return nil
	}
	var payload protocol.ErrorResp
	if err := json.Unmarshal(msg.Data, &payload); err != nil {
		return fmt.Errorf("manageserver: invalid protocol error response: %w", err)
	}
	return &RemoteProtocolError{Code: payload.Code, Message: payload.Message}
}

func protocolErrorPayload(code, message string) protocol.ErrorResp {
	return protocol.ErrorResp{Code: code, Message: message}
}
