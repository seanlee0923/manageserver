package manageserver

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/seanlee0923/manageserver/protocol"
)

// Session is a single connected Client as seen by the Server. PersistenceID
// is an opaque value set by the server's auth hook (WithAuthFunc) so the
// caller can attach its own storage identifier (e.g. a DB row id) to the
// connection without manageserver knowing its shape.
type Session struct {
	id            string
	PersistenceID any

	conn    *websocket.Conn
	outCh   chan []byte
	pTicker *time.Ticker
	done    chan struct{}

	sendTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
	pendingCalls sync.Map
	pendingCnt   atomic.Int32
	pendingDone  chan struct{}

	closeOnce sync.Once
}

// ID returns the session's connection id.
func (s *Session) ID() string {
	return s.id
}

// Pending returns the number of Send calls currently waiting on a response.
func (s *Session) Pending() int32 {
	return s.pendingCnt.Load()
}

func (s *Session) waitDrain(ctx context.Context) error {
	for s.pendingCnt.Load() > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.pendingDone:
		}
	}
	return nil
}

// Send issues a request to this client and blocks until a matching
// response arrives or the server's sendTimeout elapses.
func (s *Session) Send(action string, data any) (*protocol.Message, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	req := &protocol.Message{
		Id:     uuid.NewString(),
		Type:   protocol.Req,
		Action: action,
		Data:   raw,
	}

	msgBytes, err := req.ToBytes()
	if err != nil {
		return nil, err
	}

	respCh := make(chan *protocol.Message, 1)
	s.pendingCalls.Store(req.Id, respCh)
	s.pendingCnt.Add(1)
	defer func() {
		s.pendingCalls.Delete(req.Id)
		s.pendingCnt.Add(-1)
		select {
		case s.pendingDone <- struct{}{}:
		default:
		}
	}()

	select {
	case s.outCh <- msgBytes:
	case <-s.done:
		return nil, errors.New("manageserver: connection closed")
	}

	select {
	case resp := <-respCh:
		return resp, nil
	case <-s.done:
		return nil, errors.New("manageserver: connection closed")
	case <-time.After(s.sendTimeout):
		return nil, errors.New("manageserver: send timeout")
	}
}

// Notify sends a fire-and-forget message to this client: unlike Send, it
// does not wait for (or expect) a matching response. Intended for streaming
// payloads (e.g. terminal I/O chunks) where a full request/response round
// trip per message would be unnecessary overhead.
func (s *Session) Notify(action string, data any) error {
	raw, err := json.Marshal(data)
	if err != nil {
		return err
	}

	msg := &protocol.Message{
		Id:     uuid.NewString(),
		Type:   protocol.Notify,
		Action: action,
		Data:   raw,
	}

	msgBytes, err := msg.ToBytes()
	if err != nil {
		return err
	}

	select {
	case s.outCh <- msgBytes:
		return nil
	case <-s.done:
		return errors.New("manageserver: connection closed")
	}
}

// closeConn tears the connection down exactly once, however it's triggered —
// a read failure, a write failure, or both racing each other. Without this,
// a write failure alone would leave writePump exited but readPump still
// blocked on a read that may never come, so done never fires and
// Send/outCh sends wouldn't fail fast.
func (s *Session) closeConn(srv *Server) {
	s.closeOnce.Do(func() {
		s.conn.Close()
		close(s.done)
		srv.delete(s.id)
	})
}

func (s *Session) readPump(srv *Server) {
	defer s.closeConn(srv)

	for {
		_, message, err := s.conn.ReadMessage()
		if err != nil {
			srv.reportError(err)
			return
		}
		s.conn.SetReadDeadline(time.Now().Add(s.readTimeout))

		msg, err := protocol.ToMessage(message)
		if err != nil {
			srv.reportError(err)
			return
		}

		if msg.Type == protocol.Resp {
			if call, ok := s.pendingCalls.Load(msg.Id); ok {
				if callCh, ok := call.(chan *protocol.Message); ok {
					callCh <- msg
				}
			}
			continue
		}

		h := srv.getHandler(msg.Action)
		if h == nil {
			continue
		}

		if msg.Type == protocol.Notify {
			h(s, msg)
			if srv.onActivity != nil {
				srv.onActivity(s)
			}
			continue
		}

		resp := h(s, msg)
		if resp == nil {
			break
		}

		respBytes, err := json.Marshal(resp)
		if err != nil {
			srv.reportError(err)
			break
		}

		respMsg := protocol.Message{
			Id:     msg.Id,
			Type:   protocol.Resp,
			Action: msg.Action,
			Data:   respBytes,
		}

		outBytes, err := respMsg.ToBytes()
		if err != nil {
			srv.reportError(err)
			break
		}

		select {
		case s.outCh <- outBytes:
		case <-s.done:
			return
		}

		if srv.onActivity != nil {
			srv.onActivity(s)
		}
	}
}

func (s *Session) writePump(srv *Server) {
	defer s.pTicker.Stop()
	for {
		select {
		case <-s.done:
			return

		case msg := <-s.outCh:
			s.conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
			w, err := s.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				srv.reportError(err)
				s.closeConn(srv)
				return
			}
			if _, err = w.Write(msg); err != nil {
				srv.reportError(err)
				s.closeConn(srv)
				return
			}
			if err = w.Close(); err != nil {
				srv.reportError(err)
				s.closeConn(srv)
				return
			}

		case <-s.pTicker.C:
			s.conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))
			if err := s.conn.WriteMessage(websocket.PingMessage, []byte{}); err != nil {
				srv.reportError(err)
				s.closeConn(srv)
				return
			}
		}
	}
}
