package manageserver

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/seanlee0923/manageserver/protocol"
)

// Client is a websocket connection to a manageserver Server. Domain logic
// (what to send, on what schedule) lives entirely in the caller; Client only
// owns the connection, message framing and request/response correlation.
type Client struct {
	id     string
	conn   *websocket.Conn
	dialer *websocket.Dialer

	tlsConfig   *tls.Config
	sendTimeout time.Duration

	onError   func(error)
	onConnect func(*Client)

	ctx    context.Context
	cancel context.CancelFunc

	h map[string]ClientHandler

	pendingCalls sync.Map
	pendingCnt   atomic.Int32

	outCh   chan []byte
	pCh     chan []byte
	CloseCh chan struct{}

	mu      sync.Mutex
	started bool
}

func NewClient(opts ...ClientOption) (*Client, error) {
	c := &Client{
		h:           make(map[string]ClientHandler),
		outCh:       make(chan []byte),
		pCh:         make(chan []byte),
		CloseCh:     make(chan struct{}),
		sendTimeout: 30 * time.Minute,
	}
	// Valid from construction (not just after Start succeeds) so Context()
	// and the ctx.Done() cases in Send/writePump never see a nil context.
	c.ctx, c.cancel = context.WithCancel(context.Background())

	for _, opt := range opts {
		if err := opt(c); err != nil {
			return nil, err
		}
	}

	if c.id == "" {
		return nil, errors.New("manageserver: client id is required (use WithID)")
	}

	dialer := *websocket.DefaultDialer
	dialer.TLSClientConfig = c.tlsConfig
	c.dialer = &dialer

	return c, nil
}

// On registers a handler for messages with the given action name.
func (c *Client) On(action string, handler ClientHandler) {
	c.h[action] = handler
}

// ID returns the client's connection id.
func (c *Client) ID() string {
	return c.id
}

// Context is canceled once the connection closes, so callers can tie their
// own background work (tickers, watchers, ...) to the connection's lifetime.
func (c *Client) Context() context.Context {
	return c.ctx
}

// Close closes the underlying connection, causing Start to return and
// Context to be canceled. Safe to call before Start (returns nil then).
func (c *Client) Close() error {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()

	if conn == nil {
		return nil
	}
	return conn.Close()
}

// Start dials serverAddr (e.g. "ws://host:port" or "wss://host:port") joined
// with path (e.g. "/ws/", defaults to "/ws/" if empty) and the client id,
// then blocks, serving incoming messages, until the connection closes. A
// *Client may only be started once; construct a new one to reconnect.
func (c *Client) Start(serverAddr, path string) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return errors.New("manageserver: client already started")
	}
	c.started = true
	c.mu.Unlock()

	urlStr := serverAddr + normalizePath(path) + c.id
	conn, _, err := c.dialer.Dial(urlStr, nil)
	if err != nil {
		return err
	}

	conn.SetPingHandler(func(appData string) error {
		c.pCh <- []byte("")
		return nil
	})
	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	go c.writePump()
	if c.onConnect != nil {
		go c.onConnect(c)
	}
	c.readPump()
	return nil
}

// Send issues a request and blocks until a matching response arrives or
// sendTimeout elapses (configurable via WithSendTimeout).
func (c *Client) Send(action string, data any) (*protocol.Message, error) {
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
	c.pendingCalls.Store(req.Id, respCh)
	c.pendingCnt.Add(1)
	defer func() {
		c.pendingCalls.Delete(req.Id)
		c.pendingCnt.Add(-1)
	}()

	select {
	case c.outCh <- msgBytes:
	case <-c.ctx.Done():
		return nil, errors.New("manageserver: connection closed")
	}

	select {
	case resp := <-respCh:
		return resp, nil
	case <-c.ctx.Done():
		return nil, errors.New("manageserver: connection closed")
	case <-time.After(c.sendTimeout):
		return nil, errors.New("manageserver: send timeout")
	}
}

func (c *Client) reportError(err error) {
	if c.onError != nil {
		c.onError(err)
	}
}

func (c *Client) readPump() {
	defer func() {
		c.cancel()
		c.conn.Close()
		close(c.CloseCh)
	}()

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			c.reportError(err)
			return
		}

		msg, err := protocol.ToMessage(message)
		if err != nil {
			c.reportError(err)
			return
		}

		if msg.Type == protocol.Resp {
			if call, ok := c.pendingCalls.Load(msg.Id); ok {
				if callCh, ok := call.(chan *protocol.Message); ok {
					callCh <- msg
				}
			}
			continue
		}

		h := c.h[msg.Action]
		if h == nil {
			continue
		}

		resp := h(c, msg)
		if resp == nil {
			break
		}

		respBytes, err := json.Marshal(resp)
		if err != nil {
			c.reportError(err)
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
			c.reportError(err)
			break
		}

		c.outCh <- outBytes
	}
}

func (c *Client) writePump() {
	for {
		select {
		case <-c.ctx.Done():
			return

		case msg := <-c.outCh:
			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				c.reportError(err)
				return
			}
			if _, err = w.Write(msg); err != nil {
				c.reportError(err)
				return
			}
			if err = w.Close(); err != nil {
				c.reportError(err)
				return
			}

		case <-c.pCh:
			if err := c.conn.WriteMessage(websocket.PongMessage, nil); err != nil {
				c.reportError(err)
				return
			}
		}
	}
}
