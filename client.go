package manageserver

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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

	tlsConfig    *tls.Config
	sendTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
	readLimit    int64
	hmacSecret   string

	onError   func(error)
	onConnect func(*Client)
	onPing    func(*Client)

	ctx    context.Context
	cancel context.CancelFunc

	h map[string]ClientHandler

	pendingCalls sync.Map
	pendingCnt   atomic.Int32
	pendingDone  chan struct{}

	outCh   chan []byte
	pCh     chan []byte
	CloseCh chan struct{}

	mu        sync.Mutex
	started   bool
	closeOnce sync.Once
}

func NewClient(opts ...ClientOption) (*Client, error) {
	c := &Client{
		h:            make(map[string]ClientHandler),
		outCh:        make(chan []byte),
		pCh:          make(chan []byte),
		CloseCh:      make(chan struct{}),
		pendingDone:  make(chan struct{}, 1),
		sendTimeout:  30 * time.Minute,
		readTimeout:  5 * time.Minute,
		writeTimeout: 30 * time.Second,
		readLimit:    4 * 1024 * 1024,
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
// Any Send calls still waiting on a response are aborted immediately with
// a "connection closed" error; use Shutdown to let them finish first.
func (c *Client) Close() error {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()

	if conn == nil {
		return nil
	}
	return conn.Close()
}

// Pending returns the number of Send calls currently waiting on a response.
func (c *Client) Pending() int32 {
	return c.pendingCnt.Load()
}

// Shutdown waits for in-flight Send calls to finish (or ctx to be done),
// then closes the connection. If ctx is done first, the connection is
// force-closed via Close and ctx.Err() is returned.
func (c *Client) Shutdown(ctx context.Context) error {
	err := c.waitDrain(ctx)
	if closeErr := c.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	return err
}

func (c *Client) waitDrain(ctx context.Context) error {
	for c.pendingCnt.Load() > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.pendingDone:
		}
	}
	return nil
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

	var header http.Header
	if c.hmacSecret != "" {
		header = http.Header{}
		signHMACRequest(header, c.id, c.hmacSecret, uuid.NewString(), time.Now())
	}

	conn, _, err := c.dialer.Dial(urlStr, header)
	if err != nil {
		return err
	}

	conn.SetReadLimit(c.readLimit)
	conn.SetReadDeadline(time.Now().Add(c.readTimeout))
	conn.SetPingHandler(func(appData string) error {
		conn.SetReadDeadline(time.Now().Add(c.readTimeout))
		if c.onPing != nil {
			c.onPing(c)
		}
		select {
		case c.pCh <- []byte(""):
		case <-c.ctx.Done():
		}
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
		select {
		case c.pendingDone <- struct{}{}:
		default:
		}
	}()

	select {
	case c.outCh <- msgBytes:
	case <-c.ctx.Done():
		return nil, errors.New("manageserver: connection closed")
	}

	select {
	case resp := <-respCh:
		if err := responseProtocolError(resp); err != nil {
			return nil, err
		}
		return resp, nil
	case <-c.ctx.Done():
		return nil, errors.New("manageserver: connection closed")
	case <-time.After(c.sendTimeout):
		return nil, errors.New("manageserver: send timeout")
	}
}

// Notify sends a fire-and-forget message: unlike Send, it does not wait for
// (or expect) a matching response. Intended for streaming payloads (e.g.
// terminal I/O chunks) where a full request/response round trip per message
// would be unnecessary overhead. Returns once the message is handed off to
// the write pump, or immediately if the connection is already closed.
func (c *Client) Notify(action string, data any) error {
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
	case c.outCh <- msgBytes:
		return nil
	case <-c.ctx.Done():
		return errors.New("manageserver: connection closed")
	}
}

func (c *Client) reportError(err error) {
	if c.onError != nil {
		c.onError(err)
	}
}

// closeConn tears the connection down exactly once, however it's triggered —
// a read failure, a write failure, or both racing each other. Without this,
// a write failure alone would leave writePump exited but readPump still
// blocked on a read that may never come, so ctx never gets canceled and
// Send/outCh sends wouldn't fail fast.
func (c *Client) closeConn() {
	c.closeOnce.Do(func() {
		c.cancel()
		c.conn.Close()
		close(c.CloseCh)
	})
}

func (c *Client) readPump() {
	defer c.closeConn()

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			c.reportError(err)
			return
		}
		c.conn.SetReadDeadline(time.Now().Add(c.readTimeout))

		msg, err := protocol.ToMessage(message)
		if err != nil {
			c.reportError(err)
			return
		}

		if msg.Type == protocol.Resp || msg.Type == protocol.Err {
			if call, ok := c.pendingCalls.Load(msg.Id); ok {
				if callCh, ok := call.(chan *protocol.Message); ok {
					callCh <- msg
				}
			}
			continue
		}

		h := c.h[msg.Action]
		if h == nil {
			dispatchErr := &DispatchError{
				Code: protocolCodeUnknownAction, Side: "client", SessionID: c.ID(),
				MessageID: msg.Id, Action: msg.Action, Cause: "handler not registered",
			}
			c.reportError(dispatchErr)
			if msg.Type == protocol.Req {
				if err := c.sendResponse(msg, protocol.Err,
					protocolErrorPayload(protocolCodeUnknownAction, "unknown action")); err != nil {
					c.reportError(err)
					return
				}
			}
			continue
		}

		if msg.Type == protocol.Notify {
			_, _ = invokeClientHandler(c, msg, h)
			continue
		}

		resp, panicked := invokeClientHandler(c, msg, h)
		if panicked {
			if err := c.sendResponse(msg, protocol.Err,
				protocolErrorPayload(protocolCodeHandlerPanic, "handler failed")); err != nil {
				c.reportError(err)
				return
			}
			continue
		}
		if resp == nil {
			break
		}
		if err := c.sendResponse(msg, protocol.Resp, resp); err != nil {
			c.reportError(err)
			break
		}
	}
}

func invokeClientHandler(client *Client, msg *protocol.Message, handler ClientHandler) (resp any, panicked bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			panicked = true
			client.reportError(&DispatchError{
				Code: protocolCodeHandlerPanic, Side: "client", SessionID: client.ID(),
				MessageID: msg.Id, Action: msg.Action, Cause: recovered,
			})
		}
	}()
	return handler(client, msg), false
}

func (c *Client) sendResponse(request *protocol.Message, messageType protocol.MessageType, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	response := protocol.Message{Id: request.Id, Type: messageType, Action: request.Action, Data: data}
	outBytes, err := response.ToBytes()
	if err != nil {
		return err
	}
	select {
	case c.outCh <- outBytes:
		return nil
	case <-c.ctx.Done():
		return fmt.Errorf("manageserver: connection closed")
	}
}

func (c *Client) writePump() {
	for {
		select {
		case <-c.ctx.Done():
			return

		case msg := <-c.outCh:
			c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				c.reportError(err)
				c.closeConn()
				return
			}
			if _, err = w.Write(msg); err != nil {
				c.reportError(err)
				c.closeConn()
				return
			}
			if err = w.Close(); err != nil {
				c.reportError(err)
				c.closeConn()
				return
			}

		case <-c.pCh:
			c.conn.SetWriteDeadline(time.Now().Add(c.writeTimeout))
			if err := c.conn.WriteMessage(websocket.PongMessage, nil); err != nil {
				c.reportError(err)
				c.closeConn()
				return
			}
		}
	}
}
