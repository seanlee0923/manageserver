package manageserver

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/seanlee0923/manageserver/protocol"
)

// Server accepts websocket connections from Clients and dispatches
// incoming requests to registered ServerHandlers. It has no knowledge of
// any particular application's storage — authentication and per-connection
// bookkeeping are wired in via WithAuthFunc / WithOnConnect / WithOnDisconnect
// / WithOnActivity.
type Server struct {
	addr     string
	certFile string
	keyFile  string

	u websocket.Upgrader

	sendTimeout  time.Duration
	readTimeout  time.Duration
	writeTimeout time.Duration
	pingInterval time.Duration
	readLimit    int64

	requestValidator func(r *http.Request, id string) bool
	authFunc         func(id string) (any, bool)
	onConnect        func(*Session)
	onDisconnect     func(*Session)
	onActivity       func(*Session)
	onPong           func(*Session)
	onError          func(error)
	onInbound        func(*Session, *protocol.Message)

	mu      sync.Mutex
	h       map[string]ServerHandler
	clients map[string]*Session
}

func NewServer(opts ...ServerOption) (*Server, error) {
	s := &Server{
		addr: "0.0.0.0:8080",
		u: websocket.Upgrader{
			Subprotocols: []string{},
		},
		sendTimeout:  60 * time.Second,
		readTimeout:  5 * time.Minute,
		writeTimeout: 30 * time.Second,
		pingInterval: 2 * time.Minute,
		readLimit:    4 * 1024 * 1024,
		h:            make(map[string]ServerHandler),
		clients:      make(map[string]*Session),
	}

	for _, opt := range opts {
		if err := opt(s); err != nil {
			return nil, err
		}
	}

	return s, nil
}

// On registers a handler for messages with the given action name.
func (s *Server) On(action string, handler ServerHandler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.h[action] = handler
}

func (s *Server) getHandler(action string) ServerHandler {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.h[action]
}

// Get returns the currently connected session for id, if any.
func (s *Server) Get(id string) (*Session, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.clients[id]
	return sess, ok
}

// CloseSession waits for any Send calls in flight on the session for id to
// finish (or ctx to be done), then forcibly closes its connection. If ctx
// is done first, the connection is force-closed anyway and ctx.Err() is
// returned; the caller decides whether that's worth surfacing.
func (s *Server) CloseSession(ctx context.Context, id string) error {
	sess, ok := s.Get(id)
	if !ok {
		return fmt.Errorf("manageserver: no session for id %q", id)
	}
	err := sess.waitDrain(ctx)
	sess.closeConn(s)
	return err
}

func (s *Server) delete(id string) {
	s.mu.Lock()
	sess, ok := s.clients[id]
	if ok {
		delete(s.clients, id)
	}
	s.mu.Unlock()

	if ok && s.onDisconnect != nil {
		go s.onDisconnect(sess)
	}
}

func (s *Server) reportError(err error) {
	if s.onError != nil {
		s.onError(err)
	}
}

// Run starts serving on the configured address (WithPort) at path (e.g.
// "/ws/", defaults to "/ws/" if empty) and blocks. If WithTLS was
// configured it serves wss://, otherwise plain ws://.
func (s *Server) Run(path string) error {
	p := normalizePath(path)

	mux := http.NewServeMux()
	mux.HandleFunc(p, func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, p)
		if id == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if s.requestValidator != nil && !s.requestValidator(r, id) {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		var data any
		if s.authFunc != nil {
			var ok bool
			data, ok = s.authFunc(id)
			if !ok {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
		}

		// Fast pre-check to avoid upgrading an obviously-duplicate
		// connection; the authoritative check happens atomically in
		// registerSession after the (slow) upgrade completes below.
		if _, exists := s.Get(id); exists {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		conn, err := s.u.Upgrade(w, r, nil)
		if err != nil {
			s.reportError(err)
			return
		}

		sess := &Session{
			id:            id,
			PersistenceID: data,
			conn:          conn,
			outCh:         make(chan []byte),
			done:          make(chan struct{}),
			pendingDone:   make(chan struct{}, 1),
			pTicker:       time.NewTicker(s.pingInterval),
			sendTimeout:   s.sendTimeout,
			readTimeout:   s.readTimeout,
			writeTimeout:  s.writeTimeout,
		}

		if !s.registerSession(sess) {
			// Someone else registered this id while we were upgrading.
			conn.Close()
			return
		}

		conn.SetReadLimit(s.readLimit)
		conn.SetReadDeadline(time.Now().Add(s.readTimeout))
		conn.SetPongHandler(func(string) error {
			conn.SetReadDeadline(time.Now().Add(s.readTimeout))
			if s.onPong != nil {
				s.onPong(sess)
			}
			return nil
		})

		go sess.readPump(s)
		go sess.writePump(s)

		if s.onConnect != nil {
			go s.onConnect(sess)
		}
	})

	if s.certFile != "" && s.keyFile != "" {
		return http.ListenAndServeTLS(s.addr, s.certFile, s.keyFile, mux)
	}

	return http.ListenAndServe(s.addr, mux)
}

// registerSession atomically checks for and inserts a session under one
// lock, closing the TOCTOU window between the pre-check in Run and the
// insert (two concurrent connects for the same id could otherwise both
// pass the pre-check and the second addition would silently clobber the
// first, leaking its socket and goroutines).
func (s *Server) registerSession(sess *Session) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.clients[sess.id]; exists {
		return false
	}
	s.clients[sess.id] = sess
	return true
}
