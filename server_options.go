package manageserver

import (
	"net/http"
	"strings"
	"time"

	"github.com/seanlee0923/manageserver/protocol"
)

type ServerOption func(*Server) error

// WithPort sets the TCP port to listen on; the server binds all interfaces
// (0.0.0.0:<port>).
func WithPort(port string) ServerOption {
	return func(s *Server) error {
		s.addr = "0.0.0.0:" + strings.TrimPrefix(port, ":")
		return nil
	}
}

// WithTLS enables wss:// by serving with the given PEM-encoded certificate
// and private key instead of plain HTTP.
func WithTLS(certFile, keyFile string) ServerOption {
	return func(s *Server) error {
		s.certFile = certFile
		s.keyFile = keyFile
		return nil
	}
}

// WithAuthFunc validates an incoming connection's id (e.g. against a
// database) before the websocket upgrade completes. Returning ok=false
// rejects the connection with 401. The returned value is stashed on the
// resulting Session.PersistenceID.
func WithAuthFunc(f func(id string) (persistenceID any, ok bool)) ServerOption {
	return func(s *Server) error {
		s.authFunc = f
		return nil
	}
}

// WithRequestValidator registers an optional pre-check that runs against the
// raw incoming request before authFunc, for checks that need more than just
// the connection id — header-based signatures, bearer tokens, mTLS client
// cert inspection, and the like. Returning false rejects the connection with
// 401 before authFunc is even called. Independent of and unrelated to
// WithAuthFunc; leave unset and nothing changes.
//
// manageserver ships one ready-made validator for the common shared-secret
// case, HMACRequestValidator (see hmac.go) — pass its result here, or write
// your own func(r *http.Request, id string) bool for anything else.
func WithRequestValidator(f func(r *http.Request, id string) bool) ServerOption {
	return func(s *Server) error {
		s.requestValidator = f
		return nil
	}
}

// WithOnConnect registers a callback fired once a session finishes
// connecting (after auth, before it starts serving messages).
func WithOnConnect(f func(*Session)) ServerOption {
	return func(s *Server) error {
		s.onConnect = f
		return nil
	}
}

// WithOnDisconnect registers a callback fired once a session's connection
// closes and it's removed from the registry.
func WithOnDisconnect(f func(*Session)) ServerOption {
	return func(s *Server) error {
		s.onDisconnect = f
		return nil
	}
}

// WithOnActivity registers a callback fired after each request from a
// session that was successfully handled, useful e.g. for updating a
// last-seen timestamp.
func WithOnActivity(f func(*Session)) ServerOption {
	return func(s *Server) error {
		s.onActivity = f
		return nil
	}
}

// WithOnPong registers a callback fired each time a pong is received from a
// session in response to the server's periodic ping (see WithPingInterval).
// Runs synchronously on the session's read loop, like WithOnActivity — keep
// it fast. Useful for observability (e.g. logging last-seen times); the
// read-deadline refresh that keeps the connection alive happens regardless
// of whether this is set.
func WithOnPong(f func(*Session)) ServerOption {
	return func(s *Server) error {
		s.onPong = f
		return nil
	}
}

// WithOnError registers a callback invoked whenever the server hits a
// connection-level error. manageserver does no logging of its own, so wire
// this into the caller's own logger.
func WithOnError(f func(error)) ServerOption {
	return func(s *Server) error {
		s.onError = f
		return nil
	}
}

// WithServerInboundHandler registers an observability hook invoked for every
// valid protocol message received from a client. It covers requests,
// notifications, responses and errors, including messages whose action has no
// registered handler. The hook runs synchronously and should return quickly.
func WithServerInboundHandler(f func(*Session, *protocol.Message)) ServerOption {
	return func(s *Server) error {
		s.onInbound = f
		return nil
	}
}

// WithSendTimeout overrides how long Session.Send waits for a response
// before giving up (default 60 seconds).
func WithSendTimeout(d time.Duration) ServerOption {
	return func(s *Server) error {
		s.sendTimeout = d
		return nil
	}
}

// WithReadTimeout overrides how long a session's connection may go without
// a client pong or message before it's considered dead and torn down
// (default 5 minutes). Refreshed on every pong and every successfully read
// message, so a healthy connection under the configured WithPingInterval
// never comes close to it.
func WithReadTimeout(d time.Duration) ServerOption {
	return func(s *Server) error {
		s.readTimeout = d
		return nil
	}
}

// WithWriteTimeout overrides how long a single websocket write (a response,
// a ping, or an outgoing message) may block before the connection is
// considered dead and torn down (default 30 seconds).
func WithWriteTimeout(d time.Duration) ServerOption {
	return func(s *Server) error {
		s.writeTimeout = d
		return nil
	}
}

// WithPingInterval overrides how often the server pings each connected
// session to detect unresponsive peers (default 2 minutes). Keep this well
// under WithReadTimeout so a healthy connection gets several chances to
// respond before being torn down.
func WithPingInterval(d time.Duration) ServerOption {
	return func(s *Server) error {
		s.pingInterval = d
		return nil
	}
}

// WithReadLimit overrides the maximum size in bytes of a single incoming
// websocket message; larger frames cause the connection to be closed
// (default 4MiB, matching local-central-client's file-upload chunk size).
func WithReadLimit(n int64) ServerOption {
	return func(s *Server) error {
		s.readLimit = n
		return nil
	}
}
