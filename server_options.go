package manageserver

import (
	"net/http"
	"strings"
	"time"
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

// WithOnError registers a callback invoked whenever the server hits a
// connection-level error. manageserver does no logging of its own, so wire
// this into the caller's own logger.
func WithOnError(f func(error)) ServerOption {
	return func(s *Server) error {
		s.onError = f
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
