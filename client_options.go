package manageserver

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"
)

type ClientOption func(*Client) error

// WithID sets the client's connection id, used both as the URL path segment
// when dialing the server and to identify this client to the server side.
// Required.
func WithID(id string) ClientOption {
	return func(c *Client) error {
		c.id = id
		return nil
	}
}

// WithHMACAuth signs the connection handshake with an HMAC-SHA256 signature
// over the client id, a fresh nonce and the current time, using secret. The
// signature is sent as request headers on the websocket upgrade and checked
// server-side by HMACRequestValidator (see hmac.go) or a custom
// WithRequestValidator func — servers that don't opt into either simply
// ignore the headers.
func WithHMACAuth(secret string) ClientOption {
	return func(c *Client) error {
		c.hmacSecret = secret
		return nil
	}
}

// WithRequestTimeout overrides how long Send waits for a response before
// giving up (default 30 minutes).
func WithRequestTimeout(d time.Duration) ClientOption {
	return func(c *Client) error {
		c.sendTimeout = d
		return nil
	}
}

// WithClientReadTimeout overrides how long the connection may go without a
// server ping or message before it's considered dead and torn down (default
// 5 minutes). Refreshed on every ping received and every successfully read
// message, so a healthy connection under the server's ping interval never
// comes close to it.
func WithClientReadTimeout(d time.Duration) ClientOption {
	return func(c *Client) error {
		c.readTimeout = d
		return nil
	}
}

// WithClientWriteTimeout overrides how long a single websocket write (a
// request, a pong, or an outgoing message) may block before the connection
// is considered dead and torn down (default 30 seconds).
func WithClientWriteTimeout(d time.Duration) ClientOption {
	return func(c *Client) error {
		c.writeTimeout = d
		return nil
	}
}

// WithClientReadLimit overrides the maximum size in bytes of a single
// incoming websocket message; larger frames cause the connection to be
// closed (default 4MiB, matching local-central-client's file-upload chunk
// size).
func WithClientReadLimit(n int64) ClientOption {
	return func(c *Client) error {
		c.readLimit = n
		return nil
	}
}

// WithPingHandler registers a callback fired each time the client receives
// a ping from the server. Runs synchronously on the read loop before the
// pong is queued for sending — keep it fast. Useful for observability (e.g.
// logging last-seen times); the read-deadline refresh and pong reply happen
// regardless of whether this is set.
func WithPingHandler(f func(*Client)) ClientOption {
	return func(c *Client) error {
		c.onPing = f
		return nil
	}
}

// WithErrorHandler registers a callback invoked whenever the client hits a
// connection-level error (dial/read/write/decode failures). manageserver
// does no logging of its own, so wire this into the caller's own logger.
func WithErrorHandler(f func(error)) ClientOption {
	return func(c *Client) error {
		c.onError = f
		return nil
	}
}

// WithConnectHandler registers a callback fired once the connection is
// established (after Start's dial succeeds, before it starts serving
// incoming messages). Runs in its own goroutine — use it to kick off any
// domain-specific background work (tickers, watchers, ...) that should run
// only while connected; tie that work to Client.Context() so it stops when
// the connection closes.
func WithConnectHandler(f func(*Client)) ClientOption {
	return func(c *Client) error {
		c.onConnect = f
		return nil
	}
}

func ensureClientTLSConfig(c *Client) *tls.Config {
	if c.tlsConfig == nil {
		c.tlsConfig = &tls.Config{}
	}
	return c.tlsConfig
}

// WithTLSConfig sets a fully custom tls.Config on the client's dialer,
// giving full control over certificate verification (custom CA pool, cert
// pinning, minimum TLS version, etc). Later TLS options (WithRootCAFile,
// WithInsecureSkipVerify) mutate this same config.
func WithTLSConfig(cfg *tls.Config) ClientOption {
	return func(c *Client) error {
		c.tlsConfig = cfg
		return nil
	}
}

// WithRootCAFile adds a PEM-encoded CA certificate to the pool used to
// verify the server's certificate, for on-prem deployments signed by a
// private/self-signed CA rather than a public one.
func WithRootCAFile(pemFile string) ClientOption {
	return func(c *Client) error {
		pemBytes, err := os.ReadFile(pemFile)
		if err != nil {
			return fmt.Errorf("failed to read TLS root CA file: %w", err)
		}

		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}

		if !pool.AppendCertsFromPEM(pemBytes) {
			return fmt.Errorf("failed to parse TLS root CA file: %s", pemFile)
		}

		ensureClientTLSConfig(c).RootCAs = pool
		return nil
	}
}

// WithInsecureSkipVerify disables server certificate verification entirely.
// Intended only for lab/test environments with self-signed certs where a
// proper CA isn't available — never enable this in production. The caller
// is expected to log this choice itself; manageserver stays logging-agnostic.
func WithInsecureSkipVerify() ClientOption {
	return func(c *Client) error {
		ensureClientTLSConfig(c).InsecureSkipVerify = true
		return nil
	}
}
