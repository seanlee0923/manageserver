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

// WithRequestTimeout overrides how long Send waits for a response before
// giving up (default 30 minutes).
func WithRequestTimeout(d time.Duration) ClientOption {
	return func(c *Client) error {
		c.sendTimeout = d
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
