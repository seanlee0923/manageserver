package manageserver_test

import (
	"encoding/json"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/seanlee0923/manageserver"
	"github.com/seanlee0923/manageserver/protocol"
)

// freePort grabs an OS-assigned free TCP port for a test server to bind to.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return strconv.Itoa(port)
}

// waitForPort blocks until something is listening on port, or fails the test.
func waitForPort(t *testing.T, port string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.Dial("tcp", "127.0.0.1:"+port)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("server on port %s did not come up in time", port)
}

// waitForSession blocks until id shows up in the server's session registry.
func waitForSession(t *testing.T, s *manageserver.Server, id string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := s.Get(id); ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("session %q was never registered", id)
}

// startServer builds and runs a Server on a free port with the given
// options (WithPort is added automatically) and returns it along with the
// "ws://host:port" base URL to dial.
func startServer(t *testing.T, opts ...manageserver.ServerOption) (*manageserver.Server, string) {
	t.Helper()
	port := freePort(t)
	allOpts := append([]manageserver.ServerOption{manageserver.WithPort(port)}, opts...)

	s, err := manageserver.NewServer(allOpts...)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		_ = s.Run("/ws/")
	}()
	waitForPort(t, port)

	return s, "ws://127.0.0.1:" + port
}

func TestRoundTrip(t *testing.T) {
	srv, addr := startServer(t)

	type pingReq struct {
		N int `json:"n"`
	}
	type pingResp struct {
		N int `json:"n"`
	}

	srv.On("Ping", func(sess *manageserver.Session, msg *protocol.Message) any {
		var req pingReq
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			t.Error(err)
		}
		return pingResp{N: req.N + 1}
	})

	c, err := manageserver.NewClient(manageserver.WithID("device-1"))
	if err != nil {
		t.Fatal(err)
	}

	pushed := make(chan map[string]string, 1)
	c.On("Push", func(cl *manageserver.Client, msg *protocol.Message) any {
		var payload map[string]string
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			t.Error(err)
		}
		pushed <- payload
		return pingResp{N: 0}
	})

	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "device-1")

	resp, err := c.Send("Ping", pingReq{N: 1})
	if err != nil {
		t.Fatalf("client Send failed: %v", err)
	}
	var pr pingResp
	if err := json.Unmarshal(resp.Data, &pr); err != nil {
		t.Fatal(err)
	}
	if pr.N != 2 {
		t.Fatalf("expected N=2, got %d", pr.N)
	}

	sess, ok := srv.Get("device-1")
	if !ok {
		t.Fatal("expected session to be registered")
	}
	if _, err := sess.Send("Push", map[string]string{"hello": "world"}); err != nil {
		t.Fatalf("server Send failed: %v", err)
	}

	select {
	case payload := <-pushed:
		if payload["hello"] != "world" {
			t.Fatalf("unexpected pushed payload: %+v", payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server push to reach client handler")
	}
}

func TestDuplicateConnectionRejected(t *testing.T) {
	srv, addr := startServer(t)

	c1, err := manageserver.NewClient(manageserver.WithID("dup-1"))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c1.Start(addr, "/ws/") }()
	waitForSession(t, srv, "dup-1")

	c2, err := manageserver.NewClient(manageserver.WithID("dup-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := c2.Start(addr, "/ws/"); err == nil {
		t.Fatal("expected second connection with the same id to be rejected")
	}
}

func TestAuthRejected(t *testing.T) {
	_, addr := startServer(t, manageserver.WithAuthFunc(func(id string) (any, bool) {
		return nil, id == "allowed"
	}))

	c, err := manageserver.NewClient(manageserver.WithID("not-allowed"))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(addr, "/ws/"); err == nil {
		t.Fatal("expected connection to be rejected by auth func")
	}
}

func TestClientDoubleStartFails(t *testing.T) {
	_, addr := startServer(t)

	c, err := manageserver.NewClient(manageserver.WithID("device-double-start"))
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = c.Start(addr, "/ws/") }()
	time.Sleep(100 * time.Millisecond)

	if err := c.Start(addr, "/ws/"); err == nil {
		t.Fatal("expected second Start on the same client to fail")
	}
}

// TestSendFailsFastAfterClose guards against the writePump/Send goroutine
// leak & hang that used to happen on disconnect: Send must return promptly
// once the connection is closed, not block until sendTimeout (30m default).
func TestSendFailsFastAfterClose(t *testing.T) {
	srv, addr := startServer(t)

	c, err := manageserver.NewClient(manageserver.WithID("device-close"))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "device-close")

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case <-c.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("Context was not canceled after Close")
	}

	done := make(chan error, 1)
	go func() {
		_, err := c.Send("Whatever", map[string]string{})
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected Send to fail after Close")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Send did not return promptly after Close (regressed to hanging until sendTimeout)")
	}
}

func TestCustomPath(t *testing.T) {
	port := freePort(t)
	s, err := manageserver.NewServer(manageserver.WithPort(port))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Run("/device/") }()
	waitForPort(t, port)

	addr := "ws://127.0.0.1:" + port

	c, err := manageserver.NewClient(manageserver.WithID("device-path"))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Start(addr, "/device/") }()
	waitForSession(t, s, "device-path")

	wrongPathClient, err := manageserver.NewClient(manageserver.WithID("device-path-2"))
	if err != nil {
		t.Fatal(err)
	}
	if err := wrongPathClient.Start(addr, "/ws/"); err == nil {
		t.Fatal("expected dial against the wrong path to fail")
	}
}
