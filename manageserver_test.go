package manageserver_test

import (
	"context"
	"encoding/json"
	"errors"
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

func TestHMACRequestValidatorAcceptsCorrectSecret(t *testing.T) {
	srv, addr := startServer(t, manageserver.WithRequestValidator(
		manageserver.HMACRequestValidator(func(id string) (string, bool) {
			if id != "site-1" {
				return "", false
			}
			return "shared-secret", true
		}, 0),
	))

	c, err := manageserver.NewClient(manageserver.WithID("site-1"), manageserver.WithHMACAuth("shared-secret"))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "site-1")
}

func TestHMACRequestValidatorRejectsWrongSecret(t *testing.T) {
	_, addr := startServer(t, manageserver.WithRequestValidator(
		manageserver.HMACRequestValidator(func(id string) (string, bool) {
			return "shared-secret", true
		}, 0),
	))

	c, err := manageserver.NewClient(manageserver.WithID("site-1"), manageserver.WithHMACAuth("wrong-secret"))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(addr, "/ws/"); err == nil {
		t.Fatal("expected connection with wrong HMAC secret to be rejected")
	}
}

func TestHMACRequestValidatorRejectsMissingSignature(t *testing.T) {
	_, addr := startServer(t, manageserver.WithRequestValidator(
		manageserver.HMACRequestValidator(func(id string) (string, bool) {
			return "shared-secret", true
		}, 0),
	))

	// No WithHMACAuth on the client at all — no signature headers sent.
	c, err := manageserver.NewClient(manageserver.WithID("site-1"))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(addr, "/ws/"); err == nil {
		t.Fatal("expected connection with no HMAC signature to be rejected")
	}
}

func TestHMACRequestValidatorRejectsUnknownID(t *testing.T) {
	_, addr := startServer(t, manageserver.WithRequestValidator(
		manageserver.HMACRequestValidator(func(id string) (string, bool) {
			return "", false // unknown id, no secret provisioned
		}, 0),
	))

	c, err := manageserver.NewClient(manageserver.WithID("unknown-site"), manageserver.WithHMACAuth("whatever"))
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(addr, "/ws/"); err == nil {
		t.Fatal("expected connection for an id with no provisioned secret to be rejected")
	}
}

// TestWithAuthFuncUnaffectedByRequestValidator guards the design intent that
// WithRequestValidator is fully independent of WithAuthFunc: setting only
// WithAuthFunc (no validator) must keep behaving exactly as before.
func TestWithAuthFuncUnaffectedByRequestValidator(t *testing.T) {
	srv, addr := startServer(t, manageserver.WithAuthFunc(func(id string) (any, bool) {
		return nil, id == "allowed"
	}))

	c, err := manageserver.NewClient(manageserver.WithID("allowed"))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "allowed")
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

// waitForPendingAbove blocks until pending() reports a value greater than
// zero, i.e. a Send has registered itself but not yet returned.
func waitForPendingAbove(t *testing.T, pending func() int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for pending() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("Send never became pending")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPendingCountReturnsToZeroAfterSend(t *testing.T) {
	srv, addr := startServer(t)
	srv.On("Echo", func(sess *manageserver.Session, msg *protocol.Message) any {
		return map[string]string{"ok": "true"}
	})

	c, err := manageserver.NewClient(manageserver.WithID("device-pending-count"))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "device-pending-count")

	if _, err := c.Send("Echo", map[string]string{}); err != nil {
		t.Fatalf("Send failed: %v", err)
	}
	if got := c.Pending(); got != 0 {
		t.Fatalf("expected Pending()==0 after Send completes, got %d", got)
	}
}

// TestClientShutdownWaitsForPendingSend guards the core Shutdown contract:
// it must let an in-flight Send finish normally rather than aborting it the
// way Close does.
func TestClientShutdownWaitsForPendingSend(t *testing.T) {
	srv, addr := startServer(t)

	release := make(chan struct{})
	srv.On("Slow", func(sess *manageserver.Session, msg *protocol.Message) any {
		<-release
		return map[string]string{"ok": "true"}
	})

	c, err := manageserver.NewClient(manageserver.WithID("device-shutdown-wait"))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "device-shutdown-wait")

	sendDone := make(chan error, 1)
	go func() {
		_, err := c.Send("Slow", map[string]string{})
		sendDone <- err
	}()
	waitForPendingAbove(t, c.Pending)

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- c.Shutdown(context.Background()) }()

	select {
	case <-shutdownDone:
		t.Fatal("Shutdown returned before the pending Send completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-sendDone:
		if err != nil {
			t.Fatalf("expected the pending Send to complete successfully, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not complete after release")
	}

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("expected Shutdown to succeed, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not return after the pending Send completed")
	}

	if got := c.Pending(); got != 0 {
		t.Fatalf("expected Pending()==0 after Shutdown, got %d", got)
	}
}

// TestClientShutdownForceClosesOnTimeout guards the fallback path: if ctx
// expires before the pending Send finishes, Shutdown must force-close the
// connection instead of blocking forever.
func TestClientShutdownForceClosesOnTimeout(t *testing.T) {
	srv, addr := startServer(t)

	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	srv.On("Forever", func(sess *manageserver.Session, msg *protocol.Message) any {
		<-block
		return nil
	})

	c, err := manageserver.NewClient(manageserver.WithID("device-shutdown-timeout"))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "device-shutdown-timeout")

	go func() { _, _ = c.Send("Forever", map[string]string{}) }()
	waitForPendingAbove(t, c.Pending)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	if err := c.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
	}

	select {
	case <-c.Context().Done():
	case <-time.After(1 * time.Second):
		t.Fatal("connection was not force-closed after Shutdown timed out")
	}
}

// TestServerCloseSessionWaitsForPendingSend mirrors
// TestClientShutdownWaitsForPendingSend from the server side.
func TestServerCloseSessionWaitsForPendingSend(t *testing.T) {
	srv, addr := startServer(t)

	c, err := manageserver.NewClient(manageserver.WithID("device-close-session-wait"))
	if err != nil {
		t.Fatal(err)
	}

	release := make(chan struct{})
	c.On("Slow", func(cl *manageserver.Client, msg *protocol.Message) any {
		<-release
		return map[string]string{"ok": "true"}
	})

	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "device-close-session-wait")

	sess, ok := srv.Get("device-close-session-wait")
	if !ok {
		t.Fatal("expected session to be registered")
	}

	sendDone := make(chan error, 1)
	go func() {
		_, err := sess.Send("Slow", map[string]string{})
		sendDone <- err
	}()
	waitForPendingAbove(t, sess.Pending)

	closeDone := make(chan error, 1)
	go func() { closeDone <- srv.CloseSession(context.Background(), "device-close-session-wait") }()

	select {
	case <-closeDone:
		t.Fatal("CloseSession returned before the pending Send completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	select {
	case err := <-sendDone:
		if err != nil {
			t.Fatalf("expected the pending Send to complete successfully, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Send did not complete after release")
	}

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("expected CloseSession to succeed, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CloseSession did not return after the pending Send completed")
	}
}

// TestServerCloseSessionForceClosesOnTimeout mirrors
// TestClientShutdownForceClosesOnTimeout from the server side, and also
// checks that the session is removed from the registry once force-closed.
func TestServerCloseSessionForceClosesOnTimeout(t *testing.T) {
	srv, addr := startServer(t)

	c, err := manageserver.NewClient(manageserver.WithID("device-close-session-timeout"))
	if err != nil {
		t.Fatal(err)
	}

	block := make(chan struct{})
	t.Cleanup(func() { close(block) })
	c.On("Forever", func(cl *manageserver.Client, msg *protocol.Message) any {
		<-block
		return nil
	})

	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "device-close-session-timeout")

	sess, ok := srv.Get("device-close-session-timeout")
	if !ok {
		t.Fatal("expected session to be registered")
	}

	go func() { _, _ = sess.Send("Forever", map[string]string{}) }()
	waitForPendingAbove(t, sess.Pending)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err = srv.CloseSession(ctx, "device-close-session-timeout")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context.DeadlineExceeded, got: %v", err)
	}

	if _, ok := srv.Get("device-close-session-timeout"); ok {
		t.Fatal("expected session to be removed from the registry after force close")
	}
}

func TestServerCloseSessionUnknownID(t *testing.T) {
	srv, _ := startServer(t)
	if err := srv.CloseSession(context.Background(), "no-such-session"); err == nil {
		t.Fatal("expected an error for an unknown session id")
	}
}
