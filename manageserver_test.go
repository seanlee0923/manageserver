package manageserver_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/seanlee0923/manageserver"
	"github.com/seanlee0923/manageserver/protocol"
)

func TestClientStartReportsHandshakeResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "upstream unavailable", http.StatusBadGateway)
	}))
	defer upstream.Close()

	c, err := manageserver.NewClient(manageserver.WithID("device-1"))
	if err != nil {
		t.Fatal(err)
	}

	err = c.Start("ws"+strings.TrimPrefix(upstream.URL, "http"), "/ws/")
	if err == nil {
		t.Fatal("expected handshake error")
	}

	var handshakeErr *manageserver.HandshakeError
	if !errors.As(err, &handshakeErr) {
		t.Fatalf("expected HandshakeError, got %T: %v", err, err)
	}
	if handshakeErr.StatusCode != http.StatusBadGateway {
		t.Fatalf("status code = %d, want %d", handshakeErr.StatusCode, http.StatusBadGateway)
	}
	if handshakeErr.Body != "upstream unavailable" {
		t.Fatalf("body = %q, want %q", handshakeErr.Body, "upstream unavailable")
	}
	if !errors.Is(err, websocket.ErrBadHandshake) {
		t.Fatalf("error should wrap websocket.ErrBadHandshake: %v", err)
	}
}

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

func TestClientOutboundHandlerObservesRequestsNotifiesAndResponses(t *testing.T) {
	srv, addr := startServer(t)
	srv.On("ClientRequest", func(_ *manageserver.Session, _ *protocol.Message) any {
		return protocol.StatusResp{Ok: true}
	})

	outbound := make(chan protocol.Message, 3)
	c, err := manageserver.NewClient(
		manageserver.WithID("outbound-client"),
		manageserver.WithClientOutboundHandler(func(message *protocol.Message) {
			outbound <- *message
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	c.On("ServerRequest", func(_ *manageserver.Client, _ *protocol.Message) any {
		return protocol.StatusResp{Ok: true}
	})
	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "outbound-client")

	if _, err := c.Send("ClientRequest", struct{}{}); err != nil {
		t.Fatal(err)
	}
	if err := c.Notify("ClientNotify", struct{}{}); err != nil {
		t.Fatal(err)
	}
	sess, ok := srv.Get("outbound-client")
	if !ok {
		t.Fatal("client session not found")
	}
	if _, err := sess.Send("ServerRequest", struct{}{}); err != nil {
		t.Fatal(err)
	}

	want := map[string]protocol.MessageType{
		"ClientRequest": protocol.Req,
		"ClientNotify":  protocol.Notify,
		"ServerRequest": protocol.Resp,
	}
	for range want {
		select {
		case message := <-outbound:
			messageType, exists := want[message.Action]
			if !exists {
				t.Fatalf("unexpected outbound action %q", message.Action)
			}
			if message.Type != messageType {
				t.Fatalf("outbound %s type = %v, want %v", message.Action, message.Type, messageType)
			}
			if message.Id == "" {
				t.Fatalf("outbound %s has empty message id", message.Action)
			}
			delete(want, message.Action)
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for outbound messages; missing=%v", want)
		}
	}
	_ = c.Close()
}

func TestServerInboundHandlerObservesRequestsNotifiesAndResponses(t *testing.T) {
	inbound := make(chan protocol.Message, 3)
	srv, addr := startServer(t,
		manageserver.WithServerInboundHandler(func(_ *manageserver.Session, message *protocol.Message) {
			inbound <- *message
		}),
	)
	srv.On("ClientRequest", func(_ *manageserver.Session, _ *protocol.Message) any {
		return protocol.StatusResp{Ok: true}
	})

	c, err := manageserver.NewClient(manageserver.WithID("inbound-client"))
	if err != nil {
		t.Fatal(err)
	}
	c.On("ServerRequest", func(_ *manageserver.Client, _ *protocol.Message) any {
		return protocol.StatusResp{Ok: true}
	})
	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "inbound-client")

	if _, err := c.Send("ClientRequest", struct{}{}); err != nil {
		t.Fatal(err)
	}
	if err := c.Notify("ClientNotify", struct{}{}); err != nil {
		t.Fatal(err)
	}
	sess, ok := srv.Get("inbound-client")
	if !ok {
		t.Fatal("client session not found")
	}
	if _, err := sess.Send("ServerRequest", struct{}{}); err != nil {
		t.Fatal(err)
	}

	want := map[string]protocol.MessageType{
		"ClientRequest": protocol.Req,
		"ClientNotify":  protocol.Notify,
		"ServerRequest": protocol.Resp,
	}
	for range want {
		select {
		case message := <-inbound:
			messageType, exists := want[message.Action]
			if !exists {
				t.Fatalf("unexpected inbound action %q", message.Action)
			}
			if message.Type != messageType {
				t.Fatalf("inbound %s type = %v, want %v", message.Action, message.Type, messageType)
			}
			if message.Id == "" {
				t.Fatalf("inbound %s has empty message id", message.Action)
			}
			delete(want, message.Action)
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for inbound messages; missing=%v", want)
		}
	}
	_ = c.Close()
}

func requireRemoteProtocolError(t *testing.T, err error, code string) {
	t.Helper()
	var protocolErr *manageserver.RemoteProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("error = %v, want RemoteProtocolError", err)
	}
	if protocolErr.Code != code {
		t.Fatalf("protocol error code = %q, want %q", protocolErr.Code, code)
	}
}

func TestUnknownActionReturnsErrorAndKeepsServerSessionAlive(t *testing.T) {
	reported := make(chan error, 4)
	srv, addr := startServer(t, manageserver.WithOnError(func(err error) {
		reported <- err
	}))
	srv.On("Ping", func(sess *manageserver.Session, msg *protocol.Message) any {
		return protocol.StatusResp{Ok: true}
	})

	c, err := manageserver.NewClient(manageserver.WithID("unknown-action-server"))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "unknown-action-server")

	_, err = c.Send("NotRegistered", map[string]string{})
	requireRemoteProtocolError(t, err, "unknown_action")

	select {
	case reportedErr := <-reported:
		var dispatchErr *manageserver.DispatchError
		if !errors.As(reportedErr, &dispatchErr) || dispatchErr.Action != "NotRegistered" {
			t.Fatalf("reported error = %#v, want structured DispatchError", reportedErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unknown action was not reported")
	}

	if _, err := c.Send("Ping", map[string]string{}); err != nil {
		t.Fatalf("session did not survive unknown action: %v", err)
	}
}

func TestServerHandlerPanicReturnsErrorAndKeepsSessionAlive(t *testing.T) {
	srv, addr := startServer(t)
	srv.On("Panic", func(sess *manageserver.Session, msg *protocol.Message) any {
		panic("boom")
	})
	srv.On("Ping", func(sess *manageserver.Session, msg *protocol.Message) any {
		return protocol.StatusResp{Ok: true}
	})

	c, err := manageserver.NewClient(manageserver.WithID("panic-server"))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "panic-server")

	_, err = c.Send("Panic", map[string]string{})
	requireRemoteProtocolError(t, err, "handler_panic")
	if _, err := c.Send("Ping", map[string]string{}); err != nil {
		t.Fatalf("session did not survive handler panic: %v", err)
	}
}

func TestUnknownActionReturnsErrorAndKeepsClientConnectionAlive(t *testing.T) {
	srv, addr := startServer(t)
	c, err := manageserver.NewClient(manageserver.WithID("unknown-action-client"))
	if err != nil {
		t.Fatal(err)
	}
	c.On("Ping", func(client *manageserver.Client, msg *protocol.Message) any {
		return protocol.StatusResp{Ok: true}
	})
	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "unknown-action-client")
	sess, _ := srv.Get("unknown-action-client")

	_, err = sess.Send("NotRegistered", map[string]string{})
	requireRemoteProtocolError(t, err, "unknown_action")
	if _, err := sess.Send("Ping", map[string]string{}); err != nil {
		t.Fatalf("client connection did not survive unknown action: %v", err)
	}
}

func TestClientHandlerPanicReturnsErrorAndKeepsConnectionAlive(t *testing.T) {
	srv, addr := startServer(t)
	c, err := manageserver.NewClient(manageserver.WithID("panic-client"))
	if err != nil {
		t.Fatal(err)
	}
	c.On("Panic", func(client *manageserver.Client, msg *protocol.Message) any {
		panic("boom")
	})
	c.On("Ping", func(client *manageserver.Client, msg *protocol.Message) any {
		return protocol.StatusResp{Ok: true}
	})
	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "panic-client")
	sess, _ := srv.Get("panic-client")

	_, err = sess.Send("Panic", map[string]string{})
	requireRemoteProtocolError(t, err, "handler_panic")
	if _, err := sess.Send("Ping", map[string]string{}); err != nil {
		t.Fatalf("client connection did not survive handler panic: %v", err)
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

func TestSendContextStopsWhenCallerCancels(t *testing.T) {
	c, err := manageserver.NewClient(manageserver.WithID("context-cancel-device"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = c.SendContext(ctx, "Never", map[string]string{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("SendContext error = %v, want context canceled", err)
	}
	if got := c.Pending(); got != 0 {
		t.Fatalf("pending calls = %d after canceled SendContext", got)
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

// TestSessionNotifyDeliversWithoutResponse guards the core Notify contract
// from the server side: the client handler must receive it, and Notify must
// not register a pending call (Pending stays 0, no Resp round trip happens).
func TestSessionNotifyDeliversWithoutResponse(t *testing.T) {
	srv, addr := startServer(t)

	received := make(chan string, 1)
	c, err := manageserver.NewClient(manageserver.WithID("device-notify-from-server"))
	if err != nil {
		t.Fatal(err)
	}
	c.On("Chunk", func(cl *manageserver.Client, msg *protocol.Message) any {
		var payload map[string]string
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			t.Error(err)
		}
		received <- payload["data"]
		// Notify handlers may return whatever they like — it must be ignored,
		// not sent back as a Resp and not treated as "nil means disconnect".
		return "this should never be sent anywhere"
	})

	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "device-notify-from-server")

	sess, ok := srv.Get("device-notify-from-server")
	if !ok {
		t.Fatal("expected session to be registered")
	}

	if err := sess.Notify("Chunk", map[string]string{"data": "hello"}); err != nil {
		t.Fatalf("Notify failed: %v", err)
	}

	select {
	case payload := <-received:
		if payload != "hello" {
			t.Fatalf("unexpected payload: %q", payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for client to receive the notify")
	}

	if got := sess.Pending(); got != 0 {
		t.Fatalf("expected Pending()==0 after Notify (it must not register a pending call), got %d", got)
	}
}

// TestClientNotifyDeliversWithoutResponse mirrors
// TestSessionNotifyDeliversWithoutResponse from the client side.
func TestClientNotifyDeliversWithoutResponse(t *testing.T) {
	srv, addr := startServer(t)

	received := make(chan string, 1)
	srv.On("Chunk", func(sess *manageserver.Session, msg *protocol.Message) any {
		var payload map[string]string
		if err := json.Unmarshal(msg.Data, &payload); err != nil {
			t.Error(err)
		}
		received <- payload["data"]
		return "this should never be sent anywhere"
	})

	c, err := manageserver.NewClient(manageserver.WithID("device-notify-from-client"))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "device-notify-from-client")

	if err := c.Notify("Chunk", map[string]string{"data": "hello"}); err != nil {
		t.Fatalf("Notify failed: %v", err)
	}

	select {
	case payload := <-received:
		if payload != "hello" {
			t.Fatalf("unexpected payload: %q", payload)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server to receive the notify")
	}

	if got := c.Pending(); got != 0 {
		t.Fatalf("expected Pending()==0 after Notify (it must not register a pending call), got %d", got)
	}
}

// TestNotifyNilHandlerReturnDoesNotCloseConnection guards the key difference
// from Req dispatch: for a Req, a nil handler return closes the connection.
// A Notify handler returning nil must not — it's a fire-and-forget message,
// not a "hang up" signal.
func TestNotifyNilHandlerReturnDoesNotCloseConnection(t *testing.T) {
	srv, addr := startServer(t)

	notified := make(chan struct{}, 1)
	srv.On("SilentChunk", func(sess *manageserver.Session, msg *protocol.Message) any {
		notified <- struct{}{}
		return nil
	})
	srv.On("Ping", func(sess *manageserver.Session, msg *protocol.Message) any {
		return protocol.StatusResp{Ok: true}
	})

	c, err := manageserver.NewClient(manageserver.WithID("device-notify-nil-return"))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "device-notify-nil-return")

	if err := c.Notify("SilentChunk", map[string]string{}); err != nil {
		t.Fatalf("Notify failed: %v", err)
	}

	select {
	case <-notified:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the notify to be delivered")
	}

	// If a nil Notify return had (wrongly) closed the connection, this
	// subsequent Send would fail instead of round-tripping normally.
	if _, err := c.Send("Ping", map[string]string{}); err != nil {
		t.Fatalf("expected connection to still be alive after a nil-returning Notify handler, got: %v", err)
	}
}

// waitForSessionGone blocks until id is no longer in the server's session
// registry, or fails the test.
func waitForSessionGone(t *testing.T, s *manageserver.Server, id string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := s.Get(id); !ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("session %q was never removed", id)
}

func TestReadLimitClosesOversizedConnection(t *testing.T) {
	errCh := make(chan error, 1)
	srv, addr := startServer(t,
		manageserver.WithReadLimit(64),
		manageserver.WithOnError(func(err error) {
			select {
			case errCh <- err:
			default:
			}
		}),
	)
	srv.On("Echo", func(sess *manageserver.Session, msg *protocol.Message) any {
		return protocol.StatusResp{Ok: true}
	})

	c, err := manageserver.NewClient(manageserver.WithID("big-sender"))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "big-sender")

	// Well over the 64-byte server-side read limit once JSON-encoded.
	oversized := make([]byte, 1024)
	if err := c.Notify("Echo", map[string]string{"data": string(oversized)}); err != nil {
		t.Fatalf("Notify failed: %v", err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected a non-nil read-limit error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server to report the oversized-frame error")
	}

	waitForSessionGone(t, srv, "big-sender")
}

func TestReadTimeoutClosesIdleConnection(t *testing.T) {
	srv, addr := startServer(t,
		manageserver.WithReadTimeout(200*time.Millisecond),
		// Long enough that the ping ticker itself doesn't fire (and so
		// refresh the read deadline) during this test.
		manageserver.WithPingInterval(10*time.Minute),
	)

	c, err := manageserver.NewClient(manageserver.WithID("idle-client"))
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "idle-client")

	waitForSessionGone(t, srv, "idle-client")
}

// TestPingPongHooksFire also guards against a regression of the client-side
// ping handler deadlocking: it used to send on an unbuffered channel with no
// escape hatch, so if that ever regresses this test would hang and fail on
// timeout rather than observing a second ping.
func TestPingPongHooksFire(t *testing.T) {
	pongCh := make(chan struct{}, 1)
	srv, addr := startServer(t,
		manageserver.WithPingInterval(50*time.Millisecond),
		manageserver.WithOnPong(func(sess *manageserver.Session) {
			select {
			case pongCh <- struct{}{}:
			default:
			}
		}),
	)

	pingCh := make(chan struct{}, 1)
	c, err := manageserver.NewClient(
		manageserver.WithID("ping-pong-client"),
		manageserver.WithPingHandler(func(cl *manageserver.Client) {
			select {
			case pingCh <- struct{}{}:
			default:
			}
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = c.Start(addr, "/ws/") }()
	waitForSession(t, srv, "ping-pong-client")

	select {
	case <-pingCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the client ping handler to fire")
	}
	select {
	case <-pongCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the server pong handler to fire")
	}

	// A second round trip proves the first wasn't a fluke and the
	// connection is still healthy — in particular, that the client's ping
	// handler isn't blocked on its internal channel send.
	select {
	case <-pingCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a second ping — connection may have stalled")
	}
}
