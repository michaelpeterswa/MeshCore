package proxy

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/agessaman/MeshCore/tools/mqtt-proxy/config"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/metrics"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/presets"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/tokens"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/upstream"
)

const dev = "6C265917E1D4BA04FB14A9C2C985B5143FD74C9DA89472C2D731A9A1413A9FAD"
const aud = "mqtt-us-v1.letsmesh.net"
const base = "meshcore/SEA/" + dev + "/"

type fakeSession struct {
	mu   sync.Mutex
	msgs []upstream.Message
	done chan struct{}
}

func (f *fakeSession) Enqueue(m upstream.Message) bool {
	f.mu.Lock()
	f.msgs = append(f.msgs, m)
	f.mu.Unlock()
	return true
}
func (f *fakeSession) Run(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-f.done:
	}
}
func (f *fakeSession) Done() <-chan struct{} { return f.done }
func (f *fakeSession) topics() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, m := range f.msgs {
		out = append(out, m.Topic)
	}
	return out
}

type harness struct {
	r        *Router
	m        *metrics.Metrics
	cache    *tokens.Cache
	logs     *bytes.Buffer
	mu       sync.Mutex
	sessions map[string][]*fakeSession // by upstream name
}

func newHarness(t *testing.T, ups ...config.Upstream) *harness {
	h := &harness{m: metrics.New(), cache: tokens.New(time.Now), logs: &bytes.Buffer{}, sessions: map[string][]*fakeSession{}}
	cfg := &config.Config{Upstreams: ups, Session: config.Session{IdleTimeout: time.Minute, RenewBefore: time.Minute, QueueSize: 8}}
	h.r = NewRouter(cfg, h.cache, h.m, slog.New(slog.NewTextHandler(&syncWriter{w: h.logs}, nil)))
	h.r.newSession = func(u config.Upstream, device string) sessionRunner {
		f := &fakeSession{done: make(chan struct{})}
		h.mu.Lock()
		h.sessions[u.Name] = append(h.sessions[u.Name], f)
		h.mu.Unlock()
		return f
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { h.r.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return h
}

type syncWriter struct {
	mu sync.Mutex
	w  *bytes.Buffer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func (h *harness) only(t *testing.T, upstream string) *fakeSession {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.sessions[upstream]) != 1 {
		t.Fatalf("%s: %d sessions", upstream, len(h.sessions[upstream]))
	}
	return h.sessions[upstream][0]
}

func jwtUp() config.Upstream {
	p, _ := presets.Lookup("analyzer-us")
	return config.Upstream{Name: "letsmesh-us", Preset: p}
}

func passUp() config.Upstream {
	return config.Upstream{Name: "private", Preset: presets.Preset{Name: "custom", URL: "mqtts://x:8883", Auth: presets.AuthUserPass, Username: "u", Password: "p"}}
}

func (h *harness) auth(t *testing.T, retained bool) {
	now := time.Now()
	p := fmt.Sprintf(`{"audience":%q,"username":%q,"token":"jwt","iat":%d,"exp":%d}`, aud, dev, now.Unix(), now.Add(24*time.Hour).Unix())
	h.r.Handle(base+"auth/"+aud, []byte(p), retained)
}

func TestAuthPopulatesCacheEvenWhenRetained(t *testing.T) {
	h := newHarness(t, jwtUp())
	h.auth(t, true)
	if _, ok := h.cache.Get(dev, aud); !ok {
		t.Fatal("retained auth not cached")
	}
	if len(h.sessions) != 0 {
		t.Fatal("auth opened a session")
	}
}

func TestForwardsStatusAndPackets(t *testing.T) {
	h := newHarness(t, jwtUp())
	h.auth(t, true)
	h.r.Handle(base+"status", []byte(`{"status":"online"}`), false)
	h.r.Handle(base+"packets", []byte(`{"type":"PACKET"}`), false)
	s := h.only(t, "letsmesh-us")
	got := s.topics()
	if len(got) != 2 || got[0] != base+"status" || got[1] != base+"packets" {
		t.Fatalf("%v", got)
	}
	if s.msgs[0].QoS != 1 || !s.msgs[0].Retain || s.msgs[1].QoS != 0 {
		t.Fatalf("%+v", s.msgs)
	}
	if v := testutil.ToFloat64(h.m.MessagesIn.WithLabelValues(dev)); v != 2 {
		t.Fatalf("messages_in %v", v)
	}
}

func TestRetainedStatusIsNotForwarded(t *testing.T) {
	h := newHarness(t, passUp())
	h.r.Handle(base+"status", []byte(`{"status":"online"}`), true)
	h.r.Handle(base+"neighbors", []byte(`{}`), true)
	if len(h.sessions) != 0 {
		t.Fatal("retained replay forwarded")
	}
}

func TestRoomAndLinkNeverReachASession(t *testing.T) {
	h := newHarness(t, jwtUp(), passUp())
	h.auth(t, false)
	for _, s := range []string{"room/posts", "room/events", "room/dm", "room/stats", "link", "auth/other.example", "unknown"} {
		h.r.Handle(base+s, []byte(`{"text":"secret"}`), false)
	}
	if len(h.sessions) != 0 {
		t.Fatalf("sessions opened: %v", h.sessions)
	}
}

func TestNonJSONPacketDroppedAndCounted(t *testing.T) {
	h := newHarness(t, passUp())
	h.r.Handle(base+"packets", []byte(`not json`), false)
	h.r.Handle(base+"packets", []byte(`["array"]`), false)
	h.r.Handle("meshcore/SEA/short/packets", []byte(`{}`), false)
	if v := testutil.ToFloat64(h.m.Malformed); v != 3 {
		t.Fatalf("malformed %v", v)
	}
	h.r.Handle(base+"packets", []byte(` {"ok":1}`), false)
	if got := h.only(t, "private").topics(); len(got) != 1 {
		t.Fatalf("%v", got)
	}
}

func TestNoTokenSkipsJWTUpstreamLoggedOnce(t *testing.T) {
	h := newHarness(t, jwtUp(), passUp())
	h.r.Handle(base+"packets", []byte(`{}`), false)
	h.r.Handle(base+"packets", []byte(`{}`), false)
	if n := strings.Count(h.logs.String(), "no token"); n != 1 {
		t.Fatalf("logged %d times:\n%s", n, h.logs.String())
	}
	if len(h.sessions["letsmesh-us"]) != 0 {
		t.Fatal("JWT session opened without a token")
	}
	if got := h.only(t, "private").topics(); len(got) != 2 {
		t.Fatalf("userpass upstream got %v", got)
	}
	if v := testutil.ToFloat64(h.m.Dropped.WithLabelValues("letsmesh-us", "no_token")); v != 2 {
		t.Fatalf("no_token drops %v", v)
	}
	// Once a token arrives the JWT upstream gets traffic, and a later loss logs again.
	h.auth(t, false)
	h.r.Handle(base+"packets", []byte(`{}`), false)
	if got := h.only(t, "letsmesh-us").topics(); len(got) != 1 {
		t.Fatalf("%v", got)
	}
}

func TestExpiredAuthLoggedOncePerDevice(t *testing.T) {
	h := newHarness(t, jwtUp())
	p := fmt.Sprintf(`{"audience":%q,"username":%q,"token":"jwt","iat":1715850000,"exp":1715936400}`, aud, dev)
	h.r.Handle(base+"auth/"+aud, []byte(p), true)
	h.r.Handle(base+"auth/"+aud, []byte(p), true)
	if n := strings.Count(h.logs.String(), "expired"); n != 1 {
		t.Fatalf("logged %d times:\n%s", n, h.logs.String())
	}
}

func TestOneSessionPerDevicePerUpstream(t *testing.T) {
	h := newHarness(t, passUp())
	other := strings.Repeat("AB", 32)
	for i := 0; i < 3; i++ {
		h.r.Handle(base+"packets", []byte(`{}`), false)
		h.r.Handle("meshcore/SEA/"+other+"/packets", []byte(`{}`), false)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if n := len(h.sessions["private"]); n != 2 {
		t.Fatalf("%d sessions", n)
	}
}

func TestFinishedSessionIsReplacedOnNextMessage(t *testing.T) {
	h := newHarness(t, passUp())
	h.r.Handle(base+"packets", []byte(`{}`), false)
	first := h.only(t, "private")
	close(first.done)
	deadline := time.Now().Add(2 * time.Second)
	for {
		h.r.Handle(base+"packets", []byte(`{}`), false)
		h.mu.Lock()
		n := len(h.sessions["private"])
		h.mu.Unlock()
		if n == 2 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("finished session not replaced")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

type closedOnce struct {
	fakeSession
	refused bool
}

func (c *closedOnce) Enqueue(m upstream.Message) bool {
	if !c.refused {
		c.refused = true
		return false
	}
	return c.fakeSession.Enqueue(m)
}

func TestRefusedEnqueueGoesToAFreshSession(t *testing.T) {
	// A session that has just decided to exit refuses the message; the router
	// must hand it to a new session instead of losing it.
	h := newHarness(t, passUp())
	var made []*closedOnce
	h.r.newSession = func(u config.Upstream, device string) sessionRunner {
		c := &closedOnce{fakeSession: fakeSession{done: make(chan struct{})}}
		if len(made) > 0 {
			c.refused = true // only the first session refuses
		}
		made = append(made, c)
		return c
	}
	h.r.Handle(base+"packets", []byte(`{}`), false)
	if len(made) != 2 || len(made[1].topics()) != 1 {
		t.Fatalf("sessions %d", len(made))
	}
}
