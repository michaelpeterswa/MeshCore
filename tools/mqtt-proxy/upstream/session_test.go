package upstream

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agessaman/MeshCore/tools/mqtt-proxy/config"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/internal/testbroker"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/presets"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/tokens"
)

const dev = "6C265917E1D4BA04FB14A9C2C985B5143FD74C9DA89472C2D731A9A1413A9FAD"
const aud = "mqtt-us-v1.letsmesh.net"

func putToken(t *testing.T, c *tokens.Cache, jwt string, iat, exp time.Time) {
	t.Helper()
	p := fmt.Sprintf(`{"audience":%q,"username":%q,"token":%q,"iat":%d,"exp":%d}`, aud, dev, jwt, iat.Unix(), exp.Unix())
	if _, err := c.Put(dev, aud, []byte(p)); err != nil {
		t.Fatal(err)
	}
}

func jwtSession(b *testbroker.Broker, c *tokens.Cache, s config.Session, hooks Hooks) *Session {
	p, _ := presets.Lookup("analyzer-us")
	p.URL = b.URL
	return NewSession(SessionConfig{
		Upstream:     config.Upstream{Name: "fake-us", Preset: p, CAFile: b.CAFile},
		Device:       dev,
		Creds:        JWTCredentials(c, dev, aud),
		TokenChanged: func() <-chan struct{} { return c.Changed(dev, aud) },
		TokenExpiry: func() time.Time {
			tok, _ := c.Get(dev, aud)
			return tok.ExpiresAt
		},
		Session: s,
		Logger:  slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: testLogLevel})),
		Hooks:   hooks,
		backoff: &Backoff{Min: 50 * time.Millisecond, Max: 200 * time.Millisecond, Jitter: 0},
	})
}

var testLogLevel = slog.LevelWarn

var defSession = config.Session{IdleTimeout: time.Minute, RenewBefore: 10 * time.Minute, QueueSize: 256}

func run(t *testing.T, s *Session) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	t.Cleanup(func() { cancel(); <-s.Done() })
	return cancel
}

func TestPublishesWithJWTCredentials(t *testing.T) {
	b := testbroker.Start(t, testbroker.Options{Kind: testbroker.WSS})
	c := tokens.New(time.Now)
	putToken(t, c, "jwt-a", time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour))
	s := jwtSession(b, c, defSession, Hooks{})
	run(t, s)
	s.Enqueue(Message{Topic: "meshcore/SEA/" + dev + "/status", Payload: []byte(`{"status":"online"}`), QoS: 1, Retain: true})
	b.WaitFor(t, 5*time.Second, "status", func(r []testbroker.Received) bool { return len(r) == 1 })
	got := b.Received()[0]
	if got.Username != "v1_"+dev || got.QoS != 1 || !got.Retain || got.Topic != "meshcore/SEA/"+dev+"/status" {
		t.Fatalf("%+v", got)
	}
	if l := b.Logins(); len(l) != 1 || l[0] != "v1_"+dev+":jwt-a" {
		t.Fatalf("logins %v", l)
	}
}

func TestNoTokenWaitsForOne(t *testing.T) {
	b := testbroker.Start(t, testbroker.Options{Kind: testbroker.WSS})
	c := tokens.New(time.Now)
	s := jwtSession(b, c, defSession, Hooks{})
	run(t, s)
	s.Enqueue(Message{Topic: "meshcore/SEA/" + dev + "/packets", Payload: []byte(`{}`)})
	time.Sleep(300 * time.Millisecond)
	if len(b.Logins()) != 0 {
		t.Fatal("connected without a token")
	}
	putToken(t, c, "jwt-a", time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour))
	b.WaitFor(t, 5*time.Second, "queued packet", func(r []testbroker.Received) bool { return len(r) == 1 })
}

func TestRejectedTokenWaitsForNewToken(t *testing.T) {
	b := testbroker.Start(t, testbroker.Options{Kind: testbroker.WSS, Auth: func(u, p string) bool { return p == "jwt-b" }})
	c := tokens.New(time.Now)
	putToken(t, c, "jwt-a", time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour))
	s := jwtSession(b, c, defSession, Hooks{})
	run(t, s)
	s.Enqueue(Message{Topic: "meshcore/SEA/" + dev + "/packets", Payload: []byte(`{}`)})
	time.Sleep(600 * time.Millisecond) // 3x the max backoff
	if n := len(b.Logins()); n != 1 {
		t.Fatalf("retried a rejected token: %d logins", n)
	}
	putToken(t, c, "jwt-b", time.Now(), time.Now().Add(24*time.Hour))
	b.WaitFor(t, time.Second, "publish after new token", func(r []testbroker.Received) bool { return len(r) == 1 })
}

func TestReconnectsWithNewestTokenAfterOutage(t *testing.T) {
	b := testbroker.Start(t, testbroker.Options{Kind: testbroker.WSS})
	c := tokens.New(time.Now)
	putToken(t, c, "jwt-a", time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour))
	s := jwtSession(b, c, defSession, Hooks{})
	run(t, s)
	s.Enqueue(Message{Topic: "meshcore/SEA/" + dev + "/packets", Payload: []byte(`{"n":1}`)})
	b.WaitFor(t, 5*time.Second, "first packet", func(r []testbroker.Received) bool { return len(r) == 1 })

	b.Close()
	putToken(t, c, "jwt-b", time.Now(), time.Now().Add(24*time.Hour))
	putToken(t, c, "jwt-c", time.Now().Add(time.Second), time.Now().Add(25*time.Hour))
	s.Enqueue(Message{Topic: "meshcore/SEA/" + dev + "/packets", Payload: []byte(`{"n":2}`)})
	b2 := testbroker.Start(t, testbroker.Options{Kind: testbroker.WSS, Addr: b.Addr, CA: b.CA})
	b2.WaitFor(t, 5*time.Second, "packet after outage", func(r []testbroker.Received) bool { return len(r) == 1 })
	l := b2.Logins()
	if !strings.HasSuffix(l[len(l)-1], ":jwt-c") {
		t.Fatalf("logins after outage %v", l)
	}
}

func TestQueueBoundedWhileDown(t *testing.T) {
	b := testbroker.Start(t, testbroker.Options{Kind: testbroker.WSS})
	b.Close()
	c := tokens.New(time.Now)
	putToken(t, c, "jwt-a", time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour))
	var drops atomic.Int64
	s := jwtSession(b, c, defSession, Hooks{Dropped: func(reason string) {
		if reason == "queue_full" {
			drops.Add(1)
		}
	}})
	run(t, s)
	for i := 0; i < 1000; i++ {
		s.Enqueue(Message{Topic: "meshcore/SEA/" + dev + "/packets", Payload: []byte(`{}`)})
	}
	if d := drops.Load(); d != 744 {
		t.Fatalf("drops %d", d)
	}
}

func TestRenewsBeforeExpiry(t *testing.T) {
	b := testbroker.Start(t, testbroker.Options{Kind: testbroker.WSS})
	c := tokens.New(time.Now)
	putToken(t, c, "jwt-a", time.Now().Add(-time.Hour), time.Now().Add(5*time.Minute)) // inside RenewBefore
	s := jwtSession(b, c, defSession, Hooks{})
	run(t, s)
	s.Enqueue(Message{Topic: "meshcore/SEA/" + dev + "/packets", Payload: []byte(`{}`)})
	b.WaitFor(t, 5*time.Second, "first packet", func(r []testbroker.Received) bool { return len(r) == 1 })
	putToken(t, c, "jwt-b", time.Now(), time.Now().Add(24*time.Hour))
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		l := b.Logins()
		if len(l) == 2 && strings.HasSuffix(l[1], ":jwt-b") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("did not renew: %v", b.Logins())
}

func TestIdleTimeoutEndsRun(t *testing.T) {
	b := testbroker.Start(t, testbroker.Options{Kind: testbroker.WSS})
	c := tokens.New(time.Now)
	putToken(t, c, "jwt-a", time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour))
	var mu sync.Mutex
	var states []bool
	s := jwtSession(b, c, config.Session{IdleTimeout: 200 * time.Millisecond, RenewBefore: time.Minute, QueueSize: 8},
		Hooks{Connected: func(up bool) { mu.Lock(); states = append(states, up); mu.Unlock() }})
	run(t, s)
	s.Enqueue(Message{Topic: "meshcore/SEA/" + dev + "/packets", Payload: []byte(`{}`)})
	select {
	case <-s.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("session did not end when idle")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(states) < 2 || !states[0] || states[len(states)-1] {
		t.Fatalf("connected states %v", states)
	}
}

func TestUserPassUpstreamOverTLS(t *testing.T) {
	b := testbroker.Start(t, testbroker.Options{Kind: testbroker.TLS, Auth: func(u, p string) bool { return u == "u" && p == "p" }})
	s := NewSession(SessionConfig{
		Upstream: config.Upstream{Name: "private", CAFile: b.CAFile,
			Preset: presets.Preset{Name: "custom", URL: b.URL, Auth: presets.AuthUserPass, Username: "u", Password: "p"}},
		Device:  dev,
		Creds:   StaticCredentials("u", "p"),
		Session: defSession,
		Logger:  slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	run(t, s)
	s.Enqueue(Message{Topic: "meshcore/SEA/" + dev + "/status", Payload: []byte(`{}`), QoS: 1})
	b.WaitFor(t, 5*time.Second, "status over TLS", func(r []testbroker.Received) bool { return len(r) == 1 })
}

func TestUntrustedCertificateRefused(t *testing.T) {
	b := testbroker.Start(t, testbroker.Options{Kind: testbroker.WSS})
	c := tokens.New(time.Now)
	putToken(t, c, "jwt-a", time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour))
	s := jwtSession(b, c, defSession, Hooks{})
	s.cfg.Upstream.CAFile = "" // system roots only: the test CA is not trusted
	run(t, s)
	s.Enqueue(Message{Topic: "meshcore/SEA/" + dev + "/packets", Payload: []byte(`{}`)})
	time.Sleep(500 * time.Millisecond)
	if len(b.Logins()) != 0 || len(b.Received()) != 0 {
		t.Fatal("connected to a broker with an untrusted certificate")
	}
}

func TestConnectThenDropIsRateLimited(t *testing.T) {
	// A broker that accepts CONNECT then drops the client (post-CONNACK auth,
	// duplicate client ID) must not cause a reconnect storm.
	b := testbroker.Start(t, testbroker.Options{Kind: testbroker.WSS, KickAfterConnect: true})
	c := tokens.New(time.Now)
	putToken(t, c, "jwt-a", time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour))
	s := jwtSession(b, c, defSession, Hooks{})
	run(t, s)
	// A live gateway keeps sending, so there is always something to publish.
	stop := time.Now().Add(1500 * time.Millisecond)
	for time.Now().Before(stop) {
		s.Enqueue(Message{Topic: "meshcore/SEA/" + dev + "/packets", Payload: []byte(`{}`)})
		time.Sleep(5 * time.Millisecond)
	}
	// Backoff 50ms doubling to 200ms allows ~9 attempts in 1.5s; a storm is hundreds.
	if n := len(b.Logins()); n < 2 || n > 12 {
		t.Fatalf("%d connects in 1.5s", n)
	}
}

func TestPoisonMessageDroppedAfterRetries(t *testing.T) {
	poison := "meshcore/SEA/" + dev + "/packets"
	b := testbroker.Start(t, testbroker.Options{Kind: testbroker.WSS, KickOnPublish: poison})
	c := tokens.New(time.Now)
	putToken(t, c, "jwt-a", time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour))
	var dropped atomic.Int64
	s := jwtSession(b, c, defSession, Hooks{Dropped: func(reason string) {
		if reason == "publish_failed" {
			dropped.Add(1)
		}
	}})
	run(t, s)
	s.Enqueue(Message{Topic: poison, Payload: []byte(`{}`), QoS: 1})
	s.Enqueue(Message{Topic: "meshcore/SEA/" + dev + "/status", Payload: []byte(`{}`), QoS: 1})
	b.WaitFor(t, 10*time.Second, "status after the poison message", func(r []testbroker.Received) bool {
		for _, m := range r {
			if strings.HasSuffix(m.Topic, "/status") {
				return true
			}
		}
		return false
	})
	if d := dropped.Load(); d != 1 {
		t.Fatalf("publish_failed drops %d", d)
	}
}

func TestEnqueueAfterIdleExitIsRefused(t *testing.T) {
	b := testbroker.Start(t, testbroker.Options{Kind: testbroker.WSS})
	c := tokens.New(time.Now)
	putToken(t, c, "jwt-a", time.Now().Add(-time.Minute), time.Now().Add(24*time.Hour))
	s := jwtSession(b, c, config.Session{IdleTimeout: 100 * time.Millisecond, RenewBefore: time.Minute, QueueSize: 8}, Hooks{})
	run(t, s)
	if !s.Enqueue(Message{Topic: "meshcore/SEA/" + dev + "/packets", Payload: []byte(`{}`)}) {
		t.Fatal("live session refused a message")
	}
	<-s.Done()
	if s.Enqueue(Message{Topic: "meshcore/SEA/" + dev + "/packets", Payload: []byte(`{}`)}) {
		t.Fatal("finished session accepted a message it will never send")
	}
}

func TestIdleWhileWaitingForTokenEndsRun(t *testing.T) {
	// An offline device's session must not linger forever on a queue it cannot send.
	b := testbroker.Start(t, testbroker.Options{Kind: testbroker.WSS})
	c := tokens.New(time.Now)
	s := jwtSession(b, c, config.Session{IdleTimeout: 200 * time.Millisecond, RenewBefore: time.Minute, QueueSize: 8}, Hooks{})
	run(t, s)
	s.Enqueue(Message{Topic: "meshcore/SEA/" + dev + "/packets", Payload: []byte(`{}`)})
	select {
	case <-s.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("session waiting for a token never went idle")
	}
}

func TestUserPassRefusedOnceRecovers(t *testing.T) {
	var attempts atomic.Int64
	b := testbroker.Start(t, testbroker.Options{Kind: testbroker.TLS, Auth: func(u, p string) bool {
		return attempts.Add(1) > 1 // refuse the first login (auth backend reload, password rotation)
	}})
	s := NewSession(SessionConfig{
		Upstream: config.Upstream{Name: "private", CAFile: b.CAFile,
			Preset: presets.Preset{Name: "custom", URL: b.URL, Auth: presets.AuthUserPass, Username: "u", Password: "p"}},
		Device:  dev,
		Creds:   StaticCredentials("u", "p"),
		Session: defSession,
		Logger:  slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
		backoff: &Backoff{Min: 50 * time.Millisecond, Max: 200 * time.Millisecond},
	})
	run(t, s)
	s.Enqueue(Message{Topic: "meshcore/SEA/" + dev + "/status", Payload: []byte(`{}`), QoS: 1})
	b.WaitFor(t, 5*time.Second, "status after a refused login", func(r []testbroker.Received) bool { return len(r) == 1 })
}
