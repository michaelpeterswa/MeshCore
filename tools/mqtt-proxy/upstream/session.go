package upstream

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/eclipse/paho.mqtt.golang/packets"

	"github.com/agessaman/MeshCore/tools/mqtt-proxy/config"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/tokens"
)

// Credentials returns the login to use for the next connection attempt.
type Credentials func() (username, password string, ok bool)

// JWTCredentials logs in as the device with its newest cached token.
func JWTCredentials(c *tokens.Cache, device, audience string) Credentials {
	return func() (string, string, bool) {
		tok, ok := c.Get(device, audience)
		return tok.Username, tok.JWT, ok
	}
}

// StaticCredentials is a fixed login (or none, with empty strings).
func StaticCredentials(username, password string) Credentials {
	return func() (string, string, bool) { return username, password, true }
}

// Hooks are metrics callbacks; any may be nil.
type Hooks struct {
	Connected func(up bool)
	Published func()
	Dropped   func(reason string)
	Reconnect func()
}

type SessionConfig struct {
	Upstream     config.Upstream
	Device       string
	Creds        Credentials
	TokenChanged func() <-chan struct{} // nil for non-JWT upstreams
	TokenExpiry  func() time.Time       // nil for non-JWT upstreams
	Session      config.Session
	Logger       *slog.Logger
	Hooks        Hooks

	backoff *Backoff // tests shorten it
}

const (
	// A connection must stay up this long, or deliver a message, before the
	// reconnect backoff resets. Without it a broker that accepts CONNECT and
	// then drops the client gets reconnected to as fast as TLS allows.
	stableAfter = 30 * time.Second
	// A message whose publish fails this many times is dropped, so one the
	// broker always disconnects on cannot wedge the queue.
	maxPublishAttempts = 3
)

// Session is one device's connection to one upstream. It connects lazily,
// forwards its queue, and ends after Session.IdleTimeout without traffic.
type Session struct {
	cfg      SessionConfig
	q        *Queue
	done     chan struct{}
	lastSeen atomic.Int64 // unix nanos of the last Enqueue
	log      *slog.Logger
	backoff  *Backoff

	// mu makes "decide to exit" and Enqueue mutually exclusive: once closed
	// is set, Enqueue refuses, and the router starts a fresh session instead
	// of handing a message to one that will never send it.
	mu        sync.Mutex
	closed    bool
	connected atomic.Bool
}

func NewSession(cfg SessionConfig) *Session {
	s := &Session{cfg: cfg, q: NewQueue(cfg.Session.QueueSize), done: make(chan struct{})}
	s.log = cfg.Logger
	if s.log == nil {
		s.log = slog.Default()
	}
	s.log = s.log.With("upstream", cfg.Upstream.Name, "device", cfg.Device)
	s.backoff = cfg.backoff
	if s.backoff == nil {
		s.backoff = &Backoff{Min: time.Second, Max: 5 * time.Minute, Jitter: 0.2}
	}
	s.lastSeen.Store(time.Now().UnixNano())
	return s
}

// Enqueue queues m, or returns false if the session has finished and m
// must go to a new one.
func (s *Session) Enqueue(m Message) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.lastSeen.Store(time.Now().UnixNano())
	if s.q.Push(m) && s.cfg.Hooks.Dropped != nil {
		s.cfg.Hooks.Dropped("queue_full")
	}
	return true
}

func (s *Session) Done() <-chan struct{} { return s.done }

// tryClose ends the session if it has been idle: no Enqueue for
// IdleTimeout and, while connected, nothing left to send. A session that is
// not connected (waiting for a token, backing off) goes idle on time alone:
// its queue is undeliverable and would otherwise keep it alive forever.
func (s *Session) tryClose() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return true
	}
	if time.Since(time.Unix(0, s.lastSeen.Load())) < s.cfg.Session.IdleTimeout {
		return false
	}
	if s.connected.Load() && s.q.Len() > 0 {
		return false
	}
	if n := s.q.Len(); n > 0 {
		s.log.Info("dropping queued messages from an idle, disconnected session", "count", n)
		if s.cfg.Hooks.Dropped != nil {
			for i := 0; i < n; i++ {
				s.cfg.Hooks.Dropped("idle_undelivered")
			}
		}
	}
	s.closed = true
	return true
}

func (s *Session) markClosed() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
}

// poll bounds how long any wait lasts before re-checking idleness.
func (s *Session) poll() time.Duration {
	p := s.cfg.Session.IdleTimeout / 4
	if p > time.Second || p <= 0 {
		p = time.Second
	}
	return p
}

var errIdle = errors.New("idle")

// wait sleeps for d or until ch fires, returning errIdle or ctx.Err() when
// the session should end instead.
func (s *Session) wait(ctx context.Context, d time.Duration, ch <-chan struct{}) error {
	var until <-chan time.Time
	if d > 0 {
		t := time.NewTimer(d)
		defer t.Stop()
		until = t.C
	}
	tick := time.NewTicker(s.poll())
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-until:
			return nil
		case <-ch:
			return nil
		case <-tick.C:
			if s.tryClose() {
				return errIdle
			}
		}
	}
}

func (s *Session) Run(ctx context.Context) {
	defer close(s.done)
	defer s.markClosed()
	everConnected := false
	for {
		if ctx.Err() != nil || s.tryClose() {
			return
		}
		var changed <-chan struct{}
		if s.cfg.TokenChanged != nil {
			changed = s.cfg.TokenChanged()
		}
		user, pass, ok := s.cfg.Creds()
		if !ok {
			s.log.Debug("no usable token yet; waiting")
			if s.wait(ctx, 0, changed) != nil {
				return
			}
			continue
		}

		client, lost, err := s.connect(user, pass)
		if err != nil {
			if errors.Is(err, packets.ErrorRefusedBadUsernameOrPassword) || errors.Is(err, packets.ErrorRefusedNotAuthorised) {
				exp := ""
				if s.cfg.TokenExpiry != nil {
					exp = s.cfg.TokenExpiry().UTC().Format(time.RFC3339)
				}
				if changed != nil {
					s.log.Error("upstream rejected the device token; waiting for a new one", "err", err, "token_exp", exp)
					if s.wait(ctx, 0, changed) != nil {
						return
					}
					continue
				}
				// Fixed credentials: the broker may be mid-reload or the
				// password mid-rotation. Retry at the slowest backoff.
				d := s.backoff.Max
				s.log.Error("upstream rejected credentials; retrying", "err", err, "retry_in", d)
				if s.wait(ctx, d, nil) != nil {
					return
				}
				continue
			}
			d := s.backoff.Next()
			s.log.Warn("upstream connect failed", "err", err, "retry_in", d)
			if s.wait(ctx, d, nil) != nil {
				return
			}
			continue
		}
		if everConnected && s.cfg.Hooks.Reconnect != nil {
			s.cfg.Hooks.Reconnect()
		}
		everConnected = true
		s.setConnected(true)
		s.log.Info("upstream connected")
		var exp time.Time
		if s.cfg.TokenExpiry != nil {
			exp = s.cfg.TokenExpiry()
		}
		upAt := time.Now()
		reason, delivered := s.serve(ctx, client, lost, pass, exp)
		client.Disconnect(250)
		s.setConnected(false)
		s.log.Info("upstream session closed", "reason", reason)
		switch reason {
		case "idle", "stopped":
			return
		case "renewing token":
			continue
		}
		if delivered || time.Since(upAt) >= stableAfter {
			s.backoff.Reset()
		}
		d := s.backoff.Next()
		if s.wait(ctx, d, nil) != nil {
			return
		}
	}
}

func (s *Session) setConnected(up bool) {
	s.connected.Store(up)
	if s.cfg.Hooks.Connected != nil {
		s.cfg.Hooks.Connected(up)
	}
}

// serve publishes from the queue until the connection drops, the token needs
// renewing, the session goes idle or ctx ends. delivered reports whether any
// publish succeeded on this connection.
func (s *Session) serve(ctx context.Context, client paho.Client, lost <-chan struct{}, usedPass string, usedExp time.Time) (reason string, delivered bool) {
	for {
		if ctx.Err() != nil {
			return "stopped", delivered
		}
		if s.tryClose() {
			return "idle", delivered
		}
		select {
		case <-lost:
			return "connection lost", delivered
		default:
		}
		if !usedExp.IsZero() && time.Until(usedExp) < s.cfg.Session.RenewBefore {
			if _, pass, ok := s.cfg.Creds(); ok && pass != usedPass {
				return "renewing token", delivered
			}
		}
		pctx, cancel := context.WithTimeout(ctx, s.poll())
		go func() {
			select {
			case <-lost:
				cancel()
			case <-pctx.Done():
			}
		}()
		m, err := s.q.Pop(pctx)
		cancel()
		if err != nil {
			continue
		}
		tok := client.Publish(m.Topic, m.QoS, m.Retain, m.Payload)
		// A publish on a connection the broker just dropped never completes;
		// fail it as soon as paho reports the loss rather than at the timeout.
		ok := false
		timeout := time.NewTimer(10 * time.Second)
		select {
		case <-tok.Done():
			ok = tok.Error() == nil
		case <-lost:
		case <-timeout.C:
		}
		timeout.Stop()
		if !ok {
			m.attempts++
			if m.attempts >= maxPublishAttempts {
				s.log.Warn("dropping message after repeated publish failures", "topic", m.Topic, "attempts", m.attempts)
				if s.cfg.Hooks.Dropped != nil {
					s.cfg.Hooks.Dropped("publish_failed")
				}
			} else {
				s.q.Unpop(m)
			}
			return fmt.Sprintf("publish failed: %v", tok.Error()), delivered
		}
		delivered = true
		if s.cfg.Hooks.Published != nil {
			s.cfg.Hooks.Published()
		}
	}
}

// connect dials the upstream. lost is closed when paho reports the
// connection gone.
func (s *Session) connect(user, pass string) (paho.Client, <-chan struct{}, error) {
	broker, err := brokerURL(s.cfg.Upstream.Preset.URL)
	if err != nil {
		return nil, nil, err
	}
	lost := make(chan struct{})
	var lostOnce sync.Once
	opts := paho.NewClientOptions().
		AddBroker(broker).
		SetClientID("mcproxy-" + s.cfg.Device[:16]).
		SetCleanSession(true).
		// Pinned: left at 0, paho retries a refused CONNECT as MQTT 3.1, which
		// doubles every rejected login. The firmware observer speaks 3.1.1.
		SetProtocolVersion(4).
		SetAutoReconnect(false).
		SetConnectRetry(false).
		SetConnectTimeout(10 * time.Second).
		SetWriteTimeout(10 * time.Second).
		SetOrderMatters(true)
	if user != "" {
		opts.SetUsername(user)
	}
	if pass != "" {
		opts.SetPassword(pass)
	}
	if ka := s.cfg.Upstream.Preset.KeepAlive; ka > 0 {
		opts.SetKeepAlive(ka)
	}
	opts.SetConnectionLostHandler(func(paho.Client, error) { lostOnce.Do(func() { close(lost) }) })
	tlsCfg, err := tlsConfig(s.cfg.Upstream.CAFile)
	if err != nil {
		return nil, nil, err
	}
	opts.SetTLSConfig(tlsCfg)
	client := paho.NewClient(opts)
	tok := client.Connect()
	if !tok.WaitTimeout(15 * time.Second) {
		client.Disconnect(0)
		return nil, nil, errors.New("connect timed out")
	}
	if err := tok.Error(); err != nil {
		return nil, nil, err
	}
	return client, lost, nil
}

// brokerURL maps preset schemes onto paho's.
func brokerURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "mqtt":
		u.Scheme = "tcp"
	case "mqtts":
		u.Scheme = "ssl"
	case "ws", "wss":
	default:
		return "", fmt.Errorf("unsupported scheme %q", u.Scheme)
	}
	return u.String(), nil
}

// tlsConfig trusts the system roots plus caFile, if given.
func tlsConfig(caFile string) (*tls.Config, error) {
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if caFile != "" {
		pem, err := os.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("%s: no certificates", caFile)
		}
	}
	return &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}, nil
}
