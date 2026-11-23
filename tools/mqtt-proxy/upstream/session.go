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

// Session is one device's connection to one upstream. It connects lazily,
// forwards its queue, and ends after Session.IdleTimeout without traffic.
type Session struct {
	cfg      SessionConfig
	q        *Queue
	done     chan struct{}
	lastSeen atomic.Int64 // unix nanos of the last Enqueue
	log      *slog.Logger
	backoff  *Backoff
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

func (s *Session) Enqueue(m Message) {
	s.lastSeen.Store(time.Now().UnixNano())
	if s.q.Push(m) && s.cfg.Hooks.Dropped != nil {
		s.cfg.Hooks.Dropped("queue_full")
	}
}

func (s *Session) Done() <-chan struct{} { return s.done }

func (s *Session) idle() bool {
	return s.q.Len() == 0 && time.Since(time.Unix(0, s.lastSeen.Load())) >= s.cfg.Session.IdleTimeout
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
			if s.idle() {
				return errIdle
			}
		}
	}
}

func (s *Session) Run(ctx context.Context) {
	defer close(s.done)
	everConnected := false
	for {
		if ctx.Err() != nil || s.idle() {
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

		client, err := s.connect(user, pass)
		if err != nil {
			if errors.Is(err, packets.ErrorRefusedBadUsernameOrPassword) || errors.Is(err, packets.ErrorRefusedNotAuthorised) {
				exp := ""
				if s.cfg.TokenExpiry != nil {
					exp = s.cfg.TokenExpiry().UTC().Format(time.RFC3339)
				}
				s.log.Error("upstream rejected credentials; waiting for a new token", "err", err, "token_exp", exp)
				if s.wait(ctx, 0, changed) != nil { // non-JWT: changed is nil, waits until idle/ctx
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
		s.backoff.Reset()
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
		reason := s.serve(ctx, client, pass, exp)
		client.Disconnect(250)
		s.setConnected(false)
		s.log.Info("upstream session closed", "reason", reason)
		if reason == "idle" || reason == "stopped" {
			return
		}
	}
}

func (s *Session) setConnected(up bool) {
	if s.cfg.Hooks.Connected != nil {
		s.cfg.Hooks.Connected(up)
	}
}

// serve publishes from the queue until the connection drops, the token needs
// renewing, the session goes idle or ctx ends.
func (s *Session) serve(ctx context.Context, client paho.Client, usedPass string, usedExp time.Time) string {
	for {
		if ctx.Err() != nil {
			return "stopped"
		}
		if s.idle() {
			return "idle"
		}
		if !client.IsConnectionOpen() {
			return "connection lost"
		}
		if !usedExp.IsZero() && time.Until(usedExp) < s.cfg.Session.RenewBefore {
			if _, pass, ok := s.cfg.Creds(); ok && pass != usedPass {
				return "renewing token"
			}
		}
		pctx, cancel := context.WithTimeout(ctx, s.poll())
		m, err := s.q.Pop(pctx)
		cancel()
		if err != nil {
			continue
		}
		tok := client.Publish(m.Topic, m.QoS, m.Retain, m.Payload)
		if !tok.WaitTimeout(10*time.Second) || tok.Error() != nil {
			s.q.Unpop(m)
			return fmt.Sprintf("publish failed: %v", tok.Error())
		}
		if s.cfg.Hooks.Published != nil {
			s.cfg.Hooks.Published()
		}
	}
}

func (s *Session) connect(user, pass string) (paho.Client, error) {
	broker, err := brokerURL(s.cfg.Upstream.Preset.URL)
	if err != nil {
		return nil, err
	}
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
	tlsCfg, err := tlsConfig(s.cfg.Upstream.CAFile)
	if err != nil {
		return nil, err
	}
	opts.SetTLSConfig(tlsCfg)
	client := paho.NewClient(opts)
	tok := client.Connect()
	if !tok.WaitTimeout(15 * time.Second) {
		return nil, errors.New("connect timed out")
	}
	if err := tok.Error(); err != nil {
		return nil, err
	}
	return client, nil
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
