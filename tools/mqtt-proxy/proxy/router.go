// Package proxy wires the local subscriber to upstream sessions.
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/agessaman/MeshCore/tools/mqtt-proxy/config"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/metrics"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/policy"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/presets"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/tokens"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/upstream"
)

type sessionRunner interface {
	Enqueue(upstream.Message) bool
	Run(ctx context.Context)
	Done() <-chan struct{}
}

type Router struct {
	cfg   *config.Config
	cache *tokens.Cache
	m     *metrics.Metrics
	log   *slog.Logger

	newSession func(u config.Upstream, device string) sessionRunner

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	sessions map[string]sessionRunner // device|upstream
	warned   map[string]bool          // one-shot log keys
}

func NewRouter(cfg *config.Config, cache *tokens.Cache, m *metrics.Metrics, log *slog.Logger) *Router {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Router{cfg: cfg, cache: cache, m: m, log: log, ctx: ctx, cancel: cancel,
		sessions: map[string]sessionRunner{}, warned: map[string]bool{}}
	r.newSession = r.realSession
	return r
}

// Run blocks until ctx ends, then stops every session and waits for them.
func (r *Router) Run(ctx context.Context) {
	<-ctx.Done()
	r.cancel()
	r.wg.Wait()
}

// warnOnce logs msg the first time key is seen until clear(key).
func (r *Router) warnOnce(key, msg string, args ...any) {
	r.mu.Lock()
	seen := r.warned[key]
	r.warned[key] = true
	r.mu.Unlock()
	if !seen {
		r.log.Warn(msg, args...)
	}
}

func (r *Router) clearWarn(key string) {
	r.mu.Lock()
	delete(r.warned, key)
	r.mu.Unlock()
}

// Handle processes one local message. It never blocks on an upstream.
func (r *Router) Handle(topic string, payload []byte, retained bool) {
	t, ok := policy.Parse(topic)
	if !ok {
		r.m.Malformed.Inc()
		r.log.Debug("ignoring unparseable topic", "topic", topic)
		return
	}
	if aud, ok := policy.IsAuth(t); ok {
		r.handleAuth(t, aud, payload)
		return
	}
	// The broker replays retained status/link/room stats on every subscribe.
	// They are stale; the device republishes on its own interval.
	if retained {
		return
	}

	type target struct {
		u     config.Upstream
		route policy.Route
	}
	var targets []target
	for _, u := range r.cfg.Upstreams {
		if route, ok := policy.Forward(t, u.Preset, u.MeshRankToken); ok {
			targets = append(targets, target{u, route})
		}
	}
	if len(targets) == 0 {
		return // room/*, link, and anything else stays on the LAN
	}
	if !jsonObject(payload) {
		r.m.Malformed.Inc()
		r.warnOnce("malformed|"+t.Device+"|"+t.Suffix, "dropping non-JSON-object payload", "device", t.Device, "suffix", t.Suffix)
		return
	}
	r.m.MessagesIn.WithLabelValues(t.Device).Inc()

	for _, tg := range targets {
		if tg.u.Preset.Auth == presets.AuthJWT {
			key := "notoken|" + t.Device + "|" + tg.u.Name
			if _, ok := r.cache.Get(t.Device, tg.u.Preset.Audience); !ok {
				r.m.Dropped.WithLabelValues(tg.u.Name, "no_token").Inc()
				r.warnOnce(key, "no token for audience; skipping upstream until the device publishes one (check mqtt.auth.aud on the device)",
					"device", t.Device, "upstream", tg.u.Name, "audience", tg.u.Preset.Audience)
				continue
			}
			r.clearWarn(key)
		}
		msg := upstream.Message{Topic: tg.route.Topic, Payload: payload, QoS: tg.route.QoS, Retain: tg.route.Retain}
		// A session that has just gone idle refuses; the retry gets a new one.
		if s := r.session(tg.u, t.Device, nil); !s.Enqueue(msg) {
			r.session(tg.u, t.Device, s).Enqueue(msg)
		}
	}
}

func (r *Router) handleAuth(t policy.Topic, aud string, payload []byte) {
	key := "auth|" + t.Device + "|" + aud
	tok, err := r.cache.Put(t.Device, aud, payload)
	if err != nil {
		r.warnOnce(key, "ignoring device token", "device", t.Device, "audience", aud, "err", err)
		return
	}
	r.clearWarn(key)
	r.m.TokenExpiry.WithLabelValues(t.Device, aud).Set(float64(tok.ExpiresAt.Unix()))
}

// session returns the live session for device on u, starting one if needed.
// stale, if non-nil, is a session that refused a message and must be replaced.
func (r *Router) session(u config.Upstream, device string, stale sessionRunner) sessionRunner {
	key := device + "|" + u.Name
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.sessions[key]; ok && s != stale {
		select {
		case <-s.Done():
		default:
			return s
		}
	}
	s := r.newSession(u, device)
	r.sessions[key] = s
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		s.Run(r.ctx)
		r.mu.Lock()
		if r.sessions[key] == s {
			delete(r.sessions, key)
		}
		r.mu.Unlock()
	}()
	return s
}

func (r *Router) realSession(u config.Upstream, device string) sessionRunner {
	sc := upstream.SessionConfig{
		Upstream: u, Device: device, Session: r.cfg.Session, Logger: r.log,
		Hooks: upstream.Hooks{
			Connected: func(up bool) {
				v := 0.0
				if up {
					v = 1
				}
				r.m.UpstreamConnected.WithLabelValues(device, u.Name).Set(v)
			},
			Published: func() { r.m.MessagesOut.WithLabelValues(device, u.Name).Inc() },
			Dropped:   func(reason string) { r.m.Dropped.WithLabelValues(u.Name, reason).Inc() },
			Reconnect: func() { r.m.Reconnects.WithLabelValues(u.Name).Inc() },
		},
	}
	switch u.Preset.Auth {
	case presets.AuthJWT:
		aud := u.Preset.Audience
		sc.Creds = upstream.JWTCredentials(r.cache, device, aud)
		sc.TokenChanged = func() <-chan struct{} { return r.cache.Changed(device, aud) }
		sc.TokenExpiry = func() time.Time { tok, _ := r.cache.Get(device, aud); return tok.ExpiresAt }
	default:
		user := u.Preset.Username
		if user == presets.UsernamePubkey {
			user = device
		}
		sc.Creds = upstream.StaticCredentials(user, u.Preset.Password)
	}
	return upstream.NewSession(sc)
}

func jsonObject(b []byte) bool {
	b = bytes.TrimLeft(b, " \t\r\n")
	return len(b) > 0 && b[0] == '{' && json.Valid(b)
}
