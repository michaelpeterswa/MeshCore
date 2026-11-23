// Package tokens caches the JWTs each gateway signs and publishes, retained,
// on meshcore/{iata}/{device}/auth/{audience}. The proxy never holds device
// keys; these tokens are its only upstream credentials.
package tokens

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type Token struct {
	Device, Audience string
	Username         string // upstream login: "v1_" + uppercase device hex
	JWT              string
	IssuedAt         time.Time
	ExpiresAt        time.Time
}

type entry struct {
	tok     Token
	have    bool
	changed chan struct{}
}

type Cache struct {
	mu      sync.Mutex
	now     func() time.Time
	entries map[string]*entry
}

func New(now func() time.Time) *Cache {
	return &Cache{now: now, entries: map[string]*entry{}}
}

func key(device, audience string) string { return strings.ToUpper(device) + "|" + audience }

func (c *Cache) entryLocked(device, audience string) *entry {
	k := key(device, audience)
	e := c.entries[k]
	if e == nil {
		e = &entry{changed: make(chan struct{})}
		c.entries[k] = e
	}
	return e
}

type authMessage struct {
	Audience string `json:"audience"`
	Username string `json:"username"`
	Token    string `json:"token"`
	IAT      int64  `json:"iat"`
	EXP      int64  `json:"exp"`
}

// Put validates an auth message published for device and audience (both from
// the topic) and caches it if it is newer than what is held. Replaying the
// held token is not an error and signals nothing.
func (c *Cache) Put(device, audience string, payload []byte) (Token, error) {
	var m authMessage
	if err := json.Unmarshal(payload, &m); err != nil {
		return Token{}, fmt.Errorf("auth message: %w", err)
	}
	device = strings.ToUpper(device)
	switch {
	case m.Token == "":
		return Token{}, errors.New("auth message: no token")
	case !strings.EqualFold(m.Username, device):
		return Token{}, fmt.Errorf("auth message: username %q is not device %s", m.Username, device)
	case m.Audience != audience:
		return Token{}, fmt.Errorf("auth message: audience %q on topic for %q", m.Audience, audience)
	}
	tok := Token{
		Device: device, Audience: audience,
		Username:  "v1_" + device,
		JWT:       m.Token,
		IssuedAt:  time.Unix(m.IAT, 0),
		ExpiresAt: time.Unix(m.EXP, 0),
	}
	now := c.now()
	if !tok.ExpiresAt.After(now) {
		return Token{}, fmt.Errorf("auth message: token expired at %s (device clock wrong?)", tok.ExpiresAt.UTC().Format(time.RFC3339))
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entryLocked(device, audience)
	if e.have {
		if tok.JWT == e.tok.JWT {
			return e.tok, nil
		}
		if !tok.IssuedAt.After(e.tok.IssuedAt) {
			return Token{}, fmt.Errorf("auth message: token issued %s is not newer than the cached one", tok.IssuedAt.UTC().Format(time.RFC3339))
		}
	}
	e.tok, e.have = tok, true
	close(e.changed)
	e.changed = make(chan struct{})
	return tok, nil
}

// Get returns the cached token unless it is missing or expired.
func (c *Cache) Get(device, audience string) (Token, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.entries[key(device, audience)]
	if e == nil || !e.have || !e.tok.ExpiresAt.After(c.now()) {
		return Token{}, false
	}
	return e.tok, true
}

// Changed returns a channel closed by the next accepted new token for this
// device and audience.
func (c *Cache) Changed(device, audience string) <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entryLocked(device, audience).changed
}
