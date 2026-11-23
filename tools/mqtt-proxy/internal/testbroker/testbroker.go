// Package testbroker runs an embedded MQTT broker (mochi) for tests: plain
// TCP, TLS or WSS with a throwaway CA, a pluggable login check, and a record
// of everything published to it.
package testbroker

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"
)

type Kind int

const (
	TCP Kind = iota // mqtt://
	TLS             // mqtts://
	WSS             // wss://.../mqtt
)

type Options struct {
	Kind Kind
	// Auth decides logins; nil accepts everyone.
	Auth func(username, password string) bool
	// Addr reuses an address (to restart a broker in place); "" picks a free port.
	Addr string
	// CA reuses another broker's CA so a restarted broker keeps the same trust root.
	CA *CA
}

type Received struct {
	Topic    string
	Payload  []byte
	QoS      byte
	Retain   bool
	Username string
}

type Broker struct {
	URL    string // scheme://127.0.0.1:port[/mqtt]
	Addr   string
	CAFile string
	CA     *CA

	srv       *mqtt.Server
	closeOnce sync.Once
	mu        sync.Mutex
	received  []Received
	logins    []string // "user:pass" for every attempt
	auth      func(u, p string) bool
}

func Start(t *testing.T, o Options) *Broker {
	t.Helper()
	addr := o.Addr
	if addr == "" {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr = l.Addr().String()
		l.Close()
	}
	b := &Broker{Addr: addr, auth: o.Auth}
	srv := mqtt.New(&mqtt.Options{InlineClient: true, Logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))})
	if err := srv.AddHook(&hook{b: b}, nil); err != nil {
		t.Fatal(err)
	}
	var tlsCfg *tls.Config
	if o.Kind != TCP {
		ca := o.CA
		if ca == nil {
			ca = NewCA(t)
		}
		b.CA, b.CAFile = ca, ca.File
		tlsCfg = ca.serverConfig(t)
	}
	cfg := listeners.Config{ID: "l", Address: addr, TLSConfig: tlsCfg}
	var err error
	switch o.Kind {
	case TCP:
		b.URL = "mqtt://" + addr
		err = srv.AddListener(listeners.NewTCP(cfg))
	case TLS:
		b.URL = "mqtts://" + addr
		err = srv.AddListener(listeners.NewTCP(cfg))
	case WSS:
		b.URL = "wss://" + addr + "/mqtt"
		err = srv.AddListener(listeners.NewWebsocket(cfg))
	}
	if err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	b.srv = srv
	t.Cleanup(b.Close)
	return b
}

func (b *Broker) Close() { b.closeOnce.Do(func() { b.srv.Close() }) }

// Publish injects a message as if a device had published it.
func (b *Broker) Publish(t *testing.T, topic string, payload []byte, retain bool, qos byte) {
	t.Helper()
	if err := b.srv.Publish(topic, payload, retain, qos); err != nil {
		t.Fatal(err)
	}
}

func (b *Broker) Received() []Received {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]Received(nil), b.received...)
}

func (b *Broker) Logins() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.logins...)
}

// WaitFor polls until cond holds over the received messages or fails the test.
func (b *Broker) WaitFor(t *testing.T, d time.Duration, what string, cond func([]Received) bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond(b.Received()) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s; received %d messages", what, len(b.Received()))
}

type hook struct {
	mqtt.HookBase
	b *Broker
}

func (h *hook) ID() string { return "testbroker" }
func (h *hook) Provides(x byte) bool {
	return bytes.Contains([]byte{mqtt.OnConnectAuthenticate, mqtt.OnACLCheck, mqtt.OnPublished}, []byte{x})
}
func (h *hook) OnConnectAuthenticate(cl *mqtt.Client, pk packets.Packet) bool {
	if cl.Net.Inline {
		return true
	}
	u, p := string(pk.Connect.Username), string(pk.Connect.Password)
	h.b.mu.Lock()
	h.b.logins = append(h.b.logins, u+":"+p)
	auth := h.b.auth
	h.b.mu.Unlock()
	return auth == nil || auth(u, p)
}
func (h *hook) OnACLCheck(*mqtt.Client, string, bool) bool { return true }
func (h *hook) OnPublished(cl *mqtt.Client, pk packets.Packet) {
	if cl.Net.Inline {
		return
	}
	h.b.mu.Lock()
	defer h.b.mu.Unlock()
	h.b.received = append(h.b.received, Received{
		Topic: pk.TopicName, Payload: append([]byte(nil), pk.Payload...),
		QoS: pk.FixedHeader.Qos, Retain: pk.FixedHeader.Retain, Username: string(cl.Properties.Username),
	})
}

// CA is a throwaway certificate authority with a server cert for 127.0.0.1.
type CA struct {
	File string
	cert tls.Certificate
}

func NewCA(t *testing.T) *CA {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "testbroker CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "127.0.0.1"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, KeyUsage: x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(file, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600); err != nil {
		t.Fatal(err)
	}
	return &CA{File: file, cert: tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}}
}

func (c *CA) serverConfig(*testing.T) *tls.Config {
	return &tls.Config{Certificates: []tls.Certificate{c.cert}, MinVersion: tls.VersionTLS12}
}
