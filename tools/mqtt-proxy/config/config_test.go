package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agessaman/MeshCore/tools/mqtt-proxy/presets"
)

func write(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

const minimal = `
local:
  broker: tcp://mosquitto:1883
upstreams:
  - name: letsmesh-us
    preset: analyzer-us
`

func TestLoadMinimal(t *testing.T) {
	c, err := Load(write(t, minimal))
	if err != nil {
		t.Fatal(err)
	}
	if c.Listen != ":8080" || c.Local.ClientID != "meshcore-mqtt-proxy" {
		t.Errorf("defaults: listen %q client_id %q", c.Listen, c.Local.ClientID)
	}
	if c.Session.IdleTimeout != 10*time.Minute || c.Session.RenewBefore != 10*time.Minute || c.Session.QueueSize != 256 {
		t.Errorf("session defaults: %+v", c.Session)
	}
	if len(c.Upstreams) != 1 || c.Upstreams[0].Name != "letsmesh-us" || c.Upstreams[0].Preset.Name != "analyzer-us" ||
		c.Upstreams[0].Preset.Auth != presets.AuthJWT {
		t.Errorf("upstreams: %+v", c.Upstreams)
	}
}

func TestSessionOverrides(t *testing.T) {
	c, err := Load(write(t, minimal+`
session:
  idle_timeout: 2m
  renew_before: 30m
  queue_size: 64
listen: ":9090"
`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Session.IdleTimeout != 2*time.Minute || c.Session.RenewBefore != 30*time.Minute || c.Session.QueueSize != 64 || c.Listen != ":9090" {
		t.Errorf("%+v %q", c.Session, c.Listen)
	}
}

func TestPasswordFileWins(t *testing.T) {
	dir := t.TempDir()
	pf := filepath.Join(dir, "pw")
	os.WriteFile(pf, []byte("from-file\n"), 0o600)
	c, err := Load(write(t, `
local:
  broker: tcp://mosquitto:1883
  username: mqtt-proxy
  password: inline
  password_file: `+pf+`
upstreams:
  - name: private
    url: mqtts://broker.example:8883
    username: u
    password: inline
    password_file: `+pf+`
`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Local.Password != "from-file" || c.Upstreams[0].Preset.Password != "from-file" {
		t.Errorf("local %q upstream %q", c.Local.Password, c.Upstreams[0].Preset.Password)
	}
}

func TestCustomURLUpstream(t *testing.T) {
	c, err := Load(write(t, `
local:
  broker: tcp://mosquitto:1883
upstreams:
  - name: private
    url: mqtts://broker.example:8883
    username: u
    password: p
    ca_file: /etc/ca.pem
`))
	if err != nil {
		t.Fatal(err)
	}
	u := c.Upstreams[0]
	if u.Preset.Name != "custom" || u.Preset.URL != "mqtts://broker.example:8883" || u.Preset.Auth != presets.AuthUserPass ||
		u.Preset.Style != presets.StyleMeshCore || u.Preset.Username != "u" || u.Preset.Password != "p" || u.CAFile != "/etc/ca.pem" {
		t.Errorf("%+v", u)
	}
}

func TestCustomJWTUpstream(t *testing.T) {
	c, err := Load(write(t, `
local:
  broker: tcp://mosquitto:1883
upstreams:
  - name: test
    url: wss://127.0.0.1:8443/mqtt
    audience: mqtt-us-v1.letsmesh.net
    allow_retain: true
`))
	if err != nil {
		t.Fatal(err)
	}
	p := c.Upstreams[0].Preset
	if p.Auth != presets.AuthJWT || p.Audience != "mqtt-us-v1.letsmesh.net" || !p.AllowRetain {
		t.Errorf("%+v", p)
	}
}

func TestPresetURLOverride(t *testing.T) {
	// Tests point a named preset at a local broker; the preset's audience and style stay.
	c, err := Load(write(t, `
local:
  broker: tcp://mosquitto:1883
upstreams:
  - name: fake-letsmesh
    preset: analyzer-us
    url: wss://127.0.0.1:8443/mqtt
`))
	if err != nil {
		t.Fatal(err)
	}
	p := c.Upstreams[0].Preset
	if p.Name != "analyzer-us" || p.URL != "wss://127.0.0.1:8443/mqtt" || p.Audience != "mqtt-us-v1.letsmesh.net" {
		t.Errorf("%+v", p)
	}
}

func TestMeshRankToken(t *testing.T) {
	c, err := Load(write(t, `
local:
  broker: tcp://mosquitto:1883
upstreams:
  - name: mr
    preset: meshrank
    meshrank_token: TOK
`))
	if err != nil {
		t.Fatal(err)
	}
	if c.Upstreams[0].MeshRankToken != "TOK" {
		t.Errorf("%+v", c.Upstreams[0])
	}
}

func TestErrors(t *testing.T) {
	cases := map[string]struct{ body, want string }{
		"unknown preset":  {"local: {broker: tcp://m:1883}\nupstreams: [{name: a, preset: bogus}]", "bogus"},
		"neither":         {"local: {broker: tcp://m:1883}\nupstreams: [{name: a}]", "preset or url"},
		"duplicate":       {"local: {broker: tcp://m:1883}\nupstreams: [{name: a, preset: analyzer-us}, {name: a, preset: analyzer-eu}]", "duplicate"},
		"meshrank token":  {"local: {broker: tcp://m:1883}\nupstreams: [{name: a, preset: meshrank}]", "meshrank_token"},
		"queue size":      {"local: {broker: tcp://m:1883}\nsession: {queue_size: 0}\nupstreams: [{name: a, preset: analyzer-us}]", "queue_size"},
		"no local broker": {"upstreams: [{name: a, preset: analyzer-us}]", "local.broker"},
		"bad scheme":      {"local: {broker: tcp://m:1883}\nupstreams: [{name: a, url: http://x:80}]", "scheme"},
		"no upstreams":    {"local: {broker: tcp://m:1883}", "upstreams"},
		"unknown key":     {"local: {broker: tcp://m:1883}\nupstreams: [{name: a, preset: analyzer-us, presett: x}]", "presett"},
		"missing name":    {"local: {broker: tcp://m:1883}\nupstreams: [{preset: analyzer-us}]", "name"},
		"missing pw file": {"local: {broker: tcp://m:1883, password_file: /nonexistent/pw}\nupstreams: [{name: a, preset: analyzer-us}]", "password_file"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Load(write(t, tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want mention of %q", err, tc.want)
			}
		})
	}
}
