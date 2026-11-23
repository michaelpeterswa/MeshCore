package proxy

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agessaman/MeshCore/tools/mqtt-proxy/config"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/internal/testbroker"
)

func device(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "device", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func freeAddr(t *testing.T) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func waitHealthy(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get("http://" + addr + "/healthz"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("proxy never became healthy")
}

func TestE2EForwardingAndRoomIsolation(t *testing.T) {
	lan := testbroker.Start(t, testbroker.Options{Kind: testbroker.TCP})
	us := testbroker.Start(t, testbroker.Options{Kind: testbroker.WSS})
	mr := testbroker.Start(t, testbroker.Options{Kind: testbroker.TLS})

	const jwt = "eyJhbGciOiJFZDI1NTE5IiwidHlwIjoiSldUIn0.synthetic.signature"
	now := time.Now()
	auth := fmt.Sprintf(`{"audience":%q,"username":%q,"token":%q,"iat":%d,"exp":%d}`, aud, dev, jwt, now.Unix(), now.AddDate(1, 0, 0).Unix())
	// What the broker already holds when the proxy starts: all retained.
	lan.Publish(t, base+"auth/"+aud, []byte(auth), true, 1)
	lan.Publish(t, base+"status", []byte(`{"status":"online","stale":"retained-before-start"}`), true, 1)
	lan.Publish(t, base+"link", device(t, "link.json"), true, 1)
	lan.Publish(t, base+"room/stats", device(t, "room_stats.json"), true, 1)

	listen := freeAddr(t)
	cfgFile := filepath.Join(t.TempDir(), "config.yaml")
	os.WriteFile(cfgFile, []byte(fmt.Sprintf(`
listen: %q
local:
  broker: tcp://%s
upstreams:
  - name: letsmesh-us
    preset: analyzer-us
    url: %s
    audience: mqtt-us-v1.letsmesh.net
    ca_file: %s
  - name: meshrank
    preset: meshrank
    url: %s
    ca_file: %s
    meshrank_token: TOK
`, listen, lan.Addr, us.URL, us.CAFile, mr.URL, mr.CAFile)), 0o600)
	cfg, err := config.Load(cfgFile)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- Serve(ctx, cfg, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	}()
	t.Cleanup(func() { cancel(); <-done })
	waitHealthy(t, listen)

	live := []struct{ suffix, file string }{
		{"status", "status.json"}, {"packets", "packets.json"}, {"packets", "packets.json"}, {"packets", "packets.json"},
		{"raw", "raw.json"}, {"neighbors", "neighbors.json"}, {"link", "link.json"},
		{"room/posts", "room_posts.json"}, {"room/events", "room_events.json"}, {"room/dm", "room_dm.json"}, {"room/stats", "room_stats.json"},
	}
	for _, m := range live {
		lan.Publish(t, base+m.suffix, device(t, m.file), false, 0)
	}

	us.WaitFor(t, 10*time.Second, "6 messages on letsmesh", func(r []testbroker.Received) bool { return len(r) >= 6 })
	mr.WaitFor(t, 10*time.Second, "5 messages on meshrank", func(r []testbroker.Received) bool { return len(r) >= 5 })
	time.Sleep(300 * time.Millisecond) // let anything that should not arrive arrive

	count := func(r []testbroker.Received) map[string]int {
		c := map[string]int{}
		for _, m := range r {
			c[m.Topic]++
		}
		return c
	}
	wantUS := map[string]int{base + "status": 1, base + "packets": 3, base + "raw": 1, base + "neighbors": 1}
	if got := count(us.Received()); fmt.Sprint(got) != fmt.Sprint(wantUS) {
		t.Errorf("letsmesh got %v\nwant %v", got, wantUS)
	}
	mrBase := "meshrank/uplink/TOK/" + dev + "/"
	wantMR := map[string]int{mrBase + "status": 1, mrBase + "packets": 3, mrBase + "neighbors": 1}
	if got := count(mr.Received()); fmt.Sprint(got) != fmt.Sprint(wantMR) {
		t.Errorf("meshrank got %v\nwant %v", got, wantMR)
	}

	for name, b := range map[string]*testbroker.Broker{"letsmesh": us, "meshrank": mr} {
		for _, m := range b.Received() {
			if strings.Contains(m.Topic, "/room/") || strings.HasSuffix(m.Topic, "/link") || strings.Contains(m.Topic, "/auth/") ||
				strings.Contains(string(m.Payload), "SECRET") || strings.Contains(string(m.Payload), "retained-before-start") {
				t.Errorf("%s received %s: %s", name, m.Topic, m.Payload)
			}
		}
	}
	for _, m := range us.Received() {
		if m.Topic == base+"status" && (!m.Retain || m.QoS != 1) {
			t.Errorf("status upstream retain=%v qos=%d", m.Retain, m.QoS)
		}
	}
	if l := us.Logins(); len(l) != 1 || l[0] != "v1_"+dev+":"+jwt {
		t.Errorf("letsmesh logins %v", l)
	}
	if l := mr.Logins(); len(l) != 1 || l[0] != ":" {
		t.Errorf("meshrank logins %v", l)
	}

	resp, err := http.Get("http://" + listen + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, want := range []string{
		`meshcore_proxy_messages_out_total{device="` + dev + `",upstream="letsmesh-us"} 6`,
		`meshcore_proxy_messages_out_total{device="` + dev + `",upstream="meshrank"} 5`,
		`meshcore_proxy_local_connected 1`,
	} {
		if !strings.Contains(string(body), want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}
