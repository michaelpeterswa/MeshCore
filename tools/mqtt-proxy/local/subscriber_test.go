package local

import (
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/agessaman/MeshCore/tools/mqtt-proxy/config"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/internal/testbroker"
)

func TestRunDeliversMessages(t *testing.T) {
	b := testbroker.Start(t, testbroker.Options{Kind: testbroker.TCP})
	got := make(chan string, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	up := make(chan bool, 4)
	go Run(ctx, config.Local{Broker: strings.Replace(b.URL, "mqtt://", "tcp://", 1), ClientID: "t"},
		func(topic string, _ []byte, _ bool) { got <- topic }, func(v bool) { up <- v }, slog.Default())
	select {
	case v := <-up:
		if !v {
			t.Fatal("reported down first")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("never connected")
	}
	b.Publish(t, "meshcore/SEA/x/status", []byte(`{}`), false, 0)
	select {
	case topic := <-got:
		if topic != "meshcore/SEA/x/status" {
			t.Fatal(topic)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no message")
	}
}

func TestRunFailsWhenSubscribeDenied(t *testing.T) {
	// A refused SUBSCRIBE would otherwise leave the proxy connected and deaf.
	b := testbroker.Start(t, testbroker.Options{Kind: testbroker.TCP, DenySubscribe: true})
	errc := make(chan error, 1)
	go func() {
		errc <- Run(context.Background(), config.Local{Broker: b.URL, ClientID: "t"},
			func(string, []byte, bool) {}, func(bool) {}, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})))
	}()
	select {
	case err := <-errc:
		if err == nil || !strings.Contains(err.Error(), "subscribe") {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run kept going after a refused subscribe")
	}
}
