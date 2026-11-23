// Command mqtt-proxy forwards MeshCore gateway observer topics from a LAN
// broker to public analyzers, authenticating each device with the JWT it
// publishes. Room topics never leave the LAN.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/agessaman/MeshCore/tools/mqtt-proxy/config"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/proxy"
)

func main() {
	path := flag.String("config", "/etc/mqtt-proxy/config.yaml", "config file")
	debug := flag.Bool("debug", false, "debug logging")
	flag.Parse()
	level := slog.LevelInfo
	if *debug {
		level = slog.LevelDebug
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	if err := run(*path, log); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(path string, log *slog.Logger) error {
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	for _, u := range cfg.Upstreams {
		log.Info("upstream configured", "name", u.Name, "preset", u.Preset.Name, "url", u.Preset.URL, "audience", u.Preset.Audience)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return proxy.Serve(ctx, cfg, log)
}
