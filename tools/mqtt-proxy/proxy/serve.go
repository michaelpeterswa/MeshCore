package proxy

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/agessaman/MeshCore/tools/mqtt-proxy/config"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/local"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/metrics"
	"github.com/agessaman/MeshCore/tools/mqtt-proxy/tokens"
)

// Serve runs the proxy until ctx ends: local subscriber, router, and the
// /healthz and /metrics listener.
func Serve(ctx context.Context, cfg *config.Config, log *slog.Logger) error {
	m := metrics.New()
	cache := tokens.New(time.Now)
	router := NewRouter(cfg, cache, m, log)

	var localUp atomic.Bool
	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if !localUp.Load() {
			http.Error(w, "local broker disconnected", http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("ok\n"))
	})
	srv := &http.Server{Addr: cfg.Listen, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	httpErr := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			httpErr <- err
		}
	}()

	routerDone := make(chan struct{})
	go func() { router.Run(ctx); close(routerDone) }()
	localDone := make(chan struct{})
	go func() {
		local.Run(ctx, cfg.Local, router.Handle, func(up bool) {
			localUp.Store(up)
			v := 0.0
			if up {
				v = 1
			}
			m.LocalConnected.Set(v)
		}, log)
		close(localDone)
	}()

	select {
	case <-ctx.Done():
	case err := <-httpErr:
		return err
	}
	log.Info("shutting down")
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(shutdown)
	<-localDone
	<-routerDone
	return nil
}
