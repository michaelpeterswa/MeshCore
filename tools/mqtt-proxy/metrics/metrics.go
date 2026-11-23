// Package metrics holds the proxy's Prometheus series (prefix meshcore_proxy_).
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	Registry          *prometheus.Registry
	MessagesIn        *prometheus.CounterVec // device
	MessagesOut       *prometheus.CounterVec // device, upstream
	Dropped           *prometheus.CounterVec // upstream, reason
	Malformed         prometheus.Counter
	Reconnects        *prometheus.CounterVec // upstream
	TokenExpiry       *prometheus.GaugeVec   // device, audience: unix seconds the cached token expires
	UpstreamConnected *prometheus.GaugeVec   // device, upstream
	LocalConnected    prometheus.Gauge
}

func New() *Metrics {
	r := prometheus.NewRegistry()
	f := func(c prometheus.Collector) { r.MustRegister(c) }
	m := &Metrics{
		Registry:          r,
		MessagesIn:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "meshcore_proxy_messages_in_total", Help: "Forwardable messages received from the local broker."}, []string{"device"}),
		MessagesOut:       prometheus.NewCounterVec(prometheus.CounterOpts{Name: "meshcore_proxy_messages_out_total", Help: "Messages published to an upstream."}, []string{"device", "upstream"}),
		Dropped:           prometheus.NewCounterVec(prometheus.CounterOpts{Name: "meshcore_proxy_dropped_total", Help: "Messages not delivered upstream, by reason."}, []string{"upstream", "reason"}),
		Malformed:         prometheus.NewCounter(prometheus.CounterOpts{Name: "meshcore_proxy_malformed_total", Help: "Local messages dropped as malformed."}),
		Reconnects:        prometheus.NewCounterVec(prometheus.CounterOpts{Name: "meshcore_proxy_reconnects_total", Help: "Upstream connections re-established after a loss."}, []string{"upstream"}),
		TokenExpiry:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "meshcore_proxy_token_expiry_timestamp_seconds", Help: "When the cached device token expires."}, []string{"device", "audience"}),
		UpstreamConnected: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "meshcore_proxy_upstream_connected", Help: "1 while a device's upstream session is connected."}, []string{"device", "upstream"}),
		LocalConnected:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "meshcore_proxy_local_connected", Help: "1 while connected to the local broker."}),
	}
	for _, c := range []prometheus.Collector{m.MessagesIn, m.MessagesOut, m.Dropped, m.Malformed, m.Reconnects, m.TokenExpiry, m.UpstreamConnected, m.LocalConnected,
		collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})} {
		f(c)
	}
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
