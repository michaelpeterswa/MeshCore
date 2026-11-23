// Package local subscribes to the LAN broker the gateways publish to.
package local

import (
	"context"
	"log/slog"
	"strings"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/agessaman/MeshCore/tools/mqtt-proxy/config"
)

// Topic filter for every gateway's topics.
const Filter = "meshcore/+/+/#"

type Handler func(topic string, payload []byte, retained bool)

// Run connects to the local broker, subscribes to Filter (again after every
// reconnect) and passes messages to h until ctx ends. connected reports
// connection state changes.
func Run(ctx context.Context, cfg config.Local, h Handler, connected func(bool), log *slog.Logger) error {
	broker := cfg.Broker
	broker = strings.Replace(broker, "mqtt://", "tcp://", 1)
	broker = strings.Replace(broker, "mqtts://", "ssl://", 1)
	opts := paho.NewClientOptions().
		AddBroker(broker).
		SetClientID(cfg.ClientID).
		SetProtocolVersion(4).
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectRetry(true).
		SetConnectRetryInterval(5 * time.Second).
		SetMaxReconnectInterval(30 * time.Second).
		SetOrderMatters(false)
	if cfg.Username != "" {
		opts.SetUsername(cfg.Username)
		opts.SetPassword(cfg.Password)
	}
	opts.SetOnConnectHandler(func(c paho.Client) {
		tok := c.Subscribe(Filter, 1, func(_ paho.Client, m paho.Message) {
			h(m.Topic(), m.Payload(), m.Retained())
		})
		if tok.WaitTimeout(10*time.Second) && tok.Error() == nil {
			log.Info("local broker connected", "broker", cfg.Broker, "filter", Filter)
			connected(true)
			return
		}
		log.Error("local subscribe failed; reconnecting", "err", tok.Error())
		c.Disconnect(0) // the auto-reconnect resubscribes
	})
	opts.SetConnectionLostHandler(func(_ paho.Client, err error) {
		log.Warn("local broker connection lost", "err", err)
		connected(false)
	})
	client := paho.NewClient(opts)
	client.Connect() // retries in the background until ctx ends
	<-ctx.Done()
	client.Disconnect(500)
	connected(false)
	return nil
}
