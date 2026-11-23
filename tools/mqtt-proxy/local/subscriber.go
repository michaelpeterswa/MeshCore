// Package local subscribes to the LAN broker the gateways publish to.
package local

import (
	"context"
	"errors"
	"fmt"
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
// connection state changes. It returns an error if the broker refuses the
// subscription, so the process can exit and be restarted.
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
	fatal := make(chan error, 1)
	opts.SetOnConnectHandler(func(c paho.Client) {
		tok := c.Subscribe(Filter, 1, func(_ paho.Client, m paho.Message) {
			h(m.Topic(), m.Payload(), m.Retained())
		})
		err := tok.Error()
		if !tok.WaitTimeout(10 * time.Second) {
			err = errors.New("timed out")
		} else if st, ok := tok.(*paho.SubscribeToken); ok && err == nil && st.Result()[Filter] >= 0x80 {
			err = fmt.Errorf("broker refused (code 0x%02x)", st.Result()[Filter])
		}
		if err == nil {
			log.Info("local broker connected", "broker", cfg.Broker, "filter", Filter)
			connected(true)
			return
		}
		// Connected but deaf: exit so the orchestrator restarts us, rather
		// than sit on a session that receives nothing.
		select {
		case fatal <- fmt.Errorf("local subscribe %s: %w", Filter, err):
		default:
		}
	})
	opts.SetConnectionLostHandler(func(_ paho.Client, err error) {
		log.Warn("local broker connection lost", "err", err)
		connected(false)
	})
	client := paho.NewClient(opts)
	client.Connect() // retries in the background until ctx ends
	var err error
	select {
	case <-ctx.Done():
	case err = <-fatal:
	}
	client.Disconnect(500)
	connected(false)
	return err
}
