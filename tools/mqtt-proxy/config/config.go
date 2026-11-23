// Package config loads the proxy's YAML configuration.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/agessaman/MeshCore/tools/mqtt-proxy/presets"
)

type Config struct {
	Listen    string
	Local     Local
	Session   Session
	Upstreams []Upstream
}

// Local is the LAN broker the gateway publishes to.
type Local struct {
	Broker, ClientID, Username, Password string
}

type Session struct {
	IdleTimeout, RenewBefore time.Duration
	QueueSize                int
}

// Upstream is one analyzer. Preset is fully resolved: for a `url:` upstream
// it is a synthetic preset named "custom", and YAML credentials and URL
// overrides are already applied to it.
type Upstream struct {
	Name          string
	Preset        presets.Preset
	MeshRankToken string
	CAFile        string
}

type rawConfig struct {
	Listen string `yaml:"listen"`
	Local  struct {
		Broker       string `yaml:"broker"`
		ClientID     string `yaml:"client_id"`
		Username     string `yaml:"username"`
		Password     string `yaml:"password"`
		PasswordFile string `yaml:"password_file"`
	} `yaml:"local"`
	Session struct {
		IdleTimeout *time.Duration `yaml:"idle_timeout"`
		RenewBefore *time.Duration `yaml:"renew_before"`
		QueueSize   *int           `yaml:"queue_size"`
	} `yaml:"session"`
	Upstreams []struct {
		Name          string `yaml:"name"`
		Preset        string `yaml:"preset"`
		URL           string `yaml:"url"`
		Audience      string `yaml:"audience"`
		Username      string `yaml:"username"`
		Password      string `yaml:"password"`
		PasswordFile  string `yaml:"password_file"`
		MeshRankToken string `yaml:"meshrank_token"`
		CAFile        string `yaml:"ca_file"`
		AllowRetain   *bool  `yaml:"allow_retain"`
	} `yaml:"upstreams"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var raw rawConfig
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}

	c := &Config{
		Listen:  orDefault(raw.Listen, ":8080"),
		Session: Session{IdleTimeout: 10 * time.Minute, RenewBefore: 10 * time.Minute, QueueSize: 256},
	}
	if raw.Session.IdleTimeout != nil {
		c.Session.IdleTimeout = *raw.Session.IdleTimeout
	}
	if raw.Session.RenewBefore != nil {
		c.Session.RenewBefore = *raw.Session.RenewBefore
	}
	if raw.Session.QueueSize != nil {
		c.Session.QueueSize = *raw.Session.QueueSize
	}
	if c.Session.QueueSize < 1 {
		return nil, errors.New("session.queue_size must be at least 1")
	}
	if c.Session.IdleTimeout <= 0 {
		return nil, errors.New("session.idle_timeout must be positive")
	}

	if raw.Local.Broker == "" {
		return nil, errors.New("local.broker is required")
	}
	lpw, err := secret(raw.Local.Password, raw.Local.PasswordFile, "local.password_file")
	if err != nil {
		return nil, err
	}
	c.Local = Local{
		Broker:   raw.Local.Broker,
		ClientID: orDefault(raw.Local.ClientID, "meshcore-mqtt-proxy"),
		Username: raw.Local.Username,
		Password: lpw,
	}

	if len(raw.Upstreams) == 0 {
		return nil, errors.New("upstreams: at least one is required")
	}
	seen := map[string]bool{}
	for i, ru := range raw.Upstreams {
		where := fmt.Sprintf("upstreams[%d]", i)
		if ru.Name == "" {
			return nil, fmt.Errorf("%s: name is required", where)
		}
		where = fmt.Sprintf("upstream %q", ru.Name)
		if seen[ru.Name] {
			return nil, fmt.Errorf("%s: duplicate name", where)
		}
		seen[ru.Name] = true

		var p presets.Preset
		switch {
		case ru.Preset != "":
			var ok bool
			if p, ok = presets.Lookup(ru.Preset); !ok {
				return nil, fmt.Errorf("%s: unknown preset %q", where, ru.Preset)
			}
		case ru.URL != "":
			p = presets.Preset{Name: "custom", Style: presets.StyleMeshCore}
			switch {
			case ru.Audience != "":
				p.Auth = presets.AuthJWT
			case ru.Username != "":
				p.Auth = presets.AuthUserPass
			default:
				p.Auth = presets.AuthNone
			}
		default:
			return nil, fmt.Errorf("%s: needs a preset or url", where)
		}
		// Overrides apply to presets too (tests, private mirrors of a preset).
		// Pointing a JWT preset at another host must name the audience: the
		// device token for the preset's own audience is a live credential
		// for that analyzer and must not be sent anywhere else by accident.
		if ru.Preset != "" && ru.URL != "" && p.Auth == presets.AuthJWT && ru.Audience == "" {
			return nil, fmt.Errorf("%s: url overrides JWT preset %q; set audience explicitly (the preset's is %q)", where, ru.Preset, p.Audience)
		}
		if ru.URL != "" {
			p.URL = ru.URL
		}
		if ru.Audience != "" {
			p.Audience = ru.Audience
		}
		if ru.AllowRetain != nil {
			p.AllowRetain = *ru.AllowRetain
		}
		pw, err := secret(ru.Password, ru.PasswordFile, where+": password_file")
		if err != nil {
			return nil, err
		}
		if ru.Username != "" {
			p.Username = ru.Username
		}
		if pw != "" {
			p.Password = pw
		}
		if err := checkScheme(p.URL); err != nil {
			return nil, fmt.Errorf("%s: %w", where, err)
		}
		if p.Style == presets.StyleMeshRank && ru.MeshRankToken == "" {
			return nil, fmt.Errorf("%s: meshrank preset needs meshrank_token", where)
		}
		if p.Auth == presets.AuthJWT && p.Audience == "" {
			return nil, fmt.Errorf("%s: JWT upstream needs an audience", where)
		}
		c.Upstreams = append(c.Upstreams, Upstream{Name: ru.Name, Preset: p, MeshRankToken: ru.MeshRankToken, CAFile: ru.CAFile})
	}
	return c, nil
}

func checkScheme(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("url %q: %w", raw, err)
	}
	switch u.Scheme {
	case "mqtt", "mqtts", "ws", "wss":
	default:
		return fmt.Errorf("url %q: scheme must be mqtt, mqtts, ws or wss", raw)
	}
	if u.Host == "" {
		return fmt.Errorf("url %q: missing host", raw)
	}
	return nil
}

// secret returns the trimmed contents of file when set, else inline.
func secret(inline, file, field string) (string, error) {
	if file == "" {
		return inline, nil
	}
	b, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("%s: %w", field, err)
	}
	return strings.TrimSpace(string(b)), nil
}

func orDefault(v, d string) string {
	if v == "" {
		return d
	}
	return v
}
