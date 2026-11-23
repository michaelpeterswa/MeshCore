package policy

import (
	"strings"
	"testing"

	"github.com/agessaman/MeshCore/tools/mqtt-proxy/presets"
)

const dev = "6C265917E1D4BA04FB14A9C2C985B5143FD74C9DA89472C2D731A9A1413A9FAD"

func mustPreset(t *testing.T, name string) presets.Preset {
	p, ok := presets.Lookup(name)
	if !ok {
		t.Fatal(name)
	}
	return p
}

func TestParse(t *testing.T) {
	ok := map[string]Topic{
		"meshcore/SEA/" + dev + "/packets":                      {"SEA", dev, "packets"},
		"meshcore/SEA/" + dev + "/room/posts":                   {"SEA", dev, "room/posts"},
		"meshcore/sea/" + strings.ToLower(dev) + "/status":      {"sea", dev, "status"},
		"meshcore/SEA/" + dev + "/auth/mqtt-us-v1.letsmesh.net": {"SEA", dev, "auth/mqtt-us-v1.letsmesh.net"},
	}
	for in, want := range ok {
		got, k := Parse(in)
		if !k || got != want {
			t.Errorf("Parse(%q) = %+v, %v; want %+v", in, got, k, want)
		}
	}
	bad := []string{
		"meshrank/SEA/" + dev + "/packets",
		"meshcore/SE/" + dev + "/packets",
		"meshcore/SE-/" + dev + "/packets",
		"meshcore/XXX/" + dev + "/packets",
		"meshcore/SEA/" + dev[:63] + "/packets",
		"meshcore/SEA/" + dev[:63] + "Z/packets",
		"meshcore/SEA/" + dev + "/",
		"meshcore/SEA/" + dev,
		"meshcore/SEA",
		"",
	}
	for _, in := range bad {
		if got, k := Parse(in); k {
			t.Errorf("Parse(%q) accepted: %+v", in, got)
		}
	}
}

func TestIsAuth(t *testing.T) {
	if a, ok := IsAuth(Topic{"SEA", dev, "auth/mqtt-us-v1.letsmesh.net"}); !ok || a != "mqtt-us-v1.letsmesh.net" {
		t.Errorf("got %q %v", a, ok)
	}
	for _, s := range []string{"auth/", "auth", "status", "room/auth/x"} {
		if _, ok := IsAuth(Topic{"SEA", dev, s}); ok {
			t.Errorf("IsAuth(%q) true", s)
		}
	}
}

func TestForwardMeshCore(t *testing.T) {
	us := mustPreset(t, "analyzer-us")
	cases := []struct {
		suffix string
		qos    byte
		retain bool
	}{{"status", 1, true}, {"packets", 0, false}, {"raw", 0, false}, {"neighbors", 1, false}}
	for _, c := range cases {
		r, ok := Forward(Topic{"SEA", dev, c.suffix}, us, "")
		want := "meshcore/SEA/" + dev + "/" + c.suffix
		if !ok || r.Topic != want || r.QoS != c.qos || r.Retain != c.retain {
			t.Errorf("%s: %+v %v", c.suffix, r, ok)
		}
	}
}

func TestForwardNoRetainWhenPresetForbids(t *testing.T) {
	r, ok := Forward(Topic{"SEA", dev, "status"}, mustPreset(t, "waev"), "")
	if !ok || r.Retain {
		t.Errorf("%+v %v", r, ok)
	}
}

func TestForwardMeshRank(t *testing.T) {
	mr := mustPreset(t, "meshrank")
	for _, s := range []string{"status", "packets", "neighbors"} {
		r, ok := Forward(Topic{"SEA", dev, s}, mr, "TOK")
		if !ok || r.Topic != "meshrank/uplink/TOK/"+dev+"/"+s {
			t.Errorf("%s: %+v %v", s, r, ok)
		}
	}
	if _, ok := Forward(Topic{"SEA", dev, "raw"}, mr, "TOK"); ok {
		t.Error("meshrank got raw")
	}
	if _, ok := Forward(Topic{"SEA", dev, "status"}, mr, ""); ok {
		t.Error("meshrank without token forwarded")
	}
}

func TestNeverForwarded(t *testing.T) {
	never := []string{"room/posts", "room/events", "room/dm", "room/stats", "link", "auth/mqtt-us-v1.letsmesh.net",
		"unknown", "status/extra", "packets/x", "Status"}
	for _, name := range []string{"analyzer-us", "meshrank", "waev", "tennmesh"} {
		p := mustPreset(t, name)
		for _, s := range never {
			if r, ok := Forward(Topic{"SEA", dev, s}, p, "TOK"); ok {
				t.Errorf("%s forwarded %q as %+v", name, s, r)
			}
		}
	}
}
