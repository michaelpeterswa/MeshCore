// Package policy decides which local topics reach an analyzer and under what
// topic. It mirrors the firmware's mqttBuildPublicationTopic
// (src/helpers/MQTTTopicRouter.h) for the four observer types and refuses
// everything else: room logs, link state and device tokens stay on the LAN.
package policy

import (
	"strings"

	"github.com/agessaman/MeshCore/tools/mqtt-proxy/presets"
)

// Topic is a parsed meshcore/{iata}/{device}/{suffix} topic. Device is
// uppercased; Suffix may contain slashes (room/posts, auth/<audience>).
type Topic struct {
	IATA, Device, Suffix string
}

// forwardable suffixes and the QoS each is published upstream with.
var forwardable = map[string]byte{
	"status":    1,
	"packets":   0,
	"raw":       0,
	"neighbors": 1,
}

func Parse(topic string) (Topic, bool) {
	parts := strings.SplitN(topic, "/", 4)
	if len(parts) != 4 || parts[0] != "meshcore" || parts[3] == "" {
		return Topic{}, false
	}
	iata, device := parts[1], parts[2]
	if !iataValid(iata) || len(device) != 64 || !isHex(device) {
		return Topic{}, false
	}
	return Topic{IATA: iata, Device: strings.ToUpper(device), Suffix: parts[3]}, true
}

// IsAuth reports whether t is a device token topic, auth/<audience>.
func IsAuth(t Topic) (audience string, ok bool) {
	aud, found := strings.CutPrefix(t.Suffix, "auth/")
	if !found || aud == "" || strings.Contains(aud, "/") {
		return "", false
	}
	return aud, true
}

type Route struct {
	Topic  string
	QoS    byte
	Retain bool
}

// Forward returns where t goes on an upstream using preset p, or false if it
// must not leave the LAN.
func Forward(t Topic, p presets.Preset, meshrankToken string) (Route, bool) {
	qos, ok := forwardable[t.Suffix]
	if !ok {
		return Route{}, false
	}
	r := Route{QoS: qos, Retain: t.Suffix == "status" && p.AllowRetain}
	switch p.Style {
	case presets.StyleMeshCore:
		r.Topic = "meshcore/" + t.IATA + "/" + t.Device + "/" + t.Suffix
	case presets.StyleMeshRank:
		// Raw is withheld: highest volume, and meshrank does not consume it.
		if t.Suffix == "raw" || meshrankToken == "" {
			return Route{}, false
		}
		r.Topic = "meshrank/uplink/" + meshrankToken + "/" + t.Device + "/" + t.Suffix
	default:
		return Route{}, false
	}
	return r, true
}

// iataValid matches the firmware's mqttIataValid plus the "XXX" placeholder check.
func iataValid(s string) bool {
	if len(s) != 3 || s == "XXX" {
		return false
	}
	for _, c := range s {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func isHex(s string) bool {
	for _, c := range s {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}
