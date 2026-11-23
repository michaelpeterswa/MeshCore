package presets

import (
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// One row of MQTT_PRESETS in the firmware header.
var rowRe = regexp.MustCompile(`^\s*\{\s*"([^"]+)",\s*"([^"]+)",\s*(nullptr|"[^"]*"),\s*\w+,\s*(MQTT_AUTH_\w+),\s*(MQTT_TOPIC_\w+),\s*\d+,\s*(true|false),\s*(\d+),\s*(nullptr|MQTT_USERPASS_USERNAME_PUBKEY|"[^"]*"),\s*(nullptr|"[^"]*")\s*\},`)

func unquote(s string) string {
	switch s {
	case "nullptr":
		return ""
	case "MQTT_USERPASS_USERNAME_PUBKEY":
		return UsernamePubkey
	}
	return strings.Trim(s, `"`)
}

func TestPresetsMatchFirmware(t *testing.T) {
	src, err := os.ReadFile("../../../src/helpers/MQTTPresets.h")
	if err != nil {
		t.Fatal(err)
	}
	var want []Preset
	for _, line := range strings.Split(string(src), "\n") {
		m := rowRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		ka, _ := strconv.Atoi(m[7])
		p := Preset{
			Name: m[1], URL: m[2], Audience: unquote(m[3]),
			Auth:        map[string]AuthType{"MQTT_AUTH_NONE": AuthNone, "MQTT_AUTH_USERPASS": AuthUserPass, "MQTT_AUTH_JWT": AuthJWT}[m[4]],
			Style:       map[string]TopicStyle{"MQTT_TOPIC_MESHCORE": StyleMeshCore, "MQTT_TOPIC_MESHRANK": StyleMeshRank}[m[5]],
			AllowRetain: m[6] == "true",
			KeepAlive:   time.Duration(ka) * time.Second,
			Username:    unquote(m[8]), Password: unquote(m[9]),
		}
		want = append(want, p)
	}
	if count := regexp.MustCompile(`MQTT_PRESET_COUNT = (\d+);`).FindStringSubmatch(string(src)); count == nil || strconv.Itoa(len(want)) != count[1] {
		t.Fatalf("parsed %d firmware rows, header count %v: regexp out of date", len(want), count)
	}
	got := All()
	if len(got) != len(want) {
		t.Fatalf("Go table has %d presets, firmware has %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("preset %d:\n got  %+v\n want %+v", i, got[i], want[i])
		}
	}
}

func TestLookup(t *testing.T) {
	p, ok := Lookup("analyzer-us")
	if !ok || p.URL != "wss://mqtt-us-v1.letsmesh.net:443/mqtt" || p.Audience != "mqtt-us-v1.letsmesh.net" ||
		p.Auth != AuthJWT || !p.AllowRetain || p.KeepAlive != 55*time.Second {
		t.Fatalf("analyzer-us = %+v, %v", p, ok)
	}
	m, ok := Lookup("meshrank")
	if !ok || m.Style != StyleMeshRank || m.Auth != AuthNone {
		t.Fatalf("meshrank = %+v, %v", m, ok)
	}
	if _, ok := Lookup("nope"); ok {
		t.Fatal("unknown preset found")
	}
}
