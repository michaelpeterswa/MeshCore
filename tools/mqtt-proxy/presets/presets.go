// Package presets mirrors the firmware's broker preset table
// (src/helpers/MQTTPresets.h). TestPresetsMatchFirmware fails when the two
// drift; update this table in the same change as the header.
//
// The CA column is not carried over (upstream TLS is verified against the
// system roots) and neither is the token lifetime (the device signs tokens).
package presets

import "time"

type AuthType int

const (
	AuthNone AuthType = iota
	AuthUserPass
	AuthJWT
)

type TopicStyle int

const (
	StyleMeshCore TopicStyle = iota // meshcore/{iata}/{device}/{type}
	StyleMeshRank                   // meshrank/uplink/{token}/{device}/{type}, no raw
)

// UsernamePubkey as a preset Username means "the device public key hex".
const UsernamePubkey = "{pubkey}"

type Preset struct {
	Name, URL, Audience string
	Auth                AuthType
	Style               TopicStyle
	AllowRetain         bool
	KeepAlive           time.Duration // 0 = client default
	Username, Password  string        // embedded USERPASS credentials, if any
}

var table = []Preset{
	{Name: "analyzer-us", URL: "wss://mqtt-us-v1.letsmesh.net:443/mqtt", Audience: "mqtt-us-v1.letsmesh.net", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "analyzer-eu", URL: "wss://mqtt-eu-v1.letsmesh.net:443/mqtt", Audience: "mqtt-eu-v1.letsmesh.net", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "nz-analyzer", URL: "wss://meshcore-mqtt-1.baird.io:443", Audience: "meshcore-mqtt-1.baird.io", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "meshmapper", URL: "wss://mqtt.meshmapper.net:443/mqtt", Audience: "mqtt.meshmapper.net", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "meshrank", URL: "mqtts://meshrank.net:8883", Audience: "", Auth: AuthNone, Style: StyleMeshRank, AllowRetain: false, KeepAlive: 0 * time.Second, Username: "", Password: ""},
	{Name: "waev", URL: "wss://mqtt.waev.app:443/mqtt", Audience: "mqtt.waev.app", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: false, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "meshomatic", URL: "wss://us-east.meshomatic.net:443/mqtt", Audience: "us-east.meshomatic.net", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "cascadiamesh", URL: "wss://mqtt-v1.cascadiamesh.org:443/mqtt", Audience: "mqtt-v1.cascadiamesh.org", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "tennmesh", URL: "mqtt://mqtt.tennmesh.com:1883", Audience: "", Auth: AuthUserPass, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "mqttfeed", Password: "tc2live"},
	{Name: "nashmesh", URL: "mqtt://mqtt.nashme.sh:1883", Audience: "", Auth: AuthUserPass, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "meshdev", Password: "large4cats"},
	{Name: "ctmesh", URL: "mqtt://mqtt.ctmesh.org:1883", Audience: "", Auth: AuthUserPass, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 60 * time.Second, Username: "meshdev", Password: "large4cats"},
	{Name: "chimesh", URL: "wss://mqtt.chimesh.org:443", Audience: "mqtt.chimesh.org", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "meshat.se", URL: "wss://meshcore-mqtt.meshat.se:443", Audience: "meshcore-mqtt.meshat.se", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "eastidahomesh", URL: "mqtt://live.eastidahomesh.com:1883", Audience: "", Auth: AuthNone, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "coloradomesh", URL: "wss://mqtt.meshcore.coloradomesh.org:443", Audience: "mqtt.meshcore.coloradomesh.org", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "dutchmeshcore-1", URL: "wss://collector1.dutchmeshcore.nl:443/mqtt", Audience: "collector1.dutchmeshcore.nl", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "dutchmeshcore-2", URL: "wss://collector2.dutchmeshcore.nl:443/mqtt", Audience: "collector2.dutchmeshcore.nl", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "meshcore-ca-1", URL: "wss://mqtt1.meshcore.ca:443/mqtt", Audience: "mqtt1.meshcore.ca", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "meshcore-ca-2", URL: "wss://mqtt2.meshcore.ca:443/mqtt", Audience: "mqtt2.meshcore.ca", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "meshcore-fi", URL: "wss://mc-mqtt.meshcore.fi:443/", Audience: "mc-mqtt.meshcore.fi", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "okimesh-1", URL: "wss://mqtt1.okimesh.org:9002/mqtt", Audience: "mqtt1.okimesh.org", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "okimesh-2", URL: "wss://mqtt2.okimesh.org:9002/mqtt", Audience: "mqtt2.okimesh.org", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "inwmesh", URL: "mqtts://scope.inwmesh.org:8883", Audience: "", Auth: AuthUserPass, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "bostonmesh", URL: "wss://mqttmc01.bostonme.sh:443/mqtt", Audience: "mqttmc01.bostonme.sh", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "rflab", URL: "wss://mqtt.rflab.io:443", Audience: "mqtt.rflab.io", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "ipnt.uk", URL: "wss://mqtt.ipnt.uk:443", Audience: "mqtt.ipnt.uk", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "flmesh", URL: "wss://mcmqtt.jntconnections.com:443", Audience: "mcmqtt.jntconnections.com", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "corecomms", URL: "wss://mqtt.corecomms.net:443/mqtt", Audience: "mqtt.corecomms.net", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "meshtexas", URL: "wss://mqtt.meshtexas.org:443/mqtt", Audience: "mqtt.meshtexas.org", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "mesh-chaun14", URL: "mqtt://mqtt.mesh.chaun14.fr:1884", Audience: "", Auth: AuthUserPass, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 60 * time.Second, Username: UsernamePubkey, Password: ""},
	{Name: "wcmesh", URL: "wss://mqtt.wcmesh.com:443", Audience: "mqtt.wcmesh.com", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "atvirastinklas", URL: "wss://mqtt-mc.atvirastinklas.lt:443", Audience: "mqtt-mc.atvirastinklas.lt", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "gomesh", URL: "wss://mqtt.gomesh.dev:443", Audience: "mqtt.gomesh.dev", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "idahomesh", URL: "wss://mqtt.idahomesh.org:443/mqtt", Audience: "mqtt.idahomesh.org", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "ntxmesh", URL: "wss://ntxmesh.dhovin.me:8883", Audience: "ntxmesh.dhovin.me", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
	{Name: "bsmesh", URL: "wss://mqtt.bsmesh.de:8885", Audience: "mqtt.bsmesh.de", Auth: AuthJWT, Style: StyleMeshCore, AllowRetain: true, KeepAlive: 55 * time.Second, Username: "", Password: ""},
}

// All returns the presets in firmware order.
func All() []Preset { return append([]Preset(nil), table...) }

// Lookup finds a preset by name.
func Lookup(name string) (Preset, bool) {
	for _, p := range table {
		if p.Name == name {
			return p, true
		}
	}
	return Preset{}, false
}
