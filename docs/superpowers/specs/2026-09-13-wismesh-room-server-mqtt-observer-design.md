# WisMesh Ethernet Gateway room server with MQTT observer output

Date: 2026-09-13
Status: approved design, awaiting implementation plan
Base branch: `observer-firmware`

## Goal

Run the RAK WisMesh Ethernet Gateway (RAK4631 nRF52840 core, RAK19007 base,
RAK13800 W5100S Ethernet, optional RAK19018 PoE) as a MeshCore room server that
publishes the same metrics the ESP32 observer firmware publishes, plus a log of
everything that happens in the room, over plain MQTT to a broker on the LAN.

A Go proxy on the LAN forwards the analyzer-compatible topics to public MeshCore
analyzers over WSS with TLS. Room log topics never leave the LAN.

## Decisions already made

| Question | Decision |
|---|---|
| Broker | Local LAN broker, plain MQTT on 1883. TLS is the proxy's job. |
| Topology | The gateway itself is the room server. |
| Log content | Decrypted room posts, room events, direct messages to the room, raw packet stream. |
| Metrics | Node status, neighbors table, room stats, Ethernet link status. |
| Firmware vs proxy | Firmware speaks MQTT directly. Proxy only bridges upstream. |
| Format | Match the observer firmware's topics and JSON exactly for status, packets, raw, neighbors. |
| Proxy scope | Designed in this spec, implemented as a second plan. |
| Configuration | Runtime CLI, persisted, same prefs file as the observer. Build flags as first-boot defaults. |
| Hardware | WisMesh Ethernet Gateway only (RAK4631 + RAK13800). |
| Proxy stack | Go, Docker container. |
| Log privacy | Room log topics are LAN only. |
| Approach | New nRF52 bridge class beside the ESP32 bridge; proxy relays device-signed tokens. |

## Why not reuse the ESP32 bridge

`src/helpers/bridges/MQTTBridge.cpp` on the observer branch is about 4,400 lines
built on PsychicMqttClient (esp_mqtt and esp-tls), the ESP WiFi and Ethernet
APIs, FreeRTOS queues and a pinned task, and heap_caps and PSRAM allocation. The
`NetworkInterface` layer on `feat/abstract-networking` is wrapped in
`ESP_PLATFORM` as well. None of that compiles on nRF52, and the W5100S offers no
TLS. Refactoring the ESP32 bridge onto a transport abstraction was considered and
rejected as high risk for the same end result.

Dev already has RAK13800 support for the RAK4631 in
`src/helpers/ethernet/RAK13800/`, used by the `RAK_4631_room_server_ethernet` env
for a telnet CLI on port 23. That driver, its DHCP handling, and the early PoE
power sequencing in `RAK4631Board.cpp` are reused as they are.

## Part 1: Firmware

### 1.1 Architecture

One new bridge class, one new publisher, and a new build env. The room server is
changed only where it already calls the ESP32 bridge on the observer branch.

**`EthernetMQTTBridge`** in `src/helpers/bridges/`, derived from `BridgeBase`.
It exposes the calls the room server already makes: `begin`, `end`, `loop`,
`isRunning`, `onPacketReceived`, `sendPacket`, `storeRawRadioData`,
`setDeviceID`, `setFirmwareVersion`, `setBoardModel`, `setBuildDate`,
`setStatsSources`, `setNeighborsSchedule`, `requestPublishNeighbors`,
`getQueueSize`, and `getSlotStatusSnapshot`. A build flag selects which class
the room server's `bridge` pointer holds. Methods that only apply to ESP32
(`setSNMPAgent`, and OTA gating via `canFlashAfterStop`) become no-ops that
return a safe value. `requestForcedNtpSync` and `ntpDiag` work over Ethernet.

**Transport.** One PubSubClient over one `EthernetClient` from the RAK13800
library, plus one `EthernetUDP` for NTP. One broker slot. Slot two is fixed to
`none`. The bridge asks the Ethernet interface for link state before each
connect attempt.

**Publish path.** No task, no RTOS queue. `loop()` runs from the main loop and
performs at most one publish per pass. Two fixed ring buffers:

- Packet ring: 16 entries, each sized for the largest packet plus SNR, RSSI,
  direction, and timestamp. Serves `packets` and `raw`.
- Event ring: 8 entries for room posts, events, and direct messages, so a burst
  of packets cannot evict a login event.

When a ring is full the oldest entry is dropped and a per-ring drop counter is
incremented. Both counters are reported in `room/stats`.

**Reused unchanged from the observer branch:** `MQTTPayloadBuilder`,
`MQTTMessageBuilder`, `MQTTTopicRouter`, `MQTTTopicTemplate`,
`MQTTPacketFilter`, `MQTTPresets`, `MQTTPrefsCodec`, `MQTTPrefsStorage`,
`MQTTPrefsAtomicStore`, `MQTTPrefsRecovery`, and `JWTHelper`. The two prefs
headers include `WiFi.h` only for a size constant; that include is guarded so
they compile on nRF52. `CommonCLI_Observer.cpp` is ESP32-specific throughout, so
the nRF52 build gets `CommonCLI_ObserverNRF52.cpp` implementing the subset in
section 1.3.

**`RoomLogPublisher`** in `src/helpers/`. Platform-neutral. Takes a publish
callback and formats the four room topics. The room server calls it from
`addPost`, from the login path in `onAnonDataRecv`, from the direct-message and
CLI branches of `onPeerDataRecv`, and on the status interval for stats. It has
no dependency on either bridge class so the ESP32 bridge can adopt it later.

**Time.** NTP sets the RTC on first broker connect and refreshes hourly. Posts
and events carry RTC time.

**Build target.** `RAK_4631_room_server_observer_mqtt_ethernet` in
`variants/rak4631/platformio.ini`, extending `RAK_4631_room_server_ethernet`
with `WITH_MQTT_BRIDGE`, `WITH_MQTT_BRIDGE_ETHERNET`, the new sources, and
lib deps PubSubClient, ArduinoJson 7.4.3, NTPClient, and Timezone. It is added
to the pull request build matrix.

### 1.2 Topics and payloads

Existing observer layout: `meshcore/{iata}/{device}/{type}` with types `status`,
`packets`, `raw`, `neighbors`. JSON for these comes from the reused payload
builder, so messages are indistinguishable from an ESP32 observer.

New topics under the same device prefix:

| Suffix | When | Retain | QoS |
|---|---|---|---|
| `room/posts` | every accepted post | no | 1 |
| `room/events` | login success, login failure, ACL change, admin CLI command | no | 1 |
| `room/dm` | direct message to the room identity that is not a post | no | 1 |
| `room/stats` | on the status interval | yes | 1 |
| `link` | on Ethernet link change and on the status interval | yes | 1 |
| `auth/{audience}` | on token renewal | yes | 1 |

`packets` and `raw` are QoS 0. Client ID is the device public key prefix, as the
observer uses.

**Post payload:** `timestamp`, `origin`, `origin_id`, `author_pubkey` (hex),
`author_name` (from the ACL entry, may be empty), `role` (`admin`,
`read_write`, `guest`), `text` (as stored, up to 151 bytes), `client_timestamp`.

**Event payload:** `timestamp`, `origin`, `origin_id`, `event` (one of
`login_ok`, `login_fail`, `acl_change`, `admin_cmd`), `actor_pubkey`,
`actor_name`, `result`, `detail`. For `admin_cmd`, `detail` is the command text.
The reply is never logged. Password attempts are never logged.

**Direct message payload:** same fields as a post plus `kind` (`plain`,
`cli_data`, `cli_command`).

**Room stats payload:** `timestamp`, `origin`, `origin_id`, `posts_total`
(since boot), `posts_unsynced`, `clients_admin`, `clients_read_write`,
`clients_guest`, `acl_bytes`, `packet_drops`, `event_drops`.

**Link payload:** `timestamp`, `origin`, `origin_id`, `state` (`up` or `down`),
`ip`, `gateway`, `mode` (`dhcp` or `static`), `mac`, `link_changes`,
`secs_since_change`, `broker_connected`, `broker_reconnects`.

**Auth payload:** `audience`, `username` (device public key hex), `token`
(Ed25519 JWT from `JWTHelper`), `iat`, `exp`. One retained message per
configured audience. The observer presents the public key as MQTT username and
the JWT as password; the proxy does the same upstream.

### 1.3 Configuration and CLI

Settings persist in `/mqtt_prefs` using the observer's codec, so the `MQTTPrefs`
struct stays shared. Unsupported settings answer
`ERR: not supported on this build` rather than being silently ignored.

**Kept from the observer:** `mqtt.iata`, `mqtt.origin`, `mqtt.owner`,
`mqtt.email`, `mqtt.interval`, `mqtt.rx`, `mqtt.tx`, `mqtt.raw`, `mqtt.packets`,
`mqtt.neighbors`, `mqtt.neighbors.interval`, `mqtt.ntp`, `mqtt.ntp.diag`,
`mqtt.status`, `mqtt.stats`, `mqtt.config.valid`, `mqtt.presets`,
`mqtt1.preset`, `mqtt1.server`, `mqtt1.port`, `mqtt1.username`,
`mqtt1.password`.

`mqtt1.preset` accepts only presets whose URL scheme is `mqtt://`. Selecting a
`wss://` or `mqtts://` preset returns
`ERR: preset needs TLS; route it through the LAN proxy`.

**New settings:**

| Setting | Default | Purpose |
|---|---|---|
| `mqtt.room.posts` | on | publish room posts |
| `mqtt.room.events` | on | publish login, ACL, and admin events |
| `mqtt.room.dm` | on | publish direct messages to the room |
| `mqtt.room.stats` | on | publish room stats on the status interval |
| `mqtt.auth.aud` | empty | comma-separated analyzer audiences to sign tokens for |
| `mqtt.auth.life` | 3300 | token lifetime in seconds; renewed at two thirds of life |

**Removed on this build:** all `wifi.*`, `snmp.*`, `alert.*`,
`mqtt.analyzer.us`, `mqtt.analyzer.eu`, and `mqtt2.*`.

**Reachability:** USB serial, the Ethernet telnet CLI on port 23, and the room's
admin path over LoRa, all through the existing `handleCommand`. Changes take
effect on the next broker reconnect.

**Build-flag defaults:** `MQTT_SERVER`, `MQTT_PORT`, `MQTT_USERNAME`,
`MQTT_PASSWORD` seed the prefs on first boot. CLI values override and persist.

### 1.4 Failure handling

- **Ethernet link down.** No connect attempts. Rings keep filling to their
  limits. On link return, a `link` message is published first, then the rings
  drain. Nothing in the radio path waits on the network.
- **Broker unreachable.** Exponential backoff from 5 s to 5 min with jitter.
- **NTP not synced.** Publishing proceeds with RTC time and `time_synced: false`
  in `status`, as the observer does before its first sync.
- **Prefs corrupt.** Fall back to build-flag defaults, report through
  `mqtt.config.valid`, never block boot. Reuses the observer's atomic store and
  recovery.
- **Publish failure.** The entry stays in its ring for one retry, then is
  dropped and counted.

### 1.5 Testing

- Native unit tests under `test/` for `RoomLogPublisher`, the nRF52 CLI subset,
  and the topic policy. Host-only, no hardware.
- **Build-size gate.** The first implementation task builds the new env and
  records flash and RAM. If free flash is under 10 percent the plan pauses to
  decide what to trim before feature work continues.
- Hardware verification on the gateway against local mosquitto: post from a
  phone client; confirm `room/posts`, `room/events`, and `status` arrive with the
  expected fields; pull and reconnect the cable; confirm `link` and ring
  behavior.
- Regression: all existing ESP32 observer envs build unchanged. The new env is
  added to `.github/workflows/pr-build-check.yml`.

## Part 2: LAN proxy

### 2.1 Overview

A single Go binary in `tools/mqtt-proxy`, shipped as a Docker image, configured
by one YAML file. It holds no device keys.

Internal packages: `config`, `local` (subscriber), `tokens` (cache),
`upstream` (session), `policy` (topic rules), `metrics`.

### 2.2 Inputs

Subscribes to the local broker on `meshcore/+/+/#`. Retained `auth/{audience}`
messages populate the token cache keyed by device and audience with token,
`iat`, `exp`, and username.

### 2.3 Upstreams

Each configured upstream names a preset from the same table the firmware uses,
or a custom URL, plus the audience it expects. JWT presets take credentials
from the token cache. User and password presets (meshrank, private brokers)
take them from YAML. Transport is WSS or mqtts per the preset, verified against
the system root certificates.

One upstream session per device per upstream, opened lazily on the first
forwardable message for that device, closed after an idle timeout. A device
with no cached token for an audience is logged once and skipped until a token
appears.

### 2.4 Topic policy

Allowlist of forwardable suffixes: `status`, `packets`, `raw`, `neighbors`.
Everything else, including `room/*`, `link`, and `auth/*`, is never forwarded.
Per-preset rules mirror the firmware's `MQTTTopicRouter`: meshrank receives no
`raw`, and its prefix is rewritten to `meshrank/uplink/{token}/{device}/`. The
policy package is table-driven and tested against the firmware's cases.

### 2.5 Delivery

Per-upstream bounded in-memory queue, oldest dropped when full, drops counted.
Reconnects use exponential backoff with jitter. When a cached token is within
its renewal buffer of expiry and a newer token is present, the session
reconnects with the newer token.

### 2.6 Failure handling

- **Local broker down.** `/healthz` reports unhealthy; Docker restarts the
  container; retained `auth`, `room/stats`, and `link` messages restore state on
  reconnect.
- **Upstream rejects token.** Logged with audience and expiry, session closed,
  no retry until a newer token arrives.
- **Upstream down.** Health stays healthy. Queue and backoff absorb it.
- **Malformed device message.** Dropped and counted, never forwarded, never
  crashes the subscriber loop.

### 2.7 Observability

Structured logs to stdout. `/healthz`. Prometheus `/metrics`: messages in and
out per device and upstream, drops, reconnects, token age, upstream connection
state.

### 2.8 Testing

- Unit tests for config parsing, topic policy, and token cache including expiry
  and replacement.
- Integration test that starts mosquitto in a container, publishes a synthetic
  device's messages, and asserts on what a fake upstream broker receives,
  including that no room topic ever appears.
- Compose smoke test: replay script or real device, proxy, and a test broker
  standing in for the analyzer.

## Out of scope

- The ESP32 observer adopting `RoomLogPublisher`.
- Any nRF52 board other than the RAK4631 with RAK13800.
- TLS on the device.
- Neighbor discovery over MQTT, which the observer gates on PSRAM.
- Repeater-role builds for this hardware.

## Known risks

- Flash and RAM headroom on the nRF52840 (815 KB flash, 235 KB RAM usable) is
  unmeasured. The build-size gate in 1.5 is the first task for that reason.
- Plaintext room posts on an unencrypted LAN broker is by design. Anyone with
  broker access reads the room.
- PubSubClient's default packet limit is 256 bytes; it must be raised at
  compile time to 1024 bytes, the observer's `MQTT_MAX_PACKET_SIZE`, to cover
  the largest `raw` message (a 255-byte packet is 510 hex characters plus JSON
  framing).

## Implementation order

1. Firmware: build-size gate, then bridge, then room log, then CLI, then tests.
2. Proxy: policy and token cache first, then sessions, then integration test.
