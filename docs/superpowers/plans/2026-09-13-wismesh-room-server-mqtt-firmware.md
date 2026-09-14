# WisMesh Room Server MQTT Observer Firmware Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the RAK WisMesh Ethernet Gateway (RAK4631 + RAK13800) run the MeshCore room server and publish observer-format metrics plus a full room log over plain MQTT on the LAN.

**Architecture:** A new `EthernetMQTTBridge` class stands in for the ESP32 `MQTTBridge` behind a one-line alias in `MQTTBridge.h`, so the room server, the alert reporter, and the shared CLI compile unchanged. It runs PubSubClient over the RAK13800's `EthernetClient` from the main loop with two fixed ring buffers, reuses the observer's JSON builders, and a new platform-neutral `RoomLogPublisher` formats the room topics. nRF52-only settings live in their own small prefs file so the fleet-critical `/mqtt_prefs` layout is untouched.

**Tech Stack:** PlatformIO, Adafruit nRF52 Arduino core (FreeRTOS), RAK13800-W5100S (Arduino Ethernet API), knolleary PubSubClient 2.8, ArduinoJson 7.4.3, NTPClient, googletest native tests.

**Spec:** `docs/superpowers/specs/2026-09-13-wismesh-room-server-mqtt-observer-design.md`

## Global Constraints

- Base branch is `observer-firmware`; work happens on `feat/nrf52-ethernet-observer`, already created from it.
- Hardware target: RAK4631 nRF52840 with RAK13800 W5100S only. Flash limit 815104 bytes, RAM limit 235520 bytes (from `boards/rak4631.json`).
- Build flags for the new env: `WITH_MQTT_BRIDGE=1` and `WITH_MQTT_BRIDGE_ETHERNET=1`. Every ESP32 observer env must keep building unchanged.
- `MQTT_MAX_PACKET_SIZE=1024` for PubSubClient, matching the observer.
- Topics: `meshcore/{iata}/{device}/{type}` for `status`, `packets`, `raw`, `neighbors`; new suffixes `room/posts`, `room/events`, `room/dm`, `room/stats`, `link`, `auth/{audience}`.
- QoS 0 for `packets` and `raw`; QoS 1 elsewhere. PubSubClient only sends QoS 0 publishes, so "QoS 1" topics are published with retain where the spec says retained and otherwise as QoS 0. Record this in the docs task.
- Never log the reply to an admin command, and never log a password attempt.
- Room log topics are never bridged upstream; that is the proxy's job and out of scope here.
- Native tests run with `pio test -e native -f <suite>`. Firmware builds with `pio run -e <env>`.
- Commit after every task with the attribution lines from the session reminder.
- The ClientInfo struct has no name field, so the spec's `author_name` and `actor_name` fields are omitted from the payloads. This is a known, accepted deviation.
- Reconnect backoff reuses `MQTTConnectionPolicy::reconnectBackoffMs`, whose ladder starts at 10 s and saturates at 5 min. The spec said 5 s; the shared ladder wins.

---

## File Structure

| Path | Responsibility |
|---|---|
| `src/helpers/bridges/MQTTBridge.h` (modify) | Top-of-file switch: under `WITH_MQTT_BRIDGE_ETHERNET` include the Ethernet bridge and alias `MQTTBridge` to it; else the existing ESP32 class. |
| `src/helpers/bridges/EthernetMQTTBridge.h/.cpp` (create) | nRF52 bridge: link check, PubSubClient connect and backoff, NTP, status, link, neighbors, packet and raw publishing from rings, auth token publishing, static CLI helpers, alert reporter hooks. |
| `src/helpers/MQTTEventRing.h` (create) | Fixed-capacity ring buffer template, drop-oldest, drop counter. Host-testable. |
| `src/helpers/MQTTEthernetPolicy.h` (create) | Pure policy: is a preset usable without TLS, is the config valid, audience list parsing, token renewal timing. Host-testable. |
| `src/helpers/EthernetObserverPrefs.h` (create) | `EthernetObserverPrefs` struct plus a byte codec with magic and version. Host-testable. |
| `src/helpers/EthernetObserverPrefs.cpp` (create) | Filesystem load and save of that struct at `/mqtt_eth_prefs`, plus the process-wide instance accessor. |
| `src/helpers/MQTTEthernetPayloads.h` (create) | JSON builders for `link` and `auth/{audience}` payloads. Host-testable. |
| `src/helpers/MQTTMessageBuilder.h/.cpp` (modify) | Add `buildPacketJSONFromRawAt`, `buildPacketJSONAt`, `buildRawJSONAt` that take an explicit clock; the existing functions delegate after calling `gettimeofday`. |
| `src/helpers/RoomLogPublisher.h/.cpp` (create) | Formats `room/posts`, `room/events`, `room/dm`, `room/stats` JSON and hands it to a sink callback. Host-testable. |
| `src/helpers/CommonCLI_ObserverNRF52.cpp` (create) | nRF52 implementation of `CommonCLI::handleObserverSetCmd`, `handleObserverGetCmd`, `handleObserverCommand`. |
| `examples/simple_room_server/MyMesh.h/.cpp` (modify) | Hooks for posts, logins, direct messages, ACL changes, room stats, neighbors source, bridge loop call, eth prefs load and save. |
| `variants/rak4631/platformio.ini` (modify) | New env `RAK_4631_room_server_observer_mqtt_ethernet`. |
| `platformio.ini` (modify) | Native env source filter gains the new host-testable `.cpp` files. |
| `test/test_mqtt_event_ring/`, `test/test_mqtt_ethernet_policy/`, `test/test_ethernet_observer_prefs/`, `test/test_mqtt_ethernet_payloads/`, `test/test_room_log_publisher/` (create) | googletest suites. |
| `.github/workflows/pr-build-check.yml` (modify) | Add the new env to the matrix. |
| `docs/cli_commands.md`, `MQTT_IMPLEMENTATION.md` (modify) | Document the build and its CLI subset. |

---

### Task 1: Bridge alias switch, bridge skeleton, build env, and size gate

**Files:**
- Modify: `src/helpers/bridges/MQTTBridge.h:1-2` and the last line
- Create: `src/helpers/bridges/EthernetMQTTBridge.h`
- Create: `src/helpers/bridges/EthernetMQTTBridge.cpp`
- Modify: `variants/rak4631/platformio.ini` (after `[env:RAK_4631_room_server_ethernet]`)

**Interfaces:**
- Produces: class `EthernetMQTTBridge` with the full public surface below. Every later task fills in bodies; the names and signatures here are final.
- Produces: build env `RAK_4631_room_server_observer_mqtt_ethernet`.

- [ ] **Step 1: Add the platform switch to `MQTTBridge.h`**

Replace the first line `#pragma once` with:

```cpp
#pragma once

#if defined(WITH_MQTT_BRIDGE_ETHERNET)
// nRF52 + RAK13800 build: the Ethernet bridge stands in for MQTTBridge so the
// room server, AlertReporter, and CommonCLI compile against one name.
#include "helpers/bridges/EthernetMQTTBridge.h"
using MQTTBridge = EthernetMQTTBridge;
#else
```

Append to the very end of the file:

```cpp
#endif  // WITH_MQTT_BRIDGE_ETHERNET
```

- [ ] **Step 2: Create `src/helpers/bridges/EthernetMQTTBridge.h`**

```cpp
#pragma once

#include "MeshCore.h"
#include "helpers/bridges/BridgeBase.h"
#include "helpers/CommonCLI.h"
#include "helpers/MQTTEventRing.h"
#include "helpers/MQTTPayloadBuilder.h"
#include "helpers/EthernetObserverPrefs.h"
#include <ArduinoJson.h>
#include <RAK13800_W5100S.h>
#include <PubSubClient.h>

class MeshSNMPAgent;  // never instantiated on this build

#if defined(MQTT_DEBUG)
  #define ETH_MQTT_LOG(F, ...) do { if (Serial.availableForWrite() > 0) { Serial.printf("MQTT: " F "\n", ##__VA_ARGS__); } } while (0)
#else
  #define ETH_MQTT_LOG(...) {}
#endif

#ifndef MQTT_ETH_PACKET_RING
  #define MQTT_ETH_PACKET_RING 16
#endif
#ifndef MQTT_ETH_EVENT_RING
  #define MQTT_ETH_EVENT_RING 8
#endif

/**
 * MQTT observer bridge for nRF52 boards with a RAK13800 (W5100S) Ethernet
 * module. Plain MQTT only. Runs entirely from the main loop: no task, no
 * RTOS queue. Publishing is at most one message per loop() pass.
 */
class EthernetMQTTBridge : public BridgeBase {
public:
  // ---- Types shared by name with the ESP32 MQTTBridge ----
  enum NeighborsPhase : uint8_t { NBR_SCHEDULED, NBR_ACTIVE, NBR_DUE };

  struct SlotStatusSnapshot {
    const char* name;    // preset name or "custom"
    const char* state;   // "inactive" | "wait" | "ok" | "fail" | "disc"
    unsigned long publish_ok;
    unsigned long publish_err;
    uint16_t filter_mask;
  };

  static const size_t NEIGHBORS_JSON_BUFFER_SIZE = 2048;
  static const size_t NEIGHBORS_DOC_POOL_BUDGET = 4096;
  static const int NEIGHBORS_MAX_PUBLISH_ENTRIES = 20;

  /** Room server implements this to hand the bridge its neighbour table. */
  struct NeighborSource {
    virtual int collectNeighbors(MQTTPayloadBuilder::NeighborsMessageEntry* out,
                                 int max_entries, char* hex_pool, size_t hex_pool_size,
                                 uint32_t now_secs) = 0;
  };

  EthernetMQTTBridge(NodePrefs* prefs, MQTTPrefs* obs, mesh::PacketManager* mgr,
                     mesh::RTCClock* rtc, mesh::LocalIdentity* identity);

  // ---- AbstractBridge ----
  void begin() override;
  void end() override;
  void loop() override;
  void onPacketReceived(mesh::Packet* packet) override;
  void sendPacket(mesh::Packet* packet) override;

  // ---- Surface the room server, AlertReporter, and CLI already use ----
  void storeRawRadioData(const uint8_t* raw_data, int len, float snr, float rssi);
  void setDeviceID(const char* device_id);
  void setFirmwareVersion(const char* firmware_version);
  void setBoardModel(const char* board_model);
  void setBuildDate(const char* build_date);
  void setStatsSources(mesh::Dispatcher* dispatcher, mesh::Radio* radio,
                       mesh::MainBoard* board, mesh::MillisecondClock* ms);
  void setSlotPreset(int slot_index, const char* preset_name);
  void setSNMPAgent(MeshSNMPAgent*) {}
  bool canFlashAfterStop() const { return true; }
  int getQueueSize() const;
  bool requestForcedNtpSync(uint32_t timeout_ms = 30000);
  bool ntpDiag(char* reply, size_t reply_size, bool verbose);
  void setNeighborsSchedule(NeighborsPhase, uint32_t) {}
  void requestPublishNeighbors(const char* json, size_t len);

  static int getMaxActiveSlots() { return 1; }
  static bool isConfigValid(const MQTTPrefs* obs);
  static void getEffectiveMqttOrigin(const NodePrefs* np, const MQTTPrefs* obs, char* buf, size_t buf_size);
  static void formatMqttStatusReply(char* buf, size_t bufsize, const MQTTPrefs* obs);
  static void formatMqttStatsReply(char* buf, size_t bufsize);
  static bool getSlotStatusSnapshot(int slot_index, SlotStatusSnapshot* out);
  static unsigned long getLastWifiDisconnectTime() { return 0; }
  static unsigned long getWifiConnectedAtMillis() { return 0; }
  unsigned long getSlotCurrentOutageStartMs(int slot_index) const;
  bool isSlotEnabledAndAttempted(int slot_index) const;
  const char* getSlotPresetName(int slot_index) const;

  // ---- Ethernet-build additions ----
  void setNeighborSource(NeighborSource* src) { _neighbor_source = src; }
  void setEthernetPrefs(EthernetObserverPrefs* eth) { _eth = eth; }
  /** Sink for RoomLogPublisher: queues JSON on the event ring. */
  bool publishRoomTopic(const char* suffix, const char* json, size_t len, bool retain);
  /** Called from loop() by the room server every status interval. */
  void publishRoomStatsNow();
  uint32_t packetDrops() const { return _packet_ring.drops(); }
  uint32_t eventDrops() const { return _event_ring.drops(); }
  uint32_t brokerReconnects() const { return _broker_reconnects; }
  bool brokerConnected() const { return _client.connected(); }
  bool timeSynced() const { return _time_synced; }

  struct PacketEntry {
    uint8_t blob[MAX_TRANS_UNIT];   // packet bytes (writeTo) for both rx and tx
    uint8_t blob_len;
    uint8_t raw[MAX_TRANS_UNIT];    // radio frame for rx; unused for tx
    uint8_t raw_len;
    bool is_tx;
    float snr;
    float rssi;
    uint32_t rtc_secs;
  };
  struct EventEntry {
    char suffix[16];
    bool retain;
    uint16_t len;
    char json[512];
  };

private:
  MQTTPrefs* _obs;
  EthernetObserverPrefs* _eth;
  mesh::LocalIdentity* _identity;
  mesh::Dispatcher* _dispatcher;
  mesh::Radio* _radio;
  mesh::MainBoard* _board;
  mesh::MillisecondClock* _ms;
  NeighborSource* _neighbor_source;

  EthernetClient _net;
  PubSubClient _client;

  char _device_id[65];
  char _firmware_version[32];
  char _board_model[32];
  char _build_date[24];
  char _origin[32];
  char _iata[8];
  char _client_id[24];

  // link + broker state
  bool _link_up;
  unsigned long _link_changed_at;
  uint32_t _link_changes;
  unsigned long _last_connect_attempt;
  uint8_t _backoff;
  uint32_t _broker_reconnects;
  unsigned long _outage_started_at;
  unsigned long _publish_ok, _publish_err;
  const char* _slot_state;  // "inactive" | "wait" | "ok" | "fail" | "disc"

  // timing
  bool _time_synced;
  unsigned long _next_status_at;
  unsigned long _next_ntp_at;
  unsigned long _next_neighbors_at;
  unsigned long _next_link_at;
  unsigned long _next_auth_at;

  // staged raw for the next onPacketReceived()
  uint8_t _staged_raw[MAX_TRANS_UNIT];
  uint8_t _staged_raw_len;
  float _staged_snr, _staged_rssi;
  bool _staged_valid;

  MQTTEventRing<PacketEntry, MQTT_ETH_PACKET_RING> _packet_ring;
  MQTTEventRing<EventEntry, MQTT_ETH_EVENT_RING> _event_ring;
  bool _packet_retry_pending;
  bool _event_retry_pending;

  JsonDocument _doc;
  char _json[1024];

  static EthernetMQTTBridge* s_instance;

  bool linkReady() const;
  void refreshLink(unsigned long now);
  bool connectBroker(unsigned long now);
  void onDisconnected(unsigned long now);
  bool topicFor(const char* suffix, char* buf, size_t buf_size) const;
  void formatTimestamp(char* buf, size_t buf_size) const;
  bool publishStatus();
  bool publishLink();
  bool publishNeighbors();
  bool publishAuthTokens();
  bool drainPacketRing();
  bool drainEventRing();
  bool syncNtp(uint32_t timeout_ms);
};
```

- [ ] **Step 3: Create `src/helpers/bridges/EthernetMQTTBridge.cpp` as a compiling skeleton**

Every method exists; bodies that later tasks fill are empty or return a fixed value. This is enough to link and measure size.

```cpp
#include "EthernetMQTTBridge.h"
#include "helpers/MQTTMessageBuilder.h"
#include "helpers/MQTTEthernetPolicy.h"
#include "helpers/MQTTEthernetPayloads.h"
#include "helpers/MQTTTopicRouter.h"
#include "helpers/MQTTPresets.h"
#include "helpers/MQTTConnectionPolicy.h"
#include "helpers/JWTHelper.h"
#include <EthernetUdp.h>
#include <NTPClient.h>
#include <malloc.h>
#include <string.h>

EthernetMQTTBridge* EthernetMQTTBridge::s_instance = nullptr;

EthernetMQTTBridge::EthernetMQTTBridge(NodePrefs* prefs, MQTTPrefs* obs, mesh::PacketManager* mgr,
                                       mesh::RTCClock* rtc, mesh::LocalIdentity* identity)
  : BridgeBase(prefs, mgr, rtc), _obs(obs), _eth(nullptr), _identity(identity),
    _dispatcher(nullptr), _radio(nullptr), _board(nullptr), _ms(nullptr), _neighbor_source(nullptr),
    _client(_net), _link_up(false), _link_changed_at(0), _link_changes(0),
    _last_connect_attempt(0), _backoff(0), _broker_reconnects(0), _outage_started_at(0),
    _publish_ok(0), _publish_err(0), _slot_state("inactive"), _time_synced(false),
    _next_status_at(0), _next_ntp_at(0), _next_neighbors_at(0), _next_link_at(0), _next_auth_at(0),
    _staged_raw_len(0), _staged_snr(0), _staged_rssi(0), _staged_valid(false),
    _packet_retry_pending(false), _event_retry_pending(false) {
  _device_id[0] = _firmware_version[0] = _board_model[0] = _build_date[0] = 0;
  _origin[0] = _iata[0] = _client_id[0] = 0;
  s_instance = this;
}

void EthernetMQTTBridge::begin() { _initialized = true; }
void EthernetMQTTBridge::end() { _client.disconnect(); _initialized = false; }
void EthernetMQTTBridge::loop() {}
void EthernetMQTTBridge::onPacketReceived(mesh::Packet*) {}
void EthernetMQTTBridge::sendPacket(mesh::Packet*) {}
void EthernetMQTTBridge::storeRawRadioData(const uint8_t*, int, float, float) {}

void EthernetMQTTBridge::setDeviceID(const char* v) { strncpy(_device_id, v, sizeof(_device_id) - 1); _device_id[sizeof(_device_id) - 1] = 0; }
void EthernetMQTTBridge::setFirmwareVersion(const char* v) { strncpy(_firmware_version, v, sizeof(_firmware_version) - 1); _firmware_version[sizeof(_firmware_version) - 1] = 0; }
void EthernetMQTTBridge::setBoardModel(const char* v) { strncpy(_board_model, v, sizeof(_board_model) - 1); _board_model[sizeof(_board_model) - 1] = 0; }
void EthernetMQTTBridge::setBuildDate(const char* v) { strncpy(_build_date, v, sizeof(_build_date) - 1); _build_date[sizeof(_build_date) - 1] = 0; }
void EthernetMQTTBridge::setStatsSources(mesh::Dispatcher* d, mesh::Radio* r, mesh::MainBoard* b, mesh::MillisecondClock* ms) {
  _dispatcher = d; _radio = r; _board = b; _ms = ms;
}
void EthernetMQTTBridge::setSlotPreset(int, const char*) {}
int EthernetMQTTBridge::getQueueSize() const { return (int)(_packet_ring.size() + _event_ring.size()); }
bool EthernetMQTTBridge::requestForcedNtpSync(uint32_t) { return false; }
bool EthernetMQTTBridge::ntpDiag(char* reply, size_t reply_size, bool) { snprintf(reply, reply_size, "NTP: not implemented"); return false; }
void EthernetMQTTBridge::requestPublishNeighbors(const char*, size_t) {}

bool EthernetMQTTBridge::isConfigValid(const MQTTPrefs*) { return false; }
void EthernetMQTTBridge::getEffectiveMqttOrigin(const NodePrefs* np, const MQTTPrefs* obs, char* buf, size_t buf_size) {
  const char* src = (obs && obs->mqtt_origin[0]) ? obs->mqtt_origin : (np ? np->node_name : "");
  strncpy(buf, src, buf_size - 1); buf[buf_size - 1] = 0;
}
void EthernetMQTTBridge::formatMqttStatusReply(char* buf, size_t bufsize, const MQTTPrefs*) { snprintf(buf, bufsize, "> MQTT: skeleton"); }
void EthernetMQTTBridge::formatMqttStatsReply(char* buf, size_t bufsize) { snprintf(buf, bufsize, "> MQTT: skeleton"); }
bool EthernetMQTTBridge::getSlotStatusSnapshot(int, SlotStatusSnapshot*) { return false; }
unsigned long EthernetMQTTBridge::getSlotCurrentOutageStartMs(int) const { return _outage_started_at; }
bool EthernetMQTTBridge::isSlotEnabledAndAttempted(int slot_index) const { return slot_index == 0 && _initialized; }
const char* EthernetMQTTBridge::getSlotPresetName(int) const { return _obs ? _obs->mqtt_slot_preset[0] : ""; }

bool EthernetMQTTBridge::publishRoomTopic(const char*, const char*, size_t, bool) { return false; }
void EthernetMQTTBridge::publishRoomStatsNow() {}

bool EthernetMQTTBridge::linkReady() const { return false; }
void EthernetMQTTBridge::refreshLink(unsigned long) {}
bool EthernetMQTTBridge::connectBroker(unsigned long) { return false; }
void EthernetMQTTBridge::onDisconnected(unsigned long) {}
bool EthernetMQTTBridge::topicFor(const char*, char*, size_t) const { return false; }
void EthernetMQTTBridge::formatTimestamp(char* buf, size_t buf_size) const { if (buf_size) buf[0] = 0; }
bool EthernetMQTTBridge::publishStatus() { return false; }
bool EthernetMQTTBridge::publishLink() { return false; }
bool EthernetMQTTBridge::publishNeighbors() { return false; }
bool EthernetMQTTBridge::publishAuthTokens() { return false; }
bool EthernetMQTTBridge::drainPacketRing() { return false; }
bool EthernetMQTTBridge::drainEventRing() { return false; }
bool EthernetMQTTBridge::syncNtp(uint32_t) { return false; }
```

The skeleton includes headers created in Tasks 2 through 5. To make Task 1 build on its own, create those four headers now as minimal stubs that the later tasks replace entirely:

`src/helpers/MQTTEventRing.h`:
```cpp
#pragma once
#include <stddef.h>
#include <stdint.h>
template <typename T, size_t N> class MQTTEventRing {
public:
  size_t size() const { return 0; }
  uint32_t drops() const { return 0; }
};
```

`src/helpers/MQTTEthernetPolicy.h`, `src/helpers/MQTTEthernetPayloads.h`: a bare `#pragma once`.

`src/helpers/EthernetObserverPrefs.h`:
```cpp
#pragma once
#include <stdint.h>
struct EthernetObserverPrefs { uint8_t placeholder; };
```

- [ ] **Step 4: Add the env to `variants/rak4631/platformio.ini`** directly after the `[env:RAK_4631_room_server_ethernet]` block

```ini
[env:RAK_4631_room_server_observer_mqtt_ethernet]
extends = rak4631
build_flags =
  ${rak4631.build_flags}
  -D DISPLAY_CLASS=SSD1306Display
  -D ADVERT_NAME='"RAK4631 Room Observer"'
  -D ADVERT_LAT=0.0
  -D ADVERT_LON=0.0
  -D ADMIN_PASSWORD='"password"'
  -D ROOM_PASSWORD='"hello"'
  -D MAX_NEIGHBOURS=20
  -D ETHERNET_ENABLED=1
  -D WITH_MQTT_BRIDGE=1
  -D WITH_MQTT_BRIDGE_ETHERNET=1
  -D MQTT_MAX_PACKET_SIZE=1024
  -D MQTT_SOCKET_TIMEOUT=2
  -D MQTT_DEFAULT_SLOT1_PRESET='"custom"'
  -D MQTT_DEFAULT_SLOT2_PRESET='"none"'
  -D MQTT_DEBUG=1
;  -D MQTT_SERVER='"192.168.1.10"'
;  -D MQTT_PORT=1883
;  -D MQTT_USERNAME='"user"'
;  -D MQTT_PASSWORD='"pass"'
build_src_filter = ${rak4631.build_src_filter}
  -<helpers/CommonCLI_Observer.cpp>
  +<helpers/CommonCLI_ObserverNRF52.cpp>
  +<helpers/MQTTMessageBuilder.cpp>
  +<helpers/JWTHelper.cpp>
  +<helpers/bridges/EthernetMQTTBridge.cpp>
  +<helpers/RoomLogPublisher.cpp>
  +<helpers/EthernetObserverPrefs.cpp>
  +<helpers/ui/SSD1306Display.cpp>
  +<../examples/simple_room_server>
lib_deps =
  ${rak4631.lib_deps}
  https://github.com/RAKWireless/RAK13800-W5100S/archive/1.0.2.zip
  knolleary/PubSubClient@^2.8
  bblanchon/ArduinoJson @ 7.4.3
  arduino-libraries/NTPClient
  JChristensen/Timezone
  paulstoffregen/Time@1.6.1
```

For this task only, so the env links before Tasks 8, 5, and 12 exist, create empty placeholder files `src/helpers/RoomLogPublisher.cpp`, `src/helpers/EthernetObserverPrefs.cpp`, and `src/helpers/CommonCLI_ObserverNRF52.cpp` each containing just a comment line `// filled in by a later task`. Because `CommonCLI_Observer.cpp` is excluded, the three `CommonCLI::handleObserver*` members must exist for the link to succeed. Put these temporary definitions in `CommonCLI_ObserverNRF52.cpp` now; Task 12 replaces the file:

```cpp
#include <Arduino.h>
#include "CommonCLI.h"
bool CommonCLI::handleObserverSetCmd(uint32_t, const char*, char* reply) { strcpy(reply, "ERR: not supported on this build"); return true; }
bool CommonCLI::handleObserverGetCmd(uint32_t, const char*, char* reply) { strcpy(reply, "ERR: not supported on this build"); return true; }
bool CommonCLI::handleObserverCommand(uint32_t, char*, char*) { return false; }
```

- [ ] **Step 5: Build and record size**

Run: `pio run -e RAK_4631_room_server_observer_mqtt_ethernet 2>&1 | tail -30`
Expected: `SUCCESS`, with lines like `RAM: [=== ] 34.1% (used 80312 bytes from 235520 bytes)` and `Flash: [==== ] 61.0% (used 497120 bytes from 815104 bytes)`. Copy both lines into the commit message.

If the build fails on a symbol the room server calls that the skeleton lacks, add it to the skeleton header and cpp with the same fixed-value style, then rebuild. Do not change `MyMesh.cpp` in this task.

**Gate:** if flash used is above 90 percent, stop and report to the user before continuing; the plan's later tasks add roughly 25 to 40 KB. If below, continue.

- [ ] **Step 6: Confirm an ESP32 observer env still builds**

Run: `pio run -e Heltec_v3_repeater_observer_mqtt 2>&1 | tail -5`
Expected: `SUCCESS`.

- [ ] **Step 7: Commit**

```bash
git add src/helpers/bridges/MQTTBridge.h src/helpers/bridges/EthernetMQTTBridge.h src/helpers/bridges/EthernetMQTTBridge.cpp \
  src/helpers/MQTTEventRing.h src/helpers/MQTTEthernetPolicy.h src/helpers/MQTTEthernetPayloads.h \
  src/helpers/EthernetObserverPrefs.h src/helpers/EthernetObserverPrefs.cpp src/helpers/RoomLogPublisher.cpp \
  src/helpers/CommonCLI_ObserverNRF52.cpp variants/rak4631/platformio.ini
git commit -m "feat(nrf52): add EthernetMQTTBridge skeleton and RAK4631 room server observer env

Size gate: <paste RAM line> / <paste Flash line>"
```

---

### Task 2: MQTTEventRing

**Files:**
- Replace: `src/helpers/MQTTEventRing.h`
- Create: `test/test_mqtt_event_ring/test_mqtt_event_ring.cpp`

**Interfaces:**
- Produces: `template <typename T, size_t N> class MQTTEventRing` with `bool push(const T&)` (returns true when it had to drop the oldest), `T* front()`, `const T* front() const`, `void pop()`, `size_t size() const`, `bool empty() const`, `uint32_t drops() const`, `static const size_t capacity`.

- [ ] **Step 1: Write the failing test**

```cpp
#include <gtest/gtest.h>
#include "helpers/MQTTEventRing.h"

struct Item { int v; };

TEST(MQTTEventRing, StartsEmpty) {
  MQTTEventRing<Item, 3> ring;
  EXPECT_TRUE(ring.empty());
  EXPECT_EQ(0u, ring.size());
  EXPECT_EQ(nullptr, ring.front());
  EXPECT_EQ(0u, ring.drops());
}

TEST(MQTTEventRing, PushPopIsFifo) {
  MQTTEventRing<Item, 3> ring;
  EXPECT_FALSE(ring.push({1}));
  EXPECT_FALSE(ring.push({2}));
  EXPECT_EQ(2u, ring.size());
  EXPECT_EQ(1, ring.front()->v);
  ring.pop();
  EXPECT_EQ(2, ring.front()->v);
  ring.pop();
  EXPECT_TRUE(ring.empty());
}

TEST(MQTTEventRing, FullRingDropsOldestAndCounts) {
  MQTTEventRing<Item, 3> ring;
  ring.push({1}); ring.push({2}); ring.push({3});
  EXPECT_TRUE(ring.push({4}));
  EXPECT_EQ(3u, ring.size());
  EXPECT_EQ(1u, ring.drops());
  EXPECT_EQ(2, ring.front()->v);
  ring.pop(); ring.pop(); ring.pop();
  EXPECT_TRUE(ring.empty());
  EXPECT_EQ(nullptr, ring.front());
}

TEST(MQTTEventRing, PopOnEmptyIsHarmless) {
  MQTTEventRing<Item, 2> ring;
  ring.pop();
  EXPECT_TRUE(ring.empty());
  EXPECT_EQ(0u, ring.drops());
}

TEST(MQTTEventRing, FrontIsWritable) {
  MQTTEventRing<Item, 2> ring;
  ring.push({7});
  ring.front()->v = 9;
  EXPECT_EQ(9, ring.front()->v);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pio test -e native -f test_mqtt_event_ring 2>&1 | tail -15`
Expected: compile error, `no member named 'push'`.

- [ ] **Step 3: Write the implementation**

```cpp
#pragma once

#include <stddef.h>
#include <stdint.h>

/**
 * Fixed-capacity FIFO. When full, push() overwrites the oldest entry and
 * counts a drop. Single-threaded: the bridge fills and drains it from the
 * main loop only.
 */
template <typename T, size_t N>
class MQTTEventRing {
public:
  static const size_t capacity = N;

  MQTTEventRing() : _head(0), _count(0), _drops(0) {}

  /** Returns true when an older entry was discarded to make room. */
  bool push(const T& item) {
    bool dropped = false;
    if (_count == N) {
      _head = (_head + 1) % N;
      _count--;
      _drops++;
      dropped = true;
    }
    _items[(_head + _count) % N] = item;
    _count++;
    return dropped;
  }

  T* front() { return _count ? &_items[_head] : nullptr; }
  const T* front() const { return _count ? &_items[_head] : nullptr; }

  void pop() {
    if (_count == 0) return;
    _head = (_head + 1) % N;
    _count--;
  }

  size_t size() const { return _count; }
  bool empty() const { return _count == 0; }
  uint32_t drops() const { return _drops; }

private:
  T _items[N];
  size_t _head;
  size_t _count;
  uint32_t _drops;
};
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pio test -e native -f test_mqtt_event_ring 2>&1 | tail -5`
Expected: `5 test cases: 5 succeeded`.

- [ ] **Step 5: Commit**

```bash
git add src/helpers/MQTTEventRing.h test/test_mqtt_event_ring/test_mqtt_event_ring.cpp
git commit -m "feat(mqtt): add fixed-capacity MQTTEventRing with drop counting"
```

---

### Task 3: MQTTEthernetPolicy

**Files:**
- Replace: `src/helpers/MQTTEthernetPolicy.h`
- Create: `test/test_mqtt_ethernet_policy/test_mqtt_ethernet_policy.cpp`

**Interfaces:**
- Produces, all in `namespace MQTTEthernetPolicy`:
  - `bool presetUsableWithoutTls(const char* server_url)` true only for `mqtt://`.
  - `bool slotConfigValid(const char* preset, const char* host, uint16_t port, const char* iata, const char* device_id)`.
  - `int parseAudiences(const char* csv, char (*out)[64], int max_out)` splits on commas, trims spaces, returns count.
  - `bool tokenRenewalDue(uint32_t now_secs, uint32_t issued_at, uint32_t life_secs)` true at or past two thirds of life.
  - `uint32_t linkPublishIntervalMs()` returns the status interval fallback used for `link`, 300000.

- [ ] **Step 1: Write the failing test**

```cpp
#include <gtest/gtest.h>
#include "helpers/MQTTEthernetPolicy.h"

namespace P = MQTTEthernetPolicy;

TEST(MQTTEthernetPolicy, OnlyPlainMqttSchemeIsUsable) {
  EXPECT_TRUE(P::presetUsableWithoutTls("mqtt://mqtt.tennmesh.com:1883"));
  EXPECT_FALSE(P::presetUsableWithoutTls("mqtts://meshrank.net:8883"));
  EXPECT_FALSE(P::presetUsableWithoutTls("wss://mqtt-us-v1.letsmesh.net:443/mqtt"));
  EXPECT_FALSE(P::presetUsableWithoutTls(""));
  EXPECT_FALSE(P::presetUsableWithoutTls(nullptr));
}

TEST(MQTTEthernetPolicy, CustomSlotNeedsHostPortIataDevice) {
  EXPECT_TRUE(P::slotConfigValid("custom", "192.168.1.10", 1883, "SEA", "abcd"));
  EXPECT_FALSE(P::slotConfigValid("custom", "", 1883, "SEA", "abcd"));
  EXPECT_FALSE(P::slotConfigValid("custom", "192.168.1.10", 0, "SEA", "abcd"));
  EXPECT_FALSE(P::slotConfigValid("custom", "192.168.1.10", 1883, "", "abcd"));
  EXPECT_FALSE(P::slotConfigValid("custom", "192.168.1.10", 1883, "SEA", ""));
  EXPECT_FALSE(P::slotConfigValid("none", "192.168.1.10", 1883, "SEA", "abcd"));
}

TEST(MQTTEthernetPolicy, NamedPresetNeedsIataAndDeviceOnly) {
  EXPECT_TRUE(P::slotConfigValid("tennmesh", "", 0, "SEA", "abcd"));
  EXPECT_FALSE(P::slotConfigValid("tennmesh", "", 0, "", "abcd"));
}

TEST(MQTTEthernetPolicy, ParsesCommaSeparatedAudiences) {
  char out[4][64];
  EXPECT_EQ(2, P::parseAudiences("mqtt-us-v1.letsmesh.net, mqtt.meshmapper.net", out, 4));
  EXPECT_STREQ("mqtt-us-v1.letsmesh.net", out[0]);
  EXPECT_STREQ("mqtt.meshmapper.net", out[1]);
  EXPECT_EQ(0, P::parseAudiences("", out, 4));
  EXPECT_EQ(0, P::parseAudiences(nullptr, out, 4));
  EXPECT_EQ(1, P::parseAudiences(",,a,,", out, 4));
  EXPECT_STREQ("a", out[0]);
  EXPECT_EQ(2, P::parseAudiences("a,b,c", out, 2));
}

TEST(MQTTEthernetPolicy, RenewalAtTwoThirdsOfLife) {
  EXPECT_FALSE(P::tokenRenewalDue(1000 + 2199, 1000, 3300));
  EXPECT_TRUE(P::tokenRenewalDue(1000 + 2200, 1000, 3300));
  EXPECT_TRUE(P::tokenRenewalDue(5000, 0, 3300));   // never issued
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pio test -e native -f test_mqtt_ethernet_policy 2>&1 | tail -15`
Expected: compile error, `MQTTEthernetPolicy` has no member `presetUsableWithoutTls`.

- [ ] **Step 3: Write the implementation**

```cpp
#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string.h>

// Pure policy for the nRF52 Ethernet MQTT bridge. No Arduino, no I/O.
namespace MQTTEthernetPolicy {

static const uint32_t kLinkPublishIntervalMs = 300000UL;

static inline bool presetUsableWithoutTls(const char* server_url) {
  return server_url != nullptr && strncmp(server_url, "mqtt://", 7) == 0;
}

static inline bool slotConfigValid(const char* preset, const char* host, uint16_t port,
                                   const char* iata, const char* device_id) {
  if (!preset || !iata || !device_id) return false;
  if (iata[0] == '\0' || device_id[0] == '\0') return false;
  if (strcmp(preset, "none") == 0) return false;
  if (strcmp(preset, "custom") == 0) {
    return host != nullptr && host[0] != '\0' && port != 0;
  }
  return true;
}

static inline int parseAudiences(const char* csv, char (*out)[64], int max_out) {
  if (!csv || !out || max_out <= 0) return 0;
  int n = 0;
  const char* p = csv;
  while (*p && n < max_out) {
    while (*p == ' ' || *p == ',') p++;
    if (!*p) break;
    const char* start = p;
    while (*p && *p != ',') p++;
    const char* end = p;
    while (end > start && end[-1] == ' ') end--;
    size_t len = (size_t)(end - start);
    if (len == 0) continue;
    if (len > 63) len = 63;
    memcpy(out[n], start, len);
    out[n][len] = '\0';
    n++;
  }
  return n;
}

static inline bool tokenRenewalDue(uint32_t now_secs, uint32_t issued_at, uint32_t life_secs) {
  if (issued_at == 0) return true;
  uint32_t renew_after = (life_secs / 3) * 2;
  return now_secs >= issued_at + renew_after;
}

static inline uint32_t linkPublishIntervalMs() { return kLinkPublishIntervalMs; }

}  // namespace MQTTEthernetPolicy
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pio test -e native -f test_mqtt_ethernet_policy 2>&1 | tail -5`
Expected: `5 test cases: 5 succeeded`.

- [ ] **Step 5: Commit**

```bash
git add src/helpers/MQTTEthernetPolicy.h test/test_mqtt_ethernet_policy/test_mqtt_ethernet_policy.cpp
git commit -m "feat(mqtt): add pure Ethernet observer policy helpers"
```

---

### Task 4: Explicit-clock overloads in MQTTMessageBuilder

**Files:**
- Modify: `src/helpers/MQTTMessageBuilder.h:208-258`
- Modify: `src/helpers/MQTTMessageBuilder.cpp:150-300`

**Interfaces:**
- Produces three new static methods with the same parameters as the existing ones plus `time_t now_sec, long now_usec` inserted before `char* buffer`:
  - `int buildPacketJSONAt(JsonDocument&, mesh::Packet*, bool is_tx, const char* origin, const char* origin_id, Timezone*, time_t now_sec, long now_usec, char* buffer, size_t buffer_size)`
  - `int buildPacketJSONFromRawAt(JsonDocument&, const uint8_t* raw_data, int raw_len, mesh::Packet*, bool is_tx, const char* origin, const char* origin_id, float snr, float rssi, float score, Timezone*, time_t now_sec, long now_usec, char* buffer, size_t buffer_size)`
  - `int buildRawJSONAt(JsonDocument&, mesh::Packet*, const char* origin, const char* origin_id, Timezone*, time_t now_sec, long now_usec, char* buffer, size_t buffer_size)`
- The existing three functions become one-line wrappers: call `gettimeofday`, then the `At` variant. Output is byte-identical.

- [ ] **Step 1: Declare the overloads in the header**

After each existing declaration of `buildPacketJSON`, `buildPacketJSONFromRaw`, and `buildRawJSON`, add the matching `...At` declaration with `time_t now_sec, long now_usec,` inserted immediately before `char* buffer,`. Add `#include <time.h>` at the top of the header if not present.

- [ ] **Step 2: Move the bodies**

In `MQTTMessageBuilder.cpp`, rename each existing body to its `At` name, add the two clock parameters, and delete the `gettimeofday` block, replacing

```cpp
  struct timeval now_tv;
  gettimeofday(&now_tv, nullptr);
  time_t now = now_tv.tv_sec;
  char timestamp[40];
  formatIsoTimestampForMqtt(now, now_tv.tv_usec, timezone, timestamp, sizeof(timestamp));
```

with

```cpp
  time_t now = now_sec;
  char timestamp[40];
  formatIsoTimestampForMqtt(now, now_usec, timezone, timestamp, sizeof(timestamp));
```

Then add the three wrappers, for example:

```cpp
int MQTTMessageBuilder::buildPacketJSONFromRaw(
  JsonDocument& doc, const uint8_t* raw_data, int raw_len, mesh::Packet* packet, bool is_tx,
  const char* origin, const char* origin_id, float snr, float rssi, float score,
  Timezone* timezone, char* buffer, size_t buffer_size
) {
  struct timeval now_tv;
  gettimeofday(&now_tv, nullptr);
  return buildPacketJSONFromRawAt(doc, raw_data, raw_len, packet, is_tx, origin, origin_id,
                                  snr, rssi, score, timezone, now_tv.tv_sec, now_tv.tv_usec,
                                  buffer, buffer_size);
}
```

Write the `buildPacketJSON` and `buildRawJSON` wrappers the same way.

- [ ] **Step 3: Verify both platforms build and existing tests pass**

Run: `pio run -e Heltec_v3_repeater_observer_mqtt 2>&1 | tail -3 && pio run -e RAK_4631_room_server_observer_mqtt_ethernet 2>&1 | tail -3 && pio test -e native -f test_mqtt_payload_builder 2>&1 | tail -3`
Expected: two `SUCCESS` lines and the payload builder suite passing. There is no native test for the builder itself because it depends on the Arduino Timezone library.

- [ ] **Step 4: Commit**

```bash
git add src/helpers/MQTTMessageBuilder.h src/helpers/MQTTMessageBuilder.cpp
git commit -m "refactor(mqtt): add explicit-clock overloads to MQTTMessageBuilder"
```

---

### Task 5: EthernetObserverPrefs struct, codec, and store

**Files:**
- Replace: `src/helpers/EthernetObserverPrefs.h`
- Replace: `src/helpers/EthernetObserverPrefs.cpp`
- Create: `test/test_ethernet_observer_prefs/test_ethernet_observer_prefs.cpp`

**Interfaces:**
- Produces `struct EthernetObserverPrefs { uint8_t room_posts, room_events, room_dm, room_stats; uint32_t auth_life_secs; char auth_audiences[128]; }`.
- Produces `namespace EthernetObserverPrefsCodec { const size_t kEncodedSize; void setDefaults(EthernetObserverPrefs*); size_t encode(const EthernetObserverPrefs&, uint8_t* buf, size_t buf_size); bool decode(const uint8_t* buf, size_t len, EthernetObserverPrefs* out); }`.
- Produces `EthernetObserverPrefs& ethernetObserverPrefs()` and `namespace EthernetObserverPrefsStore { bool load(FILESYSTEM* fs, EthernetObserverPrefs* out); bool save(FILESYSTEM* fs, const EthernetObserverPrefs& in); }` in the `.cpp`, Arduino-only.
- File is `/mqtt_eth_prefs`, layout: magic `M E T H`, `uint16_t version = 1`, `uint16_t payload_len`, then the struct fields in order as little-endian bytes.

- [ ] **Step 1: Write the failing test**

```cpp
#include <gtest/gtest.h>
#include <string.h>
#include "helpers/EthernetObserverPrefs.h"

namespace C = EthernetObserverPrefsCodec;

TEST(EthernetObserverPrefsCodec, DefaultsAreOnWithSpecLifetime) {
  EthernetObserverPrefs p;
  C::setDefaults(&p);
  EXPECT_EQ(1, p.room_posts);
  EXPECT_EQ(1, p.room_events);
  EXPECT_EQ(1, p.room_dm);
  EXPECT_EQ(1, p.room_stats);
  EXPECT_EQ(3300u, p.auth_life_secs);
  EXPECT_STREQ("", p.auth_audiences);
}

TEST(EthernetObserverPrefsCodec, RoundTrips) {
  EthernetObserverPrefs in;
  C::setDefaults(&in);
  in.room_dm = 0;
  in.auth_life_secs = 600;
  strcpy(in.auth_audiences, "mqtt-us-v1.letsmesh.net,mqtt.meshmapper.net");

  uint8_t buf[256];
  size_t n = C::encode(in, buf, sizeof(buf));
  EXPECT_EQ(C::kEncodedSize, n);
  EXPECT_EQ('M', buf[0]); EXPECT_EQ('E', buf[1]); EXPECT_EQ('T', buf[2]); EXPECT_EQ('H', buf[3]);

  EthernetObserverPrefs out;
  memset(&out, 0xAA, sizeof(out));
  EXPECT_TRUE(C::decode(buf, n, &out));
  EXPECT_EQ(1, out.room_posts);
  EXPECT_EQ(0, out.room_dm);
  EXPECT_EQ(600u, out.auth_life_secs);
  EXPECT_STREQ("mqtt-us-v1.letsmesh.net,mqtt.meshmapper.net", out.auth_audiences);
}

TEST(EthernetObserverPrefsCodec, RejectsBadMagicShortAndWrongVersion) {
  EthernetObserverPrefs in, out;
  C::setDefaults(&in);
  uint8_t buf[256];
  size_t n = C::encode(in, buf, sizeof(buf));

  uint8_t bad[256]; memcpy(bad, buf, n); bad[0] = 'X';
  EXPECT_FALSE(C::decode(bad, n, &out));
  EXPECT_FALSE(C::decode(buf, n - 1, &out));
  memcpy(bad, buf, n); bad[4] = 2;
  EXPECT_FALSE(C::decode(bad, n, &out));
  EXPECT_EQ(0u, C::encode(in, buf, 4));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pio test -e native -f test_ethernet_observer_prefs 2>&1 | tail -15`
Expected: compile error about `EthernetObserverPrefsCodec`.

- [ ] **Step 3: Write the header**

```cpp
#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string.h>

// nRF52-only observer settings. Kept out of MQTTPrefs so the fleet-critical
// /mqtt_prefs layout shared with ESP32 builds is never touched.
struct EthernetObserverPrefs {
  uint8_t room_posts;
  uint8_t room_events;
  uint8_t room_dm;
  uint8_t room_stats;
  uint32_t auth_life_secs;
  char auth_audiences[128];
};

namespace EthernetObserverPrefsCodec {

static const uint8_t kMagic[4] = { 'M', 'E', 'T', 'H' };
static const uint16_t kVersion = 1;
static const size_t kHeaderSize = 8;
static const size_t kPayloadSize = 4 + 4 + 128;
static const size_t kEncodedSize = kHeaderSize + kPayloadSize;

static inline void setDefaults(EthernetObserverPrefs* p) {
  p->room_posts = p->room_events = p->room_dm = p->room_stats = 1;
  p->auth_life_secs = 3300;
  p->auth_audiences[0] = '\0';
}

static inline void put16(uint8_t* b, uint16_t v) { b[0] = v & 0xFF; b[1] = v >> 8; }
static inline void put32(uint8_t* b, uint32_t v) { b[0] = v & 0xFF; b[1] = (v >> 8) & 0xFF; b[2] = (v >> 16) & 0xFF; b[3] = v >> 24; }
static inline uint16_t get16(const uint8_t* b) { return (uint16_t)(b[0] | (b[1] << 8)); }
static inline uint32_t get32(const uint8_t* b) { return (uint32_t)b[0] | ((uint32_t)b[1] << 8) | ((uint32_t)b[2] << 16) | ((uint32_t)b[3] << 24); }

static inline size_t encode(const EthernetObserverPrefs& p, uint8_t* buf, size_t buf_size) {
  if (!buf || buf_size < kEncodedSize) return 0;
  memcpy(buf, kMagic, 4);
  put16(buf + 4, kVersion);
  put16(buf + 6, (uint16_t)kPayloadSize);
  uint8_t* b = buf + kHeaderSize;
  b[0] = p.room_posts ? 1 : 0;
  b[1] = p.room_events ? 1 : 0;
  b[2] = p.room_dm ? 1 : 0;
  b[3] = p.room_stats ? 1 : 0;
  put32(b + 4, p.auth_life_secs);
  memset(b + 8, 0, 128);
  strncpy((char*)(b + 8), p.auth_audiences, 127);
  return kEncodedSize;
}

static inline bool decode(const uint8_t* buf, size_t len, EthernetObserverPrefs* out) {
  if (!buf || !out || len < kEncodedSize) return false;
  if (memcmp(buf, kMagic, 4) != 0) return false;
  if (get16(buf + 4) != kVersion) return false;
  if (get16(buf + 6) != kPayloadSize) return false;
  const uint8_t* b = buf + kHeaderSize;
  out->room_posts = b[0] ? 1 : 0;
  out->room_events = b[1] ? 1 : 0;
  out->room_dm = b[2] ? 1 : 0;
  out->room_stats = b[3] ? 1 : 0;
  out->auth_life_secs = get32(b + 4);
  memcpy(out->auth_audiences, b + 8, 128);
  out->auth_audiences[127] = '\0';
  return true;
}

}  // namespace EthernetObserverPrefsCodec

#if defined(ARDUINO)
#include <helpers/IdentityStore.h>   // FILESYSTEM
EthernetObserverPrefs& ethernetObserverPrefs();
namespace EthernetObserverPrefsStore {
  bool load(FILESYSTEM* fs, EthernetObserverPrefs* out);   // defaults on any failure, returns whether file was read
  bool save(FILESYSTEM* fs, const EthernetObserverPrefs& in);
}
#endif
```

- [ ] **Step 4: Write the store**

```cpp
#include "EthernetObserverPrefs.h"
#include <Arduino.h>

static EthernetObserverPrefs s_prefs;
static bool s_prefs_init = false;

EthernetObserverPrefs& ethernetObserverPrefs() {
  if (!s_prefs_init) { EthernetObserverPrefsCodec::setDefaults(&s_prefs); s_prefs_init = true; }
  return s_prefs;
}

static const char* kPath = "/mqtt_eth_prefs";

bool EthernetObserverPrefsStore::load(FILESYSTEM* fs, EthernetObserverPrefs* out) {
  EthernetObserverPrefsCodec::setDefaults(out);
  if (!fs || !fs->exists(kPath)) return false;
  File f = fs->open(kPath);
  if (!f) return false;
  uint8_t buf[EthernetObserverPrefsCodec::kEncodedSize];
  size_t n = f.read(buf, sizeof(buf));
  f.close();
  EthernetObserverPrefs tmp;
  if (!EthernetObserverPrefsCodec::decode(buf, n, &tmp)) {
    EthernetObserverPrefsCodec::setDefaults(out);
    return false;
  }
  *out = tmp;
  return true;
}

bool EthernetObserverPrefsStore::save(FILESYSTEM* fs, const EthernetObserverPrefs& in) {
  if (!fs) return false;
  uint8_t buf[EthernetObserverPrefsCodec::kEncodedSize];
  size_t n = EthernetObserverPrefsCodec::encode(in, buf, sizeof(buf));
  if (n == 0) return false;
#if defined(NRF52_PLATFORM)
  File f = fs->open(kPath, FILE_O_WRITE);
  if (f) f.seek(0);
#else
  File f = fs->open(kPath, "w");
#endif
  if (!f) return false;
  bool ok = f.write(buf, n) == n;
  f.close();
  return ok;
}
```

- [ ] **Step 5: Add to the native source filter and run tests**

The codec is header-only so no native filter change is needed.

Run: `pio test -e native -f test_ethernet_observer_prefs 2>&1 | tail -5`
Expected: `3 test cases: 3 succeeded`.

Run: `pio run -e RAK_4631_room_server_observer_mqtt_ethernet 2>&1 | tail -3`
Expected: `SUCCESS`.

- [ ] **Step 6: Commit**

```bash
git add src/helpers/EthernetObserverPrefs.h src/helpers/EthernetObserverPrefs.cpp test/test_ethernet_observer_prefs/test_ethernet_observer_prefs.cpp
git commit -m "feat(nrf52): add EthernetObserverPrefs with codec and LittleFS store"
```

---

### Task 6: Link and auth JSON payloads

**Files:**
- Replace: `src/helpers/MQTTEthernetPayloads.h`
- Create: `test/test_mqtt_ethernet_payloads/test_mqtt_ethernet_payloads.cpp`

**Interfaces:**
- Produces in `namespace MQTTEthernetPayloads`:
  - `struct LinkInfo { bool up; const char* ip; const char* gateway; const char* mode; const char* mac; uint32_t link_changes; uint32_t secs_since_change; bool broker_connected; uint32_t broker_reconnects; }`
  - `int buildLinkMessage(JsonDocument& doc, const char* timestamp, const char* origin, const char* origin_id, const LinkInfo& info, char* buf, size_t buf_size)`
  - `int buildAuthMessage(JsonDocument& doc, const char* audience, const char* username, const char* token, uint32_t iat, uint32_t exp, char* buf, size_t buf_size)`
- Both return the serialized length or 0 when it does not fit.

- [ ] **Step 1: Write the failing test**

```cpp
#include <gtest/gtest.h>
#include <string.h>
#include <ArduinoJson.h>
#include "helpers/MQTTEthernetPayloads.h"

namespace P = MQTTEthernetPayloads;

TEST(MQTTEthernetPayloads, LinkMessageHasSpecFields) {
  JsonDocument doc;
  char buf[512];
  P::LinkInfo info = { true, "192.168.1.50", "192.168.1.1", "dhcp", "02:11:22:33:44:55", 3, 120, true, 2 };
  int n = P::buildLinkMessage(doc, "2026-09-13T21:00:00Z", "Room", "abcd", info, buf, sizeof(buf));
  ASSERT_GT(n, 0);
  JsonDocument back;
  ASSERT_EQ(DeserializationError::Ok, deserializeJson(back, buf));
  EXPECT_STREQ("up", back["state"]);
  EXPECT_STREQ("192.168.1.50", back["ip"]);
  EXPECT_STREQ("192.168.1.1", back["gateway"]);
  EXPECT_STREQ("dhcp", back["mode"]);
  EXPECT_STREQ("02:11:22:33:44:55", back["mac"]);
  EXPECT_EQ(3, back["link_changes"].as<int>());
  EXPECT_EQ(120, back["secs_since_change"].as<int>());
  EXPECT_TRUE(back["broker_connected"].as<bool>());
  EXPECT_EQ(2, back["broker_reconnects"].as<int>());
  EXPECT_STREQ("Room", back["origin"]);
  EXPECT_STREQ("abcd", back["origin_id"]);
  EXPECT_STREQ("2026-09-13T21:00:00Z", back["timestamp"]);
}

TEST(MQTTEthernetPayloads, LinkDownOmitsAddresses) {
  JsonDocument doc;
  char buf[512];
  P::LinkInfo info = { false, "", "", "dhcp", "02:11:22:33:44:55", 4, 5, false, 2 };
  int n = P::buildLinkMessage(doc, "t", "o", "id", info, buf, sizeof(buf));
  ASSERT_GT(n, 0);
  JsonDocument back;
  deserializeJson(back, buf);
  EXPECT_STREQ("down", back["state"]);
  EXPECT_FALSE(back["ip"].is<const char*>());
}

TEST(MQTTEthernetPayloads, AuthMessage) {
  JsonDocument doc;
  char buf[512];
  int n = P::buildAuthMessage(doc, "mqtt-us-v1.letsmesh.net", "abcd", "h.p.s", 1000, 4300, buf, sizeof(buf));
  ASSERT_GT(n, 0);
  JsonDocument back;
  deserializeJson(back, buf);
  EXPECT_STREQ("mqtt-us-v1.letsmesh.net", back["audience"]);
  EXPECT_STREQ("abcd", back["username"]);
  EXPECT_STREQ("h.p.s", back["token"]);
  EXPECT_EQ(1000, back["iat"].as<int>());
  EXPECT_EQ(4300, back["exp"].as<int>());
}

TEST(MQTTEthernetPayloads, TooSmallBufferReturnsZero) {
  JsonDocument doc;
  char buf[16];
  EXPECT_EQ(0, P::buildAuthMessage(doc, "a", "b", "c", 1, 2, buf, sizeof(buf)));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pio test -e native -f test_mqtt_ethernet_payloads 2>&1 | tail -15`
Expected: compile error about `MQTTEthernetPayloads`.

- [ ] **Step 3: Write the header**

```cpp
#pragma once

#include <stddef.h>
#include <stdint.h>
#include <ArduinoJson.h>

namespace MQTTEthernetPayloads {

struct LinkInfo {
  bool up;
  const char* ip;
  const char* gateway;
  const char* mode;        // "dhcp" | "static"
  const char* mac;
  uint32_t link_changes;
  uint32_t secs_since_change;
  bool broker_connected;
  uint32_t broker_reconnects;
};

static inline int finish(JsonDocument& doc, char* buf, size_t buf_size) {
  size_t need = measureJson(doc);
  if (need + 1 > buf_size) return 0;
  return (int)serializeJson(doc, buf, buf_size);
}

static inline int buildLinkMessage(JsonDocument& doc, const char* timestamp, const char* origin,
                                   const char* origin_id, const LinkInfo& info,
                                   char* buf, size_t buf_size) {
  doc.clear();
  doc["timestamp"] = timestamp;
  doc["origin"] = origin;
  doc["origin_id"] = origin_id;
  doc["state"] = info.up ? "up" : "down";
  if (info.up) {
    doc["ip"] = info.ip;
    doc["gateway"] = info.gateway;
  }
  doc["mode"] = info.mode;
  doc["mac"] = info.mac;
  doc["link_changes"] = info.link_changes;
  doc["secs_since_change"] = info.secs_since_change;
  doc["broker_connected"] = info.broker_connected;
  doc["broker_reconnects"] = info.broker_reconnects;
  return finish(doc, buf, buf_size);
}

static inline int buildAuthMessage(JsonDocument& doc, const char* audience, const char* username,
                                   const char* token, uint32_t iat, uint32_t exp,
                                   char* buf, size_t buf_size) {
  doc.clear();
  doc["audience"] = audience;
  doc["username"] = username;
  doc["token"] = token;
  doc["iat"] = iat;
  doc["exp"] = exp;
  return finish(doc, buf, buf_size);
}

}  // namespace MQTTEthernetPayloads
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pio test -e native -f test_mqtt_ethernet_payloads 2>&1 | tail -5`
Expected: `4 test cases: 4 succeeded`.

- [ ] **Step 5: Commit**

```bash
git add src/helpers/MQTTEthernetPayloads.h test/test_mqtt_ethernet_payloads/test_mqtt_ethernet_payloads.cpp
git commit -m "feat(mqtt): add link and auth payload builders for the Ethernet observer"
```

---

### Task 7: RoomLogPublisher

**Files:**
- Create: `src/helpers/RoomLogPublisher.h`
- Replace: `src/helpers/RoomLogPublisher.cpp`
- Create: `test/test_room_log_publisher/test_room_log_publisher.cpp`
- Modify: `platformio.ini` native env `build_src_filter`, add `+<../src/helpers/RoomLogPublisher.cpp>`

**Interfaces:**
- Produces `class RoomLogPublisher`:
  - `typedef bool (*Sink)(void* ctx, const char* suffix, const char* json, size_t len, bool retain);`
  - `struct Post { const uint8_t* author_pubkey; uint8_t permissions; const char* text; uint32_t client_timestamp; };`
  - `struct Event { const char* event; const uint8_t* actor_pubkey; const char* result; const char* detail; };`
  - `struct DirectMessage { const uint8_t* sender_pubkey; uint8_t permissions; const char* kind; bool accepted; const char* text; uint32_t client_timestamp; };`
  - `struct Stats { uint32_t posts_total, posts_unsynced; uint16_t clients_admin, clients_read_write, clients_read_only, clients_guest; uint32_t acl_bytes, packet_drops, event_drops; };`
  - `void setSink(Sink fn, void* ctx)`, `void setOrigin(const char* origin, const char* origin_id)` (pointers retained, not copied), `void setEnabled(bool posts, bool events, bool dm, bool stats)`.
  - `bool publishPost(const Post&, const char* timestamp)`, `bool publishEvent(const Event&, const char* timestamp)`, `bool publishDirectMessage(const DirectMessage&, const char* timestamp)`, `bool publishStats(const Stats&, const char* timestamp)`. Each returns false when disabled, when no sink is set, or when the sink refuses.
  - `static const char* roleName(uint8_t permissions)` maps `PERM_ACL_*` low two bits to `"guest"`, `"read_only"`, `"read_write"`, `"admin"`.
  - `static const size_t JSON_BUFFER_SIZE = 512;`
- Suffixes emitted: `room/posts`, `room/events`, `room/dm`, `room/stats`. Only stats is retained.

- [ ] **Step 1: Write the failing test**

```cpp
#include <gtest/gtest.h>
#include <string.h>
#include <string>
#include <vector>
#include <ArduinoJson.h>
#include "helpers/RoomLogPublisher.h"

struct Captured { std::string suffix; std::string json; bool retain; };
static std::vector<Captured> g_out;
static bool g_accept = true;

static bool sink(void*, const char* suffix, const char* json, size_t len, bool retain) {
  if (!g_accept) return false;
  g_out.push_back({suffix, std::string(json, len), retain});
  return true;
}

static const uint8_t KEY[32] = { 0xAB, 0xCD, 0x01, 0x02 };

class RoomLogPublisherTest : public ::testing::Test {
protected:
  RoomLogPublisher pub;
  void SetUp() override {
    g_out.clear(); g_accept = true;
    pub.setSink(sink, nullptr);
    pub.setOrigin("Room", "abcd");
    pub.setEnabled(true, true, true, true);
  }
};

TEST_F(RoomLogPublisherTest, RoleNames) {
  EXPECT_STREQ("guest", RoomLogPublisher::roleName(0));
  EXPECT_STREQ("read_only", RoomLogPublisher::roleName(1));
  EXPECT_STREQ("read_write", RoomLogPublisher::roleName(2));
  EXPECT_STREQ("admin", RoomLogPublisher::roleName(3));
  EXPECT_STREQ("admin", RoomLogPublisher::roleName(0x83));
}

TEST_F(RoomLogPublisherTest, PostPayload) {
  RoomLogPublisher::Post p = { KEY, 2, "hello room", 1700000000u };
  EXPECT_TRUE(pub.publishPost(p, "2026-09-13T21:00:00Z"));
  ASSERT_EQ(1u, g_out.size());
  EXPECT_EQ("room/posts", g_out[0].suffix);
  EXPECT_FALSE(g_out[0].retain);
  JsonDocument d; ASSERT_EQ(DeserializationError::Ok, deserializeJson(d, g_out[0].json));
  EXPECT_STREQ("2026-09-13T21:00:00Z", d["timestamp"]);
  EXPECT_STREQ("Room", d["origin"]);
  EXPECT_STREQ("abcd", d["origin_id"]);
  EXPECT_EQ(0, strncmp("abcd0102", d["author_pubkey"].as<const char*>(), 8));
  EXPECT_EQ(64u, strlen(d["author_pubkey"].as<const char*>()));
  EXPECT_STREQ("read_write", d["role"]);
  EXPECT_STREQ("hello room", d["text"]);
  EXPECT_EQ(1700000000u, d["client_timestamp"].as<uint32_t>());
}

TEST_F(RoomLogPublisherTest, EventPayloadNeverCarriesReplyOrPassword) {
  RoomLogPublisher::Event e = { "admin_cmd", KEY, "ok", "get stats" };
  EXPECT_TRUE(pub.publishEvent(e, "t"));
  JsonDocument d; deserializeJson(d, g_out[0].json);
  EXPECT_EQ("room/events", g_out[0].suffix);
  EXPECT_STREQ("admin_cmd", d["event"]);
  EXPECT_STREQ("ok", d["result"]);
  EXPECT_STREQ("get stats", d["detail"]);
  EXPECT_FALSE(d["reply"].is<const char*>());
  EXPECT_FALSE(d["password"].is<const char*>());
}

TEST_F(RoomLogPublisherTest, EventWithoutActor) {
  RoomLogPublisher::Event e = { "acl_change", nullptr, "ok", "setperm" };
  EXPECT_TRUE(pub.publishEvent(e, "t"));
  JsonDocument d; deserializeJson(d, g_out[0].json);
  EXPECT_FALSE(d["actor_pubkey"].is<const char*>());
}

TEST_F(RoomLogPublisherTest, DirectMessagePayload) {
  RoomLogPublisher::DirectMessage m = { KEY, 3, "cli_data", true, "ver", 5 };
  EXPECT_TRUE(pub.publishDirectMessage(m, "t"));
  JsonDocument d; deserializeJson(d, g_out[0].json);
  EXPECT_EQ("room/dm", g_out[0].suffix);
  EXPECT_STREQ("cli_data", d["kind"]);
  EXPECT_TRUE(d["accepted"].as<bool>());
  EXPECT_STREQ("admin", d["role"]);
  EXPECT_STREQ("ver", d["text"]);
}

TEST_F(RoomLogPublisherTest, StatsPayloadIsRetained) {
  RoomLogPublisher::Stats s = { 10, 3, 1, 4, 0, 2, 1234, 7, 1 };
  EXPECT_TRUE(pub.publishStats(s, "t"));
  EXPECT_EQ("room/stats", g_out[0].suffix);
  EXPECT_TRUE(g_out[0].retain);
  JsonDocument d; deserializeJson(d, g_out[0].json);
  EXPECT_EQ(10, d["posts_total"].as<int>());
  EXPECT_EQ(3, d["posts_unsynced"].as<int>());
  EXPECT_EQ(1, d["clients_admin"].as<int>());
  EXPECT_EQ(4, d["clients_read_write"].as<int>());
  EXPECT_EQ(0, d["clients_read_only"].as<int>());
  EXPECT_EQ(2, d["clients_guest"].as<int>());
  EXPECT_EQ(1234, d["acl_bytes"].as<int>());
  EXPECT_EQ(7, d["packet_drops"].as<int>());
  EXPECT_EQ(1, d["event_drops"].as<int>());
}

TEST_F(RoomLogPublisherTest, DisabledTopicsAndSinkRefusal) {
  pub.setEnabled(false, true, true, true);
  RoomLogPublisher::Post p = { KEY, 2, "x", 1 };
  EXPECT_FALSE(pub.publishPost(p, "t"));
  EXPECT_TRUE(g_out.empty());
  g_accept = false;
  RoomLogPublisher::Event e = { "login_ok", KEY, "ok", "" };
  EXPECT_FALSE(pub.publishEvent(e, "t"));
}

TEST_F(RoomLogPublisherTest, NoSinkReturnsFalse) {
  RoomLogPublisher bare;
  RoomLogPublisher::Stats s = {};
  EXPECT_FALSE(bare.publishStats(s, "t"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pio test -e native -f test_room_log_publisher 2>&1 | tail -15`
Expected: compile error, `RoomLogPublisher.h` not found.

- [ ] **Step 3: Write the header**

```cpp
#pragma once

#include <stddef.h>
#include <stdint.h>
#include <ArduinoJson.h>

/**
 * Formats the room server's MQTT log topics and hands each JSON document to
 * a sink. Platform-neutral: no Arduino, no bridge dependency. The sink is
 * expected to queue, not block.
 */
class RoomLogPublisher {
public:
  typedef bool (*Sink)(void* ctx, const char* suffix, const char* json, size_t len, bool retain);

  struct Post { const uint8_t* author_pubkey; uint8_t permissions; const char* text; uint32_t client_timestamp; };
  struct Event { const char* event; const uint8_t* actor_pubkey; const char* result; const char* detail; };
  struct DirectMessage { const uint8_t* sender_pubkey; uint8_t permissions; const char* kind; bool accepted; const char* text; uint32_t client_timestamp; };
  struct Stats {
    uint32_t posts_total, posts_unsynced;
    uint16_t clients_admin, clients_read_write, clients_read_only, clients_guest;
    uint32_t acl_bytes, packet_drops, event_drops;
  };

  static const size_t JSON_BUFFER_SIZE = 512;

  RoomLogPublisher();
  void setSink(Sink fn, void* ctx) { _sink = fn; _ctx = ctx; }
  void setOrigin(const char* origin, const char* origin_id) { _origin = origin; _origin_id = origin_id; }
  void setEnabled(bool posts, bool events, bool dm, bool stats) { _posts = posts; _events = events; _dm = dm; _stats = stats; }

  bool publishPost(const Post& p, const char* timestamp);
  bool publishEvent(const Event& e, const char* timestamp);
  bool publishDirectMessage(const DirectMessage& m, const char* timestamp);
  bool publishStats(const Stats& s, const char* timestamp);

  static const char* roleName(uint8_t permissions);

private:
  Sink _sink;
  void* _ctx;
  const char* _origin;
  const char* _origin_id;
  bool _posts, _events, _dm, _stats;
  JsonDocument _doc;
  char _buf[JSON_BUFFER_SIZE];

  void header(const char* timestamp);
  bool emit(const char* suffix, bool retain);
  static void pubkeyHex(const uint8_t* key, char* out66);
};
```

- [ ] **Step 4: Write the implementation**

```cpp
#include "RoomLogPublisher.h"
#include <string.h>

static const size_t kPubKeyBytes = 32;

RoomLogPublisher::RoomLogPublisher()
  : _sink(nullptr), _ctx(nullptr), _origin(""), _origin_id(""),
    _posts(true), _events(true), _dm(true), _stats(true) {}

const char* RoomLogPublisher::roleName(uint8_t permissions) {
  switch (permissions & 3) {
    case 1: return "read_only";
    case 2: return "read_write";
    case 3: return "admin";
    default: return "guest";
  }
}

void RoomLogPublisher::pubkeyHex(const uint8_t* key, char* out) {
  static const char* hex = "0123456789abcdef";
  for (size_t i = 0; i < kPubKeyBytes; i++) {
    out[i * 2] = hex[key[i] >> 4];
    out[i * 2 + 1] = hex[key[i] & 0x0F];
  }
  out[kPubKeyBytes * 2] = '\0';
}

void RoomLogPublisher::header(const char* timestamp) {
  _doc.clear();
  _doc["timestamp"] = timestamp;
  _doc["origin"] = _origin;
  _doc["origin_id"] = _origin_id;
}

bool RoomLogPublisher::emit(const char* suffix, bool retain) {
  if (!_sink) return false;
  if (measureJson(_doc) + 1 > sizeof(_buf)) return false;
  size_t len = serializeJson(_doc, _buf, sizeof(_buf));
  return _sink(_ctx, suffix, _buf, len, retain);
}

bool RoomLogPublisher::publishPost(const Post& p, const char* timestamp) {
  if (!_posts || !_sink) return false;
  char hex[kPubKeyBytes * 2 + 1];
  pubkeyHex(p.author_pubkey, hex);
  header(timestamp);
  _doc["author_pubkey"] = hex;
  _doc["role"] = roleName(p.permissions);
  _doc["text"] = p.text ? p.text : "";
  _doc["client_timestamp"] = p.client_timestamp;
  return emit("room/posts", false);
}

bool RoomLogPublisher::publishEvent(const Event& e, const char* timestamp) {
  if (!_events || !_sink) return false;
  header(timestamp);
  _doc["event"] = e.event;
  char hex[kPubKeyBytes * 2 + 1];
  if (e.actor_pubkey) { pubkeyHex(e.actor_pubkey, hex); _doc["actor_pubkey"] = hex; }
  _doc["result"] = e.result ? e.result : "";
  _doc["detail"] = e.detail ? e.detail : "";
  return emit("room/events", false);
}

bool RoomLogPublisher::publishDirectMessage(const DirectMessage& m, const char* timestamp) {
  if (!_dm || !_sink) return false;
  char hex[kPubKeyBytes * 2 + 1];
  pubkeyHex(m.sender_pubkey, hex);
  header(timestamp);
  _doc["sender_pubkey"] = hex;
  _doc["role"] = roleName(m.permissions);
  _doc["kind"] = m.kind;
  _doc["accepted"] = m.accepted;
  _doc["text"] = m.text ? m.text : "";
  _doc["client_timestamp"] = m.client_timestamp;
  return emit("room/dm", false);
}

bool RoomLogPublisher::publishStats(const Stats& s, const char* timestamp) {
  if (!_stats || !_sink) return false;
  header(timestamp);
  _doc["posts_total"] = s.posts_total;
  _doc["posts_unsynced"] = s.posts_unsynced;
  _doc["clients_admin"] = s.clients_admin;
  _doc["clients_read_write"] = s.clients_read_write;
  _doc["clients_read_only"] = s.clients_read_only;
  _doc["clients_guest"] = s.clients_guest;
  _doc["acl_bytes"] = s.acl_bytes;
  _doc["packet_drops"] = s.packet_drops;
  _doc["event_drops"] = s.event_drops;
  return emit("room/stats", true);
}
```

- [ ] **Step 5: Add the source to the native env**

In `platformio.ini` under `[env:native]` `build_src_filter`, add the line `+<../src/helpers/RoomLogPublisher.cpp>` after the `MQTTPayloadBuilder.cpp` line.

- [ ] **Step 6: Run test to verify it passes**

Run: `pio test -e native -f test_room_log_publisher 2>&1 | tail -5`
Expected: `8 test cases: 8 succeeded`.

- [ ] **Step 7: Commit**

```bash
git add src/helpers/RoomLogPublisher.h src/helpers/RoomLogPublisher.cpp test/test_room_log_publisher/test_room_log_publisher.cpp platformio.ini
git commit -m "feat(room): add RoomLogPublisher for MQTT room log topics"
```

---

### Task 8: Bridge connection lifecycle, NTP, status, and link publishing

**Files:**
- Modify: `src/helpers/bridges/EthernetMQTTBridge.cpp` (replace the skeleton bodies named below)

**Interfaces:**
- Consumes: `MQTTEthernetPolicy`, `MQTTEthernetPayloads`, `MQTTConnectionPolicy::reconnectBackoffMs`, `MQTTMessageBuilder::formatIsoTimestampForMqtt` and `buildStatusMessage`, `mqttBuildPublicationTopic`, `findMQTTPreset`.
- Produces: a bridge that connects, publishes `status` and `link` on the interval, and reports state through the static CLI helpers.

- [ ] **Step 1: Implement link and topic helpers**

```cpp
bool EthernetMQTTBridge::linkReady() const {
  if (Ethernet.hardwareStatus() == EthernetNoHardware) return false;
  if (Ethernet.linkStatus() != LinkON) return false;
  IPAddress ip = Ethernet.localIP();
  return !(ip[0] == 0 && ip[1] == 0 && ip[2] == 0 && ip[3] == 0);
}

void EthernetMQTTBridge::refreshLink(unsigned long now) {
  bool up = linkReady();
  if (up != _link_up) {
    _link_up = up;
    _link_changed_at = now;
    _link_changes++;
    ETH_MQTT_LOG("link %s", up ? "up" : "down");
    if (!up) {
      if (_client.connected()) _client.disconnect();
      onDisconnected(now);
    }
    _next_link_at = 0;   // publish link change as soon as the broker is reachable
  }
}

bool EthernetMQTTBridge::topicFor(const char* suffix, char* buf, size_t buf_size) const {
  if (!mqttIataValid(_iata) || strcmp(_iata, "XXX") == 0 || _device_id[0] == '\0') return false;
  int n = snprintf(buf, buf_size, "meshcore/%s/%s/%s", _iata, _device_id, suffix);
  return n > 0 && (size_t)n < buf_size;
}

void EthernetMQTTBridge::formatTimestamp(char* buf, size_t buf_size) const {
  uint32_t secs = _rtc ? _rtc->getCurrentTime() : 0;
  long usec = _ms ? (long)((_ms->getMillis() % 1000UL) * 1000UL) : 0;
  MQTTMessageBuilder::formatIsoTimestampForMqtt((time_t)secs, usec, nullptr, buf, buf_size);
}
```

Note `mqttIataValid` comes from `helpers/MQTTObserverValidation.h`, already included through the topic router.

- [ ] **Step 2: Implement begin, config validity, connect, and disconnect**

```cpp
void EthernetMQTTBridge::begin() {
  if (_initialized) return;
  strncpy(_iata, _obs->mqtt_iata, sizeof(_iata) - 1); _iata[sizeof(_iata) - 1] = 0;
  getEffectiveMqttOrigin(_prefs, _obs, _origin, sizeof(_origin));
  snprintf(_client_id, sizeof(_client_id), "mc-%.16s", _device_id);
  _client.setBufferSize(MQTT_MAX_PACKET_SIZE);
  _client.setKeepAlive(55);
  _client.setSocketTimeout(MQTT_SOCKET_TIMEOUT);
  _net.setConnectionTimeout(1500);
  _backoff = 0;
  _last_connect_attempt = 0;
  _slot_state = isConfigValid(_obs) ? "wait" : "inactive";
  _initialized = true;
  ETH_MQTT_LOG("bridge begin, iata=%s origin=%s", _iata, _origin);
}

void EthernetMQTTBridge::end() {
  if (_client.connected()) _client.disconnect();
  _slot_state = "inactive";
  _initialized = false;
}

bool EthernetMQTTBridge::isConfigValid(const MQTTPrefs* obs) {
  if (!obs || !s_instance) return false;
  const char* preset = obs->mqtt_slot_preset[0];
  if (strcmp(preset, MQTT_PRESET_CUSTOM) != 0 && strcmp(preset, MQTT_PRESET_NONE) != 0) {
    const MQTTPresetDef* def = findMQTTPreset(preset);
    if (!def || !MQTTEthernetPolicy::presetUsableWithoutTls(def->server_url)) return false;
  }
  return MQTTEthernetPolicy::slotConfigValid(preset, obs->mqtt_slot_host[0], obs->mqtt_slot_port[0],
                                             obs->mqtt_iata, s_instance->_device_id);
}

// Resolve host, port, username, password for slot 0 into caller buffers.
static void resolveBroker(const MQTTPrefs* obs, char* host, size_t host_size, uint16_t* port,
                          const char** user, const char** pass) {
  const char* preset = obs->mqtt_slot_preset[0];
  *user = obs->mqtt_slot_username[0][0] ? obs->mqtt_slot_username[0] : nullptr;
  *pass = obs->mqtt_slot_password[0][0] ? obs->mqtt_slot_password[0] : nullptr;
  if (strcmp(preset, MQTT_PRESET_CUSTOM) == 0) {
    strncpy(host, obs->mqtt_slot_host[0], host_size - 1); host[host_size - 1] = 0;
    *port = obs->mqtt_slot_port[0] ? obs->mqtt_slot_port[0] : 1883;
    return;
  }
  const MQTTPresetDef* def = findMQTTPreset(preset);
  host[0] = 0; *port = 1883;
  if (!def) return;
  // "mqtt://host:port" -> host, port
  const char* p = def->server_url + 7;
  const char* colon = strchr(p, ':');
  size_t hl = colon ? (size_t)(colon - p) : strlen(p);
  if (hl >= host_size) hl = host_size - 1;
  memcpy(host, p, hl); host[hl] = 0;
  if (colon) *port = (uint16_t)atoi(colon + 1);
  if (def->auth == MQTT_AUTH_USERPASS && def->userpass_username && !mqttPresetUsesDevicePubkeyUsername(def)) {
    *user = def->userpass_username;
    *pass = def->userpass_password;
  }
}

bool EthernetMQTTBridge::connectBroker(unsigned long now) {
  if (!isConfigValid(_obs)) { _slot_state = "inactive"; return false; }
  if (!MQTTConnectionPolicy::reconnectDue((uint32_t)now, (uint32_t)_last_connect_attempt, _backoff, 0)
      && _last_connect_attempt != 0) {
    return false;
  }
  _last_connect_attempt = now;
  char host[64]; uint16_t port; const char* user; const char* pass;
  resolveBroker(_obs, host, sizeof(host), &port, &user, &pass);
  _client.setServer(host, port);
  ETH_MQTT_LOG("connecting %s:%u as %s", host, (unsigned)port, _client_id);
  bool ok = user ? _client.connect(_client_id, user, pass) : _client.connect(_client_id);
  if (!ok) {
    _slot_state = "fail";
    if (_outage_started_at == 0) _outage_started_at = now;
    _backoff = MQTTConnectionPolicy::advanceBackoff(_backoff, false).next;
    ETH_MQTT_LOG("connect failed rc=%d, backoff step %u", _client.state(), (unsigned)_backoff);
    return false;
  }
  if (_outage_started_at != 0) _broker_reconnects++;
  _outage_started_at = 0;
  _backoff = 0;
  _slot_state = "ok";
  _next_status_at = 0;
  _next_link_at = 0;
  _next_auth_at = 0;
  if (!_time_synced) _next_ntp_at = 0;
  ETH_MQTT_LOG("connected");
  return true;
}

void EthernetMQTTBridge::onDisconnected(unsigned long now) {
  if (_slot_state[0] == 'o') _slot_state = "disc";
  if (_outage_started_at == 0) _outage_started_at = now;
}
```

Check the exact field name returned by `MQTTConnectionPolicy::advanceBackoff` in `src/helpers/MQTTConnectionPolicy.h:69` and use it; if that helper's signature does not fit, replace the line with `if (_backoff < 5) _backoff++;` which yields the same ladder through `reconnectBackoffMs`. Also check `MQTTPresetDef` for the password field name (`userpass_password`) at `src/helpers/MQTTPresets.h:32-46` and adjust.

- [ ] **Step 3: Implement NTP**

```cpp
bool EthernetMQTTBridge::syncNtp(uint32_t timeout_ms) {
  if (!_link_up) return false;
  const char* server = (_obs && _obs->mqtt_ntp_server[0]) ? _obs->mqtt_ntp_server : "pool.ntp.org";
  EthernetUDP udp;                       // scoped: frees the W5100S socket on return
  NTPClient ntp(udp, server);
  ntp.begin();
  unsigned long start = millis();
  bool ok = false;
  while (!ok && millis() - start < timeout_ms) {
    ok = ntp.forceUpdate();
  }
  if (ok) {
    uint32_t epoch = (uint32_t)ntp.getEpochTime();
    if (epoch > 1700000000UL) { _rtc->setCurrentTime(epoch); _time_synced = true; }
    else ok = false;
  }
  ntp.end();
  ETH_MQTT_LOG("ntp %s via %s", ok ? "ok" : "failed", server);
  return ok;
}

bool EthernetMQTTBridge::requestForcedNtpSync(uint32_t timeout_ms) {
  bool ok = syncNtp(timeout_ms);
  _next_ntp_at = millis() + 3600000UL;
  return ok;
}

bool EthernetMQTTBridge::ntpDiag(char* reply, size_t reply_size, bool verbose) {
  const char* server = (_obs && _obs->mqtt_ntp_server[0]) ? _obs->mqtt_ntp_server : "pool.ntp.org";
  if (!_link_up) { snprintf(reply, reply_size, "NTP: link down"); return false; }
  bool ok = syncNtp(3000);
  if (verbose) snprintf(reply, reply_size, "NTP: %s server=%s synced=%d rtc=%lu", ok ? "ok" : "fail", server, (int)_time_synced, (unsigned long)_rtc->getCurrentTime());
  else snprintf(reply, reply_size, "NTP: %s", ok ? "ok" : "fail");
  return ok;
}
```

`NTPClient::forceUpdate` waits up to one second internally, so the loop bounds the total wait by `timeout_ms`.

- [ ] **Step 4: Implement status and link publishing**

```cpp
static int freeHeapBytes() {
  struct mallinfo mi = mallinfo();
  return (int)mi.fordblks;
}

bool EthernetMQTTBridge::publishStatus() {
  char topic[128];
  if (!topicFor("status", topic, sizeof(topic))) return false;
  char timestamp[40]; formatTimestamp(timestamp, sizeof(timestamp));
  char radio_info[64];
  snprintf(radio_info, sizeof(radio_info), "%.6f,%.1f,%d,%d", _prefs->freq, _prefs->bw, _prefs->sf, _prefs->cr);
  char client_version[64];
  snprintf(client_version, sizeof(client_version), "meshcore-nrf52-eth %s", _build_date);

  int battery_mv = _board ? (int)_board->getBattMilliVolts() : -1;
  int uptime_secs = _ms ? (int)(_ms->getMillis() / 1000) : -1;
  int errors = -1, tx_air = -1, rx_air = -1, sent = -1, recv = -1;
  if (_dispatcher) {
    errors = _dispatcher->getErrFlags();
    tx_air = (int)(_dispatcher->getTotalAirTime() / 1000);
    rx_air = (int)(_dispatcher->getReceiveAirTime() / 1000);
    sent = (int)(_dispatcher->getNumSentFlood() + _dispatcher->getNumSentDirect());
    recv = (int)(_dispatcher->getNumRecvFlood() + _dispatcher->getNumRecvDirect());
  }
  int noise_floor = _radio ? (int)_radio->getNoiseFloor() : -999;
  int recv_errors = _radio ? (int)_radio->getPacketsRecvErrors() : -1;

  int len = MQTTMessageBuilder::buildStatusMessage(
    _doc, _origin, _device_id, _board_model, _firmware_version, radio_info, client_version,
    "online", timestamp, _json, sizeof(_json),
    battery_mv, uptime_secs, errors, getQueueSize(), noise_floor, tx_air, rx_air, recv_errors,
    freeHeapBytes(), sent, recv, _prefs->disable_fwd ? "off" : "on");
  if (len <= 0) return false;
  _doc["time_synced"] = _time_synced;   // extra field, harmless to analyzers
  len = (int)serializeJson(_doc, _json, sizeof(_json));
  bool ok = _client.publish(topic, (const uint8_t*)_json, len, true);
  ok ? _publish_ok++ : _publish_err++;
  return ok;
}

bool EthernetMQTTBridge::publishLink() {
  char topic[128];
  if (!topicFor("link", topic, sizeof(topic))) return false;
  char timestamp[40]; formatTimestamp(timestamp, sizeof(timestamp));
  char ip[16] = "", gw[16] = "", mac_str[18];
  uint8_t mac[6]; Ethernet.MACAddress(mac);
  snprintf(mac_str, sizeof(mac_str), "%02X:%02X:%02X:%02X:%02X:%02X", mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]);
  if (_link_up) {
    IPAddress a = Ethernet.localIP(); snprintf(ip, sizeof(ip), "%u.%u.%u.%u", a[0], a[1], a[2], a[3]);
    IPAddress g = Ethernet.gatewayIP(); snprintf(gw, sizeof(gw), "%u.%u.%u.%u", g[0], g[1], g[2], g[3]);
  }
#if defined(ETHERNET_STATIC_IP)
  const char* mode = "static";
#else
  const char* mode = "dhcp";
#endif
  MQTTEthernetPayloads::LinkInfo info = {
    _link_up, ip, gw, mode, mac_str, _link_changes,
    (uint32_t)((millis() - _link_changed_at) / 1000), _client.connected(), _broker_reconnects
  };
  int len = MQTTEthernetPayloads::buildLinkMessage(_doc, timestamp, _origin, _device_id, info, _json, sizeof(_json));
  if (len <= 0) return false;
  bool ok = _client.publish(topic, (const uint8_t*)_json, len, true);
  ok ? _publish_ok++ : _publish_err++;
  return ok;
}
```

If `buildStatusMessage` in `MQTTPayloadBuilder.cpp` serializes into the buffer itself and leaves `_doc` populated, the `time_synced` insertion above works; confirm by reading `src/helpers/MQTTPayloadBuilder.cpp:25-84`. If it clears the document after serializing, drop the two `time_synced` lines and instead append `,"time_synced":true` by string edit before the closing brace, guarded by a length check.

- [ ] **Step 5: Implement loop**

```cpp
void EthernetMQTTBridge::loop() {
  if (!_initialized) return;
  unsigned long now = millis();
  refreshLink(now);
  if (!_link_up) return;

  if (!_client.connected()) {
    if (_slot_state[0] == 'o') onDisconnected(now);
    if (!connectBroker(now)) return;
  }
  _client.loop();

  if ((long)(now - _next_ntp_at) >= 0) {
    syncNtp(2000);
    _next_ntp_at = now + (_time_synced ? 3600000UL : 60000UL);
    return;   // one unit of work per pass
  }
  if ((long)(now - _next_link_at) >= 0) {
    publishLink();
    _next_link_at = now + MQTTEthernetPolicy::linkPublishIntervalMs();
    return;
  }
  if (_obs->mqtt_status_enabled && (long)(now - _next_status_at) >= 0) {
    publishStatus();
    _next_status_at = now + (unsigned long)_obs->mqtt_status_interval * 1000UL;
    return;
  }
  if (drainEventRing()) return;
  if (drainPacketRing()) return;
  if (_obs->mqtt_neighbors_enabled && (long)(now - _next_neighbors_at) >= 0) {
    publishNeighbors();
    _next_neighbors_at = now + (unsigned long)_obs->mqtt_neighbors_interval * 1000UL;
    return;
  }
  if ((long)(now - _next_auth_at) >= 0) {
    publishAuthTokens();
    _next_auth_at = now + 60000UL;   // policy decides whether a token is actually due
  }
}
```

Check `mqtt_status_interval` and `mqtt_neighbors_interval` units in `MQTTDefaults.h` and the ESP32 bridge; if they are already milliseconds, drop the `* 1000UL`.

- [ ] **Step 6: Implement the static CLI helpers**

```cpp
void EthernetMQTTBridge::formatMqttStatusReply(char* buf, size_t bufsize, const MQTTPrefs* obs) {
  if (!s_instance) { snprintf(buf, bufsize, "> MQTT: bridge not created"); return; }
  char host[64]; uint16_t port; const char* u; const char* p;
  resolveBroker(obs, host, sizeof(host), &port, &u, &p);
  snprintf(buf, bufsize, "> MQTT: %s link=%s broker=%s:%u ok=%lu err=%lu q=%d drops=%lu/%lu ntp=%d",
           s_instance->_slot_state, s_instance->_link_up ? "up" : "down", host, (unsigned)port,
           s_instance->_publish_ok, s_instance->_publish_err, s_instance->getQueueSize(),
           (unsigned long)s_instance->packetDrops(), (unsigned long)s_instance->eventDrops(),
           (int)s_instance->_time_synced);
}

void EthernetMQTTBridge::formatMqttStatsReply(char* buf, size_t bufsize) {
  if (!s_instance) { snprintf(buf, bufsize, "> MQTT: bridge not created"); return; }
  snprintf(buf, bufsize, "> pub_ok=%lu pub_err=%lu reconnects=%lu pkt_drops=%lu evt_drops=%lu",
           s_instance->_publish_ok, s_instance->_publish_err, (unsigned long)s_instance->_broker_reconnects,
           (unsigned long)s_instance->packetDrops(), (unsigned long)s_instance->eventDrops());
}

bool EthernetMQTTBridge::getSlotStatusSnapshot(int slot_index, SlotStatusSnapshot* out) {
  if (slot_index != 0 || !s_instance || !out) return false;
  const char* preset = s_instance->_obs->mqtt_slot_preset[0];
  out->name = preset[0] ? preset : "custom";
  out->state = s_instance->_slot_state;
  out->publish_ok = s_instance->_publish_ok;
  out->publish_err = s_instance->_publish_err;
  out->filter_mask = s_instance->_obs->mqtt_slot_packet_filter[0];
  return true;
}

void EthernetMQTTBridge::setSlotPreset(int slot_index, const char* preset_name) {
  if (slot_index != 0 || !preset_name) return;
  strncpy(_obs->mqtt_slot_preset[0], preset_name, sizeof(_obs->mqtt_slot_preset[0]) - 1);
  _obs->mqtt_slot_preset[0][sizeof(_obs->mqtt_slot_preset[0]) - 1] = 0;
  if (_client.connected()) _client.disconnect();
  _backoff = 0; _last_connect_attempt = 0;
}
```

- [ ] **Step 7: Build and verify on hardware**

Run: `pio run -e RAK_4631_room_server_observer_mqtt_ethernet 2>&1 | tail -3`
Expected: `SUCCESS`.

Flash the gateway (`pio run -e RAK_4631_room_server_observer_mqtt_ethernet -t upload`), open a serial monitor at 115200, and run:

```
set mqtt.iata SEA
set mqtt1.preset custom
set mqtt1.server 192.168.1.10
set mqtt1.port 1883
get mqtt.config.valid
```

At this point `get mqtt.config.valid` still answers through the Task 1 stub, so instead confirm on the broker host:

```
mosquitto_sub -h 192.168.1.10 -v -t 'meshcore/#'
```

Expected within one status interval: one retained `.../link` message with `"state":"up"` and one `.../status` message with `"status":"online"` and a `stats` object. Serial shows `MQTT: connected` and `MQTT: ntp ok`.

The CLI subset arrives in Task 12; until then, configure with the build-flag defaults by uncommenting `MQTT_SERVER` and `MQTT_PORT` in the env and setting `-D MQTT_DEFAULT_IATA='"SEA"'` if the stub CLI cannot persist settings.

- [ ] **Step 8: Commit**

```bash
git add src/helpers/bridges/EthernetMQTTBridge.cpp
git commit -m "feat(nrf52): Ethernet MQTT bridge connect, NTP, status and link publishing"
```

---

### Task 9: Packet, raw, and room-topic publishing through the rings

**Files:**
- Modify: `src/helpers/bridges/EthernetMQTTBridge.cpp`

**Interfaces:**
- Consumes: `MQTTMessageBuilder::buildPacketJSONFromRawAt`, `buildPacketJSONAt`, `buildRawJSONAt` from Task 4; `MQTTPacketFilter` mask in `_obs->mqtt_slot_packet_filter[0]`.
- Produces: `storeRawRadioData`, `onPacketReceived`, `sendPacket`, `publishRoomTopic`, `drainPacketRing`, `drainEventRing` fully working.

- [ ] **Step 1: Implement staging and queueing**

```cpp
void EthernetMQTTBridge::storeRawRadioData(const uint8_t* raw_data, int len, float snr, float rssi) {
  if (len <= 0 || len > (int)sizeof(_staged_raw)) { _staged_valid = false; return; }
  memcpy(_staged_raw, raw_data, len);
  _staged_raw_len = (uint8_t)len;
  _staged_snr = snr; _staged_rssi = rssi;
  _staged_valid = true;
}

static bool typeAllowed(uint16_t mask, uint8_t type) { return (mask & (1u << (type & 0x0F))) != 0; }

void EthernetMQTTBridge::onPacketReceived(mesh::Packet* packet) {
  if (!_initialized || !packet) return;
  if (!_obs->mqtt_rx_enabled) return;
  if (!_obs->mqtt_packets_enabled && !_obs->mqtt_raw_enabled) return;
  if (!typeAllowed(_obs->mqtt_slot_packet_filter[0], packet->getPayloadType())) return;
  PacketEntry e;
  e.blob_len = packet->writeTo(e.blob);
  e.is_tx = false;
  if (_staged_valid) { memcpy(e.raw, _staged_raw, _staged_raw_len); e.raw_len = _staged_raw_len; e.snr = _staged_snr; e.rssi = _staged_rssi; }
  else { e.raw_len = 0; e.snr = 0; e.rssi = 0; }
  _staged_valid = false;
  e.rtc_secs = _rtc->getCurrentTime();
  _packet_ring.push(e);
}

void EthernetMQTTBridge::sendPacket(mesh::Packet* packet) {
  if (!_initialized || !packet) return;
  if (!_obs->mqtt_tx_enabled) return;
  if (!_obs->mqtt_packets_enabled && !_obs->mqtt_raw_enabled) return;
  if (!typeAllowed(_obs->mqtt_slot_packet_filter[0], packet->getPayloadType())) return;
  PacketEntry e;
  e.blob_len = packet->writeTo(e.blob);
  e.is_tx = true; e.raw_len = 0; e.snr = 0; e.rssi = 0;
  e.rtc_secs = _rtc->getCurrentTime();
  _packet_ring.push(e);
}

bool EthernetMQTTBridge::publishRoomTopic(const char* suffix, const char* json, size_t len, bool retain) {
  if (!_initialized || !suffix || !json) return false;
  if (len >= sizeof(EventEntry::json)) return false;
  EventEntry e;
  strncpy(e.suffix, suffix, sizeof(e.suffix) - 1); e.suffix[sizeof(e.suffix) - 1] = 0;
  e.retain = retain; e.len = (uint16_t)len;
  memcpy(e.json, json, len); e.json[len] = 0;
  _event_ring.push(e);
  return true;
}
```

`e.suffix` is 16 bytes, which fits `room/events` (11 characters) with room to spare. `room/stats` and `room/posts` also fit.

- [ ] **Step 2: Implement the drains**

```cpp
bool EthernetMQTTBridge::drainEventRing() {
  EventEntry* e = _event_ring.front();
  if (!e) return false;
  char topic[128];
  bool ok = false;
  if (topicFor(e->suffix, topic, sizeof(topic))) {
    ok = _client.publish(topic, (const uint8_t*)e->json, e->len, e->retain);
  }
  if (ok) { _publish_ok++; _event_retry_pending = false; _event_ring.pop(); return true; }
  _publish_err++;
  if (_event_retry_pending) { _event_ring.pop(); _event_retry_pending = false; }   // second failure: drop
  else _event_retry_pending = true;
  return true;
}

bool EthernetMQTTBridge::drainPacketRing() {
  PacketEntry* e = _packet_ring.front();
  if (!e) return false;
  mesh::Packet pkt;
  if (!pkt.readFrom(e->blob, e->blob_len)) { _packet_ring.pop(); return true; }
  long usec = 0;
  bool all_ok = true;
  char topic[128];

  if (_obs->mqtt_packets_enabled && topicFor("packets", topic, sizeof(topic))) {
    int len;
    if (!e->is_tx && e->raw_len > 0) {
      len = MQTTMessageBuilder::buildPacketJSONFromRawAt(_doc, e->raw, e->raw_len, &pkt, false, _origin, _device_id,
                                                          e->snr, e->rssi, NAN, nullptr, (time_t)e->rtc_secs, usec, _json, sizeof(_json));
    } else {
      len = MQTTMessageBuilder::buildPacketJSONAt(_doc, &pkt, e->is_tx, _origin, _device_id, nullptr,
                                                   (time_t)e->rtc_secs, usec, _json, sizeof(_json));
    }
    if (len > 0) { bool ok = _client.publish(topic, (const uint8_t*)_json, len, false); ok ? _publish_ok++ : _publish_err++; all_ok = all_ok && ok; }
  }
  if (_obs->mqtt_raw_enabled && topicFor("raw", topic, sizeof(topic))) {
    int len = MQTTMessageBuilder::buildRawJSONAt(_doc, &pkt, _origin, _device_id, nullptr,
                                                  (time_t)e->rtc_secs, usec, _json, sizeof(_json));
    if (len > 0) { bool ok = _client.publish(topic, (const uint8_t*)_json, len, false); ok ? _publish_ok++ : _publish_err++; all_ok = all_ok && ok; }
  }

  if (all_ok) { _packet_retry_pending = false; _packet_ring.pop(); return true; }
  if (_packet_retry_pending) { _packet_ring.pop(); _packet_retry_pending = false; }
  else _packet_retry_pending = true;
  return true;
}
```

Confirm the `readFrom` signature at `src/Packet.h:107-110` and adapt the call (it may return `bool` or `void`; if `void`, drop the `if` and always proceed).

- [ ] **Step 3: Build and verify on hardware**

Run: `pio run -e RAK_4631_room_server_observer_mqtt_ethernet 2>&1 | tail -3`
Expected: `SUCCESS`.

With `mosquitto_sub -v -t 'meshcore/#'` running, send an advert from a phone within range. Expected: one `.../packets` message with `"direction":"rx"`, a `raw` hex string, non-default `SNR` and `RSSI`, and one `.../raw` message. Send the room's own advert with `advert` on the serial CLI; expected a `"direction":"tx"` packet.

Unplug the Ethernet cable for 30 seconds while adverts arrive, then reconnect. Expected: serial logs `link down` then `link up`, a `link` message with `"link_changes":2`, and the buffered packets publish in order after reconnect. `get mqtt.status` (once Task 12 lands) shows non-zero drops only if more than 16 packets arrived while down.

- [ ] **Step 4: Commit**

```bash
git add src/helpers/bridges/EthernetMQTTBridge.cpp
git commit -m "feat(nrf52): publish packets, raw, and room topics from bounded rings"
```

---

### Task 10: Room server hooks

**Files:**
- Modify: `examples/simple_room_server/MyMesh.h` (includes near line 29, members near line 199, callbacks near line 289)
- Modify: `examples/simple_room_server/MyMesh.cpp` (`storePost`, `onAnonDataRecv`, `onPeerDataRecv`, `handleCommand` setperm branch, `begin`, `loop`)

**Interfaces:**
- Consumes: `RoomLogPublisher`, `EthernetMQTTBridge::publishRoomTopic`, `ethernetObserverPrefs()`, `EthernetObserverPrefsStore`.
- Produces: room log events flowing to the bridge; eth prefs loaded at boot and saved with `savePrefs()`.

- [ ] **Step 1: Add includes and members to `MyMesh.h`**

After the existing `#ifdef WITH_MQTT_BRIDGE ... #endif` include block near line 29, add:

```cpp
#ifdef WITH_MQTT_BRIDGE_ETHERNET
#include "helpers/RoomLogPublisher.h"
#include "helpers/EthernetObserverPrefs.h"
#endif
```

Next to the `MQTTBridge* bridge;` member, add:

```cpp
#ifdef WITH_MQTT_BRIDGE_ETHERNET
  RoomLogPublisher _room_log;
  unsigned long _next_room_stats_at = 0;
  static bool roomLogSink(void* ctx, const char* suffix, const char* json, size_t len, bool retain);
  void roomLogTimestamp(char* buf, size_t buf_size);
  void logRoomEvent(const char* event, const uint8_t* actor, const char* result, const char* detail);
  void publishRoomStats();
  void applyRoomLogFlags();
#endif
```

Change the `savePrefs()` override to:

```cpp
  void savePrefs() override {
    _cli.savePrefs(_fs);
#ifdef WITH_MQTT_BRIDGE_ETHERNET
    EthernetObserverPrefsStore::save(_fs, ethernetObserverPrefs());
    applyRoomLogFlags();
#endif
  }
```

- [ ] **Step 2: Add the helpers to `MyMesh.cpp`** after `getLogDateTime()`

```cpp
#ifdef WITH_MQTT_BRIDGE_ETHERNET
bool MyMesh::roomLogSink(void* ctx, const char* suffix, const char* json, size_t len, bool retain) {
  MyMesh* self = (MyMesh*)ctx;
  return self->bridge && self->_prefs.bridge_enabled && self->bridge->publishRoomTopic(suffix, json, len, retain);
}

void MyMesh::roomLogTimestamp(char* buf, size_t buf_size) {
  MQTTMessageBuilder::formatIsoTimestampForMqtt((time_t)getRTCClock()->getCurrentTime(),
                                                (long)((_ms->getMillis() % 1000UL) * 1000UL), nullptr, buf, buf_size);
}

void MyMesh::logRoomEvent(const char* event, const uint8_t* actor, const char* result, const char* detail) {
  char ts[40]; roomLogTimestamp(ts, sizeof(ts));
  RoomLogPublisher::Event e = { event, actor, result, detail };
  _room_log.publishEvent(e, ts);
}

void MyMesh::applyRoomLogFlags() {
  EthernetObserverPrefs& eth = ethernetObserverPrefs();
  _room_log.setEnabled(eth.room_posts, eth.room_events, eth.room_dm, eth.room_stats);
}

void MyMesh::publishRoomStats() {
  RoomLogPublisher::Stats s = {};
  s.posts_total = _num_posted;
  uint32_t now = getRTCClock()->getCurrentTime();
  for (int i = 0; i < MAX_UNSYNCED_POSTS; i++) if (posts[i].post_timestamp && now < posts[i].post_timestamp + 86400UL) s.posts_unsynced++;
  for (int i = 0; i < acl.getNumClients(); i++) {
    switch (acl.getClientByIdx(i)->permissions & PERM_ACL_ROLE_MASK) {
      case PERM_ACL_ADMIN: s.clients_admin++; break;
      case PERM_ACL_READ_WRITE: s.clients_read_write++; break;
      case PERM_ACL_READ_ONLY: s.clients_read_only++; break;
      default: s.clients_guest++; break;
    }
  }
  if (_fs->exists("/s_contacts")) { File f = _fs->open("/s_contacts"); if (f) { s.acl_bytes = f.size(); f.close(); } }
  if (bridge) { s.packet_drops = bridge->packetDrops(); s.event_drops = bridge->eventDrops(); }
  char ts[40]; roomLogTimestamp(ts, sizeof(ts));
  _room_log.publishStats(s, ts);
}
#endif
```

Add `#include "helpers/MQTTMessageBuilder.h"` under the `WITH_MQTT_BRIDGE_ETHERNET` include block in `MyMesh.h`.

- [ ] **Step 3: Hook posts** at the end of `storePost` in `MyMesh.cpp`

```cpp
#ifdef WITH_MQTT_BRIDGE_ETHERNET
  {
    ClientInfo* c = acl.getClient(author.pub_key, PUB_KEY_SIZE);
    uint8_t perms = c ? c->permissions : PERM_ACL_ADMIN;   // system posts come from self_id
    char ts[40]; roomLogTimestamp(ts, sizeof(ts));
    RoomLogPublisher::Post p = { author.pub_key, perms, posts[idx].text, posts[idx].post_timestamp };
    _room_log.publishPost(p, ts);
  }
#endif
```

`client_timestamp` for a post is the sender's own timestamp; `storePost` does not receive it, so the room's post timestamp is used instead. Record this in the docs task as a known simplification.

- [ ] **Step 4: Hook logins** in `onAnonDataRecv`

Immediately after `MESH_DEBUG_PRINTLN("Incorrect room password");` and before its `return;`:

```cpp
#ifdef WITH_MQTT_BRIDGE_ETHERNET
        logRoomEvent("login_fail", sender.pub_key, "bad_password", "");
#endif
```

Immediately after `MESH_DEBUG_PRINTLN("possible replay attack!");` and before its `return;`:

```cpp
#ifdef WITH_MQTT_BRIDGE_ETHERNET
      logRoomEvent("login_fail", sender.pub_key, "replay", "");
#endif
```

Immediately after `MESH_DEBUG_PRINTLN("Login success!");`:

```cpp
#ifdef WITH_MQTT_BRIDGE_ETHERNET
      logRoomEvent("login_ok", sender.pub_key, "ok", RoomLogPublisher::roleName(perm));
#endif
```

- [ ] **Step 5: Hook direct messages and admin commands** in `onPeerDataRecv`

Inside the `if (flags == TXT_TYPE_CLI_DATA)` branch, in the `client->isAdmin()` and `!is_retry` case, immediately before `handleCommand(sender_timestamp, ...)`:

```cpp
#ifdef WITH_MQTT_BRIDGE_ETHERNET
            {
              char ts[40]; roomLogTimestamp(ts, sizeof(ts));
              RoomLogPublisher::DirectMessage m = { client->id.pub_key, client->permissions, "cli_data", true, (const char*)&data[5], sender_timestamp };
              _room_log.publishDirectMessage(m, ts);
              logRoomEvent("admin_cmd", client->id.pub_key, "ok", (const char*)&data[5]);
            }
#endif
```

In the non-admin `else` of that branch, before `temp[5] = 0;`:

```cpp
#ifdef WITH_MQTT_BRIDGE_ETHERNET
          {
            char ts[40]; roomLogTimestamp(ts, sizeof(ts));
            RoomLogPublisher::DirectMessage m = { client->id.pub_key, client->permissions, "cli_data", false, (const char*)&data[5], sender_timestamp };
            _room_log.publishDirectMessage(m, ts);
          }
#endif
```

In the `TXT_TYPE_PLAIN` branch's guest case, before `temp[5] = 0;`:

```cpp
#ifdef WITH_MQTT_BRIDGE_ETHERNET
          {
            char ts[40]; roomLogTimestamp(ts, sizeof(ts));
            RoomLogPublisher::DirectMessage m = { client->id.pub_key, client->permissions, "plain", false, (const char*)&data[5], sender_timestamp };
            _room_log.publishDirectMessage(m, ts);
          }
#endif
```

- [ ] **Step 6: Hook ACL changes** in the `setperm` branch of `handleCommand`, right after `strcpy(reply, "OK");`

```cpp
#ifdef WITH_MQTT_BRIDGE_ETHERNET
          logRoomEvent("acl_change", pubkey, "ok", command);
#endif
```

`pubkey` here is the target key parsed from the command; the actor is whoever issued the admin command, which the `admin_cmd` event already records.

- [ ] **Step 7: Wire begin and loop**

In `MyMesh::begin`, after `bridge->begin();` inside the `if (bridge)` block:

```cpp
#ifdef WITH_MQTT_BRIDGE_ETHERNET
      EthernetObserverPrefsStore::load(_fs, &ethernetObserverPrefs());
      bridge->setEthernetPrefs(&ethernetObserverPrefs());
      _room_log.setSink(roomLogSink, this);
      _room_log.setOrigin(_prefs.node_name, device_id);
      applyRoomLogFlags();
#endif
```

`device_id` is the local `char device_id[65]` declared a few lines up in that block; since `setOrigin` retains the pointer, move that array to a member: add `char _device_id_hex[65];` next to `bridge` in `MyMesh.h` and use it instead of the local in `begin()`. Note the ESP32 bridge copies the string so the member is harmless there.

In `MyMesh::loop`, replace the comment-only block

```cpp
#ifdef WITH_MQTT_BRIDGE
  // bridge.loop() is now handled by FreeRTOS task on Core 0 - no need to call it here
#endif
```

with

```cpp
#ifdef WITH_MQTT_BRIDGE_ETHERNET
  if (bridge && _prefs.bridge_enabled) {
    bridge->loop();
    unsigned long now_ms = _ms->getMillis();
    if ((long)(now_ms - _next_room_stats_at) >= 0) {
      publishRoomStats();
      _next_room_stats_at = now_ms + (unsigned long)_cli.getObserverPrefs()->mqtt_status_interval * 1000UL;
    }
  }
#else
#ifdef WITH_MQTT_BRIDGE
  // bridge.loop() is handled by the FreeRTOS task on Core 0 for ESP32 builds
#endif
#endif
```

Use the same interval units decision made in Task 8 Step 5.

- [ ] **Step 8: Build both platforms and verify on hardware**

Run: `pio run -e RAK_4631_room_server_observer_mqtt_ethernet 2>&1 | tail -3 && pio run -e heltec_v4_room_server_observer_mqtt 2>&1 | tail -3`
Expected: two `SUCCESS` lines.

On hardware with `mosquitto_sub -v -t 'meshcore/#'`: log in from a phone with the room password, post "test one". Expected in order: `room/events` with `"event":"login_ok"`, `room/posts` with `"text":"test one"` and `"role":"read_write"`. Log in with a wrong password: `room/events` with `"login_fail"` and `"result":"bad_password"`. Send an admin CLI command from the app: `room/dm` with `"kind":"cli_data"` and a `room/events` `admin_cmd`, and no field containing the reply. Within one status interval: a retained `room/stats` with `posts_total` 1.

- [ ] **Step 9: Commit**

```bash
git add examples/simple_room_server/MyMesh.h examples/simple_room_server/MyMesh.cpp
git commit -m "feat(room): publish posts, logins, direct messages, ACL changes and stats over MQTT"
```

---

### Task 11: Neighbors and auth token publishing

**Files:**
- Modify: `src/helpers/bridges/EthernetMQTTBridge.cpp` (`publishNeighbors`, `publishAuthTokens`, `requestPublishNeighbors`)
- Modify: `examples/simple_room_server/MyMesh.h` and `.cpp` (implement `NeighborSource`)

**Interfaces:**
- Consumes: `EthernetMQTTBridge::NeighborSource`, `MQTTPayloadBuilder::buildNeighborsMessage`, `JWTHelper::createAuthToken`, `MQTTEthernetPolicy::parseAudiences` and `tokenRenewalDue`, `MQTTEthernetPayloads::buildAuthMessage`.
- Produces: `neighbors` and `auth/{audience}` topics.

- [ ] **Step 1: Make the room server a NeighborSource**

In `MyMesh.h`, change the class header to also derive from the source under the flag:

```cpp
class MyMesh : public mesh::Mesh, public CommonCLICallbacks
#ifdef WITH_MQTT_BRIDGE_ETHERNET
  , public EthernetMQTTBridge::NeighborSource
#endif
```

Keep the existing `#ifdef WITH_WEBCONFIG` base clause that follows it intact. Add the member declaration:

```cpp
#ifdef WITH_MQTT_BRIDGE_ETHERNET
  int collectNeighbors(MQTTPayloadBuilder::NeighborsMessageEntry* out, int max_entries,
                       char* hex_pool, size_t hex_pool_size, uint32_t now_secs) override;
#endif
```

In `MyMesh.cpp`:

```cpp
#ifdef WITH_MQTT_BRIDGE_ETHERNET
int MyMesh::collectNeighbors(MQTTPayloadBuilder::NeighborsMessageEntry* out, int max_entries,
                             char* hex_pool, size_t hex_pool_size, uint32_t now_secs) {
  const size_t hex_size = PUB_KEY_SIZE * 2 + 1;
  int n = 0;
  for (int i = 0; i < MAX_NEIGHBOURS && n < max_entries; i++) {
    NeighbourInfo& nb = neighbours[i];
    if (nb.heard_timestamp == 0) continue;
    if ((size_t)(n + 1) * hex_size > hex_pool_size) break;
    char* hex = hex_pool + n * hex_size;
    mesh::Utils::toHex(hex, nb.id.pub_key, PUB_KEY_SIZE);
    out[n].pubkey_hex = hex;
    out[n].snr = nb.snr / 4.0f;
    bool known = now_secs >= nb.heard_timestamp;
    out[n].heard_unknown = !known;
    out[n].heard_secs_ago = known ? (now_secs - nb.heard_timestamp) : 0;
    out[n].scopes = "";
    out[n].status = "heard";
    n++;
  }
  return n;
}
#endif
```

In `MyMesh::begin`, in the block from Task 10 Step 7, add `bridge->setNeighborSource(this);`.

- [ ] **Step 2: Implement neighbors publishing in the bridge**

```cpp
void EthernetMQTTBridge::requestPublishNeighbors(const char* json, size_t len) {
  // Compatibility with the ESP32 path: publish caller-built JSON directly.
  char topic[128];
  if (!json || len == 0 || !topicFor("neighbors", topic, sizeof(topic))) return;
  bool ok = _client.publish(topic, (const uint8_t*)json, len, false);
  ok ? _publish_ok++ : _publish_err++;
}

bool EthernetMQTTBridge::publishNeighbors() {
  if (!_neighbor_source) return false;
  char topic[128];
  if (!topicFor("neighbors", topic, sizeof(topic))) return false;
  MQTTPayloadBuilder::NeighborsMessageEntry entries[NEIGHBORS_MAX_PUBLISH_ENTRIES];
  static char hex_pool[NEIGHBORS_MAX_PUBLISH_ENTRIES * (PUB_KEY_SIZE * 2 + 1)];
  uint32_t now = _rtc->getCurrentTime();
  int n = _neighbor_source->collectNeighbors(entries, NEIGHBORS_MAX_PUBLISH_ENTRIES, hex_pool, sizeof(hex_pool), now);
  char timestamp[40]; formatTimestamp(timestamp, sizeof(timestamp));
  int len = MQTTPayloadBuilder::buildNeighborsMessage(_doc, _origin, _device_id, timestamp, "", "",
                                                      entries, n, _json, sizeof(_json), n, 0, false);
  if (len <= 0) return false;
  bool ok = _client.publish(topic, (const uint8_t*)_json, len, false);
  ok ? _publish_ok++ : _publish_err++;
  return ok;
}
```

If `buildNeighborsMessage` needs more than the 1024-byte `_json` for 20 entries, publish at most the entries that fit by lowering `NEIGHBORS_MAX_PUBLISH_ENTRIES` to 12 in the header; the builder drops the tail on overflow per its comment in the room server.

- [ ] **Step 3: Implement auth token publishing**

Add private members to the header: `uint32_t _auth_issued_at[4]; char _auth_audiences[4][64]; int _auth_count;` and initialize them to zero in the constructor.

```cpp
bool EthernetMQTTBridge::publishAuthTokens() {
  if (!_eth || !_identity) return false;
  _auth_count = MQTTEthernetPolicy::parseAudiences(_eth->auth_audiences, _auth_audiences, 4);
  if (_auth_count == 0 || !_time_synced) return false;
  uint32_t now = _rtc->getCurrentTime();
  uint32_t life = _eth->auth_life_secs ? _eth->auth_life_secs : 3300;
  for (int i = 0; i < _auth_count; i++) {
    if (!MQTTEthernetPolicy::tokenRenewalDue(now, _auth_issued_at[i], life)) continue;
    static char token[600];
    if (!JWTHelper::createAuthToken(*_identity, _auth_audiences[i], now, life, token, sizeof(token),
                                    _obs->mqtt_owner_public_key[0] ? _obs->mqtt_owner_public_key : nullptr,
                                    "meshcore-nrf52-eth",
                                    _obs->mqtt_email[0] ? _obs->mqtt_email : nullptr)) {
      ETH_MQTT_LOG("jwt failed for %s", _auth_audiences[i]);
      continue;
    }
    char suffix[80]; snprintf(suffix, sizeof(suffix), "auth/%s", _auth_audiences[i]);
    char topic[160];
    if (!topicFor(suffix, topic, sizeof(topic))) continue;
    int len = MQTTEthernetPayloads::buildAuthMessage(_doc, _auth_audiences[i], _device_id, token, now, now + life, _json, sizeof(_json));
    if (len <= 0) continue;
    bool ok = _client.publish(topic, (const uint8_t*)_json, len, true);
    ok ? _publish_ok++ : _publish_err++;
    if (ok) _auth_issued_at[i] = now;
    return true;   // one token per pass
  }
  return false;
}
```

`topicFor` takes `char buf[128]` elsewhere; it only formats, so a 160-byte buffer here is fine. `_json` is 1024 bytes; a token is under 600 characters, so the message fits.

- [ ] **Step 4: Build and verify on hardware**

Run: `pio run -e RAK_4631_room_server_observer_mqtt_ethernet 2>&1 | tail -3`
Expected: `SUCCESS`.

With neighbors enabled (Task 12 provides `set mqtt.neighbors on`; until then set `mqtt_neighbors_enabled` default via `-D MQTT_DEFAULT_NEIGHBORS=1` if such a default exists in `MQTTDefaults.h`, otherwise verify after Task 12): after one neighbors interval a `neighbors` message lists heard nodes with `snr` and `heard_secs_ago`. With `set mqtt.auth.aud mqtt-us-v1.letsmesh.net` (Task 12) a retained `auth/mqtt-us-v1.letsmesh.net` message appears with a three-part token; paste it into `jwt.io`, confirm `aud` and `exp`.

- [ ] **Step 5: Commit**

```bash
git add src/helpers/bridges/EthernetMQTTBridge.h src/helpers/bridges/EthernetMQTTBridge.cpp examples/simple_room_server/MyMesh.h examples/simple_room_server/MyMesh.cpp
git commit -m "feat(nrf52): publish neighbors table and device-signed analyzer tokens"
```

---

### Task 12: nRF52 observer CLI

**Files:**
- Replace: `src/helpers/CommonCLI_ObserverNRF52.cpp`

**Interfaces:**
- Produces `CommonCLI::handleObserverSetCmd`, `handleObserverGetCmd`, `handleObserverCommand` for this build. The three are members declared in `CommonCLI.h:385-389`; `_mqtt_prefs`, `_prefs`, `_callbacks`, and `_board` are accessible members.
- `set` returns true when handled. Unsupported observer settings answer `ERR: not supported on this build` and return true so the caller does not fall through.

- [ ] **Step 1: Write the file**

```cpp
// nRF52 + RAK13800 observer CLI: the subset of CommonCLI_Observer.cpp that
// applies to a plain-MQTT Ethernet build. WiFi, SNMP, alert, OTA and the WSS
// analyzer shortcuts are answered with an explicit "not supported".
#include <Arduino.h>
#include "CommonCLI.h"
#include "MQTTPresets.h"
#include "MQTTEthernetPolicy.h"
#include "EthernetObserverPrefs.h"
#include "MQTTObserverValidation.h"
#include "bridges/MQTTBridge.h"

static const char* NOT_SUPPORTED = "ERR: not supported on this build";

static bool startsWith(const char* s, const char* prefix) { return strncmp(s, prefix, strlen(prefix)) == 0; }
static const char* afterPrefix(const char* s, const char* prefix) { const char* p = s + strlen(prefix); while (*p == ' ') p++; return p; }
static bool parseOnOff(const char* v, uint8_t* out) {
  if (strcmp(v, "on") == 0 || strcmp(v, "1") == 0) { *out = 1; return true; }
  if (strcmp(v, "off") == 0 || strcmp(v, "0") == 0) { *out = 0; return true; }
  return false;
}
static void copyStr(char* dst, size_t dst_size, const char* src) { strncpy(dst, src, dst_size - 1); dst[dst_size - 1] = 0; }

static bool isUnsupportedSetting(const char* config) {
  return startsWith(config, "wifi.") || startsWith(config, "snmp") || startsWith(config, "alert") ||
         startsWith(config, "mqtt.analyzer.") || startsWith(config, "mqtt2") || startsWith(config, "mqtt3") ||
         startsWith(config, "mqtt4") || startsWith(config, "mqtt5") || startsWith(config, "mqtt6");
}

bool CommonCLI::handleObserverSetCmd(uint32_t sender_timestamp, const char* config, char* reply) {
  EthernetObserverPrefs& eth = ethernetObserverPrefs();
  uint8_t flag;

  if (isUnsupportedSetting(config)) { strcpy(reply, NOT_SUPPORTED); return true; }

  if (startsWith(config, "mqtt.iata ")) {
    const char* v = afterPrefix(config, "mqtt.iata");
    if (!mqttIataValid(v)) { strcpy(reply, "ERR: iata must be 3 letters"); return true; }
    copyStr(_mqtt_prefs.mqtt_iata, sizeof(_mqtt_prefs.mqtt_iata), v);
    for (char* p = _mqtt_prefs.mqtt_iata; *p; p++) *p = toupper(*p);
  } else if (startsWith(config, "mqtt.origin ")) {
    copyStr(_mqtt_prefs.mqtt_origin, sizeof(_mqtt_prefs.mqtt_origin), afterPrefix(config, "mqtt.origin"));
  } else if (startsWith(config, "mqtt.owner ")) {
    copyStr(_mqtt_prefs.mqtt_owner_public_key, sizeof(_mqtt_prefs.mqtt_owner_public_key), afterPrefix(config, "mqtt.owner"));
  } else if (startsWith(config, "mqtt.email ")) {
    copyStr(_mqtt_prefs.mqtt_email, sizeof(_mqtt_prefs.mqtt_email), afterPrefix(config, "mqtt.email"));
  } else if (startsWith(config, "mqtt.ntp ")) {
    copyStr(_mqtt_prefs.mqtt_ntp_server, sizeof(_mqtt_prefs.mqtt_ntp_server), afterPrefix(config, "mqtt.ntp"));
  } else if (startsWith(config, "mqtt.interval ")) {
    uint32_t v = atoi(afterPrefix(config, "mqtt.interval"));
    if (v < 30 || v > 86400) { strcpy(reply, "ERR: 30..86400 seconds"); return true; }
    _mqtt_prefs.mqtt_status_interval = v;
  } else if (startsWith(config, "mqtt.neighbors.interval ")) {
    uint32_t v = atoi(afterPrefix(config, "mqtt.neighbors.interval"));
    if (v < 60 || v > 86400) { strcpy(reply, "ERR: 60..86400 seconds"); return true; }
    _mqtt_prefs.mqtt_neighbors_interval = v;
  } else if (startsWith(config, "mqtt.neighbors ")) {
    if (!parseOnOff(afterPrefix(config, "mqtt.neighbors"), &_mqtt_prefs.mqtt_neighbors_enabled)) { strcpy(reply, "ERR: on|off"); return true; }
  } else if (startsWith(config, "mqtt.status ")) {
    if (!parseOnOff(afterPrefix(config, "mqtt.status"), &_mqtt_prefs.mqtt_status_enabled)) { strcpy(reply, "ERR: on|off"); return true; }
  } else if (startsWith(config, "mqtt.packets ")) {
    if (!parseOnOff(afterPrefix(config, "mqtt.packets"), &_mqtt_prefs.mqtt_packets_enabled)) { strcpy(reply, "ERR: on|off"); return true; }
  } else if (startsWith(config, "mqtt.raw ")) {
    if (!parseOnOff(afterPrefix(config, "mqtt.raw"), &_mqtt_prefs.mqtt_raw_enabled)) { strcpy(reply, "ERR: on|off"); return true; }
  } else if (startsWith(config, "mqtt.rx ")) {
    if (!parseOnOff(afterPrefix(config, "mqtt.rx"), &_mqtt_prefs.mqtt_rx_enabled)) { strcpy(reply, "ERR: on|off"); return true; }
  } else if (startsWith(config, "mqtt.tx ")) {
    if (!parseOnOff(afterPrefix(config, "mqtt.tx"), &_mqtt_prefs.mqtt_tx_enabled)) { strcpy(reply, "ERR: on|off"); return true; }
  } else if (startsWith(config, "mqtt.room.posts ")) {
    if (!parseOnOff(afterPrefix(config, "mqtt.room.posts"), &eth.room_posts)) { strcpy(reply, "ERR: on|off"); return true; }
  } else if (startsWith(config, "mqtt.room.events ")) {
    if (!parseOnOff(afterPrefix(config, "mqtt.room.events"), &eth.room_events)) { strcpy(reply, "ERR: on|off"); return true; }
  } else if (startsWith(config, "mqtt.room.dm ")) {
    if (!parseOnOff(afterPrefix(config, "mqtt.room.dm"), &eth.room_dm)) { strcpy(reply, "ERR: on|off"); return true; }
  } else if (startsWith(config, "mqtt.room.stats ")) {
    if (!parseOnOff(afterPrefix(config, "mqtt.room.stats"), &eth.room_stats)) { strcpy(reply, "ERR: on|off"); return true; }
  } else if (startsWith(config, "mqtt.auth.aud ")) {
    copyStr(eth.auth_audiences, sizeof(eth.auth_audiences), afterPrefix(config, "mqtt.auth.aud"));
  } else if (startsWith(config, "mqtt.auth.life ")) {
    uint32_t v = atoi(afterPrefix(config, "mqtt.auth.life"));
    if (v < 300 || v > 86400) { strcpy(reply, "ERR: 300..86400 seconds"); return true; }
    eth.auth_life_secs = v;
  } else if (startsWith(config, "mqtt1.preset ")) {
    const char* name = afterPrefix(config, "mqtt1.preset");
    if (strcmp(name, MQTT_PRESET_NONE) != 0 && strcmp(name, MQTT_PRESET_CUSTOM) != 0) {
      const MQTTPresetDef* def = findMQTTPreset(name);
      if (!def) { strcpy(reply, "ERR: unknown preset"); return true; }
      if (!MQTTEthernetPolicy::presetUsableWithoutTls(def->server_url)) {
        strcpy(reply, "ERR: preset needs TLS; route it through the LAN proxy"); return true;
      }
    }
    copyStr(_mqtt_prefs.mqtt_slot_preset[0], sizeof(_mqtt_prefs.mqtt_slot_preset[0]), name);
    _callbacks->restartBridgeSlot(0);
  } else if (startsWith(config, "mqtt1.server ")) {
    copyStr(_mqtt_prefs.mqtt_slot_host[0], sizeof(_mqtt_prefs.mqtt_slot_host[0]), afterPrefix(config, "mqtt1.server"));
    _callbacks->restartBridgeSlot(0);
  } else if (startsWith(config, "mqtt1.port ")) {
    int v = atoi(afterPrefix(config, "mqtt1.port"));
    if (v <= 0 || v > 65535) { strcpy(reply, "ERR: 1..65535"); return true; }
    _mqtt_prefs.mqtt_slot_port[0] = (uint16_t)v;
    _callbacks->restartBridgeSlot(0);
  } else if (startsWith(config, "mqtt1.username ")) {
    copyStr(_mqtt_prefs.mqtt_slot_username[0], sizeof(_mqtt_prefs.mqtt_slot_username[0]), afterPrefix(config, "mqtt1.username"));
    _callbacks->restartBridgeSlot(0);
  } else if (sender_timestamp == 0 && startsWith(config, "mqtt1.password ")) {   // serial or telnet only
    copyStr(_mqtt_prefs.mqtt_slot_password[0], sizeof(_mqtt_prefs.mqtt_slot_password[0]), afterPrefix(config, "mqtt1.password"));
    _callbacks->restartBridgeSlot(0);
  } else if (startsWith(config, "mqtt.")  || startsWith(config, "mqtt1.")) {
    strcpy(reply, "ERR: unknown mqtt setting");
    return true;
  } else {
    return false;
  }
  (void)flag;
  _callbacks->savePrefs();
  strcpy(reply, "OK");
  return true;
}

bool CommonCLI::handleObserverGetCmd(uint32_t sender_timestamp, const char* config, char* reply) {
  EthernetObserverPrefs& eth = ethernetObserverPrefs();
  if (isUnsupportedSetting(config)) { strcpy(reply, NOT_SUPPORTED); return true; }
  #define ONOFF(x) ((x) ? "on" : "off")

  if (strcmp(config, "mqtt.iata") == 0) sprintf(reply, "> %s", _mqtt_prefs.mqtt_iata[0] ? _mqtt_prefs.mqtt_iata : "(unset)");
  else if (strcmp(config, "mqtt.origin") == 0) { char o[32]; MQTTBridge::getEffectiveMqttOrigin(_prefs, &_mqtt_prefs, o, sizeof(o)); sprintf(reply, "> %s", o); }
  else if (sender_timestamp == 0 && strcmp(config, "mqtt.owner") == 0) sprintf(reply, "> %s", _mqtt_prefs.mqtt_owner_public_key[0] ? _mqtt_prefs.mqtt_owner_public_key : "(unset)");
  else if (sender_timestamp == 0 && strcmp(config, "mqtt.email") == 0) sprintf(reply, "> %s", _mqtt_prefs.mqtt_email[0] ? _mqtt_prefs.mqtt_email : "(unset)");
  else if (strcmp(config, "mqtt.ntp") == 0) sprintf(reply, "> %s", _mqtt_prefs.mqtt_ntp_server[0] ? _mqtt_prefs.mqtt_ntp_server : "pool.ntp.org");
  else if (startsWith(config, "mqtt.ntp.diag")) _callbacks->runMqttNtpDiag(reply, 160, strstr(config, "verbose") != nullptr);
  else if (strcmp(config, "mqtt.interval") == 0) sprintf(reply, "> %lu", (unsigned long)_mqtt_prefs.mqtt_status_interval);
  else if (strcmp(config, "mqtt.neighbors.interval") == 0) sprintf(reply, "> %lu", (unsigned long)_mqtt_prefs.mqtt_neighbors_interval);
  else if (strcmp(config, "mqtt.neighbors") == 0) sprintf(reply, "> %s", ONOFF(_mqtt_prefs.mqtt_neighbors_enabled));
  else if (strcmp(config, "mqtt.status") == 0) MQTTBridge::formatMqttStatusReply(reply, 160, &_mqtt_prefs);
  else if (strcmp(config, "mqtt.stats") == 0) MQTTBridge::formatMqttStatsReply(reply, 160);
  else if (strcmp(config, "mqtt.packets") == 0) sprintf(reply, "> %s", ONOFF(_mqtt_prefs.mqtt_packets_enabled));
  else if (strcmp(config, "mqtt.raw") == 0) sprintf(reply, "> %s", ONOFF(_mqtt_prefs.mqtt_raw_enabled));
  else if (strcmp(config, "mqtt.rx") == 0) sprintf(reply, "> %s", ONOFF(_mqtt_prefs.mqtt_rx_enabled));
  else if (strcmp(config, "mqtt.tx") == 0) sprintf(reply, "> %s", ONOFF(_mqtt_prefs.mqtt_tx_enabled));
  else if (strcmp(config, "mqtt.room.posts") == 0) sprintf(reply, "> %s", ONOFF(eth.room_posts));
  else if (strcmp(config, "mqtt.room.events") == 0) sprintf(reply, "> %s", ONOFF(eth.room_events));
  else if (strcmp(config, "mqtt.room.dm") == 0) sprintf(reply, "> %s", ONOFF(eth.room_dm));
  else if (strcmp(config, "mqtt.room.stats") == 0) sprintf(reply, "> %s", ONOFF(eth.room_stats));
  else if (strcmp(config, "mqtt.auth.aud") == 0) sprintf(reply, "> %s", eth.auth_audiences[0] ? eth.auth_audiences : "(unset)");
  else if (strcmp(config, "mqtt.auth.life") == 0) sprintf(reply, "> %lu", (unsigned long)eth.auth_life_secs);
  else if (strcmp(config, "mqtt.config.valid") == 0) sprintf(reply, "> %s", MQTTBridge::isConfigValid(&_mqtt_prefs) ? "valid" : "invalid");
  else if (strcmp(config, "mqtt.presets") == 0) {
    int pos = sprintf(reply, "> ");
    for (int i = 0; i < MQTT_PRESET_COUNT && pos < 150; i++) {
      if (MQTTEthernetPolicy::presetUsableWithoutTls(MQTT_PRESETS[i].server_url)) pos += snprintf(reply + pos, 160 - pos, "%s ", MQTT_PRESETS[i].name);
    }
  }
  else if (strcmp(config, "mqtt1.preset") == 0) sprintf(reply, "> %s", _mqtt_prefs.mqtt_slot_preset[0]);
  else if (strcmp(config, "mqtt1.server") == 0) sprintf(reply, "> %s", _mqtt_prefs.mqtt_slot_host[0][0] ? _mqtt_prefs.mqtt_slot_host[0] : "(unset)");
  else if (strcmp(config, "mqtt1.port") == 0) sprintf(reply, "> %u", (unsigned)_mqtt_prefs.mqtt_slot_port[0]);
  else if (strcmp(config, "mqtt1.username") == 0) sprintf(reply, "> %s", _mqtt_prefs.mqtt_slot_username[0][0] ? _mqtt_prefs.mqtt_slot_username[0] : "(unset)");
  else if (strcmp(config, "mqtt1.password") == 0) sprintf(reply, "> %s", _mqtt_prefs.mqtt_slot_password[0][0] ? "(set)" : "(unset)");
  else if (startsWith(config, "mqtt.") || startsWith(config, "mqtt1.")) { strcpy(reply, "ERR: unknown mqtt setting"); }
  else return false;
  #undef ONOFF
  return true;
}

bool CommonCLI::handleObserverCommand(uint32_t sender_timestamp, char* command, char* reply) {
  if (startsWith(command, "tls.bundletest") || startsWith(command, "ota ") ||
      startsWith(command, "start webconfig") || startsWith(command, "stop webconfig")) {
    strcpy(reply, NOT_SUPPORTED);
    return true;
  }
  if (strcmp(command, "mqtt.ntp.sync") == 0) {
    strcpy(reply, _callbacks->syncMqttNtp() ? "OK: time synced" : "ERR: NTP sync failed");
    return true;
  }
  return false;
}
```

Check `mqttIataValid` in `src/helpers/MQTTObserverValidation.h` for its exact contract (it may require uppercase); if so, uppercase before validating. Confirm `_prefs` in `CommonCLI` is a `NodePrefs*` and `_callbacks->restartBridgeSlot(int)`, `runMqttNtpDiag`, and `syncMqttNtp` exist as shown at `src/helpers/CommonCLI.h:228-320`.

- [ ] **Step 2: Build and verify on hardware**

Run: `pio run -e RAK_4631_room_server_observer_mqtt_ethernet 2>&1 | tail -3`
Expected: `SUCCESS`.

Over telnet (`nc <ip> 23`) run, one per line:

```
set mqtt.iata SEA
set mqtt1.preset custom
set mqtt1.server 192.168.1.10
set mqtt1.port 1883
get mqtt.config.valid
set mqtt1.preset analyzer-us
get mqtt.presets
set wifi.ssid foo
set mqtt.auth.aud mqtt-us-v1.letsmesh.net
get mqtt.status
```

Expected replies in order: `OK`, `OK`, `OK`, `OK`, `> valid`, `ERR: preset needs TLS; route it through the LAN proxy`, a list containing `tennmesh nashmesh ctmesh eastidahomesh` and no `analyzer-us`, `ERR: not supported on this build`, `OK`, and a status line starting `> MQTT: ok link=up broker=192.168.1.10:1883`. Reboot the device and run `get mqtt.auth.aud`; expected `> mqtt-us-v1.letsmesh.net`, proving `/mqtt_eth_prefs` persisted.

- [ ] **Step 3: Commit**

```bash
git add src/helpers/CommonCLI_ObserverNRF52.cpp
git commit -m "feat(nrf52): observer CLI subset for the Ethernet room server build"
```

---

### Task 13: CI matrix, documentation, and final verification

**Files:**
- Modify: `.github/workflows/pr-build-check.yml` (the env list near line 36)
- Modify: `docs/cli_commands.md` (Ethernet section near line 1298)
- Modify: `MQTT_IMPLEMENTATION.md` (board list near line 185)

- [ ] **Step 1: Add the env to the PR build matrix**

Next to the existing `- RAK_4631_room_server_ethernet` entry add `- RAK_4631_room_server_observer_mqtt_ethernet`.

- [ ] **Step 2: Document the build in `docs/cli_commands.md`**

Under the Ethernet section add:

```markdown
#### MQTT observer over Ethernet (RAK4631 + RAK13800)

The `RAK_4631_room_server_observer_mqtt_ethernet` firmware publishes the observer topics (`status`, `packets`, `raw`, `neighbors`) plus a room log (`room/posts`, `room/events`, `room/dm`, `room/stats`), Ethernet `link` status, and device-signed analyzer tokens (`auth/{audience}`) to a plain MQTT broker on the LAN. TLS and the public analyzers are reached through a LAN proxy, not from the device.

Supported settings: `mqtt.iata`, `mqtt.origin`, `mqtt.owner`, `mqtt.email`, `mqtt.ntp`, `mqtt.interval`, `mqtt.status`, `mqtt.packets`, `mqtt.raw`, `mqtt.rx`, `mqtt.tx`, `mqtt.neighbors`, `mqtt.neighbors.interval`, `mqtt1.preset`, `mqtt1.server`, `mqtt1.port`, `mqtt1.username`, `mqtt1.password` (serial or telnet only).

New settings: `mqtt.room.posts`, `mqtt.room.events`, `mqtt.room.dm`, `mqtt.room.stats` (all default on); `mqtt.auth.aud` (comma-separated analyzer audiences, default empty); `mqtt.auth.life` (token lifetime in seconds, default 3300, renewed at two thirds).

Not available on this build: `wifi.*`, `snmp.*`, `alert.*`, `mqtt.analyzer.*`, `mqtt2` and above, `ota`, `webconfig`, `tls.bundletest`. `mqtt1.preset` only accepts presets with an `mqtt://` URL; `get mqtt.presets` lists them.

Notes: PubSubClient publishes at QoS 0; retained topics are `room/stats`, `link`, `auth/*`, and `status`. Room posts report the room's own timestamp as `client_timestamp`. Reconnect backoff starts at 10 s and caps at 5 min. Packets are held in a 16-entry ring and room events in an 8-entry ring while the broker is unreachable; overflow drops the oldest and counts in `room/stats`.
```

- [ ] **Step 3: Add the board to `MQTT_IMPLEMENTATION.md`**

After the ThinkNode M7 note add:

```markdown
**RAK4631 + RAK13800 (WisMesh Ethernet Gateway) — Ethernet, plain MQTT only:**
`RAK_4631_room_server_observer_mqtt_ethernet` uses `EthernetMQTTBridge`, a
separate nRF52 implementation behind the `MQTTBridge` alias in
`src/helpers/bridges/MQTTBridge.h`. It publishes the same `status`, `packets`,
`raw`, and `neighbors` JSON, plus room log topics, but has no TLS; WSS and
mqtts presets are rejected with a pointer to the LAN proxy. See
`docs/cli_commands.md` for the CLI subset.
```

- [ ] **Step 4: Full verification pass**

Run: `pio test -e native 2>&1 | tail -8`
Expected: all suites pass, including the five new ones.

Run: `pio run -e RAK_4631_room_server_observer_mqtt_ethernet -e RAK_4631_room_server_ethernet -e Heltec_v3_repeater_observer_mqtt -e heltec_v4_room_server_observer_mqtt 2>&1 | grep -E "SUCCESS|FAILED|RAM:|Flash:"`
Expected: four `SUCCESS` lines. Record the nRF52 env's RAM and Flash lines in the commit message and compare with Task 1; the delta is the feature cost.

Hardware soak: leave the gateway running against the local broker for one hour with a phone posting occasionally. Expected: no reboots (uptime in `status` keeps increasing), `get mqtt.stats` shows `pub_err` well under 1 percent of `pub_ok`, and `get mqtt.status` shows `ntp=1`.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/pr-build-check.yml docs/cli_commands.md MQTT_IMPLEMENTATION.md
git commit -m "docs(nrf52): document the RAK4631 Ethernet room server observer build

Final size: <paste RAM line> / <paste Flash line>"
```

---

## Self-review notes

- Spec 1.1 architecture: Tasks 1, 8, 9. Rings: Task 2 and 9. Reused helpers: Tasks 4, 8, 9. `RoomLogPublisher`: Tasks 7, 10. Build target: Task 1.
- Spec 1.2 topics: `status`, `link` in Task 8; `packets`, `raw`, room topics in Task 9 and 10; `neighbors`, `auth` in Task 11. QoS deviation recorded in Global Constraints and Task 13 docs.
- Spec 1.3 CLI: Task 12. Build-flag defaults: Task 1 env comments.
- Spec 1.4 failure handling: link down and backoff in Task 8; ring retry-then-drop in Task 9; prefs fallback in Task 5; NTP unsynced flag in Task 8.
- Spec 1.5 testing: native suites in Tasks 2, 3, 5, 6, 7; size gate in Task 1; hardware checks in Tasks 8 through 12; regression and CI in Task 13.
- Known deviations from spec, all recorded: no `author_name` or `actor_name` (no name field in `ClientInfo`); backoff ladder starts at 10 s; QoS 0 everywhere with retain where specified; post `client_timestamp` is the room's timestamp.
- Type consistency: `EthernetMQTTBridge::NeighborSource::collectNeighbors` signature matches between Task 1 header, Task 11 room server, and Task 11 bridge. `RoomLogPublisher` struct field orders match between Task 7 header and Task 10 initializers. `MQTTEthernetPayloads::LinkInfo` field order matches between Task 6 and Task 8.
