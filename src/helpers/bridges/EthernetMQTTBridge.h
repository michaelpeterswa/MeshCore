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
  static uint8_t getLastWifiDisconnectReason() { return 0; }
  static int getRuntimeSlotCount() { return 1; }
  unsigned long getSlotCurrentOutageStartMs(int slot_index) const;
  bool isSlotEnabledAndAttempted(int slot_index) const;
  const char* getSlotPresetName(int slot_index) const;

  // ---- Ethernet-build additions ----
  void setNeighborSource(NeighborSource* src) { _neighbor_source = src; }
  void setEthernetPrefs(EthernetObserverPrefs* eth) { _eth = eth; }
  /** Sink for RoomLogPublisher: queues JSON on the event ring. */
  bool publishRoomTopic(const char* suffix, const char* json, size_t len, bool retain);
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
  // mutable: PubSubClient::connected() is non-const, but brokerConnected() must be.
  mutable PubSubClient _client;

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
  // Cached DNS results: resolveHost() must run before any socket-holding
  // connect()/beginPacket() call, never a hostname string, or the RAK13800
  // library resolves it internally (blocking loop() ~15s and briefly opening
  // a fifth UDP socket on a four-socket chip). Unset == IPAddress((uint32_t)0).
  IPAddress _broker_ip;
  IPAddress _ntp_ip;
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
  // Per-topic progress for the current _packet_ring front entry. Reset to
  // false whenever the front entry changes (pop(), or push() dropping the
  // front while a retry was pending) so state never leaks onto the next
  // entry. A topic disabled by prefs is treated as done without ever being
  // set here.
  bool _front_packets_done;
  bool _front_raw_done;

  JsonDocument _doc;
  char _json[1024];

  // Auth token state: up to 4 audiences parsed from _eth->auth_audiences.
  uint32_t _auth_issued_at[4];
  char _auth_audiences[4][64];
  int _auth_count;

  static EthernetMQTTBridge* s_instance;

  bool linkReady() const;
  void refreshLink(unsigned long now);
  // Resolves a dotted-quad literal instantly via IPAddress::fromString(), else
  // does a single DNSClient::getHostByName() with a ~1s-bounded timeout (see
  // the .cpp: getHostByName's `timeout` arg is per-attempt and it retries up
  // to 3x internally, so the passed timeout is well under 1000ms).
  // Never pass `host` on to connect()/beginPacket() directly.
  bool resolveHost(const char* host, IPAddress& out) const;
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
