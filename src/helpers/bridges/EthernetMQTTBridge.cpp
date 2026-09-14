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
