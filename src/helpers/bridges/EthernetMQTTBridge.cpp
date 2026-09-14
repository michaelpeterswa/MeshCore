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
#include <Dns.h>
#include <string.h>

EthernetMQTTBridge* EthernetMQTTBridge::s_instance = nullptr;

EthernetMQTTBridge::EthernetMQTTBridge(NodePrefs* prefs, MQTTPrefs* obs, mesh::PacketManager* mgr,
                                       mesh::RTCClock* rtc, mesh::LocalIdentity* identity)
  : BridgeBase(prefs, mgr, rtc), _obs(obs), _eth(nullptr), _identity(identity),
    _dispatcher(nullptr), _radio(nullptr), _board(nullptr), _ms(nullptr), _neighbor_source(nullptr),
    _client(_net), _link_up(false), _link_changed_at(0), _link_changes(0),
    _last_connect_attempt(0), _backoff(0), _broker_reconnects(0),
    _broker_ip((uint32_t)0), _ntp_ip((uint32_t)0), _outage_started_at(0),
    _publish_ok(0), _publish_err(0), _slot_state("inactive"), _time_synced(false),
    _next_status_at(0), _next_ntp_at(0), _next_neighbors_at(0), _next_link_at(0), _next_auth_at(0),
    _staged_raw_len(0), _staged_snr(0), _staged_rssi(0), _staged_valid(false),
    _packet_retry_pending(false), _event_retry_pending(false) {
  _device_id[0] = _firmware_version[0] = _board_model[0] = _build_date[0] = 0;
  _origin[0] = _iata[0] = _client_id[0] = 0;
  s_instance = this;
}

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
int EthernetMQTTBridge::getQueueSize() const { return (int)(_packet_ring.size() + _event_ring.size()); }
void EthernetMQTTBridge::requestPublishNeighbors(const char*, size_t) {}

void EthernetMQTTBridge::getEffectiveMqttOrigin(const NodePrefs* np, const MQTTPrefs* obs, char* buf, size_t buf_size) {
  const char* src = (obs && obs->mqtt_origin[0]) ? obs->mqtt_origin : (np ? np->node_name : "");
  strncpy(buf, src, buf_size - 1); buf[buf_size - 1] = 0;
}
unsigned long EthernetMQTTBridge::getSlotCurrentOutageStartMs(int) const { return _outage_started_at; }
bool EthernetMQTTBridge::isSlotEnabledAndAttempted(int slot_index) const { return slot_index == 0 && _initialized; }
const char* EthernetMQTTBridge::getSlotPresetName(int) const { return _obs ? _obs->mqtt_slot_preset[0] : ""; }

bool EthernetMQTTBridge::publishRoomTopic(const char*, const char*, size_t, bool) { return false; }

bool EthernetMQTTBridge::publishNeighbors() { return false; }
bool EthernetMQTTBridge::publishAuthTokens() { return false; }
bool EthernetMQTTBridge::drainPacketRing() { return false; }
bool EthernetMQTTBridge::drainEventRing() { return false; }

// ---- Step 1: link and topic helpers ----

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
    // "fire immediately": = now, not 0. now - 0 only stays non-negative under
    // (long) for the first ~24.8 days of uptime; now - now is always exactly
    // due and stays wrap-safe forever.
    _next_link_at = now;
  }
}

// Never hand a hostname to connect()/beginPacket(): the RAK13800 library
// resolves it internally via a blocking, ~5s/3-retry DNSClient lookup that
// also opens its own UDP socket (RAK13800-W5100S/src/EthernetClient.cpp:26-38,
// RAK13800-W5100S/src/Dns.cpp), which can stall loop() for ~15s and exhaust
// the W5100S's four sockets alongside telnet + MQTT + NTP. Resolve here with a
// short bounded timeout instead, and pass only the resulting IPAddress on.
bool EthernetMQTTBridge::resolveHost(const char* host, IPAddress& out) const {
  if (!host || host[0] == '\0') return false;
  if (out.fromString(host)) return true;   // dotted-quad literal: no DNS needed
  DNSClient dns;
  dns.begin(Ethernet.dnsServerIP());
  // getHostByName()'s `timeout` argument is a PER-ATTEMPT wait, not a total
  // budget: on no response it retries ProcessResponse(timeout) up to 3 times
  // internally before giving up (RAK13800-W5100S/src/Dns.cpp:106-124). Passing
  // 1000 here would let a single call block up to ~3s. 334ms keeps the worst
  // case (3 * 334ms ~= 1s) near the ~1s bound documented on the header
  // declaration — do not "round this up" to 1000 without re-deriving that
  // budget against the x3 multiplier.
  int rc = dns.getHostByName(host, out, 334);
  return rc == 1;
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

// ---- Step 2: begin, config validity, connect, disconnect ----

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
  // Explicit "= millis()" rather than relying on the constructor's 0: the
  // constructor's 0 only reads as "due now" because millis() is still small
  // at boot; begin() can run later (e.g. after end()/begin() cycling), so set
  // these the same wrap-safe way refreshLink()/connectBroker() do.
  unsigned long boot_now = millis();
  _next_status_at = boot_now;
  _next_link_at = boot_now;
  _next_ntp_at = boot_now;
  _next_neighbors_at = boot_now;
  _next_auth_at = boot_now;
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
  if (def->auth_type == MQTT_AUTH_USERPASS && def->userpass_username && !mqttPresetUsesDevicePubkeyUsername(def)) {
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

  // Re-resolve only when the cache is empty (never resolved, or invalidated
  // by a previous failure / preset change) — never pass `host` on to
  // _client.connect(const char*, ...), which would trigger the RAK13800
  // library's internal blocking hostname lookup instead (see resolveHost()).
  bool have_cached_ip = !(_broker_ip == IPAddress((uint32_t)0));
  if (!have_cached_ip) {
    if (!resolveHost(host, _broker_ip)) {
      _slot_state = "fail";
      if (_outage_started_at == 0) _outage_started_at = now;
      if (_backoff < 5) _backoff++;
      ETH_MQTT_LOG("dns resolve failed for %s, backoff step %u", host, (unsigned)_backoff);
      return false;
    }
  }
  _client.setServer(_broker_ip, port);
  ETH_MQTT_LOG("connecting %s:%u as %s", host, (unsigned)port, _client_id);
  bool ok = user ? _client.connect(_client_id, user, pass) : _client.connect(_client_id);
  if (!ok) {
    _slot_state = "fail";
    if (_outage_started_at == 0) _outage_started_at = now;
    // MQTTConnectionPolicy::advanceBackoff() also tracks a max-backoff failure
    // counter and a circuit breaker this bridge does not maintain; its ladder
    // (reconnectBackoffMs) saturates at index 5, so a plain clamp reproduces
    // the same delays without that extra state.
    if (_backoff < 5) _backoff++;
    // A failed connect counts as a failed cache entry too: the broker may
    // have moved (DHCP re-lease, DNS record change), so force a fresh
    // resolveHost() on the next attempt instead of retrying a stale IP.
    _broker_ip = IPAddress((uint32_t)0);
    ETH_MQTT_LOG("connect failed rc=%d, backoff step %u", _client.state(), (unsigned)_backoff);
    return false;
  }
  if (_outage_started_at != 0) _broker_reconnects++;
  _outage_started_at = 0;
  _backoff = 0;
  _slot_state = "ok";
  // "fire immediately", wrap-safe: see refreshLink()'s comment.
  _next_status_at = now;
  _next_link_at = now;
  _next_auth_at = now;
  if (!_time_synced) _next_ntp_at = now;
  ETH_MQTT_LOG("connected");
  return true;
}

void EthernetMQTTBridge::onDisconnected(unsigned long now) {
  if (_slot_state[0] == 'o') _slot_state = "disc";
  if (_outage_started_at == 0) _outage_started_at = now;
}

// ---- Step 3: NTP ----

bool EthernetMQTTBridge::syncNtp(uint32_t timeout_ms) {
  if (!_link_up) return false;
  const char* server = (_obs && _obs->mqtt_ntp_server[0]) ? _obs->mqtt_ntp_server : "pool.ntp.org";
  // Start the deadline clock BEFORE resolving: resolveHost() itself can block
  // (up to ~1s; see its own comment), and that time must count against
  // timeout_ms, not be added on top of it, or syncNtp(2000) could really take
  // ~3s end to end.
  unsigned long start = millis();
  // Resolve before opening the NTP UDP socket: NTPClient(udp, IPAddress) skips
  // its own internal beginPacket(hostname, ...) DNS lookup entirely (it only
  // does that when constructed with a `const char*` and _poolServerName is
  // non-null; see NTPClient::sendNTPPacket() in NTPClient.cpp), so nothing
  // else in this function can trigger the blocking hostname path.
  if (!resolveHost(server, _ntp_ip)) {
    ETH_MQTT_LOG("ntp dns failed for %s", server);
    return false;
  }
  if ((unsigned long)(millis() - start) >= timeout_ms) {
    // DNS alone ate the whole budget; do not even open the UDP socket.
    ETH_MQTT_LOG("ntp dns for %s used the full %lu ms budget", server, (unsigned long)timeout_ms);
    return false;
  }
  EthernetUDP udp;                       // scoped: frees the W5100S socket on return
  NTPClient ntp(udp, _ntp_ip);
  ntp.begin();
  // NTPClient::forceUpdate() waits up to ~1s internally per call (see
  // NTPClient.cpp: up to 100 * 10ms delay iterations), so bound both the
  // elapsed wall time (measured from `start`, i.e. including the DNS lookup
  // above) AND the attempt count to timeout_ms / 1000 (at least one attempt),
  // checking the deadline before every call.
  uint32_t max_attempts = timeout_ms / 1000UL;
  if (max_attempts < 1) max_attempts = 1;
  bool ok = false;
  for (uint32_t attempt = 0; attempt < max_attempts && !ok; attempt++) {
    if ((unsigned long)(millis() - start) >= timeout_ms) break;
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
  if (timeout_ms > 5000UL) timeout_ms = 5000UL;   // cap regardless of caller's argument
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

// ---- Step 4: status and link publishing ----

// mallinfo().fordblks is free space *inside the arena newlib has already
// carved from the heap* — not free RAM, since the arena only grows on demand
// and never shrinks. The Adafruit nRF52 core's cores/nRF5/utility/debug.h
// (declared unconditionally — not gated behind CFG_DEBUG — and pulled in
// transitively via MeshCore.h -> Arduino.h) provides dbgHeapFree(), a static
// inline already computing dbgHeapTotal() - dbgHeapUsed() (true heap ceiling
// __HeapLimit - __HeapBase minus bytes actually allocated, mallinfo().uordblks;
// see debug.h:58-61), which is the real free-RAM figure.
static int freeHeapBytes() {
  return dbgHeapFree();
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
  // MQTTPayloadBuilder::buildStatusMessage() serializes the JsonObject it built
  // on _doc directly into _json and does not clear _doc afterward, so _doc
  // still holds the same tree here; adding a field and re-serializing works.
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

// ---- Step 5: loop ----

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
    _next_status_at = now + _obs->mqtt_status_interval;
    return;
  }
  if (drainEventRing()) return;
  if (drainPacketRing()) return;
  if (_obs->mqtt_neighbors_enabled && (long)(now - _next_neighbors_at) >= 0) {
    publishNeighbors();
    _next_neighbors_at = now + _obs->mqtt_neighbors_interval;
    return;
  }
  if ((long)(now - _next_auth_at) >= 0) {
    publishAuthTokens();
    _next_auth_at = now + 60000UL;   // policy decides whether a token is actually due
  }
}

// ---- Step 6: static CLI helpers ----

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
  _broker_ip = IPAddress((uint32_t)0);   // preset/host may have changed; force re-resolve
}
