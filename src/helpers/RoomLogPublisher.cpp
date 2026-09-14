// Room server MQTT log formatting. The base PlatformIO config globs every
// src/helpers/*.cpp into every firmware env, so this is guarded here rather
// than in the build filter — same idiom as helpers/JWTHelper.cpp — but the
// native test env (no ARDUINO, no WITH_MQTT_BRIDGE_ETHERNET) must still see it.
#if defined(WITH_MQTT_BRIDGE_ETHERNET) || !defined(ARDUINO)

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

#endif  // WITH_MQTT_BRIDGE_ETHERNET || !ARDUINO
