#pragma once

#include <stddef.h>
#include <stdint.h>
#include <ArduinoJson.h>

/**
 * Formats the room server's MQTT log topics and hands each JSON document to
 * a sink. Platform-neutral: no Arduino, no bridge dependency. The sink is
 * expected to queue, not block. The origin strings passed to setOrigin() are
 * retained by pointer, not copied — see setOrigin() for the lifetime
 * requirement.
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

  static const size_t JSON_BUFFER_SIZE = 768;

  RoomLogPublisher();
  void setSink(Sink fn, void* ctx) { _sink = fn; _ctx = ctx; }
  // Stores the pointers as given, not copies of the strings — origin and
  // origin_id must outlive this RoomLogPublisher (or be updated again before
  // they are freed/reused).
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
