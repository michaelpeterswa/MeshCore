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
