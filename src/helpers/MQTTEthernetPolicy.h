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
