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

// The audience is placed directly into an MQTT topic segment (see
// RoomLogPublisher/JWTHelper), so '+'/'#' (MQTT wildcard characters) or a
// control byte would let a device-signed PUBLISH topic be mistaken for a
// subscription filter, which makes conforming brokers drop the connection.
static inline bool audienceValid(const char* s) {
  for (const unsigned char* p = (const unsigned char*)s; *p; p++) {
    if (*p == '+' || *p == '#' || *p < 0x20 || *p == 0x7F) return false;
  }
  return true;
}

// Copies cmd into out, replacing the argument of secret-bearing commands with "<redacted>".
// Redacts: `password <arg>`, and `set <key> <arg>` where <key> contains (case-insensitively)
// "password", "prv.key", or "psk". Everything up to and including the key token is kept;
// the rest of the command is replaced with " <redacted>". Anything else is copied unchanged.
static inline void redactAdminCommand(const char* cmd, char* out, size_t out_size) {
  if (!out || out_size == 0) return;
  if (!cmd) { out[0] = 0; return; }

  const char* p = cmd;
  while (*p == ' ') p++;
  const char* tok1_start = p;
  while (*p && *p != ' ') p++;
  size_t tok1_len = (size_t)(p - tok1_start);

  bool redact = false;
  size_t keep_len = 0; // bytes of cmd (from the start) to keep before appending " <redacted>"

  if (tok1_len == 8 && strncmp(tok1_start, "password", 8) == 0) {
    redact = true;
    keep_len = (size_t)(p - cmd);
  } else if (tok1_len == 3 && strncmp(tok1_start, "set", 3) == 0) {
    const char* q = p;
    while (*q == ' ') q++;
    const char* tok2_start = q;
    while (*q && *q != ' ') q++;
    size_t tok2_len = (size_t)(q - tok2_start);
    if (tok2_len > 0) {
      char key[64];
      size_t klen = tok2_len < sizeof(key) - 1 ? tok2_len : sizeof(key) - 1;
      for (size_t i = 0; i < klen; i++) {
        char c = tok2_start[i];
        if (c >= 'A' && c <= 'Z') c = (char)(c - 'A' + 'a');
        key[i] = c;
      }
      key[klen] = 0;
      if (strstr(key, "password") || strstr(key, "prv.key") || strstr(key, "psk")) {
        redact = true;
        keep_len = (size_t)(q - cmd);
      }
    }
  }

  size_t cap = out_size - 1;
  if (redact) {
    size_t n = keep_len < cap ? keep_len : cap;
    memcpy(out, cmd, n);
    static const char kSuffix[] = " <redacted>";
    size_t rem = cap - n;
    size_t sn = sizeof(kSuffix) - 1 < rem ? sizeof(kSuffix) - 1 : rem;
    memcpy(out + n, kSuffix, sn);
    out[n + sn] = 0;
  } else {
    size_t n = strlen(cmd);
    if (n > cap) n = cap;
    memcpy(out, cmd, n);
    out[n] = 0;
  }
}

}  // namespace MQTTEthernetPolicy
