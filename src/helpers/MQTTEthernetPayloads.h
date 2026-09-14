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
