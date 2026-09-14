// Every variant's build_src_filter globs helpers/*.cpp, so this TU is
// compiled into every env, not just the nRF52 ethernet observer. Guard it
// like helpers/esp32/WebConfigServer.cpp and helpers/JWTHelper.cpp do, so it
// is an empty TU everywhere except the one env that excludes
// CommonCLI_Observer.cpp in its place.
//
// nRF52 + RAK13800 observer CLI: the subset of CommonCLI_Observer.cpp that
// applies to a plain-MQTT Ethernet build. WiFi, SNMP, alert, OTA and the WSS
// analyzer shortcuts are answered with an explicit "not supported".
#if defined(WITH_MQTT_BRIDGE_ETHERNET)
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

// The audience is placed directly into an MQTT topic segment (see
// RoomLogPublisher/JWTHelper), so '+'/'#' (MQTT wildcard characters) or a
// control byte would let a device-signed PUBLISH topic be mistaken for a
// subscription filter, which makes conforming brokers drop the connection.
static bool audienceValid(const char* s) {
  for (const unsigned char* p = (const unsigned char*)s; *p; p++) {
    if (*p == '+' || *p == '#' || *p < 0x20 || *p == 0x7F) return false;
  }
  return true;
}

static bool isUnsupportedSetting(const char* config) {
  return startsWith(config, "wifi.") || startsWith(config, "snmp") || startsWith(config, "alert") ||
         startsWith(config, "mqtt.analyzer.") || startsWith(config, "mqtt2") || startsWith(config, "mqtt3") ||
         startsWith(config, "mqtt4") || startsWith(config, "mqtt5") || startsWith(config, "mqtt6");
}

bool CommonCLI::handleObserverSetCmd(uint32_t sender_timestamp, const char* config, char* reply) {
  EthernetObserverPrefs& eth = ethernetObserverPrefs();

  if (isUnsupportedSetting(config)) { strcpy(reply, NOT_SUPPORTED); return true; }

  if (startsWith(config, "mqtt.iata ")) {
    const char* v = afterPrefix(config, "mqtt.iata");
    if (!mqttIataValid(v)) { strcpy(reply, "ERR: iata must be 3 letters"); return true; }
    copyStr(_mqtt_prefs.mqtt_iata, sizeof(_mqtt_prefs.mqtt_iata), v);
    for (char* p = _mqtt_prefs.mqtt_iata; *p; p++) *p = toupper((unsigned char)*p);
  } else if (startsWith(config, "mqtt.origin ")) {
    copyStr(_mqtt_prefs.mqtt_origin, sizeof(_mqtt_prefs.mqtt_origin), afterPrefix(config, "mqtt.origin"));
  } else if (startsWith(config, "mqtt.owner ")) {
    copyStr(_mqtt_prefs.mqtt_owner_public_key, sizeof(_mqtt_prefs.mqtt_owner_public_key), afterPrefix(config, "mqtt.owner"));
  } else if (startsWith(config, "mqtt.email ")) {
    copyStr(_mqtt_prefs.mqtt_email, sizeof(_mqtt_prefs.mqtt_email), afterPrefix(config, "mqtt.email"));
  } else if (startsWith(config, "mqtt.ntp ")) {
    copyStr(_mqtt_prefs.mqtt_ntp_server, sizeof(_mqtt_prefs.mqtt_ntp_server), afterPrefix(config, "mqtt.ntp"));
  } else if (startsWith(config, "mqtt.interval ")) {
    // Minutes in, milliseconds stored -- mirrors CommonCLI_Observer.cpp (the
    // ESP32 observer CLI) so existing scripts written against that unit keep
    // working unchanged on this build.
    uint32_t minutes = (uint32_t)atoi(afterPrefix(config, "mqtt.interval"));
    if (minutes < 1 || minutes > 60) { strcpy(reply, "ERR: interval must be 1-60 minutes"); return true; }
    _mqtt_prefs.mqtt_status_interval = minutes * 60000UL;
    _callbacks->savePrefs();
    _callbacks->restartBridge();
    snprintf(reply, 160, "OK - interval set to %u minutes (%lu ms)",
             (unsigned)minutes, (unsigned long)_mqtt_prefs.mqtt_status_interval);
    return true;
  } else if (startsWith(config, "mqtt.neighbors.interval ")) {
    // Hours in, milliseconds stored -- same 12-336h band and unit as
    // CommonCLI_Observer.cpp.
    uint32_t hours = (uint32_t)atoi(afterPrefix(config, "mqtt.neighbors.interval"));
    if (hours < MQTT_NEIGHBORS_MIN_INTERVAL_HOURS || hours > MQTT_NEIGHBORS_MAX_INTERVAL_HOURS) {
      snprintf(reply, 160, "ERR: neighbors interval must be %lu-%lu hours",
               (unsigned long)MQTT_NEIGHBORS_MIN_INTERVAL_HOURS, (unsigned long)MQTT_NEIGHBORS_MAX_INTERVAL_HOURS);
      return true;
    }
    _mqtt_prefs.mqtt_neighbors_interval = hours * 3600000UL;
    _callbacks->savePrefs();
    _callbacks->restartBridge();
    snprintf(reply, 160, "OK - neighbors interval set to %u hours (%lu ms)",
             (unsigned)hours, (unsigned long)_mqtt_prefs.mqtt_neighbors_interval);
    return true;
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
    // The audience becomes an MQTT topic segment (see JWTHelper/RoomLogPublisher),
    // so reject wildcard/control bytes up front rather than let a bad value take
    // the bridge's connection down later.
    const char* v = afterPrefix(config, "mqtt.auth.aud");
    if (!audienceValid(v)) { strcpy(reply, "ERR: audience must not contain + # or control characters"); return true; }
    copyStr(eth.auth_audiences, sizeof(eth.auth_audiences), v);
  } else if (startsWith(config, "mqtt.auth.life ")) {
    uint32_t v = (uint32_t)atoi(afterPrefix(config, "mqtt.auth.life"));
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
  } else if (startsWith(config, "mqtt.") || startsWith(config, "mqtt1.")) {
    strcpy(reply, "ERR: unknown mqtt setting");
    return true;
  } else {
    return false;
  }
  _callbacks->savePrefs();
  strcpy(reply, "OK");
  return true;
}

bool CommonCLI::handleObserverGetCmd(uint32_t sender_timestamp, const char* config, char* reply) {
  EthernetObserverPrefs& eth = ethernetObserverPrefs();
  if (isUnsupportedSetting(config)) { strcpy(reply, NOT_SUPPORTED); return true; }
  #define ONOFF(x) ((x) ? "on" : "off")

  if (strcmp(config, "mqtt.iata") == 0) snprintf(reply, 160, "> %s", _mqtt_prefs.mqtt_iata[0] ? _mqtt_prefs.mqtt_iata : "(unset)");
  else if (strcmp(config, "mqtt.origin") == 0) { char o[32]; MQTTBridge::getEffectiveMqttOrigin(_prefs, &_mqtt_prefs, o, sizeof(o)); snprintf(reply, 160, "> %s", o); }
  else if (sender_timestamp == 0 && strcmp(config, "mqtt.owner") == 0) snprintf(reply, 160, "> %s", _mqtt_prefs.mqtt_owner_public_key[0] ? _mqtt_prefs.mqtt_owner_public_key : "(unset)");
  else if (sender_timestamp == 0 && strcmp(config, "mqtt.email") == 0) snprintf(reply, 160, "> %s", _mqtt_prefs.mqtt_email[0] ? _mqtt_prefs.mqtt_email : "(unset)");
  else if (strcmp(config, "mqtt.ntp") == 0) snprintf(reply, 160, "> %s", _mqtt_prefs.mqtt_ntp_server[0] ? _mqtt_prefs.mqtt_ntp_server : "pool.ntp.org");
  else if (startsWith(config, "mqtt.ntp.diag")) _callbacks->runMqttNtpDiag(reply, 160, strstr(config, "verbose") != nullptr);
  else if (strcmp(config, "mqtt.interval") == 0) {
    // Minutes rounded up, mirroring CommonCLI_Observer.cpp's (ms + 29999) / 60000.
    uint32_t minutes = (_mqtt_prefs.mqtt_status_interval + 29999) / 60000;
    snprintf(reply, 160, "> %lu minutes (%lu ms)", (unsigned long)minutes, (unsigned long)_mqtt_prefs.mqtt_status_interval);
  }
  else if (strcmp(config, "mqtt.neighbors.interval") == 0) {
    uint32_t hours = (_mqtt_prefs.mqtt_neighbors_interval + 3599999) / 3600000;
    snprintf(reply, 160, "> %lu hours (%lu ms)", (unsigned long)hours, (unsigned long)_mqtt_prefs.mqtt_neighbors_interval);
  }
  else if (strcmp(config, "mqtt.neighbors") == 0) snprintf(reply, 160, "> %s", ONOFF(_mqtt_prefs.mqtt_neighbors_enabled));
  else if (strcmp(config, "mqtt.status") == 0) MQTTBridge::formatMqttStatusReply(reply, 160, &_mqtt_prefs);
  else if (strcmp(config, "mqtt.stats") == 0) MQTTBridge::formatMqttStatsReply(reply, 160);
  else if (strcmp(config, "mqtt.packets") == 0) snprintf(reply, 160, "> %s", ONOFF(_mqtt_prefs.mqtt_packets_enabled));
  else if (strcmp(config, "mqtt.raw") == 0) snprintf(reply, 160, "> %s", ONOFF(_mqtt_prefs.mqtt_raw_enabled));
  else if (strcmp(config, "mqtt.rx") == 0) snprintf(reply, 160, "> %s", ONOFF(_mqtt_prefs.mqtt_rx_enabled));
  else if (strcmp(config, "mqtt.tx") == 0) snprintf(reply, 160, "> %s", ONOFF(_mqtt_prefs.mqtt_tx_enabled));
  else if (strcmp(config, "mqtt.room.posts") == 0) snprintf(reply, 160, "> %s", ONOFF(eth.room_posts));
  else if (strcmp(config, "mqtt.room.events") == 0) snprintf(reply, 160, "> %s", ONOFF(eth.room_events));
  else if (strcmp(config, "mqtt.room.dm") == 0) snprintf(reply, 160, "> %s", ONOFF(eth.room_dm));
  else if (strcmp(config, "mqtt.room.stats") == 0) snprintf(reply, 160, "> %s", ONOFF(eth.room_stats));
  else if (strcmp(config, "mqtt.auth.aud") == 0) snprintf(reply, 160, "> %s", eth.auth_audiences[0] ? eth.auth_audiences : "(unset)");
  else if (strcmp(config, "mqtt.auth.life") == 0) snprintf(reply, 160, "> %lu", (unsigned long)eth.auth_life_secs);
  else if (strcmp(config, "mqtt.config.valid") == 0) snprintf(reply, 160, "> %s", MQTTBridge::isConfigValid(&_mqtt_prefs) ? "valid" : "invalid");
  else if (strcmp(config, "mqtt.presets") == 0) {
    // Only list presets this build can actually use without TLS -- a WSS/mqtts
    // preset here would just bounce off "preset needs TLS" if selected.
    int pos = snprintf(reply, 160, "> ");
    for (int i = 0; i < MQTT_PRESET_COUNT && pos < 150; i++) {
      if (MQTTEthernetPolicy::presetUsableWithoutTls(MQTT_PRESETS[i].server_url)) pos += snprintf(reply + pos, 160 - pos, "%s ", MQTT_PRESETS[i].name);
    }
  }
  else if (strcmp(config, "mqtt1.preset") == 0) snprintf(reply, 160, "> %s", _mqtt_prefs.mqtt_slot_preset[0]);
  else if (strcmp(config, "mqtt1.server") == 0) snprintf(reply, 160, "> %s", _mqtt_prefs.mqtt_slot_host[0][0] ? _mqtt_prefs.mqtt_slot_host[0] : "(unset)");
  else if (strcmp(config, "mqtt1.port") == 0) snprintf(reply, 160, "> %u", (unsigned)_mqtt_prefs.mqtt_slot_port[0]);
  else if (strcmp(config, "mqtt1.username") == 0) snprintf(reply, 160, "> %s", _mqtt_prefs.mqtt_slot_username[0][0] ? _mqtt_prefs.mqtt_slot_username[0] : "(unset)");
  else if (strcmp(config, "mqtt1.password") == 0) snprintf(reply, 160, "> %s", _mqtt_prefs.mqtt_slot_password[0][0] ? "(set)" : "(unset)");
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
#endif  // WITH_MQTT_BRIDGE_ETHERNET
