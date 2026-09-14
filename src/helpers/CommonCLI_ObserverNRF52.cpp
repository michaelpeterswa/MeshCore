// Every variant's build_src_filter globs helpers/*.cpp, so this TU is
// compiled into every env, not just the nRF52 ethernet observer. Guard it
// like helpers/esp32/WebConfigServer.cpp and helpers/JWTHelper.cpp do, so it
// is an empty TU everywhere except the one env that excludes
// CommonCLI_Observer.cpp in its place.
#if defined(WITH_MQTT_BRIDGE_ETHERNET)
#include <Arduino.h>
#include "CommonCLI.h"
bool CommonCLI::handleObserverSetCmd(uint32_t, const char*, char* reply) { strcpy(reply, "ERR: not supported on this build"); return true; }
bool CommonCLI::handleObserverGetCmd(uint32_t, const char*, char* reply) { strcpy(reply, "ERR: not supported on this build"); return true; }
bool CommonCLI::handleObserverCommand(uint32_t, char*, char*) { return false; }
#endif  // WITH_MQTT_BRIDGE_ETHERNET
