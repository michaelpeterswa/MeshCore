// nRF52 Ethernet-observer-only translation unit. The base PlatformIO config
// globs every src/helpers/*.cpp into every firmware env, so this is guarded
// here rather than in the build filter — same idiom as helpers/JWTHelper.cpp.
#if defined(WITH_MQTT_BRIDGE_ETHERNET)

#include "EthernetObserverPrefs.h"
#include <Arduino.h>

static EthernetObserverPrefs s_prefs;
static bool s_prefs_init = false;

EthernetObserverPrefs& ethernetObserverPrefs() {
  if (!s_prefs_init) { EthernetObserverPrefsCodec::setDefaults(&s_prefs); s_prefs_init = true; }
  return s_prefs;
}

static const char* kPath = "/mqtt_eth_prefs";

bool EthernetObserverPrefsStore::load(FILESYSTEM* fs, EthernetObserverPrefs* out) {
  EthernetObserverPrefsCodec::setDefaults(out);
  if (!fs || !fs->exists(kPath)) return false;
  File f = fs->open(kPath);
  if (!f) return false;
  uint8_t buf[EthernetObserverPrefsCodec::kEncodedSize];
  size_t n = f.read(buf, sizeof(buf));
  f.close();
  EthernetObserverPrefs tmp;
  if (!EthernetObserverPrefsCodec::decode(buf, n, &tmp)) {
    EthernetObserverPrefsCodec::setDefaults(out);
    return false;
  }
  *out = tmp;
  return true;
}

bool EthernetObserverPrefsStore::save(FILESYSTEM* fs, const EthernetObserverPrefs& in) {
  if (!fs) return false;
  uint8_t buf[EthernetObserverPrefsCodec::kEncodedSize];
  size_t n = EthernetObserverPrefsCodec::encode(in, buf, sizeof(buf));
  if (n == 0) return false;
#if defined(NRF52_PLATFORM)
  fs->remove(kPath);
  File f = fs->open(kPath, FILE_O_WRITE);
#else
  File f = fs->open(kPath, "w");
#endif
  if (!f) return false;
  bool ok = f.write(buf, n) == n;
  f.close();
  return ok;
}

#endif  // WITH_MQTT_BRIDGE_ETHERNET
