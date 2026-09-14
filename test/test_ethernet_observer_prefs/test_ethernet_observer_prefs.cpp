#include <gtest/gtest.h>
#include <string.h>
#include "helpers/EthernetObserverPrefs.h"

namespace C = EthernetObserverPrefsCodec;

TEST(EthernetObserverPrefsCodec, DefaultsAreOnWithSpecLifetime) {
  EthernetObserverPrefs p;
  C::setDefaults(&p);
  EXPECT_EQ(1, p.room_posts);
  EXPECT_EQ(1, p.room_events);
  EXPECT_EQ(1, p.room_dm);
  EXPECT_EQ(1, p.room_stats);
  EXPECT_EQ(3300u, p.auth_life_secs);
  EXPECT_STREQ("", p.auth_audiences);
}

TEST(EthernetObserverPrefsCodec, RoundTrips) {
  EthernetObserverPrefs in;
  C::setDefaults(&in);
  in.room_dm = 0;
  in.auth_life_secs = 600;
  strcpy(in.auth_audiences, "mqtt-us-v1.letsmesh.net,mqtt.meshmapper.net");

  uint8_t buf[256];
  size_t n = C::encode(in, buf, sizeof(buf));
  EXPECT_EQ(C::kEncodedSize, n);
  EXPECT_EQ('M', buf[0]); EXPECT_EQ('E', buf[1]); EXPECT_EQ('T', buf[2]); EXPECT_EQ('H', buf[3]);

  EthernetObserverPrefs out;
  memset(&out, 0xAA, sizeof(out));
  EXPECT_TRUE(C::decode(buf, n, &out));
  EXPECT_EQ(1, out.room_posts);
  EXPECT_EQ(0, out.room_dm);
  EXPECT_EQ(600u, out.auth_life_secs);
  EXPECT_STREQ("mqtt-us-v1.letsmesh.net,mqtt.meshmapper.net", out.auth_audiences);
}

TEST(EthernetObserverPrefsCodec, RejectsBadMagicShortAndWrongVersion) {
  EthernetObserverPrefs in, out;
  C::setDefaults(&in);
  uint8_t buf[256];
  size_t n = C::encode(in, buf, sizeof(buf));

  uint8_t bad[256]; memcpy(bad, buf, n); bad[0] = 'X';
  EXPECT_FALSE(C::decode(bad, n, &out));
  EXPECT_FALSE(C::decode(buf, n - 1, &out));
  memcpy(bad, buf, n); bad[4] = 2;
  EXPECT_FALSE(C::decode(bad, n, &out));
  EXPECT_EQ(0u, C::encode(in, buf, 4));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
