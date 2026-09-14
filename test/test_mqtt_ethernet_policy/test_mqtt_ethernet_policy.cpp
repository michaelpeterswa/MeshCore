#include <gtest/gtest.h>
#include "helpers/MQTTEthernetPolicy.h"

namespace P = MQTTEthernetPolicy;

TEST(MQTTEthernetPolicy, OnlyPlainMqttSchemeIsUsable) {
  EXPECT_TRUE(P::presetUsableWithoutTls("mqtt://mqtt.tennmesh.com:1883"));
  EXPECT_FALSE(P::presetUsableWithoutTls("mqtts://meshrank.net:8883"));
  EXPECT_FALSE(P::presetUsableWithoutTls("wss://mqtt-us-v1.letsmesh.net:443/mqtt"));
  EXPECT_FALSE(P::presetUsableWithoutTls(""));
  EXPECT_FALSE(P::presetUsableWithoutTls(nullptr));
}

TEST(MQTTEthernetPolicy, CustomSlotNeedsHostPortIataDevice) {
  EXPECT_TRUE(P::slotConfigValid("custom", "192.168.1.10", 1883, "SEA", "abcd"));
  EXPECT_FALSE(P::slotConfigValid("custom", "", 1883, "SEA", "abcd"));
  EXPECT_FALSE(P::slotConfigValid("custom", "192.168.1.10", 0, "SEA", "abcd"));
  EXPECT_FALSE(P::slotConfigValid("custom", "192.168.1.10", 1883, "", "abcd"));
  EXPECT_FALSE(P::slotConfigValid("custom", "192.168.1.10", 1883, "SEA", ""));
  EXPECT_FALSE(P::slotConfigValid("none", "192.168.1.10", 1883, "SEA", "abcd"));
}

TEST(MQTTEthernetPolicy, NamedPresetNeedsIataAndDeviceOnly) {
  EXPECT_TRUE(P::slotConfigValid("tennmesh", "", 0, "SEA", "abcd"));
  EXPECT_FALSE(P::slotConfigValid("tennmesh", "", 0, "", "abcd"));
}

TEST(MQTTEthernetPolicy, ParsesCommaSeparatedAudiences) {
  char out[4][64];
  EXPECT_EQ(2, P::parseAudiences("mqtt-us-v1.letsmesh.net, mqtt.meshmapper.net", out, 4));
  EXPECT_STREQ("mqtt-us-v1.letsmesh.net", out[0]);
  EXPECT_STREQ("mqtt.meshmapper.net", out[1]);
  EXPECT_EQ(0, P::parseAudiences("", out, 4));
  EXPECT_EQ(0, P::parseAudiences(nullptr, out, 4));
  EXPECT_EQ(1, P::parseAudiences(",,a,,", out, 4));
  EXPECT_STREQ("a", out[0]);
  EXPECT_EQ(2, P::parseAudiences("a,b,c", out, 2));
}

TEST(MQTTEthernetPolicy, RenewalAtTwoThirdsOfLife) {
  EXPECT_FALSE(P::tokenRenewalDue(1000 + 2199, 1000, 3300));
  EXPECT_TRUE(P::tokenRenewalDue(1000 + 2200, 1000, 3300));
  EXPECT_TRUE(P::tokenRenewalDue(5000, 0, 3300));   // never issued
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
