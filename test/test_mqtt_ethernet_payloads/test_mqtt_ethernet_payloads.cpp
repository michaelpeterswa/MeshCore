#include <gtest/gtest.h>
#include <string.h>
#include <ArduinoJson.h>
#include "helpers/MQTTEthernetPayloads.h"

namespace P = MQTTEthernetPayloads;

TEST(MQTTEthernetPayloads, LinkMessageHasSpecFields) {
  JsonDocument doc;
  char buf[512];
  P::LinkInfo info = { true, "192.168.1.50", "192.168.1.1", "dhcp", "02:11:22:33:44:55", 3, 120, true, 2 };
  int n = P::buildLinkMessage(doc, "2026-09-13T21:00:00Z", "Room", "abcd", info, buf, sizeof(buf));
  ASSERT_GT(n, 0);
  JsonDocument back;
  ASSERT_EQ(DeserializationError::Ok, deserializeJson(back, buf));
  EXPECT_STREQ("up", back["state"]);
  EXPECT_STREQ("192.168.1.50", back["ip"]);
  EXPECT_STREQ("192.168.1.1", back["gateway"]);
  EXPECT_STREQ("dhcp", back["mode"]);
  EXPECT_STREQ("02:11:22:33:44:55", back["mac"]);
  EXPECT_EQ(3, back["link_changes"].as<int>());
  EXPECT_EQ(120, back["secs_since_change"].as<int>());
  EXPECT_TRUE(back["broker_connected"].as<bool>());
  EXPECT_EQ(2, back["broker_reconnects"].as<int>());
  EXPECT_STREQ("Room", back["origin"]);
  EXPECT_STREQ("abcd", back["origin_id"]);
  EXPECT_STREQ("2026-09-13T21:00:00Z", back["timestamp"]);
}

TEST(MQTTEthernetPayloads, LinkDownOmitsAddresses) {
  JsonDocument doc;
  char buf[512];
  P::LinkInfo info = { false, "", "", "dhcp", "02:11:22:33:44:55", 4, 5, false, 2 };
  int n = P::buildLinkMessage(doc, "t", "o", "id", info, buf, sizeof(buf));
  ASSERT_GT(n, 0);
  JsonDocument back;
  deserializeJson(back, buf);
  EXPECT_STREQ("down", back["state"]);
  EXPECT_FALSE(back["ip"].is<const char*>());
}

TEST(MQTTEthernetPayloads, AuthMessage) {
  JsonDocument doc;
  char buf[512];
  int n = P::buildAuthMessage(doc, "mqtt-us-v1.letsmesh.net", "abcd", "h.p.s", 1000, 4300, buf, sizeof(buf));
  ASSERT_GT(n, 0);
  JsonDocument back;
  deserializeJson(back, buf);
  EXPECT_STREQ("mqtt-us-v1.letsmesh.net", back["audience"]);
  EXPECT_STREQ("abcd", back["username"]);
  EXPECT_STREQ("h.p.s", back["token"]);
  EXPECT_EQ(1000, back["iat"].as<int>());
  EXPECT_EQ(4300, back["exp"].as<int>());
}

TEST(MQTTEthernetPayloads, TooSmallBufferReturnsZero) {
  JsonDocument doc;
  char buf[16];
  EXPECT_EQ(0, P::buildAuthMessage(doc, "a", "b", "c", 1, 2, buf, sizeof(buf)));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
