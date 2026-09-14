#include <gtest/gtest.h>
#include <string.h>
#include <string>
#include <vector>
#include <ArduinoJson.h>
#include "helpers/RoomLogPublisher.h"

struct Captured { std::string suffix; std::string json; bool retain; };
static std::vector<Captured> g_out;
static bool g_accept = true;

static bool sink(void*, const char* suffix, const char* json, size_t len, bool retain) {
  if (!g_accept) return false;
  g_out.push_back({suffix, std::string(json, len), retain});
  return true;
}

static const uint8_t KEY[32] = { 0xAB, 0xCD, 0x01, 0x02 };

class RoomLogPublisherTest : public ::testing::Test {
protected:
  RoomLogPublisher pub;
  void SetUp() override {
    g_out.clear(); g_accept = true;
    pub.setSink(sink, nullptr);
    pub.setOrigin("Room", "abcd");
    pub.setEnabled(true, true, true, true);
  }
};

TEST_F(RoomLogPublisherTest, RoleNames) {
  EXPECT_STREQ("guest", RoomLogPublisher::roleName(0));
  EXPECT_STREQ("read_only", RoomLogPublisher::roleName(1));
  EXPECT_STREQ("read_write", RoomLogPublisher::roleName(2));
  EXPECT_STREQ("admin", RoomLogPublisher::roleName(3));
  EXPECT_STREQ("admin", RoomLogPublisher::roleName(0x83));
}

TEST_F(RoomLogPublisherTest, PostPayload) {
  RoomLogPublisher::Post p = { KEY, 2, "hello room", 1700000000u };
  EXPECT_TRUE(pub.publishPost(p, "2026-09-13T21:00:00Z"));
  ASSERT_EQ(1u, g_out.size());
  EXPECT_EQ("room/posts", g_out[0].suffix);
  EXPECT_FALSE(g_out[0].retain);
  JsonDocument d; ASSERT_EQ(DeserializationError::Ok, deserializeJson(d, g_out[0].json));
  EXPECT_STREQ("2026-09-13T21:00:00Z", d["timestamp"]);
  EXPECT_STREQ("Room", d["origin"]);
  EXPECT_STREQ("abcd", d["origin_id"]);
  EXPECT_EQ(0, strncmp("abcd0102", d["author_pubkey"].as<const char*>(), 8));
  EXPECT_EQ(64u, strlen(d["author_pubkey"].as<const char*>()));
  EXPECT_STREQ("read_write", d["role"]);
  EXPECT_STREQ("hello room", d["text"]);
  EXPECT_EQ(1700000000u, d["client_timestamp"].as<uint32_t>());
}

TEST_F(RoomLogPublisherTest, EventPayloadNeverCarriesReplyOrPassword) {
  RoomLogPublisher::Event e = { "admin_cmd", KEY, "ok", "get stats" };
  EXPECT_TRUE(pub.publishEvent(e, "t"));
  JsonDocument d; deserializeJson(d, g_out[0].json);
  EXPECT_EQ("room/events", g_out[0].suffix);
  EXPECT_STREQ("admin_cmd", d["event"]);
  EXPECT_STREQ("ok", d["result"]);
  EXPECT_STREQ("get stats", d["detail"]);
  EXPECT_FALSE(d["reply"].is<const char*>());
  EXPECT_FALSE(d["password"].is<const char*>());
}

TEST_F(RoomLogPublisherTest, EventWithoutActor) {
  RoomLogPublisher::Event e = { "acl_change", nullptr, "ok", "setperm" };
  EXPECT_TRUE(pub.publishEvent(e, "t"));
  JsonDocument d; deserializeJson(d, g_out[0].json);
  EXPECT_FALSE(d["actor_pubkey"].is<const char*>());
}

TEST_F(RoomLogPublisherTest, DirectMessagePayload) {
  RoomLogPublisher::DirectMessage m = { KEY, 3, "cli_data", true, "ver", 5 };
  EXPECT_TRUE(pub.publishDirectMessage(m, "t"));
  JsonDocument d; deserializeJson(d, g_out[0].json);
  EXPECT_EQ("room/dm", g_out[0].suffix);
  EXPECT_STREQ("cli_data", d["kind"]);
  EXPECT_TRUE(d["accepted"].as<bool>());
  EXPECT_STREQ("admin", d["role"]);
  EXPECT_STREQ("ver", d["text"]);
}

TEST_F(RoomLogPublisherTest, StatsPayloadIsRetained) {
  RoomLogPublisher::Stats s = { 10, 3, 1, 4, 0, 2, 1234, 7, 1 };
  EXPECT_TRUE(pub.publishStats(s, "t"));
  EXPECT_EQ("room/stats", g_out[0].suffix);
  EXPECT_TRUE(g_out[0].retain);
  JsonDocument d; deserializeJson(d, g_out[0].json);
  EXPECT_EQ(10, d["posts_total"].as<int>());
  EXPECT_EQ(3, d["posts_unsynced"].as<int>());
  EXPECT_EQ(1, d["clients_admin"].as<int>());
  EXPECT_EQ(4, d["clients_read_write"].as<int>());
  EXPECT_EQ(0, d["clients_read_only"].as<int>());
  EXPECT_EQ(2, d["clients_guest"].as<int>());
  EXPECT_EQ(1234, d["acl_bytes"].as<int>());
  EXPECT_EQ(7, d["packet_drops"].as<int>());
  EXPECT_EQ(1, d["event_drops"].as<int>());
}

TEST_F(RoomLogPublisherTest, DisabledTopicsAndSinkRefusal) {
  pub.setEnabled(false, true, true, true);
  RoomLogPublisher::Post p = { KEY, 2, "x", 1 };
  EXPECT_FALSE(pub.publishPost(p, "t"));
  EXPECT_TRUE(g_out.empty());
  g_accept = false;
  RoomLogPublisher::Event e = { "login_ok", KEY, "ok", "" };
  EXPECT_FALSE(pub.publishEvent(e, "t"));
}

TEST_F(RoomLogPublisherTest, NoSinkReturnsFalse) {
  RoomLogPublisher bare;
  RoomLogPublisher::Stats s = {};
  EXPECT_FALSE(bare.publishStats(s, "t"));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
