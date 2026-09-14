#include <gtest/gtest.h>
#include "helpers/MQTTEventRing.h"

struct Item { int v; };

TEST(MQTTEventRing, StartsEmpty) {
  MQTTEventRing<Item, 3> ring;
  EXPECT_TRUE(ring.empty());
  EXPECT_EQ(0u, ring.size());
  EXPECT_EQ(nullptr, ring.front());
  EXPECT_EQ(0u, ring.drops());
}

TEST(MQTTEventRing, PushPopIsFifo) {
  MQTTEventRing<Item, 3> ring;
  EXPECT_FALSE(ring.push({1}));
  EXPECT_FALSE(ring.push({2}));
  EXPECT_EQ(2u, ring.size());
  EXPECT_EQ(1, ring.front()->v);
  ring.pop();
  EXPECT_EQ(2, ring.front()->v);
  ring.pop();
  EXPECT_TRUE(ring.empty());
}

TEST(MQTTEventRing, FullRingDropsOldestAndCounts) {
  MQTTEventRing<Item, 3> ring;
  ring.push({1}); ring.push({2}); ring.push({3});
  EXPECT_TRUE(ring.push({4}));
  EXPECT_EQ(3u, ring.size());
  EXPECT_EQ(1u, ring.drops());
  EXPECT_EQ(2, ring.front()->v);
  ring.pop(); ring.pop(); ring.pop();
  EXPECT_TRUE(ring.empty());
  EXPECT_EQ(nullptr, ring.front());
}

TEST(MQTTEventRing, PopOnEmptyIsHarmless) {
  MQTTEventRing<Item, 2> ring;
  ring.pop();
  EXPECT_TRUE(ring.empty());
  EXPECT_EQ(0u, ring.drops());
}

TEST(MQTTEventRing, FrontIsWritable) {
  MQTTEventRing<Item, 2> ring;
  ring.push({7});
  ring.front()->v = 9;
  EXPECT_EQ(9, ring.front()->v);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
