#include <gtest/gtest.h>
#include "helpers/TelnetAuth.h"

using R = TelnetAuth::Result;

TEST(TelnetAuth, NewSessionIsNotAuthenticated) {
  TelnetAuth a;
  a.begin(1000);
  EXPECT_FALSE(a.authed());
}

TEST(TelnetAuth, CorrectPasswordAuthenticates) {
  TelnetAuth a;
  a.begin(1000);
  EXPECT_EQ(R::Accepted, a.submit("hunter2", "hunter2", 1500));
  EXPECT_TRUE(a.authed());
}

TEST(TelnetAuth, WrongPasswordIsRejectedAndStaysUnauthenticated) {
  TelnetAuth a;
  a.begin(1000);
  EXPECT_EQ(R::Rejected, a.submit("hunter3", "hunter2", 1500));
  EXPECT_FALSE(a.authed());
}

TEST(TelnetAuth, PrefixOrLongerPasswordIsRejected) {
  TelnetAuth a;
  a.begin(0);
  EXPECT_EQ(R::Rejected, a.submit("hunter", "hunter2", 1));
  EXPECT_EQ(R::Rejected, a.submit("hunter22", "hunter2", 2));
  EXPECT_FALSE(a.authed());
}

TEST(TelnetAuth, EmptyConfiguredPasswordNeverAuthenticates) {
  TelnetAuth a;
  a.begin(0);
  EXPECT_EQ(R::Rejected, a.submit("", "", 1));
  EXPECT_FALSE(a.authed());
}

TEST(TelnetAuth, NullConfiguredPasswordNeverAuthenticates) {
  TelnetAuth a;
  a.begin(0);
  EXPECT_EQ(R::Rejected, a.submit("x", nullptr, 1));
  EXPECT_FALSE(a.authed());
}

TEST(TelnetAuth, ThirdFailureDisconnectsAndLocksOut) {
  TelnetAuth a;
  a.begin(0);
  EXPECT_EQ(R::Rejected, a.submit("a", "pw", 1));
  EXPECT_EQ(R::Rejected, a.submit("b", "pw", 2));
  EXPECT_EQ(R::Disconnect, a.submit("c", "pw", 3));
  EXPECT_TRUE(a.lockedOut(3));
  EXPECT_TRUE(a.lockedOut(3 + TelnetAuth::kLockoutMs - 1));
  EXPECT_FALSE(a.lockedOut(3 + TelnetAuth::kLockoutMs));
}

TEST(TelnetAuth, FailuresCarryAcrossReconnects) {
  // Reconnecting after two misses must not buy two more guesses.
  TelnetAuth a;
  a.begin(0);
  a.submit("a", "pw", 1);
  a.submit("b", "pw", 2);
  a.begin(10);
  EXPECT_EQ(R::Disconnect, a.submit("c", "pw", 11));
  EXPECT_TRUE(a.lockedOut(11));
}

TEST(TelnetAuth, SuccessClearsFailures) {
  TelnetAuth a;
  a.begin(0);
  a.submit("a", "pw", 1);
  a.submit("b", "pw", 2);
  EXPECT_EQ(R::Accepted, a.submit("pw", "pw", 3));
  a.begin(10);
  EXPECT_EQ(R::Rejected, a.submit("c", "pw", 11));
  EXPECT_EQ(R::Rejected, a.submit("d", "pw", 12));
}

static void strikeOut(TelnetAuth& a, uint32_t now) {
  a.begin(now);
  for (int i = 0; i < TelnetAuth::kMaxAttempts; i++) a.submit("x", "pw", now);
}

TEST(TelnetAuth, RepeatedLockoutsDoubleUpToCap) {
  TelnetAuth a;
  uint32_t t = 0;
  uint32_t expect = TelnetAuth::kLockoutMs;
  for (int round = 0; round < 8; round++) {
    strikeOut(a, t);
    EXPECT_TRUE(a.lockedOut(t + expect - 1)) << "round " << round;
    EXPECT_FALSE(a.lockedOut(t + expect)) << "round " << round;
    t += expect;
    expect = expect * 2 > TelnetAuth::kMaxLockoutMs ? TelnetAuth::kMaxLockoutMs : expect * 2;
  }
}

TEST(TelnetAuth, SuccessResetsLockoutBackoff) {
  TelnetAuth a;
  strikeOut(a, 0);
  uint32_t t = TelnetAuth::kLockoutMs;
  strikeOut(a, t);                               // second lockout: doubled
  t += 2 * TelnetAuth::kLockoutMs;
  a.begin(t);
  EXPECT_EQ(R::Accepted, a.submit("pw", "pw", t));
  strikeOut(a, t + 1);
  EXPECT_FALSE(a.lockedOut(t + 1 + TelnetAuth::kLockoutMs));   // back to the base duration
}

TEST(TelnetAuth, LockoutSurvivesANewSession) {
  TelnetAuth a;
  a.begin(0);
  a.submit("a", "pw", 1); a.submit("b", "pw", 2); a.submit("c", "pw", 3);
  a.begin(5);
  EXPECT_TRUE(a.lockedOut(5));
}

TEST(TelnetAuth, UnauthenticatedSessionTimesOut) {
  TelnetAuth a;
  a.begin(1000);
  EXPECT_FALSE(a.loginTimedOut(1000 + TelnetAuth::kLoginTimeoutMs - 1));
  EXPECT_TRUE(a.loginTimedOut(1000 + TelnetAuth::kLoginTimeoutMs));
}

TEST(TelnetAuth, AuthenticatedSessionNeverTimesOut) {
  TelnetAuth a;
  a.begin(1000);
  a.submit("pw", "pw", 1001);
  EXPECT_FALSE(a.loginTimedOut(1000 + 10 * TelnetAuth::kLoginTimeoutMs));
}

TEST(TelnetAuth, TimersAreWrapSafe) {
  TelnetAuth a;
  a.begin(0xFFFFFF00u);
  EXPECT_FALSE(a.loginTimedOut(0x00000100u));   // 512 ms later, across the wrap
  a.submit("a", "pw", 0xFFFFFF00u); a.submit("b", "pw", 0xFFFFFF00u);
  a.submit("c", "pw", 0xFFFFFF00u);
  EXPECT_TRUE(a.lockedOut(0x00000100u));
}

TEST(TelnetAuth, NoLockoutInitially) {
  TelnetAuth a;
  EXPECT_FALSE(a.lockedOut(0));
  EXPECT_FALSE(a.lockedOut(123456));
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
