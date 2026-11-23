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

TEST(TelnetAuth, FailuresDoNotCarryAcrossSessions) {
  TelnetAuth a;
  a.begin(0);
  a.submit("a", "pw", 1);
  a.submit("b", "pw", 2);
  a.begin(10);
  EXPECT_EQ(R::Rejected, a.submit("c", "pw", 11));
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
