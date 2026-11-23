#pragma once

#include <stddef.h>
#include <stdint.h>

// Login gate for the Ethernet telnet CLI. A telnet session runs commands with
// the same authority as the serial console (sender_timestamp 0: it can set
// mqtt1.password and read the owner key), so it must not be open to anyone who
// can reach port 23. The password is the node's admin password.
//
// Pure logic, no I/O. Times are millis() values; all comparisons are
// wrap-safe. One instance serves the single telnet client slot.
class TelnetAuth {
public:
  enum class Result { Accepted, Rejected, Disconnect };

  static const uint8_t kMaxAttempts = 3;
  static const uint32_t kLoginTimeoutMs = 30000UL;   // unauthenticated session is closed after this
  static const uint32_t kLockoutMs = 30000UL;        // first lockout after kMaxAttempts failures
  static const uint32_t kMaxLockoutMs = 900000UL;    // each further lockout doubles, up to this

  TelnetAuth() : _authed(false), _failures(0), _lockouts(0), _locked(false),
                 _session_start(0), _locked_at(0), _lockout_ms(0) { }

  /**
   * A new client connected. Neither the failure count nor an active lockout
   * resets here: otherwise reconnecting would buy a fresh set of guesses.
   * Only a successful login clears them.
   */
  void begin(uint32_t now) {
    _authed = false;
    _session_start = now;
  }

  bool authed() const { return _authed; }

  /** Check a password line. An empty or null configured password never matches. */
  Result submit(const char* line, const char* password, uint32_t now) {
    if (matches(line, password)) {
      _authed = true;
      _failures = 0;
      _lockouts = 0;
      return Result::Accepted;
    }
    if (++_failures >= kMaxAttempts) {
      uint32_t ms = kLockoutMs;
      for (uint8_t i = 0; i < _lockouts && ms < kMaxLockoutMs; i++) ms *= 2;
      _lockout_ms = ms < kMaxLockoutMs ? ms : kMaxLockoutMs;
      if (_lockouts < 0xFF) _lockouts++;
      _failures = 0;   // the next round starts once this lockout expires
      _locked = true;
      _locked_at = now;
      return Result::Disconnect;
    }
    return Result::Rejected;
  }

  bool loginTimedOut(uint32_t now) const {
    return !_authed && (uint32_t)(now - _session_start) >= kLoginTimeoutMs;
  }

  bool lockedOut(uint32_t now) const {
    return _locked && (uint32_t)(now - _locked_at) < _lockout_ms;
  }

private:
  // Compares every byte up to the longer string's end (bounded), so the time
  // taken does not reveal how long a correct prefix was.
  static bool matches(const char* line, const char* password) {
    if (!line || !password || password[0] == 0) return false;
    static const size_t kMax = 64;
    uint8_t diff = 0;
    bool line_end = false, pw_end = false;
    for (size_t i = 0; i < kMax; i++) {
      uint8_t a = line_end ? 0 : (uint8_t)line[i];
      uint8_t b = pw_end ? 0 : (uint8_t)password[i];
      diff |= (uint8_t)(a ^ b);
      if (a == 0) line_end = true;
      if (b == 0) pw_end = true;
    }
    return diff == 0 && line_end && pw_end;
  }

  bool _authed;
  uint8_t _failures;      // since the last success or lockout, across connections
  uint8_t _lockouts;      // consecutive lockouts since the last success
  bool _locked;
  uint32_t _session_start;
  uint32_t _locked_at;
  uint32_t _lockout_ms;
};
