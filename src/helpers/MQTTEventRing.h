#pragma once

#include <stddef.h>
#include <stdint.h>

/**
 * Fixed-capacity FIFO. When full, push() overwrites the oldest entry and
 * counts a drop. Single-threaded: the bridge fills and drains it from the
 * main loop only.
 */
template <typename T, size_t N>
class MQTTEventRing {
public:
  static const size_t capacity = N;

  MQTTEventRing() : _head(0), _count(0), _drops(0) {}

  /** Returns true when an older entry was discarded to make room. */
  bool push(const T& item) {
    bool dropped = false;
    if (_count == N) {
      _head = (_head + 1) % N;
      _count--;
      _drops++;
      dropped = true;
    }
    _items[(_head + _count) % N] = item;
    _count++;
    return dropped;
  }

  T* front() { return _count ? &_items[_head] : nullptr; }
  const T* front() const { return _count ? &_items[_head] : nullptr; }

  void pop() {
    if (_count == 0) return;
    _head = (_head + 1) % N;
    _count--;
  }

  size_t size() const { return _count; }
  bool empty() const { return _count == 0; }
  uint32_t drops() const { return _drops; }

private:
  T _items[N];
  size_t _head;
  size_t _count;
  uint32_t _drops;
};
