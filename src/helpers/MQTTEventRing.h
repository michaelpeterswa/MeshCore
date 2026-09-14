#pragma once
#include <stddef.h>
#include <stdint.h>
template <typename T, size_t N> class MQTTEventRing {
public:
  size_t size() const { return 0; }
  uint32_t drops() const { return 0; }
};
