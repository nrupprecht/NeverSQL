//
// Created by Nathaniel Rupprecht on 3/12/24.
//

#pragma once

#include "NeverSQL/utility/Defines.h"

namespace neversql::internal {

inline std::string HexDumpBytes(std::span<const std::byte> key, bool in_brackets = true) {
  using namespace lightning::formatting;

  lightning::memory::StringMemoryBuffer buffer;
  std::size_t count = 0;
  if (in_brackets) {
    buffer.PushBack('{');
  }
  for (const auto& byte : key) {
    if (0 < count++) {
      buffer.PushBack(' ');
    }
    FormatHex(static_cast<uint8_t>(byte), buffer, true, PrefixFmtType::None, true);
  }
  if (in_brackets) {
    buffer.PushBack('}');
  }
  return buffer.MoveString();
}

inline std::string PrintUInt64(std::span<const std::byte> key) {
  int64_t x;
  std::memcpy(&x, key.data(), sizeof(int64_t));
  return lightning::formatting::Format("{}", x);
}

inline std::string PrintString(std::span<const std::byte> key) {
  return lightning::formatting::Format(
      "{:?}", std::string_view(reinterpret_cast<const char*>(key.data()), key.size()));
}

}  // namespace neversql::internal