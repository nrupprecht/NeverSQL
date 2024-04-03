//
// Created by Nathaniel Rupprecht on 3/12/24.
//

#pragma once

#include "NeverSQL/utility/Defines.h"

namespace neversql::internal {

template<typename Value_t> requires std::is_trivially_copyable_v<Value_t>
std::span<const std::byte> SpanValue(const Value_t& value) noexcept {
  return std::span(reinterpret_cast<const std::byte*>(&value), sizeof(Value_t));
}

inline std::span<const std::byte> SpanValue(const std::string& value) noexcept {
  return std::span(reinterpret_cast<const std::byte*>(value.data()), value.size());
}

}  // namespace neversql::internal
