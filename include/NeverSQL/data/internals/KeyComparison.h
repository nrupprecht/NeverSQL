//
// Created by Nathaniel Rupprecht on 3/12/24.
//

#pragma once

#include "NeverSQL/utility/Defines.h"

namespace neversql::internal {

template<typename T>
requires std::is_trivially_copyable_v<T>
inline bool CompareTrivial(std::span<const std::byte> lhs, std::span<const std::byte> rhs) {
  T lhs_value;
  T rhs_value;
  std::memcpy(&lhs_value, lhs.data(), sizeof(uint64_t));
  std::memcpy(&rhs_value, rhs.data(), sizeof(uint64_t));
  return lhs_value < rhs_value;
}

inline bool CompareString(std::span<const std::byte> lhs, std::span<const std::byte> rhs) {
  return std::ranges::lexicographical_compare(lhs, rhs);
}

}  // namespace neversql::internal