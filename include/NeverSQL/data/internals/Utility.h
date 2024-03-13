//
// Created by Nathaniel Rupprecht on 3/12/24.
//

#pragma once

#include "NeverSQL/utility/Defines.h"

namespace neversql::internal {

template<typename T> requires std::is_trivially_copyable_v<T>
std::span<const std::byte> SpanValue(const T& value) noexcept {
  return std::span<const std::byte>(reinterpret_cast<const std::byte*>(&value), sizeof(T));
}

}