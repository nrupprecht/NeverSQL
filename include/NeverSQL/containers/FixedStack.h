//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#pragma once

#include "NeverSQL/utility/Defines.h"

namespace neversql {

//! \brief A stack (first in, last out) that uses a buffer of a predetermined, fixed size to store its data.
template<typename T, std::size_t StackSize_v = 128>
  requires std::is_nothrow_constructible_v<T>
class FixedStack {
public:
  NO_DISCARD constexpr std::size_t Capacity() const noexcept { return StackSize_v; }
  NO_DISCARD constexpr std::size_t Size() const noexcept { return size_; }

  NO_DISCARD constexpr bool Empty() const noexcept { return size_ == 0; }
  NO_DISCARD constexpr bool Full() const noexcept { return size_ == StackSize_v; }
  NO_DISCARD constexpr std::size_t GetRemainingSize() const noexcept { return StackSize_v - size_; }

  constexpr void Push(const T& value) noexcept {
    if (!Full()) {
      buffer_[size_++] = value;
    }
  }

  constexpr void Pop() noexcept {
    if (!Empty()) {
      --size_;
    }
  }

  constexpr T& Top() noexcept { return buffer_[size_ - 1]; }

private:
  T buffer_[StackSize_v];
  std::size_t size_ = 0;
};

}  // namespace neversql
