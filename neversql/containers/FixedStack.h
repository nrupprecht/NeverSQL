//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#pragma once

#include <functional>
#include <concepts>
#include <type_traits>

#include "neversql/utility/Defines.h"

namespace neversql {

//! \brief A stack (first in, last out) that uses a buffer of a predetermined, fixed size to store its data.
template<std::default_initializable T, std::size_t StackSize_v = 128>
class FixedStack {
public:
  NO_DISCARD constexpr std::size_t Capacity() const noexcept { return StackSize_v; }
  NO_DISCARD constexpr std::size_t Size() const noexcept { return size_; }

  NO_DISCARD constexpr bool Empty() const noexcept { return size_ == 0; }
  NO_DISCARD constexpr bool Full() const noexcept { return size_ == StackSize_v; }
  NO_DISCARD constexpr std::size_t GetRemainingSize() const noexcept { return StackSize_v - size_; }

  NO_DISCARD constexpr std::optional<T> operator[](std::size_t index) noexcept {
    if (StackSize_v <= index) {
      return {};
    }
    return buffer_[index];
  }

  constexpr void Push(const T& value) noexcept {
    if (!Full()) {
      buffer_[size_++] = value;
    }
  }

  template<typename... Args_t>
  constexpr void Emplace(Args_t&&... args) {
    if (!Full()) {
      new (&buffer_[size_++]) T(args...);
    }
  }

  constexpr void Pop() noexcept {
    if (!Empty()) {
      // Call destructor.
      buffer_[size_].~T();
      --size_;
    }
  }

  constexpr std::optional<std::reference_wrapper<const T>> Top() const noexcept {
    return 0 < size_ ? std::optional(std::cref(buffer_[size_ - 1])) : std::nullopt;
  }

  constexpr std::optional<std::reference_wrapper<T>> Top() noexcept {
    return 0 < size_ ? std::optional(std::ref(buffer_[size_ - 1])) : std::nullopt;
  }

  //! \brief Check if two FixedStacks are equal.
  bool operator==(const FixedStack& other) const noexcept
    requires std::equality_comparable<T>
  {
    if (size_ != other.size_) {
      return false;
    }
    for (std::size_t i = 0; i < size_; ++i) {
      if (buffer_[i] != other.buffer_[i]) {
        return false;
      }
    }
    return true;
  }

  //! \brief Check if two FixedStacks are not equal.
  bool operator!=(const FixedStack& other) const noexcept
    requires std::equality_comparable<T>
  {
    return !(*this == other);
  }

private:
  T buffer_[StackSize_v];
  std::size_t size_ = 0;
};

}  // namespace neversql
