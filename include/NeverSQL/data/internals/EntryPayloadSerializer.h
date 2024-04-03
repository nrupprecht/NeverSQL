//
// Created by Nathaniel Rupprecht on 3/28/24.
//

#pragma once

#include <cstddef>

namespace neversql::internal {

//! \brief Base class for objects that act as byte generators for entry payloads. They serialize whatever the
//!        entry payload is into bytes.
class EntryPayloadSerializer {
public:
  virtual ~EntryPayloadSerializer() = default;

  virtual bool HasData() = 0;
  virtual std::byte GetNextByte() = 0;
  virtual std::size_t GetRequiredSize() const = 0;
};

}  // namespace neversql::internal