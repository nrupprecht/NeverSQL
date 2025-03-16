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

  //! \brief Check whether there is any data from the payload left to serialize.
  virtual bool HasData() = 0;

  //! \brief Get the next byte from the payload.
  //!
  //! \note This function should only be called if `HasData` returns true.
  virtual std::byte GetNextByte() = 0;

  //! \brief Get the amount of size required by the payload.
  virtual std::size_t GetRequiredSize() const = 0;
};

}  // namespace neversql::internal