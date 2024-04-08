//
// Created by Nathaniel Rupprecht on 3/26/24.
//

#pragma once

#include "NeverSQL/data/internals/DatabaseEntry.h"

namespace neversql::internal {


//! \brief Represents an entry that is stored across one or more overflow pages.
//!
class OverflowEntry : public DatabaseEntry {
public:
  OverflowEntry([[maybe_unused]] std::span<const std::byte> entry_header,
                [[maybe_unused]] const BTreeManager* btree_manager) : btree_manager_(btree_manager) {
    // Get information from the header, get the first overflow page.
    // TODO: Implement.
  }

  std::span<const std::byte> GetData() const noexcept override {
    // TODO: Implement.
    return {};
  }

  bool Advance() override {
    // TODO: Implement.
    return false;
  }

private:
  std::span<std::byte> data_;

  [[maybe_unused]] const BTreeManager* btree_manager_;
};

} // namespace neversql::internal