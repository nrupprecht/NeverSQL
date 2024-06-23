//
// Created by Nathaniel Rupprecht on 3/26/24.
//

#pragma once

#include "NeverSQL/data/internals/DatabaseEntry.h"

namespace neversql::internal {

//! \brief Represents an entry that is stored across one or more overflow pages.
class OverflowEntry : public DatabaseEntry {
public:
  OverflowEntry([[maybe_unused]] std::span<const std::byte> entry_header,
                [[maybe_unused]] const BTreeManager* btree_manager)
      : btree_manager_(btree_manager) {
    // Get information from the header, get the first overflow page.
    auto overflow_key = entry_header.subspan(0, 8);
    auto overflow_page_number = entry_header.subspan(8, 8);

    // Convert the key and page number to the correct types.
    overflow_key_ = *reinterpret_cast<const primary_key_t*>(overflow_key.data());
    auto page_number = *reinterpret_cast<const page_number_t*>(overflow_page_number.data());

    // Load up the first overflow page.
    node_ = btree_manager_->loadNodePage(page_number);

    setup();
  }

  std::span<const std::byte> GetData() const noexcept override {
    const auto entry = node_->GetEntry(SpanValue(overflow_key_), btree_manager_);
    const auto data = entry->GetData();
    // Bypass next page (first sizeof(page_number_t) bytes), just return the data.
    return data.subspan(sizeof(page_number_t));
  }

  bool Advance() override {
    if (next_page_number_ == 0) {
      return false;
    }

    node_ = btree_manager_->loadNodePage(next_page_number_);
    setup();

    return true;
  }

  bool IsValid() const override { return node_.has_value(); }

private:
  void setup() {
    if (!IsValid()) {
      return;
    }

    const auto entry = node_->GetEntry(SpanValue(overflow_key_), btree_manager_);
    NOSQL_ASSERT(
        entry,
        "could not find entry for overflow key " << overflow_key_ << " in page " << node_->GetPageNumber());
    const auto data = entry->GetData();
    const auto next_page_span = data.subspan(0, sizeof(primary_key_t));
    next_page_number_ = *reinterpret_cast<const page_number_t*>(next_page_span.data());
  }

  primary_key_t overflow_key_ = 0;
  page_number_t next_page_number_ = 0;

  const BTreeManager* btree_manager_;

  std::optional<BTreeNodeMap> node_;
};

}  // namespace neversql::internal