//
// Created by Nathaniel Rupprecht on 3/28/24.
//

#pragma once

#include "NeverSQL/data/internals/DatabaseEntry.h"

namespace neversql::internal {

class SinglePageEntry : public DatabaseEntry {
public:
  SinglePageEntry(page_size_t starting_offset, const Page* page)
      : starting_offset_(starting_offset)
      , page_(page) {
    entry_size_ = page->Read<page_size_t>(starting_offset);
  }

  //! \brief Get the data. All the data is on the same page.
  std::span<const std::byte> GetData() const noexcept override {
    return page_->ReadFromPage(starting_offset_ + 2, entry_size_);
  }

  //! \brief There is no further page to advance to.
  bool Advance() override { return false; }

private:
  page_size_t starting_offset_;
  page_size_t entry_size_;
  const Page* page_;
};

}  // namespace neversql::internal