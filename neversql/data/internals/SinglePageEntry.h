//
// Created by Nathaniel Rupprecht on 3/28/24.
//

#pragma once

#include "neversql/data/internals/DatabaseEntry.h"

namespace neversql::internal {

class SinglePageEntry : public DatabaseEntry {
public:
  SinglePageEntry(const page_size_t starting_offset, std::unique_ptr<const Page>&& page)
      : starting_offset_(starting_offset + sizeof(page_size_t))
      , page_(std::move(page)) {
    entry_size_ = page_->Read<page_size_t>(starting_offset);
  }

  //! \brief Get the data. All the data is on the same page.
  std::span<const std::byte> GetData() const noexcept override {
    return page_->ReadFromPage(starting_offset_, entry_size_);
  }

  //! \brief There is no further page to advance to.
  bool Advance() override { return false; }

  //! \brief The page must be valid.
  bool IsValid() const override { return page_ != nullptr && page_->GetData() != nullptr; }

private:
  page_size_t starting_offset_;
  page_size_t entry_size_;
  std::unique_ptr<const Page> page_;
};

}  // namespace neversql::internal