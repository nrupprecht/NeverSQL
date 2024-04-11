//
// Created by Nathaniel Rupprecht on 3/4/24.
//

#include "NeverSQL/data/Page.h"
// Other files.
#include "NeverSQL/data/PageCache.h"
#include "NeverSQL/recovery/WriteAheadLog.h"

namespace neversql {

// =================================================================================================
//  RCPage.
// =================================================================================================

RCPage::RCPage(page_number_t page_number,
               transaction_t transaction_number,
               page_size_t page_size,
               uint32_t descriptor_index,
               PageCache* owning_cache) noexcept
    : Page(page_number, transaction_number, page_size)
    , owning_cache_(owning_cache)
    , descriptor_index_(descriptor_index) {}

RCPage::RCPage(page_size_t page_size, uint32_t descriptor_index, PageCache* owning_cache) noexcept
    : Page(page_size)
    , owning_cache_(owning_cache)
    , descriptor_index_(descriptor_index) {}

RCPage::~RCPage() {
  if (owning_cache_) {
    owning_cache_->ReleasePage(page_number_);
  }
}

page_size_t RCPage::WriteToPage(page_size_t offset, std::span<const std::byte> data) {
  NOSQL_REQUIRE(offset + data.size() <= page_size_,
                "WriteToPage: offset + data.size() ("
                    << offset + data.size() << ") is greater than page size (" << page_size_ << ").");

  // Since we write to the page, the page is now dirty.
  owning_cache_->SetDirty(descriptor_index_);
  owning_cache_->GetWAL().Update(transaction_number_,
                                 page_number_,
                                 offset,
                                 GetSpan(offset, static_cast<page_size_t>(data.size())),
                                 data);

  // Copy the data to the page cache buffered page.
  std::memcpy(data_ + offset, data.data(), data.size());
  return static_cast<page_size_t>(offset + data.size());
}

std::unique_ptr<Page> RCPage::NewHandle() const {
  return owning_cache_->GetPage(page_number_);
}

}  // namespace neversql