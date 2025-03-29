//
// Created by Nathaniel Rupprecht on 3/4/24.
//

#include "neversql/data/Page.h"
// Other files.
#include "neversql/data/PageCache.h"
#include "neversql/recovery/WriteAheadLog.h"

namespace neversql {

// =================================================================================================
//  RCPage.
// =================================================================================================

RCPage::RCPage(page_size_t page_size, uint32_t descriptor_index, PageCache* owning_cache) noexcept
    : Page(page_size)
    , owning_cache_(owning_cache)
    , descriptor_index_(descriptor_index) {}

RCPage::~RCPage() {
  if (owning_cache_) {
    owning_cache_->ReleasePage(page_number_);
  }
}

std::unique_ptr<Page> RCPage::NewHandle() const {
  return owning_cache_->GetPage(page_number_);
}

page_size_t RCPage::writeToPage(page_size_t offset, std::span<const std::byte> data, bool omit_log) {
  NOSQL_REQUIRE(offset + data.size() <= page_size_,
                "WriteToPage: offset + data.size() ("
                    << offset + data.size() << ") is greater than page size (" << page_size_ << ").");

  // Since we write to the page, the page is now dirty.
  owning_cache_->SetDirty(descriptor_index_);
  if (!omit_log) {
    auto lsn = owning_cache_->GetWAL().Update(transaction_number_,
                                              page_number_,
                                              offset,
                                              GetSpan(offset, static_cast<page_size_t>(data.size())),
                                              data);
    // Set the LSN in the page
    // owning_cache_->SetLSN(descriptor_index_, lsn);
  }

  // Copy the data to the page cache buffered page.
  std::memcpy(data_ + offset, data.data(), data.size());

  return static_cast<page_size_t>(offset + data.size());
}

}  // namespace neversql