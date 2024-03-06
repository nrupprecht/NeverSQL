//
// Created by Nathaniel Rupprecht on 3/4/24.
//

#include "NeverSQL/data/Page.h"
// Other files.
#include "NeverSQL/data/PageCache.h"

namespace neversql {

// =================================================================================================
// PageCounter.
// =================================================================================================

PageCounter::PageCounter(std::optional<page_number_t> page_number, class PageCache* owning_cache) noexcept
    : page_number(page_number)
    , owning_cache(owning_cache) {}

PageCounter::~PageCounter() {
  if (owning_cache && page_number) {
    owning_cache->ReleasePage(*page_number);
  }
}

// =================================================================================================
// RCPage.
// =================================================================================================

RCPage::RCPage(page_number_t page_number, page_size_t page_size, class PageCache* owning_cache) noexcept
    : Page(page_number, page_size)
    , counter_(std::make_shared<PageCounter>(page_number, owning_cache)) {}

RCPage::RCPage(page_size_t page_size, class PageCache* owning_cache) noexcept
    : Page(page_size)
    , counter_(std::make_shared<PageCounter>(std::nullopt, owning_cache)) {}

void RCPage::setPageNumber(page_number_t page_number) {
  page_number_ = page_number;
  counter_->page_number = page_number;
}

}  // namespace neversql