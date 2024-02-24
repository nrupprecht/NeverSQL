//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#include "NeverSQL/data/FreeList.h"

namespace neversql {

page_number_t FreeList::GetNextPage() {
  is_dirty_ = true;
  if (!freed_pages_.empty()) {
    auto next_page = freed_pages_.front();
    freed_pages_.pop_front();
    return next_page;
  }
  return next_page_number_++;
}

void FreeList::ReleasePage(page_number_t page_number) {
  NOSQL_REQUIRE(
      page_number < next_page_number_,
      "invalid page number, maximum page number is " << next_page_number_ - 1 << ", got " << page_number);

  is_dirty_ = true;
  // If the page is not already freed, add it to the list of freed pages.
  if (std::ranges::count(freed_pages_, page_number) == 0) {
    freed_pages_.push_back(page_number);
  }
}

page_number_t FreeList::GetNumAllocatedPages() const {
  return next_page_number_;
}

bool FreeList::IsPageValid(page_number_t page_number) const {
  return page_number <= next_page_number_ && std::ranges::count(freed_pages_, page_number) == 0;
}

NO_DISCARD bool FreeList::IsDirty() const {
  return is_dirty_;
}

void FreeList::Clean() const {
  is_dirty_ = false;
}

}  // namespace neversql