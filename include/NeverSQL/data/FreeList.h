//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#pragma once

#include <deque>
#include "NeverSQL/utility/Defines.h"

namespace neversql {

//! \brief Free-list data structure. This is used to keep track of the pages in the database and to allocate new pages.
class FreeList {
  friend class DataAccessLayer;

 public:
  //! \brief Get the next page from the free list.
  NO_DISCARD PageNumber GetNextPage();

  //! \brief Release a page back to the free list.
  void ReleasePage(PageNumber page_number);

  //! \brief Get the number of allocated pages.
  NO_DISCARD PageNumber GetNumAllocatedPages() const;

  //! \brief Check if a page is valid, i.e. whether it is both allocated and used (not in the free list).
  NO_DISCARD bool IsPageValid(PageNumber page_number) const;

  //! \brief Check if the free list has been modified since the last time it was "cleaned."
  NO_DISCARD bool IsDirty() const;

  //! \brief Mark the free list as clean.
  void Clean() const;

 private:
  //! \brief Deque of freed pages.
  std::deque<PageNumber> freed_pages_{};

  //! \brief The total number of allocated pages, also, the next page number to be allocated.
  PageNumber next_page_number_ = 0;

  //! \brief If true, the free list has been modified since the last time it was "cleaned."
  mutable bool is_dirty_ = false;
};

}  // namespace neversql
