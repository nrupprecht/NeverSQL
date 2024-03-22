//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#pragma once

#include <deque>

#include "NeverSQL/utility/Defines.h"

namespace neversql {

//! \brief Free-list data structure. This is used to keep track of the pages in the database and to allocate
//!        new pages.
class FreeList {
  friend class DataAccessLayer;

public:
  //! \brief Constructs a free list that can allocate new slots, and starts with zero free slots.
  FreeList() = default;

  //! \brief Constructs a free list that starts with the given number of free slots and specify whether it can
  //!        allocate new slots.
  FreeList(std::size_t starting_slots, bool can_allocate);

  //! \brief Get the next page from the free list. If a next free slot cannot be found or allocated, returns
  //!        nullptr.
  NO_DISCARD std::optional<page_number_t> GetNextPage();

  //! \brief Release a page back to the free list. Returns true if the page was successfully released, false
  //!        if the page was not acquired in the first place.
  bool ReleasePage(page_number_t page_number);

  //! \brief Get the number of allocated pages.
  NO_DISCARD page_number_t GetNumAllocatedPages() const;

  //! \brief Get the number of free pages, those that are allocated, but not in use.
  NO_DISCARD page_number_t GetNumFreePages() const;

  //! \brief Check if a page is valid, i.e. whether it is both allocated and used (not in the free list).
  NO_DISCARD bool IsPageValid(page_number_t page_number) const;

  //! \brief Check if the free list has been modified since the last time it was "cleaned."
  NO_DISCARD bool IsDirty() const;

  //! \brief Mark the free list as clean.
  void Clean() const;

private:
  //! \brief Deque of freed pages.
  std::deque<page_number_t> freed_pages_ {};

  //! \brief The total number of allocated pages, also, the next page number to be allocated.
  page_number_t next_page_number_ = 0;

  //! \brief Whether the free list is allowed to allocate new elements when the free list is empty.
  bool can_allocate_ = true;

  //! \brief If true, the free list has been modified since the last time it was "cleaned."
  mutable bool is_dirty_ = false;
};

}  // namespace neversql
