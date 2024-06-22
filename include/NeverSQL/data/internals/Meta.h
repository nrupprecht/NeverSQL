//
// Created by Nathaniel Rupprecht on 2/18/24.
//

#pragma once

#include <memory>

#include "NeverSQL/utility/Defines.h"

namespace neversql {

//! \brief Programmatic representation of the meta page for the database.
class Meta {
  friend class DataAccessLayer;

public:
  explicit Meta(uint8_t page_size_power)
      : page_size_power_(page_size_power)
      , page_size_(static_cast<page_size_t>(1 << page_size_power)) {
    // Make sure pages are not too big or small.
    NOSQL_REQUIRE(9 <= page_size_power && page_size_power <= 16,
                  "page size out of range, must be between 2^9 and 2^16, was 2^" << page_size_power_);
  }

  NO_DISCARD page_size_t GetPageSize() const noexcept { return page_size_; }

  NO_DISCARD page_number_t GetFreeListPage() const noexcept { return free_list_page_; }

  NO_DISCARD page_number_t GetIndexPage() const noexcept { return index_page_; }

private:
  //! \brief The magic sequence for the database.
  static inline uint64_t meta_magic_number_ = ToUInt64("NeverSQL");  // Null terminated.

  //! \brief The power of 2 used for the page size, alternatively, lg(page_size_).
  uint8_t page_size_power_ = 12;

  //! \brief The size of the page in bytes, computed by page_size_ = 2 ^ page_size_power_.
  page_size_t page_size_ = 4096;

  // =================================================================================================
  // Other data.
  // =================================================================================================

  //! \brief The page on which the free list starts. Will be 0 if unassigned.
  page_number_t free_list_page_ {};

  //! \brief The page on which the B-tree index starts. Will be 0 if unassigned.
  page_number_t index_page_ {};
};

}  // namespace neversql
