//
// Created by Nathaniel Rupprecht on 2/18/24.
//

#pragma once

#include <memory>

#include "NeverSQL/utility/Defines.h"

namespace neversql {

//! \brief Programattic representation of the meta page for the database.
class Meta {
  friend class DataAccessLayer;

public:
  explicit Meta(uint16_t page_size_power)
      : page_size_power_(page_size_power)
      , page_size_(1 << page_size_power) {
    // Make sure pages are not too big or small.
    NOSQL_REQUIRE(9 <= page_size_power && page_size_power <= 16,
                  "page size out of range, must be between 2^9 and 2^16, was 2^" << page_size_power_);
  }

  NO_DISCARD uint32_t GetPageSize() const { return page_size_; }

private:
  //! \brief The magic sequence for the database.
  static constexpr char magic_sequence_[17] = "NeverSQL00000000";  // Null terminated.

  uint16_t page_size_power_ = 12;

  //! \brief The size of the page in bytes, computed by page_size_ = 2 ^ page_size_power_.
  uint32_t page_size_ = 4096;

  // Other data.

  //! \brief The page on which the free list starts. Will be 0 if unassigned.
  PageNumber free_list_page_{};

  //! \brief The page on which the B-tree index starts. Will be 0 if unassigned.
  PageNumber index_page_{};
};

}  // namespace neversql
