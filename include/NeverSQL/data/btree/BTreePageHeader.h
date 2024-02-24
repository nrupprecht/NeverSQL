//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#pragma once

#include "NeverSQL/utility/Defines.h"

namespace neversql {

enum class BTreePageType : uint8_t {
  Leaf = 0b01,
  Internal = 0b10,
  Root = 0b11,
};

//! \brief The header for a B-tree page.
//!
//! [ HEADER      ][ Pointers ][      FREE SPACE        ][ Stored data  ][ RESERVED SPACE ]
//! |              |           |                         |               |                |
//! 0      sizeof(Header)   free_start               free_end       reserved_start       page_size
//!
struct BTreePageHeader {
  //! \brief A magic byte to identify the page as a B-tree page.
  //!
  //! This should be 0x42.
  uint64_t magic_byte = ToUInt67("NOSQLBTR");

  //! \brief Flags that give information about the page.
  //! 0000 0 K TT
  //! TT => Leaf node (01), Internal node (10), Root node (11)
  //! K  => Key type (0 = uint64_t, 1 = variable length)
  uint8_t flags {};

  //! \brief The start of free space on the page.
  page_size_t free_start {};
  //! \brief The end of free space on the page.
  page_size_t free_end {};

  //! \brief Points to the start of the reserved space on the page.
  //!
  //! If there is no reserved space, this points to the end of the page.
  page_size_t reserved_start {};

  //! \brief The page's number.
  page_number_t page_number {};

  // =================================================================================================
  // Helper functions.
  // =================================================================================================

  //! \brief Helper function to find the start of the pointers space.
  NO_DISCARD static page_size_t GetPointersStart() { return sizeof(BTreePageHeader); }

  //! \brief Get a pointer to the first place where a pointer can be allocated.
  NO_DISCARD page_size_t* GetFirstPointer() {
    return reinterpret_cast<page_size_t*>(Data() + GetPointersStart());
  }

  //! \brief Get a pointer to the next place where a pointer can be allocated.
  NO_DISCARD page_size_t* GetNextPointer() { return reinterpret_cast<page_size_t*>(Data() + free_start); }

  //! \brief Get the number of pointers on the page.
  NO_DISCARD page_size_t GetNumPointers() const {
    return (free_start - GetPointersStart()) / sizeof(page_size_t);
  }

  //! \brief Get the amount of de-fragmented free space on the page.
  NO_DISCARD page_size_t GetDefragmentedFreeSpace() const { return free_end - free_start; }

  //! \brief Get the type of this page.
  NO_DISCARD BTreePageType GetPageType() const { return static_cast<BTreePageType>(flags & 0b11); }

  NO_DISCARD bool IsUInt64Key() const { return (flags & 0b100) == 0; }

  NO_DISCARD std::byte* Data() { return reinterpret_cast<std::byte*>(this); }
};

}  // namespace neversql
