//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#pragma once

#include "NeverSQL/utility/Defines.h"

namespace neversql {

enum class BTreePageType : uint8_t {
  Leaf = 0b00,
  Internal = 0b01,
  RootLeaf = 0b10,
  RootInternal = 0b11,
};

//! \brief The header for a B-tree page.
//!
//! [ HEADER      ][ Pointers ][      FREE SPACE        ][ Stored data  ][ RESERVED SPACE ]
//! |              |           |                         |               |                |
//! 0      sizeof(Header)   free_start               free_end       reserved_start       page_size
//!
struct BTreePageHeader {
  //! \brief A magic number to identify the page as a B-tree page.
  //!
  //! This should be "NOSQLBTR" in ASCII.
  uint64_t magic_number = ToUInt64("NOSQLBTR");

  //! \brief Flags that give information about the page.
  //! Flags bit layout:
  //!     0000 0K RP
  //!
  //! P => Pointers page (0 = no, 1 = yes). Leaf nodes and the root node in pointer-mode (when the root has
  //!     children) have this flag set to true.
  //! R => Root node (0 = no, 1 = yes). The root node has this flag set to true.
  //! K => Key type (0 = uint64_t, 1 = variable length)
  //!
  //! The rest of the bits are reserved for future use and denoted by '0's.
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

  //! \brief Additional data.
  //! If this is a pointers page, this is the "rightmost" pointer, i.e. the pointer to the page containing all data
  //! greater than the greatest key in this page.
  //! If this is a leaf page, I am leaving it empty for now.
  page_number_t additional_data{};

  // =================================================================================================
  // Helper functions.
  // =================================================================================================

  //! \brief Helper function to find the start of the pointers space.
  NO_DISCARD page_size_t GetPointersStart() const noexcept { return sizeof(BTreePageHeader); }

  //! \brief Get a pointer to the first place where a pointer can be allocated.
  NO_DISCARD page_size_t* GetFirstPointer() noexcept {
    return reinterpret_cast<page_size_t*>(Data() + GetPointersStart());
  }

  //! \brief Get a pointer to the next place where a pointer can be allocated.
  NO_DISCARD page_size_t* GetNextPointer() noexcept {
    return reinterpret_cast<page_size_t*>(Data() + free_start);
  }

  //! \brief Get the number of pointers on the page.
  NO_DISCARD page_size_t GetNumPointers() const noexcept {
    return (free_start - GetPointersStart()) / sizeof(page_size_t);
  }

  //! \brief Get the amount of de-fragmented free space on the page.
  NO_DISCARD page_size_t GetDefragmentedFreeSpace() const { return free_end - free_start; }

  //! \brief Check whether this page is a pointers page, that is, whether it only stores pointers to other
  //! pages instead of storing data.
  NO_DISCARD bool IsPointersPage() const noexcept { return (flags & 0b1) != 0; }

  //! \brief Check whether this page is the root page.
  NO_DISCARD bool IsRootPage() const noexcept { return (flags & 0b10) != 0; }

  //! \brief Get the type of this page.
  NO_DISCARD BTreePageType GetPageType() const noexcept {
    return static_cast<BTreePageType>(flags & 0b11);
  }

  //! \brief Check whether the key type for this BTree is a primary_key_t.
  NO_DISCARD bool IsUInt64Key() const noexcept { return (flags & 0b100) == 0; }

  //! \brief Get a std::byte pointer to the start of the page.
  NO_DISCARD std::byte* Data() noexcept { return reinterpret_cast<std::byte*>(this); }
};

}  // namespace neversql
