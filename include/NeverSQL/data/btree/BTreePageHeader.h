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
//! Flag bits: flags that give information about the page.
//! Flags bit layout:
//!     0000 0K RP
//!
//! P => Pointers page (0 = no, 1 = yes). Leaf nodes and the root node in pointer-mode (when the root has
//!     children) have this flag set to true.
//! R => Root node (0 = no, 1 = yes). The root node has this flag set to true.
//! K => Key sizes specified (0 = no, 1 = yes). The key size is a 16 bit integer.
//!
//! The rest of the bits are reserved for future use and denoted by '0's.
//!
//! Header layout:
//! Magic number:       8 bytes,    offset 0
//! Flags:              1 byte,     offset 8
//! Free start:         2 bytes,    offset 9
//! Free end:           2 bytes,    offset 11
//! Reserved start:     2 bytes,    offset 13
//! Page number:        8 bytes,    offset 15
//! Additional data:    8 bytes,    offset 23
//!
//! Pointers start at offset 31
class BTreePageHeader {
  friend class BTreeNodeMap;

public:
  NO_DISCARD uint64_t GetMagicNumber() const noexcept { return page_->Read<uint64_t>(0); }
  NO_DISCARD uint8_t GetFlags() const noexcept { return page_->Read<uint8_t>(8); }
  NO_DISCARD page_size_t GetFreeStart() const noexcept { return page_->Read<page_size_t>(9); }
  NO_DISCARD page_size_t GetFreeEnd() const noexcept { return page_->Read<page_size_t>(11); }
  NO_DISCARD page_size_t GetReservedStart() const noexcept { return page_->Read<page_size_t>(13); }
  NO_DISCARD page_number_t GetPageNumber() const noexcept { return page_->Read<page_number_t>(15); }
  NO_DISCARD page_number_t GetAdditionalData() const noexcept { return page_->Read<page_number_t>(23); }
  NO_DISCARD page_size_t GetPageSize() const noexcept { return page_->GetPageSize(); }

  void SetMagicNumber(uint64_t magic_number) { page_->WriteToPage(0, magic_number); }
  void SetFlags(uint8_t flags) { page_->WriteToPage(8, flags); }
  void SetFreeBegin(page_size_t free_begin) { page_->WriteToPage(9, free_begin); }
  void SetFreeEnd(page_size_t free_end) { page_->WriteToPage(11, free_end); }
  void SetReservedStart(page_size_t reserved_start) { page_->WriteToPage(13, reserved_start); }
  void SetPageNumber(page_number_t page_number) { page_->WriteToPage(15, page_number); }
  void SetAdditionalData(page_number_t data) { page_->WriteToPage(23, data); }

  NO_DISCARD page_size_t GetPointersStart() const noexcept { return 31; }

  // =========================================================================================
  //  Other helper functions.
  // =========================================================================================

  void InitializePage(page_number_t page_number, BTreePageType type, page_size_t reserved_size = 0) {
    SetMagicNumber(ToUInt64("NOSQLBTR"));
    SetPageNumber(page_number);
    SetFlags(static_cast<uint8_t>(type));
    auto reserved_start = static_cast<page_size_t>(GetPageSize() - reserved_size);
    SetReservedStart(reserved_start);
    SetFreeEnd(reserved_start);
    SetFreeBegin(GetPointersStart());
  }

  //! \brief Get the number of pointers on the page.
  NO_DISCARD page_size_t GetNumPointers() const noexcept {
    return (GetFreeStart() - GetPointersStart()) / sizeof(page_size_t);
  }

  //! \brief Get the amount of de-fragmented free space on the page.
  NO_DISCARD page_size_t GetDefragmentedFreeSpace() const { return GetFreeEnd() - GetFreeStart(); }

  //! \brief Check whether this page is a pointers page, that is, whether it only stores pointers to other
  //! pages instead of storing data.
  NO_DISCARD bool IsPointersPage() const noexcept { return (GetFlags() & 0b1) != 0; }

  //! \brief Check whether this page is the root page.
  NO_DISCARD bool IsRootPage() const noexcept { return (GetFlags() & 0b10) != 0; }

  //! \brief Check whether the key sizes are specified for this BTree.
  NO_DISCARD bool AreKeySizesSpecified() const noexcept { return (GetFlags() & 0b100) != 0; }

  //! \brief Get the type of this page.
  NO_DISCARD BTreePageType GetPageType() const noexcept {
    return static_cast<BTreePageType>(GetFlags() & 0b11);
  }

private:
  explicit BTreePageHeader(Page* page)
      : page_(page) {}

  Page* page_;
};

}  // namespace neversql
