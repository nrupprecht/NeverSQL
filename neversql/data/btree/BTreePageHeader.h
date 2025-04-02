//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#pragma once

#include "neversql/data/internals/PageUtilities.h"

namespace neversql {

enum class BTreePageType : uint8_t {
  // clang-format off
  Leaf         = 0b000,
  Internal     = 0b001,
  RootLeaf     = 0b010,
  RootInternal = 0b011,
  OverflowPage = 0b100,
};

inline constexpr uint8_t POINTERS_PAGE_FLAG = 0x1;
inline constexpr uint8_t ROOT_PAGE_FLAG = 0x2;
inline constexpr uint8_t KEY_SIZES_SERIALIZED_FLAG = 0x4;
inline constexpr uint8_t OVERFLOW_PAGE_FLAG = 0x8;


// clang-format off
//! \brief The header for a B-tree page.
//!
//! Sections and pointers to end of section (one past):
//!
//! | Header         | Pointers    | Free space | Stored data    | Reserved space |
//! |---------------:|------------:|-----------:|---------------:|---------------:|
//! | sizeof(Header) | free_start  | free_end   | reserved_start | page_size      |
//!
//! The rest of the bits are reserved for future use and denoted by '0's.
//!
//! | Entry           | Size    | Offset |
//! |:----------------|:-------:|:------:|
//! | Magic number    | 8 bytes | 0      |
//! | Flags           | 1 byte  | 8      |
//! | Free start      | 2 bytes | 9      |
//! | Free end        | 2 bytes | 11     |
//! | Reserved start  | 2 bytes | 13     |
//! | Page number     | 8 bytes | 15     |
//! | Additional data | 8 bytes | 23     |
//!
//! Pointers start at offset 31.
//!
//! Flag definitions:
//!
//! | Bit | Name  | Description  |
//! |:---:|:-----:|:-------------|
//! |   0 | P     | Pointers page (0 = no, 1 = yes). Leaf nodes and the root node in pointer-mode (when the root has children) have this flag set to true. |
//! |   1 | R     | Root node (0 = no, 1 = yes). The root node has this flag set to true. |
//! |   2 | K     | Key sizes specified (0 = no, 1 = yes). The key size, if serialized, is a 16 bit integer. |
//! |   3 | V     | Is overflow page (0 = no, 1 = yes). |
// clang-format on
class BTreePageHeader {
  friend class BTreeNodeMap;

  struct Header {
    STRUCT_DATA(uint64_t, magic_number);          // 8 bytes
    STRUCT_DATA(uint8_t, flags);                  // 1 byte
    STRUCT_DATA(page_size_t, free_begin);         // 2 bytes
    STRUCT_DATA(page_size_t, free_end);           // 2 bytes
    STRUCT_DATA(page_size_t, reserved_start);     // 2 bytes
    STRUCT_DATA(page_number_t, page_number);      // 8 bytes
    STRUCT_DATA(page_number_t, additional_data);  // 8 bytes
  };

public:
  NO_DISCARD uint64_t GetMagicNumber() const noexcept { return page_->Read<uint64_t>(&Header::magic_number); }

  NO_DISCARD uint8_t GetFlags() const noexcept { return page_->Read<uint8_t>(&Header::flags); }

  NO_DISCARD page_size_t GetFreeBegin() const noexcept {
    return page_->Read<page_size_t>(&Header::free_begin);
  }

  NO_DISCARD page_size_t GetFreeEnd() const noexcept { return page_->Read<page_size_t>(&Header::free_end); }

  NO_DISCARD page_size_t GetReservedStart() const noexcept {
    return page_->Read<page_size_t>(&Header::reserved_start);
  }

  NO_DISCARD page_number_t GetPageNumber() const noexcept {
    return page_->Read<page_number_t>(&Header::page_number);
  }

  NO_DISCARD page_number_t GetAdditionalData() const noexcept {
    return page_->Read<page_number_t>(&Header::additional_data);
  }

  NO_DISCARD page_size_t GetPointersStart() const noexcept { return sizeof(Header); }

  NO_DISCARD page_size_t GetPageSize() const noexcept { return page_->GetPageSize(); }

  // Mutators

  void SetMagicNumber(Transaction& transaction, uint64_t magic_number) {
    transaction.WriteToPage(*page_, &Header::magic_number, magic_number);
  }

  void SetFlags(Transaction& transaction, uint8_t flags) {
    transaction.WriteToPage(*page_, &Header::flags, flags);
  }

  void SetFreeBegin(Transaction& transaction, page_size_t free_begin) {
    transaction.WriteToPage(*page_, &Header::free_begin, free_begin);
  }

  void SetFreeEnd(Transaction& transaction, page_size_t free_end) {
    transaction.WriteToPage(*page_, &Header::free_end, free_end);
  }

  void SetReservedStart(Transaction& transaction, page_size_t reserved_start) {
    transaction.WriteToPage(*page_, &Header::reserved_start, reserved_start);
  }

  void SetPageNumber(Transaction& transaction, page_number_t page_number) {
    transaction.WriteToPage(*page_, &Header::page_number, page_number);
  }

  void SetAdditionalData(Transaction& transaction, page_number_t data) {
    transaction.WriteToPage(*page_, &Header::additional_data, data);
  }

  // =========================================================================================
  //  Other helper functions.
  // =========================================================================================

  void InitializePage(page_number_t page_number, BTreePageType type, page_size_t reserved_size = 0) {
    Transaction transaction {0};  // TODO

    SetMagicNumber(transaction, ToUInt64("NOSQLBTR"));
    SetPageNumber(transaction, page_number);
    SetFlags(transaction, static_cast<uint8_t>(type));
    // Reserved space is at the end of the page.
    const auto reserved_start = static_cast<page_size_t>(GetPageSize() - reserved_size);
    SetReservedStart(transaction, reserved_start);
    SetFreeEnd(transaction, reserved_start);
    SetFreeBegin(transaction, GetPointersStart());
  }

  void InitializeOverflowPage(page_number_t page_number) {
    Transaction transaction {0};  // TODO

    SetMagicNumber(transaction, ToUInt64("OVERFLOW"));
    SetPageNumber(transaction, page_number);
    SetFlags(transaction, OVERFLOW_PAGE_FLAG);

    const auto reserved_start = GetPageSize();
    SetReservedStart(transaction, reserved_start);
    SetFreeEnd(transaction, reserved_start);
    SetFreeBegin(transaction, GetPointersStart());
  }

  //! \brief Get the number of pointers on the page.
  NO_DISCARD page_size_t GetNumPointers() const noexcept {
    return (GetFreeBegin() - GetPointersStart()) / sizeof(page_size_t);
  }

  //! \brief Get the amount of de-fragmented free space on the page.
  NO_DISCARD page_size_t GetDefragmentedFreeSpace() const { return GetFreeEnd() - GetFreeBegin(); }

  //! \brief Check whether this page is a pointers page, that is, whether it only stores pointers to other
  //!        pages instead of storing data.
  NO_DISCARD bool IsPointersPage() const noexcept { return (GetFlags() & 0b1) != 0; }

  //! \brief Check whether this page is the root page.
  NO_DISCARD bool IsRootPage() const noexcept { return (GetFlags() & 0b10) != 0; }

  //! \brief Check whether this page is an overflow page.
  NO_DISCARD bool IsOverflowPage() const noexcept { return (GetFlags() & OVERFLOW_PAGE_FLAG) != 0; }

  //! \brief Check whether this is a data page - either an overflow or leaf page.
  NO_DISCARD bool IsDataPage() const noexcept { return !IsPointersPage(); }

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
