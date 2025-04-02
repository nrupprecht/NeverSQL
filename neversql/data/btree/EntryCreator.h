//
// Created by Nathaniel Rupprecht on 3/26/24.
//

#pragma once

#include "neversql/data/internals/EntryPayloadSerializer.h"
#include "neversql/utility/Defines.h"

namespace neversql {
class Page;
class BTreeManager;
class BTreeNodeMap;
}  // namespace neversql

namespace neversql::internal {

enum EntryFlags : uint8_t {
  IsActive = 0b1000'0000,
  KeySizeIsSerialized = 0b0100'0000,
  // ...
  NoteFlag = 0b0010,
  IsSinglePageEntry = 0b0001
};

inline bool GetIsActive(std::byte flags) {
  return (static_cast<uint8_t>(flags) & static_cast<uint8_t>(EntryFlags::IsActive)) != 0;
}

inline bool GetKeySizeIsSerialized(std::byte flags) {
  return (static_cast<uint8_t>(flags) & static_cast<uint8_t>(EntryFlags::KeySizeIsSerialized)) != 0;
}

inline bool IsNoteFlagTrue(std::byte flags) {
  return (static_cast<uint8_t>(flags) & static_cast<uint8_t>(EntryFlags::NoteFlag)) != 0;
}

inline bool GetIsSinglePageEntry(std::byte flags) {
  return (static_cast<uint8_t>(flags) & static_cast<uint8_t>(EntryFlags::IsSinglePageEntry)) != 0;
}

inline bool GetNextOverflowPageIsPresent(std::byte flags) {
  return IsNoteFlagTrue(flags) && !GetIsSinglePageEntry(flags);
}

inline bool GetIsEntrySizeSerialized(std::byte flags) {
  return IsNoteFlagTrue(flags) && GetIsSinglePageEntry(flags);
}

//! \brief Object that knows how to create entries inside a B-tree, or read B-tree entries to create a
//!        DatabaseEntry.
//!
//! An EntryCreator will be created to create a data payload in a B-tree for some object, like a document,
//! that needs to be stored in the database. The EntryCreator may need to request that the B-tree create
//! overflow pages for it, or at least notify it of the current overflow page number.
//!
//! Single page entry layout (data cell):
// clang-format off
//! [flags: 1 byte] [key_size: 2 bytes]? [key: 8 bytes | variable] + [entry_size: 2 bytes] [entry_data: entry_size bytes]
// clang-format on
//! \note Whether the entry is a single page or overflow page entry is determined by the flags byte.
//!
//! Start of overflow entry layout, "overflow header" (data cell):
// clang-format off
//! [flags: 1 byte] [key_size: 2 bytes]? [key: 8 bytes | variable] + [overflow_key: 8 bytes] [overflow page number: 8 bytes]
// clang-format on
//! \note An overflow entry, when the key size is not serialized, takes at least 19 bytes.
//!       When the key is serialized, it takes at least 21 bytes. The entry size is 16 bytes.
//!
//! Overflow entry continuation layout (data cell):
// clang-format off
//! [flags: 1 byte] [overflow page number: 8 bytes] + [next overflow page number: 8 bytes]? [entry_size: 2 bytes]? [entry_data: entry_size bytes]
// clang-format on
//! \note Whether the next overflow page is present is determined by the flags byte.
//! \note Whether the entry size is serialized is determined by the flags byte.
//!
//! Tombstone entry layout (data cell, not created by an EntryCreator, but listed here for now):
// clang-format off
//! [flags: 1 byte] [cell_size: 2 bytes] [cell contents (freed space): cell_size bytes]
// clang-format on
//!
//! \note: The B-tree is responsible for creating the cell parts before the "+" (i.e., the key related parts),
//! except for (part of) the flags, while the entry creator creates (part of the) flags and the control fields
//! and data payload for the rest of the cell. For tombstone cells, the flags are used to indicate that the
//! cell has been freed
//!
//! Flags:
//! 0b DK00 00NT
//!  * D: Deleted flag: 1 if the entry is active, 0 if it is a tombstone (deleted space).
//!  * K: Key flag: 1 if the key size is serialized, 0 if the key size is not serialized.
//!  ... unused flags...
//!  * N: Note flag: Depends on whether the entry is a single page entry or an overflow entry (currently not
//!       used for single page entries):
//!      * If T == 0: 1 if the next overflow page is present, 0 if the next overflow page is not present.
//!      * If T == 1: 1 if the entry size is serialized, 0 if the entry size is not serialized.
//!  * T: Type flag: 1 if the entry is a single page entry, 0 if the entry is an overflow entry.
//! \note The B-tree is responsible for setting the D and K flags, while the EntryCreator is responsible for
//!       setting the N and T flags.
//!
class EntryCreator {
public:
  virtual ~EntryCreator() = default;

  explicit EntryCreator(uint64_t transaction_id,
                        std::unique_ptr<EntryPayloadSerializer>&& payload,
                        bool serialize_size = true);

  //! \brief The minimum amount of space that the part of an entry that the EntryCreator creates can take up
  //! in a page.
  page_size_t GetMinimumEntrySize() const;

  //! \brief Get how much space the EntryCreator wants in the initial page.
  //!
  //! The EntryCreator may change its internal stage, e.g., store the amount of space it decided on, or store
  //! whether an overflow page is necessary, when this function is called.
  page_size_t GetRequestedSize(page_size_t maximum_entry_size);

  //! \brief Generate the EntryCreator's part of the flags.
  //!
  //! Should be called after GetRequiredSize.
  virtual std::byte GenerateFlags() const;

  //! \brief Create an entry, starting with the given offset in the page.
  //!
  //! \return Returns the offset to the place after the entry, in the original page (even if an overflow page
  //!         was created and data was added to other pages).
  page_size_t Create(page_size_t starting_offset, Page* page, BTreeManager* btree_manager);

  //! \brief Return whether the EntryCreator is going to create overflow pages.
  bool GetNeedsOverflow() const noexcept { return overflow_page_needed_ && next_overflow_entry_size_ == 0; }

protected:
  page_size_t createOverflowEntry(page_size_t starting_offset, Page* page, BTreeManager* btree_manager);
  page_size_t createSinglePageEntry(page_size_t starting_offset, Page* page);
  page_size_t createOverflowDataEntry(page_size_t starting_offset, Page* page);

  //! \brief Load the current overflow page, switching to the next page if the current page does not have
  //!        the minimum allowed amount of space.
  static page_number_t loadOverflowPage(primary_key_t overflow_key, BTreeManager* btree_manager);

  void writeOverflowData(primary_key_t overflow_key,
                         page_number_t overflow_page_number,
                         BTreeManager* btree_manager);

  //! \brief To keep the overflow page from getting too small, if there is not either enough space for the
  //!        entire remaining data to fit in, or at least this much space, we go to the next overflow page
  //!        even if there is a little bit pf space left on the page.
  constexpr static page_size_t min_overflow_entry_capacity_ = 16;

  bool overflow_page_needed_ = false;
  bool serialize_size_ = true;

  primary_key_t next_overflow_page_ {};
  entry_size_t next_overflow_entry_size_ {};

  //! \brief The transaction that produced this EntryCreator.
  uint64_t transaction_id_ {};

  std::unique_ptr<EntryPayloadSerializer> payload_;
};

//! \brief Create an entry creator with a payload of type Payload_t.
template<typename Payload_t, typename... Args_t>
auto MakeCreator(uint64_t transaction_id, Args_t&&... args) {
  return EntryCreator(transaction_id, std::make_unique<Payload_t>(std::forward<Args_t>(args)...));
}

//! \brief Create an entry creator with a payload of type Payload_t that does not serialize the entry size.
template<typename Payload_t, typename... Args_t>
auto MakeSizelessCreator(uint64_t transaction_id, Args_t&&... args) {
  return EntryCreator(transaction_id, std::make_unique<Payload_t>(std::forward<Args_t>(args)...), false);
}

}  // namespace neversql::internal