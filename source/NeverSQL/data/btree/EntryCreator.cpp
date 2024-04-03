//
// Created by Nathaniel Rupprecht on 3/27/24.
//

#include "NeverSQL/data/btree/EntryCreator.h"
// Other files.
#include "NeverSQL/data/Page.h"

namespace neversql::internal {

EntryCreator::EntryCreator(std::unique_ptr<EntryPayloadSerializer>&& payload, bool serialize_size)
    : serialize_size_(serialize_size)
    , payload_(std::move(payload)) {}

page_size_t EntryCreator::GetMinimumEntrySize() const {
  // An overflow page header needs 16 bytes (plus the flags and whatever the B-tree needs).
  return 16;
}

page_size_t EntryCreator::GetRequestedSize(page_size_t maximum_entry_size) {
if (maximum_entry_size == 1) {
  std::cout << "";
}

  NOSQL_REQUIRE(GetMinimumEntrySize() <= maximum_entry_size,
                "maximum entry size too small ("
                    << maximum_entry_size << ", minimum is " << GetMinimumEntrySize()
                    << "), this should have been checked before calling this function");

  auto size = (serialize_size_ ? sizeof(page_size_t) : 0) + payload_->GetRequiredSize();
  if (maximum_entry_size < size) {
    LOG_SEV(Trace) << "Size of entry is " << size << ", which is larger than the maximum entry size of "
                   << maximum_entry_size << ". Overflow page needed.";
    overflow_page_needed_ = true;
    return 16;
  }
  return size;
}

std::byte EntryCreator::GenerateFlags() const {
  using enum EntryFlags;
  // The note flag is set if the entry is an overflow page entry or the entry size is serialized.
  uint8_t flags = IsActive | (serialize_size_ || overflow_page_needed_ ? NoteFlag : 0)
      | (overflow_page_needed_ ? 0 : IsSinglePageEntry);
  return std::byte {flags};
}

//! \brief Create an entry, starting with the given offset in the page.
page_size_t EntryCreator::Create(page_size_t starting_offset, Page* page, const BTreeManager* btree_manager) {
  // Header or single page entry.
  if (overflow_page_needed_) {
    createOverflowEntry(starting_offset, page, btree_manager);
    return starting_offset + 2 * sizeof(page_number_t);
  }

  // Single page entry.
  return createSinglePageEntry(starting_offset, page);
}

void EntryCreator::createOverflowEntry(page_size_t starting_offset,
                                       Page* page,
                                       const BTreeManager* btree_manager) {
  // [overflow_key: 8 bytes] [overflow page number: 8 bytes]

  // For each subsequent overflow page:
  // [next overflow page number: 8 bytes]? [entry_size: 2 bytes] [entry_data: entry_size bytes]

  NOSQL_FAIL("unimplemented");
}

page_size_t EntryCreator::createSinglePageEntry(page_size_t starting_offset, Page* page) {
  // Entry size.
  auto offset = starting_offset;
  if (serialize_size_) {
    const auto entry_size = static_cast<page_size_t>(payload_->GetRequiredSize());
    offset = page->WriteToPage(starting_offset, entry_size);
  }
  while (payload_->HasData()) {
    offset = page->WriteToPage(offset, payload_->GetNextByte());
  }
  return offset;
}

}  // namespace neversql::internal