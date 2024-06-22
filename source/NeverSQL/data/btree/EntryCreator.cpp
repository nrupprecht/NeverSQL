//
// Created by Nathaniel Rupprecht on 3/27/24.
//

#include "NeverSQL/data/btree/EntryCreator.h"
// Other files.
#include <NeverSQL/data/btree/BTree.h>
#include <NeverSQL/data/internals/Utility.h>

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
  if (next_overflow_entry_size_) {
    return next_overflow_entry_size_ + sizeof(primary_key_t) + sizeof(entry_size_t);
  }

  NOSQL_REQUIRE(GetMinimumEntrySize() <= maximum_entry_size,
                "maximum entry size too small ("
                    << maximum_entry_size << ", minimum is " << GetMinimumEntrySize()
                    << "), this should have been checked before calling this function");

  auto size =
      static_cast<page_size_t>((serialize_size_ ? sizeof(page_size_t) : 0) + payload_->GetRequiredSize());
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
page_size_t EntryCreator::Create(page_size_t starting_offset, Page* page, BTreeManager* btree_manager) {
  // Data in an overflow page.
  if (next_overflow_entry_size_ != 0) {
    return createOverflowDataEntry(starting_offset, page);
  }

  // Header or single page entry.
  if (overflow_page_needed_) {
    return createOverflowEntry(starting_offset, page, btree_manager);
  }

  // Single page entry.
  return createSinglePageEntry(starting_offset, page);
}

page_size_t EntryCreator::createOverflowEntry([[maybe_unused]] page_size_t starting_offset,
                                              [[maybe_unused]] Page* page,
                                              [[maybe_unused]] BTreeManager* btree_manager) {
  // Create the entry in the main page.
  // Header:
  // [overflow_key: 8 bytes] [overflow page number: 8 bytes]

  auto offset = starting_offset;

  // Get an overflow entry number.
  auto overflow_key = btree_manager->getNextOverflowEntryNumber();
  offset = page->WriteToPage(offset, overflow_key);

  // For each subsequent overflow page:
  // [next overflow page number: 8 bytes]? [entry_size: 2 bytes] [entry_data: entry_size bytes]
  // If there is no next overflow page, the next overflow page number is 0.

  // TODO(Nate): Whose job is it to make sure that an overflow page always has at least the minimum amount of
  //             space required?
  auto overflow_page_number = btree_manager->getCurrentOverflowPage();

  // Write the overflow page number.
  offset = page->WriteToPage(offset, overflow_page_number);

  // TODO: Write actual data to the overflow pages.
  writeOverflowData(overflow_key, overflow_page_number, btree_manager);

  // Return the offset after the entry on the *primary* page (not any of the overflow pages).
  return offset;
}

page_size_t EntryCreator::createSinglePageEntry(page_size_t starting_offset, Page* page) {
  // Write the entry size to the page. If the entry size needs to be serialized, it is written first.
  // Note that the entry flags indicate whether the entry size is serialized.
  auto offset = starting_offset;
  if (serialize_size_) {
    const auto entry_size = static_cast<page_size_t>(payload_->GetRequiredSize());
    LOG_SEV(Trace) << "Writing entry size " << entry_size << " for single page entry at " << offset << ".";
    offset = page->WriteToPage(offset, entry_size);
  }

  // Write the entry payload to the page.
  LOG_SEV(Trace) << "Starting writing data for single page entry at " << offset << ".";
  while (payload_->HasData()) {
    // Note that the payload acts like a generator, you keep asking for the next byte until it is empty.
    offset = page->WriteToPage(offset, payload_->GetNextByte());
  }

  LOG_SEV(Trace) << "Done writing data for single page entry, offset is " << offset << ".";
  return offset;
}

page_size_t EntryCreator::createOverflowDataEntry(page_size_t starting_offset, Page* page) {
  LOG_SEV(Trace) << "Writing data to overflow page at " << starting_offset << ", will write "
                 << next_overflow_entry_size_ << " bytes.";
  auto offset = starting_offset;
  offset = page->WriteToPage(offset, next_overflow_page_);        // 8 bytes
  offset = page->WriteToPage(offset, next_overflow_entry_size_);  // 2 bytes
  for (std::size_t i = 0; i < next_overflow_entry_size_; ++i) {
    offset = page->WriteToPage(offset, payload_->GetNextByte());
  }
  LOG_SEV(Trace) << "Done writing data to overflow page, offset is " << offset << ".";
  return offset;
}

void EntryCreator::writeOverflowData(primary_key_t overflow_key,
                                     page_number_t overflow_page_number,
                                     BTreeManager* btree_manager) {
  LOG_SEV(Debug) << "Adding all data for overflow pages for. Overflow page number starts at "
                 << overflow_page_number << ".";

  auto overflow_page = btree_manager->loadNodePage(overflow_page_number);
  std::optional<BTreeNodeMap> next_overflow_page {};
  page_number_t next_overflow_page_number = 0;

  auto total_size = payload_->GetRequiredSize();
  std::size_t serialized_size = 0;

  // Convert the overflow_key, as a primary_key_t, to a GeneralKey
  GeneralKey general_overflow_key = SpanValue(overflow_key);

  // [next overflow page number: 8 bytes]? [entry_size: 2 bytes]? [entry_data: entry_size bytes]
  constexpr page_size_t header_size = sizeof(primary_key_t) + sizeof(entry_size_t);

  // To keep the overflow page from getting too small, if there is not either enough space for the entire
  // remaining data to fit in, or at least this much space, we go to the next overflow page even if there is
  // a little bit pf space left on the page.
  constexpr page_size_t min_entry_capacity = 16;

  // Helper lambda to load the next overflow page, making sure that there is enough space in the page.
  auto load_next_overflow_page = [&] {
    auto remaining_space = static_cast<page_size_t>(total_size - serialized_size);
    for (;;) {
      next_overflow_page_number = btree_manager->getNextOverflowPage();
      next_overflow_page = btree_manager->loadNodePage(next_overflow_page_number);
      const auto max_entry_space =
          next_overflow_page->CalculateSpaceRequirements(general_overflow_key).max_entry_space;
      if (header_size + std::min(min_entry_capacity, remaining_space) < max_entry_space) {
        LOG_SEV(Trace) << "Found suitable overflow page, page " << next_overflow_page_number << ".";
        break;  // Found a suitable page.
      }
    }
  };

  while (payload_->HasData()) {
    // Check how much space is available in the current page. If there is not enough space in the overflow
    // page, we will need another overflow page.
    auto max_entry_space = overflow_page->CalculateSpaceRequirements(general_overflow_key).max_entry_space;
    if (max_entry_space < header_size) {
      load_next_overflow_page();
      overflow_page = std::move(*next_overflow_page);
      overflow_page_number = next_overflow_page_number;
    }

    const bool needs_next_page = max_entry_space - header_size < total_size - serialized_size;
    // If necessary, load the next overflow page.
    if (needs_next_page) {
      load_next_overflow_page();
    }
    LOG_SEV(Trace) << "Max entry space is " << max_entry_space << ", remaining entry data size is "
                   << total_size - serialized_size << ".";
    
    // We need to write the header, plus all the data we can.
    next_overflow_entry_size_ =
        std::min<page_size_t>(max_entry_space - header_size, total_size - serialized_size);
    next_overflow_page_ = next_overflow_page_number;

    // Add entry.
    StoreData store_data {.key = general_overflow_key, .entry_creator = this};
    btree_manager->addElementToNode(*overflow_page, store_data);

    // Add all the data we can to the current overflow page.
    serialized_size += next_overflow_entry_size_;

    if (needs_next_page) {
      // Cue up the next page.
      overflow_page = std::move(*next_overflow_page);
      overflow_page_number = next_overflow_page_number;
    }
  }

  LOG_SEV(Debug) << "Done creating overflow entry.";
}

}  // namespace neversql::internal
