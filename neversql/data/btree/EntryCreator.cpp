//
// Created by Nathaniel Rupprecht on 3/27/24.
//

#include "neversql/data/btree/EntryCreator.h"
// Other files.
#include "neversql/data/btree/BTree.h"
#include "neversql/data/internals/Utility.h"

#include "neversql/data/Page.h"

namespace neversql::internal {

EntryCreator::EntryCreator(uint64_t transaction_id, std::unique_ptr<EntryPayloadSerializer>&& payload, bool serialize_size)
    : serialize_size_(serialize_size)
    , transaction_id_(transaction_id)
    , payload_(std::move(payload)) {}

page_size_t EntryCreator::GetMinimumEntrySize() const {
  // An overflow page header needs 16 bytes (plus the flags and whatever the B-tree needs).
  return 16;
}

page_size_t EntryCreator::GetRequestedSize(page_size_t maximum_entry_size) {
  if (next_overflow_entry_size_) {
    return next_overflow_entry_size_ + sizeof(primary_key_t) + sizeof(page_size_t);
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

  uint8_t flags =
      // The page is active.
      IsActive
      // The note flag is set if the entry is an overflow page entry or the entry size is serialized.
      | (serialize_size_ || overflow_page_needed_ ? NoteFlag : 0)
      // An entry on an overflow page should be loaded as a single page entry, since it just contains the data
      // for this part of the overflow page, plus some additional data to find the next overflow page (if
      // applicable) and the logic for traversing all the pages is handled elsewhere.
      | (overflow_page_needed_ && next_overflow_entry_size_ == 0 ? 0 : IsSinglePageEntry);
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

  Transaction transaction {0};  // TODO

  auto offset = starting_offset;

  // Get an overflow entry number.
  auto overflow_key = btree_manager->getNextOverflowEntryNumber();
  offset = transaction.WriteToPage(*page, offset, overflow_key);
  LOG_SEV(Trace) << "Creating overflow entry with overflow key " << overflow_key << ".";

  // For each subsequent overflow page:
  // [next overflow page number: 8 bytes]? [entry_size: 2 bytes] [entry_data: entry_size bytes]
  // If there is no next overflow page, the next overflow page number is 0.

  // Get the overflow page, making sure that we go to a new page if ther would not be enough space on the
  // current page.
  auto overflow_page_number = loadOverflowPage(overflow_key, btree_manager);

  // Write the overflow page number.
  offset = transaction.WriteToPage(*page, offset, overflow_page_number);
  writeOverflowData(overflow_key, overflow_page_number, btree_manager);

  // Return the offset after the entry on the *primary* page (not any of the overflow pages).
  return offset;
}

page_size_t EntryCreator::createSinglePageEntry(page_size_t starting_offset, Page* page) {
  Transaction transaction {0};  // TODO

  // Write the entry size to the page. If the entry size needs to be serialized, it is written first.
  // Note that the entry flags indicate whether the entry size is serialized.
  auto offset = starting_offset;
  if (serialize_size_) {
    const auto entry_size = static_cast<page_size_t>(payload_->GetRequiredSize());
    LOG_SEV(Trace) << "Writing entry size " << entry_size << " for single page entry at " << offset << ".";
    offset = transaction.WriteToPage(*page, offset, entry_size);
  }

  // Write the entry payload to the page.
  LOG_SEV(Trace) << "Starting writing data for single page entry at " << offset << ".";
  while (payload_->HasData()) {
    // Note that the payload acts like a generator, you keep asking for the next byte until it is empty.
    offset = transaction.WriteToPage(*page, offset, payload_->GetNextByte());
  }

  LOG_SEV(Trace) << "Done writing data for single page entry, offset is " << offset << ".";
  return offset;
}

page_size_t EntryCreator::createOverflowDataEntry(page_size_t starting_offset, Page* page) {
  Transaction transaction {0};  // TODO

  LOG_SEV(Trace) << "Writing data to overflow page (page " << page->GetPageNumber() << ") at "
                 << starting_offset << ", will write " << next_overflow_entry_size_ << " bytes.";
  auto offset = starting_offset;

  // Since this will be read as a single page entry (its just a special single page entry), we need to first
  // serialize the entry size, so the Entry reader will work properly.
  const auto entry_size = static_cast<page_size_t>(sizeof(primary_key_t) + next_overflow_entry_size_);
  LOG_SEV(Trace) << "Writing entry size " << entry_size << " for single page entry at " << offset << ".";
  offset = transaction.WriteToPage(*page, offset, entry_size);

  // First part of the overflow entry is the next page number, which is 0 if there is no next page.
  offset = transaction.WriteToPage(*page, offset, next_overflow_page_);  // 8 bytes

  // Then, all the data is written.
  LOG_SEV(Trace) << "Writing overflow data to offset " << offset << " on page " << page->GetPageNumber()
                 << ".";
  for (std::size_t i = 0; i < next_overflow_entry_size_; ++i) {
    offset = transaction.WriteToPage(*page, offset, payload_->GetNextByte());
  }
  LOG_SEV(Trace) << "Done writing data to overflow page (page " << page->GetPageNumber() << "), offset is "
                 << offset << ".";

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

  // Helper lambda to load the next overflow page, making sure that there is enough space in the page.
  auto load_next_overflow_page = [&] {
    const auto remaining_space = static_cast<page_size_t>(total_size - serialized_size);
    for (;;) {
      next_overflow_page_number = btree_manager->getNextOverflowPage();
      next_overflow_page = btree_manager->loadNodePage(next_overflow_page_number);
      const auto max_entry_space =
          next_overflow_page->CalculateSpaceRequirements(general_overflow_key).max_entry_space;
      if (header_size + std::min(min_overflow_entry_capacity_, remaining_space) < max_entry_space) {
        LOG_SEV(Trace) << "Found suitable overflow page, page " << next_overflow_page_number << ".";
        break;  // Found a suitable page.
      }
    }
  };

  // This here is only a check if the initial overflow page was actually not suitable. It is not responsible
  // for loading the "next" page.
  auto max_entry_space = overflow_page->CalculateSpaceRequirements(general_overflow_key).max_entry_space;
  if (max_entry_space < header_size) {
    load_next_overflow_page();
    overflow_page = std::move(*next_overflow_page);
    next_overflow_page_number = 0;
    LOG_SEV(Trace) << "Initial overflow page was not suitable, loading new page.";
  }

  // Keep loading pages and storing data as long as is necessary.
  while (payload_->HasData()) {
    // Check how much space is available in the current page. If there is not enough space in the overflow
    // page, we will need another overflow page.
    max_entry_space = overflow_page->CalculateSpaceRequirements(general_overflow_key).max_entry_space;

    const bool needs_next_page = std::cmp_less(max_entry_space - header_size, total_size - serialized_size);
    // If necessary, load the next overflow page.
    if (needs_next_page) {
      load_next_overflow_page();
      LOG_SEV(Trace) << "Another overflow page will be needed, page will be " << next_overflow_page_number
                     << ".";
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
      next_overflow_page_number = 0;  // As far as we know, there is no next overflow page.
    }
  }

  LOG_SEV(Debug) << "Done creating overflow entry.";
}

page_number_t EntryCreator::loadOverflowPage(primary_key_t overflow_key, BTreeManager* btree_manager) {
  auto overflow_page_number = btree_manager->getCurrentOverflowPage();
  auto overflow_page = btree_manager->loadNodePage(overflow_page_number);

  // Convert the overflow_key, as a primary_key_t, to a GeneralKey
  GeneralKey general_overflow_key = SpanValue(overflow_key);

  // [next overflow page number: 8 bytes]? [entry_size: 2 bytes]? [entry_data: entry_size bytes]
  constexpr page_size_t header_size = sizeof(primary_key_t) + sizeof(entry_size_t);

  // Helper lambda to load the next overflow page, making sure that there is enough space in the page.
  auto load_next_overflow_page = [&] {
    for (;;) {
      overflow_page_number = btree_manager->getNextOverflowPage();
      overflow_page = btree_manager->loadNodePage(overflow_page_number);
      const auto max_entry_space =
          overflow_page->CalculateSpaceRequirements(general_overflow_key).max_entry_space;
      if (header_size + min_overflow_entry_capacity_ < max_entry_space) {
        LOG_SEV(Trace) << "Found suitable overflow page, page " << overflow_page_number << ".";
        return;
      }
    }
  };

  auto max_entry_space = overflow_page->CalculateSpaceRequirements(general_overflow_key).max_entry_space;
  if (max_entry_space < header_size) {
    LOG_SEV(Trace) << "Initial overflow page was not suitable, loading new page.";
    load_next_overflow_page();
  }
  return overflow_page_number;
}

}  // namespace neversql::internal
