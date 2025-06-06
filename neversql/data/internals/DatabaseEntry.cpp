//
// Created by Nathaniel Rupprecht on 3/28/24.
//

#include "neversql/data/internals/DatabaseEntry.h"
// Other files.
#include "neversql/data/Document.h"
#include "neversql/data/btree/BTree.h"
#include "neversql/data/btree/EntryCreator.h"
#include "neversql/data/internals/OverflowEntry.h"
#include "neversql/data/internals/SinglePageEntry.h"

namespace neversql::internal {

std::unique_ptr<DatabaseEntry> ReadEntry(page_size_t starting_offset,
                                         std::unique_ptr<const Page>&& page,
                                         const BTreeManager* btree_manager) {
  // TODO: This function takes care of some things that are the B-tree's responsibility, i.e., key related
  //       things. These things should probably be moved to the B-tree, and the offset should be at the start
  //       of the entry, and the flags are passed in.

  // Single page entry.
  // [flags: 1 byte]
  // [key_size: 2 bytes]?
  // [key: 8 bytes | variable]
  // -----------------------------------
  // [entry_size: 4 bytes]
  // [entry_data: entry_size bytes]

  // Overflow entry header.
  // [flags: 1 byte]
  // [key_size: 2 bytes]?
  // [key: 8 bytes | variable]
  // -----------------------------------
  // [overflow_key: 8 bytes]
  // [overflow page number: 8 bytes]

  LOG_SEV(Trace) << "Reading entry, starting offset is " << starting_offset << ".";

  // Read flags to determine whether the entry is a single database entry or an overflow entry.
  const auto flags = page->Read<std::byte>(starting_offset);
  // Read individual flags.
  const bool is_active = GetIsActive(flags);
  NOSQL_ASSERT(is_active, "cannot load entry, entry is inactive");
  const bool is_single_page = GetIsSinglePageEntry(flags);
  const bool key_size_serialized = GetKeySizeIsSerialized(flags);

  auto entry_offset = static_cast<page_size_t>(starting_offset + 1);  // Skip the flags.
  if (key_size_serialized) {
    const auto key_size = page->Read<page_size_t>(entry_offset);
    entry_offset += sizeof(page_size_t) + key_size;
  }
  else {
    entry_offset += sizeof(primary_key_t);
  }

  LOG_SEV(Trace) << "ReadEntry: Start of cell data is at offset " << entry_offset << " in page "
                 << page->GetPageNumber() << ".";

  if (is_single_page) {
    return std::make_unique<SinglePageEntry>(entry_offset, std::move(page));
  }

  auto header = page->GetSpan(entry_offset, 16);
  return std::make_unique<OverflowEntry>(header, btree_manager);
}

std::unique_ptr<Document> EntryToDocument(DatabaseEntry& entry) {
  NOSQL_REQUIRE(entry.IsValid(), "entry is not valid");
  // TODO: Some smarter, byte-by-byte document construction, so we don't need the intermediate buffer.
  lightning::memory::MemoryBuffer<std::byte> buffer;
  do {
    auto data = entry.GetData();
    buffer.Append(data);
  } while (entry.Advance());
  auto view = std::span {buffer.Data(), buffer.Size()};
  return ReadDocumentFromBuffer(view);
}

}  // namespace neversql::internal