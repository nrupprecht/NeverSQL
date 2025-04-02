//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#include "neversql/data/btree/BTreeNodeMap.h"
// Other files.
#include "neversql/data/btree/EntryCreator.h"

namespace neversql {

BTreePageHeader BTreeNodeMap::GetHeader() {
  return BTreePageHeader(page_.get());
}

BTreePageType BTreeNodeMap::GetType() const {
  return getHeader().GetPageType();
}

page_number_t BTreeNodeMap::GetPageNumber() const {
  return page_->GetPageNumber();
}

page_size_t BTreeNodeMap::GetPageSize() const {
  return page_->GetPageSize();
}

const std::unique_ptr<Page>& BTreeNodeMap::GetPage() const {
  return page_;
}

std::unique_ptr<Page>& BTreeNodeMap::GetPage() {
  return page_;
}

page_size_t BTreeNodeMap::GetNumPointers() const {
  return getHeader().GetNumPointers();
}

page_size_t BTreeNodeMap::GetDefragmentedFreeSpace() const {
  return getHeader().GetDefragmentedFreeSpace();
}

SpaceRequirement BTreeNodeMap::CalculateSpaceRequirements(GeneralKey key) const {
  SpaceRequirement requirement;

  auto&& header = getHeader();

  // Amount of space needed for the pointer.
  auto pointer_space = static_cast<page_size_t>(sizeof(page_size_t));
  // Calculate amount of space for the cell.
  // [Flags: 1 byte] [Key size: 2 bytes]? [Key: 8 bytes | key-size bytes]
  auto cell_header_space = static_cast<page_size_t>(sizeof(uint8_t) + key.size());
  if (header.AreKeySizesSpecified()) {
    cell_header_space += sizeof(uint16_t);
  }

  // Given the current free space and the space needed for the pointer and the other parts of the cell, what
  // is the maximum amount of space available for the entry (not counting any page entry space restrictions).
  auto free_space = header.GetDefragmentedFreeSpace();
  auto helper_space = pointer_space + cell_header_space;
  requirement.max_entry_space =
      helper_space < free_space ? static_cast<page_size_t>(free_space - helper_space) : 0;
  requirement.pointer_space = pointer_space;
  requirement.cell_header_space = cell_header_space;

  return requirement;
}

std::optional<GeneralKey> BTreeNodeMap::GetLargestKey() const {
  if (auto&& pointers = getPointers(); !pointers.empty()) {
    return getKeyForCell(pointers.back());
  }
  return {};
}

bool BTreeNodeMap::IsPointersPage() const noexcept {
  return getHeader().IsPointersPage();
}

bool BTreeNodeMap::IsRootPage() const noexcept {
  return getHeader().IsRootPage();
}

std::unique_ptr<internal::DatabaseEntry> BTreeNodeMap::GetEntry(GeneralKey key,
                                                                const BTreeManager* btree_manager) const {
  if (!getHeader().IsDataPage()) {
    return nullptr;
  }
  if (const auto cell_offset = getCellByKey(key)) {
    // Have to pass in a new page handle to read entry.
    return internal::ReadEntry(*cell_offset, GetPage()->NewHandle(), btree_manager);
  }
  return nullptr;
}

std::optional<page_size_t> BTreeNodeMap::GetOffset(GeneralKey key) const {
  return getCellByKey(key);
}

BTreeNodeMap::BTreeNodeMap(std::unique_ptr<Page>&& page) noexcept
    : page_(std::move(page)) {}

BTreePageHeader BTreeNodeMap::getHeader() const {
  return BTreePageHeader(page_.get());
}

std::optional<page_size_t> BTreeNodeMap::getCellByKey(GeneralKey key) const {
  std::span<const page_size_t> pointers = getPointers();

  // Note: there was an issue trying to point in the span directly as the second argument, so I am using a
  // placeholder value (0) and just using the compare function directly, ignoring its second argument (0).
  auto it =
      std::ranges::lower_bound(pointers, 0 /* unused */, [this, key](page_size_t ptr, [[maybe_unused]] int) {
        return cmp_(getKeyForCell(ptr), key);
      });
  if (it == pointers.end()) {
    return std::nullopt;
  }
  auto cell_key = getKeyForCell(*it);
  // Crude way to check if the keys are equal without forcing a separate == function to be defined.
  if (!cmp_(cell_key, key) && !cmp_(key, cell_key)) {
    return *it;
  }
  return {};
}

std::optional<std::pair<page_size_t, page_index_t>> BTreeNodeMap::getCellLowerBoundByPK(
    GeneralKey key) const {
  std::span<const page_size_t> pointers = getPointers();
  auto it = std::ranges::lower_bound(pointers, key, cmp_, [this](auto&& ptr) { return getKeyForCell(ptr); });
  if (it == pointers.end()) {
    return {};
  }
  return std::make_optional(std::pair {*it, static_cast<page_index_t>(std::distance(pointers.begin(), it))});
}

std::pair<page_number_t, page_index_t> BTreeNodeMap::searchForNextPageInPointersPage(GeneralKey key) const {
  NOSQL_REQUIRE(getHeader().IsPointersPage(), "cannot get next page from a page that is not a pointers page");

  if (GetNumPointers() == 0) {
    auto next_page = getHeader().GetAdditionalData();
    NOSQL_ASSERT(next_page != 0, "next page cannot be the 0 page");
    return {next_page, 0};
  }

  auto num_pointers = GetNumPointers();
  auto last_cell = std::get<PointersNodeCell>(getNthCell(num_pointers - 1));
  if (cmp_(last_cell.key, key)) {
    auto next_page = getHeader().GetAdditionalData();
    NOSQL_ASSERT(next_page != 0,
                 "rightmost pointer in page " << GetPageNumber() << " set to 0, error in rightmost pointer");
    return {next_page, num_pointers};
  }
  // Get the offset to the first key that is greater
  auto offset = getCellLowerBoundByPK(key);
  NOSQL_ASSERT(offset.has_value(),
               "could not find a cell with a key greater than or equal to " << debugKey(key));

  auto pointer_cell = std::get<PointersNodeCell>(getCell(offset->first));
  return {pointer_cell.page_number, offset->second};
}

std::span<const page_size_t> BTreeNodeMap::getPointers() const {
  auto&& header = getHeader();
  auto start_ptrs = header.GetPointersStart();
  auto num_pointers = header.GetNumPointers();

  return page_->GetSpan<const page_size_t>(start_ptrs, num_pointers);
}

page_size_t BTreeNodeMap::getCellOffsetByIndex(page_size_t cell_index) const {
  auto&& pointers = getPointers();
  NOSQL_ASSERT(cell_index < pointers.size(), "cell number " << cell_index << " is out of range");
  return pointers[cell_index];
}

GeneralKey BTreeNodeMap::getKeyForCell(page_size_t cell_offset) const {
  // Bypass flags.
  cell_offset += 1;

  if (getHeader().AreKeySizesSpecified()) {
    auto key_size = page_->Read<uint16_t>(cell_offset);
    return page_->GetSpan(cell_offset + sizeof(uint16_t), key_size);
  }
  // TODO: For now at least, assume that keys whose size are not specified are uint64_t. This can be relaxed
  // later.
  return page_->GetSpan(cell_offset, sizeof(primary_key_t));
}

GeneralKey BTreeNodeMap::getKeyForNthCell(page_size_t cell_index) const {
  auto&& pointers = getPointers();
  NOSQL_ASSERT(cell_index < pointers.size(), "cell number " << cell_index << " is out of range");
  return getKeyForCell(pointers[cell_index]);
}

std::variant<DataNodeCell, PointersNodeCell> BTreeNodeMap::getCell(page_size_t cell_offset) const {
  // Single page entry.
  // [flags: 1 byte]
  // [key_size: 2 bytes]?
  // [key: 8 bytes | variable]
  // ---- Entry -------------------
  // [entry_size: 2 bytes]
  // [entry_data: entry_size bytes]

  // Overflow entry header.
  // [flags: 1 byte]
  // [key_size: 2 bytes]?
  // [key: 8 bytes | variable]
  // ---- Entry -------------------
  // [overflow_key: 8 bytes]
  // [overflow page number: 8 bytes]

  std::span<const std::byte> key;

  auto entry_offset = cell_offset;

  // Flags.
  const auto flags = page_->Read<std::byte>(entry_offset);
  entry_offset += 1;
  // ==== Read the flags ====
  const bool is_active = internal::GetIsActive(flags);
  NOSQL_ASSERT(is_active, "cannot load entry, entry is inactive");
  const bool is_single_page = internal::GetIsSinglePageEntry(flags);
  const bool key_size_is_serialized = internal::GetKeySizeIsSerialized(flags);
  const bool is_note_flag_true = internal::IsNoteFlagTrue(flags);
  // ========================

  if (key_size_is_serialized) {
    const auto key_size = page_->Read<uint16_t>(entry_offset);
    entry_offset += sizeof(uint16_t);
    key = page_->GetSpan(entry_offset, key_size);
    entry_offset += key_size;
  }
  else {
    // TODO: For now at least, assume that keys whose size are not specified are uint64_t.
    //       This can be relaxed later.
    key = page_->GetSpan(entry_offset, sizeof(primary_key_t));
    entry_offset += sizeof(primary_key_t);
  }

  if (getHeader().IsPointersPage()) {
    return PointersNodeCell {.key = key, .page_number = page_->Read<page_number_t>(entry_offset)};
  }

  // If this is an overflow header, it is 16 bytes. Otherwise, the size of the entry is stored in the next 2
  // bytes.
  const auto potential_entry_size = page_->Read<page_size_t>(entry_offset);
  const auto entry_data = is_single_page
      ? page_->GetSpan(entry_offset + (is_note_flag_true ? sizeof(page_size_t) : 0), potential_entry_size)
      : page_->GetSpan(entry_offset, 16);

  return DataNodeCell {.flags = flags, .key = key, .data = entry_data};
}

std::variant<DataNodeCell, PointersNodeCell> BTreeNodeMap::getNthCell(page_size_t cell_number) const {
  auto&& pointers = getPointers();
  NOSQL_ASSERT(cell_number < pointers.size(), "cell number " << cell_number << " is out of range");
  return getCell(pointers[cell_number]);
}

void BTreeNodeMap::sortKeys(Transaction& transaction) {
  auto pointers = getPointers();

  std::vector<page_size_t> data {pointers.begin(), pointers.end()};

  std::ranges::sort(
      data, [this](auto&& ptr1, auto&& ptr2) { return cmp_(getKeyForCell(ptr1), getKeyForCell(ptr2)); });
  std::span<page_size_t> sorted_ptrs {data.data(), data.size()};
  transaction.WriteToPage(*GetPage(), getHeader().GetPointersStart(), sorted_ptrs);
}

std::string BTreeNodeMap::debugKey(GeneralKey key) const {
  if (debug_key_func_) {
    return debug_key_func_(key);
  }
  // Hex dump the key.
  return internal::HexDumpBytes(key);
}

}  // namespace neversql
