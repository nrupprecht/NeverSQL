//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#include "NeverSQL/data/btree/BTreeNodeMap.h"
// Other files.

namespace neversql {

BTreePageHeader BTreeNodeMap::GetHeader() {
  // TODO: Remove. All modifications need to go through logging.
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

const Page& BTreeNodeMap::GetPage() const {
  return *page_;
}

NO_DISCARD Page& BTreeNodeMap::GetPage() {
  return *page_;
}

page_size_t BTreeNodeMap::GetNumPointers() const {
  return getHeader().GetNumPointers();
}

page_size_t BTreeNodeMap::GetDefragmentedFreeSpace() const {
  return getHeader().GetDefragmentedFreeSpace();
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

BTreeNodeMap::BTreeNodeMap(std::unique_ptr<Page>&& page) noexcept
    : page_(std::move(page)) {}

BTreePageHeader BTreeNodeMap::getHeader() const {
  return BTreePageHeader(page_.get());
}

std::optional<page_size_t> BTreeNodeMap::getCellByKey(GeneralKey key) const {
  std::span<const page_size_t> pointers = getPointers();

  // Note: there was an issue trying to point in the span directly as the second argument, so I am using a placeholder
  // value (0) and just using the compare function directly, ignoring its second argument (0).
  auto it = std::ranges::lower_bound(pointers, 0 /* unused */, [this, key](page_size_t ptr, [[maybe_unused]] int) {
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

std::optional<std::pair<page_size_t, page_index_t>> BTreeNodeMap::getCellLowerBoundByPK(GeneralKey key) const {
  std::span<const page_size_t> pointers = getPointers();
  auto it = std::ranges::lower_bound(
      pointers, key, cmp_, [this](auto&& ptr) { return getKeyForCell(ptr); });
  if (it == pointers.end()) {
    return {};
  }
  return std::make_optional(std::pair{*it, static_cast<page_index_t>(std::distance(pointers.begin(), it))});
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
  NOSQL_ASSERT(offset.has_value(), "could not find a cell with a key greater than or equal to " << debugKey(key));
  // Offset gets us to the primary key, so we need to add the size of the primary key to get to the page
  // number.
  return {page_->Read<primary_key_t>(offset->first + sizeof(primary_key_t)), offset->second};
}

std::span<const page_size_t> BTreeNodeMap::getPointers() const {
  auto&& header = getHeader();
  auto start_ptrs = header.GetPointersStart();
  auto num_pointers = header.GetNumPointers();

  return page_->GetSpan<const page_size_t>(start_ptrs, num_pointers);
}

GeneralKey BTreeNodeMap::getKeyForCell(page_size_t cell_offset) const {
  if (getHeader().AreKeySizesSpecified()) {
    auto key_size = page_->Read<uint16_t>(cell_offset);
    return page_->GetSpan(cell_offset + sizeof(uint16_t), key_size);
  }
  // TODO: For now at least, assume that keys whose size are not specified are uint64_t. This can be relaxed
  //  later.
  return page_->GetSpan(cell_offset, sizeof(primary_key_t));
}

GeneralKey BTreeNodeMap::getKeyForNthCell(page_size_t cell_index) const {
  auto&& pointers = getPointers();
  NOSQL_ASSERT(cell_index < pointers.size(), "cell number " << cell_index << " is out of range");
  return getKeyForCell(pointers[cell_index]);
}

std::variant<DataNodeCell, PointersNodeCell> BTreeNodeMap::getCell(page_size_t cell_offset) const {
  const auto& header = getHeader();
  auto key_size_is_serialized = header.AreKeySizesSpecified();
  std::span<const std::byte> key;

  auto post_key_offset = cell_offset;
  if (key_size_is_serialized) {
    auto key_size = page_->Read<uint16_t>(cell_offset);
    post_key_offset += sizeof(uint16_t) + key_size;
    key = page_->GetSpan(cell_offset + sizeof(uint16_t), key_size);
  }
  else {
    // TODO: For now at least, assume that keys whose size are not specified are uint64_t. This can be relaxed later.
    post_key_offset += sizeof(primary_key_t);
    key = page_->GetSpan(cell_offset, sizeof(primary_key_t));
  }

  if (getHeader().IsPointersPage()) {
    return PointersNodeCell {.key = key,
                             .page_number = page_->Read<page_number_t>(post_key_offset),
                             .key_size_is_serialized = key_size_is_serialized};
  }
  return DataNodeCell {
      .key = key,
      .size_of_entry = page_->Read<entry_size_t>(post_key_offset),
      .start_of_value = page_->GetData() + post_key_offset + sizeof(entry_size_t),
      .key_size_is_serialized = key_size_is_serialized};
}

std::variant<DataNodeCell, PointersNodeCell> BTreeNodeMap::getNthCell(page_size_t cell_number) const {
  auto&& pointers = getPointers();
  NOSQL_ASSERT(cell_number < pointers.size(), "cell number " << cell_number << " is out of range");
  return getCell(pointers[cell_number]);
}

void BTreeNodeMap::sortKeys() {
  auto pointers = getPointers();

  std::vector<page_size_t> data {pointers.begin(), pointers.end()};

  std::ranges::sort(
      data, [this](auto&& ptr1, auto&& ptr2) { return cmp_(getKeyForCell(ptr1), getKeyForCell(ptr2)); });
  std::span<page_size_t> sorted_ptrs {data.data(), data.size()};
  GetPage().WriteToPage(getHeader().GetPointersStart(), sorted_ptrs);
}

std::string BTreeNodeMap::debugKey(GeneralKey key) const {
  if (debug_key_func_) {
    return debug_key_func_(key);
  }
  // Hex dump the key.
  return internal::HexDumpBytes(key);
}

}  // namespace neversql
