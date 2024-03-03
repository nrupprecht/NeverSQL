//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#include "NeverSQL/data/btree/BTreeNodeMap.h"
// Other files.

namespace neversql {

BTreePageHeader& BTreeNodeMap::GetHeader() {
  return *reinterpret_cast<BTreePageHeader*>(page_.GetData());
}

const BTreePageHeader& BTreeNodeMap::GetHeader() const {
  return *reinterpret_cast<const BTreePageHeader*>(page_.GetData());
}

BTreePageType BTreeNodeMap::GetType() const {
  return GetHeader().GetPageType();
}

page_number_t BTreeNodeMap::GetPageNumber() const {
  return page_.GetPageNumber();
}

const Page& BTreeNodeMap::GetPage() const {
  return page_;
}

page_size_t BTreeNodeMap::GetNumPointers() const {
  return GetHeader().GetNumPointers();
}

page_size_t BTreeNodeMap::GetDefragmentedFreeSpace() const {
  return GetHeader().GetDefragmentedFreeSpace();
}

std::optional<primary_key_t> BTreeNodeMap::GetLargestKey() const {
  if (auto&& pointers = getPointers(); !pointers.empty()) {
    return getKeyForCell(pointers.back());
  }
  return {};
}

bool BTreeNodeMap::IsPointersPage() const noexcept {
  return GetHeader().IsPointersPage();
}

bool BTreeNodeMap::IsRootPage() const noexcept {
  return GetHeader().IsRootPage();
}

BTreeNodeMap::BTreeNodeMap(Page&& page) noexcept
    : page_(std::move(page)) {}

std::optional<page_size_t> BTreeNodeMap::getCellByPK(primary_key_t key) const {
  std::span<const page_size_t> pointers = getPointers();

  auto it = std::ranges::lower_bound(pointers, key, [this](auto&& ptr, decltype(key) k) {
    return getKeyForCell(static_cast<page_size_t>(ptr)) < k;
  });
  if (it == pointers.end()) {
    return std::nullopt;
  }
  if (getKeyForCell(*it) == key) {
    return *it;
  }
  return {};
}

std::optional<page_size_t> BTreeNodeMap::getCellLowerBoundByPK(primary_key_t key) const {
  std::span<const page_size_t> pointers = getPointers();
  auto it = std::ranges::lower_bound(
      pointers,
      key,
      std::ranges::less{},
      [this](auto&& ptr) { return getKeyForCell(ptr); });
  if (it == pointers.end()) {
    return {};
  }
  return *it;
}

page_number_t BTreeNodeMap::searchForNextPageInPointersPage(primary_key_t key) const {
  NOSQL_REQUIRE(GetHeader().IsPointersPage(), "cannot get next page from a page that is not a pointers page");

  if (GetNumPointers() == 0) {
    auto next_page = GetHeader().additional_data;
    NOSQL_ASSERT(next_page != 0, "next page cannot be the 0 page");
    return next_page;
  }

  auto num_pointers = GetNumPointers();
  auto last_cell = std::get<PointersNodeCell>(getNthCell(num_pointers - 1));
  if (last_cell.key < key) {
    auto&& header = GetHeader();
    auto next_page = header.additional_data;
    NOSQL_ASSERT(next_page != 0,
                 "rightmost pointer in page " << GetPageNumber() << " set to 0, error in rightmost pointer");
    return next_page;
  }
  // Get the offset to the first key that is greater
  auto offset = getCellLowerBoundByPK(key);
  NOSQL_ASSERT(offset.has_value(), "could not find a cell with a key greater than or equal to " << key);
  // Offset gets us to the primary key, so we need to add the size of the primary key to get to the page
  // number.
  return *reinterpret_cast<const page_number_t*>(page_.GetPtr(*offset + sizeof(primary_key_t)));
}

std::span<page_size_t> BTreeNodeMap::getPointers() {
  auto&& header = GetHeader();
  auto start_ptrs = header.GetPointersStart();
  auto num_pointers = header.GetNumPointers();
  return std::span(page_.GetPtr<page_size_t>(start_ptrs), num_pointers);
}

std::span<const page_size_t> BTreeNodeMap::getPointers() const {
  auto&& header = GetHeader();
  auto start_ptrs = header.GetPointersStart();
  auto num_pointers = header.GetNumPointers();
  return std::span(page_.GetPtr<const page_size_t>(start_ptrs), num_pointers);
}

primary_key_t BTreeNodeMap::getKeyForCell(page_size_t cell_offset) const {
  // Copy so we don't have to worry about alignment.
  primary_key_t pk;
  std::memcpy(&pk, page_.GetPtr(cell_offset), sizeof(primary_key_t));
  return pk;
}

primary_key_t BTreeNodeMap::getKeyForNthCell(page_size_t cell_index) const {
  auto&& pointers = getPointers();
  NOSQL_ASSERT(cell_index < pointers.size(), "cell number " << cell_index << " is out of range");
  return getKeyForCell(pointers[cell_index]);
}

std::variant<DataNodeCell, PointersNodeCell> BTreeNodeMap::getCell(page_size_t cell_offset) const {
  auto&& header = GetHeader();
  if (header.IsPointersPage()) {
    return PointersNodeCell {
        getKeyForCell(cell_offset),
        *reinterpret_cast<const page_number_t*>(page_.GetPtr(cell_offset + sizeof(primary_key_t)))};
  }
  return DataNodeCell {
      getKeyForCell(cell_offset),
      page_.CopyAs<entry_size_t>(cell_offset + sizeof(primary_key_t)),
      page_.GetPtr(cell_offset + sizeof(primary_key_t) + sizeof(entry_size_t))};
}

std::variant<DataNodeCell, PointersNodeCell> BTreeNodeMap::getNthCell(page_size_t cell_number) const {
  auto&& pointers = getPointers();
  NOSQL_ASSERT(cell_number < pointers.size(), "cell number " << cell_number << " is out of range");
  return getCell(pointers[cell_number]);
}

void BTreeNodeMap::sortKeys() {
  auto pointers = getPointers();
  std::ranges::sort(pointers,
                    [this](auto&& ptr1, auto&& ptr2) { return getKeyForCell(ptr1) < getKeyForCell(ptr2); });
}

}  // namespace neversql
