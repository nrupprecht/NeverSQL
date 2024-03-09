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

std::optional<primary_key_t> BTreeNodeMap::GetLargestKey() const {
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
  NOSQL_REQUIRE(getHeader().IsPointersPage(), "cannot get next page from a page that is not a pointers page");

  if (GetNumPointers() == 0) {
    auto next_page = getHeader().GetAdditionalData();
    NOSQL_ASSERT(next_page != 0, "next page cannot be the 0 page");
    return next_page;
  }

  auto num_pointers = GetNumPointers();
  auto last_cell = std::get<PointersNodeCell>(getNthCell(num_pointers - 1));
  if (last_cell.key < key) {
    auto next_page = getHeader().GetAdditionalData();
    NOSQL_ASSERT(next_page != 0,
                 "rightmost pointer in page " << GetPageNumber() << " set to 0, error in rightmost pointer");
    return next_page;
  }
  // Get the offset to the first key that is greater
  auto offset = getCellLowerBoundByPK(key);
  NOSQL_ASSERT(offset.has_value(), "could not find a cell with a key greater than or equal to " << key);
  // Offset gets us to the primary key, so we need to add the size of the primary key to get to the page
  // number.
  return page_->Read<primary_key_t>(*offset + sizeof(primary_key_t));
}

std::span<const page_size_t> BTreeNodeMap::getPointers() const {
  auto&& header = getHeader();
  auto start_ptrs = header.GetPointersStart();
  auto num_pointers = header.GetNumPointers();

  return page_->GetSpan<const page_size_t>(start_ptrs, num_pointers);
}

primary_key_t BTreeNodeMap::getKeyForCell(page_size_t cell_offset) const {
  // Copy so we don't have to worry about alignment.
  return page_->Read<primary_key_t>(cell_offset);
}

primary_key_t BTreeNodeMap::getKeyForNthCell(page_size_t cell_index) const {
  auto&& pointers = getPointers();
  NOSQL_ASSERT(cell_index < pointers.size(), "cell number " << cell_index << " is out of range");
  return getKeyForCell(pointers[cell_index]);
}

std::variant<DataNodeCell, PointersNodeCell> BTreeNodeMap::getCell(page_size_t cell_offset) const {
  if (getHeader().IsPointersPage()) {
    return PointersNodeCell {
        getKeyForCell(cell_offset),
                             page_->Read<page_number_t>(cell_offset + sizeof(primary_key_t))};
  }
  return DataNodeCell {
      getKeyForCell(cell_offset),
                       page_->Read<entry_size_t>(cell_offset + sizeof(primary_key_t)),
                       page_->GetData() + cell_offset + sizeof(primary_key_t) + sizeof(entry_size_t)};
}

std::variant<DataNodeCell, PointersNodeCell> BTreeNodeMap::getNthCell(page_size_t cell_number) const {
  auto&& pointers = getPointers();
  NOSQL_ASSERT(cell_number < pointers.size(), "cell number " << cell_number << " is out of range");
  return getCell(pointers[cell_number]);
}

void BTreeNodeMap::sortKeys() {
  auto pointers = getPointers();

  std::vector<page_size_t> data{pointers.begin(), pointers.end()};

  std::ranges::sort(data,
                    [this](auto&& ptr1, auto&& ptr2) { return getKeyForCell(ptr1) < getKeyForCell(ptr2); });
  std::span<page_size_t> sorted_ptrs{data.data(), data.size()};
  GetPage().WriteToPage(getHeader().GetPointersStart(), sorted_ptrs);
}

}  // namespace neversql
