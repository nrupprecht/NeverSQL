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

bool BTreeNodeMap::IsLeaf() const {
  return GetType() == BTreePageType::Leaf;
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
  auto it = std::ranges::lower_bound(pointers, key, [this](decltype(key) k, auto&& ptr) {
    return k < getKeyForCell(static_cast<page_size_t>(ptr));
  });
  if (it == pointers.end()) {
    return {};
  }
  return *it;
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

std::variant<LeafNodeCell, InteriorNodeCell> BTreeNodeMap::getCell(page_size_t cell_offset) const {
  auto&& header = GetHeader();
  if (header.GetPageType() == BTreePageType::Leaf) {
    return LeafNodeCell {
        getKeyForCell(cell_offset),
        page_.CopyAs<entry_size_t>(cell_offset + sizeof(primary_key_t)),
        //*reinterpret_cast<const entry_size_t*>(page_.GetPtr(cell_offset + sizeof(primary_key_t))),
        page_.GetPtr(cell_offset + sizeof(primary_key_t) + sizeof(entry_size_t))};
  }
  return InteriorNodeCell {
      getKeyForCell(cell_offset),
      *reinterpret_cast<const page_number_t*>(page_.GetPtr(cell_offset + sizeof(primary_key_t)))};
}

void BTreeNodeMap::sortKeys() {
  auto pointers = getPointers();
  std::ranges::sort(pointers,
                    [this](auto&& ptr1, auto&& ptr2) { return getKeyForCell(ptr1) < getKeyForCell(ptr2); });
}

}  // namespace neversql
