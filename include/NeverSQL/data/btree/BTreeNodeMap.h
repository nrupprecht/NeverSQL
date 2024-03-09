//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#pragma once

#include <span>

#include "NeverSQL/data/Page.h"
#include "NeverSQL/data/btree/BTreePageHeader.h"

namespace neversql {

//! \brief Helper structure that represents a cell in a leaf node.
struct DataNodeCell {
  const primary_key_t key;
  const entry_size_t size_of_entry;
  const std::byte* start_of_value;

  // =================================================================================================
  // Helper functions.
  // =================================================================================================

  //! \brief Get a span of the value in the cell.
  NO_DISCARD std::span<const std::byte> SpanValue() const noexcept {
    return std::span<const std::byte>(start_of_value, size_of_entry);
  }

  NO_DISCARD page_size_t GetSize() const noexcept {
    return static_cast<page_size_t>(sizeof(primary_key_t) + sizeof(entry_size_t) + size_of_entry);
  }
};

//! \brief Helper structure that represents a cell in an internal node.
struct PointersNodeCell {
  const primary_key_t key;
  const page_number_t page_number;

  NO_DISCARD page_size_t GetSize() const noexcept { return sizeof(primary_key_t) + sizeof(page_number_t); }
};


namespace utility {
class PageInspector;
}  // namespace utility

//! \brief Node object that can "memory map" a B-tree node onto a page.
class BTreeNodeMap {
  friend class BTreeManager;

  friend class DataManager;

  friend class utility::PageInspector;

public:
  //! \brief Get the header of the page.
  NO_DISCARD BTreePageHeader GetHeader();

  //! \brief Get the type of this node.
  NO_DISCARD BTreePageType GetType() const;

  //! \brief Get the page number of the page this node is written in.
  page_number_t GetPageNumber() const;

  //! \brief Get the size of a page.
  page_size_t GetPageSize() const;

  //! \brief Get the underlying page.
  NO_DISCARD const Page& GetPage() const;
  NO_DISCARD Page& GetPage();

  //! \brief Get the number of pointers (and therefore cells) in the node.
  NO_DISCARD page_size_t GetNumPointers() const;

  //! \brief Get the amount of "de-fragmented" free space, that is, the amount of free space in between free space start
  //! and free space end.
  //!
  //! There may be more "fragmented" free space due to cells being erased, this is not counted here.
  //! \return The amount of free space in the node, in the free space section.
  NO_DISCARD page_size_t GetDefragmentedFreeSpace() const;

  //! \brief Get the largest key of any element in the node. If there are no keys, returns nullopt.
  NO_DISCARD std::optional<primary_key_t> GetLargestKey() const;

  //! \brief Check whether this page is a pointers page, that is, whether it only stores pointers to other
  //! pages instead of storing data.
  NO_DISCARD bool IsPointersPage() const noexcept;

  //! \brief Check whether this page is the root page.
  NO_DISCARD bool IsRootPage() const noexcept;

private:
  //! \brief Create a new BTreeNodeMap wrapping a page. No checks are done to see if the page is a valid B-tree node.
  explicit BTreeNodeMap(std::unique_ptr<Page>&& page) noexcept;

  //! \brief Get the header of the page.
  NO_DISCARD BTreePageHeader getHeader() const;

  //! \brief Get an offset to the start of an entry, indicated by its primary key, in a leaf node.
  //!
  //! If the key cannot be found, returns std::nullopt. The the offsets are assumed to be ordered by the
  //! primary key of the cell that they refer to.
  //!
  //! \param key The primary key to search for.
  //! \return The offset to the start of the cell, or std::nullopt if the key cannot be found.
  std::optional<page_size_t> getCellByPK(primary_key_t key) const;

  //! \brief Get an offset to the start of the first entry whose primary key is greater than or equal to the given key.
  //!
  //! If the key cannot be found, returns std::nullopt. The the offsets are assumed to be ordered by the
  //! primary key of the cell that they refer to.
  //!
  //! \param key The primary key to search for.
  //! \return The offset to the start of the cell, or std::nullopt if there are no keys greater than or equal to the given key.
  std::optional<page_size_t> getCellLowerBoundByPK(primary_key_t) const;

  //! \brief If this is a pointers page, get the next page to search on. If this is not a pointers page,
  //! raises and error.
  page_number_t searchForNextPageInPointersPage(primary_key_t key) const;

  //! \brief Get a span of the offsets in the node.
  std::span<const page_size_t> getPointers() const;

  //! \brief Get the primary key from a cell, given the cell offset.
  primary_key_t getKeyForCell(page_size_t cell_offset) const;

  //! \brief Get the primary key from a cell, given the cell's index.
  primary_key_t getKeyForNthCell(page_size_t cell_index) const;

  //! \brief Get the cell at the given offset, as a structure. If the node is a leaf node, LeafNodeCell is
  //! returned. If the node is an interior node, InteriorNodeCell is returned.
  std::variant<DataNodeCell, PointersNodeCell> getCell(page_size_t cell_offset) const;

  //! \brief Get the N-th cell in the node, as a structure. If the node is a leaf node, LeafNodeCell is
  //! returned. If the node is an interior node, InteriorNodeCell is returned.
  std::variant<DataNodeCell, PointersNodeCell> getNthCell(page_size_t cell_number) const;

  //! \brief Sort the keys in the node by the primary key they refer to.
  void sortKeys();

  //! \brief The underlying page, that this class interprets as a B-tree node.
  std::unique_ptr<Page> page_;
};

}  // namespace neversql