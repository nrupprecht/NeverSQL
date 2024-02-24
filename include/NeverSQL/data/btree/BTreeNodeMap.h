//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#pragma once

#include <span>

#include "NeverSQL/data/Page.h"
#include "NeverSQL/data/btree/BTreePageHeader.h"

namespace neversql {

//! \brief Helper structure that represents a cell in a leaf node.
struct LeafNodeCell {
  const primary_key_t key;
  const entry_size_t size_of_entry;
  const std::byte* start_of_value;

  // =================================================================================================
  // Helper functions.
  // =================================================================================================

  //! \brief Get a span of the value in the cell.
  std::span<const std::byte> SpanValue() const {
    return std::span<const std::byte>(start_of_value, size_of_entry);
  }
};

//! \brief Helper structure that represents a cell in an internal node.
struct InteriorNodeCell {
  const primary_key_t key;
  const page_number_t page_number;
};

//! \brief Node object that can "memory map" a B-tree node onto a page.
class BTreeNodeMap {
  friend class BTreeManager;

public:
  //! \brief Get the header of the page.
  NO_DISCARD BTreePageHeader& GetHeader();

    //! \brief Get the header of the page.
  NO_DISCARD const BTreePageHeader& GetHeader() const;

  //! \brief Get the type of this node.
  NO_DISCARD BTreePageType GetType() const;

  //! \brief Get the page number of the page this node is written in.
  page_number_t GetPageNumber() const;

  //! \brief Get the underlying page.
  NO_DISCARD const Page& GetPage() const;

  //! \brief Get the number of pointers (and therefore cells) in the node.
  NO_DISCARD page_size_t GetNumPointers() const;

  NO_DISCARD page_size_t GetDefragmentedFreeSpace() const;

  //! \brief Get the largest key of any element in the node. If there are no keys, returns nullopt.
  NO_DISCARD std::optional<primary_key_t> GetLargestKey() const;

  //! \brief Returns whether this is a leaf node.
  NO_DISCARD bool IsLeaf() const;

private:
  //! \brief Create a new BTreeNodeMap wrapping a page. No checks are done to see if the page is a valid B-tree node.
  explicit BTreeNodeMap(Page&& page) noexcept;

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

  //! \brief Get a span of the offsets in the node.
  std::span<page_size_t> getPointers();

  //! \brief Get a span of the offsets in the node.
  std::span<const page_size_t> getPointers() const;

  //! \brief Get the primary key from a cell, given the cell offset.
  primary_key_t getKeyForCell(page_size_t cell_offset) const;

  //! \brief Get the cell at the given offset, as a structure. If the node is a leaf node, LeafNodeCell is
  //! returned. If the node is an interior node, InteriorNodeCell is returned.
  std::variant<LeafNodeCell, InteriorNodeCell> getCell(page_size_t cell_offset) const;

  //! \brief Sort the keys in the node by the primary key they refer to.
  void sortKeys();

  //! \brief The underlying page, that this class interprets as a B-tree node.
  Page page_;
};

}  // namespace neversql