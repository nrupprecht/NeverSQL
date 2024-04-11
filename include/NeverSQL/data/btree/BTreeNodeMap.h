//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#pragma once

#include <span>

#include "EntryCreator.h"
#include "NeverSQL/data/Page.h"
#include "NeverSQL/data/btree/BTreePageHeader.h"
#include "NeverSQL/data/internals/DatabaseEntry.h"
#include "NeverSQL/data/internals/KeyPrinting.h"

namespace neversql {

//! \brief A comparison function knows how to interpret and compare keys, represented as spans of bytes.
using CmpFunc = std::function<bool(std::span<const std::byte>, std::span<const std::byte>)>;

//! \brief A function that can serialize a key to a string for debugging purposes.
using DebugKeyFunc = std::function<std::string(std::span<const std::byte>)>;

//! \brief A general key, represented as a span of bytes. How these bytes are interpreted and compares is
//! determined by the comparison function.
using GeneralKey = std::span<const std::byte>;

//! \brief Helper structure that represents a cell in a leaf node.
struct DataNodeCell {
  std::byte flags;
  const std::span<const std::byte> key;
  std::span<const std::byte> data;

  // =================================================================================================
  // Helper functions.
  // =================================================================================================

  //! \brief Get a span of the value in the cell.
  NO_DISCARD std::span<const std::byte> SpanValue() const noexcept { return data; }

  NO_DISCARD page_size_t GetCellSize() const noexcept {
    return static_cast<page_size_t>(
        // Flags.
        sizeof(std::byte)
        // Potentially, key size.
        + (internal::GetKeySizeIsSerialized(flags) ? 2 : 0)
        // The key.
        + key.size()
        // Potentially, the entry size.
        + (internal::GetIsEntrySizeSerialized(flags) ? sizeof(entry_size_t) : 0)
        // The entry.
        + data.size());
  }

  NO_DISCARD page_size_t GetDataSize() const noexcept { return static_cast<page_size_t>(data.size()); }
};

//! \brief Helper structure that represents a cell in an internal node.
struct PointersNodeCell {
  std::byte flags;
  const std::span<const std::byte> key;

  //! \brief The pointer value.
  //!
  //! \note The data of a pointers node cell (the entry) is just the page number.
  const page_number_t page_number;

  NO_DISCARD page_size_t GetCellSize() const noexcept {
    return static_cast<page_size_t>(
        // Flags.
        sizeof(std::byte)
        // Potentially, key size.
        + (internal::GetKeySizeIsSerialized(flags) ? 2 : 0)
        // The key.
        + key.size()
        // The page number (pointer value).
        + sizeof(page_number_t));
  }

  NO_DISCARD static page_size_t GetDataSize() noexcept { return sizeof(page_number_t); }
};

struct SpaceRequirement {
  //! \brief The amount of space that the pointer needs.
  page_size_t pointer_space;

  //! \brief The amount of space that the cell header needs.
  page_size_t cell_header_space;

  //! \brief The maximum amount of space that would be available for storing an entry.
  page_size_t max_entry_space;
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
  NO_DISCARD const std::unique_ptr<Page>& GetPage() const;
  NO_DISCARD std::unique_ptr<Page>& GetPage();

  //! \brief Get the number of pointers (and therefore cells) in the node.
  NO_DISCARD page_size_t GetNumPointers() const;

  //! \brief Get the amount of "de-fragmented" free space, that is, the amount of free space in between free
  //! space start and free space end.
  //!
  //! There may be more "fragmented" free space due to cells being erased, this is not counted here.
  //! \return The amount of free space in the node, in the free space section.
  NO_DISCARD page_size_t GetDefragmentedFreeSpace() const;

  //! \brief Calculate the space requirements for adding a new entry to a node, given the key.
  //!
  //! This calculates the amount of space needed for the pointer, the cell, and the maximum amount of space
  //! available for the entry.
  NO_DISCARD SpaceRequirement CalculateSpaceRequirements(GeneralKey key) const;

  //! \brief Get the largest key of any element in the node. If there are no keys, returns nullopt.
  NO_DISCARD std::optional<GeneralKey> GetLargestKey() const;

  //! \brief Check whether this page is a pointers page, that is, whether it only stores pointers to other
  //! pages instead of storing data.
  NO_DISCARD bool IsPointersPage() const noexcept;

  //! \brief Check whether this page is the root page.
  NO_DISCARD bool IsRootPage() const noexcept;

private:
  //! \brief Create a new BTreeNodeMap wrapping a page. No checks are done to see if the page is a valid
  //! B-tree node.
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
  std::optional<page_size_t> getCellByKey(GeneralKey key) const;

  //! \brief Get an offset to the start of the first entry whose primary key is greater than or equal to the
  //! given key.
  //!
  //! If the key cannot be found, returns std::nullopt. The the offsets are assumed to be ordered by the
  //! primary key of the cell that they refer to.
  //!
  //! \param key The primary key to search for.
  //! \return The offset to the start of the cell and index of the cell in the page (index of the pointers
  //! cell), or std::nullopt if there are no keys greater than or equal to the given key.
  std::optional<std::pair<page_size_t, page_index_t>> getCellLowerBoundByPK(GeneralKey key) const;

  //! \brief If this is a pointers page, get the next page to search on, returning the page number and the
  //! index of the pointer to the next page in the current page. If the page is the rightmost page, returns
  //! the number of pointers as the index.
  //! If this is not a pointers page, raises and error.
  std::pair<page_number_t, page_index_t> searchForNextPageInPointersPage(GeneralKey key) const;

  //! \brief Get a span of the offsets in the node.
  std::span<const page_size_t> getPointers() const;

  //! \brief Get the offset to the start of the free space in the node.
  page_size_t getCellOffsetByIndex(page_size_t cell_index) const;

  //! \brief Get the primary key from a cell, given the cell offset.
  GeneralKey getKeyForCell(page_size_t cell_offset) const;

  //! \brief Get the primary key from a cell, given the cell's index.
  GeneralKey getKeyForNthCell(page_size_t cell_index) const;

  //! \brief Get the cell at the given offset, as a structure. If the node is a leaf node, LeafNodeCell is
  //! returned. If the node is an interior node, InteriorNodeCell is returned.
  std::variant<DataNodeCell, PointersNodeCell> getCell(page_size_t cell_offset) const;

  //! \brief Get the N-th cell in the node, as a structure. If the node is a leaf node, LeafNodeCell is
  //! returned. If the node is an interior node, InteriorNodeCell is returned.
  std::variant<DataNodeCell, PointersNodeCell> getNthCell(page_size_t cell_number) const;

  //! \brief Sort the keys in the node by the primary key they refer to.
  void sortKeys();

  //! \brief Debug function that returns a string representation of a key.
  //!
  //! Uses the debug_key_func_ if it is available, otherwise, string-ize the bytes.
  //!
  //! \param key
  //! \return string representation of the key, implementation defined.
  std::string debugKey(GeneralKey key) const;

  //! \brief The underlying page, that this class interprets as a B-tree node.
  std::unique_ptr<Page> page_;

  //! \brief The key comparison function.
  CmpFunc cmp_;

  //! \brief Optionally, a function that can serialize a key to a string for debugging purposes.
  DebugKeyFunc debug_key_func_;
};

}  // namespace neversql