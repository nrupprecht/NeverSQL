//
// Created by Nathaniel Rupprecht on 2/18/24.
//

#pragma once

#include <memory>
#include <span>
#include <stack>
#include <variant>
#include <vector>

#include "NeverSQL/containers/FixedStack.h"
#include "NeverSQL/data/PageCache.h"
#include "NeverSQL/data/btree/BTreeNodeMap.h"
#include "NeverSQL/data/internals/KeyComparison.h"
#include "NeverSQL/utility/DataTypes.h"

namespace neversql {

//! \brief Structure used to represent a position in the B-tree.
//!
//! Represents the page, and the index of the cell in the page.
using TreePosition = FixedStack<std::pair<page_number_t, page_size_t>>;

//! \brief The result of doing a search in the B-tree.
//!
//! Includes the search path (in pages) that was taken, along with the node that was found.
struct SearchResult {
  TreePosition path;
  std::optional<BTreeNodeMap> node {};

  //! \brief Get how many layers had to be searched to find the node.
  std::size_t GetSearchDepth() const noexcept { return path.Size(); }
};

//! \brief Convenient structure for packing up data to store in a B-tree.
struct StoreData {
  //! \brief The key is some collection of bytes. It is context dependent how to compare different keys.
  GeneralKey key {};

  //! \brief The payload of the store operation.
  std::span<const std::byte> serialized_value {};

  //! \brief Whether to serialize the size of the key. If false, it is assumed that all keys have a fixed sizes
  //!        that is known by the B-tree manager.
  //!
  bool serialize_key_size = false;

  //! \brief Whether to serialize the size of the data.
  bool serialize_data_size = true;
};

//! \brief Structure that represents the result of splitting a page.
struct SplitPage {
  page_number_t left_page {};
  page_number_t right_page {};

  lightning::memory::MemoryBuffer<std::byte> split_key {};

  void SetKey(GeneralKey key) { split_key.Append(key); }
};

//! \brief An object that manages a B-tree structure for the NeverSQL database.
//!
//! Technically, a B+ tree, since all data is stored in leaf nodes.
class BTreeManager {
  friend class DataManager;

public:
  explicit BTreeManager(page_number_t root_page, PageCache& page_cache);

  //! \brief Set up a new B-tree, returning the root page.
  static std::unique_ptr<BTreeManager> CreateNewBTree(PageCache& page_cache, DataTypeEnum key_type);

  //! \brief Add a value with a specified key to the BTree.
  void AddValue(GeneralKey key, std::span<const std::byte> value);

  //! \brief Add a value with an auto-incrementing key to the B-tree.
  //!
  //! Only works if the B-tree is configured to generate auto-incrementing keys.
  //!
  //! \param value The value payload to add to the B-tree.
  void AddValue(std::span<const std::byte> value);

  //! \brief Get the root page number of the B-tree.
  page_number_t GetRootPageNumber() const noexcept { return root_page_; }

  class Iterator {
  public:
    using difference_type = std::ptrdiff_t;
    using value_type = std::span<const std::byte>;
    using pointer = value_type*;
    using reference = value_type&;
    using iterator_category = std::forward_iterator_tag;

    //! \brief Create a begin iterator for the B-tree.
    explicit Iterator(const BTreeManager& manager);

    //! \brief Create a specific B-tree iterator.
    Iterator(const BTreeManager& manager, FixedStack<std::pair<page_number_t, page_size_t>> progress);

    //! \brief Create an end B-Tree iterator
    Iterator(const BTreeManager& manager, [[maybe_unused]] bool);

    Iterator& operator++();
    std::span<const std::byte> operator*() const;
    bool operator==(const Iterator& other) const;
    bool operator!=(const Iterator& other) const;

    bool IsEnd() const noexcept { return done(); }

  private:
    //! \brief Check if the iterator is at the end.
    bool done() const noexcept;

    //! \brief Descend to the leftmost node in the tree given the page and pointer cell to start at.
    void descend(const BTreeNodeMap& page, page_size_t index);

    //! \brief Reference to the B-tree being traversed.
    const BTreeManager& manager_;

    //! \brief The current progress at every level of the tree.
    TreePosition progress_;
  };

  Iterator begin() const { return Iterator(*this); }
  Iterator end() const { return Iterator(*this, true); }

private:
  //! \brief Initialize the B-tree manager object from the data in its root page.
  void initialize();

  primary_key_t getNextPrimaryKey() const;

  BTreeNodeMap newNodePage(BTreePageType type, page_size_t reserved_space) const;

  std::optional<BTreeNodeMap> loadNodePage(page_number_t page_number) const;

  //! \brief Add an element to the node. Returns false if there is not enough space to add the element.
  //!
  //! \param node_map The node to which the data should be added.
  //! \param data The data to add to the node, and some information about how to represent the data.
  //! \param unique_keys Whether the keys in the node must be unique. This will generally be true.
  bool addElementToNode(BTreeNodeMap& node_map, const StoreData& data, bool unique_keys = true) const;

  //! \brief Split a node. This may, recursively, lead to more splits if the split causes the parent node to
  //! be full.
  void splitNode(BTreeNodeMap& node, SearchResult& result, std::optional<StoreData> data);

  //! \brief Split a single node, returning the key that was split on and the nodes.
  SplitPage splitSingleNode(BTreeNodeMap& node, std::optional<StoreData> data);

  //! \brief Special case for splitting the root node, which causes the height of the tree to increase by one.
  void splitRoot(std::optional<StoreData> data);

  //! \brief Vacuums the node, removing any fragmented space.
  void vacuum(BTreeNodeMap& node) const;

  //! \brief Look for the leaf node where a key should be inserted or can be found.
  SearchResult search(GeneralKey key) const;

  //! \brief Checks if the key is less than or equal to the other key.
  //!
  //! Uses the lt comparison provided, uses std::ranges::equal to check if the keys are equal.
  bool lte(GeneralKey key1, GeneralKey key2) const;

  //! \brief Debug function that returns a string representation of a key.
  //!
  //! Uses the debug_key_func_ if it is available, otherwise, string-ize the bytes.
  //!
  //! \param key The key to convert to a string.
  //! \return string representation of the key, implementation defined.
  std::string debugKey(GeneralKey key) const;

  // =================================================================================================
  // Private member variables.
  // =================================================================================================

  //! \brief Pointer to the NeverSQL database's data access layer.
  PageCache& page_cache_;

  //! \brief The page on which the B-tree index starts. Will be 0 if unassigned.
  //!
  page_number_t root_page_ {};

  //! \brief Whether the key's size needs to be serialized. TODO: Get this from the key type.
  bool serialize_key_size_ = false;

  //! \brief The type of the primary key used for this tree.
  DataTypeEnum key_type_ = DataTypeEnum::UInt64;

  //! \brief Function for comparing keys.
  CmpFunc cmp_;

  //! \brief Optionally, a function that can serialize a key to a string for debugging purposes.
  DebugKeyFunc debug_key_func_;

  //! \brief The maximum entry size, in bytes, before an overflow page is needed
  page_size_t max_entry_size_ = 256;

  //! \brief The maximum number of entries per page.
  //! NOTE(Nate): I am adding this for now to make testing easier.
  page_size_t max_entries_per_page_ = 10000;
};

}  // namespace neversql