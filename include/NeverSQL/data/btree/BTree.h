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

namespace neversql {

//! \brief The result of doing a search in the B-tree.
//!
//! Includes the search path (in pages) that was taken, along with the node that was found.
struct SearchResult {
  FixedStack<page_number_t> path;
  std::optional<BTreeNodeMap> node {};
};

//! \brief Convenient structure for packing up data to store in a B-tree.
struct StoreData {
  primary_key_t key;
  std::span<const std::byte> serialized_value;

  bool serialize_data_size = true;
};

//! \brief Structure that represents the result of splitting a page.
struct SplitPage {
  page_number_t left_page {};
  page_number_t right_page {};
  primary_key_t split_key {};
};

//! \brief An object that manages a B-tree structure for the NeverSQL database.
//!
//! Technically, a B+ tree, since all data is stored in leaf nodes.
class BTreeManager {
  friend class DataManager;

public:
  explicit BTreeManager(PageCache* page_cache) noexcept
      : page_cache_(page_cache) {}

  //! \brief Add a value with a specified key to the BTree.
  void AddValue(primary_key_t key, std::span<const std::byte> value);

  //! \brief Add a value with an auto-incrementing key to the B-tree.
  void AddValue(std::span<const std::byte> value);

private:
  primary_key_t getNextPrimaryKey() const;

  BTreeNodeMap newNodePage(BTreePageType type, page_size_t reserved_space) const;

  std::optional<BTreeNodeMap> loadNodePage(page_number_t page_number) const;

  //! \brief Add an element to the node. Returns false if there is not enough space to add the element.
  //!
  //! TODO: Better way to add value?
  //! TODO: Keys that aren't primary_key_t.
  //!
  //! \param node_map
  //! \param key
  //! \param serialized_value
  //! \param store_size Whether the size of the serialized value needs to be stored in the node. This will be
  //!     true for leaf nodes, but false for internal nodes.
  //! \param unique_keys Whether the keys in the node must be unique. This will generally be true.
  bool addElementToNode(BTreeNodeMap& node_map, const StoreData& data, bool unique_keys = true) const;

  //! \brief Adaptor function that allows adding a trivially copyable type to the node by creating a span of
  //! the value and calling the other addElementToNode function.
  template<typename T>
    requires std::is_trivially_copyable_v<T>
  bool addElementToNode(BTreeNodeMap& node_map,
                        primary_key_t key,
                        const T& value,
                        bool store_size = true,
                        bool unique_keys = true) const {
    if constexpr (std::is_same_v<T, std::span<const std::byte>>) {
      return addElementToNode(
          node_map,
          StoreData {.key = key, .serialized_value = value, .serialize_data_size = store_size},
          unique_keys);
    }
    else {
      auto data_span = std::span<const std::byte>(reinterpret_cast<const std::byte*>(&value), sizeof(value));
      return addElementToNode(
          node_map,
          StoreData {.key = key, .serialized_value = data_span, .serialize_data_size = store_size},
          unique_keys);
    }
  }

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
  SearchResult search(primary_key_t key) const;

  // =================================================================================================
  // Private member variables.
  // =================================================================================================

  //! \brief Pointer to the NeverSQL database's data access layer.
  PageCache* page_cache_ {};

  //! \brief The page on which the B-tree index starts. Will be 0 if unassigned.
  //!
  //! This information comes from the DAL.
  page_number_t index_page_ {};

  //! \brief The maximum entry size, in bytes, before an overflow page is needed
  page_size_t max_entry_size_ = 256;

  //! \brief The maximum number of entries per page.
  //! NOTE(Nate): I am adding this for now to make testing easier.
  page_size_t max_entries_per_page_ = 10000;
};

}  // namespace neversql