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
#include "NeverSQL/data/DataAccessLayer.h"
#include "NeverSQL/data/btree/BTreeNodeMap.h"

namespace neversql {

//! \brief The result of doing a search in the B-tree.
//!
//! Includes the search path (in pages) that was taken, along with the node that was found.
struct SearchResult {
  FixedStack<page_number_t> path;
  std::optional<BTreeNodeMap> node {};
};

//! \brief An object that manages a B-tree structure for the NeverSQL database.
//!
//! Technically, a B+ tree, since all data is stored in leaf nodes.
class BTreeManager {
public:
  explicit BTreeManager(DataAccessLayer* data_access_layer) noexcept
      : data_access_layer_(data_access_layer) {}

  // private:
  BTreeNodeMap newNodePage(BTreePageType type) const;

  std::optional<BTreeNodeMap> loadNodePage(page_number_t page_number) const;

  //! \brief Add an element to the node. Returns false if there is not enough space to add the element.
  //!
  //! TODO: Better way to add value?
  //! TODO: Keys that aren't primary_key_t.
  //! \param node_map
  //! \param key
  //! \param serialized_value
  bool addElementToNode(BTreeNodeMap& node_map,
                        primary_key_t key,
                        std::span<const std::byte> serialized_value);

  //! \brief Write the node back to the file.
  void writeBack(const BTreeNodeMap& node_map) const;

  //! \brief Look for a place where a key should be inserted or can be found.
  SearchResult search(primary_key_t key) const;

  // =================================================================================================
  // Private member variables.
  // =================================================================================================

  //! \brief Pointer to the NeverSQL database's data access layer.
  DataAccessLayer* data_access_layer_;

  //! \brief The page on which the B-tree index starts. Will be 0 if unassigned.
  //!
  //! This information comes from the DAL.
  page_number_t index_page_ {};
};

}  // namespace neversql