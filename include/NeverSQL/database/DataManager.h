//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#pragma once

#include "NeverSQL/data/DataAccessLayer.h"
#include "NeverSQL/data/btree/BTree.h"
#include "NeverSQL/utility/HexDump.h"

namespace neversql {

//! \brief Structure that represents data on retrieving data from the data manager.
struct RetrievalResult {
  SearchResult search_result;
  page_size_t cell_offset {};
  std::span<const std::byte> value_view;

  bool IsFound() const noexcept { return search_result.node.has_value(); }
};

//! \brief Object that manages the data in the database, e.g. setting up B-trees and indices within the
//! database.
class DataManager {
public:
  explicit DataManager(const std::filesystem::path& database_path);

  //! \brief Add a value to the database.
  void AddValue(primary_key_t key, std::span<const std::byte> value);

  //! \brief Get a search result for a given key.
  SearchResult Search(primary_key_t key) const;

  RetrievalResult Retrieve(primary_key_t key) const;

  // ========================================
  // Debugging and Diagnostic Functions
  // ========================================

  bool HexDumpPage(page_number_t page_number, std::ostream& out, utility::HexDumpOptions options = {}) const;

  bool NodeDumpPage(page_number_t page_number, std::ostream& out) const;
private:
  //! \brief The data access layer for the database.
  DataAccessLayer data_access_layer_;

  BTreeManager primary_index_;
};

}  // namespace neversql