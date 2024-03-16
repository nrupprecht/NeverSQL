//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#pragma once

#include "NeverSQL/data/PageCache.h"
#include "NeverSQL/data/btree/BTree.h"
#include "NeverSQL/data/Document.h"
#include "NeverSQL/utility/HexDump.h"

namespace neversql {

//! \brief Structure that represents data on retrieving data from the data manager.
struct RetrievalResult {
  SearchResult search_result;
  page_size_t cell_offset {};
  std::span<const std::byte> value_view {};

  bool IsFound() const noexcept { return search_result.node.has_value(); }
};

//! \brief Object that manages the data in the database, e.g. setting up B-trees and indices within the
//! database.
class DataManager {
public:
  explicit DataManager(const std::filesystem::path& database_path);

  //! \brief Add a collection to the database.
  void AddCollection(const std::string& collection_name, DataTypeEnum key_type);

  // ========================================
  //  General key methods
  // ========================================

  void AddValue(const std::string& collection_name, GeneralKey key, std::span<const std::byte> value);

  void AddValue(const std::string& collection_name, GeneralKey key, const DocumentBuilder& document);

  //! \brief Get a search result for a given key.
  SearchResult Search(const std::string& collection_name, GeneralKey key) const;

  //! \brief Retrieve a value from the database along with data about the retrieval.
  RetrievalResult Retrieve(const std::string& collection_name, GeneralKey key) const;

  // ========================================
  //  Primary key methods
  // ========================================

  //! \brief Add a value to the database.
  void AddValue(const std::string& collection_name, primary_key_t key, std::span<const std::byte> value);

  //! \brief Add a value to the database using an auto incrementing key.
  void AddValue(const std::string& collection_name, std::span<const std::byte> value);

  //! \brief Add a document to the database.
  void AddValue(const std::string& collection_name, const DocumentBuilder& document);

  //! \brief Get a search result for a given key.
  SearchResult Search(const std::string& collection_name, primary_key_t key) const;

  //! \brief Retrieve a value from the database along with data about the retrieval.
  RetrievalResult Retrieve(const std::string& collection_name, primary_key_t key) const;

  // ========================================
  // Debugging and Diagnostic Functions
  // ========================================

  //! \brief Hex dump a page to the given output stream.
  bool HexDumpPage(page_number_t page_number, std::ostream& out, utility::HexDumpOptions options = {}) const;

  //! \brief Dump the contents of a node page to the given output stream, getting the B-tree node information.
  bool NodeDumpPage(page_number_t page_number, std::ostream& out) const;

  //! \brief Get the data access layer, so it can be queried for information.
  const DataAccessLayer& GetDataAccessLayer() const { return data_access_layer_; }

private:
  //! \brief The data access layer for the database.
  DataAccessLayer data_access_layer_;

  mutable PageCache page_cache_;

  std::unique_ptr<BTreeManager> collection_index_;

  //! \brief Cache the collections that are in the database.
  std::map<std::string, std::unique_ptr<BTreeManager>> collections_;
};

}  // namespace neversql