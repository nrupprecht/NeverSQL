//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#pragma once

#include "neversql/data/Document.h"
#include "neversql/data/PageCache.h"
#include "neversql/data/btree/BTree.h"
#include "neversql/utility/HexDump.h"

namespace neversql {

struct CollectionInfo {
  std::string collection_name;
  DataTypeEnum key_type;
};

//! \brief Object that manages the data in the database, e.g. setting up B-trees and indices within the
//!        database.
class DataManager {
public:
  explicit DataManager(const std::filesystem::path& database_path);

  //! \brief Add a collection to the database.
  void AddCollection(const std::string& collection_name, DataTypeEnum key_type);

  void AddCollection(const CollectionInfo& info);

  // ========================================
  //  General key methods
  // ========================================

  // void AddValue(const std::string& collection_name, GeneralKey key, std::span<const std::byte> value);

  void AddValue(const std::string& collection_name, GeneralKey key, const Document& document);

  //! \brief Get a search result for a given key.
  SearchResult Search(const std::string& collection_name, GeneralKey key) const;

  //! \brief Retrieve a value from the database along with data about the retrieval.
  RetrievalResult Retrieve(const std::string& collection_name, GeneralKey key) const;

  // ========================================
  //  Primary key methods
  // ========================================

  //! \brief Add a value to the database.
  // void AddValue(const std::string& collection_name, primary_key_t key, std::span<const std::byte> value);

  //! \brief Add a value to the database using an auto incrementing key.
  // void AddValue(const std::string& collection_name, std::span<const std::byte> value);

  //! \brief Add a document to the database.
  void AddValue(const std::string& collection_name, const Document& document);

  //! \brief Get a search result for a given key.
  SearchResult Search(const std::string& collection_name, primary_key_t key) const;

  //! \brief Retrieve a value from the database along with data about the retrieval.
  RetrievalResult Retrieve(const std::string& collection_name, primary_key_t key) const;

  //! \brief Get the names of all collections.
  std::set<std::string> GetCollectionNames() const;

  // ========================================
  // FOR NOW: Test search and iteration methods.
  // ========================================

  BTreeManager::Iterator Begin(const std::string& collection_name) const;
  BTreeManager::Iterator End(const std::string& collection_name) const;

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

  //! \brief The page cache for the database.
  mutable PageCache page_cache_;

  //! \brief Cache the collection index.
  std::unique_ptr<BTreeManager> collection_index_ {};

  //! \brief Cache the collections that are in the database.
  std::map<std::string, std::unique_ptr<BTreeManager>> collections_;
};

}  // namespace neversql