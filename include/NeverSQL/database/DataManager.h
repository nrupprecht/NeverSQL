//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#pragma once

#include "NeverSQL/data/DataAccessLayer.h"
#include "NeverSQL/data/btree/BTree.h"
#include "NeverSQL/utility/HexDump.h"

namespace neversql {

//! \brief Object that manages the data in the database, e.g. setting up B-trees and indices within the
//! database.
class DataManager {
public:
  explicit DataManager(const std::filesystem::path& database_path);

  //! \brief Add a value to the database.
  void AddValue(primary_key_t key, std::span<const std::byte> value);

  // ========================================
  // Debugging and Diagnostic Functions
  // ========================================

  bool HexDumpPage(page_number_t page_number, std::ostream& out, utility::HexDumpOptions options = {}) const;

private:
  //! \brief The data access layer for the database.
  DataAccessLayer data_access_layer_;

  BTreeManager primary_index_;
};

}  // namespace neversql