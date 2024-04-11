//
// Created by Nathaniel Rupprecht on 3/26/24.
//

#pragma once

#include "NeverSQL/data/Page.h"

namespace neversql {
class BTreeManager;
class Document;
}

namespace neversql::internal {

//! \brief An object that allow for access of the data payload of an entry in a B-tree, abstracting away the
//!        exact layout of the data (e.g. whether it is stored in a leaf node or in an overflow page).
class DatabaseEntry {
public:
  //! \brief Get the entry data in the current focus of the entry.
  virtual std::span<const std::byte> GetData() const noexcept = 0;

  //! \brief Go to the next part of the database entry. Returns true if there was another entry to go to.
  virtual bool Advance() = 0;

  virtual ~DatabaseEntry() = default;

  //! \brief Do a check of whether the entry is valid.
  virtual bool IsValid() const = 0;
};

//! \brief Read an entry, starting with the given offset in the page.
std::unique_ptr<DatabaseEntry> ReadEntry(page_size_t starting_offset,
                                         std::unique_ptr<const Page>&& page,
                                         const BTreeManager* btree_manager);

//! \brief Convert a database entry to a document.
std::unique_ptr<Document> EntryToDocument(DatabaseEntry& entry);

}  // namespace neversql::internal