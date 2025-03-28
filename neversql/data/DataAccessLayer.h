//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#pragma once

#include <filesystem>
#include <set>
#include <shared_mutex>
#include <utility>

#include "neversql/data/FreeList.h"
#include "neversql/data/Page.h"
#include "neversql/data/internals/Meta.h"

namespace neversql {

//! \brief Manager to read and write pages from persistent memory.
//!
//! The DAL is responsible for managing the pages in the database file. It keeps track of structure of the
//! file (e.g. which pages are free, database information, etc.), and provides an interface to read and write
//! pages. Actual applications are responsible for interpreting the data in the pages, and
//! serializing/deserializing data to and from the pages.
class DataAccessLayer {
  friend class DataManager;

public:
  //! \brief Create or open a database file located at the given path.
  explicit DataAccessLayer(std::filesystem::path db_path);

  //! \brief Destructor, makes sure data is written back to file.
  ~DataAccessLayer();

  //! \brief Check whether the DAL is initialized.
  NO_DISCARD bool IsInitialized() const;

  //! \brief Get a new page from the DAL and read its meta data into the provided Page object.
  void GetNewPage(Page& page);

  //! \brief Write a page back to the DAL.
  void WriteBackPage(const Page& page) const;

  //! \brief Release a page back to the DAL.
  void ReleasePage(const Page& page);

  //! \brief Get the number of pages in the DAL.
  NO_DISCARD page_number_t GetNumPages() const;

  //! \brief Get the size of a page in the DAL.
  NO_DISCARD page_size_t GetPageSize() const;

  //! \brief Get a page from the DAL, if the page exists and is valid (not freed). Writes the page's
  //!        information into the provided Page object.
  void GetPage(page_number_t page_number, Page& page) const;

  //! \brief Get the meta data.
  const Meta& GetMeta() const { return meta_; }

private:
  //! \brief Get the number of allocated pages.
  NO_DISCARD uint64_t getNumAllocatedPages() const;

  //! \brief Get a page from the DAL. If safe_mode is true, we will check if the page is valid (not freed).
  //!        Otherwise, we will return the page regardless of its status.
  //!
  //! Unsafe mode is useful when loading the meta page, since the free list is not yet initialized.
  //!
  //! \param page_number
  //! \param safe_mode
  void getPage(page_number_t page_number, Page& page, bool safe_mode = true) const;

  //! \brief Get a new page. This uses the free-list. First, it will try to find an available freed page. If
  //! there are none, it will assign a new page number. In this case, we will need to allocate actual file
  //! space for the new page.
  NO_DISCARD page_number_t getNewPage();

  //! \brief Release a page back to the free list, "deleting" its contents.
  void releasePage(page_number_t page_number);

  //! \brief Write a page back to the file. The page must have come from the database file to begin with.
  void writePage(const Page& page) const;

  //! \brief Read a page from memory into the page structure.
  void readPage(Page& page, bool safe_mode = true) const;

  //! \brief Initialize a new database file.
  void createDB();

  //! \brief Open a database file to set up the DataAccessLayer object.
  void openDB();

  //! \brief Read or create the DAL.
  void initialize();

  //! \brief Update the meta page.
  void updateMeta() const;

  //! \brief Set the index page in the meta.
  void setIndexPage(page_number_t index_page);

  //! \brief Update the free list page.
  void updateFreeList() const;

  //! \brief Serialize a free list to a page.
  static void serialize(Page& page, const FreeList& free_list);
  //! \brief Deserialize a free list from a page.
  static void deserialize(const Page& page, FreeList& free_list);

  //! \brief Serialize the meta to a page.
  static void serialize(Page& page, const Meta& meta);
  //! \brief Deserialize the meta from a page.
  static void deserialize(const Page& page, Meta& meta);

  //! \brief Get an output file stream that does not erase the file, but allows for writing.
  NO_DISCARD static std::ofstream getOutputFileStream(const std::filesystem::path& file_path);

  // =================================================================================================
  // Data Members
  // =================================================================================================

  //! \brief A read/write lock, for synchronizing access to the file.
  mutable std::shared_mutex read_write_lock_;

  //! \brief The file path to the database directory.
  std::filesystem::path db_path_ {};

  //! \brief The file path to the database file.
  std::filesystem::path file_path_ {};

  //! \brief The meta page for the database (in-memory representation).
  Meta meta_ {12};

  //! \brief The free list, to keep track of the pages in the database.
  FreeList free_list_;

  //! \brief Set of pages that are reserved for the DAL. Other applications cannot access these pages.
  //  TODO(Nate): This needs to be stored somewhere in persistent memory too.
  std::set<page_number_t> reserved_pages_;
};

}  // namespace neversql
