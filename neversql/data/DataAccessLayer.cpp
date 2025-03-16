//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#include "neversql/data/DataAccessLayer.h"
// Other files.
#include "neversql/utility/HexDump.h"

namespace neversql {

template<typename T>
// requires std::is_trivially_copyable_v<T>
void write(char*& buffer, const T& data) {
  std::memcpy(buffer, reinterpret_cast<const char*>(&data), sizeof(data));
  buffer += sizeof(data);
}

template<typename T>
// requires std::is_trivially_copyable_v<T>
void read(const char*& buffer, T& data) {
  std::memcpy(reinterpret_cast<char*>(&data), buffer, sizeof(data));
  buffer += sizeof(data);
}

// }  // namespace

// =================================================================================================
// DataAccessLayer
// =================================================================================================

DataAccessLayer::DataAccessLayer(std::filesystem::path db_path)
    : db_path_(db_path)
    , file_path_(db_path / "neversql.db") {
  initialize();
}

DataAccessLayer::~DataAccessLayer() {
  try {
    updateMeta();
  } catch (...) {
    LOG_SEV(Error) << "Error updating meta.";
  }
  try {
    updateFreeList();
  } catch (...) {
    LOG_SEV(Error) << "Error updating free list.";
  }
}

bool DataAccessLayer::IsInitialized() const {
  return !file_path_.empty();
}

void DataAccessLayer::GetNewPage(Page& page) {
  // Allocate a page and return the page number.
  page.SetPageNumber(getNewPage());
  page.page_size_ = GetPageSize();
}

void DataAccessLayer::WriteBackPage(const Page& page) const {
  std::unique_lock guard(read_write_lock_);
  // Load the page from the file.
  auto fout = getOutputFileStream(file_path_);
  fout.seekp(static_cast<std::streamoff>(page.GetPageNumber() * GetPageSize()));
  fout.write(page.GetChars(), static_cast<std::streamsize>(page.GetPageSize()));
}

void DataAccessLayer::ReleasePage(const Page& page) {
  releasePage(page.GetPageNumber());
}

page_number_t DataAccessLayer::GetNumPages() const {
  return getNumAllocatedPages();
}

page_size_t DataAccessLayer::GetPageSize() const {
  return meta_.GetPageSize();
}

void DataAccessLayer::GetPage(page_number_t page_number, Page& page) const {
  return getPage(page_number, page, true);
}

uint64_t DataAccessLayer::getNumAllocatedPages() const {
  return free_list_.GetNumAllocatedPages();
}

void DataAccessLayer::getPage(page_number_t page_number, Page& page, bool safe_mode) const {
  page.SetPageNumber(page_number);
  page.page_size_ = GetPageSize();
  readPage(page, safe_mode);
}

page_number_t DataAccessLayer::getNewPage() {
  std::unique_lock guard(read_write_lock_);

  // Note: since this free list can allocate new pages, the return will never be a nullopt.
  auto page_number = *free_list_.GetNextPage();
  if (page_number == getNumAllocatedPages() - 1) {
    auto file_size = GetPageSize() * (page_number + 1);
    std::filesystem::resize_file(file_path_, file_size);
    LOG_SEV(Debug) << "Getting new page (" << page_number << "), resizing file " << file_path_ << " to size "
                   << file_size << ".";
  }
  return page_number;
}

void DataAccessLayer::releasePage(page_number_t page_number) {
  std::unique_lock guard(read_write_lock_);
  if (reserved_pages_.contains(page_number)) {
    reserved_pages_.erase(page_number);
  }
  free_list_.ReleasePage(page_number);
}

void DataAccessLayer::writePage(const Page& page) const {
  std::unique_lock guard(read_write_lock_);

  NOSQL_REQUIRE(IsInitialized(), "DAL is not initialized");
  NOSQL_REQUIRE(page.GetPageNumber() < getNumAllocatedPages(),
                "page number out of bounds, was " << page.GetPageNumber() << ", max page number is "
                                                  << getNumAllocatedPages());

  auto fout = getOutputFileStream(file_path_);
  fout.seekp(static_cast<std::streamoff>(page.page_number_ * GetPageSize()));
  fout.write(page.GetChars(), static_cast<std::streamsize>(GetPageSize()));
}

void DataAccessLayer::readPage(Page& page, bool safe_mode) const {
  std::shared_lock guard(read_write_lock_);

  NOSQL_REQUIRE(IsInitialized(), "DAL is not initialized");
  NOSQL_REQUIRE(page.GetPageSize() == GetPageSize(),
                "page size mismatch, page had size " << page.GetPageSize() << ", but DAL page size is "
                                                     << GetPageSize());
  if (safe_mode) {
    NOSQL_REQUIRE(page.GetPageNumber() < getNumAllocatedPages(),
                  "page number out of bounds, was " << page.GetPageNumber() << ", max page number is "
                                                    << getNumAllocatedPages());
  }
  std::ifstream fin(file_path_, std::ios::binary);
  fin.seekg(static_cast<std::streamoff>(page.GetPageNumber() * GetPageSize()));
  fin.read(page.getChars(), static_cast<std::streamsize>(GetPageSize()));
}

void DataAccessLayer::createDB() {
  // Allocate page 0 for the meta. Allocates the first page of space.
  auto initial_page = getNewPage();
  NOSQL_REQUIRE(initial_page == 0, "page 0 is not free, next page was " << initial_page);

  // Get a page for the free list.
  auto free_list_page = getNewPage();
  meta_.free_list_page_ = free_list_page;

  // Write the meta to page 0.
  FreestandingPage freestanding_page(0, /* transaction_number= */ 0, GetPageSize());
  GetPage(0 /* meta page */, freestanding_page);

  serialize(freestanding_page, meta_);
  writePage(freestanding_page);

  // Write the free list to the free list page.
  GetPage(free_list_page, freestanding_page);

  serialize(freestanding_page, free_list_);
  writePage(freestanding_page);

  // NOTE: Not creating the main index page yet, since the lack of this page indicates to the DataManager that
  // the database is being set up.
}

void DataAccessLayer::openDB() {
  NOSQL_REQUIRE(std::filesystem::exists(file_path_), "file '" << file_path_ << "' not exist");

  // Open the meta page and store it. The meta page is always page 0.
  FreestandingPage freestanding_page(0, /* transaction_number= */ 0, GetPageSize());
  getPage(0, freestanding_page, false);
  // Deserialize the meta page, in the freestanding_page, into the meta structure.
  deserialize(freestanding_page, meta_);

  // Find the free-list page. Still have to use "unsafe" get page, since we haven't loaded the free list yet
  // to tell us whether loading a page is "safe!"
  getPage(meta_.free_list_page_, freestanding_page, false);
  deserialize(freestanding_page, free_list_);
}

void DataAccessLayer::initialize() {
  if (!std::filesystem::exists(db_path_)) {
    std::filesystem::create_directories(db_path_);
  }

  if (!std::filesystem::exists(file_path_)) {
    {  // "Touch" the file.
      std::ofstream fout(file_path_, std::ios::binary | std::ios::out);
      NOSQL_ASSERT(!fout.fail(), "could not open file '" << file_path_ << "'");
    }
    createDB();
  }
  else {
    openDB();
  }
}

void DataAccessLayer::updateMeta() const {
  // TODO: This needs to interact with the WAL.
  FreestandingPage meta_page(0, /* transaction_number= */ 0, GetPageSize());
  serialize(meta_page, meta_);
  writePage(meta_page);
}

void DataAccessLayer::setIndexPage(page_number_t index_page) {
  meta_.index_page_ = index_page;
  updateMeta();
}

void DataAccessLayer::updateFreeList() const {
  // If there is no free list, or the free list is already current, we don't need to update it.
  if (meta_.free_list_page_ == 0 || !free_list_.IsDirty()) {
    return;
  }

  try {
    // Get the free list page. TODO: This needs to interact with the WAL.
    FreestandingPage free_list_page(meta_.free_list_page_, /* transaction_number= */ 0, GetPageSize());
    // Serialize the free list into the page.
    serialize(free_list_page, free_list_);
    // Write the page back to storage.
    writePage(free_list_page);
    free_list_.Clean();
  } catch (std::exception& ex) {
    LOG_SEV(Error) << "Error updating free list: " << ex.what();
  }
}

void DataAccessLayer::serialize(Page& page, const FreeList& free_list) {
  // TODO: Check that there is enough space left in the meta page.
  // TODO: Allow the free list to be written to multiple pages?

  auto offset = page.WriteToPage(0, free_list.next_page_number_);
  offset = page.WriteToPage(offset, free_list.freed_pages_.size());
  for (auto page_number : free_list.freed_pages_) {
    offset = page.WriteToPage(offset, page_number);
  }
}

void DataAccessLayer::deserialize(const Page& page, FreeList& free_list) {
  const auto* buffer = page.GetChars();

  read(buffer, free_list.next_page_number_);
  std::size_t size;
  read(buffer, size);
  for (std::size_t i = 0; i < size; ++i) {
    page_number_t page_number;
    read(buffer, page_number);
    free_list.freed_pages_.push_back(page_number);
  }
}

void DataAccessLayer::serialize(Page& page, const Meta& meta) {
  auto offset = page.WriteToPage(0, Meta::meta_magic_number_);
  offset = page.WriteToPage(offset, meta.page_size_power_);
  offset = page.WriteToPage(offset, meta.free_list_page_);
  page.WriteToPage(offset, meta.index_page_);
}

void DataAccessLayer::deserialize(const Page& page, Meta& meta) {
  const auto* buffer = page.GetChars();

  uint64_t check_sequence;
  read(buffer, check_sequence);
  // Make sure the magic sequence matches.
  NOSQL_ASSERT(
      check_sequence == Meta::meta_magic_number_,
      "magic number mismatch, expected '" << Meta::meta_magic_number_ << "', got '" << check_sequence << "'");

  read(buffer, meta.page_size_power_);
  // Set the page size.
  meta.page_size_ = static_cast<page_size_t>(1 << meta.page_size_power_);

  read(buffer, meta.free_list_page_);
  read(buffer, meta.index_page_);
}

std::ofstream DataAccessLayer::getOutputFileStream(const std::filesystem::path& file_path) {
  std::ofstream fout(file_path, std::ios::binary | std::ios::in | std::ios::out | std::ios::ate);
  NOSQL_ASSERT(!fout.fail(), "could not open file '" << file_path << "'");
  // Go to the beginning of the file.
  fout.seekp(0, std::ios_base::beg);
  return fout;
}

}  // namespace neversql