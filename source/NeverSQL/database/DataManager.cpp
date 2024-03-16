//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#include "NeverSQL/database/DataManager.h"
// Other files.
#include "NeverSQL/data/internals/Utility.h"
#include "NeverSQL/utility/PageDump.h"

namespace neversql {

DataManager::DataManager(const std::filesystem::path& database_path)
    : data_access_layer_(database_path)
    , page_cache_(database_path / "walfiles", 16 /* Just a random number for now */, &data_access_layer_)
    , collection_index_(nullptr) {
  // TODO: Make the meta page more independent from the database.
  auto&& meta = data_access_layer_.GetMeta();

  // Check if the database was just initialized.
  if (meta.GetIndexPage() == 0) {
    // No index page has been created yet. Create a new B-tree for the index.
    collection_index_ = BTreeManager::CreateNewBTree(page_cache_, DataTypeEnum::String);

    auto root_page_number = collection_index_->GetRootPageNumber();
    LOG_SEV(Trace) << "Collection index root page allocated to be page " << root_page_number << ".";

    data_access_layer_.setIndexPage(root_page_number);
  }
  else {
    LOG_SEV(Trace) << "Loaded collection index from page " << meta.GetIndexPage() << ".";

    collection_index_ = std::make_unique<BTreeManager>(meta.GetIndexPage(), page_cache_);
    // TODO: Traverse all the collections and cache them as BTreeManagers in collections_.
  }
}

void DataManager::AddCollection(const std::string& collection_name, DataTypeEnum key_type) {
  // Create a new B-tree for the collection
  auto btree = BTreeManager::CreateNewBTree(page_cache_, key_type);

  auto page_number = btree->GetRootPageNumber();
  collection_index_->AddValue(internal::SpanValue(collection_name), internal::SpanValue(page_number));

  collections_.emplace(collection_name, std::move(btree));
}

void DataManager::AddValue(const std::string& collection_name,
                           GeneralKey key,
                           std::span<const std::byte> value) {
  // Find the collection.
  auto it = collections_.find(collection_name);
  // TODO: Error handling without throwing.
  NOSQL_ASSERT(it != collections_.end(), "Collection '" << collection_name << "' does not exist.");
  it->second->AddValue(key, value);
}

void DataManager::AddValue(const std::string& collection_name,
                           GeneralKey key,
                           const DocumentBuilder& document) {
  // Serialize the document and add it to the database.
  [[maybe_unused]] auto size = document.CalculateRequiredSize();
  lightning::memory::MemoryBuffer<std::byte> buffer;

  WriteToBuffer(buffer, document);
  std::span<const std::byte> value(buffer.Data(), buffer.Size());
  AddValue(collection_name, key, value);
}

SearchResult DataManager::Search(const std::string& collection_name, GeneralKey key) const {
  // Find the collection.
  auto it = collections_.find(collection_name);
  // TODO: Error handling without throwing.
  NOSQL_ASSERT(it != collections_.end(), "Collection '" << collection_name << "' does not exist.");
  return it->second->search(key);
}

RetrievalResult DataManager::Retrieve(const std::string& collection_name, GeneralKey key) const {
  RetrievalResult result {.search_result = Search(collection_name, key)};
  if (result.search_result.node) {
    if (auto offset = *result.search_result.node->getCellByKey(key)) {
      result.cell_offset = offset;
      result.value_view = std::get<DataNodeCell>(result.search_result.node->getCell(offset)).SpanValue();
    }
    else {
      // Element *DID NOT EXIST IN THE NODE* that it was expected to exist in.
      result.search_result.node = {};
    }
  }
  return result;
}

void DataManager::AddValue(const std::string& collection_name,
                           primary_key_t key,
                           std::span<const std::byte> value) {
  // Find the collection.
  auto it = collections_.find(collection_name);
  // TODO: Error handling without throwing.
  NOSQL_ASSERT(it != collections_.end(), "Collection '" << collection_name << "' does not exist.");

  GeneralKey key_span = internal::SpanValue(key);
  it->second->AddValue(key_span, value);
}

void DataManager::AddValue(const std::string& collection_name, std::span<const std::byte> value) {
  // Find the collection.
  auto it = collections_.find(collection_name);
  // TODO: Error handling without throwing.
  NOSQL_ASSERT(it != collections_.end(), "Collection '" << collection_name << "' does not exist.");

  it->second->AddValue(value);
}

void DataManager::AddValue(const std::string& collection_name, const DocumentBuilder& document) {
  // Serialize the document and add it to the database.
  // TODO: Deal with documents that are too long.
  //  NOTE: This is not the best way to do this, I just want to get something that works.
  [[maybe_unused]] auto size = document.CalculateRequiredSize();
  lightning::memory::MemoryBuffer<std::byte> buffer;

  WriteToBuffer(buffer, document);
  std::span<const std::byte> value(buffer.Data(), buffer.Size());
  AddValue(collection_name, value);
}

SearchResult DataManager::Search(const std::string& collection_name, primary_key_t key) const {
  GeneralKey key_span = internal::SpanValue(key);
  return Search(collection_name, key_span);
}

RetrievalResult DataManager::Retrieve(const std::string& collection_name, primary_key_t key) const {
  GeneralKey key_span = internal::SpanValue(key);

  return Retrieve(collection_name, key_span);
}


BTreeManager::Iterator DataManager::Begin(const std::string& collection_name) const {
  auto it = collections_.find(collection_name);
  auto& manager = *it->second;
  return manager.begin();
}

BTreeManager::Iterator DataManager::End(const std::string& collection_name) const {
  auto it = collections_.find(collection_name);
  auto& manager = *it->second;
  return manager.end();
}

bool DataManager::HexDumpPage(page_number_t page_number,
                              std::ostream& out,
                              utility::HexDumpOptions options) const {
  if (data_access_layer_.GetNumPages() <= page_number) {
    return false;
  }
  auto page = page_cache_.GetPage(page_number);
  auto view = page->GetView();
  std::istringstream stream(std::string {view});
  neversql::utility::HexDump(stream, out, options);
  return true;
}

bool DataManager::NodeDumpPage(page_number_t page_number, std::ostream& out) const {
  if (auto page = collection_index_->loadNodePage(page_number)) {
    neversql::utility::PageInspector::NodePageDump(*page, out);
    return true;
  }
  return false;
}

}  // namespace neversql
