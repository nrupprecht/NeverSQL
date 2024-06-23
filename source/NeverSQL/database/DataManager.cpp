//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#include "NeverSQL/database/DataManager.h"
// Other files.
#include "NeverSQL/data/internals/DocumentPayloadSerializer.h"
#include "NeverSQL/data/internals/Utility.h"
#include "NeverSQL/utility/PageDump.h"

namespace neversql {

DataManager::DataManager(const std::filesystem::path& database_path)
    : data_access_layer_(database_path)
    , page_cache_(database_path / "walfiles", 256 /* Just a random number for now */, &data_access_layer_) {
  // TODO: Make the meta page more independent from the database.

  // Check if the database was just initialized.
  if (auto&& meta = data_access_layer_.GetMeta(); meta.GetIndexPage() == 0) {
    // No index page has been created yet. Create a new B-tree for the index.
    collection_index_ = BTreeManager::CreateNewBTree(page_cache_, DataTypeEnum::String);

    auto root_page_number = collection_index_->GetRootPageNumber();
    LOG_SEV(Trace) << "Collection index root page allocated to be page " << root_page_number << ".";

    data_access_layer_.setIndexPage(root_page_number);
  }
  else {
    LOG_SEV(Trace) << "Loaded collection index from page " << meta.GetIndexPage() << ".";

    collection_index_ = std::make_unique<BTreeManager>(meta.GetIndexPage(), page_cache_);
    std::size_t num_collections {};
    for (auto entry : *collection_index_) {
      // Interpret the data as a document.
      auto document = internal::EntryToDocument(*entry);

      auto collection_name = document->TryGetAs<std::string>("collection_name").value();
      auto page_number = document->TryGetAs<page_number_t>("index_page_number").value();

      LOG_SEV(Debug) << "Loaded collection named '" << collection_name << "' with index page " << page_number
                     << ".";
      collections_.emplace(collection_name, std::make_unique<BTreeManager>(page_number, page_cache_));
      ++num_collections;
    }
    LOG_SEV(Debug) << "Found " << num_collections << " collections.";
  }
}

void DataManager::AddCollection(const std::string& collection_name, DataTypeEnum key_type) {
  // Create a new B-tree for the collection
  auto btree = BTreeManager::CreateNewBTree(page_cache_, key_type);

  auto page_number = btree->GetRootPageNumber();

  auto document = std::make_unique<Document>();
  document->AddElement("collection_name", StringValue {collection_name});
  document->AddElement("index_page_number", IntegralValue {page_number});

  auto creator = internal::MakeCreator<internal::DocumentPayloadSerializer>(std::move(document));
  collection_index_->AddValue(internal::SpanValue(collection_name), creator);

  // Cache the collection in the data manager.
  collections_.emplace(collection_name, std::move(btree));
}

void DataManager::AddValue(const std::string& collection_name, GeneralKey key, const Document& document) {
  // Find the collection.
  auto it = collections_.find(collection_name);
  // TODO: Error handling without throwing.
  NOSQL_ASSERT(it != collections_.end(), "Collection '" << collection_name << "' does not exist.");

  auto creator = internal::MakeCreator<internal::DocumentPayloadSerializer>(document);
  it->second->AddValue(key, creator);
}

SearchResult DataManager::Search(const std::string& collection_name, GeneralKey key) const {
  // Find the collection.
  auto it = collections_.find(collection_name);
  // TODO: Error handling without throwing.
  NOSQL_ASSERT(it != collections_.end(), "Collection '" << collection_name << "' does not exist.");
  return it->second->search(key);
}

RetrievalResult DataManager::Retrieve(const std::string& collection_name, GeneralKey key) const {
  // Find the collection.
  auto it = collections_.find(collection_name);
  // TODO: Error handling without throwing.
  NOSQL_ASSERT(it != collections_.end(), "Collection '" << collection_name << "' does not exist.");
  return it->second->retrieve(key);
}

void DataManager::AddValue(const std::string& collection_name, const Document& document) {
  // Find the collection.
  auto it = collections_.find(collection_name);
  // TODO: Error handling without throwing.
  NOSQL_ASSERT(it != collections_.end(), "Collection '" << collection_name << "' does not exist.");

  auto creator = internal::MakeCreator<internal::DocumentPayloadSerializer>(document);
  it->second->AddValue(creator);
}

SearchResult DataManager::Search(const std::string& collection_name, primary_key_t key) const {
  const GeneralKey key_span = internal::SpanValue(key);
  return Search(collection_name, key_span);
}

RetrievalResult DataManager::Retrieve(const std::string& collection_name, primary_key_t key) const {
  const GeneralKey key_span = internal::SpanValue(key);
  return Retrieve(collection_name, key_span);
}

std::set<std::string> DataManager::GetCollectionNames() const {
  std::set<std::string> output;
  std::ranges::for_each(collections_, [&output](const auto& pair) { output.insert(pair.first); });
  return output;
}

BTreeManager::Iterator DataManager::Begin(const std::string& collection_name) const {
  auto it = collections_.find(collection_name);
  const auto& manager = *it->second;
  return manager.begin();
}

BTreeManager::Iterator DataManager::End(const std::string& collection_name) const {
  auto it = collections_.find(collection_name);
  const auto& manager = *it->second;
  return manager.end();
}

bool DataManager::HexDumpPage(page_number_t page_number,
                              std::ostream& out,
                              utility::HexDumpOptions options) const {
  if (data_access_layer_.GetNumPages() <= page_number) {
    return false;
  }
  const auto page = page_cache_.GetPage(page_number);
  const auto view = page->GetView();
  std::istringstream stream(std::string {view});
  HexDump(stream, out, options);
  return true;
}

bool DataManager::NodeDumpPage(page_number_t page_number, std::ostream& out) const {
  if (auto page = collection_index_->loadNodePage(page_number)) {
    utility::PageInspector::NodePageDump(*page, out);
    return true;
  }
  return false;
}

}  // namespace neversql
