//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#include "NeverSQL/database/DataManager.h"
// Other files.
#include "NeverSQL/utility/PageDump.h"

namespace neversql {

DataManager::DataManager(const std::filesystem::path& database_path)
    : data_access_layer_(database_path)
    , page_cache_(database_path / "walfiles", 16 /* Just a random number for now */, &data_access_layer_)
    , primary_index_(&page_cache_) {
  // TODO: Make the meta page more independent from the database.

  auto&& meta = data_access_layer_.GetMeta();
  // Check if the database was just initialized.
  if (meta.GetIndexPage() == 0) {
    // For now, we only support one index, a primary key index. So just set up this page as the root page for
    // the index B-tree.
    // Allocate reserved space in which to fit the auto incrementing primary key.
    auto root_page =
        primary_index_.newNodePage(BTreePageType::RootLeaf, /*reserved_space=*/sizeof(primary_key_t));

    LOG_SEV(Trace) << "Root page allocated to be page " << root_page.GetPageNumber() << ".";

    // Write zero into the reserved space.
    root_page.GetPage().WriteToPage<primary_key_t>(root_page.GetHeader().GetReservedStart(), 0);

    data_access_layer_.setIndexPage(root_page.GetPageNumber());
    primary_index_.index_page_ = root_page.GetPageNumber();
  }
  else {
    // Root node page already allocated.
    primary_index_.index_page_ = meta.GetIndexPage();
  }
}

void DataManager::AddValue(primary_key_t key, std::span<const std::byte> value) {
  primary_index_.AddValue(key, value);
}

void DataManager::AddValue(std::span<const std::byte> value) {
  primary_index_.AddValue(value);
}

SearchResult DataManager::Search(primary_key_t key) const {
  return primary_index_.search(key);
}

RetrievalResult DataManager::Retrieve(primary_key_t key) const {
  RetrievalResult result {.search_result = Search(key)};
  if (result.search_result.node) {
    if (auto offset = *result.search_result.node->getCellByPK(key)) {
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
  if (auto page = primary_index_.loadNodePage(page_number)) {
    neversql::utility::PageInspector::NodePageDump(*page, out);
    return true;
  }
  return false;
}

}  // namespace neversql
