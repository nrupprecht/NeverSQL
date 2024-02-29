//
// Created by Nathaniel Rupprecht on 2/24/24.
//

#include "NeverSQL/database/DataManager.h"
// Other files.
#include "NeverSQL/utility/PageDump.h"

namespace neversql {

DataManager::DataManager(const std::filesystem::path& database_path)
    : data_access_layer_(database_path)
    , primary_index_(&data_access_layer_) {
  // TODO: Make the meta page more independent from the database.

  auto&& meta = data_access_layer_.GetMeta();
  // Check if the database was just initialized.
  if (meta.GetIndexPage() == 0) {
    // For now, we only support one index, a primary key index. So just set up this page as the root page for
    // the index B-tree.
    auto root_page = primary_index_.newNodePage(BTreePageType::RootLeaf);
    LOG_SEV(Trace) << "Root page allocated to be page " << root_page.GetPageNumber() << ".";

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

SearchResult DataManager::Search(primary_key_t key) const {
  return primary_index_.search(key);
}

bool DataManager::HexDumpPage(page_number_t page_number,
                              std::ostream& out,
                              utility::HexDumpOptions options) const {
  if (auto page = data_access_layer_.GetPage(page_number)) {
    auto view = page->GetView();
    std::istringstream stream(std::string {view});
    neversql::utility::HexDump(stream, out, options);
    return true;
  }
  return false;
}

bool DataManager::NodeDumpPage(page_number_t page_number, std::ostream& out) const {
  if (auto page = primary_index_.loadNodePage(page_number)) {
    neversql::utility::PageInspector::NodePageDump(*page, out);
    return true;
  }
  return false;
}

}  // namespace neversql
