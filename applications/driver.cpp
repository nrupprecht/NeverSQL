// https://betterprogramming.pub/build-a-nosql-database-from-the-scratch-in-1000-lines-of-code-8ed1c15ed924
// SQLite database format: https://sqlite.org/fileformat.html
// Slotted pages:
//  * https://siemens.blog/posts/database-page-layout/
// PostgreSQL btree:
//  * https://www.postgresql.org/docs/current/btree-behavior.html

#include <iostream>
#include <string>

#include "NeverSQL/data/DataAccessLayer.h"
#include "NeverSQL/data/Document.h"
#include "NeverSQL/data/btree/BTree.h"
#include "NeverSQL/utility/HexDump.h"

using lightning::AnsiColor8Bit;
using lightning::formatting::AnsiForegroundColor;

int main() {
  lightning::Global::GetCore()->AddSink(lightning::NewSink<lightning::StdoutSink>());

  // ---> Your database path here.
  std::filesystem::path database_path = "";
  neversql::DataAccessLayer layer(database_path);

  LOG_SEV(Info) << "Number of pages in the database is " << layer.GetNumPages() << ".";

  neversql::BTreeManager manager(&layer);
  // NOTE: This is going to be a private method, but for testing, I made it public for now.
  auto node = manager.newNodePage(neversql::BTreePageType::Leaf);
  LOG_SEV(Info) << "Loaded new node page with page number " << node.GetPageNumber() << ".";
  LOG_SEV(Info) << "We expect there to be no pointers, there are " << node.GetNumPointers() << " pointers.";
  LOG_SEV(Info) << "Amount of (de-fragmented) free space is " << node.GetDefragmentedFreeSpace() << " bytes.";

  std::string str[] = {"Garbanzo beans!", "Hello, World!", "This is a test."};

  std::span<const std::byte> data[] = {
      std::span(reinterpret_cast<const std::byte*>(str[0].data()), str[0].size()),
      std::span(reinterpret_cast<const std::byte*>(str[1].data()), str[1].size()),
      std::span(reinterpret_cast<const std::byte*>(str[2].data()), str[2].size()),
  };

  for (neversql::primary_key_t pk = 0; manager.addElementToNode(node, pk, data[pk % 3]); ++pk) {
    LOG_SEV(Info) << "Added element '" << AnsiColor8Bit(str[pk % 3], AnsiForegroundColor::BrightBlue)
                  << "' to node, PK = " << pk << ".";
  }

  const auto& page = node.GetPage();
  {
    auto view = page.GetView();
    std::istringstream stream(std::string {view});
    neversql::utility::HexDump(
        stream, std::cout, neversql::utility::HexDumpOptions {.color_nonzero = true, .width = 8});
  }

  LOG_SEV(Info) << "Number of pages in the database is " << layer.GetNumPages() << ".";
  LOG_SEV(Info) << "After adding elements to page, there are " << node.GetNumPointers()
                << " entries. Amount of de-fragmented free space is " << node.GetDefragmentedFreeSpace()
                << ".";

  return 0;
}
