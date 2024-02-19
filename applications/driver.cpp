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
#include "NeverSQL/utility/HexDump.h"


int main() {
  neversql::Document document(0);
  document.AddEntry("Age", 42);
  document.AddEntry("Birthday", neversql::DateTime(2020'01'01));

  std::cout << (document.GetEntryAs<int>("Age")) << std::endl;
  std::cout << (document.GetEntryAs<neversql::DateTime>("Birthday")) << std::endl;

  neversql::DataAccessLayer layer("database.db");

  std::cout << "Number of pages: " << layer.GetNumPages() << std::endl;

  // auto page = layer.GetNewPage();
  auto page = layer.GetPage(2).value();
  {
    auto* data = page.GetData();
    std::ranges::copy("Hello, World!", data);
  }
  {
    auto view = page.GetView();
    std::istringstream stream(std::string {view});
    neversql::utility::HexDump(stream, std::cout);
  }
  layer.WriteBackPage(page);

  return 0;
}
