//
// Created by Nathaniel Rupprecht on 2/25/24.
//

#include <iostream>
#include <string>

#include "NeverSQL/database/DataManager.h"

#include "NeverSQL/data/btree/BTree.h"
#include "NeverSQL/utility/HexDump.h"

using lightning::AnsiColor8Bit;
using lightning::formatting::AnsiForegroundColor;

int main() {
  lightning::Global::GetCore()->AddSink(lightning::NewSink<lightning::StdoutSink>());

  // ---> Your database path here.
  std::filesystem::path database_path = "test.db";

  neversql::DataManager manager(database_path);
  {
    std::string str = "Brave new world.";
    manager.AddValue(0,
                     std::span<const std::byte>(reinterpret_cast<const std::byte*>(str.data()), str.size()));
  }
  {
    std::string str = "Hello, World!";
    manager.AddValue(1,
                     std::span<const std::byte>(reinterpret_cast<const std::byte*>(str.data()), str.size()));
  }
  {
    std::string str = "This is a test.";
    manager.AddValue(2,
                     std::span<const std::byte>(reinterpret_cast<const std::byte*>(str.data()), str.size()));
  }
  {
    std::string str = "Garbanzo beans!";
    manager.AddValue(3,
                     std::span<const std::byte>(reinterpret_cast<const std::byte*>(str.data()), str.size()));
  }
  {
    std::string str = "Brent is a real product";
    manager.AddValue(4,
                     std::span<const std::byte>(reinterpret_cast<const std::byte*>(str.data()), str.size()));
  }

  // Index page is only on page 2.
  manager.HexDumpPage(2, std::cout);

  return 0;
}
