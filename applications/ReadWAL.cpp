//
// Created by Nathaniel Rupprecht on 12/19/24.
//

#include <filesystem>
#include <fstream>

#include "neversql/recovery/WriteAheadLog.h"

using namespace neversql;

int main() {
  std::filesystem::path path =
      "/Users/nrupprecht/Library/Mobile "
      "Documents/com~apple~CloudDocs/Documents/Nathaniel/Programs/C++/NeverSQL/";

  std::filesystem::path database_path = path / "dbs/shakespeare-database";
  std::ifstream walfile;
  walfile.open(database_path / "walfiles/wal.log", std::ios::binary | std::ios::in);

  if (!walfile.is_open()) {
    std::cerr << "Could not open WAL file." << std::endl;
    return 1;
  }

  RecordType type;

  sequence_number_t next_sequence_number;
  transaction_t transaction_id;
  page_number_t page_number;
  page_size_t offset;
  std::streamsize data_size;
  std::vector<std::byte> data_old;
  std::vector<std::byte> data_new;

  std::size_t read_max = 10, count = 0;

  auto read = [&walfile](auto& data) { walfile.read(reinterpret_cast<char*>(&data), sizeof(data)); };

  std::cout << "====================================================" << std::endl;
  while (!walfile.eof()) {
    if (count == read_max) {
      break;
    }

    read(type);
    read(transaction_id);

    if (type == RecordType::UPDATE) {
      read(next_sequence_number);
      read(page_number);
      read(offset);
      read(data_size);
      data_old.resize(data_size);
      walfile.read(reinterpret_cast<char*>(data_old.data()), data_size);
      data_new.resize(data_size);
      walfile.read(reinterpret_cast<char*>(data_new.data()), data_size);

      std::cout << "Transaction ID:  " << transaction_id << std::endl;
      std::cout << "Sequence number: " << next_sequence_number << std::endl;
      std::cout << "Page number:     " << page_number << std::endl;
      std::cout << "Offset:          " << offset << std::endl;
      std::cout << "Data size:       " << data_size << std::endl;

      std::cout << "Data old:        ";
      for (auto byte : data_old) {
        std::cout << std::hex << static_cast<int>(byte) << " ";
      }
      std::cout << std::endl;

      std::cout << "Data new:        ";
      for (auto byte : data_new) {
        std::cout << std::hex << static_cast<int>(byte) << " ";
      }
      std::cout << std::dec << std::endl;
    }
    else {
      LL_FAIL("Unknown record type.");
    }

    std::cout << "====================================================" << std::endl;

    ++count;
  }

  return 0;
}