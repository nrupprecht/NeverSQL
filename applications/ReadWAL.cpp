//
// Created by Nathaniel Rupprecht on 12/19/24.
//

#include <filesystem>
#include <fstream>

#include "NeverSQL/recovery/WriteAheadLog.h"

using namespace neversql;

int main() {
  std::filesystem::path database_path =
      "/Users/nathaniel/Documents/Nathaniel/Programs/C++/NeverSQL/shakespeare-database";
  std::ifstream walfile;
  walfile.open(database_path / "walfiles/wal.log", std::ios::binary | std::ios::in);

  sequence_number_t next_sequence_number;
  transaction_t transaction_id;
  page_number_t page_number;
  page_size_t offset;
  std::streamsize data_size;
  std::vector<std::byte> data_old;
  std::vector<std::byte> data_new;

  std::cout << "====================================================" << std::endl;
  while (!walfile.eof()) {
    walfile.read(reinterpret_cast<char*>(&next_sequence_number), sizeof(sequence_number_t));
    walfile.read(reinterpret_cast<char*>(&transaction_id), sizeof(transaction_t));
    walfile.read(reinterpret_cast<char*>(&page_number), sizeof(page_number_t));
    walfile.read(reinterpret_cast<char*>(&offset), sizeof(page_size_t));
    walfile.read(reinterpret_cast<char*>(&data_size), sizeof(std::streamsize));

    data_old.resize(data_size);
    walfile.read(reinterpret_cast<char*>(data_old.data()), data_size);
    data_new.resize(data_size);
    walfile.read(reinterpret_cast<char*>(data_new.data()), data_size);

    std::cout << "Sequence number: " << next_sequence_number << std::endl;
    std::cout << "Transaction ID:  " << transaction_id << std::endl;
    std::cout << "Page number:     " << page_number << std::endl;
    std::cout << "Offset:          " << offset << std::endl;
    std::cout << "Data size:       " << data_size << std::endl;

    std::cout << "====================================================" << std::endl;
  }

  return 0;
}