//
// Created by Nathaniel Rupprecht on 2/28/24.
//

#include "NeverSQL/utility/PageDump.h"
// Other files.
#include <format>
#include <numeric>

#include "NeverSQL/utility/HexDump.h"
#include "NeverSQL/utility/DisplayTable.h"

namespace neversql::utility {


void PageInspector::NodePageDump(const BTreeNodeMap& node, std::ostream& out) {
  // First, inspect the header.
  auto&& header = node.GetHeader();

  lightning::memory::StringMemoryBuffer buffer;

  std::vector<std::size_t> numbers;
  std::vector<page_size_t> offsets;
  std::vector<std::string> cell_types;
  std::vector<primary_key_t> primary_keys;
  std::vector<entry_size_t> data_size;
  std::vector<std::span<const char>> data;

  // Now traverse the offsets.
  auto pointers = node.getPointers();
  for (std::size_t i = 0; i < pointers.size(); ++i) {
    numbers.push_back(i);
    offsets.push_back(pointers[i]);

    // out << "Pointer " << i << ": Offset=" << pointers[i] << "   ==>  ";
    auto cell = node.getCell(pointers[i]);
    std::visit(
        [&](auto&& cell) {
          using T = std::decay_t<decltype(cell)>;
          if constexpr (std::is_same_v<T, DataNodeCell>) {
            // out << " (Data cell) PK=" << cell.key << " Sz=" << cell.size_of_entry;
            auto view = cell.SpanValue();
            std::string_view sv{reinterpret_cast<const char*>(view.data()), view.size()};
            // out << " Data={" << sv << "}";

            cell_types.push_back("Data cell");
            primary_keys.push_back(cell.key);
            data_size.push_back(cell.size_of_entry);
            data.push_back(sv);
          }
          else if constexpr (std::is_same_v<T, PointersNodeCell>) {
            // out << " (Pointer cell) PK=" << cell.key << " Page=" << cell.page_number;

            cell_types.push_back("Pointer cell");
            primary_keys.push_back(cell.key);
            data_size.push_back(0);
            data.push_back(std::to_string(cell.page_number));
          }
          else {
            static_assert(lightning::typetraits::always_false_v<T>, "non-exhaustive visitor!");
          }
        },
        cell);
  }

  DisplayTable table;
  table.AddColumn(
      "Pointer", numbers, [](const std::size_t& num) { return std::to_string(num); }, "BWHITE", "BBLUE");

  table.AddColumn(
      "Offset", offsets, [](const page_size_t& offset) { return std::to_string(offset); }, "RED", "BBLUE");

  table.AddColumn("Type", cell_types, [](const std::string& type) { return type; }, "BWHITE", "BBLUE");

  table.AddColumn(
      "PK", primary_keys, [](const primary_key_t& pk) { return std::to_string(pk); }, "BLUE", "BBLUE");

  table.AddColumn(
      "Data size",
      data_size,
      [](const page_size_t& size) { return std::to_string(size); },
      "BWHITE",
      "BBLUE");

  auto data_col = table.AddColumn(
      "Data",
      data,
      [](const std::span<const char>& span) {
        std::string_view sv {reinterpret_cast<const char*>(span.data()), span.size()};
        return std::string(sv);
      },
      "BYELLOW",
      "BBLUE");
  data_col->min_width = 40;
  data_col->data_alignment = '<';

  // Write header above the table, using the same width.

  auto header_width = table.GetTotalWidth();
  { // Write HEADER
    std::fill_n(std::ostream_iterator<char>(out), header_width, '=');
    std::string fmt_string = "\n|{@BWHITE}{:^" + std::to_string(header_width - 2) + "}{@RESET}|\n";
    out << lightning::formatting::Format(fmt_string, "HEADER");
    std::fill_n(std::ostream_iterator<char>(out), header_width, '=');
    out << std::endl;
  }

  std::string_view sv {reinterpret_cast<const char*>(&header.magic_number), sizeof(header.magic_number)};
  out << lightning::formatting::Format("|  {:<20}\"{@BRED}{}{@RESET}\"\n", "Magic number:", std::string(sv));

  FormatBinary(buffer, header.flags);
  out << lightning::formatting::Format("|  {:<20}{@BBLUE}{}{@RESET}\n", "Flags:", buffer.MoveString());
  buffer.Clear();

  out << lightning::formatting::Format("|  {:<20}{@BWHITE}{}{@RESET}\n", "Free start:", header.free_start);
  out << lightning::formatting::Format("|  {:<20}{@BWHITE}{}{@RESET}\n", "Free end:", header.free_end);
  out << lightning::formatting::Format("|  {:<20}{@BWHITE}{}{@RESET}\n", "Reserved start:", header.reserved_start);
  out << lightning::formatting::Format("|  {:<20}{@BGREEN}{}{@RESET}\n", "Page number:", header.page_number);
  out << lightning::formatting::Format("|  {:<20}{@BWHITE}{}{@RESET}\n", "Additional data:", header.additional_data);

  {
    std::fill_n(std::ostream_iterator<char>(out), header_width, '=');
    std::string fmt_string = "\n|{@BWHITE}{:^" + std::to_string(header_width - 2) + "}{@RESET}|\n";
    out << lightning::formatting::Format(fmt_string, "POINTERS INFO");
  }
  // Write the table, which contains information about the slotted pages.
  out << table;
}

}  // namespace neversql::utility