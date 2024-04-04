//
// Created by Nathaniel Rupprecht on 2/28/24.
//

#include "NeverSQL/utility/PageDump.h"
// Other files.
#include <numeric>

#include "NeverSQL/data/internals/KeyPrinting.h"
#include "NeverSQL/utility/DisplayTable.h"
#include "NeverSQL/utility/HexDump.h"

namespace neversql::utility {

void PageInspector::NodePageDump(const BTreeNodeMap& node, std::ostream& out) {
  lightning::memory::StringMemoryBuffer buffer;

  std::vector<std::size_t> numbers;
  std::vector<page_size_t> offsets;
  std::vector<std::string> cell_types;
  std::vector<GeneralKey> primary_keys;
  std::vector<std::byte> flags;
  std::vector<entry_size_t> data_size;
  std::vector<std::string> data;

  // Now traverse the offsets.
  auto pointers = node.getPointers();
  auto is_pointers_page = node.getHeader().IsPointersPage();
  // TODO: Get key type information, correctly read the keys and format them as strings.
  [[maybe_unused]] auto are_key_size_specified = node.getHeader().AreKeySizesSpecified();

  for (std::size_t i = 0; i < pointers.size(); ++i) {
    numbers.push_back(i);
    offsets.push_back(pointers[i]);
    auto cell = node.getCell(pointers[i]);
    std::visit(
        [&](auto&& cell) {
          using T = std::decay_t<decltype(cell)>;
          if constexpr (std::is_same_v<T, DataNodeCell>) {
            auto view = cell.SpanValue();
            std::string_view sv {reinterpret_cast<const char*>(view.data()), view.size()};
            cell_types.emplace_back("Data cell");
            primary_keys.push_back(cell.key);
            flags.push_back(cell.flags);
            data_size.push_back(cell.GetDataSize());
            data.emplace_back(sv);
          }
          else if constexpr (std::is_same_v<T, PointersNodeCell>) {
            cell_types.emplace_back("Pointer cell");
            primary_keys.push_back(cell.key);
            flags.push_back(cell.flags);
            data_size.push_back(cell.GetDataSize());
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
      "PK", primary_keys, [](const GeneralKey& pk) { return internal::HexDumpBytes(pk); }, "BLUE", "BBLUE");

  table.AddColumn(
      "Flags",
      flags,
      [](const std::byte& flag) { return lightning::formatting::Format("{:b}", static_cast<uint8_t>(flag)); },
      "BWHITE",
      "BBLUE");

  table.AddColumn(
      "Data size",
      data_size,
      [](const page_size_t& size) { return std::to_string(size); },
      "BWHITE",
      "BBLUE");

  auto data_col = table.AddColumn(
      "Data",
      data,
      [is_pointers_page](const std::span<const char>& span) {
        std::string_view sv {reinterpret_cast<const char*>(span.data()), span.size()};
        if (is_pointers_page) {
          return lightning::formatting::Format("{}", sv);
        }
        return lightning::formatting::Format("{:?}", sv);
      },
      "BYELLOW",
      "BBLUE");
  data_col->min_width = 40;
  data_col->data_alignment = '<';

  // Write header above the table, using the same width.

  auto header_width = table.GetTotalWidth();
  {  // Write HEADER
    std::fill_n(std::ostream_iterator<char>(out), header_width, '=');
    std::string fmt_string = "\n|{@BWHITE}{:^" + std::to_string(header_width - 2) + "}{@RESET}|\n";
    out << lightning::formatting::Format(fmt_string, "HEADER");
    std::fill_n(std::ostream_iterator<char>(out), header_width, '=');
    out << std::endl;
  }

  // Get the node header.
  auto header = node.getHeader();

  // TODO: Make and use Format function that formats numbers as chars.
  auto magic_number = header.GetMagicNumber();
  std::string_view sv {reinterpret_cast<const char*>(&magic_number), sizeof(magic_number)};
  out << lightning::formatting::Format("|  {:<20}\"{@BRED}{}{@RESET}\"\n", "Magic number:", sv);

  FormatBinary(buffer, header.GetFlags());
  out << lightning::formatting::Format("|  {:<20}{@BBLUE}{}{@RESET}\n", "Flags:", buffer.MoveString());
  buffer.Clear();

  out << lightning::formatting::Format(
      "|  {:<20}{@BWHITE}{}{@RESET}\n", "Free start:", header.GetFreeStart());
  out << lightning::formatting::Format("|  {:<20}{@BWHITE}{}{@RESET}\n", "Free end:", header.GetFreeEnd());
  out << lightning::formatting::Format(
      "|  {:<20}{@BWHITE}{}{@RESET}\n", "Reserved start:", header.GetReservedStart());
  out << lightning::formatting::Format(
      "|  {:<20}{@BGREEN}{}{@RESET}\n", "Page number:", header.GetPageNumber());
  out << lightning::formatting::Format(
      "|  {:<20}{@BWHITE}{}{@RESET}\n", "Additional data:", header.GetAdditionalData());

  out << "|\n|\n";
  out << "|  Hex dump of header:\n";
  out << lightning::formatting::Format(
      "|  {@BYELLOW}{}{@RESET}\n",
      internal::HexDumpBytes(node.GetPage().GetSpan(0, header.GetPointersStart()), false));

  {
    std::fill_n(std::ostream_iterator<char>(out), header_width, '=');
    std::string fmt_string = "\n|{@BWHITE}{:^" + std::to_string(header_width - 2) + "}{@RESET}|\n";
    out << lightning::formatting::Format(fmt_string, "POINTERS INFO");
  }

  // Write the table, which contains information about the slotted pages.
  out << table;
}

}  // namespace neversql::utility