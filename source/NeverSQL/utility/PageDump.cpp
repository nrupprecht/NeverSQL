//
// Created by Nathaniel Rupprecht on 2/28/24.
//

#include "NeverSQL/utility/PageDump.h"
// Other files.
#include "NeverSQL/utility/HexDump.h"

namespace neversql::utility {

void PageInspector::NodePageDump(const BTreeNodeMap& node, std::ostream& out) {
  // First, inspect the header.
  auto&& header = node.GetHeader();

  lightning::memory::StringMemoryBuffer buffer;

  out << "============================================" << std::endl;
  out << "               HEADER                       " << std::endl;
  out << "============================================" << std::endl;
  std::string_view sv {reinterpret_cast<const char*>(&header.magic_number), sizeof(header.magic_number)};
  out << "Magic number:    " << sv << std::endl;

  FormatBinary(buffer, header.flags);
  out << "Flags:           " << buffer.ToString() << std::endl;
  buffer.Clear();

  out << "Free start:      " << header.free_start << std::endl;
  out << "Free end:        " << header.free_end << std::endl;
  out << "Reserved start:  " << header.reserved_start << std::endl;
  out << "Page number:     " << header.page_number << std::endl;
  out << "Additional data: " << header.additional_data << std::endl;
  out << "============================================" << std::endl;

  // Now traverse the offsets.
  auto pointers = node.getPointers();
  for (std::size_t i = 0; i < pointers.size(); ++i) {
    out << "Pointer " << i << ": Offset=" << pointers[i] << "   ==>  ";
    auto cell = node.getCell(pointers[i]);
    std::visit(
        [&out](auto&& cell) {
          using T = std::decay_t<decltype(cell)>;
          if constexpr (std::is_same_v<T, DataNodeCell>) {
            out << " (Data cell) PK=" << cell.key << " Sz=" << cell.size_of_entry;
            auto view = cell.SpanValue();
            std::string_view sv{reinterpret_cast<const char*>(view.data()), view.size()};
            out << " Data={" << sv << "}";
          }
          else if constexpr (std::is_same_v<T, PointersNodeCell>) {
            out << " (Pointer cell) PK=" << cell.key << " Page=" << cell.page_number;
          }
          else {
            static_assert(lightning::typetraits::always_false_v<T>, "non-exhaustive visitor!");
          }
        },
        cell);

    out << std::endl;
  }

  out << "============================================" << std::endl;
}

}  // namespace neversql::utility