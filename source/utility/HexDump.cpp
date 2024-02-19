//
// Created by Nathaniel Rupprecht on 2/18/24.
//

#include "NeverSQL/utility/HexDump.h"
// Other files.
#include <iomanip>

#include "NeverSQL/utility/Defines.h"

namespace neversql::utility {

namespace {

void toCharacters(char* begin, const char* end, uint32_t x) {
  // Copy x, as bytes, into the range.
  NOSQL_REQUIRE(4 <= end - begin, "buffer too small");
  std::memcpy(begin, &x, 4);
  std::ranges::for_each(begin, end, [](char& c) {
    if (c < 32 || 126 < c) {
      c = '.';
    }
  });
}

}  // namespace

void FormatHex(char* begin, const char* end, uint32_t x) {
  // x will take up 8 characters, then two for "0x".
  NOSQL_REQUIRE(10 <= end - begin, "buffer too small");
  begin[0] = '0';
  begin[1] = 'x';
  static const char* hex_digits = "0123456789ABCDEF";
  for (char* p = begin + 9; begin + 2 <= p; --p) {
    *p = hex_digits[x & 0xF];
    x >>= 4;
  }
}

void HexDump(std::istream& in, std::ostream& hex_out, const HexDumpOptions& options) {
  unsigned int i = 0;
  unsigned int rows = 0;

  unsigned header_width = options.width * 11 + 9;
  if (options.write_characters) {
    header_width += 4 * options.width + 5;
  }

  // Header.
  std::ranges::fill_n(std::ostream_iterator<char>(hex_out), header_width, '-');
  hex_out << "\n";

  auto color_on =
      lightning::formatting::SetAnsiColorFmt(lightning::formatting::AnsiForegroundColor::BrightBlue);
  auto color_off = lightning::formatting::SetAnsiColorFmt(lightning::formatting::AnsiForegroundColor::Reset);

  char buffer[11];
  buffer[10] = ' ';  // Separator.

  char character_buffer[5];
  buffer[4] = ' ';  // Separator.
  std::stringstream character_stream;

  uint32_t x;
  bool printed_newline = false;
  while (in.read(reinterpret_cast<char*>(&x), sizeof(x))) {
    // If starting a new row, print the row indicator.
    if (i == 0) {
      hex_out << "| " << std::setw(4) << rows << ": | ";
    }

    // If the color is enabled and the value is non-zero, turn on the color.
    if (options.color_nonzero && x != 0) {
      hex_out << color_on;
    }
    // Write the hex representation of the next four bytes.
    FormatHex(buffer, buffer + 11, x);
    hex_out.write(buffer, sizeof(buffer));
    if (options.color_nonzero && x != 0) {
      hex_out << color_off;
    }
    // Reset the newline indicator.
    printed_newline = false;

    if (options.write_characters) {
      toCharacters(character_buffer, character_buffer + 5, x);
      character_stream.write(character_buffer, 4);
    }

    ++i;
    if (i % options.width == 0) {
      hex_out << "| ";
      if (options.write_characters) {
        hex_out << character_stream.str() << " |";
        character_stream.str("");
      }
      hex_out << "\n";
      printed_newline = true;
      i = 0;
      ++rows;
    }
  }
  if (!printed_newline) {
    hex_out << "\n";
  }

  // Footer.
  std::ranges::fill_n(std::ostream_iterator<char>(hex_out), header_width, '-');
  // New line and flush.
  hex_out << std::endl;
}

}  // namespace neversql::utility