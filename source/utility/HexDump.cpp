//
// Created by Nathaniel Rupprecht on 2/18/24.
//

#include "NeverSQL/utility/HexDump.h"
// Other files.
#include <iomanip>

#include "NeverSQL/utility/Defines.h"

namespace neversql::utility {

namespace {

void toCharacters(lightning::memory::BasicMemoryBuffer<char>& buffer,
                  uint32_t x,
                  bool color_characters = false) {
  using namespace lightning::memory;
  using namespace lightning::formatting;

  bool coloring_chars = false;
  char x_buffer[4];
  std::memcpy(x_buffer, &x, 4);

  std::ranges::for_each(x_buffer, x_buffer + 4, [&](char& c) {
    if (c < 32 || 126 < c) {
      if (color_characters && coloring_chars) {
        // Turn off character (yellow) coloring.
        AppendBuffer(buffer, SetAnsiColorFmt(AnsiForegroundColor::Reset));
        coloring_chars = false;
      }
      buffer.PushBack('.');
    }
    else {
      if (color_characters && !coloring_chars) {
        // Turn on character (yellow) coloring
        AppendBuffer(buffer, SetAnsiColorFmt(AnsiForegroundColor::BrightYellow));
        coloring_chars = true;
      }
      buffer.PushBack(c);
    }
  });

  if (color_characters && coloring_chars) {
    // Turn off character (yellow) coloring.
    AppendBuffer(buffer, SetAnsiColorFmt(AnsiForegroundColor::Reset));
  }
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

void FancyFormatHex(lightning::memory::BasicMemoryBuffer<char>& buffer, uint32_t x) {
  using namespace lightning::memory;
  using namespace lightning::formatting;

  // x will take up 8 characters, then two for "0x".
  static const char* hex_digits = "0123456789ABCDEF";

  if (x == 0) {
    // If x is zero, just write "0x00000000", in light gray.
    auto fmt = SetAnsiColorFmt(lightning::formatting::AnsiForegroundColor::BrightBlack);
    AppendBuffer(buffer, fmt);
    AppendBuffer(buffer, "0x00000000");
    AppendBuffer(buffer, SetAnsiColorFmt(AnsiForegroundColor::Reset));
    return;
  }

  // Serialize each byte of x into the buffer.
  char bytes[4][2];
  auto y = x;
  for (auto& byte : bytes) {
    byte[0] = hex_digits[y & 0xF];
    y >>= 4;
    byte[1] = hex_digits[y & 0xF];
    y >>= 4;
  }
  // Color any character bytes green, any other bytes blue.
  AppendBuffer(buffer, SetAnsiColorFmt(AnsiForegroundColor::BrightBlue));
  AppendBuffer(buffer, "0x");

  bool is_coloring_char = false;
  y = x;

  for (auto& byte : bytes) {
    auto c = y & 0xFF;
    y >>= 8;

    if (c < 32 || 126 < c) {
      if (is_coloring_char) {
        // Turn off character (green) coloring.
        AppendBuffer(buffer, SetAnsiColorFmt(AnsiForegroundColor::Green));
        is_coloring_char = false;
      }
    }
    else if (!is_coloring_char) {
      // Turn on character (green) coloring.
      AppendBuffer(buffer, SetAnsiColorFmt(AnsiForegroundColor::Green));
      is_coloring_char = true;
    }
    // The buffer is not null terminated, so pass in the size explicitly.
    AppendBuffer(buffer, byte, byte + 2);
  }
  AppendBuffer(buffer, SetAnsiColorFmt(AnsiForegroundColor::Reset));
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

  lightning::memory::StringMemoryBuffer str_buffer;
  lightning::memory::StringMemoryBuffer character_buffer;

  uint32_t x;
  bool printed_newline = false;
  while (in.read(reinterpret_cast<char*>(&x), sizeof(x))) {
    // If starting a new row, print the row indicator.
    if (i == 0) {
      hex_out << "| " << std::setw(4) << rows << ": | ";
    }

    if (options.color_nonzero) {
      FancyFormatHex(str_buffer, x);
      hex_out.write(str_buffer.Data(), static_cast<int64_t>(str_buffer.Size()));
      hex_out.write(" ", 1);
      str_buffer.Clear();
    }
    else {
      // Write the hex representation of the next four bytes.
      FormatHex(buffer, buffer + 11, x);
      hex_out.write(buffer, 11);
    }

    // Reset the newline indicator.
    printed_newline = false;

    if (options.write_characters) {
      toCharacters(character_buffer, x, options.color_nonzero);
    }

    ++i;
    if (i % options.width == 0) {
      hex_out << "| ";
      if (options.write_characters) {
        hex_out << character_buffer.Data() << " |";
        character_buffer.Clear();
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

void HexDump(const std::filesystem::path& filepath, std::ostream& hex_out, const HexDumpOptions& options) {
  std::ifstream in(filepath, std::ios::binary);
  if (in.fail()) {
    return;
  }
  HexDump(in, hex_out, options);
}

}  // namespace neversql::utility