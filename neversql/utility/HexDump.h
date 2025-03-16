#pragma once

#include <iostream>
#include <filesystem>
#include <span>

#include <Lightning/Lightning.h>

namespace neversql::utility {

//! \brief Options for a hex dump.
struct HexDumpOptions {
  //! \brief Whether to color non-zero values.
  bool color_nonzero = true;

  //! \brief Whether to interpret the data as characters and write them to the output stream, to the right of the hex dump.
  bool write_characters = true;

  //! \brief The number of u32 to write per row.
  unsigned int width = 8;
};

//! \brief Format a u32 as a hex string, writing to a provided buffer.
//!
//! The buffer must be at least 10 characters, two for "0x" and eight for the hex value.
//!
//! \param begin The beginning of the buffer to write to.
//! \param end The end of the buffer to write to.
//! \param x The value to format.
void FormatHex(char* begin, const char* end, uint32_t x);

//! \brief Format a span of bytes as a binary string.
void FormatBinary(lightning::memory::BasicMemoryBuffer<char>& buffer, std::span<std::byte> data);

//! \brief Format a integral type as a binary string, writing to a provided buffer.
template<typename Integer_t> requires std::is_integral_v<Integer_t>
void FormatBinary(lightning::memory::BasicMemoryBuffer<char>& buffer, Integer_t x) {
  return FormatBinary(buffer, std::span(reinterpret_cast<std::byte*>(&x), sizeof(x)));
}

//! \brief Read data as u32 from an input stream and write it as a hex dump to an output stream.
//! The hex dump will be formatted in rows of `width` bytes, with an optional color for non-zero values.
//!
//! \param in The input stream to read from.
//! \param hex_out The output stream to write the hex dump to.
//! \param options The options for the hex dump.
void HexDump(std::istream& in, std::ostream& hex_out, const HexDumpOptions& options = {});

//! \brief  Read data as u32 from a file and write it as a hex dump to an output stream.
//! The hex dump will be formatted in rows of `width` bytes, with an optional color for non-zero values.
//!
//! \param filepath The path to the file to read to. If the file does not exist, the function exits.
//! \param hex_out The output stream to write the hex dump to.
//! \param options The options for the hex dump.
void HexDump(const std::filesystem::path& filepath, std::ostream& hex_out, const HexDumpOptions& options = {});

}  // namespace neversql::utility
