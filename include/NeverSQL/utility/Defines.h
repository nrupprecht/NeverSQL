//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#pragma once

#include <algorithm>
#include <cstddef>  // std::byte
#include <span>

#include <Lightning/Lightning.h>

namespace neversql {

// Use the lightning datetime as the datetime.
using lightning::time::DateTime;

#define NO_DISCARD [[nodiscard]]

// Just rebrand the standard contract macros.
#define NOSQL_REQUIRE(condition, message) LL_REQUIRE(condition, message)
#define NOSQL_ASSERT(condition, message) LL_ASSERT(condition, message)
#define NOSQL_FAIL(message) LL_FAIL(message)

//! \brief The datatype to use to represent the integral primary key of a record.
using primary_key_t = uint64_t;

//! \brief The datatype to use to represent the number of a page.
using page_number_t = uint64_t;

//! \brief The datatype to use to represent the size of a page, or the size of offsets within a page.
using page_size_t = uint16_t;

//! \brief The data type to use to represent the size of an entry in a page.
using entry_size_t = uint32_t;

//! \brief The data type used to represent a transaction ID.
using transaction_t = uint64_t;

//! \brief The data type used to represent an LSN (log sequence number).
using sequence_number_t = uint64_t;

//! \brief Convert a c-string of length at most 8 to a uint64_t.
inline uint64_t ToUInt64(const char* str) {
  NOSQL_REQUIRE(std::strlen(str) <= sizeof(uint64_t), "string too long");
  uint64_t value = 0;
  std::memcpy(&value, str, std::strlen(str));
  return value;
}

}  // namespace neversql

namespace std {

inline void format_logstream(const exception& ex, lightning::RefBundle& handler) {
  using namespace lightning;
  using namespace lightning::formatting;

  handler << NewLineIndent << AnsiColor8Bit(R"(""")", AnsiForegroundColor::Red)
          << AnsiColorSegment(AnsiForegroundColor::Yellow);  // Exception in yellow.
  const char *begin = ex.what();
  const char *end = ex.what();
  while (*end) {
    for (; *end && *end != '\n'; ++end)
      ;  // Find next newline.
    handler << NewLineIndent << string_view(begin, static_cast<string_view::size_type>(end - begin));
    for (; *end && *end == '\n'; ++end)
      ;  // Pass any number of newlines.
    begin = end;
  }
  handler << AnsiResetSegment << NewLineIndent  // Reset colors to default.
          << AnsiColor8Bit(R"(""")", AnsiForegroundColor::Red);
}

template<typename T>
strong_ordering CompareSpanValues(span<T> lhs, span<T> rhs) {
  auto min_size = min(lhs.size(), rhs.size());
  for (typename span<T>::size_type i = 0; i < min_size; ++i) {
    auto cmp = lhs[i] <=> rhs[i];
    if (cmp != strong_ordering::equivalent) {
      return cmp;
    }
  }
  if (lhs.size() < rhs.size()) {
    return strong_ordering::less;
  }
  if (lhs.size() > rhs.size()) {
    return strong_ordering::greater;
  }
  return strong_ordering::equivalent;
}

}  // namespace std
