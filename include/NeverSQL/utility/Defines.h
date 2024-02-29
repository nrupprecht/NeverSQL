//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#pragma once

#include <algorithm>

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

//! \brief Convert a c-string of length at most 8 to a uint64_t.
inline uint64_t ToUInt64(const char* str) {
  NOSQL_REQUIRE(std::strlen(str) <= sizeof(uint64_t), "string too long");
  uint64_t value = 0;
  std::memcpy(&value, str, std::strlen(str));
  return value;
}

}  // namespace neversql
