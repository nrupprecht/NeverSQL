//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#pragma once

#include <Lightning/Lightning.h>

namespace neversql {

// Use the lightning datetime as the datetime.
using lightning::time::DateTime;

#define NO_DISCARD [[nodiscard]]

// Just rebrand the standard contract macros.
#define NOSQL_REQUIRE(condition, message) LL_REQUIRE(condition, message)
#define NOSQL_ASSERT(condition, message) LL_ASSERT(condition, message)
#define NOSQL_FAIL(message) LL_FAIL(message)

using PrimaryKey = uint64_t;

using PageNumber = uint32_t;

}  // namespace neversql
