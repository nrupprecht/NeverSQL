//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#pragma once

#include "NeverSQL/utility/Defines.h"

namespace neversql {

enum class DataTypeEnum : uint32_t {
  Integer = 1,
  Double = 2,
  Boolean = 3,
  DateTime = 4,
  String = 5,
};

class Document;

template <typename T>
DataTypeEnum GetDataTypeEnum() {
  using type = std::remove_cvref_t<T>;
  using enum DataTypeEnum;
  if constexpr (std::is_same_v<type, int>)
    return Integer;
  else if constexpr (std::is_same_v<type, double>)
    return Double;
  else if constexpr (std::is_same_v<type, bool>)
    return Boolean;
  else if constexpr (std::is_same_v<type, std::string>)
    return String;
  else if constexpr (std::is_same_v<type, lightning::time::DateTime>)
    return DateTime;

  NOSQL_FAIL("unknown or unhandled type");
}

}  // namespace neversql
