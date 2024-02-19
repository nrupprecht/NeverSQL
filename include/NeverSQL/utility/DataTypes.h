//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#pragma once

#include "NeverSQL/utility/Defines.h"

namespace neversql {

enum class DataTypeEnum : uint32_t {
  Integer = 0b0000'0000'0000'0001,
  LongInteger = 0b0000'0000'0000'0010,
  Float = 0b0000'0000'0000'0100,
  LongFloat = 0b0000'0000'0000'1000,
  Boolean = 0b0000'0000'0001'0000,
  String = 0b0000'0000'0010'0000,
  // Date        = 0b0000'0000'0100'0000,
  DateTime = 0b0000'0000'1000'0000,
  DBDocument = 0b0000'0001'0000'0000,

  // Special types.
  PrimaryKey = 0b0100'0000'0000'0000,
  ForeignKey = 0b1000'0000'0000'0000,
};

class Document;

template <typename T>
DataTypeEnum GetDataTypeEnum() {
  using enum DataTypeEnum;
  if constexpr (std::is_same_v<T, int>)
    return Integer;
  else if constexpr (std::is_same_v<T, long>)
    return LongInteger;
  else if constexpr (std::is_same_v<T, float>)
    return Float;
  else if constexpr (std::is_same_v<T, double>)
    return LongFloat;
  else if constexpr (std::is_same_v<T, bool>)
    return Boolean;
  else if constexpr (std::is_same_v<T, std::string>)
    return String;
  // else if constexpr (std::is_same_v<T, std::chrono::date>) return Date;
  else if constexpr (std::is_same_v<T, lightning::time::DateTime>)
    return DateTime;
  else if constexpr (std::is_same_v<T, Document>)
    return DBDocument;

  NOSQL_FAIL("unknown or unhandled type");
}

}  // namespace neversql
