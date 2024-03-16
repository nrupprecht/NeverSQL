//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#pragma once

#include "NeverSQL/utility/Defines.h"

namespace neversql {

enum class DataTypeEnum : int8_t {
  Double = 1,
  String = 2,
  Document = 3,
  Array = 4,
  Binary = 5,
  // ObjectId = 7,
  Boolean = 8,
  DateTime = 9,
  // Null = 10,
  // RegularExpression = 11,

  Int32 = 16,
  // Timestamp = 17,
  Int64 = 18,
  // Decimal128 = 19,

  // MinKey = -1,
  // MaxKey = 127,

  // My own additions
  UInt64 = 20
};

class Document;

namespace detail {

template<typename T>
DataTypeEnum GetDataTypeEnum();

template<>
inline DataTypeEnum GetDataTypeEnum<int32_t>() {
  return DataTypeEnum::Int32;
}

template<>
inline DataTypeEnum GetDataTypeEnum<double>() {
  return DataTypeEnum::Double;
}

template<>
inline DataTypeEnum GetDataTypeEnum<bool>() {
  return DataTypeEnum::Boolean;
}

template<>
inline DataTypeEnum GetDataTypeEnum<std::string>() {
  return DataTypeEnum::String;
}

template<>
inline DataTypeEnum GetDataTypeEnum<lightning::time::DateTime>() {
  return DataTypeEnum::DateTime;
}

template<>
inline DataTypeEnum GetDataTypeEnum<int64_t>() {
  return DataTypeEnum::Int64;
}

template<>
inline DataTypeEnum GetDataTypeEnum<uint64_t>() {
  return DataTypeEnum::UInt64;
}

}  // namespace detail

template <typename T>
DataTypeEnum GetDataTypeEnum() {
  return detail::GetDataTypeEnum<std::remove_cvref_t<T>>();
}

}  // namespace neversql
