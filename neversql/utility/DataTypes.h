//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#pragma once

#include "neversql/utility/Defines.h"

namespace neversql {

enum class DataTypeEnum : int8_t {
  Null = 0,
  Double = 1,
  String = 2,
  Document = 3,
  Array = 4,
  BinaryData = 5,
  Boolean = 6,
  DateTime = 7,
  Int32 = 8,
  Int64 = 9,
  UInt64 = 10
};

inline std::string to_string(DataTypeEnum value) {
  // clang-format off
  using enum DataTypeEnum;
  switch (value) {
    case Null: return "Null";
    case Double: return "Double";
    case String: return "String";
    case Document: return "Document";
    case Array: return "Array";
    case BinaryData: return "BinaryData";
    case Boolean: return "Boolean";
    case DateTime: return "DateTime";
    case Int32: return "Int32";
    case Int64: return "Int64";
    case UInt64: return "UInt64";
    default: return lightning::formatting::Format("<unknown, value = {}>", static_cast<int>(value));
  }
  // clang-format on
}

class Document;

namespace detail {

template<typename T>
DataTypeEnum GetDataTypeEnum();

template<>
inline DataTypeEnum GetDataTypeEnum<double>() {
  return DataTypeEnum::Double;
}

template<>
inline DataTypeEnum GetDataTypeEnum<std::string>() {
  return DataTypeEnum::String;
}

// Document

// Array

// Binary data

template<>
inline DataTypeEnum GetDataTypeEnum<bool>() {
  return DataTypeEnum::Boolean;
}

template<>
inline DataTypeEnum GetDataTypeEnum<lightning::time::DateTime>() {
  return DataTypeEnum::DateTime;
}

template<>
inline DataTypeEnum GetDataTypeEnum<int32_t>() {
  return DataTypeEnum::Int32;
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

template<typename Data_t>
DataTypeEnum GetDataTypeEnum() {
  return detail::GetDataTypeEnum<std::remove_cvref_t<Data_t>>();
}

}  // namespace neversql
