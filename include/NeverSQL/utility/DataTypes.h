//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#pragma once

#include "NeverSQL/utility/Defines.h"

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
inline DataTypeEnum GetDataTypeEnum<unsigned long>() {
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
