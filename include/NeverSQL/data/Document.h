//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#pragma once

#include <string>

#include "NeverSQL/utility/DataTypes.h"

namespace neversql {

//! \brief A representation of a document, which is a collection of keys and typed data fields.
class Document {
 private:
  struct Field {
    std::string name;
    std::size_t data_start;
    DataTypeEnum type;
  };

 public:
  explicit Document(primary_key_t pk) noexcept : primary_key(pk) {}

  primary_key_t primary_key;
  std::vector<Field> fields;
  std::vector<unsigned char> compressed_data;

  NO_DISCARD std::size_t GetNumFields() const { return fields.size(); }

  template <typename T>
  void AddEntry(const std::string& name, T&& data) {
    auto type = GetDataTypeEnum<T>();
    NOSQL_ASSERT(type != DataTypeEnum::DBDocument,
                 "cannot add a document to a document right now");

    // Add the data.
    std::size_t data_start;
    if constexpr (std::is_same_v<T, Document>) {
      // Add the primary key.
      data_start = addBytes(std::forward<T>(data.primary_key));
    }
    else {
      data_start = addBytes(std::forward<T>(data));
    }
    // Add field descriptor.
    fields.push_back({.name = name, .data_start = data_start, .type = type});
  }

  template <typename T>
  NO_DISCARD T GetEntryAs(const std::string& name) const {
    auto it = std::ranges::find_if(
        fields, [&name](const auto& field) { return field.name == name; });

    NOSQL_ASSERT(it != fields.end(), "field not found");
    NOSQL_ASSERT(it->type == GetDataTypeEnum<T>(), "type mismatch");

    if constexpr (std::is_trivially_copyable_v<T>) {
      T value;
      std::memcpy(&value, compressed_data.data() + it->data_start, sizeof(T));
      return value;
    }
    else {
      NOSQL_FAIL("unhandled type");
    }
  }

 private:
  template <typename T>
  std::size_t addBytes(T&& data) {
    auto current_size = compressed_data.size();
    compressed_data.resize(current_size + sizeof(T));
    std::memcpy(compressed_data.data() + current_size, &data, sizeof(T));
    return current_size;
  }
};

}  // namespace neversql