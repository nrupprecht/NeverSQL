//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#pragma once

#include <span>
#include <string>

#include "NeverSQL/utility/DataTypes.h"

namespace neversql {

//! \brief A representation of a document, which is a collection of keys and typed data fields.
class DocumentBuilder {
private:
  struct Field {
    std::string name;
    DataTypeEnum type;
    std::variant<int, double, bool, std::string> data;
  };

  std::vector<Field> fields_;

  friend void WriteToBuffer(lightning::memory::BasicMemoryBuffer<std::byte>& buffer,
                            const DocumentBuilder& document);

public:
  DocumentBuilder() = default;

  //! \brief Get the number of fields in the document.
  NO_DISCARD std::size_t GetNumFields() const { return fields_.size(); }

  //! \brief Calculate the amount of memory required to store the document.
  std::size_t CalculateRequiredSize() const;

  template<typename T>
  void AddEntry(const std::string& name, T&& data) {
    fields_.emplace_back(Field {.name = name, .type = GetDataTypeEnum<T>(), .data = std::forward<T>(data)});
  }

  template<typename T>
  NO_DISCARD const T& GetEntryAs(const std::string& name) const {
    auto it = std::ranges::find_if(fields_, [&name](const auto& field) { return field.name == name; });
    NOSQL_ASSERT(it != fields_.end(), "field '" << name << "' not found");
    NOSQL_ASSERT(it->type == GetDataTypeEnum<T>(), "type mismatch");
    return std::get<T>(it->data);
  }
};

class DocumentReader {
public:
  explicit DocumentReader(std::span<const std::byte> buffer);

  std::size_t GetNumFields() const;

  std::string_view GetFieldName(std::size_t index) const;

  DataTypeEnum GetFieldType(std::size_t index) const;

  template<typename T>
  NO_DISCARD T GetEntryAs(std::size_t index) const {
    NOSQL_REQUIRE(index < fields_.size(), "index " << index << " out of range");
    NOSQL_REQUIRE(fields_[index].type == GetDataTypeEnum<T>(), "type mismatch");
    if constexpr (!std::is_same_v<T, std::string>) {
      T out;
      std::memcpy(&out, fields_[index].data.data(), sizeof(T));
      return out;
    }
    else {
      return std::string(reinterpret_cast<const char*>(fields_[index].data.data()),
                         fields_[index].data.size());
    }
  }

private:
  //! \brief Scan the data and initialize the field descriptors.
  void initialize();

  struct FieldDescriptor {
    std::string_view field_name;
    DataTypeEnum type;
    std::span<const std::byte> data;
  };

  const std::span<const std::byte> buffer_;

  std::vector<FieldDescriptor> fields_;
};

//! \brief Serialize a document to a memory buffer.
void WriteToBuffer(lightning::memory::BasicMemoryBuffer<std::byte>& buffer, const DocumentBuilder& document);

//! \brief Pretty print the contents of a document to a stream.
void PrettyPrint(const DocumentReader& reader, std::ostream& out);

//! \brief Pretty print the contents of a document to a string.
std::string PrettyPrint(const DocumentReader& reader);

}  // namespace neversql