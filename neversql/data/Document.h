//
// Created by Nathaniel Rupprecht on 2/17/24.
//

#pragma once

#include <any>
#include <bit>
#include <numeric>
#include <span>
#include <string>
#include <utility>

#include <unistd.h>

#include "neversql/utility/DataTypes.h"
#include "internals/Utility.h"

namespace neversql {

//! \brief Base class for values that can be stored in documents (include documents themselves).
class DocumentValue {
public:
  explicit DocumentValue(DataTypeEnum type);

  virtual ~DocumentValue() = default;

  void WriteToBuffer(lightning::memory::BasicMemoryBuffer<std::byte>& buffer, bool write_enum = true) const;

  void InitializeFromBuffer(std::span<const std::byte>& buffer);

  std::size_t CalculateRequiredSize(bool with_enum = true) const;

  void PrintToStream(std::ostream& out, std::size_t indent = 0) const;

  DataTypeEnum GetDataType() const noexcept;

  std::any GetData() const;

  template<typename DataType_t>
  std::optional<DataType_t> TryGetAs() const {
    if (type_ != GetDataTypeEnum<DataType_t>()) {
      return std::nullopt;
    }
    auto data = GetData();
    return std::any_cast<DataType_t>(data);
  }

protected:
  virtual std::any getData() const = 0;

  //! \brief Write only the data (not the data type enum) to the buffer.
  virtual void writeData(lightning::memory::BasicMemoryBuffer<std::byte>& buffer) const = 0;
  //! \brief Calculate the size required by the writeData function.
  virtual std::size_t calculateRequiredDataSize() const = 0;
  //! \brief Initialize the document value from a data representation in a buffer.
  virtual void initializeFromBuffer(std::span<const std::byte>& buffer) = 0;

  virtual void printToStream(std::ostream& out, std::size_t indent) const = 0;

  DataTypeEnum type_;
};

class DoubleValue final : public DocumentValue {
public:
  DoubleValue();
  explicit DoubleValue(double value);

  double GetValue() const noexcept { return value_; }

private:
  std::any getData() const override { return value_; }
  void writeData(lightning::memory::BasicMemoryBuffer<std::byte>& buffer) const override;
  std::size_t calculateRequiredDataSize() const override;
  void initializeFromBuffer(std::span<const std::byte>& buffer) override;
  void printToStream(std::ostream& out, std::size_t indent) const override;

  double value_ {};
};

template<std::integral Integral_t>
class IntegralValue final : public DocumentValue {
public:
  IntegralValue()
      : DocumentValue(GetDataTypeEnum<Integral_t>()) {}

  explicit IntegralValue(Integral_t value)
      : DocumentValue(GetDataTypeEnum<Integral_t>())
      , value_(value) {}

  Integral_t GetValue() const noexcept { return value_; }

private:
  std::any getData() const override { return value_; }

  void writeData(lightning::memory::BasicMemoryBuffer<std::byte>& buffer) const override {
    // Write the data to the buffer.
    buffer.Append(internal::SpanValue(value_));
  }

  std::size_t calculateRequiredDataSize() const override { return sizeof(Integral_t); }

  void initializeFromBuffer(std::span<const std::byte>& buffer) override {
    std::memcpy(&value_, buffer.data(), sizeof(Integral_t));
    buffer = buffer.subspan(sizeof(Integral_t));
  }

  void printToStream(std::ostream& out, [[maybe_unused]] std::size_t indent) const override { out << value_; }

  Integral_t value_ {};
};

class BooleanValue final : public DocumentValue {
public:
  BooleanValue();
  explicit BooleanValue(bool value);

  bool GetValue() const noexcept;

private:
  std::any getData() const override;

  void writeData(lightning::memory::BasicMemoryBuffer<std::byte>& buffer) const override;
  std::size_t calculateRequiredDataSize() const override;
  void initializeFromBuffer(std::span<const std::byte>& buffer) override;
  void printToStream(std::ostream& out, std::size_t indent) const override;

  bool value_ {};
};

//! \brief Document value representing a string.
class StringValue final : public DocumentValue {
public:
  StringValue();
  explicit StringValue(std::string value);

  const std::string& GetValue() const noexcept;

private:
  std::any getData() const override { return value_; }

  void writeData(lightning::memory::BasicMemoryBuffer<std::byte>& buffer) const override;
  std::size_t calculateRequiredDataSize() const override;
  void initializeFromBuffer(std::span<const std::byte>& buffer) override;
  void printToStream(std::ostream& out, std::size_t indent) const override;

  std::string value_;
};

class ArrayValue final : public DocumentValue {
public:
  ArrayValue();
  explicit ArrayValue(DataTypeEnum element_type);

  void AddElement(std::unique_ptr<DocumentValue>&& value);

  template<typename DocValue_t>
    requires std::is_base_of_v<DocumentValue, DocValue_t>
  void AddElement(DocValue_t&& value) {
    values_.emplace_back(std::make_unique<DocValue_t>(std::forward<DocValue_t>(value)));
  }

  const DocumentValue& GetElement(std::size_t index) const;

private:
  std::any getData() const override { NOSQL_FAIL("ArrayValue has no GetData"); }

  void writeData(lightning::memory::BasicMemoryBuffer<std::byte>& buffer) const override;
  std::size_t calculateRequiredDataSize() const override;
  void initializeFromBuffer(std::span<const std::byte>& buffer) override;
  void printToStream(std::ostream& out, std::size_t indent) const override;

  DataTypeEnum element_type_;

  std::vector<std::unique_ptr<DocumentValue>> values_;
};

//! \brief Document value representing a document.
class Document final : public DocumentValue {
public:
  Document();

  void AddElement(const std::string& name, std::unique_ptr<DocumentValue> value);

  template<typename DocValue_t>
    requires std::is_base_of_v<DocumentValue, DocValue_t>
  void AddElement(const std::string& name, DocValue_t&& value) {
    elements_.emplace_back(name, std::make_unique<DocValue_t>(std::forward<DocValue_t>(value)));
  }

  std::optional<std::reference_wrapper<const DocumentValue>> GetElement(std::string_view name) const;

  std::size_t GetNumFields() const noexcept;

  template<typename DataType_t>
  std::optional<DataType_t> TryGetAs(std::string_view field_name) const {
    if (auto element = GetElement(field_name)) {
      return element->get().TryGetAs<DataType_t>();
    }
    return std::nullopt;
  }

  template<typename DataType_t>
  std::optional<DataType_t> TryGetAs(std::size_t index) const {
    if (elements_.size() <= index) {
      return {};
    }
    return elements_[index].second->TryGetAs<DataType_t>();
  }

  std::string_view GetFieldName(std::size_t index) const;

  DataTypeEnum GetFieldType(std::size_t index) const;

protected:
  std::any getData() const override { NOSQL_FAIL("ArrayValue has no GetData"); }
  void writeData(lightning::memory::BasicMemoryBuffer<std::byte>& buffer) const override;
  std::size_t calculateRequiredDataSize() const override;
  void initializeFromBuffer(std::span<const std::byte>& buffer) override;
  void printToStream(std::ostream& out, std::size_t indent) const override;

  std::vector<std::pair<std::string, std::unique_ptr<DocumentValue>>> elements_;
};

inline void WriteToBuffer(lightning::memory::BasicMemoryBuffer<std::byte>& buffer, const Document& document) {
  document.WriteToBuffer(buffer);
}

//! \brief Read a document value from a buffer.
std::unique_ptr<DocumentValue> ReadFromBuffer(std::span<const std::byte> buffer);

//! \brief Read a document from a buffer.
std::unique_ptr<Document> ReadDocumentFromBuffer(std::span<const std::byte> buffer, bool expect_enum = true);

void PrettyPrint(const Document& document, std::ostream& out);
std::string PrettyPrint(const Document& document);

}  // namespace neversql