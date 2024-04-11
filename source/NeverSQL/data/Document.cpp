//
// Created by Nathaniel Rupprecht on 3/11/24.
//

#include "NeverSQL/data/Document.h"

using namespace std::string_view_literals;

namespace neversql {

namespace {

std::unique_ptr<DocumentValue> makeDocumentValue(DataTypeEnum data_type) {
  switch (data_type) {
    case DataTypeEnum::Int32:
      return std::make_unique<IntegralValue<int32_t>>();
    case DataTypeEnum::Int64:
      return std::make_unique<IntegralValue<int64_t>>();
    case DataTypeEnum::UInt64:
      return std::make_unique<IntegralValue<uint64_t>>();
    // case DataTypeEnum::Double:
    //   return std::make_unique<IntegralValue<double>>();
    case DataTypeEnum::Boolean:
      return std::make_unique<BooleanValue>();
    // case DataTypeEnum::DateTime:
    //   return std::make_unique<IntegralValue<DateTime>>();
    case DataTypeEnum::String:
      return std::make_unique<StringValue>();
    case DataTypeEnum::Document:
      return std::make_unique<Document>();
    case DataTypeEnum::Array:
      return std::make_unique<ArrayValue>();
    // case DataTypeEnum::BinaryData:
    //   return std::make_unique<BinaryDataValue>();
    default:
      NOSQL_FAIL("unknown data type");
  }
}

}  // namespace

// ===========================================================================================================
//  DocumentValue
// ===========================================================================================================

DocumentValue::DocumentValue(DataTypeEnum type)
    : type_(type) {}

void DocumentValue::WriteToBuffer(lightning::memory::BasicMemoryBuffer<std::byte>& buffer,
                                  bool write_enum) const {
  if (write_enum) {
    // Write the data type enum to the buffer.
    buffer.PushBack(std::bit_cast<std::byte>(type_));
  }
  // Write the data.
  writeData(buffer);
}

void DocumentValue::InitializeFromBuffer(std::span<const std::byte>& buffer) {
  initializeFromBuffer(buffer);
}

std::size_t DocumentValue::CalculateRequiredSize(bool with_enum) const {
  return calculateRequiredDataSize() + (with_enum ? 1 : 0);
}

void DocumentValue::PrintToStream(std::ostream& out, std::size_t indent) const {
  printToStream(out, indent);
}

DataTypeEnum DocumentValue::GetDataType() const noexcept {
  return type_;
}

std::any DocumentValue::GetData() const {
  return getData();
}

// ===========================================================================================================
//  DoubleValue
// ===========================================================================================================

DoubleValue::DoubleValue()
    : DocumentValue(DataTypeEnum::Double) {}

DoubleValue::DoubleValue(double value)
    : DocumentValue(DataTypeEnum::Double)
    , value_(value) {}

void DoubleValue::writeData(lightning::memory::BasicMemoryBuffer<std::byte>& buffer) const {
  // Write the data to the buffer.
  buffer.Append(internal::SpanValue(value_));
}

std::size_t DoubleValue::calculateRequiredDataSize() const {
  return sizeof(double);
}

void DoubleValue::initializeFromBuffer(std::span<const std::byte>& buffer) {
  std::memcpy(&value_, buffer.data(), sizeof(double));
  buffer = buffer.subspan(sizeof(double));
}

void DoubleValue::printToStream(std::ostream& out, [[maybe_unused]] std::size_t indent) const {
  out << value_;
}

// ===========================================================================================================
//  BooleanValue
// ===========================================================================================================

BooleanValue::BooleanValue()
    : DocumentValue(DataTypeEnum::Boolean) {}

BooleanValue::BooleanValue(bool value)
    : DocumentValue(DataTypeEnum::Boolean)
    , value_(value) {}

bool BooleanValue::GetValue() const noexcept {
  return value_;
}

std::any BooleanValue::getData() const {
  return value_;
}

void BooleanValue::writeData(lightning::memory::BasicMemoryBuffer<std::byte>& buffer) const {
  // Write the data to the buffer.
  buffer.Append(internal::SpanValue(value_));
}

std::size_t BooleanValue::calculateRequiredDataSize() const {
  return 1;
}

void BooleanValue::initializeFromBuffer(std::span<const std::byte>& buffer) {
  std::memcpy(&value_, buffer.data(), 1);
  buffer = buffer.subspan(1);
}

void BooleanValue::printToStream(std::ostream& out, [[maybe_unused]] std::size_t indent) const {
  out << (value_ ? "true"sv : "false"sv);
}

// ===========================================================================================================
//  StringValue
// ===========================================================================================================

StringValue::StringValue()
    : DocumentValue(DataTypeEnum::String) {}

StringValue::StringValue(std::string value)
    : DocumentValue(DataTypeEnum::String)
    , value_(std::move(value)) {}

const std::string& StringValue::GetValue() const noexcept {
  return value_;
}

void StringValue::writeData(lightning::memory::BasicMemoryBuffer<std::byte>& buffer) const {
  // Write the string length to the buffer.
  const auto str_length = static_cast<uint32_t>(value_.size());
  buffer.Append(internal::SpanValue(str_length));

  // Write the string data to the buffer.
  buffer.Append(internal::SpanValue(value_));
}

std::size_t StringValue::calculateRequiredDataSize() const {
  return sizeof(uint32_t) + value_.size();
}

void StringValue::initializeFromBuffer(std::span<const std::byte>& buffer) {
  // Read the string length.
  uint32_t str_length {};
  std::memcpy(&str_length, buffer.data(), sizeof(str_length));
  buffer = buffer.subspan(sizeof(str_length));  // Shrink.

  // Read the string data.
  value_ = std::string(reinterpret_cast<const char*>(buffer.data()), str_length);
  buffer = buffer.subspan(str_length);  // Shrink.
}

void StringValue::printToStream(std::ostream& out, [[maybe_unused]] std::size_t indent) const {
  out << lightning::formatting::Format("{:?}", value_);
}

// ===========================================================================================================
//  ArrayValue
// ===========================================================================================================

ArrayValue::ArrayValue()
    : DocumentValue(DataTypeEnum::Array)
    , element_type_(DataTypeEnum::Null) {}

ArrayValue::ArrayValue(DataTypeEnum element_type)
    : DocumentValue(DataTypeEnum::Array)
    , element_type_(element_type) {}

void ArrayValue::AddElement(std::unique_ptr<DocumentValue>&& value) {
  values_.emplace_back(std::move(value));
}

const DocumentValue& ArrayValue::GetElement(std::size_t index) const {
  NOSQL_REQUIRE(index < values_.size(), "index " << index << " out of range");
  return *values_[index];
}

void ArrayValue::writeData(lightning::memory::BasicMemoryBuffer<std::byte>& buffer) const {
  // Write the element type to the buffer
  buffer.PushBack(std::bit_cast<std::byte>(element_type_));

  // Write the array size to the buffer.
  const auto num_elements = static_cast<uint32_t>(values_.size());
  buffer.Append(internal::SpanValue(num_elements));

  // Write the data to the buffer.
  for (auto const& value : values_) {
    value->WriteToBuffer(buffer, false);
  }
}

std::size_t ArrayValue::calculateRequiredDataSize() const {
  return sizeof(DataTypeEnum) + sizeof(uint32_t)
      + static_cast<std::size_t>(
             std::accumulate(values_.begin(), values_.end(), 0, [](std::size_t acc, const auto& value) {
               return acc + value->CalculateRequiredSize(false);
             }));
}

void ArrayValue::initializeFromBuffer(std::span<const std::byte>& buffer) {
  // Read the element type.
  std::memcpy(&element_type_, buffer.data(), 1);
  buffer = buffer.subspan(1);  // Shrink.

  // Get the number of elements in the array.
  uint32_t num_elements {};
  std::memcpy(&num_elements, buffer.data(), 4);
  buffer = buffer.subspan(4);  // Shrink.

  for (std::size_t i = 0; i < num_elements; ++i) {
    auto value = makeDocumentValue(element_type_);
    value->InitializeFromBuffer(buffer);
    values_.emplace_back(std::move(value));
  }
}

void ArrayValue::printToStream(std::ostream& out, std::size_t indent) const {
  out << "[\n";
  for (const auto& value : values_) {
    std::ranges::fill_n(std::ostream_iterator<char>(out), static_cast<long>(indent) + 2, ' ');
    value->PrintToStream(out);
    out << ",\n";
  }
  std::ranges::fill_n(std::ostream_iterator<char>(out), static_cast<long>(indent), ' ');
  out << "]";
}

// ===========================================================================================================
//  Document
// ===========================================================================================================

Document::Document()
    : DocumentValue(DataTypeEnum::Document) {}

void Document::AddElement(const std::string& name, std::unique_ptr<DocumentValue> value) {
  elements_.emplace_back(name, std::move(value));
}

std::optional<std::reference_wrapper<const DocumentValue>> Document::GetElement(std::string_view name) const {
  auto pred = [name](const auto& element) { return element.first == name; };
  auto it = std::ranges::find_if(elements_, pred);
  if (it == elements_.end()) {
    return {};
  }
  return std::optional {std::cref(*it->second)};
}

std::size_t Document::GetNumFields() const noexcept {
  return elements_.size();
}

std::string_view Document::GetFieldName(std::size_t index) const {
  NOSQL_ASSERT(index < elements_.size(), "index " << index << " out of range");
  return elements_[index].first;
}

DataTypeEnum Document::GetFieldType(std::size_t index) const {
  NOSQL_ASSERT(index < elements_.size(), "index " << index << " out of range");
  return elements_[index].second->GetDataType();
}

void Document::writeData(lightning::memory::BasicMemoryBuffer<std::byte>& buffer) const {
  // Write the number of fields in the document to the buffer.
  const auto num_elements = elements_.size();
  buffer.Append(internal::SpanValue(num_elements));

  // Write the data to the buffer.
  for (const auto& value : elements_) {
    // Write the field name to the buffer.
    auto name_length = static_cast<uint16_t>(value.first.size());
    // String: string length, then string data.
    buffer.Append(internal::SpanValue(name_length));
    buffer.Append(internal::SpanValue(value.first));

    // Write the field value to the buffer.
    value.second->WriteToBuffer(buffer);
  }
}

std::size_t Document::calculateRequiredDataSize() const {
  auto size = sizeof(uint64_t);  // Number of elements.
  for (const auto& [name, value] : elements_) {
    size += sizeof(uint16_t) + name.size() + value->CalculateRequiredSize();
  }
  return size;
}

void Document::initializeFromBuffer(std::span<const std::byte>& buffer) {
  // Read the number of elements in the document.
  uint64_t num_elements {};
  std::memcpy(&num_elements, buffer.data(), 8);
  buffer = buffer.subspan(8);  // Shrink.

  for (std::size_t i = 0; i < num_elements; ++i) {
    // Read the length of the field name.
    uint16_t name_size {};
    std::memcpy(&name_size, buffer.data(), 2);
    buffer = buffer.subspan(2);  // Shrink.

    // Read the field name.
    std::string field_name(reinterpret_cast<const char*>(buffer.data()), name_size);
    buffer = buffer.subspan(name_size);  // Shrink.

    // Read the type of the field.
    DataTypeEnum type;
    std::memcpy(&type, buffer.data(), 1);
    buffer = buffer.subspan(1);  // Shrink.

    // Read the data.
    auto value = makeDocumentValue(type);
    value->InitializeFromBuffer(buffer);

    // Add the field to the document.
    elements_.emplace_back(field_name, std::move(value));
  }
}

void Document::printToStream(std::ostream& out, std::size_t indent) const {
  out << "{\n";
  for (const auto& [name, value] : elements_) {
    std::ranges::fill_n(std::ostream_iterator<char>(out), static_cast<long>(indent) + 2, ' ');
    out << lightning::formatting::Format("{:?}", name) << ": ";
    value->PrintToStream(out, indent + 2);
    out << ",\n";
  }
  std::ranges::fill_n(std::ostream_iterator<char>(out), static_cast<long>(indent), ' ');
  out << "}";
}

// ===========================================================================================================
//  Free functions.
// ===========================================================================================================

std::unique_ptr<DocumentValue> ReadFromBuffer(std::span<const std::byte> buffer) {
  // Read the enum.
  DataTypeEnum enum_value;
  std::memcpy(&enum_value, buffer.data(), 1);
  buffer = buffer.subspan(1);  // Shrink.

  auto document_value = makeDocumentValue(enum_value);
  document_value->InitializeFromBuffer(buffer);
  return document_value;
}

std::unique_ptr<Document> ReadDocumentFromBuffer(std::span<const std::byte> buffer, bool expect_enum) {
  if (buffer.empty()) {
    return {};
  }
  if (expect_enum) {
    // Read the enum.
    DataTypeEnum enum_value;
    std::memcpy(&enum_value, buffer.data(), 1);
    buffer = buffer.subspan(1);  // Shrink.
    NOSQL_ASSERT(enum_value == DataTypeEnum::Document,
                 "expected DataTypeEnum::Document, value is " << static_cast<int8_t>(enum_value));
  }
  auto document = std::make_unique<Document>();
  document->InitializeFromBuffer(buffer);
  return document;
}

void PrettyPrint(const Document& document, std::ostream& out) {
  document.PrintToStream(out, 0);
}

std::string PrettyPrint(const Document& document) {
  std::ostringstream out;
  PrettyPrint(document, out);
  return out.str();
}

}  // namespace neversql
