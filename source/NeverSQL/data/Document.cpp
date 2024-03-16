//
// Created by Nathaniel Rupprecht on 3/11/24.
//

#include "NeverSQL/data/Document.h"

namespace neversql {

std::size_t DocumentBuilder::CalculateRequiredSize() const {
  std::size_t size = 0;
  for (const auto& field : fields_) {
    // Length of the field name.
    size += 2;
    // Field name.
    size += field.name.size();
    // Type of the field.
    size += 1;
    // Data. How we do this is type dependent.
    std::visit(
        [&](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          // Trivially copyable types.
          if constexpr (std::is_same_v<T, int> || std::is_same_v<T, bool> || std::is_same_v<T, double>) {
            size += sizeof(arg);
          }
          // String has to store its size also.
          else if constexpr (std::is_same_v<T, std::string>) {
            size += sizeof(uint32_t);
            size += arg.size();
          }
        },
        field.data);
  }
  return size;
}

DocumentReader::DocumentReader(std::span<const std::byte> buffer)
    : buffer_(buffer) {
  initialize();
}

std::size_t DocumentReader::GetNumFields() const {
  return fields_.size();
}

std::string_view DocumentReader::GetFieldName(std::size_t index) const {
  NOSQL_REQUIRE(index < fields_.size(), "index " << index << " out of range");
  return fields_[index].field_name;
}

DataTypeEnum DocumentReader::GetFieldType(std::size_t index) const {
  NOSQL_REQUIRE(index < fields_.size(), "index " << index << " out of range");
  return fields_[index].type;
}

void DocumentReader::initialize() {
  // Read the fields.
  auto buffer = buffer_;
  while (!buffer.empty()) {
    FieldDescriptor field_descriptor;

    // Read the length of the field name. Two bytes.
    uint16_t name_size;
    std::memcpy(&name_size, buffer.data(), 2);
    NOSQL_ASSERT(name_size < buffer.size() - 2, "field name is impossibly long (" << name_size << " bytes)");

    buffer = buffer.subspan(2);  // Shrink.
    // Read the field name.
    field_descriptor.field_name = std::string_view(reinterpret_cast<const char*>(buffer.data()), name_size);
    buffer = buffer.subspan(name_size);  // Shrink.
    // Read the type of the field. One byte.
    uint8_t type;
    std::memcpy(&type, buffer.data(), 1);
    buffer = buffer.subspan(1);  // Shrink.
    field_descriptor.type = static_cast<DataTypeEnum>(type);
    // Read the data. How we do this is type dependent.
    switch (field_descriptor.type) {
      case DataTypeEnum::Int32: {
        field_descriptor.data = buffer.subspan(0, sizeof(int));
        buffer = buffer.subspan(sizeof(int));  // Shrink.
        break;
      }

      case DataTypeEnum::Double: {
        field_descriptor.data = buffer.subspan(0, sizeof(double));
        buffer = buffer.subspan(sizeof(double));  // Shrink.
        break;
      }
      case DataTypeEnum::Boolean: {
        field_descriptor.data = buffer.subspan(0, sizeof(bool));
        buffer = buffer.subspan(sizeof(bool));  // Shrink
        break;
      }
      case DataTypeEnum::DateTime: {
        field_descriptor.data = buffer.subspan(0, sizeof(DateTime));
        buffer = buffer.subspan(sizeof(DateTime));  // Shrink
        break;
      }
      case DataTypeEnum::String: {
        uint32_t str_size;
        std::memcpy(&str_size, buffer.data(), sizeof(str_size));
        buffer = buffer.subspan(sizeof(str_size));  // Shrink.
        field_descriptor.data = buffer.subspan(0, str_size);
        buffer = buffer.subspan(str_size);  // Shrink.
        break;
      }
      case DataTypeEnum::Document:
        break;
      case DataTypeEnum::Array:
        break;
      case DataTypeEnum::Binary:
        break;
      case DataTypeEnum::Int64:
        break;

      default:
        NOSQL_FAIL(lightning::formatting::Format("unknown data type, uint8_t value was {}", type));

    }

    fields_.emplace_back(std::move(field_descriptor));
  }
}

void WriteToBuffer(lightning::memory::BasicMemoryBuffer<std::byte>& buffer, const DocumentBuilder& document) {
  for (const auto& field : document.fields_) {
    // Write the length of the field name. Two bytes.
    auto name_size = static_cast<uint16_t>(field.name.size());
    const auto* begin = reinterpret_cast<const std::byte*>(&name_size);
    buffer.Append(begin, begin + 2);
    // Write the field name.
    buffer.Append(reinterpret_cast<const std::byte*>(field.name.data()),
                  reinterpret_cast<const std::byte*>(field.name.data() + field.name.size()));
    // Write the type of the field. One byte.
    auto type = static_cast<uint8_t>(field.type);
    buffer.Append(reinterpret_cast<const std::byte*>(&type), reinterpret_cast<const std::byte*>(&type) + 1);
    // Write the data. How we do this is type dependent.
    std::visit(
        [&](auto&& arg) {
          using T = std::decay_t<decltype(arg)>;
          // Trivially copyable types.
          if constexpr (std::is_same_v<T, int> || std::is_same_v<T, bool> || std::is_same_v<T, double>) {
            const auto* ptr = reinterpret_cast<const std::byte*>(&arg);
            buffer.Append(ptr, ptr + sizeof(arg));
          }
          // String has to store its size also.
          else if constexpr (std::is_same_v<T, std::string>) {
            // First, write string size.
            auto str_size = static_cast<uint32_t>(arg.size());
            const auto* ptr = reinterpret_cast<const std::byte*>(&str_size);
            buffer.Append(ptr, ptr + sizeof(str_size));
            // Then write the string itself.
            const auto* start_ptr = reinterpret_cast<const std::byte*>(arg.data());
            buffer.Append(start_ptr, start_ptr + str_size);
          }
        },
        field.data);
  }
}

void PrettyPrint(const DocumentReader& reader, std::ostream& out) {
  for (std::size_t i = 0; i < reader.GetNumFields(); ++i) {
    out << reader.GetFieldName(i) << ": ";
    switch (reader.GetFieldType(i)) {
      case DataTypeEnum::Int32: {
        out << reader.GetEntryAs<int32_t>(i);
        break;
      }
      case DataTypeEnum::Double: {
        out << reader.GetEntryAs<double>(i);
        break;
      }
      case DataTypeEnum::Boolean: {
        out << (reader.GetEntryAs<bool>(i) ? "true" : "false");
        break;
      }
      case DataTypeEnum::String: {
        out << lightning::formatting::Format("{:?}", reader.GetEntryAs<std::string>(i));
        break;
      }
      case DataTypeEnum::DateTime: {
        out << reader.GetEntryAs<DateTime>(i);
        break;
      }
      default:
        NOSQL_FAIL("unknown data type");
      case DataTypeEnum::Document:
        out << "<Subdocument (TODO)>";
        break;
      case DataTypeEnum::Array:
        out << "<Array (TODO)>";
        break;
      case DataTypeEnum::Binary:
        out << "<Binary>";
        break;
      case DataTypeEnum::Int64:
        out << reader.GetEntryAs<int64_t>(i);
        break;
    }
    out << std::endl;
  }
}

std::string PrettyPrint(const DocumentReader& reader) {
  std::ostringstream out;
  PrettyPrint(reader, out);
  return out.str();
}

}  // namespace neversql