//
// Created by Nathaniel Rupprecht on 2/18/24.
//

#include <gtest/gtest.h>

#include "NeverSQL/data/Document.h"

using namespace std::string_literals;
using namespace std::string_view_literals;

using namespace neversql;

namespace testing {

TEST(Document, Single_Integer) {
  Document document;
  document.AddElement("Age", IntegralValue {42});
  ASSERT_EQ(document.GetNumFields(), 1);

  // [DataTypeEnum: 1 byte]
  // [Num elements: 8 bytes]
  // [Field Name Length: 2 bytes]
  // [Field Name: 3 bytes]
  // -> (element)
  // [DataTypeEnum: 1 byte] [Data: 4 bytes]
  // Total: 19 bytes

  lightning::memory::MemoryBuffer<std::byte> buffer;

  WriteToBuffer(buffer, document);
  std::span written_data(buffer.Data(), buffer.Size());
  EXPECT_EQ(written_data.size(), 19);
  EXPECT_EQ(document.CalculateRequiredSize(), 19);

  auto read_document = ReadDocumentFromBuffer(written_data);

  ASSERT_EQ(read_document->GetNumFields(), 1);
  EXPECT_EQ(read_document->GetFieldName(0), "Age"sv);
  EXPECT_EQ(read_document->GetFieldType(0), DataTypeEnum::Int32);
  EXPECT_EQ(read_document->TryGetAs<int32_t>("Age").value(), 42);
}

TEST(Document, Array) {
  Document document;

  ArrayValue array(DataTypeEnum::Int32);
  array.AddElement(IntegralValue {1});
  array.AddElement(IntegralValue {3});
  array.AddElement(IntegralValue {5});
  array.AddElement(IntegralValue {7});
  array.AddElement(IntegralValue {9});

  document.AddElement("elements", std::move(array));

  // Checks

  lightning::memory::MemoryBuffer<std::byte> buffer;
  WriteToBuffer(buffer, document);
  std::span written_data(buffer.Data(), buffer.Size());
  EXPECT_EQ(written_data.size(), 45);
  EXPECT_EQ(document.CalculateRequiredSize(), 45);

  auto read_document = ReadDocumentFromBuffer(written_data);
  ASSERT_EQ(read_document->GetNumFields(), 1);
  EXPECT_EQ(read_document->GetFieldName(0), "elements"sv);
  EXPECT_EQ(read_document->GetFieldType(0), DataTypeEnum::Array);
}

TEST(Document, NestedDocument) {}

TEST(Document, Basic) {
  Document document;
  document.AddElement("Age", IntegralValue {42});
  document.AddElement("Birthday", StringValue {"My business"});
  document.AddElement("IsAlive", IntegralValue {true});

  ASSERT_EQ(document.GetNumFields(), 3);

  lightning::memory::MemoryBuffer<std::byte> buffer;

  WriteToBuffer(buffer, document);
  std::span written_data(buffer.Data(), buffer.Size());
  EXPECT_EQ(written_data.size(), 56);
  EXPECT_EQ(document.CalculateRequiredSize(), 56);

  auto read_document = ReadDocumentFromBuffer(written_data);
  ASSERT_EQ(read_document->GetNumFields(), 3);
  EXPECT_EQ(read_document->GetFieldName(0), "Age"sv);
  EXPECT_EQ(read_document->GetFieldName(1), "Birthday"sv);
  EXPECT_EQ(read_document->GetFieldName(2), "IsAlive"sv);
  EXPECT_ANY_THROW(read_document->GetFieldName(3));

  EXPECT_EQ(read_document->GetFieldType(0), DataTypeEnum::Int32);
  EXPECT_EQ(read_document->GetFieldType(1), DataTypeEnum::String);
  EXPECT_EQ(read_document->GetFieldType(2), DataTypeEnum::Boolean);
  EXPECT_ANY_THROW(read_document->GetFieldType(3));

  EXPECT_EQ(read_document->TryGetAs<int>(0).value(), 42);
  EXPECT_EQ(read_document->TryGetAs<std::string>(1).value(), "My business");
  EXPECT_EQ(read_document->TryGetAs<bool>(2).value(), true);
  EXPECT_FALSE(read_document->TryGetAs<int>(1));
  EXPECT_FALSE(read_document->TryGetAs<std::string>(0));
  EXPECT_FALSE(read_document->TryGetAs<bool>(0));
}

TEST(Document, Strings) {
  Document document;
  document.AddElement("A-string", StringValue {"Hello"});
  document.AddElement("B-string", StringValue {"There"});
  document.AddElement("C-string", StringValue {"World"});

  lightning::memory::MemoryBuffer<std::byte> buffer;

  WriteToBuffer(buffer, document);
  std::span written_data(buffer.Data(), buffer.Size());
  EXPECT_EQ(written_data.size(), 69);
  EXPECT_EQ(document.CalculateRequiredSize(), 69);

  auto read_document = ReadDocumentFromBuffer(written_data);
  ASSERT_EQ(read_document->GetNumFields(), 3);
  EXPECT_EQ(read_document->GetFieldName(0), "A-string"sv);
  EXPECT_EQ(read_document->GetFieldName(1), "B-string"sv);
  EXPECT_EQ(read_document->GetFieldName(2), "C-string"sv);
  EXPECT_ANY_THROW(read_document->GetFieldName(3));

  EXPECT_EQ(read_document->GetFieldType(0), DataTypeEnum::String);
  EXPECT_EQ(read_document->GetFieldType(1), DataTypeEnum::String);
  EXPECT_EQ(read_document->GetFieldType(2), DataTypeEnum::String);
  EXPECT_ANY_THROW(read_document->GetFieldType(3));

  EXPECT_EQ(read_document->TryGetAs<std::string>(0).value(), "Hello");
  EXPECT_EQ(read_document->TryGetAs<std::string>(1).value(), "There");
  EXPECT_EQ(read_document->TryGetAs<std::string>(2).value(), "World");
}

}  // namespace testing