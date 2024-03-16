//
// Created by Nathaniel Rupprecht on 2/18/24.
//

#include <gtest/gtest.h>

#include "NeverSQL/data/Document.h"

using namespace std::string_literals;
using namespace std::string_view_literals;

namespace testing {

TEST(Document, Basic) {
  neversql::DocumentBuilder document;
  document.AddEntry("Age", 42);
  document.AddEntry("Birthday", "My business"s);
  document.AddEntry("IsAlive", true);

  ASSERT_EQ(document.GetNumFields(), 3);

  lightning::memory::MemoryBuffer<std::byte> buffer;

  WriteToBuffer(buffer, document);
  std::span written_data(buffer.Data(), buffer.Size());
  EXPECT_EQ(written_data.size(), 47);
  EXPECT_EQ(document.CalculateRequiredSize(), 47);

  neversql::DocumentReader reader(written_data);
  ASSERT_EQ(reader.GetNumFields(), 3);
  EXPECT_EQ(reader.GetFieldName(0), "Age"sv);
  EXPECT_EQ(reader.GetFieldName(1), "Birthday"sv);
  EXPECT_EQ(reader.GetFieldName(2), "IsAlive"sv);
  EXPECT_ANY_THROW(reader.GetFieldName(3));

  EXPECT_EQ(reader.GetFieldType(0), neversql::DataTypeEnum::Int32);
  EXPECT_EQ(reader.GetFieldType(1), neversql::DataTypeEnum::String);
  EXPECT_EQ(reader.GetFieldType(2), neversql::DataTypeEnum::Boolean);
  EXPECT_ANY_THROW(reader.GetFieldType(3));

  EXPECT_EQ(reader.GetEntryAs<int>(0), 42);
  EXPECT_EQ(reader.GetEntryAs<std::string>(1), "My business");
  EXPECT_EQ(reader.GetEntryAs<bool>(2), true);
  EXPECT_ANY_THROW([[maybe_unused]] auto x = reader.GetEntryAs<int>(1));
  EXPECT_ANY_THROW([[maybe_unused]] auto x = reader.GetEntryAs<std::string>(0));
  EXPECT_ANY_THROW([[maybe_unused]] auto x = reader.GetEntryAs<bool>(0));

  std::ostringstream stream;
  neversql::PrettyPrint(reader, stream);
  EXPECT_EQ(stream.str(), "Age: 42\nBirthday: \"My business\"\nIsAlive: true\n");
}

TEST(Document, Strings) {
  neversql::DocumentBuilder document;
  document.AddEntry("A-string", "Hello"s);
  document.AddEntry("B-string", "There"s);
  document.AddEntry("C-string", "World"s);

  lightning::memory::MemoryBuffer<std::byte> buffer;

  WriteToBuffer(buffer, document);
  std::span written_data(buffer.Data(), buffer.Size());
  EXPECT_EQ(written_data.size(), 60);
  EXPECT_EQ(document.CalculateRequiredSize(), 60);

  neversql::DocumentReader reader(written_data);
  ASSERT_EQ(reader.GetNumFields(), 3);
  EXPECT_EQ(reader.GetFieldName(0), "A-string"sv);
  EXPECT_EQ(reader.GetFieldName(1), "B-string"sv);
  EXPECT_EQ(reader.GetFieldName(2), "C-string"sv);
  EXPECT_ANY_THROW(reader.GetFieldName(3));

  EXPECT_EQ(reader.GetFieldType(0), neversql::DataTypeEnum::String);
  EXPECT_EQ(reader.GetFieldType(1), neversql::DataTypeEnum::String);
  EXPECT_EQ(reader.GetFieldType(2), neversql::DataTypeEnum::String);
  EXPECT_ANY_THROW(reader.GetFieldType(3));

  EXPECT_EQ(reader.GetEntryAs<std::string>(0), "Hello");
  EXPECT_EQ(reader.GetEntryAs<std::string>(1), "There");
  EXPECT_EQ(reader.GetEntryAs<std::string>(2), "World");
}

}  // namespace testing