//
// Created by Nathaniel Rupprecht on 3/29/25.
//

#include <cstdlib>
#include <iostream>
#include <ranges>
#include <string>

#include <gtest/gtest.h>

#include "neversql/data/btree/BTree.h"
#include "neversql/data/internals/Utility.h"
#include "neversql/database/DataManager.h"
#include "neversql/database/Query.h"

using namespace std::string_literals;
using namespace std::string_view_literals;

using namespace neversql;

namespace testing {

TEST(Document, Single_Integer) {
  const char* scratch = std::getenv("TEST_TMPDIR");
  ASSERT_TRUE(scratch);
  auto database_path = std::filesystem::path(scratch) / "test_db";

  neversql::DataManager manager(database_path);

  ASSERT_EQ(manager.GetDataAccessLayer().GetNumPages(), 3);

  manager.AddCollection("elements", neversql::DataTypeEnum::UInt64);

  std::vector<std::string> quotes {
      "Hello, there!",
      "Forsooth, I say unto you.",
      "To be, or not to be.",
      "A rose by any other name would smell as sweet.",
  };
  std::vector<std::string> speakers {
      "First Citizen",
      "Second Citizen",
      "Hamlet",
      "Juliet",
  };

  std::size_t count = 0;
  for (const auto& [speaker, quote] : std::views::zip(speakers, quotes)) {
    neversql::Document builder;
    builder.AddElement("number", neversql::IntegralValue {count});
    builder.AddElement("speaker", neversql::StringValue {speaker});
    builder.AddElement("quote", neversql::StringValue {quote});
    manager.AddValue("elements", builder);

    ++count;
  }

  EXPECT_EQ(manager.GetDataAccessLayer().GetNumPages(), 4);

  auto it_begin = manager.Begin("elements");
  auto end_it = manager.End("elements");

  count = 0;
  for (auto it = it_begin; it != end_it; ++it) {
    auto entry = *it;
    // Interpret the data as a document.

    ASSERT_TRUE(entry->IsValid());

    auto document = EntryToDocument(*entry);
    auto speaker = document->TryGetAs<std::string>("speaker");
    auto quote = document->TryGetAs<std::string>("quote");
    auto number = document->TryGetAs<uint64_t>("number");
    ASSERT_TRUE(speaker);
    ASSERT_TRUE(quote);
    ASSERT_TRUE(number);
    EXPECT_EQ(*speaker, speakers[count]);
    EXPECT_EQ(*quote, quotes[count]);
    EXPECT_EQ(*number, count);

    ++count;
  }
}

}