//
// Created by Nathaniel Rupprecht on 2/18/24.
//

#include <gtest/gtest.h>

#include "NeverSQL/data/Document.h"

namespace Testing {

TEST(Document, Basic) {
  neversql::Document document(0);
  document.AddEntry("Age", 42);
  document.AddEntry("Birthday", neversql::DateTime(2020'01'01));

  EXPECT_EQ(document.GetEntryAs<int>("Age"), 42);
  EXPECT_EQ(document.GetEntryAs<neversql::DateTime>("Birthday"), neversql::DateTime(2020'01'01));
}

}  // namespace Testing