//
// Created by Nathaniel Rupprecht on 3/18/24.
//

#include <gtest/gtest.h>

#include "neversql/containers/FixedStack.h"

using namespace std::string_literals;
using namespace std::string_view_literals;

namespace testing {

TEST(FixedStack, Overflow_And_Underflow) {
  auto fixed_stack = neversql::FixedStack<int, 4>();
  EXPECT_NO_THROW(fixed_stack.Pop());
  ASSERT_FALSE(fixed_stack.Top());

  EXPECT_EQ(fixed_stack.Size(), 0);
  EXPECT_TRUE(fixed_stack.Empty());

  EXPECT_NO_THROW(fixed_stack.Push(1));
  EXPECT_EQ(fixed_stack.Size(), 1);
  EXPECT_NO_THROW(fixed_stack.Push(2));
  EXPECT_EQ(fixed_stack.Size(), 2);
  EXPECT_NO_THROW(fixed_stack.Push(3));
  EXPECT_EQ(fixed_stack.Size(), 3);
  EXPECT_NO_THROW(fixed_stack.Push(4));
  EXPECT_EQ(fixed_stack.Size(), 4);
  EXPECT_NO_THROW(fixed_stack.Push(5));
  EXPECT_EQ(fixed_stack.Size(), 4);
}

}  // namespace testing