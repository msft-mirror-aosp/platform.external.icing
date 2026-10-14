// Copyright (C) 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "icing/scoring/advanced_scoring/double-list.h"

#include <cstddef>
#include <iterator>
#include <numeric>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAreArray;
using ::testing::IsEmpty;

std::vector<double> CreateSequenceVec(size_t size, double start = 0.0) {
  std::vector<double> vec(size);
  std::iota(vec.begin(), vec.end(), start);
  return vec;
}

TEST(DoubleListTest, DefaultConstructorCreatesEmptyView) {
  DoubleList list;
  EXPECT_EQ(list.size(), 0);
  EXPECT_TRUE(list.empty());
  EXPECT_EQ(list.data(), nullptr);
  EXPECT_EQ(list.begin(), list.end());
}

// Test constructor taking ownership of an empty vector
TEST(DoubleListTest, EmptyVectorMoveConstructor) {
  std::vector<double> source_vec;
  const double* expected_data_ptr =
      source_vec.data();  // Capture pointer before move
  DoubleList list(std::move(source_vec));

  EXPECT_EQ(list.size(), 0);
  EXPECT_TRUE(list.empty());
  EXPECT_EQ(list.data(), expected_data_ptr);
  EXPECT_EQ(list.begin(), list.end());
}

// Test constructor creating an empty non-owning view, which is the same
// behavior of the default constructor.
TEST(DoubleListTest, EmptyDataViewConstructor) {
  const double* external_data = nullptr;
  DoubleList list(external_data, 0);
  EXPECT_EQ(list.size(), 0);
  EXPECT_TRUE(list.empty());
  EXPECT_EQ(list.data(), nullptr);
  EXPECT_EQ(list.begin(), list.end());
}

// Test constructor taking ownership via std::vector rvalue reference
TEST(DoubleListTest, VectorMoveConstructor) {
  std::vector<double> source_vec = {1.1, 2.2, 3.3};
  const double* expected_data_ptr =
      source_vec.data();  // Capture pointer before move
  const size_t expected_size = source_vec.size();

  DoubleList list(std::move(source_vec));

  EXPECT_EQ(list.size(), expected_size);
  ASSERT_FALSE(list.empty());
  EXPECT_EQ(list.data(), expected_data_ptr);
  EXPECT_THAT(list, ElementsAreArray({1.1, 2.2, 3.3}));
}

// Test constructor creating a non-owning view
TEST(DoubleListTest, DataViewConstructor) {
  const double external_data[] = {4.4, 5.5, 6.6};
  const size_t external_size = std::size(external_data);

  DoubleList list(external_data, external_size);

  EXPECT_EQ(list.size(), external_size);
  ASSERT_FALSE(list.empty());
  EXPECT_EQ(list.data(),
            external_data);  // Should point directly to external data
  EXPECT_THAT(list, ElementsAreArray({4.4, 5.5, 6.6}));
}

// Test accessors on an owned list
TEST(DoubleListTest, AccessorsOwned) {
  const DoubleList list(CreateSequenceVec(3, 10.0));  // {10.0, 11.0, 12.0}

  EXPECT_EQ(list.size(), 3);
  ASSERT_FALSE(list.empty());
  ASSERT_NE(list.data(), nullptr);
  EXPECT_EQ(list.begin()[0], 10.0);
  EXPECT_EQ(list.begin()[1], 11.0);
  EXPECT_EQ(list.begin()[2], 12.0);
  EXPECT_EQ(list.end(), list.begin() + 3);
}

// Test accessors on a non-owning list
TEST(DoubleListTest, AccessorsView) {
  const double external_data[] = {20.0, 21.0};
  const DoubleList list(external_data, 2);

  EXPECT_EQ(list.size(), 2);
  ASSERT_FALSE(list.empty());
  ASSERT_EQ(list.data(), external_data);
  EXPECT_EQ(list.begin()[0], 20.0);
  EXPECT_EQ(list.begin()[1], 21.0);
  EXPECT_EQ(list.end(), list.begin() + 2);
}

// Test move constructor from an owned list
TEST(DoubleListTest, MoveConstructionFromOwned) {
  DoubleList list1(CreateSequenceVec(2, 1.0));  // {1.0, 2.0}
  const double* original_data_ptr = list1.data();

  DoubleList list2(std::move(list1));

  // Check list2 has the data
  EXPECT_EQ(list2.size(), 2);
  ASSERT_FALSE(list2.empty());
  EXPECT_EQ(list2.data(), original_data_ptr);
  EXPECT_THAT(list2, ElementsAreArray({1.0, 2.0}));
}

// Test move constructor from a non-owning list
TEST(DoubleListTest, MoveConstructionFromView) {
  const double external_data[] = {3.0, 4.0};
  DoubleList list1(external_data, 2);
  const size_t original_size = list1.size();

  DoubleList list2(std::move(list1));

  // Check list2 has the view details
  EXPECT_EQ(list2.size(), original_size);
  ASSERT_FALSE(list2.empty());
  EXPECT_EQ(list2.data(), external_data);
  EXPECT_THAT(list2, ElementsAreArray({3.0, 4.0}));
}

// Test move assignment from an owned list to another
TEST(DoubleListTest, MoveAssignmentFromOwned) {
  DoubleList list1(CreateSequenceVec(3, 5.0));    // {5.0, 6.0, 7.0}
  DoubleList list2(CreateSequenceVec(1, 100.0));  // {100.0}
  const double* original_data_ptr_list1 = list1.data();

  list2 = std::move(list1);

  // Check list2 has the data from list1
  EXPECT_EQ(list2.size(), 3);
  ASSERT_FALSE(list2.empty());
  EXPECT_EQ(list2.data(), original_data_ptr_list1);
  EXPECT_THAT(list2, ElementsAreArray({5.0, 6.0, 7.0}));
}

// Test move assignment from a non-owning list to another
TEST(DoubleListTest, MoveAssignmentFromView) {
  const double external_data[] = {8.0, 9.0};
  DoubleList list1(external_data, 2);
  DoubleList list2(CreateSequenceVec(1, 200.0));  // {200.0}

  list2 = std::move(list1);

  // Check list2 has the view details from list1
  EXPECT_EQ(list2.size(), 2);
  ASSERT_FALSE(list2.empty());
  EXPECT_EQ(list2.data(), external_data);
  EXPECT_THAT(list2, ElementsAreArray({8.0, 9.0}));
}

// Test ReleaseVector when the list owns the data
TEST(DoubleListTest, ReleaseVectorOwned) {
  DoubleList list(CreateSequenceVec(3, 10.0));  // {10.0, 11.0, 12.0}
  const double* original_data_ptr = list.data();

  // ReleaseVector must be called on an rvalue
  std::vector<double> released_vec = std::move(list).ReleaseVector();

  // Check the released vector
  EXPECT_EQ(released_vec.size(), 3);
  EXPECT_THAT(released_vec, ElementsAreArray({10.0, 11.0, 12.0}));
  EXPECT_EQ(released_vec.data(), original_data_ptr);
}

// Test ReleaseVector when the list has a non-owning view
TEST(DoubleListTest, ReleaseVectorView) {
  const double external_data[] = {13.0, 14.0};
  DoubleList list(external_data, 2);
  const double* original_view_ptr = list.data();

  // ReleaseVector must be called on an rvalue
  std::vector<double> released_vec = std::move(list).ReleaseVector();

  // Check the released vector - it should be a COPY
  EXPECT_EQ(released_vec.size(), 2);
  EXPECT_THAT(released_vec, ElementsAreArray({13.0, 14.0}));

  // IMPORTANT: Verify it's a copy, not pointing to the original external data
  EXPECT_NE(released_vec.data(), original_view_ptr);
}

// Test ReleaseVector when the list is empty (default constructed - view)
TEST(DoubleListTest, ReleaseVectorEmptyDefault) {
  DoubleList list;  // Empty view by default
  std::vector<double> released_vec = std::move(list).ReleaseVector();
  EXPECT_TRUE(released_vec.empty());
  EXPECT_THAT(released_vec, IsEmpty());
}

// Test ReleaseVector when the list is empty (constructed from empty view)
TEST(DoubleListTest, ReleaseVectorEmptyView) {
  DoubleList list(nullptr, 0);  // Empty view
  std::vector<double> released_vec = std::move(list).ReleaseVector();
  EXPECT_TRUE(released_vec.empty());
  EXPECT_THAT(released_vec, IsEmpty());
}

// Test ReleaseVector when the list is empty (constructed from empty vector -
// owned)
TEST(DoubleListTest, ReleaseVectorEmptyOwned) {
  DoubleList list(std::vector<double>{});  // Empty owned vector
  std::vector<double> released_vec = std::move(list).ReleaseVector();
  EXPECT_TRUE(released_vec.empty());
  EXPECT_THAT(released_vec, IsEmpty());
}

}  // namespace

}  // namespace lib
}  // namespace icing
