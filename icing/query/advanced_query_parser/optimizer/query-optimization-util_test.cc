// Copyright (C) 2026 Google LLC
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

#include "icing/query/advanced_query_parser/optimizer/query-optimization-util.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/index/iterator/doc-hit-info-iterator-test-util.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"

namespace icing {
namespace lib {
namespace query_optimization_util {

namespace {

using ::testing::Eq;
using ::testing::HasSubstr;

class MockCanAdoptDelegateIterator : public DocHitInfoIteratorDummy {
 public:
  bool CanAdoptDelegate() const override { return true; }
  std::string ToString() const override {
    return "MockCanAdoptDelegateIterator";
  }

  void AdoptDelegate(std::unique_ptr<DocHitInfoIterator> delegate,
                     bool delegate_node_is_right_most) override {
    delegate_ = std::move(delegate);
    delegate_node_is_right_most_ = delegate_node_is_right_most;
  }

  std::unique_ptr<DocHitInfoIterator> delegate_;
  bool delegate_node_is_right_most_ = false;
};

class MockCannotAdoptDelegateIterator : public DocHitInfoIteratorDummy {
 public:
  bool CanAdoptDelegate() const override { return false; }
  std::string ToString() const override {
    return "MockCannotAdoptDelegateIterator";
  }
};

TEST(QueryOptimizationUtilTest,
     OptimizeAndIteratorsIfPossible_FindsNonIndexZero) {
  std::vector<std::unique_ptr<DocHitInfoIterator>> iterators;
  iterators.push_back(std::make_unique<MockCannotAdoptDelegateIterator>());
  iterators.push_back(std::make_unique<MockCanAdoptDelegateIterator>());

  std::unique_ptr<DocHitInfoIterator> result =
      OptimizeAndIteratorsIfPossible(std::move(iterators));

  // Check if the result is the MockCanAdoptDelegateIterator (indicating
  // optimization happened).
  EXPECT_THAT(result->ToString(), Eq("MockCanAdoptDelegateIterator"));
}

TEST(QueryOptimizationUtilTest, Optimize_NoEmbeddingIterator) {
  std::vector<std::unique_ptr<DocHitInfoIterator>> iterators;
  iterators.push_back(std::make_unique<MockCannotAdoptDelegateIterator>());
  iterators.push_back(std::make_unique<MockCannotAdoptDelegateIterator>());

  std::unique_ptr<DocHitInfoIterator> result =
      OptimizeAndIteratorsIfPossible(std::move(iterators));

  EXPECT_THAT(result->ToString(), HasSubstr("AND"));
}

TEST(QueryOptimizationUtilTest, Optimize_EmbeddingIteratorNotLast) {
  std::vector<std::unique_ptr<DocHitInfoIterator>> iterators;
  auto mock_embed = std::make_unique<MockCanAdoptDelegateIterator>();
  MockCanAdoptDelegateIterator* mock_embed_ptr = mock_embed.get();

  iterators.push_back(std::move(mock_embed));
  iterators.push_back(std::make_unique<MockCannotAdoptDelegateIterator>());

  std::unique_ptr<DocHitInfoIterator> result =
      OptimizeAndIteratorsIfPossible(std::move(iterators));

  EXPECT_THAT(result.get(), Eq(mock_embed_ptr));
  EXPECT_TRUE(mock_embed_ptr->delegate_node_is_right_most_);
}

TEST(QueryOptimizationUtilTest, Optimize_EmbeddingIteratorIsLast) {
  std::vector<std::unique_ptr<DocHitInfoIterator>> iterators;
  auto mock_embed = std::make_unique<MockCanAdoptDelegateIterator>();
  MockCanAdoptDelegateIterator* mock_embed_ptr = mock_embed.get();

  iterators.push_back(std::make_unique<MockCannotAdoptDelegateIterator>());
  iterators.push_back(std::move(mock_embed));

  std::unique_ptr<DocHitInfoIterator> result =
      OptimizeAndIteratorsIfPossible(std::move(iterators));

  EXPECT_THAT(result.get(), Eq(mock_embed_ptr));
  EXPECT_FALSE(mock_embed_ptr->delegate_node_is_right_most_);
}

}  // namespace
}  // namespace query_optimization_util
}  // namespace lib
}  // namespace icing
