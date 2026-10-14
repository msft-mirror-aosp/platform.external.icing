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

#include "icing/monkey_test/abstract_query_tree/monkey-or-query-node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-query-node.h"
#include "icing/monkey_test/abstract_query_tree/monkey-term-query-node.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/monkey_test/monkey-test-util.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/schema-builder.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {
namespace {

using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::UnorderedElementsAre;

class MonkeyOrQueryNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    SchemaProto schema =
        SchemaBuilder()
            .AddType(SchemaTypeConfigBuilder().SetType("Email").AddProperty(
                PropertyConfigBuilder().SetName("body").SetDataTypeString(
                    TermMatchType::EXACT_ONLY,
                    StringIndexingConfig::TokenizerType::PLAIN)))
            .Build();
    engine_->SetSchema(schema);
  }

  void IndexDoc(const std::string& uri,
                const std::vector<std::string>& body_values) {
    MonkeyTokenizedDocument doc;
    doc.document.set_schema("Email");
    doc.document.set_namespace_("namespace");
    doc.document.set_uri(uri);
    doc.sections.push_back(
        MonkeySection{.path = "body", .string_values = body_values});
    engine_->Put(doc);
  }

  MonkeyTestRandomEngine random_{/*seed=*/0};
  std::unique_ptr<InMemoryIcingSearchEngine> engine_ =
      std::make_unique<InMemoryIcingSearchEngine>(&random_);
};

TEST_F(MonkeyOrQueryNodeTest, BothSubqueriesReturnEmpty) {
  IndexDoc("uri0", {"apple"});                      // doc 0
  IndexDoc("uri1", {"banana"});                     // doc 1
  IndexDoc("uri2", {"apple", "orange"});            // doc 2
  IndexDoc("uri3", {"banana", "grape"});            // doc 3
  IndexDoc("uri4", {"apple", "banana"});            // doc 4
  IndexDoc("uri5", {"apple", "banana", "orange"});  // doc 5

  std::vector<std::unique_ptr<MonkeyAbstractQueryNode>> children;
  children.push_back(std::make_unique<MonkeyTermQueryNode>(
      "other1", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::EXACT_ONLY));
  children.push_back(std::make_unique<MonkeyTermQueryNode>(
      "other2", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::EXACT_ONLY));
  MonkeyOrQueryNode node(std::move(children));
  // "other1" matches no docs, "other2" matches no docs.
  EXPECT_THAT(node.EvaluateQuery(engine_.get()), IsOkAndHolds(IsEmpty()));
}

TEST_F(MonkeyOrQueryNodeTest, OneSubqueryReturnsEmpty) {
  IndexDoc("uri0", {"apple"});                      // doc 0
  IndexDoc("uri1", {"banana"});                     // doc 1
  IndexDoc("uri2", {"apple", "orange"});            // doc 2
  IndexDoc("uri3", {"banana", "grape"});            // doc 3
  IndexDoc("uri4", {"apple", "banana"});            // doc 4
  IndexDoc("uri5", {"apple", "banana", "orange"});  // doc 5

  std::vector<std::unique_ptr<MonkeyAbstractQueryNode>> children;
  children.push_back(std::make_unique<MonkeyTermQueryNode>(
      "apple", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::EXACT_ONLY));
  children.push_back(std::make_unique<MonkeyTermQueryNode>(
      "other", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::EXACT_ONLY));
  MonkeyOrQueryNode node(std::move(children));
  // "apple" matches docs 0, 2, 4, 5. "other" matches no docs.
  EXPECT_THAT(node.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(0, 2, 4, 5)));
}

TEST_F(MonkeyOrQueryNodeTest, NoIntersection) {
  IndexDoc("uri0", {"apple"});                      // doc 0
  IndexDoc("uri1", {"banana"});                     // doc 1
  IndexDoc("uri2", {"apple", "orange"});            // doc 2
  IndexDoc("uri3", {"banana", "grape"});            // doc 3
  IndexDoc("uri4", {"apple", "banana"});            // doc 4
  IndexDoc("uri5", {"apple", "banana", "orange"});  // doc 5

  std::vector<std::unique_ptr<MonkeyAbstractQueryNode>> children;
  children.push_back(std::make_unique<MonkeyTermQueryNode>(
      "orange", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::EXACT_ONLY));
  children.push_back(std::make_unique<MonkeyTermQueryNode>(
      "grape", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::EXACT_ONLY));
  MonkeyOrQueryNode node(std::move(children));
  // "orange" matches docs 2, 5. "grape" matches doc 3.
  EXPECT_THAT(node.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(2, 3, 5)));
}

TEST_F(MonkeyOrQueryNodeTest, TwoSubqueriesWithIntersection) {
  IndexDoc("uri0", {"apple"});                      // doc 0
  IndexDoc("uri1", {"banana"});                     // doc 1
  IndexDoc("uri2", {"apple", "orange"});            // doc 2
  IndexDoc("uri3", {"banana", "grape"});            // doc 3
  IndexDoc("uri4", {"apple", "banana"});            // doc 4
  IndexDoc("uri5", {"apple", "banana", "orange"});  // doc 5

  std::vector<std::unique_ptr<MonkeyAbstractQueryNode>> children;
  children.push_back(std::make_unique<MonkeyTermQueryNode>(
      "apple", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::EXACT_ONLY));
  children.push_back(std::make_unique<MonkeyTermQueryNode>(
      "banana", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::EXACT_ONLY));
  MonkeyOrQueryNode node(std::move(children));
  // "apple" matches docs 0, 2, 4, 5. "banana" matches docs 1, 3, 4, 5.
  EXPECT_THAT(node.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(0, 1, 2, 3, 4, 5)));
}

TEST_F(MonkeyOrQueryNodeTest, ThreeSubqueriesWithIntersection) {
  IndexDoc("uri0", {"apple"});                      // doc 0
  IndexDoc("uri1", {"banana"});                     // doc 1
  IndexDoc("uri2", {"apple", "orange"});            // doc 2
  IndexDoc("uri3", {"banana", "grape"});            // doc 3
  IndexDoc("uri4", {"apple", "banana"});            // doc 4
  IndexDoc("uri5", {"apple", "banana", "orange"});  // doc 5

  std::vector<std::unique_ptr<MonkeyAbstractQueryNode>> children;
  children.push_back(std::make_unique<MonkeyTermQueryNode>(
      "apple", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::EXACT_ONLY));
  children.push_back(std::make_unique<MonkeyTermQueryNode>(
      "banana", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::EXACT_ONLY));
  children.push_back(std::make_unique<MonkeyTermQueryNode>(
      "orange", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::EXACT_ONLY));
  MonkeyOrQueryNode node(std::move(children));
  // "apple" matches docs 0, 2, 4, 5. "banana" matches docs 1, 3, 4, 5.
  // "orange" matches docs 2, 5.
  EXPECT_THAT(node.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(0, 1, 2, 3, 4, 5)));
}

TEST_F(MonkeyOrQueryNodeTest, GenerateQueryString) {
  // 2 children
  std::vector<std::unique_ptr<MonkeyAbstractQueryNode>> children2;
  children2.push_back(std::make_unique<MonkeyTermQueryNode>(
      "foo", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::EXACT_ONLY));
  children2.push_back(std::make_unique<MonkeyTermQueryNode>(
      "bar", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::EXACT_ONLY));
  MonkeyOrQueryNode node2(std::move(children2));
  EXPECT_THAT(node2.GenerateQueryString(), Eq("((foo) OR (bar))"));

  // 3 children
  std::vector<std::unique_ptr<MonkeyAbstractQueryNode>> children3;
  children3.push_back(std::make_unique<MonkeyTermQueryNode>(
      "foo", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::EXACT_ONLY));
  children3.push_back(std::make_unique<MonkeyTermQueryNode>(
      "bar", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::EXACT_ONLY));
  children3.push_back(std::make_unique<MonkeyTermQueryNode>(
      "baz", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::EXACT_ONLY));
  MonkeyOrQueryNode node3(std::move(children3));
  EXPECT_THAT(node3.GenerateQueryString(), Eq("((foo) OR (bar) OR (baz))"));
}

}  // namespace
}  // namespace lib
}  // namespace icing
