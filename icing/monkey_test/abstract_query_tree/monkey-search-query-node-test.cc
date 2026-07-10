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

#include "icing/monkey_test/abstract_query_tree/monkey-search-query-node.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/monkey_test/abstract_query_tree/monkey-semantic-query-node.h"
#include "icing/monkey_test/abstract_query_tree/monkey-term-query-node.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/monkey_test/monkey-test-util.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/schema-builder.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/embedding-test-utils.h"

namespace icing {
namespace lib {
namespace {

using ::testing::Eq;
using ::testing::UnorderedElementsAre;

class MonkeySearchQueryNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    SchemaProto schema =
        SchemaBuilder()
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("Email")
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("subject")
                            .SetDataTypeString(
                                TermMatchType::EXACT_ONLY,
                                StringIndexingConfig::TokenizerType::PLAIN))
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("body")
                            .SetDataTypeString(
                                TermMatchType::EXACT_ONLY,
                                StringIndexingConfig::TokenizerType::PLAIN))
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("nested")
                            .SetDataTypeDocument(
                                "Email", /*index_nested_properties=*/true)))
            .Build();

    engine_->SetSchema(schema);

    // Doc 0: "foo" in subject
    doc0_.document.set_schema("Email");
    doc0_.document.set_namespace_("namespace");
    doc0_.document.set_uri("uri0");
    doc0_.sections.push_back(
        MonkeySection{.path = "subject", .string_values = {"foo"}});
    engine_->Put(doc0_);

    // Doc 1: "foo" in body
    doc1_.document.set_schema("Email");
    doc1_.document.set_namespace_("namespace");
    doc1_.document.set_uri("uri1");
    doc1_.sections.push_back(
        MonkeySection{.path = "body", .string_values = {"foo"}});
    engine_->Put(doc1_);

    // Doc 2: "foo" in both
    doc2_.document.set_schema("Email");
    doc2_.document.set_namespace_("namespace");
    doc2_.document.set_uri("uri2");
    doc2_.sections.push_back(
        MonkeySection{.path = "subject", .string_values = {"foo"}});
    doc2_.sections.push_back(
        MonkeySection{.path = "body", .string_values = {"foo"}});
    engine_->Put(doc2_);

    // Doc 3: "bar" in body
    doc3_.document.set_schema("Email");
    doc3_.document.set_namespace_("namespace");
    doc3_.document.set_uri("uri3");
    doc3_.sections.push_back(
        MonkeySection{.path = "body", .string_values = {"bar"}});
    engine_->Put(doc3_);
  }

  MonkeyTestRandomEngine random_{/*seed=*/0};
  std::unique_ptr<InMemoryIcingSearchEngine> engine_ =
      std::make_unique<InMemoryIcingSearchEngine>(&random_);
  MonkeyTokenizedDocument doc0_, doc1_, doc2_, doc3_;
};

TEST_F(MonkeySearchQueryNodeTest, SearchWithoutProperties) {
  auto term_node = std::make_unique<MonkeyTermQueryNode>(
      "foo", /*is_prefix=*/false, /*is_verbatim=*/false,
      TermMatchType::EXACT_ONLY);
  MonkeySearchQueryNode node(std::move(term_node));

  EXPECT_THAT(node.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(0, 1, 2)));
}

TEST_F(MonkeySearchQueryNodeTest, SearchWithSingleProperty) {
  auto term_node = std::make_unique<MonkeyTermQueryNode>(
      "foo", /*is_prefix=*/false, /*is_verbatim=*/false,
      TermMatchType::EXACT_ONLY);
  MonkeySearchQueryNode node(std::move(term_node), {"subject"});

  EXPECT_THAT(node.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(0, 2)));
}

TEST_F(MonkeySearchQueryNodeTest, SearchWithMultipleProperties) {
  auto term_node = std::make_unique<MonkeyTermQueryNode>(
      "foo", /*is_prefix=*/false, /*is_verbatim=*/false,
      TermMatchType::EXACT_ONLY);
  MonkeySearchQueryNode node(std::move(term_node), {"subject", "body"});

  // Should be union of results from subject and body.
  EXPECT_THAT(node.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(0, 1, 2)));
}

TEST_F(MonkeySearchQueryNodeTest, GenerateQueryString) {
  auto term_node = std::make_unique<MonkeyTermQueryNode>(
      "foo bar", /*is_prefix=*/false, /*is_verbatim=*/true,
      TermMatchType::EXACT_ONLY);
  // "foo bar" verbatim generates "foo bar" (with quotes)
  MonkeySearchQueryNode search_term_node(std::move(term_node),
                                         {"subject", "body"});

  EXPECT_THAT(
      search_term_node.GenerateQueryString(),
      Eq("search(\"\\\"foo bar\\\"\", createList(\"subject\", \"body\"))"));

  PropertyProto::VectorProto vector = CreateVector("model1", {1.0f, 0.0f});

  auto semantic_node = std::make_unique<MonkeySemanticQueryNode>(
      /*vector_index=*/0, /*low=*/0.5, /*high=*/0.9,
      /*metric_type=*/SearchSpecProto::EmbeddingQueryMetricType::COSINE,
      /*nprobe=*/10,
      /*vector=*/vector);
  MonkeySearchQueryNode search_semantic_node(std::move(semantic_node),
                                             {"subject", "body"});

  EXPECT_THAT(
      search_semantic_node.GenerateQueryString(),
      Eq("search(\"semanticSearch(getEmbeddingParameter(0), 0.50, 0.90, "
         "\\\"COSINE\\\")\", createList(\"subject\", \"body\"))"));
}

}  // namespace
}  // namespace lib
}  // namespace icing
