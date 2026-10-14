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

#include "icing/monkey_test/abstract_query_tree/monkey-not-query-node.h"

#include <memory>
#include <string>
#include <unordered_set>
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

class MonkeyNotQueryNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    SchemaProto schema =
        SchemaBuilder()
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("Email")
                    // Add a property path "subject" with EXACT_ONLY term match
                    // type.
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("subject")
                            .SetDataTypeString(
                                TermMatchType::EXACT_ONLY,
                                StringIndexingConfig::TokenizerType::PLAIN))
                    // Add a property path "body" with PREFIX term match type.
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("body")
                            .SetDataTypeString(
                                TermMatchType::PREFIX,
                                StringIndexingConfig::TokenizerType::PLAIN)))
            .Build();

    engine_->SetSchema(schema);
  }

  void IndexDocument(const std::string& uri,
                     const std::vector<MonkeySection>& sections,
                     const char* name_space = "namespace") {
    MonkeyTokenizedDocument doc;
    doc.document.set_schema("Email");
    doc.document.set_namespace_(name_space);
    doc.document.set_uri(uri);
    doc.sections = sections;
    engine_->Put(doc);
  }

  MonkeyTestRandomEngine random_{/*seed=*/0};
  std::unique_ptr<InMemoryIcingSearchEngine> engine_ =
      std::make_unique<InMemoryIcingSearchEngine>(&random_);
};

TEST_F(MonkeyNotQueryNodeTest, NotFoo) {
  // Doc 0: Doesn't match "foo"
  IndexDocument("uri0",
                {MonkeySection{.path = "subject", .string_values = {"bar"}},
                 MonkeySection{.path = "body", .string_values = {"baz"}}});
  // Doc 1: Matches "foo" on the "subject" property
  IndexDocument("uri1",
                {MonkeySection{.path = "subject", .string_values = {"foo"}},
                 MonkeySection{.path = "body", .string_values = {"bar"}}});
  // Doc 2: Matches "foo" on the "body" property
  IndexDocument("uri2",
                {MonkeySection{.path = "body", .string_values = {"foo"}}});
  // Doc 3: Matches "foo" on both "subject" and "body" properties.
  IndexDocument("uri3",
                {MonkeySection{.path = "subject", .string_values = {"foo"}},
                 MonkeySection{.path = "body", .string_values = {"foo"}}});
  // Doc 4: Matches "foo" on the "body" property as a prefix
  IndexDocument("uri4",
                {MonkeySection{.path = "body", .string_values = {"foobar"}}});
  // Doc 5: Doesn't match "foo", but in a different namespace
  IndexDocument("uri5",
                {MonkeySection{.path = "body", .string_values = {"baz"}}},
                "different_namespace");

  auto term_node = std::make_unique<MonkeyTermQueryNode>(
      "foo", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::UNKNOWN);
  MonkeyNotQueryNode node(std::move(term_node));
  // The subquery "foo" should match documents with doc_ids 1, 2, 3, 4.
  // Thus, NOT foo should return documents with doc_ids 0, 5.
  EXPECT_THAT(node.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(0, 5)));
}

TEST_F(MonkeyNotQueryNodeTest, NotEmpty) {
  // Doc 0: Doesn't match "foo"
  IndexDocument("uri0",
                {MonkeySection{.path = "subject", .string_values = {"bar"}},
                 MonkeySection{.path = "body", .string_values = {"baz"}}});
  // Doc 1: Matches "foo" on the "subject" property
  IndexDocument("uri1",
                {MonkeySection{.path = "subject", .string_values = {"foo"}},
                 MonkeySection{.path = "body", .string_values = {"bar"}}});
  // Doc 2: Matches "foo" on the "body" property
  IndexDocument("uri2",
                {MonkeySection{.path = "body", .string_values = {"foo"}}});
  // Doc 3: Matches "foo" on both "subject" and "body" properties.
  IndexDocument("uri3",
                {MonkeySection{.path = "subject", .string_values = {"foo"}},
                 MonkeySection{.path = "body", .string_values = {"foo"}}});
  // Doc 4: Matches "foo" on the "body" property as a prefix
  IndexDocument("uri4",
                {MonkeySection{.path = "body", .string_values = {"foobar"}}});
  // Doc 5: Doesn't match "foo", but in a different namespace
  IndexDocument("uri5",
                {MonkeySection{.path = "body", .string_values = {"baz"}}},
                "different_namespace");

  auto term_node = std::make_unique<MonkeyTermQueryNode>(
      "", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::UNKNOWN);
  MonkeyNotQueryNode node(std::move(term_node));
  // The subquery "" matches all documents, so the overall query should match no
  // documents.
  EXPECT_THAT(node.EvaluateQuery(engine_.get()), IsOkAndHolds(IsEmpty()));
}

TEST_F(MonkeyNotQueryNodeTest, NotMatchesNothing) {
  // Doc 0: Doesn't match "foo"
  IndexDocument("uri0",
                {MonkeySection{.path = "subject", .string_values = {"bar"}},
                 MonkeySection{.path = "body", .string_values = {"baz"}}});
  // Doc 1: Matches "foo" on the "subject" property
  IndexDocument("uri1",
                {MonkeySection{.path = "subject", .string_values = {"foo"}},
                 MonkeySection{.path = "body", .string_values = {"bar"}}});
  // Doc 2: Matches "foo" on the "body" property
  IndexDocument("uri2",
                {MonkeySection{.path = "body", .string_values = {"foo"}}});
  // Doc 3: Matches "foo" on both "subject" and "body" properties.
  IndexDocument("uri3",
                {MonkeySection{.path = "subject", .string_values = {"foo"}},
                 MonkeySection{.path = "body", .string_values = {"foo"}}});
  // Doc 4: Matches "foo" on the "body" property as a prefix
  IndexDocument("uri4",
                {MonkeySection{.path = "body", .string_values = {"foobar"}}});
  // Doc 5: Doesn't match "foo", but in a different namespace
  IndexDocument("uri5",
                {MonkeySection{.path = "body", .string_values = {"baz"}}},
                "different_namespace");

  auto term_node = std::make_unique<MonkeyTermQueryNode>(
      "nevereverpresent",
      /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::UNKNOWN);
  MonkeyNotQueryNode node(std::move(term_node));
  // The subquery "nevereverpresent" should match no documents.
  // Thus, the overall query should return all documents.
  EXPECT_THAT(node.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(0, 1, 2, 3, 4, 5)));
}

TEST_F(MonkeyNotQueryNodeTest, NotPropertyRestrict) {
  // Doc 0: Doesn't match "foo"
  IndexDocument("uri0",
                {MonkeySection{.path = "subject", .string_values = {"bar"}},
                 MonkeySection{.path = "body", .string_values = {"baz"}}});
  // Doc 1: Matches "foo" on the "subject" property
  IndexDocument("uri1",
                {MonkeySection{.path = "subject", .string_values = {"foo"}},
                 MonkeySection{.path = "body", .string_values = {"bar"}}});
  // Doc 2: Matches "foo" on the "body" property
  IndexDocument("uri2",
                {MonkeySection{.path = "body", .string_values = {"foo"}}});
  // Doc 3: Matches "foo" on both "subject" and "body" properties.
  IndexDocument("uri3",
                {MonkeySection{.path = "subject", .string_values = {"foo"}},
                 MonkeySection{.path = "body", .string_values = {"foo"}}});
  // Doc 4: Matches "foo" on the "body" property as a prefix
  IndexDocument("uri4",
                {MonkeySection{.path = "body", .string_values = {"foobar"}}});
  // Doc 5: Doesn't match "foo", but in a different namespace
  IndexDocument("uri5",
                {MonkeySection{.path = "body", .string_values = {"baz"}}},
                "different_namespace");

  auto property_restricted_node = std::make_unique<MonkeyTermQueryNode>(
      "foo", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::PREFIX,
      std::unordered_set<std::string>{"body"});

  MonkeyNotQueryNode node(std::move(property_restricted_node));

  // The subquery "foo" with property restriction "body" should match documents
  // with doc_ids 2, 3, 4.
  // Thus the overall query should return documents with doc_ids 0, 1, 5.
  EXPECT_THAT(node.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(0, 1, 5)));
}

TEST_F(MonkeyNotQueryNodeTest, NotFooWithNamespaceFilter) {
  // Doc 0: Doesn't match "foo"
  IndexDocument("uri0",
                {MonkeySection{.path = "subject", .string_values = {"bar"}},
                 MonkeySection{.path = "body", .string_values = {"baz"}}});
  // Doc 1: Matches "foo" on the "subject" property
  IndexDocument("uri1",
                {MonkeySection{.path = "subject", .string_values = {"foo"}},
                 MonkeySection{.path = "body", .string_values = {"bar"}}});
  // Doc 2: Matches "foo" on the "body" property
  IndexDocument("uri2",
                {MonkeySection{.path = "body", .string_values = {"foo"}}});
  // Doc 3: Matches "foo" on both "subject" and "body" properties.
  IndexDocument("uri3",
                {MonkeySection{.path = "subject", .string_values = {"foo"}},
                 MonkeySection{.path = "body", .string_values = {"foo"}}});
  // Doc 4: Matches "foo" on the "body" property as a prefix
  IndexDocument("uri4",
                {MonkeySection{.path = "body", .string_values = {"foobar"}}});
  // Doc 5: Doesn't match "foo", but in a different namespace
  IndexDocument("uri5",
                {MonkeySection{.path = "body", .string_values = {"baz"}}},
                "different_namespace");

  auto term_node = std::make_unique<MonkeyTermQueryNode>(
      "foo", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::UNKNOWN);
  MonkeyNotQueryNode node(std::move(term_node),
                          /*document_namespaces=*/{"namespace"},
                          /*document_schema_types=*/{});
  // The subquery "foo" should match documents with doc_ids 1, 2, 3, 4.
  // With namespace filter "namespace", GetAllDocIds returns doc_ids 0, 1, 2, 3,
  // 4.
  // Thus, NOT foo with namespace filter should return documents with doc_id 0
  EXPECT_THAT(node.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(0)));
}

TEST_F(MonkeyNotQueryNodeTest, GenerateQueryString) {
  auto term_node = std::make_unique<MonkeyTermQueryNode>(
      "foo", /*is_prefix=*/false,
      /*is_verbatim=*/false, TermMatchType::EXACT_ONLY);
  MonkeyNotQueryNode node(std::move(term_node));
  EXPECT_THAT(node.GenerateQueryString(), Eq("NOT (foo)"));
}

}  // namespace
}  // namespace lib
}  // namespace icing
