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

#include "icing/monkey_test/abstract_query_tree/monkey-term-query-node.h"

#include <memory>
#include <string>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/monkey_test/monkey-test-util.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/schema-builder.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {
namespace {

using ::testing::Eq;
using ::testing::UnorderedElementsAre;

class MonkeyTermQueryNodeTest : public ::testing::Test {
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

    // Doc 1: Doesn't match "foo"
    doc1_.document.set_schema("Email");
    doc1_.document.set_namespace_("namespace");
    doc1_.document.set_uri("uri1");
    doc1_.sections.push_back(
        MonkeySection{.path = "subject", .string_values = {"bar"}});
    doc1_.sections.push_back(
        MonkeySection{.path = "body", .string_values = {"baz"}});
    engine_->Put(doc1_);

    // Doc 2: Has one section ("subject") that matches "foo" exactly.
    doc2_.document.set_schema("Email");
    doc2_.document.set_namespace_("namespace");
    doc2_.document.set_uri("uri2");
    doc2_.sections.push_back(
        MonkeySection{.path = "subject", .string_values = {"foo"}});
    engine_->Put(doc2_);

    // Doc 3: Has two sections, "body" matches "foo" exactly.
    doc3_.document.set_schema("Email");
    doc3_.document.set_namespace_("namespace");
    doc3_.document.set_uri("uri3");
    doc3_.sections.push_back(
        MonkeySection{.path = "subject", .string_values = {"bar"}});
    doc3_.sections.push_back(
        MonkeySection{.path = "body", .string_values = {"foo"}});
    engine_->Put(doc3_);

    // Doc 4: "body" matches "foo" as a prefix.
    doc4_.document.set_schema("Email");
    doc4_.document.set_namespace_("namespace");
    doc4_.document.set_uri("uri4");
    doc4_.sections.push_back(
        MonkeySection{.path = "subject", .string_values = {"bar"}});
    doc4_.sections.push_back(
        MonkeySection{.path = "body", .string_values = {"foobar"}});
    engine_->Put(doc4_);

    // Doc 5: "subject" matches "foo" as a prefix, but section is EXACT_ONLY.
    doc5_.document.set_schema("Email");
    doc5_.document.set_namespace_("namespace");
    doc5_.document.set_uri("uri5");
    doc5_.sections.push_back(
        MonkeySection{.path = "subject", .string_values = {"foobar"}});
    engine_->Put(doc5_);
  }

  MonkeyTestRandomEngine random_{/*seed=*/0};
  std::unique_ptr<InMemoryIcingSearchEngine> engine_ =
      std::make_unique<InMemoryIcingSearchEngine>(&random_);
  MonkeyTokenizedDocument doc1_, doc2_, doc3_, doc4_, doc5_;
};

TEST_F(MonkeyTermQueryNodeTest, SearchSpecExactOnly) {
  MonkeyTermQueryNode node("foo", /*is_prefix=*/false, /*is_verbatim=*/false,
                           TermMatchType::EXACT_ONLY);
  EXPECT_THAT(node.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(1, 2)));
}

TEST_F(MonkeyTermQueryNodeTest, SearchSpecPrefix) {
  MonkeyTermQueryNode node("foo", /*is_prefix=*/false, /*is_verbatim=*/false,
                           TermMatchType::PREFIX);

  // If search spec is PREFIX:
  // - If section is EXACT_ONLY, we still only exact match.
  // - If section is PREFIX, we prefix match.
  //
  // Doc 1: no match.
  // Doc 2: 'subject' is EXACT_ONLY and "foo" == "foo", matches.
  // Doc 3: 'body' is PREFIX and IsPrefix("foo", "foo"), matches.
  // Doc 4: 'body' is PREFIX and IsPrefix("foo", "foobar"), matches.
  // Doc 5: 'subject' is EXACT_ONLY and "foobar" != "foo", no match.
  libtextclassifier3::StatusOr<std::vector<DocumentId>> result =
      node.EvaluateQuery(engine_.get());
  EXPECT_THAT(result, IsOkAndHolds(UnorderedElementsAre(1, 2, 3)));
}

TEST_F(MonkeyTermQueryNodeTest, GenerateQueryString) {
  MonkeyTermQueryNode node1("foo", /*is_prefix=*/false, /*is_verbatim=*/false,
                            TermMatchType::EXACT_ONLY);
  EXPECT_THAT(node1.GenerateQueryString(), Eq("foo"));

  MonkeyTermQueryNode node2("foo", /*is_prefix=*/true, /*is_verbatim=*/false,
                            TermMatchType::EXACT_ONLY);
  EXPECT_THAT(node2.GenerateQueryString(), Eq("foo*"));

  MonkeyTermQueryNode node3("foo", /*is_prefix=*/false, /*is_verbatim=*/true,
                            TermMatchType::EXACT_ONLY);
  EXPECT_THAT(node3.GenerateQueryString(), Eq("\"foo\""));

  MonkeyTermQueryNode node4("foo", /*is_prefix=*/true, /*is_verbatim=*/true,
                            TermMatchType::EXACT_ONLY);
  EXPECT_THAT(node4.GenerateQueryString(), Eq("\"foo\"*"));
}

}  // namespace
}  // namespace lib
}  // namespace icing
