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

#include "icing/monkey_test/abstract_query_tree/monkey-has-property-query-node.h"

#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/monkey_test/monkey-test-util.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/schema-builder.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {
namespace {

using ::testing::Eq;
using ::testing::UnorderedElementsAre;

class MonkeyHasPropertyQueryNodeTest : public ::testing::Test {
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
                                TermMatchType::PREFIX,
                                StringIndexingConfig::TokenizerType::PLAIN)))
            .Build();

    engine_->SetSchema(schema);
  }

  void IndexDoc(const std::string& uri, std::vector<MonkeySection> sections) {
    MonkeyTokenizedDocument doc;
    doc.document.set_schema("Email");
    doc.document.set_namespace_("namespace");
    doc.document.set_uri(uri);
    doc.sections = std::move(sections);
    engine_->Put(doc);
  }

  MonkeyTestRandomEngine random_{/*seed=*/0};
  std::unique_ptr<InMemoryIcingSearchEngine> engine_ =
      std::make_unique<InMemoryIcingSearchEngine>(&random_);
};

TEST_F(MonkeyHasPropertyQueryNodeTest, HasSubject) {
  IndexDoc("uri0", {{.path = "subject", .string_values = {"subject"}},
                    {.path = "body", .string_values = {"body"}}});
  IndexDoc("uri1", {{.path = "subject", .string_values = {"subject"}}});
  IndexDoc("uri2", {{.path = "body", .string_values = {"body"}}});
  IndexDoc("uri3", {});

  MonkeyHasPropertyQueryNode node("subject");
  EXPECT_THAT(node.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(0, 1)));
}

TEST_F(MonkeyHasPropertyQueryNodeTest, HasBody) {
  IndexDoc("uri0", {{.path = "subject", .string_values = {"subject"}},
                    {.path = "body", .string_values = {"body"}}});
  IndexDoc("uri1", {{.path = "subject", .string_values = {"subject"}}});
  IndexDoc("uri2", {{.path = "body", .string_values = {"body"}}});
  IndexDoc("uri3", {});

  MonkeyHasPropertyQueryNode node("body");
  EXPECT_THAT(node.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(0, 2)));
}

TEST_F(MonkeyHasPropertyQueryNodeTest, HasNonExistentProperty) {
  IndexDoc("uri0", {{.path = "subject", .string_values = {"subject"}},
                    {.path = "body", .string_values = {"body"}}});
  IndexDoc("uri1", {{.path = "subject", .string_values = {"subject"}}});
  IndexDoc("uri2", {{.path = "body", .string_values = {"body"}}});
  IndexDoc("uri3", {});

  MonkeyHasPropertyQueryNode node("non_existent");
  EXPECT_THAT(node.EvaluateQuery(engine_.get()),
              IsOkAndHolds(::testing::IsEmpty()));
}

TEST_F(MonkeyHasPropertyQueryNodeTest, GenerateQueryString) {
  MonkeyHasPropertyQueryNode node("foo.bar");
  EXPECT_THAT(node.GenerateQueryString(), Eq("hasProperty(\"foo.bar\")"));
}

}  // namespace
}  // namespace lib
}  // namespace icing
