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

#include "icing/monkey_test/abstract_query_tree/monkey-numeric-query-node.h"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema-builder.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {
namespace {

using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::UnorderedElementsAre;
using NumericComparator = MonkeyNumericQueryNode::NumericComparator;

class MonkeyNumericQueryNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // engine_ = std::make_unique<InMemoryIcingSearchEngine>(&random_);

    // Define schemas with different numeric indexing configs.
    SchemaProto schema =
        SchemaBuilder()
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("Product")
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("price")
                                     .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("weight")
                                     .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                     .SetCardinality(CARDINALITY_REPEATED))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("id_num")
                                     .SetDataTypeInt64(NUMERIC_MATCH_UNKNOWN)
                                     .SetCardinality(CARDINALITY_REPEATED)))
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("Event")
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("timestamp")
                                     .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                     .SetCardinality(CARDINALITY_REQUIRED))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("duration_ms")
                                     .SetDataTypeInt64(NUMERIC_MATCH_UNKNOWN)
                                     .SetCardinality(CARDINALITY_OPTIONAL)))
            .Build();
    engine_->SetSchema(schema);

    // product1 (ID 0)
    std::vector<int64_t> p1_price = {150};
    std::vector<int64_t> p1_weight = {100, 200, 300};
    std::vector<int64_t> p1_id_num = {1};
    product1_.document =
        DocumentBuilder()
            .SetNamespace("store")
            .SetSchema("Product")
            .SetUri("product1")
            .SetCreationTimestampMs(1000L)
            .AddInt64Property("price", p1_price.begin(), p1_price.end())
            .AddInt64Property("weight", p1_weight.begin(), p1_weight.end())
            .AddInt64Property("id_num", p1_id_num.begin(), p1_id_num.end())
            .Build();
    product1_.sections = {{.path = "price", .integer_values = p1_price},
                          {.path = "weight", .integer_values = p1_weight},
                          {.path = "id_num", .integer_values = p1_id_num}};
    engine_->Put(product1_);

    // product2 (ID 1)
    std::vector<int64_t> p2_price = {99};
    std::vector<int64_t> p2_weight = {500};
    product2_.document =
        DocumentBuilder()
            .SetNamespace("store")
            .SetSchema("Product")
            .SetUri("product2")
            .AddInt64Property("price", p2_price.begin(), p2_price.end())
            .AddInt64Property("weight", p2_weight.begin(), p2_weight.end())
            .Build();
    product2_.sections = {{.path = "price", .integer_values = p2_price},
                          {.path = "weight", .integer_values = p2_weight}};
    engine_->Put(product2_);

    // product3 (ID 2)
    std::vector<int64_t> p3_price = {200};
    std::vector<int64_t> p3_weight = {1000};
    product3_.document =
        DocumentBuilder()
            .SetNamespace("store")
            .SetSchema("Product")
            .SetUri("product3")
            .AddInt64Property("price", p3_price.begin(), p3_price.end())
            .AddInt64Property("weight", p3_weight.begin(), p3_weight.end())
            .Build();
    product3_.sections = {{.path = "price", .integer_values = p3_price},
                          {.path = "weight", .integer_values = p3_weight}};
    engine_->Put(product3_);

    // product4 (ID 3)
    std::vector<int64_t> p4_price = {100};
    std::vector<int64_t> p4_weight = {499, 500, 501};
    product4_.document =
        DocumentBuilder()
            .SetNamespace("store")
            .SetSchema("Product")
            .SetUri("product4")
            .AddInt64Property("price", p4_price.begin(), p4_price.end())
            .AddInt64Property("weight", p4_weight.begin(), p4_weight.end())
            .Build();
    product4_.sections = {{.path = "price", .integer_values = p4_price},
                          {.path = "weight", .integer_values = p4_weight}};
    engine_->Put(product4_);

    // product_no_price (ID 4)
    std::vector<int64_t> p5_weight = {100};
    product_no_price_.document =
        DocumentBuilder()
            .SetNamespace("store")
            .SetSchema("Product")
            .SetUri("product_no_price")
            .AddInt64Property("weight", p5_weight.begin(), p5_weight.end())
            .Build();
    product_no_price_.sections = {
        {.path = "weight", .integer_values = p5_weight}};
    engine_->Put(product_no_price_);

    // event1 (ID 5)
    std::vector<int64_t> e1_timestamp = {1000000000000};
    std::vector<int64_t> e1_duration = {3600000};
    event1_.document = DocumentBuilder()
                           .SetNamespace("events")
                           .SetSchema("Event")
                           .SetUri("event1")
                           .AddInt64Property("timestamp", e1_timestamp.begin(),
                                             e1_timestamp.end())
                           .AddInt64Property("duration_ms", e1_duration.begin(),
                                             e1_duration.end())
                           .Build();
    event1_.sections = {{.path = "timestamp", .integer_values = e1_timestamp},
                        {.path = "duration_ms", .integer_values = e1_duration}};
    engine_->Put(event1_);

    // event2 (ID 6)
    std::vector<int64_t> e2_timestamp = {1680000000001};
    event2_.document = DocumentBuilder()
                           .SetNamespace("events")
                           .SetSchema("Event")
                           .SetUri("event2")
                           .AddInt64Property("timestamp", e2_timestamp.begin(),
                                             e2_timestamp.end())
                           .Build();
    event2_.sections = {{.path = "timestamp", .integer_values = e2_timestamp}};
    engine_->Put(event2_);

    // event3 (ID 7)
    std::vector<int64_t> e3_timestamp = {1680000000001};
    event3_.document = DocumentBuilder()
                           .SetNamespace("other_events")
                           .SetSchema("Event")
                           .SetUri("event3")
                           .AddInt64Property("timestamp", e3_timestamp.begin(),
                                             e3_timestamp.end())
                           .Build();
    event3_.sections = {{.path = "timestamp", .integer_values = e3_timestamp}};
    engine_->Put(event3_);
  }

  MonkeyTestRandomEngine random_{/*seed=*/0};
  std::unique_ptr<InMemoryIcingSearchEngine> engine_ =
      std::make_unique<InMemoryIcingSearchEngine>(&random_);
  // Stored documents for easy referencing in tests.
  MonkeyTokenizedDocument product1_, product2_, product3_, product4_,
      product_no_price_;
  MonkeyTokenizedDocument event1_, event2_, event3_;
};

TEST_F(MonkeyNumericQueryNodeTest, GenerateQueryString) {
  EXPECT_THAT(MonkeyNumericQueryNode("price", NumericComparator::kEqual, 100)
                  .GenerateQueryString(),
              Eq("price==100"));
  EXPECT_THAT(MonkeyNumericQueryNode("price", NumericComparator::kNotEqual, 100)
                  .GenerateQueryString(),
              Eq("price!=100"));
  EXPECT_THAT(MonkeyNumericQueryNode("price", NumericComparator::kLessThan, 100)
                  .GenerateQueryString(),
              Eq("price<100"));
  EXPECT_THAT(
      MonkeyNumericQueryNode("price", NumericComparator::kLessThanEqual, 100)
          .GenerateQueryString(),
      Eq("price<=100"));
  EXPECT_THAT(
      MonkeyNumericQueryNode("price", NumericComparator::kGreaterThan, 100)
          .GenerateQueryString(),
      Eq("price>100"));
  EXPECT_THAT(
      MonkeyNumericQueryNode("price", NumericComparator::kGreaterThanEqual, 100)
          .GenerateQueryString(),
      Eq("price>=100"));
  EXPECT_THAT(MonkeyNumericQueryNode(
                  "timestamp", NumericComparator::kGreaterThan, 1678900000000)
                  .GenerateQueryString(),
              Eq("timestamp>1678900000000"));
}

TEST_F(MonkeyNumericQueryNodeTest, EvaluateQuery_GreaterThan) {
  // weight > 200
  MonkeyNumericQueryNode query("weight", NumericComparator::kGreaterThan, 200);
  // product1 (0): {100, 200, 300}, product2 (1): {500}, product3 (2): {1000},
  // product4 (3): {499, 500, 501}
  EXPECT_THAT(query.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(0, 1, 2, 3)));
}

TEST_F(MonkeyNumericQueryNodeTest, EvaluateQuery_LessThanEqual) {
  // weight <= 500
  MonkeyNumericQueryNode query("weight", NumericComparator::kLessThanEqual,
                               500);
  // 0: {100, 200, 300}, 1: {500}, 3: {499, 500, 501}, 4: {100}
  EXPECT_THAT(query.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(0, 1, 3, 4)));
}

TEST_F(MonkeyNumericQueryNodeTest, EvaluateQuery_Equal) {
  // price == 150
  MonkeyNumericQueryNode query("price", NumericComparator::kEqual, 150);
  EXPECT_THAT(query.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(0)));
}

TEST_F(MonkeyNumericQueryNodeTest, EvaluateQuery_NotEqual) {
  // price != 100
  MonkeyNumericQueryNode query("price", NumericComparator::kNotEqual, 100);
  // product1 (0): 150, product2 (1): 99, product3 (2): 200
  // product4 (3) is 100. product_no_price (4) doesn't have 'price'.
  EXPECT_THAT(query.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(0, 1, 2)));
}

TEST_F(MonkeyNumericQueryNodeTest, EvaluateQuery_WithNamespaceFilter) {
  // timestamp > 1680000000000, only in "events" namespace
  MonkeyNumericQueryNode query("timestamp", NumericComparator::kGreaterThan,
                               1680000000000, {"events"}, {});
  // event2 (ID 6) in "events" and timestamp > 1.68e12.
  EXPECT_THAT(query.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(6)));
}

TEST_F(MonkeyNumericQueryNodeTest, EvaluateQuery_WithSchemaTypeFilter) {
  // price < 150, only in "Product" schema
  MonkeyNumericQueryNode query("price", NumericComparator::kLessThan, 150, {},
                               {"Product"});
  // product2 (ID 1) has price 99, product4 (ID 3) has price 100. Both < 150.
  EXPECT_THAT(query.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(1, 3)));
}

TEST_F(MonkeyNumericQueryNodeTest,
       EvaluateQuery_PropertyNotNumericallyIndexable) {
  // Query for id_num == 1. "id_num" has NumericMatchType::UNKNOWN.
  MonkeyNumericQueryNode query("id_num", NumericComparator::kEqual, 1);
  EXPECT_THAT(query.EvaluateQuery(engine_.get()), IsOkAndHolds(IsEmpty()));

  // Query for duration_ms > 1000000. "duration_ms" has
  // NumericMatchType::UNKNOWN.
  MonkeyNumericQueryNode query2("duration_ms", NumericComparator::kGreaterThan,
                                1000000);
  EXPECT_THAT(query2.EvaluateQuery(engine_.get()), IsOkAndHolds(IsEmpty()));
}

TEST_F(MonkeyNumericQueryNodeTest, EvaluateQuery_PropertyPathNotFound) {
  // Query for a non-existent property path.
  MonkeyNumericQueryNode query("non_existent_prop", NumericComparator::kEqual,
                               100);
  EXPECT_THAT(query.EvaluateQuery(engine_.get()), IsOkAndHolds(IsEmpty()));
}

TEST_F(MonkeyNumericQueryNodeTest, EvaluateQuery_RepeatedPropertyMatch) {
  // weight >= 500
  MonkeyNumericQueryNode query("weight", NumericComparator::kGreaterThanEqual,
                               500);
  // product2 (1): {500}, product3 (2): {1000}, product4 (3): {499, 500, 501}
  EXPECT_THAT(query.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(1, 2, 3)));
}

}  // namespace
}  // namespace lib
}  // namespace icing