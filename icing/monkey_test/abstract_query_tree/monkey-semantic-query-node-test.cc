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

#include "icing/monkey_test/abstract_query_tree/monkey-semantic-query-node.h"

#include <memory>
#include <string>
#include <string_view>
#include <vector>

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
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {
namespace {

using ::testing::Eq;
using ::testing::UnorderedElementsAre;

PropertyProto::VectorProto CreateVector(const std::vector<float>& values,
                                        std::string_view model_signature) {
  PropertyProto::VectorProto vector;
  vector.mutable_values()->Add(values.begin(), values.end());
  vector.set_model_signature(model_signature);
  return vector;
}

class MonkeySemanticQueryNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    SchemaProto schema =
        SchemaBuilder()
            .AddType(SchemaTypeConfigBuilder()
                         .SetType("Message")
                         .AddProperty(
                             PropertyConfigBuilder()
                                 .SetName("embedding")
                                 .SetDataTypeVector(
                                     EmbeddingIndexingConfig::
                                         EmbeddingIndexingType::LINEAR_SEARCH,
                                     EmbeddingIndexingConfig::QuantizationType::
                                         QUANTIZE_8_BIT))
                         .AddProperty(
                             PropertyConfigBuilder()
                                 .SetName("embedding2")
                                 .SetDataTypeVector(
                                     EmbeddingIndexingConfig::
                                         EmbeddingIndexingType::LINEAR_SEARCH,
                                     EmbeddingIndexingConfig::QuantizationType::
                                         QUANTIZE_8_BIT)))
            .Build();
    engine_->SetSchema(schema);

    // Doc 1: No embedding vectors
    doc1_.document.set_schema("Message");
    doc1_.document.set_namespace_("ns");
    doc1_.document.set_uri("uri1");
    engine_->Put(doc1_);

    // Doc 2: Different model signature
    doc2_.document.set_schema("Message");
    doc2_.document.set_namespace_("ns");
    doc2_.document.set_uri("uri2");
    doc2_.sections.push_back(
        MonkeySection{.path = "embedding",
                      .vector_values = {CreateVector({0.8f, 0.6f}, "model2")}});
    engine_->Put(doc2_);

    // Doc 3: Different size
    doc3_.document.set_schema("Message");
    doc3_.document.set_namespace_("ns");
    doc3_.document.set_uri("uri3");
    doc3_.sections.push_back(MonkeySection{
        .path = "embedding",
        .vector_values = {CreateVector({1.0f, 0.0f, 0.0f}, "model1")}});
    engine_->Put(doc3_);

    // Doc 4: Score 0.8 - MATCHES [0.6, 0.9]
    doc4_.document.set_schema("Message");
    doc4_.document.set_namespace_("ns");
    doc4_.document.set_uri("uri4");
    doc4_.sections.push_back(
        MonkeySection{.path = "embedding",
                      .vector_values = {CreateVector({0.8f, 0.6f}, "model1")}});
    engine_->Put(doc4_);

    // Doc 5: Score 0.0 - score < min
    doc5_.document.set_schema("Message");
    doc5_.document.set_namespace_("ns");
    doc5_.document.set_uri("uri5");
    doc5_.sections.push_back(
        MonkeySection{.path = "embedding",
                      .vector_values = {CreateVector({0.0f, 1.0f}, "model1")}});
    engine_->Put(doc5_);

    // Doc 6: Score 1.0 - score > max
    doc6_.document.set_schema("Message");
    doc6_.document.set_namespace_("ns");
    doc6_.document.set_uri("uri6");
    doc6_.sections.push_back(
        MonkeySection{.path = "embedding",
                      .vector_values = {CreateVector({1.0f, 0.0f}, "model1")}});
    engine_->Put(doc6_);

    // Doc 7: Two vectors, one matches (score 0.8), one doesn't (score 0.0)
    // - MATCHES [0.6, 0.9]
    doc7_.document.set_schema("Message");
    doc7_.document.set_namespace_("ns");
    doc7_.document.set_uri("uri7");
    doc7_.sections.push_back(
        MonkeySection{.path = "embedding",
                      .vector_values = {CreateVector({0.0f, 1.0f}, "model1"),
                                        CreateVector({0.8f, 0.6f}, "model1")}});
    engine_->Put(doc7_);

    // Doc 8: Matching score in "embedding2", non-matching in "embedding"
    doc8_.document.set_schema("Message");
    doc8_.document.set_namespace_("ns");
    doc8_.document.set_uri("uri8");
    doc8_.sections.push_back(
        MonkeySection{.path = "embedding",
                      .vector_values = {CreateVector({0.0f, 1.0f}, "model1")}});
    doc8_.sections.push_back(
        MonkeySection{.path = "embedding2",
                      .vector_values = {CreateVector({0.8f, 0.6f}, "model1")}});
    engine_->Put(doc8_);
  }

  MonkeyTestRandomEngine random_{/*seed=*/0};
  std::unique_ptr<InMemoryIcingSearchEngine> engine_ =
      std::make_unique<InMemoryIcingSearchEngine>(&random_);
  MonkeyTokenizedDocument doc1_, doc2_, doc3_, doc4_, doc5_, doc6_, doc7_,
      doc8_;
};

TEST_F(MonkeySemanticQueryNodeTest, SemanticSearch) {
  PropertyProto::VectorProto query_vector =
      CreateVector({1.0f, 0.0f}, "model1");
  MonkeySemanticQueryNode node(
      /*vector_index=*/0, /*min_score=*/0.6,
      /*max_score=*/0.9, /*distance_metric=*/
      SearchSpecProto::EmbeddingQueryMetricType::COSINE, /*nprobe=*/0,
      query_vector);

  EXPECT_THAT(node.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(3, 6, 7)));
}

TEST_F(MonkeySemanticQueryNodeTest, SemanticSearchWithPropertyRestrict) {
  PropertyProto::VectorProto query_vector =
      CreateVector({1.0f, 0.0f}, "model1");
  MonkeySemanticQueryNode node(
      /*vector_index=*/0, /*min_score=*/0.6, /*max_score=*/0.9,
      SearchSpecProto::EmbeddingQueryMetricType::COSINE, /*nprobe=*/0,
      query_vector,
      /*property_restricts=*/{"embedding2"}, /*document_namespaces=*/{},
      /*document_schema_types=*/{});

  // Only doc8_ matches because its "embedding2" section matches.
  // doc4_ and doc7_ have matching vectors in "embedding" section, but not
  // "embedding2".
  EXPECT_THAT(node.EvaluateQuery(engine_.get()),
              IsOkAndHolds(UnorderedElementsAre(7)));
}

TEST_F(MonkeySemanticQueryNodeTest, GenerateQueryString) {
  PropertyProto::VectorProto query_vector =
      CreateVector({1.0f, 0.0f}, "model1");
  MonkeySemanticQueryNode basic_node(
      /*vector_index=*/0, /*min_score=*/0.6,
      /*max_score=*/0.9, /*distance_metric=*/
      SearchSpecProto::EmbeddingQueryMetricType::COSINE, /*nprobe=*/0,
      query_vector);

  EXPECT_THAT(basic_node.GenerateQueryString(),
              Eq("semanticSearch(getEmbeddingParameter(0), 0.60, 0.90, "
                 "\"COSINE\")"));

  MonkeySemanticQueryNode property_restrict_node(
      /*vector_index=*/0, /*min_score=*/0.6,
      /*max_score=*/0.9, /*distance_metric=*/
      SearchSpecProto::EmbeddingQueryMetricType::COSINE, /*nprobe=*/0,
      query_vector,
      /*property_restricts=*/{"embedding2"}, /*document_namespaces=*/{},
      /*document_schema_types=*/{});
  EXPECT_THAT(property_restrict_node.GenerateQueryString(),
              Eq("embedding2:semanticSearch(getEmbeddingParameter(0), 0.60, "
                 "0.90, \"COSINE\")"));

  MonkeySemanticQueryNode dot_product_distance_metric_node(
      /*vector_index=*/0, /*min_score=*/0.6,
      /*max_score=*/0.9, /*distance_metric=*/
      SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT, /*nprobe=*/0,
      query_vector,
      /*property_restricts=*/{"embedding2"}, /*document_namespaces=*/{},
      /*document_schema_types=*/{});
  EXPECT_THAT(dot_product_distance_metric_node.GenerateQueryString(),
              Eq("embedding2:semanticSearch(getEmbeddingParameter(0), 0.60, "
                 "0.90, \"DOT_PRODUCT\")"));
}

}  // namespace
}  // namespace lib
}  // namespace icing
