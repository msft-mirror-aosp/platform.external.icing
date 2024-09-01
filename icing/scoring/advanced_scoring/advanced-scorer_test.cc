// Copyright (C) 2022 Google LLC
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

#include "icing/scoring/advanced_scoring/advanced-scorer.h"

#include <cmath>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/index/embed/embedding-query-results.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/join/join-children-fetcher.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/usage.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/scoring/scorer-factory.h"
#include "icing/scoring/scorer.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {
using ::testing::DoubleNear;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;

class AdvancedScorerTest : public testing::Test {
 protected:
  AdvancedScorerTest()
      : test_dir_(GetTestTempDir() + "/icing"),
        doc_store_dir_(test_dir_ + "/doc_store"),
        schema_store_dir_(test_dir_ + "/schema_store") {}

  void SetUp() override {
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(doc_store_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_));

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(
            &filesystem_, doc_store_dir_, &fake_clock_, schema_store_.get(),
            /*force_recovery_and_revalidate_documents=*/false,
            /*namespace_id_fingerprint=*/true, /*pre_mapping_fbv=*/false,
            /*use_persistent_hash_map=*/true,
            PortableFileBackedProtoLog<
                DocumentWrapper>::kDeflateCompressionLevel,
            /*initialize_stats=*/nullptr));
    document_store_ = std::move(create_result.document_store);

    // Creates a simple email schema
    SchemaProto test_email_schema =
        SchemaBuilder()
            .AddType(SchemaTypeConfigBuilder().SetType("email").AddProperty(
                PropertyConfigBuilder()
                    .SetName("subject")
                    .SetDataTypeString(
                        TermMatchType::PREFIX,
                        StringIndexingConfig::TokenizerType::PLAIN)
                    .SetCardinality(CARDINALITY_OPTIONAL)))
            .AddType(SchemaTypeConfigBuilder()
                         .SetType("person")
                         .AddProperty(
                             PropertyConfigBuilder()
                                 .SetName("emailAddress")
                                 .SetDataTypeString(
                                     TermMatchType::PREFIX,
                                     StringIndexingConfig::TokenizerType::PLAIN)
                                 .SetCardinality(CARDINALITY_OPTIONAL))
                         .AddProperty(
                             PropertyConfigBuilder()
                                 .SetName("name")
                                 .SetDataTypeString(
                                     TermMatchType::PREFIX,
                                     StringIndexingConfig::TokenizerType::PLAIN)
                                 .SetCardinality(CARDINALITY_OPTIONAL))

                         .AddProperty(
                             PropertyConfigBuilder()
                                 .SetName("phoneNumber")
                                 .SetDataTypeString(
                                     TermMatchType::PREFIX,
                                     StringIndexingConfig::TokenizerType::PLAIN)
                                 .SetCardinality(CARDINALITY_OPTIONAL)))
            .Build();

    ICING_ASSERT_OK(schema_store_->SetSchema(
        test_email_schema, /*ignore_errors_and_delete_documents=*/false,
        /*allow_circular_schema_definitions=*/false));
  }

  void TearDown() override {
    document_store_.reset();
    schema_store_.reset();
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  const std::string test_dir_;
  const std::string doc_store_dir_;
  const std::string schema_store_dir_;
  EmbeddingQueryResults empty_embedding_query_results_;
  Filesystem filesystem_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
  FakeClock fake_clock_;
};

constexpr double kEps = 0.0000001;
constexpr int kDefaultScore = 0;
constexpr int64_t kDefaultCreationTimestampMs = 1571100001111;
constexpr SearchSpecProto::EmbeddingQueryMetricType::Code
    kDefaultSemanticMetricType =
        SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT;

DocumentProto CreateDocument(
    const std::string& name_space, const std::string& uri,
    int score = kDefaultScore,
    int64_t creation_timestamp_ms = kDefaultCreationTimestampMs) {
  return DocumentBuilder()
      .SetKey(name_space, uri)
      .SetSchema("email")
      .SetScore(score)
      .SetCreationTimestampMs(creation_timestamp_ms)
      .Build();
}

UsageReport CreateUsageReport(std::string name_space, std::string uri,
                              int64_t timestamp_ms,
                              UsageReport::UsageType usage_type) {
  UsageReport usage_report;
  usage_report.set_document_namespace(name_space);
  usage_report.set_document_uri(uri);
  usage_report.set_usage_timestamp_ms(timestamp_ms);
  usage_report.set_usage_type(usage_type);
  return usage_report;
}

ScoringSpecProto CreateAdvancedScoringSpec(
    const std::string& advanced_scoring_expression) {
  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  scoring_spec.set_advanced_scoring_expression(advanced_scoring_expression);
  return scoring_spec;
}

PropertyWeight CreatePropertyWeight(std::string path, double weight) {
  PropertyWeight property_weight;
  property_weight.set_path(std::move(path));
  property_weight.set_weight(weight);
  return property_weight;
}

TypePropertyWeights CreateTypePropertyWeights(
    std::string schema_type, std::vector<PropertyWeight>&& property_weights) {
  TypePropertyWeights type_property_weights;
  type_property_weights.set_schema_type(std::move(schema_type));
  type_property_weights.mutable_property_weights()->Reserve(
      property_weights.size());

  for (PropertyWeight& property_weight : property_weights) {
    *type_property_weights.add_property_weights() = std::move(property_weight);
  }

  return type_property_weights;
}

TEST_F(AdvancedScorerTest, InvalidAdvancedScoringSpec) {
  // Empty scoring expression for advanced scoring
  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  EXPECT_THAT(scorer_factory::Create(scoring_spec, /*default_score=*/10,
                                     kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  // Non-empty scoring expression for normal scoring
  scoring_spec = ScoringSpecProto::default_instance();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  scoring_spec.set_advanced_scoring_expression("1");
  EXPECT_THAT(scorer_factory::Create(scoring_spec, /*default_score=*/10,
                                     kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(AdvancedScorerTest, SimpleExpression) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id,
      document_store_->Put(CreateDocument("namespace", "uri")));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Scorer> scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("123"),
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));

  DocHitInfo docHitInfo = DocHitInfo(document_id);

  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(123));
}

TEST_F(AdvancedScorerTest, BasicPureArithmeticExpression) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id,
      document_store_->Put(CreateDocument("namespace", "uri")));
  DocHitInfo docHitInfo = DocHitInfo(document_id);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Scorer> scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("1 + 2"),
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(3));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("-1 + 2"),
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(1));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("1 + -2"),
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(-1));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("1 - 2"),
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(-1));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("1 * 2"),
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(2));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("1 / 2"),
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(0.5));
}

TEST_F(AdvancedScorerTest, BasicMathFunctionExpression) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id,
      document_store_->Put(CreateDocument("namespace", "uri")));
  DocHitInfo docHitInfo = DocHitInfo(document_id);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Scorer> scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("log(10, 1000)"),
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(3, kEps));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("log(2.718281828459045)"),
          /*default_score=*/10, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(1, kEps));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("pow(2, 10)"),
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(1024));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("max(10, 11, 12, 13, 14)"),
          /*default_score=*/10, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(14));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("min(10, 11, 12, 13, 14)"),
          /*default_score=*/10, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(10));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("len(10, 11, 12, 13, 14)"),
          /*default_score=*/10, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(5));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("sum(10, 11, 12, 13, 14)"),
          /*default_score=*/10, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(10 + 11 + 12 + 13 + 14));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("avg(10, 11, 12, 13, 14)"),
          /*default_score=*/10, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq((10 + 11 + 12 + 13 + 14) / 5.));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("sqrt(2)"),
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(sqrt(2), kEps));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("abs(-2) + abs(2)"),
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(4));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("sin(3.141592653589793)"),
          /*default_score=*/10, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(0, kEps));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("cos(3.141592653589793)"),
          /*default_score=*/10, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(-1, kEps));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("tan(3.141592653589793 / 4)"),
          /*default_score=*/10, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(1, kEps));
}

TEST_F(AdvancedScorerTest, DocumentScoreCreationTimestampFunctionExpression) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id,
      document_store_->Put(CreateDocument(
          "namespace", "uri", /*score=*/123,
          /*creation_timestamp_ms=*/kDefaultCreationTimestampMs)));
  DocHitInfo docHitInfo = DocHitInfo(document_id);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Scorer> scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("this.documentScore()"),
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(123));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("this.creationTimestamp()"),
          /*default_score=*/10, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(kDefaultCreationTimestampMs));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec(
              "this.documentScore() + this.creationTimestamp()"),
          /*default_score=*/10, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo),
              Eq(123 + kDefaultCreationTimestampMs));
}

TEST_F(AdvancedScorerTest, DocumentUsageFunctionExpression) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id,
      document_store_->Put(CreateDocument("namespace", "uri")));
  DocHitInfo docHitInfo = DocHitInfo(document_id);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Scorer> scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("this.usageCount(1) + this.usageCount(2) "
                                    "+ this.usageLastUsedTimestamp(3)"),
          /*default_score=*/10, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(0));
  ICING_ASSERT_OK(document_store_->ReportUsage(
      CreateUsageReport("namespace", "uri", 100000, UsageReport::USAGE_TYPE1)));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(1));
  ICING_ASSERT_OK(document_store_->ReportUsage(
      CreateUsageReport("namespace", "uri", 200000, UsageReport::USAGE_TYPE2)));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(2));
  ICING_ASSERT_OK(document_store_->ReportUsage(
      CreateUsageReport("namespace", "uri", 300000, UsageReport::USAGE_TYPE3)));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(300002));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("this.usageLastUsedTimestamp(1)"),
          /*default_score=*/10, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(100000));
  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("this.usageLastUsedTimestamp(2)"),
          /*default_score=*/10, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(200000));
  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("this.usageLastUsedTimestamp(3)"),
          /*default_score=*/10, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(300000));
}

TEST_F(AdvancedScorerTest, DocumentUsageFunctionOutOfRange) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id,
      document_store_->Put(CreateDocument("namespace", "uri")));
  DocHitInfo docHitInfo = DocHitInfo(document_id);

  const double default_score = 123;

  // Should get default score for the following expressions that cause "runtime"
  // errors.

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Scorer> scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("this.usageCount(4)"), default_score,
          kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(default_score));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("this.usageCount(0)"), default_score,
          kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(default_score));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("this.usageCount(1.5)"), default_score,
          kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(default_score));
}

// scoring-processor_test.cc will help to get better test coverage for relevance
// score.
TEST_F(AdvancedScorerTest, RelevanceScoreFunctionScoreExpression) {
  DocumentProto test_document =
      DocumentBuilder()
          .SetScore(5)
          .SetKey("namespace", "uri")
          .SetSchema("email")
          .AddStringProperty("subject", "subject foo")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(test_document));
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<AdvancedScorer> scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("this.relevanceScore()"),
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  scorer->PrepareToScore(/*query_term_iterators=*/{});

  // Should get the default score.
  DocHitInfo docHitInfo = DocHitInfo(document_id);
  EXPECT_THAT(scorer->GetScore(docHitInfo, /*query_it=*/nullptr), Eq(10));
}

TEST_F(AdvancedScorerTest, ChildrenScoresFunctionScoreExpression) {
  const double default_score = 123;

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id_1,
      document_store_->Put(CreateDocument("namespace", "uri1")));
  DocHitInfo docHitInfo1 = DocHitInfo(document_id_1);
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id_2,
      document_store_->Put(CreateDocument("namespace", "uri2")));
  DocHitInfo docHitInfo2 = DocHitInfo(document_id_2);
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id_3,
      document_store_->Put(CreateDocument("namespace", "uri3")));
  DocHitInfo docHitInfo3 = DocHitInfo(document_id_3);

  // Create a JoinChildrenFetcher that matches:
  //   document_id_1 to fake_child1 with score 1 and fake_child2 with score 2.
  //   document_id_2 to fake_child3 with score 4.
  //   document_id_3 has no child.
  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression("this.qualifiedId()");
  join_spec.set_child_property_expression("sender");
  std::unordered_map<DocumentId, std::vector<ScoredDocumentHit>>
      map_joinable_qualified_id;
  ScoredDocumentHit fake_child1(/*document_id=*/10, kSectionIdMaskNone,
                                /*score=*/1.0);
  ScoredDocumentHit fake_child2(/*document_id=*/11, kSectionIdMaskNone,
                                /*score=*/2.0);
  ScoredDocumentHit fake_child3(/*document_id=*/12, kSectionIdMaskNone,
                                /*score=*/4.0);
  map_joinable_qualified_id[document_id_1].push_back(fake_child1);
  map_joinable_qualified_id[document_id_1].push_back(fake_child2);
  map_joinable_qualified_id[document_id_2].push_back(fake_child3);
  JoinChildrenFetcher fetcher(join_spec, std::move(map_joinable_qualified_id));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<AdvancedScorer> scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("len(this.childrenRankingSignals())"),
          default_score, kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          &fetcher, &empty_embedding_query_results_));
  // document_id_1 has two children.
  EXPECT_THAT(scorer->GetScore(docHitInfo1, /*query_it=*/nullptr), Eq(2));
  // document_id_2 has one child.
  EXPECT_THAT(scorer->GetScore(docHitInfo2, /*query_it=*/nullptr), Eq(1));
  // document_id_3 has no child.
  EXPECT_THAT(scorer->GetScore(docHitInfo3, /*query_it=*/nullptr), Eq(0));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("sum(this.childrenRankingSignals())"),
          default_score, kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          &fetcher, &empty_embedding_query_results_));
  // document_id_1 has two children with scores 1 and 2.
  EXPECT_THAT(scorer->GetScore(docHitInfo1, /*query_it=*/nullptr), Eq(3));
  // document_id_2 has one child with score 4.
  EXPECT_THAT(scorer->GetScore(docHitInfo2, /*query_it=*/nullptr), Eq(4));
  // document_id_3 has no child.
  EXPECT_THAT(scorer->GetScore(docHitInfo3, /*query_it=*/nullptr), Eq(0));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("avg(this.childrenRankingSignals())"),
          default_score, kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          &fetcher, &empty_embedding_query_results_));
  // document_id_1 has two children with scores 1 and 2.
  EXPECT_THAT(scorer->GetScore(docHitInfo1, /*query_it=*/nullptr), Eq(3 / 2.));
  // document_id_2 has one child with score 4.
  EXPECT_THAT(scorer->GetScore(docHitInfo2, /*query_it=*/nullptr), Eq(4 / 1.));
  // document_id_3 has no child.
  // This is an evaluation error, so default_score will be returned.
  EXPECT_THAT(scorer->GetScore(docHitInfo3, /*query_it=*/nullptr),
              Eq(default_score));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec(
              // Equivalent to "avg(this.childrenRankingSignals())"
              "sum(this.childrenRankingSignals()) / "
              "len(this.childrenRankingSignals())"),
          default_score, kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          &fetcher, &empty_embedding_query_results_));
  // document_id_1 has two children with scores 1 and 2.
  EXPECT_THAT(scorer->GetScore(docHitInfo1, /*query_it=*/nullptr), Eq(3 / 2.));
  // document_id_2 has one child with score 4.
  EXPECT_THAT(scorer->GetScore(docHitInfo2, /*query_it=*/nullptr), Eq(4 / 1.));
  // document_id_3 has no child.
  // This is an evaluation error, so default_score will be returned.
  EXPECT_THAT(scorer->GetScore(docHitInfo3, /*query_it=*/nullptr),
              Eq(default_score));
}

TEST_F(AdvancedScorerTest, PropertyWeightsFunctionScoreExpression) {
  DocumentProto test_document_1 =
      DocumentBuilder().SetKey("namespace", "uri1").SetSchema("email").Build();
  DocumentProto test_document_2 =
      DocumentBuilder().SetKey("namespace", "uri2").SetSchema("person").Build();
  DocumentProto test_document_3 =
      DocumentBuilder().SetKey("namespace", "uri3").SetSchema("person").Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id_1,
                             document_store_->Put(test_document_1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id_2,
                             document_store_->Put(test_document_2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id_3,
                             document_store_->Put(test_document_3));

  ScoringSpecProto spec_proto = CreateAdvancedScoringSpec("");

  *spec_proto.add_type_property_weights() = CreateTypePropertyWeights(
      /*schema_type=*/"email",
      {CreatePropertyWeight(/*path=*/"subject", /*weight=*/1.0)});
  *spec_proto.add_type_property_weights() = CreateTypePropertyWeights(
      /*schema_type=*/"person",
      {CreatePropertyWeight(/*path=*/"emailAddress", /*weight=*/0.5),
       CreatePropertyWeight(/*path=*/"name", /*weight=*/0.8),
       CreatePropertyWeight(/*path=*/"phoneNumber", /*weight=*/1.0)});

  // Let the hit for test_document_1 match property "subject".
  // So this.propertyWeights() for test_document_1 will return [1].
  DocHitInfo doc_hit_info_1 = DocHitInfo(document_id_1);
  doc_hit_info_1.UpdateSection(0);

  // Let the hit for test_document_2 match properties "emailAddress" and "name".
  // So this.propertyWeights() for test_document_2 will return [0.5, 0.8].
  DocHitInfo doc_hit_info_2 = DocHitInfo(document_id_2);
  doc_hit_info_2.UpdateSection(0);
  doc_hit_info_2.UpdateSection(1);

  // Let the hit for test_document_3 match properties "emailAddress", "name" and
  // "phoneNumber". So this.propertyWeights() for test_document_3 will return
  // [0.5, 0.8, 1].
  DocHitInfo doc_hit_info_3 = DocHitInfo(document_id_3);
  doc_hit_info_3.UpdateSection(0);
  doc_hit_info_3.UpdateSection(1);
  doc_hit_info_3.UpdateSection(2);

  spec_proto.set_advanced_scoring_expression("min(this.propertyWeights())");
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<AdvancedScorer> scorer,
      AdvancedScorer::Create(spec_proto,
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  // min([1]) = 1
  EXPECT_THAT(scorer->GetScore(doc_hit_info_1, /*query_it=*/nullptr), Eq(1));
  // min([0.5, 0.8]) = 0.5
  EXPECT_THAT(scorer->GetScore(doc_hit_info_2, /*query_it=*/nullptr), Eq(0.5));
  // min([0.5, 0.8, 1.0]) = 0.5
  EXPECT_THAT(scorer->GetScore(doc_hit_info_3, /*query_it=*/nullptr), Eq(0.5));

  spec_proto.set_advanced_scoring_expression("max(this.propertyWeights())");
  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(spec_proto,
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  // max([1]) = 1
  EXPECT_THAT(scorer->GetScore(doc_hit_info_1, /*query_it=*/nullptr), Eq(1));
  // max([0.5, 0.8]) = 0.8
  EXPECT_THAT(scorer->GetScore(doc_hit_info_2, /*query_it=*/nullptr), Eq(0.8));
  // max([0.5, 0.8, 1.0]) = 1
  EXPECT_THAT(scorer->GetScore(doc_hit_info_3, /*query_it=*/nullptr), Eq(1));

  spec_proto.set_advanced_scoring_expression("sum(this.propertyWeights())");
  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(spec_proto,
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  // sum([1]) = 1
  EXPECT_THAT(scorer->GetScore(doc_hit_info_1, /*query_it=*/nullptr), Eq(1));
  // sum([0.5, 0.8]) = 1.3
  EXPECT_THAT(scorer->GetScore(doc_hit_info_2, /*query_it=*/nullptr), Eq(1.3));
  // sum([0.5, 0.8, 1.0]) = 2.3
  EXPECT_THAT(scorer->GetScore(doc_hit_info_3, /*query_it=*/nullptr), Eq(2.3));
}

TEST_F(AdvancedScorerTest,
       PropertyWeightsFunctionScoreExpressionUnspecifiedWeights) {
  DocumentProto test_document_1 =
      DocumentBuilder().SetKey("namespace", "uri1").SetSchema("email").Build();
  DocumentProto test_document_2 =
      DocumentBuilder().SetKey("namespace", "uri2").SetSchema("person").Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id_1,
                             document_store_->Put(test_document_1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id_2,
                             document_store_->Put(test_document_2));

  ScoringSpecProto spec_proto = CreateAdvancedScoringSpec("");

  // The entry for type "email" is missing, so every properties in "email"
  // should get weight 1.0.
  // The weight of "phoneNumber" in "person" type is unspecified, which should
  // default to 1/2 = 0.5
  *spec_proto.add_type_property_weights() = CreateTypePropertyWeights(
      /*schema_type=*/"person",
      {CreatePropertyWeight(/*path=*/"emailAddress", /*weight=*/1.0),
       CreatePropertyWeight(/*path=*/"name", /*weight=*/2)});

  // Let the hit for test_document_1 match property "subject".
  // So this.propertyWeights() for test_document_1 will return [1].
  DocHitInfo doc_hit_info_1 = DocHitInfo(document_id_1);
  doc_hit_info_1.UpdateSection(0);

  // Let the hit for test_document_2 match properties "emailAddress", "name" and
  // "phoneNumber". So this.propertyWeights() for test_document_3 will return
  // [0.5, 1, 0.5].
  DocHitInfo doc_hit_info_2 = DocHitInfo(document_id_2);
  doc_hit_info_2.UpdateSection(0);
  doc_hit_info_2.UpdateSection(1);
  doc_hit_info_2.UpdateSection(2);

  spec_proto.set_advanced_scoring_expression("min(this.propertyWeights())");
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<AdvancedScorer> scorer,
      AdvancedScorer::Create(spec_proto,
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  // min([1]) = 1
  EXPECT_THAT(scorer->GetScore(doc_hit_info_1, /*query_it=*/nullptr), Eq(1));
  // min([0.5, 1, 0.5]) = 0.5
  EXPECT_THAT(scorer->GetScore(doc_hit_info_2, /*query_it=*/nullptr), Eq(0.5));

  spec_proto.set_advanced_scoring_expression("max(this.propertyWeights())");
  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(spec_proto,
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  // max([1]) = 1
  EXPECT_THAT(scorer->GetScore(doc_hit_info_1, /*query_it=*/nullptr), Eq(1));
  // max([0.5, 1, 0.5]) = 1
  EXPECT_THAT(scorer->GetScore(doc_hit_info_2, /*query_it=*/nullptr), Eq(1));

  spec_proto.set_advanced_scoring_expression("sum(this.propertyWeights())");
  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(spec_proto,
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  // sum([1]) = 1
  EXPECT_THAT(scorer->GetScore(doc_hit_info_1, /*query_it=*/nullptr), Eq(1));
  // sum([0.5, 1, 0.5]) = 2
  EXPECT_THAT(scorer->GetScore(doc_hit_info_2, /*query_it=*/nullptr), Eq(2));
}

TEST_F(AdvancedScorerTest, InvalidChildrenScoresFunctionScoreExpression) {
  const double default_score = 123;

  // Without join_children_fetcher provided,
  // "len(this.childrenRankingSignals())" cannot be created.
  EXPECT_THAT(
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("len(this.childrenRankingSignals())"),
          default_score, kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  // The root expression can only be of double type, but here it is of list
  // type.
  JoinChildrenFetcher fake_fetcher(JoinSpecProto::default_instance(),
                                   /*map_joinable_qualified_id=*/{});
  EXPECT_THAT(
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("this.childrenRankingSignals()"),
          default_score, kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          &fake_fetcher, &empty_embedding_query_results_),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(AdvancedScorerTest, ComplexExpression) {
  const int64_t creation_timestamp_ms = 123;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id,
      document_store_->Put(CreateDocument("namespace", "uri", /*score=*/123,
                                          creation_timestamp_ms)));
  DocHitInfo docHitInfo = DocHitInfo(document_id);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<AdvancedScorer> scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec(
                                 "pow(sin(2), 2)"
                                 // This is this.usageCount(1)
                                 "+ this.usageCount(this.documentScore() - 122)"
                                 "/ 12.34"
                                 "* (10 * pow(2 * 1, sin(2))"
                                 "+ 10 * (2 + 10 + this.creationTimestamp()))"
                                 // This should evaluate to default score.
                                 "+ this.relevanceScore()"),
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  EXPECT_FALSE(scorer->is_constant());
  scorer->PrepareToScore(/*query_term_iterators=*/{});

  ICING_ASSERT_OK(document_store_->ReportUsage(
      CreateUsageReport("namespace", "uri", 0, UsageReport::USAGE_TYPE1)));
  ICING_ASSERT_OK(document_store_->ReportUsage(
      CreateUsageReport("namespace", "uri", 0, UsageReport::USAGE_TYPE1)));
  EXPECT_THAT(scorer->GetScore(docHitInfo, /*query_it=*/nullptr),
              DoubleNear(pow(sin(2), 2) +
                             2 / 12.34 *
                                 (10 * pow(2 * 1, sin(2)) +
                                  10 * (2 + 10 + creation_timestamp_ms)) +
                             10,
                         kEps));
}

TEST_F(AdvancedScorerTest, ConstantExpression) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<AdvancedScorer> scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec(
                                 "pow(sin(2), 2)"
                                 "+ log(2, 122) / 12.34"
                                 "* (10 * pow(2 * 1, sin(2)) + 10 * (2 + 10))"),
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  EXPECT_TRUE(scorer->is_constant());
}

// Should be a parsing Error
TEST_F(AdvancedScorerTest, EmptyExpression) {
  EXPECT_THAT(
      AdvancedScorer::Create(CreateAdvancedScoringSpec(""),
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(AdvancedScorerTest, EvaluationErrorShouldReturnDefaultScore) {
  const double default_score = 123;

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id,
      document_store_->Put(CreateDocument("namespace", "uri")));
  DocHitInfo docHitInfo = DocHitInfo(document_id);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Scorer> scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("log(0)"), default_score,
          kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(default_score, kEps));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer, AdvancedScorer::Create(CreateAdvancedScoringSpec("1 / 0"),
                                     default_score, kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(default_score, kEps));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer, AdvancedScorer::Create(CreateAdvancedScoringSpec("sqrt(-1)"),
                                     default_score, kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(default_score, kEps));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer, AdvancedScorer::Create(CreateAdvancedScoringSpec("pow(-1, 0.5)"),
                                     default_score, kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(default_score, kEps));
}

// The following tests should trigger a type error while the visitor tries to
// build a ScoreExpression object.
TEST_F(AdvancedScorerTest, MathTypeError) {
  const double default_score = 0;

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("test"),
                                     default_score, kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("log()"),
                                     default_score, kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("log(1, 2, 3)"),
                                     default_score, kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("log(1, this)"),
                                     default_score, kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("pow(1)"),
                                     default_score, kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("sqrt(1, 2)"),
                                     default_score, kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("abs(1, 2)"),
                                     default_score, kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("sin(1, 2)"),
                                     default_score, kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("cos(1, 2)"),
                                     default_score, kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("tan(1, 2)"),
                                     default_score, kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("this"),
                                     default_score, kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("-this"),
                                     default_score, kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("1 + this"),
                                     default_score, kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(AdvancedScorerTest, DocumentFunctionTypeError) {
  const double default_score = 0;

  EXPECT_THAT(
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("documentScore(1)"), default_score,
          kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("this.creationTimestamp(1)"), default_score,
          kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("this.usageCount()"), default_score,
          kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("usageLastUsedTimestamp(1, 1)"),
          default_score, kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("relevanceScore(1)"), default_score,
          kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("documentScore(this)"), default_score,
          kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("that.documentScore()"), default_score,
          kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("this.this.creationTimestamp()"),
          default_score, kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("this.log(2)"),
                                     default_score, kDefaultSemanticMetricType,
                                     document_store_.get(), schema_store_.get(),
                                     fake_clock_.GetSystemTimeMilliseconds(),
                                     /*join_children_fetcher=*/nullptr,
                                     &empty_embedding_query_results_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(AdvancedScorerTest,
       MatchedSemanticScoresFunctionScoreExpressionTypeError) {
  libtextclassifier3::StatusOr<std::unique_ptr<AdvancedScorer>> scorer_or =
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec(
              "sum(matchedSemanticScores(getEmbeddingParameter(0)))"),
          kDefaultSemanticMetricType, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_);
  EXPECT_THAT(scorer_or,
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(scorer_or.status().error_message(),
              HasSubstr("not called with \"this\""));

  scorer_or = AdvancedScorer::Create(
      CreateAdvancedScoringSpec("sum(this.matchedSemanticScores(0))"),
      kDefaultSemanticMetricType, kDefaultSemanticMetricType,
      document_store_.get(), schema_store_.get(),
      fake_clock_.GetSystemTimeMilliseconds(),
      /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_);
  EXPECT_THAT(scorer_or,
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(scorer_or.status().error_message(),
              HasSubstr("got invalid argument type for embedding vector"));

  scorer_or = AdvancedScorer::Create(
      CreateAdvancedScoringSpec(
          "sum(this.matchedSemanticScores(getEmbeddingParameter(0), 0))"),
      kDefaultSemanticMetricType, kDefaultSemanticMetricType,
      document_store_.get(), schema_store_.get(),
      fake_clock_.GetSystemTimeMilliseconds(),
      /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_);
  EXPECT_THAT(scorer_or,
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(scorer_or.status().error_message(),
              HasSubstr("Embedding metric can only be given as a string"));

  scorer_or = AdvancedScorer::Create(
      CreateAdvancedScoringSpec("sum(this.matchedSemanticScores("
                                "getEmbeddingParameter(0), \"COSINE\", 0))"),
      kDefaultSemanticMetricType, kDefaultSemanticMetricType,
      document_store_.get(), schema_store_.get(),
      fake_clock_.GetSystemTimeMilliseconds(),
      /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_);
  EXPECT_THAT(scorer_or,
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(scorer_or.status().error_message(),
              HasSubstr("got invalid number of arguments"));

  scorer_or = AdvancedScorer::Create(
      CreateAdvancedScoringSpec("sum(this.matchedSemanticScores("
                                "getEmbeddingParameter(0), \"COSIGN\"))"),
      kDefaultSemanticMetricType, kDefaultSemanticMetricType,
      document_store_.get(), schema_store_.get(),
      fake_clock_.GetSystemTimeMilliseconds(),
      /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_);
  EXPECT_THAT(scorer_or,
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(scorer_or.status().error_message(),
              HasSubstr("Unknown metric type: COSIGN"));

  scorer_or = AdvancedScorer::Create(
      CreateAdvancedScoringSpec(
          "sum(this.matchedSemanticScores(getEmbeddingParameter(\"0\")))"),
      kDefaultSemanticMetricType, kDefaultSemanticMetricType,
      document_store_.get(), schema_store_.get(),
      fake_clock_.GetSystemTimeMilliseconds(),
      /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_);
  EXPECT_THAT(scorer_or,
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(scorer_or.status().error_message(),
              HasSubstr("getEmbeddingParameter got invalid argument type"));

  scorer_or = AdvancedScorer::Create(
      CreateAdvancedScoringSpec(
          "sum(this.matchedSemanticScores(getEmbeddingParameter()))"),
      kDefaultSemanticMetricType, kDefaultSemanticMetricType,
      document_store_.get(), schema_store_.get(),
      fake_clock_.GetSystemTimeMilliseconds(),
      /*join_children_fetcher=*/nullptr, &empty_embedding_query_results_);
  EXPECT_THAT(scorer_or,
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(scorer_or.status().error_message(),
              HasSubstr("getEmbeddingParameter must have 1 argument"));
}

void AddEntryToEmbeddingQueryScoreMap(
    EmbeddingQueryResults::EmbeddingQueryScoreMap& score_map,
    double semantic_score, DocumentId document_id) {
  score_map[document_id].push_back(semantic_score);
}

TEST_F(AdvancedScorerTest, MatchedSemanticScoresFunctionScoreExpression) {
  DocumentId document_id_0 = 0;
  DocumentId document_id_1 = 1;
  DocHitInfo doc_hit_info_0(document_id_0);
  DocHitInfo doc_hit_info_1(document_id_1);
  EmbeddingQueryResults embedding_query_results;

  // Let the first query assign the following semantic scores:
  // COSINE:
  //   Document 0: 0.1, 0.2
  //   Document 1: 0.3, 0.4
  // DOT_PRODUCT:
  //   Document 0: 0.5
  //   Document 1: 0.6
  // EUCLIDEAN:
  //   Document 0: 0.7
  //   Document 1: 0.8
  EmbeddingQueryResults::EmbeddingQueryScoreMap* score_map =
      &embedding_query_results
           .result_scores[0][SearchSpecProto::EmbeddingQueryMetricType::COSINE];
  AddEntryToEmbeddingQueryScoreMap(*score_map,
                                   /*semantic_score=*/0.1, document_id_0);
  AddEntryToEmbeddingQueryScoreMap(*score_map,
                                   /*semantic_score=*/0.2, document_id_0);
  AddEntryToEmbeddingQueryScoreMap(*score_map,
                                   /*semantic_score=*/0.3, document_id_1);
  AddEntryToEmbeddingQueryScoreMap(*score_map,
                                   /*semantic_score=*/0.4, document_id_1);
  score_map = &embedding_query_results.result_scores
                   [0][SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT];
  AddEntryToEmbeddingQueryScoreMap(*score_map,
                                   /*semantic_score=*/0.5, document_id_0);
  AddEntryToEmbeddingQueryScoreMap(*score_map,
                                   /*semantic_score=*/0.6, document_id_1);
  score_map =
      &embedding_query_results
           .result_scores[0]
                         [SearchSpecProto::EmbeddingQueryMetricType::EUCLIDEAN];
  AddEntryToEmbeddingQueryScoreMap(*score_map,
                                   /*semantic_score=*/0.7, document_id_0);
  AddEntryToEmbeddingQueryScoreMap(*score_map,
                                   /*semantic_score=*/0.8, document_id_1);

  // Let the second query only assign DOT_PRODUCT scores:
  // DOT_PRODUCT:
  //   Document 0: 0.1
  //   Document 1: 0.2
  score_map = &embedding_query_results.result_scores
                   [1][SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT];
  AddEntryToEmbeddingQueryScoreMap(*score_map,
                                   /*semantic_score=*/0.1, document_id_0);
  AddEntryToEmbeddingQueryScoreMap(*score_map,
                                   /*semantic_score=*/0.2, document_id_1);

  // Get semantic scores for default metric (DOT_PRODUCT) for the first query.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Scorer> scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec(
              "sum(this.matchedSemanticScores(getEmbeddingParameter(0)))"),
          kDefaultScore, /*default_semantic_metric_type=*/
          SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &embedding_query_results));
  EXPECT_THAT(scorer->GetScore(doc_hit_info_0), DoubleNear(0.5, kEps));
  EXPECT_THAT(scorer->GetScore(doc_hit_info_1), DoubleNear(0.6, kEps));

  // Get semantic scores for a metric overriding the default one for the first
  // query.
  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("sum(this.matchedSemanticScores("
                                    "getEmbeddingParameter(0), \"COSINE\"))"),
          kDefaultScore, /*default_semantic_metric_type=*/
          SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &embedding_query_results));
  EXPECT_THAT(scorer->GetScore(doc_hit_info_0), DoubleNear(0.1 + 0.2, kEps));
  EXPECT_THAT(scorer->GetScore(doc_hit_info_1), DoubleNear(0.3 + 0.4, kEps));

  // Get semantic scores for multiple metrics for the first query.
  ICING_ASSERT_OK_AND_ASSIGN(
      scorer, AdvancedScorer::Create(
                  CreateAdvancedScoringSpec(
                      "sum(this.matchedSemanticScores(getEmbeddingParameter(0)"
                      ", \"COSINE\")) + "
                      "sum(this.matchedSemanticScores(getEmbeddingParameter(0)"
                      ", \"DOT_PRODUCT\")) + "
                      "sum(this.matchedSemanticScores(getEmbeddingParameter(0)"
                      ", \"EUCLIDEAN\"))"),
                  kDefaultScore, /*default_semantic_metric_type=*/
                  SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT,
                  document_store_.get(), schema_store_.get(),
                  fake_clock_.GetSystemTimeMilliseconds(),
                  /*join_children_fetcher=*/nullptr, &embedding_query_results));
  EXPECT_THAT(scorer->GetScore(doc_hit_info_0),
              DoubleNear(0.1 + 0.2 + 0.5 + 0.7, kEps));
  EXPECT_THAT(scorer->GetScore(doc_hit_info_1),
              DoubleNear(0.3 + 0.4 + 0.6 + 0.8, kEps));

  // Get semantic scores for the second query.
  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec(
              "sum(this.matchedSemanticScores(getEmbeddingParameter(1)))"),
          kDefaultScore, /*default_semantic_metric_type=*/
          SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &embedding_query_results));
  EXPECT_THAT(scorer->GetScore(doc_hit_info_0), DoubleNear(0.1, kEps));
  EXPECT_THAT(scorer->GetScore(doc_hit_info_1), DoubleNear(0.2, kEps));

  // The second query does not contain cosine scores.
  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("sum(this.matchedSemanticScores("
                                    "getEmbeddingParameter(1), \"COSINE\"))"),
          kDefaultScore, /*default_semantic_metric_type=*/
          SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &embedding_query_results));
  EXPECT_THAT(scorer->GetScore(doc_hit_info_0), DoubleNear(0, kEps));
  EXPECT_THAT(scorer->GetScore(doc_hit_info_1), DoubleNear(0, kEps));
}

TEST_F(AdvancedScorerTest, ListRelatedFunctions) {
  DocumentId document_id_0 = 0;
  DocHitInfo doc_hit_info_0(document_id_0);
  EmbeddingQueryResults embedding_query_results;

  // Construct a score map so that:
  // - this.matchedSemanticScores(getEmbeddingParameter(0)) returns
  //   {4, 5, 2, 1, 3}.
  // - this.matchedSemanticScores(getEmbeddingParameter(1)) returns an empty
  //   list.
  EmbeddingQueryResults::EmbeddingQueryScoreMap* score_map =
      &embedding_query_results
           .result_scores[0][SearchSpecProto::EmbeddingQueryMetricType::COSINE];
  AddEntryToEmbeddingQueryScoreMap(*score_map,
                                   /*semantic_score=*/4, document_id_0);
  AddEntryToEmbeddingQueryScoreMap(*score_map,
                                   /*semantic_score=*/5, document_id_0);
  AddEntryToEmbeddingQueryScoreMap(*score_map,
                                   /*semantic_score=*/2, document_id_0);
  AddEntryToEmbeddingQueryScoreMap(*score_map,
                                   /*semantic_score=*/1, document_id_0);
  AddEntryToEmbeddingQueryScoreMap(*score_map,
                                   /*semantic_score=*/3, document_id_0);

  // maxOrDefault({4, 5, 2, 1, 3}, 100) = 5
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Scorer> scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("maxOrDefault(this.matchedSemanticScores("
                                    "getEmbeddingParameter(0)), 100)"),
          kDefaultScore, /*default_semantic_metric_type=*/
          SearchSpecProto::EmbeddingQueryMetricType::COSINE,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &embedding_query_results));
  EXPECT_THAT(scorer->GetScore(doc_hit_info_0), DoubleNear(5, kEps));

  // minOrDefault({4, 5, 2, 1, 3}, -100) = 1
  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("minOrDefault(this.matchedSemanticScores("
                                    "getEmbeddingParameter(0)), -100)"),
          kDefaultScore, /*default_semantic_metric_type=*/
          SearchSpecProto::EmbeddingQueryMetricType::COSINE,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &embedding_query_results));
  EXPECT_THAT(scorer->GetScore(doc_hit_info_0), DoubleNear(1, kEps));

  // maxOrDefault({}, 100) = 100
  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("maxOrDefault(this.matchedSemanticScores("
                                    "getEmbeddingParameter(1)), 100)"),
          kDefaultScore, /*default_semantic_metric_type=*/
          SearchSpecProto::EmbeddingQueryMetricType::COSINE,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &embedding_query_results));
  EXPECT_THAT(scorer->GetScore(doc_hit_info_0), DoubleNear(100, kEps));

  // minOrDefault({}, -100) = -100
  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("minOrDefault(this.matchedSemanticScores("
                                    "getEmbeddingParameter(1)), -100)"),
          kDefaultScore, /*default_semantic_metric_type=*/
          SearchSpecProto::EmbeddingQueryMetricType::COSINE,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr, &embedding_query_results));
  EXPECT_THAT(scorer->GetScore(doc_hit_info_0), DoubleNear(-100, kEps));

  // sum(filterByRange({4, 5, 2, 1, 3}, 2, 4)) = sum({4, 2, 3}) = 9
  ICING_ASSERT_OK_AND_ASSIGN(
      scorer, AdvancedScorer::Create(
                  CreateAdvancedScoringSpec(
                      "sum(filterByRange(this.matchedSemanticScores("
                      "getEmbeddingParameter(0)), 2, 4))"),
                  kDefaultScore, /*default_semantic_metric_type=*/
                  SearchSpecProto::EmbeddingQueryMetricType::COSINE,
                  document_store_.get(), schema_store_.get(),
                  fake_clock_.GetSystemTimeMilliseconds(),
                  /*join_children_fetcher=*/nullptr, &embedding_query_results));
  EXPECT_THAT(scorer->GetScore(doc_hit_info_0), DoubleNear(9, kEps));
}

TEST_F(AdvancedScorerTest, AdditionalScores) {
  const int64_t creation_timestamp_ms = 123;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id,
      document_store_->Put(CreateDocument("namespace", "uri", /*score=*/123,
                                          creation_timestamp_ms)));
  DocHitInfo docHitInfo = DocHitInfo(document_id);

  ScoringSpecProto scoring_spec =
      // This evaluates to 123
      CreateAdvancedScoringSpec("this.documentScore()");
  // This evaluates to 4
  scoring_spec.add_additional_advanced_scoring_expressions("pow(2, 2)");
  // This evaluates to 123 + 4 = 127
  scoring_spec.add_additional_advanced_scoring_expressions(
      "this.documentScore() + pow(2, 2)");

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<AdvancedScorer> scorer,
      AdvancedScorer::Create(scoring_spec,
                             /*default_score=*/10, kDefaultSemanticMetricType,
                             document_store_.get(), schema_store_.get(),
                             fake_clock_.GetSystemTimeMilliseconds(),
                             /*join_children_fetcher=*/nullptr,
                             &empty_embedding_query_results_));
  EXPECT_FALSE(scorer->is_constant());
  scorer->PrepareToScore(/*query_term_iterators=*/{});
  EXPECT_THAT(scorer->GetScore(docHitInfo, /*query_it=*/nullptr),
              DoubleNear(123, kEps));
  std::vector<double> additional_scores =
      scorer->GetAdditionalScores(docHitInfo, /*query_it=*/nullptr);
  EXPECT_THAT(additional_scores,
              ElementsAre(DoubleNear(4, kEps), DoubleNear(127, kEps)));
}

}  // namespace

}  // namespace lib
}  // namespace icing
