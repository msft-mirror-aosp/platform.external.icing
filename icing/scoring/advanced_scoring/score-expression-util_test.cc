// Copyright (C) 2024 Google LLC
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

#include "icing/scoring/advanced_scoring/score-expression-util.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/feature-flags.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/usage.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/scoring/advanced_scoring/score-expression.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::testing::DoubleEq;
using ::testing::HasSubstr;

class ScoreExpressionUtilTest : public testing::Test {
 protected:
  ScoreExpressionUtilTest()
      : test_dir_(GetTestTempDir() + "/icing"),
        doc_store_dir_(test_dir_ + "/doc_store"),
        schema_store_dir_(test_dir_ + "/schema_store") {}

  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    scoring_feature_types_enabled_ = {
        ScoringFeatureType::SCORABLE_PROPERTY_RANKING};
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(doc_store_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, schema_store_dir_,
                                           &fake_clock_, feature_flags_.get()));

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(&filesystem_, doc_store_dir_, &fake_clock_,
                              schema_store_.get(), feature_flags_.get(),
                              /*force_recovery_and_revalidate_documents=*/false,
                              /*pre_mapping_fbv=*/false,
                              /*use_persistent_hash_map=*/true,
                              PortableFileBackedProtoLog<
                                  DocumentWrapper>::kDefaultCompressionLevel,
                              /*initialize_stats=*/nullptr));
    document_store_ = std::move(create_result.document_store);

    // Creates the schema
    SchemaProto test_schema =
        SchemaBuilder()
            .AddType(SchemaTypeConfigBuilder().SetType("email").AddProperty(
                PropertyConfigBuilder()
                    .SetName("subject")
                    .SetDataTypeString(
                        TermMatchType::PREFIX,
                        StringIndexingConfig::TokenizerType::PLAIN)
                    .SetCardinality(CARDINALITY_OPTIONAL)))
            .Build();

    ICING_ASSERT_OK(schema_store_->SetSchema(
        std::move(test_schema), /*ignore_errors_and_delete_documents=*/false));
  }

  void TearDown() override {
    document_store_.reset();
    schema_store_.reset();
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  std::unordered_set<ScoringFeatureType> scoring_feature_types_enabled_;
  Filesystem filesystem_;
  FakeClock fake_clock_;
  const std::string test_dir_;
  const std::string doc_store_dir_;
  const std::string schema_store_dir_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
};

constexpr int kDefaultScore = 0;
constexpr SearchSpecProto::EmbeddingQueryMetricType::Code
    kDefaultSemanticMetricType =
        SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT;

DocumentProto CreateDocument(const std::string& name_space,
                             const std::string& uri, int score,
                             int64_t creation_timestamp_ms) {
  return DocumentBuilder()
      .SetKey(name_space, uri)
      .SetSchema("email")
      .SetScore(score)
      .SetCreationTimestampMs(creation_timestamp_ms)
      .Build();
}

TEST_F(ScoreExpressionUtilTest,
       FunctionsAreBannedIfMissingCorrespondingDependencies) {
  EXPECT_THAT(
      score_expression_util::GetScoreExpression(
          "len(this.childrenRankingSignals())", kDefaultScore,
          kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr,
          /*embedding_query_results=*/nullptr, /*section_weights=*/nullptr,
          /*bm25f_calculator=*/nullptr, /*schema_type_alias_map=*/nullptr,
          feature_flags_.get(), &scoring_feature_types_enabled_),
      StatusIs(
          libtextclassifier3::StatusCode::INVALID_ARGUMENT,
          HasSubstr("childrenRankingSignals must only be used with join")));

  EXPECT_THAT(
      score_expression_util::GetScoreExpression(
          "sum(this.matchedSemanticScores(getEmbeddingParameter(0)))",
          kDefaultScore, kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr,
          /*embedding_query_results=*/nullptr, /*section_weights=*/nullptr,
          /*bm25f_calculator=*/nullptr, /*schema_type_alias_map=*/nullptr,
          feature_flags_.get(), &scoring_feature_types_enabled_),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
               HasSubstr("matchedSemanticScores function is not "
                         "available in this context")));

  EXPECT_THAT(
      score_expression_util::GetScoreExpression(
          "sum(this.propertyWeights())", kDefaultScore,
          kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr,
          /*embedding_query_results=*/nullptr, /*section_weights=*/nullptr,
          /*bm25f_calculator=*/nullptr, /*schema_type_alias_map=*/nullptr,
          feature_flags_.get(), &scoring_feature_types_enabled_),
      StatusIs(
          libtextclassifier3::StatusCode::INVALID_ARGUMENT,
          HasSubstr(
              "propertyWeights function is not available in this context")));

  EXPECT_THAT(
      score_expression_util::GetScoreExpression(
          "this.relevanceScore()", kDefaultScore, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr,
          /*embedding_query_results=*/nullptr, /*section_weights=*/nullptr,
          /*bm25f_calculator=*/nullptr, /*schema_type_alias_map=*/nullptr,
          feature_flags_.get(), &scoring_feature_types_enabled_),
      StatusIs(
          libtextclassifier3::StatusCode::INVALID_ARGUMENT,
          HasSubstr(
              "relevanceScore function is not available in this context")));

  EXPECT_THAT(
      score_expression_util::GetScoreExpression(
          "sum(getScorableProperty(\"aliasPerson\", \"contactTimes\"))",
          kDefaultScore, kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr,
          /*embedding_query_results=*/nullptr, /*section_weights=*/nullptr,
          /*bm25f_calculator=*/nullptr, /*schema_type_alias_map=*/nullptr,
          feature_flags_.get(), &scoring_feature_types_enabled_),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
               HasSubstr("getScorableProperty function is not "
                         "available in this context")));
}

TEST_F(ScoreExpressionUtilTest, SimpleExpression) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result,
      document_store_->Put(CreateDocument("namespace", "uri", /*score=*/10,
                                          /*creation_timestamp_ms=*/100)));
  DocumentId document_id = put_result.new_document_id;
  DocHitInfo doc_hit_info = DocHitInfo(document_id);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScoreExpression> score_expression,
      score_expression_util::GetScoreExpression(
          "1 + 1", kDefaultScore, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr,
          /*embedding_query_results=*/nullptr, /*section_weights=*/nullptr,
          /*bm25f_calculator=*/nullptr, /*schema_type_alias_map=*/nullptr,
          feature_flags_.get(), &scoring_feature_types_enabled_));

  EXPECT_THAT(
      score_expression->EvaluateDouble(doc_hit_info, /*query_it=*/nullptr),
      IsOkAndHolds(DoubleEq(2)));
}

TEST_F(ScoreExpressionUtilTest, DocumentFunctionsWithoutOptionalDependencies) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result,
      document_store_->Put(CreateDocument("namespace", "uri", /*score=*/10,
                                          /*creation_timestamp_ms=*/100)));
  DocumentId document_id = put_result.new_document_id;
  DocHitInfo doc_hit_info = DocHitInfo(document_id);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScoreExpression> score_expression,
      score_expression_util::GetScoreExpression(
          "this.documentScore()", kDefaultScore, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr,
          /*embedding_query_results=*/nullptr, /*section_weights=*/nullptr,
          /*bm25f_calculator=*/nullptr, /*schema_type_alias_map=*/nullptr,
          feature_flags_.get(), &scoring_feature_types_enabled_));
  EXPECT_THAT(
      score_expression->EvaluateDouble(doc_hit_info, /*query_it=*/nullptr),
      IsOkAndHolds(DoubleEq(10)));

  ICING_ASSERT_OK_AND_ASSIGN(
      score_expression,
      score_expression_util::GetScoreExpression(
          "this.creationTimestamp()", kDefaultScore, kDefaultSemanticMetricType,
          document_store_.get(), schema_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr,
          /*embedding_query_results=*/nullptr, /*section_weights=*/nullptr,
          /*bm25f_calculator=*/nullptr, /*schema_type_alias_map=*/nullptr,
          feature_flags_.get(), &scoring_feature_types_enabled_));
  EXPECT_THAT(
      score_expression->EvaluateDouble(doc_hit_info, /*query_it=*/nullptr),
      IsOkAndHolds(DoubleEq(100)));

  ICING_ASSERT_OK_AND_ASSIGN(
      score_expression,
      score_expression_util::GetScoreExpression(
          "this.documentScore() + this.creationTimestamp()", kDefaultScore,
          kDefaultSemanticMetricType, document_store_.get(),
          schema_store_.get(), fake_clock_.GetSystemTimeMilliseconds(),
          /*join_children_fetcher=*/nullptr,
          /*embedding_query_results=*/nullptr, /*section_weights=*/nullptr,
          /*bm25f_calculator=*/nullptr, /*schema_type_alias_map=*/nullptr,
          feature_flags_.get(), &scoring_feature_types_enabled_));
  EXPECT_THAT(
      score_expression->EvaluateDouble(doc_hit_info, /*query_it=*/nullptr),
      IsOkAndHolds(DoubleEq(10 + 100)));
}

}  // namespace

}  // namespace lib
}  // namespace icing
