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
#include <memory>
#include <string>
#include <string_view>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/usage.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
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
using ::testing::Eq;

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
        DocumentStore::Create(&filesystem_, doc_store_dir_, &fake_clock_,
                              schema_store_.get()));
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
                    .SetDataType(TYPE_STRING)
                    .SetCardinality(CARDINALITY_OPTIONAL)))
            .Build();

    ICING_ASSERT_OK(schema_store_->SetSchema(test_email_schema));
  }

  void TearDown() override {
    document_store_.reset();
    schema_store_.reset();
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  const std::string test_dir_;
  const std::string doc_store_dir_;
  const std::string schema_store_dir_;
  Filesystem filesystem_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
  FakeClock fake_clock_;
};

constexpr double kEps = 0.0000000001;
constexpr int kDefaultScore = 0;
constexpr int64_t kDefaultCreationTimestampMs = 1571100001111;

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
                              int64 timestamp_ms,
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

TEST_F(AdvancedScorerTest, InvalidAdvancedScoringSpec) {
  // Empty scoring expression for advanced scoring
  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
  EXPECT_THAT(
      scorer_factory::Create(scoring_spec, /*default_score=*/10,
                             document_store_.get(), schema_store_.get()),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  // Non-empty scoring expression for normal scoring
  scoring_spec = ScoringSpecProto::default_instance();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  scoring_spec.set_advanced_scoring_expression("1");
  EXPECT_THAT(
      scorer_factory::Create(scoring_spec, /*default_score=*/10,
                             document_store_.get(), schema_store_.get()),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(AdvancedScorerTest, SimpleExpression) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id,
      document_store_->Put(CreateDocument("namespace", "uri")));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Scorer> scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("123"),
                             /*default_score=*/10, document_store_.get(),
                             schema_store_.get()));

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
                             /*default_score=*/10, document_store_.get(),
                             schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(3));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("-1 + 2"),
                             /*default_score=*/10, document_store_.get(),
                             schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(1));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("1 + -2"),
                             /*default_score=*/10, document_store_.get(),
                             schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(-1));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("1 - 2"),
                             /*default_score=*/10, document_store_.get(),
                             schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(-1));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("1 * 2"),
                             /*default_score=*/10, document_store_.get(),
                             schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(2));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("1 / 2"),
                             /*default_score=*/10, document_store_.get(),
                             schema_store_.get()));
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
                             /*default_score=*/10, document_store_.get(),
                             schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(3, kEps));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("log(2.718281828459045)"),
          /*default_score=*/10, document_store_.get(), schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(1, kEps));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("pow(2, 10)"),
                             /*default_score=*/10, document_store_.get(),
                             schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(1024));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("max(10, 11, 12, 13, 14)"),
          /*default_score=*/10, document_store_.get(), schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(14));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("min(10, 11, 12, 13, 14)"),
          /*default_score=*/10, document_store_.get(), schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(10));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("sqrt(2)"),
                             /*default_score=*/10, document_store_.get(),
                             schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(sqrt(2), kEps));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("abs(-2) + abs(2)"),
                             /*default_score=*/10, document_store_.get(),
                             schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(4));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("sin(3.141592653589793)"),
          /*default_score=*/10, document_store_.get(), schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(0, kEps));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("cos(3.141592653589793)"),
          /*default_score=*/10, document_store_.get(), schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(-1, kEps));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("tan(3.141592653589793 / 4)"),
          /*default_score=*/10, document_store_.get(), schema_store_.get()));
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
                             /*default_score=*/10, document_store_.get(),
                             schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(123));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("this.creationTimestamp()"),
          /*default_score=*/10, document_store_.get(), schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(kDefaultCreationTimestampMs));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec(
              "this.documentScore() + this.creationTimestamp()"),
          /*default_score=*/10, document_store_.get(), schema_store_.get()));
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
          /*default_score=*/10, document_store_.get(), schema_store_.get()));
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
          /*default_score=*/10, document_store_.get(), schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(100000));
  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("this.usageLastUsedTimestamp(2)"),
          /*default_score=*/10, document_store_.get(), schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(200000));
  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(
          CreateAdvancedScoringSpec("this.usageLastUsedTimestamp(3)"),
          /*default_score=*/10, document_store_.get(), schema_store_.get()));
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
      AdvancedScorer::Create(CreateAdvancedScoringSpec("this.usageCount(4)"),
                             default_score, document_store_.get(),
                             schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(default_score));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer, AdvancedScorer::Create(
                  CreateAdvancedScoringSpec("this.usageCount(0)"),
                  default_score, document_store_.get(), schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), Eq(default_score));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer, AdvancedScorer::Create(
                  CreateAdvancedScoringSpec("this.usageCount(1.5)"),
                  default_score, document_store_.get(), schema_store_.get()));
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
                             /*default_score=*/10, document_store_.get(),
                             schema_store_.get()));
  scorer->PrepareToScore(/*query_term_iterators=*/{});

  // Should get the default score.
  DocHitInfo docHitInfo = DocHitInfo(document_id);
  EXPECT_THAT(scorer->GetScore(docHitInfo, /*query_it=*/nullptr), Eq(10));
}

TEST_F(AdvancedScorerTest, ComplexExpression) {
  const int64_t creation_timestamp_ms = 123;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id,
      document_store_->Put(CreateDocument("namespace", "uri", /*score=*/123,
                                          creation_timestamp_ms)));
  DocHitInfo docHitInfo = DocHitInfo(document_id);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Scorer> scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec(
                                 "pow(sin(2), 2)"
                                 // This is this.usageCount(1)
                                 "+ this.usageCount(this.documentScore() - 122)"
                                 "/ 12.34"
                                 "* (10 * pow(2 * 1, sin(2))"
                                 "+ 10 * (2 + 10 + this.creationTimestamp()))"
                                 // This should evaluate to default score.
                                 "+ this.relevanceScore()"),
                             /*default_score=*/10, document_store_.get(),
                             schema_store_.get()));
  scorer->PrepareToScore(/*query_term_iterators=*/{});

  ICING_ASSERT_OK(document_store_->ReportUsage(
      CreateUsageReport("namespace", "uri", 0, UsageReport::USAGE_TYPE1)));
  ICING_ASSERT_OK(document_store_->ReportUsage(
      CreateUsageReport("namespace", "uri", 0, UsageReport::USAGE_TYPE1)));
  EXPECT_THAT(scorer->GetScore(docHitInfo),
              DoubleNear(pow(sin(2), 2) +
                             2 / 12.34 *
                                 (10 * pow(2 * 1, sin(2)) +
                                  10 * (2 + 10 + creation_timestamp_ms)) +
                             10,
                         kEps));
}

// Should be a parsing Error
TEST_F(AdvancedScorerTest, EmptyExpression) {
  EXPECT_THAT(
      AdvancedScorer::Create(CreateAdvancedScoringSpec(""),
                             /*default_score=*/10, document_store_.get(),
                             schema_store_.get()),
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
      AdvancedScorer::Create(CreateAdvancedScoringSpec("log(0)"), default_score,
                             document_store_.get(), schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(default_score, kEps));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer,
      AdvancedScorer::Create(CreateAdvancedScoringSpec("1 / 0"), default_score,
                             document_store_.get(), schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(default_score, kEps));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer, AdvancedScorer::Create(CreateAdvancedScoringSpec("sqrt(-1)"),
                                     default_score, document_store_.get(),
                                     schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(default_score, kEps));

  ICING_ASSERT_OK_AND_ASSIGN(
      scorer, AdvancedScorer::Create(CreateAdvancedScoringSpec("pow(-1, 0.5)"),
                                     default_score, document_store_.get(),
                                     schema_store_.get()));
  EXPECT_THAT(scorer->GetScore(docHitInfo), DoubleNear(default_score, kEps));
}

// The following tests should trigger a type error while the visitor tries to
// build a ScoreExpression object.
TEST_F(AdvancedScorerTest, MathTypeError) {
  const double default_score = 0;

  EXPECT_THAT(
      AdvancedScorer::Create(CreateAdvancedScoringSpec("test"), default_score,
                             document_store_.get(), schema_store_.get()),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(
      AdvancedScorer::Create(CreateAdvancedScoringSpec("log()"), default_score,
                             document_store_.get(), schema_store_.get()),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("log(1, 2, 3)"),
                                     default_score, document_store_.get(),
                                     schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("log(1, this)"),
                                     default_score, document_store_.get(),
                                     schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(
      AdvancedScorer::Create(CreateAdvancedScoringSpec("pow(1)"), default_score,
                             document_store_.get(), schema_store_.get()),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("sqrt(1, 2)"),
                                     default_score, document_store_.get(),
                                     schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("abs(1, 2)"),
                                     default_score, document_store_.get(),
                                     schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("sin(1, 2)"),
                                     default_score, document_store_.get(),
                                     schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("cos(1, 2)"),
                                     default_score, document_store_.get(),
                                     schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("tan(1, 2)"),
                                     default_score, document_store_.get(),
                                     schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(
      AdvancedScorer::Create(CreateAdvancedScoringSpec("this"), default_score,
                             document_store_.get(), schema_store_.get()),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(
      AdvancedScorer::Create(CreateAdvancedScoringSpec("-this"), default_score,
                             document_store_.get(), schema_store_.get()),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("1 + this"),
                                     default_score, document_store_.get(),
                                     schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(AdvancedScorerTest, DocumentFunctionTypeError) {
  const double default_score = 0;

  EXPECT_THAT(AdvancedScorer::Create(
                  CreateAdvancedScoringSpec("documentScore(1)"), default_score,
                  document_store_.get(), schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(AdvancedScorer::Create(
                  CreateAdvancedScoringSpec("this.creationTimestamp(1)"),
                  default_score, document_store_.get(), schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(AdvancedScorer::Create(
                  CreateAdvancedScoringSpec("this.usageCount()"), default_score,
                  document_store_.get(), schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(AdvancedScorer::Create(
                  CreateAdvancedScoringSpec("usageLastUsedTimestamp(1, 1)"),
                  default_score, document_store_.get(), schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(AdvancedScorer::Create(
                  CreateAdvancedScoringSpec("relevanceScore(1)"), default_score,
                  document_store_.get(), schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(AdvancedScorer::Create(
                  CreateAdvancedScoringSpec("documentScore(this)"),
                  default_score, document_store_.get(), schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(AdvancedScorer::Create(
                  CreateAdvancedScoringSpec("that.documentScore()"),
                  default_score, document_store_.get(), schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(AdvancedScorer::Create(
                  CreateAdvancedScoringSpec("this.this.creationTimestamp()"),
                  default_score, document_store_.get(), schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(AdvancedScorer::Create(CreateAdvancedScoringSpec("this.log(2)"),
                                     default_score, document_store_.get(),
                                     schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

}  // namespace

}  // namespace lib
}  // namespace icing