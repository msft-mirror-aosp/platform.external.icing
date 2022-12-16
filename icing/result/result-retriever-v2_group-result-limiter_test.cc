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

#include <memory>
#include <vector>

#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/portable/equals-proto.h"
#include "icing/portable/platform.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/result/page-result.h"
#include "icing/result/result-retriever-v2.h"
#include "icing/result/result-state-v2.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/scoring/priority-queue-scored-document-hits-ranker.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/store/document-id.h"
#include "icing/store/namespace-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/icu-data-file-helper.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/transform/normalizer-factory.h"
#include "icing/transform/normalizer.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::Pair;
using ::testing::Pointee;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;

class ResultRetrieverV2GroupResultLimiterTest : public testing::Test {
 protected:
  ResultRetrieverV2GroupResultLimiterTest()
      : test_dir_(GetTestTempDir() + "/icing") {
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());
  }

  void SetUp() override {
    if (!IsCfStringTokenization() && !IsReverseJniTokenization()) {
      ICING_ASSERT_OK(
          // File generated via icu_data_file rule in //icing/BUILD.
          icu_data_file_helper::SetUpICUDataFile(
              GetTestFilePath("icing/icu.dat")));
    }
    language_segmenter_factory::SegmenterOptions options(ULOC_US);
    ICING_ASSERT_OK_AND_ASSIGN(
        language_segmenter_,
        language_segmenter_factory::Create(std::move(options)));

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_,
        SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
    ICING_ASSERT_OK_AND_ASSIGN(normalizer_, normalizer_factory::Create(
                                                /*max_term_byte_size=*/10000));

    SchemaProto schema;
    schema.add_types()->set_schema_type("Document");
    ICING_ASSERT_OK(schema_store_->SetSchema(std::move(schema)));

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                              schema_store_.get()));
    document_store_ = std::move(create_result.document_store);
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  const Filesystem filesystem_;
  const std::string test_dir_;
  std::unique_ptr<LanguageSegmenter> language_segmenter_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<Normalizer> normalizer_;
  std::unique_ptr<DocumentStore> document_store_;
  FakeClock fake_clock_;
};

// TODO(sungyc): Refactor helper functions below (builder classes or common test
//               utility).

SearchSpecProto CreateSearchSpec(TermMatchType::Code match_type) {
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(match_type);
  return search_spec;
}

ScoringSpecProto CreateScoringSpec(bool is_descending_order) {
  ScoringSpecProto scoring_spec;
  scoring_spec.set_order_by(is_descending_order ? ScoringSpecProto::Order::DESC
                                                : ScoringSpecProto::Order::ASC);
  return scoring_spec;
}

ResultSpecProto CreateResultSpec(int num_per_page) {
  ResultSpecProto result_spec;
  result_spec.set_num_per_page(num_per_page);
  return result_spec;
}

TEST_F(ResultRetrieverV2GroupResultLimiterTest,
       ResultGroupingShouldLimitResults) {
  // Creates 2 documents and ensures the relationship in terms of document
  // score is: document1 < document2
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace", "uri/1")
                                .SetSchema("Document")
                                .SetScore(1)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(document1));

  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace", "uri/2")
                                .SetSchema("Document")
                                .SetScore(2)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(document2));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      ScoredDocumentHit(document_id1, kSectionIdMaskNone, document1.score()),
      ScoredDocumentHit(document_id2, kSectionIdMaskNone, document2.score())};

  // Create a ResultSpec that limits "namespace" to a single result.
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/5);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  result_grouping->set_max_results(1);
  result_grouping->add_namespaces("namespace");

  // Creates a ResultState with 2 ScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits), /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      *document_store_);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(document_store_.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  // Only the top ranked document in "namespace" (document2), should be
  // returned.
  auto [page_result, has_more_results] =
      result_retriever->RetrieveNextPage(result_state);
  ASSERT_THAT(page_result.results, SizeIs(1));
  EXPECT_THAT(page_result.results.at(0).document(), EqualsProto(document2));
  // Document1 has not been returned due to GroupResultLimiter, but since it was
  // "filtered out", there should be no more results.
  EXPECT_FALSE(has_more_results);
}

TEST_F(ResultRetrieverV2GroupResultLimiterTest,
       ResultGroupingHasEmptyFirstPage) {
  // Creates 2 documents and ensures the relationship in terms of document
  // score is: document1 < document2
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace", "uri/1")
                                .SetSchema("Document")
                                .SetScore(1)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(document1));

  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace", "uri/2")
                                .SetSchema("Document")
                                .SetScore(2)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(document2));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      ScoredDocumentHit(document_id1, kSectionIdMaskNone, document1.score()),
      ScoredDocumentHit(document_id2, kSectionIdMaskNone, document2.score())};

  // Create a ResultSpec that limits "namespace" to 0 results.
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/1);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  result_grouping->set_max_results(0);
  result_grouping->add_namespaces("namespace");

  // Creates a ResultState with 2 ScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits), /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      *document_store_);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(document_store_.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  // First page: empty page
  auto [page_result, has_more_results] =
      result_retriever->RetrieveNextPage(result_state);
  ASSERT_THAT(page_result.results, IsEmpty());
  EXPECT_FALSE(has_more_results);
}

TEST_F(ResultRetrieverV2GroupResultLimiterTest,
       ResultGroupingHasEmptyLastPage) {
  // Creates 4 documents and ensures the relationship in terms of document
  // score is: document1 < document2 < document3 < document4
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace", "uri/1")
                                .SetSchema("Document")
                                .SetScore(1)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(document1));

  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace", "uri/2")
                                .SetSchema("Document")
                                .SetScore(2)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(document2));

  DocumentProto document3 = DocumentBuilder()
                                .SetKey("namespace", "uri/3")
                                .SetSchema("Document")
                                .SetScore(3)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             document_store_->Put(document3));

  DocumentProto document4 = DocumentBuilder()
                                .SetKey("namespace", "uri/4")
                                .SetSchema("Document")
                                .SetScore(4)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id4,
                             document_store_->Put(document4));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      ScoredDocumentHit(document_id1, kSectionIdMaskNone, document1.score()),
      ScoredDocumentHit(document_id2, kSectionIdMaskNone, document2.score()),
      ScoredDocumentHit(document_id3, kSectionIdMaskNone, document3.score()),
      ScoredDocumentHit(document_id4, kSectionIdMaskNone, document4.score())};

  // Create a ResultSpec that limits "namespace" to 2 results.
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/2);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  result_grouping->set_max_results(2);
  result_grouping->add_namespaces("namespace");

  // Creates a ResultState with 4 ScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits), /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      *document_store_);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(document_store_.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  // First page: document4 and document3 should be returned.
  auto [page_result1, has_more_results1] =
      result_retriever->RetrieveNextPage(result_state);
  ASSERT_THAT(page_result1.results, SizeIs(2));
  EXPECT_THAT(page_result1.results.at(0).document(), EqualsProto(document4));
  EXPECT_THAT(page_result1.results.at(1).document(), EqualsProto(document3));
  EXPECT_TRUE(has_more_results1);

  // Second page: although there are valid document hits in result state, all of
  // them will be filtered out by group result limiter, so we should get an
  // empty page.
  auto [page_result2, has_more_results2] =
      result_retriever->RetrieveNextPage(result_state);
  EXPECT_THAT(page_result2.results, SizeIs(0));
  EXPECT_FALSE(has_more_results2);
}

TEST_F(ResultRetrieverV2GroupResultLimiterTest,
       ResultGroupingDoesNotLimitOtherNamespaceResults) {
  // Creates 4 documents and ensures the relationship in terms of document
  // score is: document1 < document2 < document3 < document4
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace1", "uri/1")
                                .SetSchema("Document")
                                .SetScore(1)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(document1));

  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace1", "uri/2")
                                .SetSchema("Document")
                                .SetScore(2)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(document2));

  DocumentProto document3 = DocumentBuilder()
                                .SetKey("namespace2", "uri/3")
                                .SetSchema("Document")
                                .SetScore(3)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             document_store_->Put(document3));

  DocumentProto document4 = DocumentBuilder()
                                .SetKey("namespace2", "uri/4")
                                .SetSchema("Document")
                                .SetScore(4)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id4,
                             document_store_->Put(document4));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      ScoredDocumentHit(document_id1, kSectionIdMaskNone, document1.score()),
      ScoredDocumentHit(document_id2, kSectionIdMaskNone, document2.score()),
      ScoredDocumentHit(document_id3, kSectionIdMaskNone, document3.score()),
      ScoredDocumentHit(document_id4, kSectionIdMaskNone, document4.score())};

  // Create a ResultSpec that limits "namespace1" to a single result, but
  // doesn't limit "namespace2".
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/5);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  result_grouping->set_max_results(1);
  result_grouping->add_namespaces("namespace1");

  // Creates a ResultState with 4 ScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits), /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      *document_store_);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(document_store_.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  // All documents in "namespace2" should be returned.
  PageResult page_result =
      result_retriever->RetrieveNextPage(result_state).first;
  ASSERT_THAT(page_result.results, SizeIs(3));
  EXPECT_THAT(page_result.results.at(0).document(), EqualsProto(document4));
  EXPECT_THAT(page_result.results.at(1).document(), EqualsProto(document3));
  EXPECT_THAT(page_result.results.at(2).document(), EqualsProto(document2));
}

TEST_F(ResultRetrieverV2GroupResultLimiterTest,
       ResultGroupingNonexistentNamespaceShouldBeIgnored) {
  // Creates 2 documents and ensures the relationship in terms of document
  // score is: document1 < document2
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace", "uri/1")
                                .SetSchema("Document")
                                .SetScore(1)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(document1));

  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace", "uri/2")
                                .SetSchema("Document")
                                .SetScore(2)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(document2));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      ScoredDocumentHit(document_id1, kSectionIdMaskNone, document1.score()),
      ScoredDocumentHit(document_id2, kSectionIdMaskNone, document2.score())};

  // Create a ResultSpec that limits "namespace"+"nonExistentNamespace" to a
  // single result.
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/5);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  result_grouping->set_max_results(1);
  result_grouping->add_namespaces("namespace");
  result_grouping->add_namespaces("nonexistentNamespace");

  // Creates a ResultState with 2 ScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits), /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      *document_store_);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(document_store_.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  // Only the top ranked document in "namespace" (document2), should be
  // returned. The presence of "nonexistentNamespace" in the same result
  // grouping should have no effect.
  PageResult page_result =
      result_retriever->RetrieveNextPage(result_state).first;
  ASSERT_THAT(page_result.results, SizeIs(1));
  EXPECT_THAT(page_result.results.at(0).document(), EqualsProto(document2));
}

TEST_F(ResultRetrieverV2GroupResultLimiterTest,
       ResultGroupingMultiNamespaceGrouping) {
  // Creates 6 documents and ensures the relationship in terms of document
  // score is: document1 < document2 < document3 < document4 < document5 <
  // document6
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace1", "uri/1")
                                .SetSchema("Document")
                                .SetScore(1)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(document1));

  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace1", "uri/2")
                                .SetSchema("Document")
                                .SetScore(2)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(document2));

  DocumentProto document3 = DocumentBuilder()
                                .SetKey("namespace2", "uri/3")
                                .SetSchema("Document")
                                .SetScore(3)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             document_store_->Put(document3));

  DocumentProto document4 = DocumentBuilder()
                                .SetKey("namespace2", "uri/4")
                                .SetSchema("Document")
                                .SetScore(4)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id4,
                             document_store_->Put(document4));

  DocumentProto document5 = DocumentBuilder()
                                .SetKey("namespace3", "uri/5")
                                .SetSchema("Document")
                                .SetScore(5)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id5,
                             document_store_->Put(document5));

  DocumentProto document6 = DocumentBuilder()
                                .SetKey("namespace3", "uri/6")
                                .SetSchema("Document")
                                .SetScore(6)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id6,
                             document_store_->Put(document6));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      ScoredDocumentHit(document_id1, kSectionIdMaskNone, document1.score()),
      ScoredDocumentHit(document_id2, kSectionIdMaskNone, document2.score()),
      ScoredDocumentHit(document_id3, kSectionIdMaskNone, document3.score()),
      ScoredDocumentHit(document_id4, kSectionIdMaskNone, document4.score()),
      ScoredDocumentHit(document_id5, kSectionIdMaskNone, document5.score()),
      ScoredDocumentHit(document_id6, kSectionIdMaskNone, document6.score())};

  // Create a ResultSpec that limits "namespace1" to a single result and limits
  // "namespace2"+"namespace3" to a total of two results.
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/5);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  result_grouping->set_max_results(1);
  result_grouping->add_namespaces("namespace1");
  result_grouping = result_spec.add_result_groupings();
  result_grouping->set_max_results(2);
  result_grouping->add_namespaces("namespace2");
  result_grouping->add_namespaces("namespace3");

  // Creates a ResultState with 6 ScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits), /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      *document_store_);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(document_store_.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  // Only the top-ranked result in "namespace1" (document2) should be returned.
  // Only the top-ranked results across "namespace2" and "namespace3"
  // (document6, document5) should be returned.
  PageResult page_result =
      result_retriever->RetrieveNextPage(result_state).first;
  ASSERT_THAT(page_result.results, SizeIs(3));
  EXPECT_THAT(page_result.results.at(0).document(), EqualsProto(document6));
  EXPECT_THAT(page_result.results.at(1).document(), EqualsProto(document5));
  EXPECT_THAT(page_result.results.at(2).document(), EqualsProto(document2));
}

TEST_F(ResultRetrieverV2GroupResultLimiterTest,
       ResultGroupingOnlyNonexistentNamespaces) {
  // Creates 2 documents and ensures the relationship in terms of document
  // score is: document1 < document2
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace", "uri/1")
                                .SetSchema("Document")
                                .SetScore(1)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(document1));

  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace", "uri/2")
                                .SetSchema("Document")
                                .SetScore(2)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(document2));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      ScoredDocumentHit(document_id1, kSectionIdMaskNone, document1.score()),
      ScoredDocumentHit(document_id2, kSectionIdMaskNone, document2.score())};

  // Create a ResultSpec that limits "nonexistentNamespace" to a single result.
  // but doesn't limit "namespace"
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/5);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  result_grouping->set_max_results(1);
  result_grouping->add_namespaces("nonexistentNamespace");

  // Creates a ResultState with 2 ScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits), /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      *document_store_);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(document_store_.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  // All documents in "namespace" should be returned. The presence of
  // "nonexistentNamespace" should have no effect.
  PageResult page_result =
      result_retriever->RetrieveNextPage(result_state).first;
  ASSERT_THAT(page_result.results, SizeIs(2));
  EXPECT_THAT(page_result.results.at(0).document(), EqualsProto(document2));
  EXPECT_THAT(page_result.results.at(1).document(), EqualsProto(document1));
}

TEST_F(ResultRetrieverV2GroupResultLimiterTest,
       ShouldUpdateResultStateCorrectlyWithGroupResultLimiter) {
  // Creates 5 documents and ensures the relationship in terms of document
  // score is: document1 < document2 < document3 < document4 < document5
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace2", "uri/1")
                                .SetSchema("Document")
                                .SetScore(1)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(document1));

  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace1", "uri/2")
                                .SetSchema("Document")
                                .SetScore(2)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(document2));

  DocumentProto document3 = DocumentBuilder()
                                .SetKey("namespace1", "uri/3")
                                .SetSchema("Document")
                                .SetScore(3)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             document_store_->Put(document3));

  DocumentProto document4 = DocumentBuilder()
                                .SetKey("namespace2", "uri/4")
                                .SetSchema("Document")
                                .SetScore(4)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id4,
                             document_store_->Put(document4));

  DocumentProto document5 = DocumentBuilder()
                                .SetKey("namespace2", "uri/5")
                                .SetSchema("Document")
                                .SetScore(5)
                                .SetCreationTimestampMs(1000)
                                .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id5,
                             document_store_->Put(document5));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      ScoredDocumentHit(document_id1, kSectionIdMaskNone, document1.score()),
      ScoredDocumentHit(document_id2, kSectionIdMaskNone, document2.score()),
      ScoredDocumentHit(document_id3, kSectionIdMaskNone, document3.score()),
      ScoredDocumentHit(document_id4, kSectionIdMaskNone, document4.score()),
      ScoredDocumentHit(document_id5, kSectionIdMaskNone, document5.score())};

  // Create a ResultSpec that limits "namespace1" to 3 results and "namespace2"
  // to a single result.
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/2);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  result_grouping->set_max_results(3);
  result_grouping->add_namespaces("namespace1");
  result_grouping = result_spec.add_result_groupings();
  result_grouping->set_max_results(1);
  result_grouping->add_namespaces("namespace2");

  // Get namespace ids.
  ICING_ASSERT_OK_AND_ASSIGN(NamespaceId namespace_id1,
                             document_store_->GetNamespaceId("namespace1"));
  ICING_ASSERT_OK_AND_ASSIGN(NamespaceId namespace_id2,
                             document_store_->GetNamespaceId("namespace2"));

  // Creates a ResultState with 5 ScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits), /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      *document_store_);
  {
    absl_ports::shared_lock l(&result_state.mutex);

    ASSERT_THAT(
        result_state.namespace_group_id_map(),
        UnorderedElementsAre(Pair(namespace_id1, 0), Pair(namespace_id2, 1)));
    ASSERT_THAT(result_state.group_result_limits, ElementsAre(3, 1));
  }

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetrieverV2> result_retriever,
      ResultRetrieverV2::Create(document_store_.get(), schema_store_.get(),
                                language_segmenter_.get(), normalizer_.get()));

  // document5, document4, document1 belong to namespace2 (with max_results =
  // 1).
  // docuemnt3, document2 belong to namespace 1 (with max_results = 3).
  // Since num_per_page is 2, we expect to get document5 and document3 in the
  // first page.
  auto [page_result1, has_more_results1] =
      result_retriever->RetrieveNextPage(result_state);
  ASSERT_THAT(page_result1.results, SizeIs(2));
  ASSERT_THAT(page_result1.results.at(0).document(), EqualsProto(document5));
  ASSERT_THAT(page_result1.results.at(1).document(), EqualsProto(document3));
  ASSERT_TRUE(has_more_results1);
  {
    absl_ports::shared_lock l(&result_state.mutex);

    // Should remove document5, document4 and document3 from
    // scored_document_hits. It removes more than num_per_page documents because
    // document4 is filtered out by GroupResultLimiter and ResultRetriever has
    // to fetch the next one until returning num_per_page documents or no
    // remaining documents in scored_document_hits.
    ScoredDocumentHit scored_document_hit1(document_id1, kSectionIdMaskNone,
                                           document1.score());
    ScoredDocumentHit scored_document_hit2(document_id2, kSectionIdMaskNone,
                                           document2.score());
    EXPECT_THAT(result_state.scored_document_hits_ranker, Pointee(SizeIs(2)));

    // Even though we removed 3 document hits from scored_document_hits this
    // round, num_returned should still be 2, since document4 was "filtered out"
    // and should not be counted into num_returned.
    EXPECT_THAT(result_state.num_returned, Eq(2));
    // namespace_group_id_map should be unchanged.
    EXPECT_THAT(
        result_state.namespace_group_id_map(),
        UnorderedElementsAre(Pair(namespace_id1, 0), Pair(namespace_id2, 1)));
    // GroupResultLimiter should decrement the # in group_result_limits.
    EXPECT_THAT(result_state.group_result_limits, ElementsAre(2, 0));
  }

  // Although there are document2 and document1 left, since namespace2 has
  // reached its max results, document1 should be excluded from the second page.
  auto [page_result2, has_more_results2] =
      result_retriever->RetrieveNextPage(result_state);
  ASSERT_THAT(page_result2.results, SizeIs(1));
  ASSERT_THAT(page_result2.results.at(0).document(), EqualsProto(document2));
  ASSERT_FALSE(has_more_results2);
  {
    absl_ports::shared_lock l(&result_state.mutex);

    // Should remove document2 and document1 from scored_document_hits.
    EXPECT_THAT(result_state.scored_document_hits_ranker, Pointee(IsEmpty()));
    // Even though we removed 2 document hits from scored_document_hits this
    // round, num_returned should only be incremented by 1 (and thus become 3),
    // since document1 was "filtered out" and should not be counted into
    // num_returned.
    EXPECT_THAT(result_state.num_returned, Eq(3));
    // namespace_group_id_map should be unchanged.
    EXPECT_THAT(
        result_state.namespace_group_id_map(),
        UnorderedElementsAre(Pair(namespace_id1, 0), Pair(namespace_id2, 1)));
    // GroupResultLimiter should decrement the # in group_result_limits.
    EXPECT_THAT(result_state.group_result_limits, ElementsAre(1, 0));
  }
}

}  // namespace

}  // namespace lib
}  // namespace icing