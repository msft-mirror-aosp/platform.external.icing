// Copyright (C) 2019 Google LLC
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

#include "icing/result/result-state-manager.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/document-builder.h"
#include "icing/feature-flags.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/index/embed/embedding-query-results.h"
#include "icing/portable/equals-proto.h"
#include "icing/portable/gzip_stream.h"
#include "icing/portable/platform.h"
#include "icing/query/query-terms.h"
#include "icing/result/page-result.h"
#include "icing/result/result-adjustment-info.h"
#include "icing/result/result-retriever-v2.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/scoring/priority-queue-scored-document-hits-ranker.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/scoring/scored-document-hits-ranker.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/test-data.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/transform/normalizer-factory.h"
#include "icing/transform/normalizer-options.h"
#include "icing/transform/normalizer.h"
#include "icing/util/document-util.h"
#include "icing/util/icu-data-file-helper.h"
#include "icing/util/status-macros.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {
namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::Ne;
using ::testing::Not;
using ::testing::SizeIs;
using PageResultInfo = std::pair<uint64_t, PageResult>;

struct ScoredDocumentInfo {
  std::string name_space;
  std::string uri;
  double score;

  explicit ScoredDocumentInfo(std::string name_space_in, std::string uri_in,
                              double score_in = 1.0)
      : name_space(std::move(name_space_in)),
        uri(std::move(uri_in)),
        score(score_in) {}
};

ScoringSpecProto CreateScoringSpec() {
  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  return scoring_spec;
}

ResultSpecProto CreateResultSpec(
    int num_per_page, ResultSpecProto::ResultGroupingType result_group_type) {
  ResultSpecProto result_spec;
  result_spec.set_result_group_type(result_group_type);
  result_spec.set_num_per_page(num_per_page);
  return result_spec;
}

DocumentProto CreateDocument(int id) {
  return DocumentBuilder()
      .SetNamespace("namespace")
      .SetUri(std::to_string(id))
      .SetSchema("SchemaType")
      .SetCreationTimestampMs(1574365086666 + id)
      .SetScore(1)
      .Build();
}

class ResultStateManagerTest : public testing::Test {
 protected:
  ResultStateManagerTest()
      : test_dir_(GetTestTempDir() + "/icing"),
        schema_store_dir_(test_dir_ + "/schema_store"),
        document_store_dir_(test_dir_ + "/document_store") {}

  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    if (!IsCfStringTokenization() && !IsReverseJniTokenization()) {
      ICING_ASSERT_OK(
          // File generated via icu_data_file rule in //icing/BUILD.
          icu_data_file_helper::SetUpIcuDataFile(
              GetTestFilePath("icing/icu.dat")));
    }

    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(document_store_dir_.c_str());

    clock_ = std::make_unique<FakeClock>();

    language_segmenter_factory::SegmenterOptions options(ULOC_US);
    ICING_ASSERT_OK_AND_ASSIGN(
        language_segmenter_,
        language_segmenter_factory::Create(std::move(options)));

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, schema_store_dir_,
                                           clock_.get(), feature_flags_.get()));
    SchemaProto schema;
    schema.add_types()->set_schema_type("SchemaType");
    ICING_ASSERT_OK(schema_store_->SetSchema(
        std::move(schema), /*ignore_errors_and_delete_documents=*/false));

    NormalizerOptions normalizer_options(
        /*max_term_byte_size=*/std::numeric_limits<int32_t>::max());
    ICING_ASSERT_OK_AND_ASSIGN(normalizer_,
                               normalizer_factory::Create(normalizer_options));

    CreateDocumentStore();

    ICING_ASSERT_OK_AND_ASSIGN(
        result_retriever_,
        ResultRetrieverV2::Create(document_store_.get(), schema_store_.get(),
                                  language_segmenter_.get(), normalizer_.get(),
                                  feature_flags_.get()));
  }

  void TearDown() override {
    result_retriever_.reset();
    document_store_.reset();
    normalizer_.reset();
    schema_store_.reset();
    language_segmenter_.reset();
    clock_.reset();
    feature_flags_.reset();

    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  std::pair<ScoredDocumentHit, DocumentProto> AddScoredDocument(
      ScoredDocumentInfo info) {
    DocumentProto document;
    document.set_namespace_(std::move(info.name_space));
    document.set_uri(std::move(info.uri));
    document.set_schema("SchemaType");
    document.set_creation_timestamp_ms(1574365086666);

    DocumentWrapper document_wrapper;
    *document_wrapper.mutable_document() = std::move(document);

    DocumentId document_id =
        document_store_->Put(document_wrapper).ValueOrDie().new_document_id;
    return std::make_pair(
        ScoredDocumentHit(document_id, kSectionIdMaskNone, info.score),
        std::move(*document_wrapper.mutable_document()));
  }

  std::pair<std::vector<ScoredDocumentHit>, std::vector<DocumentProto>>
  AddScoredDocuments(std::vector<ScoredDocumentInfo>&& scored_document_infos) {
    std::vector<ScoredDocumentHit> scored_document_hits;
    std::vector<DocumentProto> document_protos;

    for (ScoredDocumentInfo& info : scored_document_infos) {
      std::pair<ScoredDocumentHit, DocumentProto> pair =
          AddScoredDocument(std::move(info));
      scored_document_hits.emplace_back(std::move(pair.first));
      document_protos.emplace_back(std::move(pair.second));
    }

    std::reverse(document_protos.begin(), document_protos.end());

    return std::make_pair(std::move(scored_document_hits),
                          std::move(document_protos));
  }

  void CreateDocumentStore() {
    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult result,
        DocumentStore::Create(
            &filesystem_, document_store_dir_, clock_.get(),
            schema_store_.get(), feature_flags_.get(),
            /*force_recovery_and_revalidate_documents=*/false,
            /*pre_mapping_fbv=*/false,
            /*use_persistent_hash_map=*/true,
            PortableFileBackedProtoLog<
                DocumentWrapper>::kDefaultCompressionLevel,
            PortableFileBackedProtoLog<
                DocumentWrapper>::kDefaultCompressionThresholdBytes,
            protobuf_ports::kDefaultMemLevel,
            /*initialize_stats=*/nullptr));
    document_store_ = std::move(result.document_store);
  }

  libtextclassifier3::StatusOr<DocumentStore::OptimizeResult>
  OptimizeDocumentStore() {
    std::string optimized_document_store_dir =
        test_dir_ + "/document_store_optimized";
    if (!filesystem_.CreateDirectoryRecursively(
            optimized_document_store_dir.c_str())) {
      return absl_ports::InternalError(
          "Failed to create optimized document store directory.");
    }
    ICING_ASSIGN_OR_RETURN(
        DocumentStore::OptimizeResult optimize_result,
        document_store_->OptimizeInto(
            optimized_document_store_dir, /*lang_segmenter=*/nullptr,
            /*potentially_optimizable_blob_handles=*/{}));

    document_store_.reset();
    if (!filesystem_.SwapFiles(document_store_dir_.c_str(),
                               optimized_document_store_dir.c_str())) {
      return absl_ports::InternalError(
          "Failed to swap files between document store and optimized document "
          "store.");
    }
    filesystem_.DeleteDirectoryRecursively(
        optimized_document_store_dir.c_str());

    CreateDocumentStore();

    return optimize_result;
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  Filesystem filesystem_;
  const std::string test_dir_;
  const std::string schema_store_dir_;
  const std::string document_store_dir_;
  std::unique_ptr<FakeClock> clock_;
  std::unique_ptr<LanguageSegmenter> language_segmenter_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<Normalizer> normalizer_;
  std::unique_ptr<DocumentStore> document_store_;
  std::unique_ptr<ResultRetrieverV2> result_retriever_;
};

TEST_F(ResultStateManagerTest, ShouldCacheAndRetrieveFirstPageOnePage) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/1))));
  DocumentId document_id1 = put_result1.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result2,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/2))));
  DocumentId document_id2 = put_result2.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result3,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/3))));
  DocumentId document_id3 = put_result3.new_document_id;
  std::vector<ScoredDocumentHit> scored_document_hits = {
      ScoredDocumentHit(document_id1, kSectionIdMaskNone, /*score=*/1),
      ScoredDocumentHit(document_id2, kSectionIdMaskNone, /*score=*/1),
      ScoredDocumentHit(document_id3, kSectionIdMaskNone, /*score=*/1)};
  std::unique_ptr<ScoredDocumentHitsRanker> ranker = std::make_unique<
      PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
      std::move(scored_document_hits), /*is_descending=*/true);

  ResultStateManager result_state_manager(
      /*max_total_hits=*/std::numeric_limits<int>::max());

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::move(ranker), /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/10, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  EXPECT_THAT(page_result_info.first, Eq(kInvalidNextPageToken));

  // Should get docs.
  ASSERT_THAT(page_result_info.second.results, SizeIs(3));
  EXPECT_THAT(page_result_info.second.results.at(0).document(),
              EqualsProto(CreateDocument(/*id=*/3)));
  EXPECT_THAT(page_result_info.second.results.at(1).document(),
              EqualsProto(CreateDocument(/*id=*/2)));
  EXPECT_THAT(page_result_info.second.results.at(2).document(),
              EqualsProto(CreateDocument(/*id=*/1)));
}

TEST_F(ResultStateManagerTest, ShouldCacheAndRetrieveFirstPageMultiplePages) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/1))));
  DocumentId document_id1 = put_result1.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result2,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/2))));
  DocumentId document_id2 = put_result2.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result3,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/3))));
  DocumentId document_id3 = put_result3.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result4,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/4))));
  DocumentId document_id4 = put_result4.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result5,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/5))));
  DocumentId document_id5 = put_result5.new_document_id;
  std::vector<ScoredDocumentHit> scored_document_hits = {
      ScoredDocumentHit(document_id1, kSectionIdMaskNone, /*score=*/1),
      ScoredDocumentHit(document_id2, kSectionIdMaskNone, /*score=*/1),
      ScoredDocumentHit(document_id3, kSectionIdMaskNone, /*score=*/1),
      ScoredDocumentHit(document_id4, kSectionIdMaskNone, /*score=*/1),
      ScoredDocumentHit(document_id5, kSectionIdMaskNone, /*score=*/1)};
  std::unique_ptr<ScoredDocumentHitsRanker> ranker = std::make_unique<
      PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
      std::move(scored_document_hits), /*is_descending=*/true);

  ResultStateManager result_state_manager(
      /*max_total_hits=*/std::numeric_limits<int>::max());

  // First page, 2 results
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::move(ranker), /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/2, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));
  EXPECT_THAT(page_result_info1.first, Not(Eq(kInvalidNextPageToken)));
  ASSERT_THAT(page_result_info1.second.results, SizeIs(2));
  EXPECT_THAT(page_result_info1.second.results.at(0).document(),
              EqualsProto(CreateDocument(/*id=*/5)));
  EXPECT_THAT(page_result_info1.second.results.at(1).document(),
              EqualsProto(CreateDocument(/*id=*/4)));

  uint64_t next_page_token = page_result_info1.first;

  // Second page, 2 results
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.GetNextPage(
          next_page_token, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  EXPECT_THAT(page_result_info2.first, Eq(next_page_token));
  ASSERT_THAT(page_result_info2.second.results, SizeIs(2));
  EXPECT_THAT(page_result_info2.second.results.at(0).document(),
              EqualsProto(CreateDocument(/*id=*/3)));
  EXPECT_THAT(page_result_info2.second.results.at(1).document(),
              EqualsProto(CreateDocument(/*id=*/2)));

  // Third page, 1 result
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info3,
      result_state_manager.GetNextPage(
          next_page_token, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  EXPECT_THAT(page_result_info3.first, Eq(kInvalidNextPageToken));
  ASSERT_THAT(page_result_info3.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info3.second.results.at(0).document(),
              EqualsProto(CreateDocument(/*id=*/1)));

  // No results
  EXPECT_THAT(
      result_state_manager.GetNextPage(
          next_page_token, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()),
      StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(ResultStateManagerTest, NullRankerShouldReturnError) {
  ResultStateManager result_state_manager(
      /*max_total_hits=*/std::numeric_limits<int>::max());

  EXPECT_THAT(
      result_state_manager.CacheAndRetrieveFirstPage(
          /*ranker=*/nullptr, /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(ResultStateManagerTest, EmptyRankerShouldReturnEmptyFirstPage) {
  ResultStateManager result_state_manager(
      /*max_total_hits=*/std::numeric_limits<int>::max());
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::vector<ScoredDocumentHit>(), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  EXPECT_THAT(page_result_info.first, Eq(kInvalidNextPageToken));
  EXPECT_THAT(page_result_info.second.results, IsEmpty());
}

TEST_F(ResultStateManagerTest, ShouldAllowEmptyFirstPage) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/1))));
  DocumentId document_id1 = put_result1.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result2,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/2))));
  DocumentId document_id2 = put_result2.new_document_id;
  std::vector<ScoredDocumentHit> scored_document_hits = {
      ScoredDocumentHit(document_id1, kSectionIdMaskNone, /*score=*/1),
      ScoredDocumentHit(document_id2, kSectionIdMaskNone, /*score=*/1)};

  ResultStateManager result_state_manager(
      /*max_total_hits=*/std::numeric_limits<int>::max());

  // Create a ResultSpec that limits "namespace" to 0 results.
  ResultSpecProto result_spec =
      CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  ResultSpecProto::ResultGrouping::Entry* entry =
      result_grouping->add_entry_groupings();
  result_grouping->set_max_results(0);
  entry->set_namespace_("namespace");

  // First page, no result.
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr, result_spec, *schema_store_,
          *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));
  // If the first page has no result, then it should be the last page.
  EXPECT_THAT(page_result_info.first, Eq(kInvalidNextPageToken));
  EXPECT_THAT(page_result_info.second.results, IsEmpty());
}

TEST_F(ResultStateManagerTest, ShouldAllowEmptyLastPage) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/1))));
  DocumentId document_id1 = put_result1.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result2,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/2))));
  DocumentId document_id2 = put_result2.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result3,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/3))));
  DocumentId document_id3 = put_result3.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result4,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/4))));
  DocumentId document_id4 = put_result4.new_document_id;
  std::vector<ScoredDocumentHit> scored_document_hits = {
      ScoredDocumentHit(document_id1, kSectionIdMaskNone, /*score=*/1),
      ScoredDocumentHit(document_id2, kSectionIdMaskNone, /*score=*/1),
      ScoredDocumentHit(document_id3, kSectionIdMaskNone, /*score=*/1),
      ScoredDocumentHit(document_id4, kSectionIdMaskNone, /*score=*/1)};

  ResultStateManager result_state_manager(
      /*max_total_hits=*/std::numeric_limits<int>::max());

  // Create a ResultSpec that limits "namespace" to 2 results.
  ResultSpecProto result_spec =
      CreateResultSpec(/*num_per_page=*/2, ResultSpecProto::NAMESPACE);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  ResultSpecProto::ResultGrouping::Entry* entry =
      result_grouping->add_entry_groupings();
  result_grouping->set_max_results(2);
  entry->set_namespace_("namespace");

  // First page, 2 results.
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr, result_spec, *schema_store_,
          *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));
  EXPECT_THAT(page_result_info1.first, Not(Eq(kInvalidNextPageToken)));
  ASSERT_THAT(page_result_info1.second.results, SizeIs(2));
  EXPECT_THAT(page_result_info1.second.results.at(0).document(),
              EqualsProto(CreateDocument(/*id=*/4)));
  EXPECT_THAT(page_result_info1.second.results.at(1).document(),
              EqualsProto(CreateDocument(/*id=*/3)));

  uint64_t next_page_token = page_result_info1.first;

  // Second page, all remaining documents will be filtered out by group result
  // limiter, so we should get an empty page.
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.GetNextPage(
          next_page_token, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  EXPECT_THAT(page_result_info2.first, Eq(kInvalidNextPageToken));
  EXPECT_THAT(page_result_info2.second.results, IsEmpty());
}

TEST_F(ResultStateManagerTest,
       ShouldRemoveExpiredTokensWhenCacheAndRetrieveFirstPage) {
  auto [scored_document_hits1, document_protos1] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1"),
                          ScoredDocumentInfo("namespace", "uri2")});
  auto [scored_document_hits2, document_protos2] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri3"),
                          ScoredDocumentInfo("namespace", "uri4"),
                          ScoredDocumentInfo("namespace", "uri5")});

  ResultStateManager result_state_manager(
      /*max_total_hits=*/std::numeric_limits<int>::max());

  SectionRestrictQueryTermsMap query_terms;
  SearchSpecProto search_spec;
  ScoringSpecProto scoring_spec = CreateScoringSpec();
  ResultSpecProto result_spec =
      CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE);

  // Set time as 1s and add state 1.
  clock_->SetSystemTimeMilliseconds(1000);
  std::unique_ptr<ScoredDocumentHitsRanker> ranker = std::make_unique<
      PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
      std::move(scored_document_hits1), /*is_descending=*/true);
  std::unordered_set<DocumentId> documents_to_snippet =
      ranker->GetTopKDocumentIds(result_spec.snippet_spec().num_to_snippet());
  std::unique_ptr<ResultAdjustmentInfo> parent_adjustment_info =
      std::make_unique<ResultAdjustmentInfo>(
          search_spec, scoring_spec, result_spec, schema_store_.get(),
          EmbeddingQueryResults(), std::move(documents_to_snippet),
          query_terms);
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::move(ranker), std::move(parent_adjustment_info),
          /*child_adjustment_info_in=*/nullptr, result_spec, *schema_store_,
          *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info1.first, Not(Eq(kInvalidNextPageToken)));

  // Set time as 1hr1s and add state 2.
  clock_->SetSystemTimeMilliseconds(kDefaultResultStateTtlInMs + 1000);
  ranker = std::make_unique<
      PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
      std::move(scored_document_hits2), /*is_descending=*/true);
  documents_to_snippet =
      ranker->GetTopKDocumentIds(result_spec.snippet_spec().num_to_snippet());
  parent_adjustment_info = std::make_unique<ResultAdjustmentInfo>(
      search_spec, scoring_spec, result_spec, schema_store_.get(),
      EmbeddingQueryResults(), std::move(documents_to_snippet), query_terms);
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::move(ranker), std::move(parent_adjustment_info),
          /*child_adjustment_info_in=*/nullptr, result_spec, *schema_store_,
          *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  // Calling CacheAndRetrieveFirstPage() on state 2 should remove the expired
  // state 1 internally.
  //
  // We test the behavior by setting time back to 1s, to make sure the
  // invalidation of state 1 was done by the previous
  // CacheAndRetrieveFirstPage() instead of the following GetNextPage().
  clock_->SetSystemTimeMilliseconds(1000);
  // page_result_info1's token (page_result_info1.first) shouldn't be found.
  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info1.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(ResultStateManagerTest,
       ShouldRemoveExpiredTokensWhenGetNextPageOnOthers) {
  auto [scored_document_hits1, document_protos1] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1"),
                          ScoredDocumentInfo("namespace", "uri2")});
  auto [scored_document_hits2, document_protos2] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri3"),
                          ScoredDocumentInfo("namespace", "uri4"),
                          ScoredDocumentInfo("namespace", "uri5")});

  ResultStateManager result_state_manager(
      /*max_total_hits=*/std::numeric_limits<int>::max());

  // Set time as 1s and add state 1.
  clock_->SetSystemTimeMilliseconds(1000);
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits1), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info1.first, Not(Eq(kInvalidNextPageToken)));

  // Set time as 2s and add state 2.
  clock_->SetSystemTimeMilliseconds(2000);
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits2), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info2.first, Not(Eq(kInvalidNextPageToken)));

  // 1. Set time as 1hr1s.
  // 2. Call GetNextPage() on state 2. It should correctly remove the expired
  //    state 1.
  // 3. Then calling GetNextPage() on state 1 shouldn't get anything.
  clock_->SetSystemTimeMilliseconds(kDefaultResultStateTtlInMs + 1000);
  // page_result_info2's token (page_result_info2.first) should be found
  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info2,
      result_state_manager.GetNextPage(
          page_result_info2.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  // We test the behavior by setting time back to 2s, to make sure the
  // invalidation of state 1 was done by the previous GetNextPage() instead of
  // the following GetNextPage().
  clock_->SetSystemTimeMilliseconds(2000);
  // page_result_info1's token (page_result_info1.first) shouldn't be found.
  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info1.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(ResultStateManagerTest,
       ShouldRemoveExpiredTokensWhenGetNextPageOnItself) {
  auto [scored_document_hits1, document_protos1] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1"),
                          ScoredDocumentInfo("namespace", "uri2")});
  auto [scored_document_hits2, document_protos2] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri3"),
                          ScoredDocumentInfo("namespace", "uri4"),
                          ScoredDocumentInfo("namespace", "uri5")});

  ResultStateManager result_state_manager(
      /*max_total_hits=*/std::numeric_limits<int>::max());

  // Set time as 1s and add state.
  clock_->SetSystemTimeMilliseconds(1000);
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits1), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info.first, Not(Eq(kInvalidNextPageToken)));

  // 1. Set time as 1hr1s.
  // 2. Then calling GetNextPage() on the state shouldn't get anything.
  clock_->SetSystemTimeMilliseconds(kDefaultResultStateTtlInMs + 1000);
  // page_result_info's token (page_result_info.first) shouldn't be found.
  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(ResultStateManagerTest, RemoveAllResultStates) {
  auto [scored_document_hits1, document_protos1] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1"),
                          ScoredDocumentInfo("namespace", "uri2")});
  auto [scored_document_hits2, document_protos2] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri3"),
                          ScoredDocumentInfo("namespace", "uri4"),
                          ScoredDocumentInfo("namespace", "uri5")});
  auto [scored_document_hits3, document_protos3] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri6"),
                          ScoredDocumentInfo("namespace", "uri7"),
                          ScoredDocumentInfo("namespace", "uri8")});

  ResultStateManager result_state_manager(
      /*max_total_hits=*/std::numeric_limits<int>::max());

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits1), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits2), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info3,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits3), /*is_descending=*/true),
          /*parent_adjustment_info=*/nullptr, /*child_adjustment_info=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ASSERT_THAT(result_state_manager.GetNumActiveResultStates(
                  clock_->GetSystemTimeMilliseconds()),
              Eq(3));
  ASSERT_THAT(result_state_manager.num_total_hits(), Eq(6));
  // Invalidate state 2.
  result_state_manager.InvalidateResultState(page_result_info2.first);
  ASSERT_THAT(result_state_manager.GetNumActiveResultStates(
                  clock_->GetSystemTimeMilliseconds()),
              Eq(2));
  ASSERT_THAT(result_state_manager.num_total_hits(), Eq(4));

  // Remove all states.
  // - State 1 and 3 are active and removed.
  // - State 2 is already invalidated and not counted as active.
  ResultStateManager::TokenRemovalStats removal_stats =
      result_state_manager.RemoveAllResultStates();
  EXPECT_THAT(removal_stats.num_active_tokens_removed, Eq(2));
  EXPECT_THAT(removal_stats.num_invalidated_tokens_removed, Eq(1));

  // page_result_info1's token (page_result_info1.first) shouldn't be found
  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info1.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  // page_result_info2's token (page_result_info2.first) shouldn't be found
  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info2.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  // page_result_info3's token (page_result_info3.first) shouldn't be found
  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info3.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(ResultStateManagerTest,
       RemoveAllResultStates_ShouldResetCurrentHitCount) {
  auto [scored_document_hits1, document_protos1] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1")});
  auto [scored_document_hits2, document_protos2] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri2"),
                          ScoredDocumentInfo("namespace", "uri3")});
  auto [scored_document_hits3, document_protos3] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri4"),
                          ScoredDocumentInfo("namespace", "uri5")});

  // Add the first three states. Remember, the first page for each result state
  // won't be cached (since it is returned immediately from
  // CacheAndRetrieveFirstPage). Each result state has a page size of 1 and a
  // result set of 2 hits. So each result will take up one hit of our three hit
  // budget.
  ResultStateManager result_state_manager(/*max_total_hits=*/3);

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits1), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits2), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info3,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits3), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  // Removes all states so that the current hit count will be 0.
  ResultStateManager::TokenRemovalStats removal_stats =
      result_state_manager.RemoveAllResultStates();
  EXPECT_THAT(removal_stats.num_active_tokens_removed, Eq(3));
  EXPECT_THAT(removal_stats.num_invalidated_tokens_removed, Eq(0));

  // If invalidating all states correctly reset the current hit count to 0,
  // then adding state 4, 5, 6 should still be within our budget and no other
  // result states should be evicted.
  auto [scored_document_hits4, document_protos4] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri6"),
                          ScoredDocumentInfo("namespace", "uri7")});
  auto [scored_document_hits5, document_protos5] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri8"),
                          ScoredDocumentInfo("namespace", "uri9")});
  auto [scored_document_hits6, document_protos6] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri10"),
                          ScoredDocumentInfo("namespace", "uri11")});

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info4,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits4), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info5,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits5), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info6,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits6), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info1.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info2.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info3.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info4,
      result_state_manager.GetNextPage(
          page_result_info4.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info4.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info4.second.results.at(0).document(),
              EqualsProto(document_protos4.at(1)));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info5,
      result_state_manager.GetNextPage(
          page_result_info5.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info5.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info5.second.results.at(0).document(),
              EqualsProto(document_protos5.at(1)));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info6,
      result_state_manager.GetNextPage(
          page_result_info6.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info6.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info6.second.results.at(0).document(),
              EqualsProto(document_protos6.at(1)));
}

TEST_F(ResultStateManagerTest, InvalidateResultState) {
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result1,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/1))));
  DocumentId document_id1 = put_result1.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result2,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/2))));
  DocumentId document_id2 = put_result2.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result3,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/3))));
  DocumentId document_id3 = put_result3.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result4,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/4))));
  DocumentId document_id4 = put_result4.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result5,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/5))));
  DocumentId document_id5 = put_result5.new_document_id;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result6,
      document_store_->Put(
          document_util::CreateDocumentWrapper(CreateDocument(/*id=*/6))));
  DocumentId document_id6 = put_result6.new_document_id;
  std::vector<ScoredDocumentHit> scored_document_hits1 = {
      ScoredDocumentHit(document_id1, kSectionIdMaskNone, /*score=*/1),
      ScoredDocumentHit(document_id2, kSectionIdMaskNone, /*score=*/1),
      ScoredDocumentHit(document_id3, kSectionIdMaskNone, /*score=*/1)};
  std::vector<ScoredDocumentHit> scored_document_hits2 = {
      ScoredDocumentHit(document_id4, kSectionIdMaskNone, /*score=*/1),
      ScoredDocumentHit(document_id5, kSectionIdMaskNone, /*score=*/1),
      ScoredDocumentHit(document_id6, kSectionIdMaskNone, /*score=*/1)};

  ResultStateManager result_state_manager(
      /*max_total_hits=*/std::numeric_limits<int>::max());

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits1), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits2), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  // Invalidate first result state by the token.
  result_state_manager.InvalidateResultState(page_result_info1.first);

  // page_result_info1's token (page_result_info1.first) shouldn't be found
  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info1.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  // page_result_info2's token (page_result_info2.first) should still exist
  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info2,
      result_state_manager.GetNextPage(
          page_result_info2.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  // Should get docs.
  ASSERT_THAT(page_result_info2.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info2.second.results.at(0).document(),
              EqualsProto(CreateDocument(/*id=*/5)));
}

TEST_F(ResultStateManagerTest,
       InvalidateResultState_ShouldDecreaseCurrentHitsCount) {
  auto [scored_document_hits1, document_protos1] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1")});
  auto [scored_document_hits2, document_protos2] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri2"),
                          ScoredDocumentInfo("namespace", "uri3")});
  auto [scored_document_hits3, document_protos3] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri4"),
                          ScoredDocumentInfo("namespace", "uri5")});

  // Add the first three states. Remember, the first page for each result state
  // won't be cached (since it is returned immediately from
  // CacheAndRetrieveFirstPage). Each result state has a page size of 1 and a
  // result set of 2 hits. So each result will take up one hit of our three hit
  // budget.
  ResultStateManager result_state_manager(/*max_total_hits=*/3);

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits1), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits2), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info3,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits3), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  // Invalidates state 2, so that the number of hits current cached should be
  // decremented to 2.
  result_state_manager.InvalidateResultState(page_result_info2.first);

  // If invalidating state 2 correctly decremented the current hit count to 2,
  // then adding state 4 should still be within our budget and no other result
  // states should be evicted.
  auto [scored_document_hits4, document_protos4] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri6"),
                          ScoredDocumentInfo("namespace", "uri7")});
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info4,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits4), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info1,
      result_state_manager.GetNextPage(
          page_result_info1.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info1.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info1.second.results.at(0).document(),
              EqualsProto(document_protos1.at(1)));

  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info2.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info3,
      result_state_manager.GetNextPage(
          page_result_info3.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info3.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info3.second.results.at(0).document(),
              EqualsProto(document_protos3.at(1)));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info4,
      result_state_manager.GetNextPage(
          page_result_info4.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info4.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info4.second.results.at(0).document(),
              EqualsProto(document_protos4.at(1)));
}

TEST_F(
    ResultStateManagerTest,
    InvalidateResultState_ShouldDecreaseCurrentHitsCountByExactStateHitCount) {
  auto [scored_document_hits1, document_protos1] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1")});
  auto [scored_document_hits2, document_protos2] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri2"),
                          ScoredDocumentInfo("namespace", "uri3")});
  auto [scored_document_hits3, document_protos3] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri4"),
                          ScoredDocumentInfo("namespace", "uri5")});

  // Add the first three states. Remember, the first page for each result state
  // won't be cached (since it is returned immediately from
  // CacheAndRetrieveFirstPage). Each result state has a page size of 1 and a
  // result set of 2 hits. So each result will take up one hit of our three hit
  // budget.
  ResultStateManager result_state_manager(/*max_total_hits=*/3);

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits1), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits2), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info3,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits3), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  // Invalidates state 2, so that the number of hits current cached should be
  // decremented to 2.
  result_state_manager.InvalidateResultState(page_result_info2.first);

  // If invalidating state 2 correctly decremented the current hit count to 2,
  // then adding state 4 should still be within our budget and no other result
  // states should be evicted.
  auto [scored_document_hits4, document_protos4] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri6"),
                          ScoredDocumentInfo("namespace", "uri7")});
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info4,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits4), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  // If invalidating result state 2 correctly decremented the current hit count
  // to 2 and adding state 4 correctly incremented it to 3, then adding this
  // result state should trigger the eviction of state 1.
  auto [scored_document_hits5, document_protos5] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri8"),
                          ScoredDocumentInfo("namespace", "uri9")});
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info5,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits5), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info1.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info2.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info3,
      result_state_manager.GetNextPage(
          page_result_info3.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info3.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info3.second.results.at(0).document(),
              EqualsProto(document_protos3.at(1)));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info4,
      result_state_manager.GetNextPage(
          page_result_info4.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info4.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info4.second.results.at(0).document(),
              EqualsProto(document_protos4.at(1)));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info5,
      result_state_manager.GetNextPage(
          page_result_info5.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info5.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info5.second.results.at(0).document(),
              EqualsProto(document_protos5.at(1)));
}

TEST_F(ResultStateManagerTest, GetNextPage_ShouldDecreaseCurrentHitsCount) {
  auto [scored_document_hits1, document_protos1] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1")});
  auto [scored_document_hits2, document_protos2] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri2"),
                          ScoredDocumentInfo("namespace", "uri3")});
  auto [scored_document_hits3, document_protos3] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri4"),
                          ScoredDocumentInfo("namespace", "uri5")});

  // Add the first three states. Remember, the first page for each result state
  // won't be cached (since it is returned immediately from
  // CacheAndRetrieveFirstPage). Each result state has a page size of 1 and a
  // result set of 2 hits. So each result will take up one hit of our three hit
  // budget.
  ResultStateManager result_state_manager(/*max_total_hits=*/3);

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits1), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits2), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info3,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits3), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  // GetNextPage for result state 1 should return its result and decrement the
  // number of cached hits to 2.
  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info1,
      result_state_manager.GetNextPage(
          page_result_info1.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info1.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info1.second.results.at(0).document(),
              EqualsProto(document_protos1.at(1)));

  // If retrieving the next page for result state 1 correctly decremented the
  // current hit count to 2, then adding state 4 should still be within our
  // budget and no other result states should be evicted.
  auto [scored_document_hits4, document_protos4] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri6"),
                          ScoredDocumentInfo("namespace", "uri7")});
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info4,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits4), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info1.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info2,
      result_state_manager.GetNextPage(
          page_result_info2.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info2.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info2.second.results.at(0).document(),
              EqualsProto(document_protos2.at(1)));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info3,
      result_state_manager.GetNextPage(
          page_result_info3.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info3.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info3.second.results.at(0).document(),
              EqualsProto(document_protos3.at(1)));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info4,
      result_state_manager.GetNextPage(
          page_result_info4.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info4.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info4.second.results.at(0).document(),
              EqualsProto(document_protos4.at(1)));
}

TEST_F(ResultStateManagerTest,
       GetNextPage_ShouldDecreaseCurrentHitsCountByExactlyOnePage) {
  auto [scored_document_hits1, document_protos1] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1")});
  auto [scored_document_hits2, document_protos2] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri2"),
                          ScoredDocumentInfo("namespace", "uri3")});
  auto [scored_document_hits3, document_protos3] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri4"),
                          ScoredDocumentInfo("namespace", "uri5")});

  // Add the first three states. Remember, the first page for each result state
  // won't be cached (since it is returned immediately from
  // CacheAndRetrieveFirstPage). Each result state has a page size of 1 and a
  // result set of 2 hits. So each result will take up one hit of our three hit
  // budget.
  ResultStateManager result_state_manager(/*max_total_hits=*/3);

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits1), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits2), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info3,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits3), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  // GetNextPage for result state 1 should return its result and decrement the
  // number of cached hits to 2.
  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info1,
      result_state_manager.GetNextPage(
          page_result_info1.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info1.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info1.second.results.at(0).document(),
              EqualsProto(document_protos1.at(1)));

  // If retrieving the next page for result state 1 correctly decremented the
  // current hit count to 2, then adding state 4 should still be within our
  // budget and no other result states should be evicted.
  auto [scored_document_hits4, document_protos4] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri6"),
                          ScoredDocumentInfo("namespace", "uri7")});
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info4,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits4), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  // If retrieving the next page for result state 1 correctly decremented the
  // current hit count to 2 and adding state 4 correctly incremented it to 3,
  // then adding this result state should trigger the eviction of state 2.
  auto [scored_document_hits5, document_protos5] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri8"),
                          ScoredDocumentInfo("namespace", "uri9")});
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info5,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits5), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info1.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info2.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info3,
      result_state_manager.GetNextPage(
          page_result_info3.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info3.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info3.second.results.at(0).document(),
              EqualsProto(document_protos3.at(1)));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info4,
      result_state_manager.GetNextPage(
          page_result_info4.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info4.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info4.second.results.at(0).document(),
              EqualsProto(document_protos4.at(1)));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info5,
      result_state_manager.GetNextPage(
          page_result_info5.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info5.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info5.second.results.at(0).document(),
              EqualsProto(document_protos5.at(1)));
}

TEST_F(ResultStateManagerTest, CacheEviction_ShouldRemoveOldestResultState) {
  auto [scored_document_hits1, document_protos1] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1")});
  auto [scored_document_hits2, document_protos2] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri2"),
                          ScoredDocumentInfo("namespace", "uri3")});
  auto [scored_document_hits3, document_protos3] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri4"),
                          ScoredDocumentInfo("namespace", "uri5")});

  ResultStateManager result_state_manager(/*max_total_hits=*/2);

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits1), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits2), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  // Adding state 3 should cause state 1 to be removed due to budget limit.
  QueryStatsProto query_stats;
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info3,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits3), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds(), &query_stats));
  EXPECT_THAT(query_stats.num_result_states_evicted(), Eq(1));

  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info1.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info2,
      result_state_manager.GetNextPage(
          page_result_info2.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info2.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info2.second.results.at(0).document(),
              EqualsProto(document_protos2.at(1)));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info3,
      result_state_manager.GetNextPage(
          page_result_info3.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info3.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info3.second.results.at(0).document(),
              EqualsProto(document_protos3.at(1)));
}

TEST_F(ResultStateManagerTest,
       CacheEviction_SingleOverBudgetStateShouldEvictAllStatesAndTruncateHits) {
  auto [scored_document_hits1, document_protos1] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1"),
                          ScoredDocumentInfo("namespace", "uri2")});
  auto [scored_document_hits2, document_protos2] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri3"),
                          ScoredDocumentInfo("namespace", "uri4")});

  // Add the first two states. Remember, the first page for each result state
  // won't be cached (since it is returned immediately from
  // CacheAndRetrieveFirstPage). Each result state has a page size of 1. So 3
  // hits will remain cached.
  ResultStateManager result_state_manager(/*max_total_hits=*/4);

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits1), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits2), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  // Add a result state that is larger than the entire budget. This should
  // result in all previous result states being evicted, the first hit from
  // result state 3 being returned and the next four hits being cached (the last
  // hit should be dropped because it exceeds the max).
  auto [scored_document_hits3, document_protos3] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri5"),
                          ScoredDocumentInfo("namespace", "uri6"),
                          ScoredDocumentInfo("namespace", "uri7"),
                          ScoredDocumentInfo("namespace", "uri8"),
                          ScoredDocumentInfo("namespace", "uri9"),
                          ScoredDocumentInfo("namespace", "uri10")});
  QueryStatsProto query_stats;
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info3,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits3), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds(), &query_stats));
  EXPECT_THAT(page_result_info3.first, Not(Eq(kInvalidNextPageToken)));
  // Should set num_result_states_evicted since result state 1 and 2 were
  // evicted.
  EXPECT_THAT(query_stats.num_result_states_evicted(), Eq(2));

  // GetNextPage for result state 1 and 2 should return NOT_FOUND.
  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info1.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info2.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  // Only the next four results in state 3 should be retrievable.
  uint64_t next_page_token3 = page_result_info3.first;
  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info3,
      result_state_manager.GetNextPage(
          next_page_token3, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  EXPECT_THAT(page_result_info3.first, Eq(next_page_token3));
  ASSERT_THAT(page_result_info3.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info3.second.results.at(0).document(),
              EqualsProto(document_protos3.at(1)));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info3,
      result_state_manager.GetNextPage(
          next_page_token3, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  EXPECT_THAT(page_result_info3.first, Eq(next_page_token3));
  ASSERT_THAT(page_result_info3.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info3.second.results.at(0).document(),
              EqualsProto(document_protos3.at(2)));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info3,
      result_state_manager.GetNextPage(
          next_page_token3, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  EXPECT_THAT(page_result_info3.first, Eq(next_page_token3));
  ASSERT_THAT(page_result_info3.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info3.second.results.at(0).document(),
              EqualsProto(document_protos3.at(3)));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info3,
      result_state_manager.GetNextPage(
          next_page_token3, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  // The final document should have been dropped because it exceeded the budget,
  // so the next page token of the second last round should be
  // kInvalidNextPageToken.
  EXPECT_THAT(page_result_info3.first, Eq(kInvalidNextPageToken));
  ASSERT_THAT(page_result_info3.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info3.second.results.at(0).document(),
              EqualsProto(document_protos3.at(4)));

  // Double check that next_page_token3 is not retrievable anymore.
  EXPECT_THAT(
      result_state_manager.GetNextPage(
          next_page_token3, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()),
      StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(ResultStateManagerTest,
       CacheEviction_ShouldEvictStatesUntilBudgetIsReached) {
  // Add a result state that is larger than the entire budget. The entire result
  // state will still be cached
  auto [scored_document_hits1, document_protos1] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1"),
                          ScoredDocumentInfo("namespace", "uri2"),
                          ScoredDocumentInfo("namespace", "uri3"),
                          ScoredDocumentInfo("namespace", "uri4"),
                          ScoredDocumentInfo("namespace", "uri5")});

  ResultStateManager result_state_manager(/*max_total_hits=*/4);

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits1), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  // Add a result state. Because state2 + state1 is larger than the budget,
  // state1 should be evicted.
  auto [scored_document_hits2, document_protos2] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri6"),
                          ScoredDocumentInfo("namespace", "uri7")});
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits2), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  // state1 should have been evicted and state2 should still be retrievable.
  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info1.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info2,
      result_state_manager.GetNextPage(
          page_result_info2.first,
          /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info2.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info2.second.results.at(0).document(),
              EqualsProto(document_protos2.at(1)));
}

TEST_F(ResultStateManagerTest, CacheEviction_ShouldNotTruncatedAfterFirstPage) {
  // Add a result state that is larger than the entire budget, but within the
  // entire budget after the first page. The entire result state will still be
  // cached and not truncated.
  auto [scored_document_hits, document_protos] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1"),
                          ScoredDocumentInfo("namespace", "uri2"),
                          ScoredDocumentInfo("namespace", "uri3"),
                          ScoredDocumentInfo("namespace", "uri4")});

  ResultStateManager result_state_manager(/*max_total_hits=*/4);

  // The 5 input scored document hits will not be truncated. The first page of
  // two hits will be returned immediately and the other three hits will fit
  // within our caching budget.
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/2, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  // First page, 2 results
  ASSERT_THAT(page_result_info1.second.results, SizeIs(2));
  EXPECT_THAT(page_result_info1.second.results.at(0).document(),
              EqualsProto(document_protos.at(0)));
  EXPECT_THAT(page_result_info1.second.results.at(1).document(),
              EqualsProto(document_protos.at(1)));

  uint64_t next_page_token = page_result_info1.first;

  // Second page, 2 results.
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.GetNextPage(
          next_page_token, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info2.second.results, SizeIs(2));
  EXPECT_THAT(page_result_info2.second.results.at(0).document(),
              EqualsProto(document_protos.at(2)));
  EXPECT_THAT(page_result_info2.second.results.at(1).document(),
              EqualsProto(document_protos.at(3)));

  // Third page, 1 result.
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info3,
      result_state_manager.GetNextPage(
          next_page_token, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info3.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info3.second.results.at(0).document(),
              EqualsProto(document_protos.at(4)));

  // Fourth page, 0 results.
  EXPECT_THAT(
      result_state_manager.GetNextPage(
          next_page_token, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()),
      StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(ResultStateManagerTest,
       CacheEviction_EvictionStatsShouldNotCountInvalidatedTokens) {
  auto [scored_document_hits1, document_protos1] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1"),
                          ScoredDocumentInfo("namespace", "uri2")});
  auto [scored_document_hits2, document_protos2] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri3"),
                          ScoredDocumentInfo("namespace", "uri4")});
  auto [scored_document_hits3, document_protos3] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri5"),
                          ScoredDocumentInfo("namespace", "uri6")});

  // Add the first three states. Remember, the first page for each result state
  // won't be cached (since it is returned immediately from
  // CacheAndRetrieveFirstPage). Each result state has a page size of 1. So 2 +
  // 1 + 1 = 4 hits will remain cached.
  ResultStateManager result_state_manager(/*max_total_hits=*/4);

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits1), /*is_descending=*/true),
          /*parent_adjustment_info=*/nullptr, /*child_adjustment_info=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits2), /*is_descending=*/true),
          /*parent_adjustment_info=*/nullptr, /*child_adjustment_info=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info3,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits3), /*is_descending=*/true),
          /*parent_adjustment_info=*/nullptr, /*child_adjustment_info=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  // Invalidate the second result state, which is not the front token in the
  // token queue.
  // - The corresponding ResultState is removed from result_state_map_ and
  //   destroyed.
  // - The token will remain in the queue, and be added into
  //   invalidated_token_set_.
  // - The num_total_hits_ will be decremented.
  ASSERT_THAT(result_state_manager.num_total_hits(), Eq(4));
  result_state_manager.InvalidateResultState(page_result_info2.first);
  ASSERT_THAT(result_state_manager.num_total_hits(), Eq(3));

  // Add a result state that is larger than the entire budget. This should
  // result in all previous result states being evicted.
  auto [scored_document_hits4, document_protos4] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri7"),
                          ScoredDocumentInfo("namespace", "uri8"),
                          ScoredDocumentInfo("namespace", "uri9"),
                          ScoredDocumentInfo("namespace", "uri10"),
                          ScoredDocumentInfo("namespace", "uri11"),
                          ScoredDocumentInfo("namespace", "uri12")});
  QueryStatsProto query_stats;
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info4,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits4), /*is_descending=*/true),
          /*parent_adjustment_info=*/nullptr, /*child_adjustment_info=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds(), &query_stats));
  EXPECT_THAT(page_result_info4.first, Not(Eq(kInvalidNextPageToken)));
  // num_result_states_evicted should be 2.
  // - result state 1 was evicted and destroyed, so it should be counted as
  //   cache eviction.
  // - result state 2 was already invalidated and destroyed, so it should not be
  //   counted as cache eviction.
  // - result state 3 was evicted and destroyed, so it should be counted as
  //   cache eviction.
  EXPECT_THAT(query_stats.num_result_states_evicted(), Eq(2));

  // GetNextPage for result state 1, 2 and 3 should return NOT_FOUND.
  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info1.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info2.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  EXPECT_THAT(result_state_manager.GetNextPage(
                  page_result_info3.first,
                  /*max_results=*/std::numeric_limits<int32_t>::max(),
                  *result_retriever_, clock_->GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  // Only the next four results in state 4 should be retrievable.
  uint64_t next_page_token4 = page_result_info4.first;
  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info4,
      result_state_manager.GetNextPage(
          next_page_token4, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  EXPECT_THAT(page_result_info4.first, Eq(next_page_token4));
  ASSERT_THAT(page_result_info4.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info4.second.results.at(0).document(),
              EqualsProto(document_protos4.at(1)));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info4,
      result_state_manager.GetNextPage(
          next_page_token4, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  EXPECT_THAT(page_result_info4.first, Eq(next_page_token4));
  ASSERT_THAT(page_result_info4.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info4.second.results.at(0).document(),
              EqualsProto(document_protos4.at(2)));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info4,
      result_state_manager.GetNextPage(
          next_page_token4, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  EXPECT_THAT(page_result_info4.first, Eq(next_page_token4));
  ASSERT_THAT(page_result_info4.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info4.second.results.at(0).document(),
              EqualsProto(document_protos4.at(3)));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_info4,
      result_state_manager.GetNextPage(
          next_page_token4, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  // The final document should have been dropped because it exceeded the budget,
  // so the next page token of the second last round should be
  // kInvalidNextPageToken.
  EXPECT_THAT(page_result_info4.first, Eq(kInvalidNextPageToken));
  ASSERT_THAT(page_result_info4.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info4.second.results.at(0).document(),
              EqualsProto(document_protos4.at(4)));

  // Double check that next_page_token3 is not retrievable anymore.
  EXPECT_THAT(
      result_state_manager.GetNextPage(
          next_page_token4, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()),
      StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(ResultStateManagerTest, GetNumActiveResultStates) {
  auto [scored_document_hits1, document_protos1] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1"),
                          ScoredDocumentInfo("namespace", "uri2")});
  auto [scored_document_hits2, document_protos2] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri3"),
                          ScoredDocumentInfo("namespace", "uri4"),
                          ScoredDocumentInfo("namespace", "uri5")});
  auto [scored_document_hits3, document_protos3] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri6"),
                          ScoredDocumentInfo("namespace", "uri7"),
                          ScoredDocumentInfo("namespace", "uri8")});

  ResultStateManager result_state_manager(
      /*max_total_hits=*/std::numeric_limits<int>::max());

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits1), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits2), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info3,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits3), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  EXPECT_THAT(result_state_manager.GetNumActiveResultStates(
                  /*current_time_ms=*/clock_->GetSystemTimeMilliseconds()),
              Eq(3));
}

TEST_F(ResultStateManagerTest,
       GetNumActiveResultStatesShouldRemoveExpiredResultStates) {
  auto [scored_document_hits1, document_protos1] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1"),
                          ScoredDocumentInfo("namespace", "uri2")});
  auto [scored_document_hits2, document_protos2] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri3"),
                          ScoredDocumentInfo("namespace", "uri4"),
                          ScoredDocumentInfo("namespace", "uri5")});
  auto [scored_document_hits3, document_protos3] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri6"),
                          ScoredDocumentInfo("namespace", "uri7"),
                          ScoredDocumentInfo("namespace", "uri8")});

  ResultStateManager result_state_manager(
      /*max_total_hits=*/std::numeric_limits<int>::max());

  // Set time as 1s and add state.
  clock_->SetSystemTimeMilliseconds(1000);
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits1), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  // Set time as 10s and add state 2, state 3.
  clock_->SetSystemTimeMilliseconds(10000);
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits2), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info3,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits3), /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));

  // 1. Set time as 1hr1s.
  // 2. Then calling GetNumActiveResultStates() should remove the expired state
  //    1, and return count == 2 (for state 2 and state 3).
  clock_->SetSystemTimeMilliseconds(kDefaultResultStateTtlInMs + 1000);
  EXPECT_THAT(result_state_manager.GetNumActiveResultStates(
                  /*current_time_ms=*/clock_->GetSystemTimeMilliseconds()),
              Eq(2));
}

TEST_F(ResultStateManagerTest, Optimize) {
  // Add 5 documents (doc id 0 to 4).
  auto [scored_document_hits, document_protos] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1"),
                          ScoredDocumentInfo("namespace", "uri2"),
                          ScoredDocumentInfo("namespace", "uri3"),
                          ScoredDocumentInfo("namespace", "uri4")});

  // Only include  "uri2", "uri3", and "uri4" in the search results.
  std::vector<ScoredDocumentHit> desired_scored_document_hits = {
      scored_document_hits[2], scored_document_hits[3],
      scored_document_hits[4]};

  std::unique_ptr<ScoredDocumentHitsRanker> ranker = std::make_unique<
      PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
      std::move(desired_scored_document_hits), /*is_descending=*/true);

  ResultStateManager result_state_manager(
      /*max_total_hits=*/std::numeric_limits<int>::max());

  // First page, 2 results ("uri4", "uri3").
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::move(ranker), /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/2, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info1.first, Ne(kInvalidNextPageToken));
  ASSERT_THAT(page_result_info1.second.results, SizeIs(2));
  ASSERT_THAT(page_result_info1.second.results.at(0).document().uri(),
              Eq("uri4"));
  ASSERT_THAT(page_result_info1.second.results.at(1).document().uri(),
              Eq("uri3"));

  uint64_t next_page_token = page_result_info1.first;

  // Delete doc ("namespace", "uri0") and ("namespace", "uri3").
  int64_t current_time_ms = clock_->GetSystemTimeMilliseconds();
  ICING_ASSERT_OK(
      document_store_->Delete("namespace", "uri0", current_time_ms));
  ICING_ASSERT_OK(
      document_store_->Delete("namespace", "uri3", current_time_ms));

  // Optimize the document store.
  // Remapping:
  // - 1 -> 0
  // - 2 -> 1
  // - 4 -> 2
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::OptimizeResult doc_store_optimize_result,
      OptimizeDocumentStore());
  ASSERT_THAT(doc_store_optimize_result.document_id_old_to_new,
              ElementsAre(kInvalidDocumentId, 0, 1, kInvalidDocumentId, 2));

  // Optimize ResultStateManager.
  ResultStateManager::OptimizeResult optimize_result =
      result_state_manager.Optimize(doc_store_optimize_result);
  EXPECT_THAT(optimize_result.num_result_states_optimized, Eq(1));
  EXPECT_THAT(optimize_result.num_result_states_invalidated, Eq(0));

  // Fetch the second page after optimization.
  // - Expect to get "uri2" in the second page.
  // - "uri2" has old doc id 2 and new doc id 1.
  // - This test verifies that ResultStateManager can correctly remap the
  //   document ids and get the correct document from the second page.
  //   - If the remap had not been done correctly and the ResultState was still
  //     using the old doc id 2, then it would've fetched "uri4" from the
  //     optimized doc store.
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.GetNextPage(
          next_page_token, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  EXPECT_THAT(page_result_info2.first, Eq(kInvalidNextPageToken));
  ASSERT_THAT(page_result_info2.second.results, SizeIs(1));
  EXPECT_THAT(page_result_info2.second.results.at(0).document().uri(),
              Eq("uri2"));
}

TEST_F(ResultStateManagerTest, Optimize_documentDeleted) {
  // Add 5 documents (doc id 0 to 4).
  auto [scored_document_hits, document_protos] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1"),
                          ScoredDocumentInfo("namespace", "uri2"),
                          ScoredDocumentInfo("namespace", "uri3"),
                          ScoredDocumentInfo("namespace", "uri4")});

  std::unique_ptr<ScoredDocumentHitsRanker> ranker = std::make_unique<
      PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
      std::move(scored_document_hits), /*is_descending=*/true);

  ResultStateManager result_state_manager(
      /*max_total_hits=*/std::numeric_limits<int>::max());

  // First page, 2 results ("uri4", "uri3").
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::move(ranker), /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/2, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info1.first, Ne(kInvalidNextPageToken));
  ASSERT_THAT(page_result_info1.second.results, SizeIs(2));
  ASSERT_THAT(page_result_info1.second.results.at(0).document().uri(),
              Eq("uri4"));
  ASSERT_THAT(page_result_info1.second.results.at(1).document().uri(),
              Eq("uri3"));

  uint64_t next_page_token = page_result_info1.first;

  // Delete doc ("namespace", "uri1").
  int64_t current_time_ms = clock_->GetSystemTimeMilliseconds();
  ICING_ASSERT_OK(
      document_store_->Delete("namespace", "uri1", current_time_ms));

  // Optimize the document store.
  // Remapping:
  // - 0 -> 0
  // - 1 -> kInvalidDocumentId
  // - 2 -> 1
  // - 3 -> 2
  // - 4 -> 3
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::OptimizeResult doc_store_optimize_result,
      OptimizeDocumentStore());
  ASSERT_THAT(doc_store_optimize_result.document_id_old_to_new,
              ElementsAre(0, kInvalidDocumentId, 1, 2, 3));

  // Optimize ResultStateManager.
  ResultStateManager::OptimizeResult optimize_result =
      result_state_manager.Optimize(doc_store_optimize_result);
  EXPECT_THAT(optimize_result.num_result_states_optimized, Eq(1));
  EXPECT_THAT(optimize_result.num_result_states_invalidated, Eq(0));

  // Fetch the second page after optimization. Should get "uri2" and "uri0".
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.GetNextPage(
          next_page_token, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  EXPECT_THAT(page_result_info2.first, Eq(kInvalidNextPageToken));
  ASSERT_THAT(page_result_info2.second.results, SizeIs(2));
  EXPECT_THAT(page_result_info2.second.results.at(0).document().uri(),
              Eq("uri2"));
  EXPECT_THAT(page_result_info2.second.results.at(1).document().uri(),
              Eq("uri0"));
}

TEST_F(ResultStateManagerTest, Optimize_allRemainingDocumentsAreDeleted) {
  // Add 5 documents (doc id 0 to 4).
  auto [scored_document_hits, document_protos] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1"),
                          ScoredDocumentInfo("namespace", "uri2"),
                          ScoredDocumentInfo("namespace", "uri3"),
                          ScoredDocumentInfo("namespace", "uri4")});

  std::unique_ptr<ScoredDocumentHitsRanker> ranker = std::make_unique<
      PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
      std::move(scored_document_hits), /*is_descending=*/true);

  ResultStateManager result_state_manager(
      /*max_total_hits=*/std::numeric_limits<int>::max());

  // First page, 2 results ("uri4", "uri3").
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::move(ranker), /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/2, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info1.first, Ne(kInvalidNextPageToken));
  ASSERT_THAT(page_result_info1.second.results, SizeIs(2));
  ASSERT_THAT(page_result_info1.second.results.at(0).document().uri(),
              Eq("uri4"));
  ASSERT_THAT(page_result_info1.second.results.at(1).document().uri(),
              Eq("uri3"));

  uint64_t next_page_token = page_result_info1.first;

  // Delete doc ("namespace", "uri0"), ("namespace", "uri1") and ("namespace",
  // "uri2").
  int64_t current_time_ms = clock_->GetSystemTimeMilliseconds();
  ICING_ASSERT_OK(
      document_store_->Delete("namespace", "uri0", current_time_ms));
  ICING_ASSERT_OK(
      document_store_->Delete("namespace", "uri1", current_time_ms));
  ICING_ASSERT_OK(
      document_store_->Delete("namespace", "uri2", current_time_ms));

  // Optimize the document store.
  // Remapping:
  // - 0 -> kInvalidDocumentId
  // - 1 -> kInvalidDocumentId
  // - 2 -> kInvalidDocumentId
  // - 3 -> 0
  // - 4 -> 1
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::OptimizeResult doc_store_optimize_result,
      OptimizeDocumentStore());
  ASSERT_THAT(doc_store_optimize_result.document_id_old_to_new,
              ElementsAre(kInvalidDocumentId, kInvalidDocumentId,
                          kInvalidDocumentId, 0, 1));

  // Optimize ResultStateManager.
  ResultStateManager::OptimizeResult optimize_result =
      result_state_manager.Optimize(doc_store_optimize_result);
  EXPECT_THAT(optimize_result.num_result_states_optimized, Eq(1));
  EXPECT_THAT(optimize_result.num_result_states_invalidated, Eq(0));

  // Fetch the second page after optimization.
  // - next_page_token is still valid, so we should get the second page instead
  //   of NOT_FOUND error.
  // - But all the remaining documents have been deleted, so we should get an
  //   empty page.
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.GetNextPage(
          next_page_token, /*max_results=*/std::numeric_limits<int32_t>::max(),
          *result_retriever_, clock_->GetSystemTimeMilliseconds()));
  EXPECT_THAT(page_result_info2.first, Eq(kInvalidNextPageToken));
  EXPECT_THAT(page_result_info2.second.results, IsEmpty());
}

TEST_F(ResultStateManagerTest, Optimize_numTotalHits) {
  // Add 5 documents (doc id 0 to 4).
  auto [scored_document_hits, document_protos] =
      AddScoredDocuments({ScoredDocumentInfo("namespace", "uri0"),
                          ScoredDocumentInfo("namespace", "uri1"),
                          ScoredDocumentInfo("namespace", "uri2"),
                          ScoredDocumentInfo("namespace", "uri3"),
                          ScoredDocumentInfo("namespace", "uri4")});

  // Create state1 with 5 hits ("uri0", "uri1", "uri2", "uri3", "uri4").
  std::vector<ScoredDocumentHit> scored_doc_hits_vec1 = {
      scored_document_hits[0], scored_document_hits[1], scored_document_hits[2],
      scored_document_hits[3], scored_document_hits[4]};
  std::unique_ptr<ScoredDocumentHitsRanker> ranker1 = std::make_unique<
      PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
      std::move(scored_doc_hits_vec1), /*is_descending=*/true);

  // Create state2 with 2 hits ("uri0", "uri4").
  std::vector<ScoredDocumentHit> scored_doc_hits_vec2 = {
      scored_document_hits[0], scored_document_hits[4]};
  std::unique_ptr<ScoredDocumentHitsRanker> ranker2 = std::make_unique<
      PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
      std::move(scored_doc_hits_vec2), /*is_descending=*/true);

  // Create state3 with 4 hits ("uri0", "uri1", "uri2", "uri3").
  std::vector<ScoredDocumentHit> scored_doc_hits_vec3 = {
      scored_document_hits[0], scored_document_hits[1], scored_document_hits[2],
      scored_document_hits[3]};
  std::unique_ptr<ScoredDocumentHitsRanker> ranker3 = std::make_unique<
      PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
      std::move(scored_doc_hits_vec3), /*is_descending=*/true);

  ResultStateManager result_state_manager(
      /*max_total_hits=*/std::numeric_limits<int>::max());

  // State1, first page, 1 result ("uri4").
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info1,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::move(ranker1), /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info1.first, Ne(kInvalidNextPageToken));
  ASSERT_THAT(page_result_info1.second.results, SizeIs(1));
  ASSERT_THAT(page_result_info1.second.results.at(0).document().uri(),
              Eq("uri4"));
  // num_total_hits_ should be 4, since state1 was added into the cache with 4
  // remaining hits.
  ASSERT_THAT(result_state_manager.num_total_hits(), Eq(4));

  // State2, first page, 1 result ("uri4").
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info2,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::move(ranker2), /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info2.first, Ne(kInvalidNextPageToken));
  ASSERT_THAT(page_result_info2.second.results, SizeIs(1));
  ASSERT_THAT(page_result_info2.second.results.at(0).document().uri(),
              Eq("uri4"));
  // num_total_hits_ should be 5, since state2 was added into the cache with 1
  // remaining hits.
  ASSERT_THAT(result_state_manager.num_total_hits(), Eq(5));

  // State3, first page, 1 result ("uri3").
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultInfo page_result_info3,
      result_state_manager.CacheAndRetrieveFirstPage(
          std::move(ranker3), /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/1, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_, *result_retriever_,
          clock_->GetSystemTimeMilliseconds()));
  ASSERT_THAT(page_result_info3.first, Ne(kInvalidNextPageToken));
  ASSERT_THAT(page_result_info3.second.results, SizeIs(1));
  ASSERT_THAT(page_result_info3.second.results.at(0).document().uri(),
              Eq("uri3"));
  // num_total_hits_ should be 8, since state3 was added into the cache with 3
  // remaining hits.
  ASSERT_THAT(result_state_manager.num_total_hits(), Eq(8));

  // There are 3 active result states.
  int64_t current_time_ms = clock_->GetSystemTimeMilliseconds();
  ASSERT_THAT(result_state_manager.GetNumActiveResultStates(current_time_ms),
              Eq(3));

  // Delete doc ("namespace", "uri0"), ("namespace", "uri4").
  ICING_ASSERT_OK(
      document_store_->Delete("namespace", "uri0", current_time_ms));
  ICING_ASSERT_OK(
      document_store_->Delete("namespace", "uri4", current_time_ms));

  // Optimize the document store.
  // Remapping:
  // - 0 -> kInvalidDocumentId
  // - 1 -> 0
  // - 2 -> 1
  // - 3 -> 2
  // - 4 -> kInvalidDocumentId
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::OptimizeResult doc_store_optimize_result,
      OptimizeDocumentStore());
  ASSERT_THAT(doc_store_optimize_result.document_id_old_to_new,
              ElementsAre(kInvalidDocumentId, 0, 1, 2, kInvalidDocumentId));

  // Optimize ResultStateManager.
  ResultStateManager::OptimizeResult optimize_result =
      result_state_manager.Optimize(doc_store_optimize_result);
  EXPECT_THAT(optimize_result.num_result_states_optimized, Eq(3));
  EXPECT_THAT(optimize_result.num_result_states_invalidated, Eq(0));

  // - State1: "uri1", "uri2", "uri3" (3 hits)
  // - State2: X (0 hits)
  // - State3: "uri1", "uri2" (2 hits)
  //
  // So num_total_hits_ should be 5. Still, there should be 3 active result
  // states even though state2 is empty.
  EXPECT_THAT(result_state_manager.num_total_hits(), Eq(5));
  EXPECT_THAT(result_state_manager.GetNumActiveResultStates(current_time_ms),
              Eq(3));
}

}  // namespace
}  // namespace lib
}  // namespace icing
