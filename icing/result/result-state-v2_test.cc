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

#include "icing/result/result-state-v2.h"

#include <atomic>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/mutex.h"
#include "icing/absl_ports/thread_annotations.h"
#include "icing/document-builder.h"
#include "icing/feature-flags.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/index/embed/embedding-query-results.h"
#include "icing/portable/gzip_stream.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/document_wrapper.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/query/query-terms.h"
#include "icing/result/projection-tree.h"
#include "icing/result/result-adjustment-info.h"
#include "icing/result/result-utils.h"
#include "icing/result/snippet-context.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/scoring/priority-queue-scored-document-hits-ranker.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/scoring/scored-document-hits-ranker.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/store/namespace-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/embedding-test-utils.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/clock.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {
namespace {

using ::testing::AnyOf;
using ::testing::Contains;
using ::testing::DoubleEq;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::IsNull;
using ::testing::Key;
using ::testing::Ne;
using ::testing::NotNull;
using ::testing::Pair;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;

constexpr SearchSpecProto::EmbeddingQueryMetricType::Code
    EMBEDDING_METRIC_DOT_PRODUCT =
        SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT;
constexpr SearchSpecProto::EmbeddingQueryMetricType::Code
    EMBEDDING_METRIC_COSINE = SearchSpecProto::EmbeddingQueryMetricType::COSINE;

SearchSpecProto CreateSearchSpec(
    TermMatchType::Code match_type,
    const std::vector<PropertyProto::VectorProto>& embedding_query_vectors,
    SearchSpecProto::EmbeddingQueryMetricType::Code metric_type) {
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(match_type);
  search_spec.mutable_embedding_query_vectors()->Add(
      embedding_query_vectors.begin(), embedding_query_vectors.end());
  search_spec.set_embedding_query_metric_type(metric_type);
  return search_spec;
}

ScoringSpecProto CreateScoringSpec(bool is_descending_order) {
  ScoringSpecProto scoring_spec;
  scoring_spec.set_order_by(is_descending_order ? ScoringSpecProto::Order::DESC
                                                : ScoringSpecProto::Order::ASC);
  return scoring_spec;
}

ResultSpecProto CreateResultSpec(
    int num_per_page, ResultSpecProto::ResultGroupingType result_group_type) {
  ResultSpecProto result_spec;
  result_spec.set_result_group_type(result_group_type);
  result_spec.set_num_per_page(num_per_page);
  return result_spec;
}

ResultSpecProto CreateResultSpec(int num_per_page) {
  ResultSpecProto result_spec;
  result_spec.set_num_per_page(num_per_page);
  return result_spec;
}

std::vector<JoinedScoredDocumentHit> RetrieveNextK(ResultStateV2& result_state,
                                                   int k)
    ICING_EXCLUSIVE_LOCKS_REQUIRED(result_state.mutex) {
  std::vector<JoinedScoredDocumentHit> hits;
  while (!result_state.scored_document_hits_ranker->empty() &&
         hits.size() < k) {
    hits.push_back(result_state.scored_document_hits_ranker->Top());
    result_state.scored_document_hits_ranker->Pop();
  }

  result_state.IncrementNumTotalHits(-1 * static_cast<int>(hits.size()));
  return hits;
}

std::vector<JoinedScoredDocumentHit> RetrieveAll(ResultStateV2& result_state)
    ICING_EXCLUSIVE_LOCKS_REQUIRED(result_state.mutex) {
  return RetrieveNextK(result_state, std::numeric_limits<int32_t>::max());
}

class ResultStateV2Test : public ::testing::Test {
 protected:
  ResultStateV2Test()
      : test_dir_(GetTestTempDir() + "/icing"),
        schema_store_dir_(test_dir_ + "/schema_store"),
        document_store_dir_(test_dir_ + "/document_store") {}

  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());

    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(document_store_dir_.c_str());

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, schema_store_dir_,
                                           &clock_, feature_flags_.get()));
    SchemaProto schema =
        SchemaBuilder()
            .AddType(SchemaTypeConfigBuilder().SetType("SchemaType"))
            .AddType(SchemaTypeConfigBuilder().SetType("Email"))
            .AddType(SchemaTypeConfigBuilder().SetType("Phone"))
            .Build();
    ICING_ASSERT_OK(schema_store_->SetSchema(
        std::move(schema), /*ignore_errors_and_delete_documents=*/false));

    CreateDocumentStore();

    num_total_hits_ = 0;
  }

  void TearDown() override {
    num_total_hits_ = 0;

    document_store_.reset();
    schema_store_.reset();

    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  // Helper function to add a new document into the document store and return
  // the ScoredDocumentHit for that document according to the given score.
  // Note: the section id mask is not important in this test.
  ScoredDocumentHit AddScoredDocument(std::string name_space, std::string uri,
                                      double score = 1.0) {
    DocumentProto document;
    document.set_namespace_(std::move(name_space));
    document.set_uri(std::move(uri));
    document.set_schema("SchemaType");

    DocumentWrapper document_wrapper;
    *document_wrapper.mutable_document() = std::move(document);

    DocumentId document_id =
        document_store_->Put(document_wrapper).ValueOrDie().new_document_id;
    return ScoredDocumentHit(document_id, kSectionIdMaskNone, score);
  }

  void CreateDocumentStore() {
    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult result,
        DocumentStore::Create(
            &filesystem_, document_store_dir_, &clock_, schema_store_.get(),
            feature_flags_.get(),
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
  Clock clock_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
  std::atomic<int> num_total_hits_;
};

TEST_F(ResultStateV2Test, ShouldInitializeValuesAccordingToSpecs) {
  ResultSpecProto result_spec =
      CreateResultSpec(/*num_per_page=*/2, ResultSpecProto::NAMESPACE);
  result_spec.set_num_total_bytes_per_page_threshold(4096);
  result_spec.set_max_joined_children_per_parent_to_return(2048);

  // Adjustment info is not important in this test.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::vector<ScoredDocumentHit>(), /*is_descending=*/true),
      /*parent_adjustment_info_in=*/nullptr,
      /*child_adjustment_info_in=*/nullptr, result_spec, *schema_store_,
      *document_store_);

  absl_ports::shared_lock l(&result_state.mutex);

  EXPECT_THAT(result_state.num_returned, Eq(0));
  EXPECT_THAT(result_state.num_per_page(), Eq(result_spec.num_per_page()));
  EXPECT_THAT(result_state.num_total_bytes_per_page_threshold(),
              Eq(result_spec.num_total_bytes_per_page_threshold()));
  EXPECT_THAT(result_state.max_joined_children_per_parent_to_return(),
              Eq(result_spec.max_joined_children_per_parent_to_return()));
}

TEST_F(ResultStateV2Test, ShouldInitializeValuesAccordingToDefaultSpecs) {
  ResultSpecProto default_result_spec = ResultSpecProto::default_instance();
  ASSERT_THAT(default_result_spec.num_per_page(), Eq(10));
  ASSERT_THAT(default_result_spec.num_total_bytes_per_page_threshold(),
              Eq(std::numeric_limits<int32_t>::max()));

  // Adjustment info is not important in this test.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::vector<ScoredDocumentHit>(),
          /*is_descending=*/true),
      /*parent_adjustment_info_in=*/nullptr,
      /*child_adjustment_info_in=*/nullptr, default_result_spec, *schema_store_,
      *document_store_);

  absl_ports::shared_lock l(&result_state.mutex);

  EXPECT_THAT(result_state.num_returned, Eq(0));
  EXPECT_THAT(result_state.num_per_page(),
              Eq(default_result_spec.num_per_page()));
  EXPECT_THAT(result_state.num_total_bytes_per_page_threshold(),
              Eq(default_result_spec.num_total_bytes_per_page_threshold()));
  EXPECT_THAT(
      result_state.max_joined_children_per_parent_to_return(),
      Eq(default_result_spec.max_joined_children_per_parent_to_return()));
}

TEST_F(ResultStateV2Test,
       ShouldConstructNamespaceGroupIdMapAndGroupResultLimitsAccordingToSpecs) {
  // Create 3 docs under namespace1, namespace2, namespace3.
  DocumentWrapper document_wrapper1;
  *document_wrapper1.mutable_document() = DocumentBuilder()
                                              .SetNamespace("namespace1")
                                              .SetUri("uri/1")
                                              .SetSchema("SchemaType")
                                              .Build();
  ICING_ASSERT_OK(document_store_->Put(document_wrapper1));

  DocumentWrapper document_wrapper2;
  *document_wrapper2.mutable_document() = DocumentBuilder()
                                              .SetNamespace("namespace2")
                                              .SetUri("uri/2")
                                              .SetSchema("SchemaType")
                                              .Build();
  ICING_ASSERT_OK(document_store_->Put(document_wrapper2));

  DocumentWrapper document_wrapper3;
  *document_wrapper3.mutable_document() = DocumentBuilder()
                                              .SetNamespace("namespace3")
                                              .SetUri("uri/3")
                                              .SetSchema("SchemaType")
                                              .Build();
  ICING_ASSERT_OK(document_store_->Put(document_wrapper3));

  // Create a ResultSpec that limits "namespace1" to 3 results and limits
  // "namespace2"+"namespace3" to a total of 2 results. Also add
  // "nonexistentNamespace1" and "nonexistentNamespace2" to test the behavior.
  ResultSpecProto::ResultGroupingType result_grouping_type =
      ResultSpecProto::NAMESPACE;
  ResultSpecProto result_spec =
      CreateResultSpec(/*num_per_page=*/5, result_grouping_type);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  ResultSpecProto::ResultGrouping::Entry* entry =
      result_grouping->add_entry_groupings();
  result_grouping->set_max_results(3);
  entry->set_namespace_("namespace1");
  result_grouping = result_spec.add_result_groupings();
  result_grouping->set_max_results(5);
  entry = result_grouping->add_entry_groupings();
  entry->set_namespace_("nonexistentNamespace2");
  result_grouping = result_spec.add_result_groupings();
  result_grouping->set_max_results(2);
  entry = result_grouping->add_entry_groupings();
  entry->set_namespace_("namespace2");
  entry = result_grouping->add_entry_groupings();
  entry->set_namespace_("namespace3");
  entry = result_grouping->add_entry_groupings();
  entry->set_namespace_("nonexistentNamespace1");

  // Get entry ids.
  ICING_ASSERT_HAS_VALUE_AND_ASSIGN(
      result_utils::ResultGroupingEntryId entry_id1,
      result_utils::EncodeResultGroupingEntryId(
          *schema_store_, *document_store_, result_grouping_type, "namespace1",
          "SchemaType"));
  ICING_ASSERT_HAS_VALUE_AND_ASSIGN(
      result_utils::ResultGroupingEntryId entry_id2,
      result_utils::EncodeResultGroupingEntryId(
          *schema_store_, *document_store_, result_grouping_type, "namespace2",
          "SchemaType"));
  ICING_ASSERT_HAS_VALUE_AND_ASSIGN(
      result_utils::ResultGroupingEntryId entry_id3,
      result_utils::EncodeResultGroupingEntryId(
          *schema_store_, *document_store_, result_grouping_type, "namespace3",
          "SchemaType"));

  // Adjustment info is not important in this test.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::vector<ScoredDocumentHit>(),
          /*is_descending=*/true),
      /*parent_adjustment_info_in=*/nullptr,
      /*child_adjustment_info_in=*/nullptr, result_spec, *schema_store_,
      *document_store_);

  absl_ports::shared_lock l(&result_state.mutex);

  // "namespace1" should be in group index 0, and "namespace2" + "namespace3"
  // should be in group index 2. "nonexistentNamespace1" and
  // "nonexistentNamespace2" shouldn't exist.
  EXPECT_THAT(result_state.entry_id_group_index_map,
              UnorderedElementsAre(Pair(entry_id1, 0), Pair(entry_id2, 2),
                                   Pair(entry_id3, 2)));

  // group_result_limits should contain 3 (at index 0 for group 0), 5 (at index
  // 1 for group 1), 2 (at index 2 for group 2), even though there is no valid
  // namespace in group 1.
  EXPECT_THAT(result_state.group_result_limits, ElementsAre(3, 5, 2));
}

TEST_F(ResultStateV2Test, ShouldUpdateNumTotalHits) {
  // Create 5 ScoredDocumentHits.
  ScoredDocumentHit scored_doc_hit0 = AddScoredDocument("namespace", "uri0");
  ScoredDocumentHit scored_doc_hit1 = AddScoredDocument("namespace", "uri1");
  ScoredDocumentHit scored_doc_hit2 = AddScoredDocument("namespace", "uri2");
  ScoredDocumentHit scored_doc_hit3 = AddScoredDocument("namespace", "uri3");
  ScoredDocumentHit scored_doc_hit4 = AddScoredDocument("namespace", "uri4");

  // Shuffle the order of the ScoredDocumentHits.
  std::vector<ScoredDocumentHit> scored_document_hits = {
      std::move(scored_doc_hit1), std::move(scored_doc_hit0),
      std::move(scored_doc_hit2), std::move(scored_doc_hit4),
      std::move(scored_doc_hit3)};

  // Adjustment info is not important in this test.
  // Creates a ResultState with 5 ScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits),
          /*is_descending=*/true),
      /*parent_adjustment_info_in=*/nullptr,
      /*child_adjustment_info_in=*/nullptr,
      CreateResultSpec(/*num_per_page=*/5, ResultSpecProto::NAMESPACE),
      *schema_store_, *document_store_);

  absl_ports::unique_lock l(&result_state.mutex);

  EXPECT_THAT(num_total_hits_, Eq(0));
  result_state.RegisterNumTotalHits(&num_total_hits_);
  EXPECT_THAT(num_total_hits_, Eq(5));
  result_state.IncrementNumTotalHits(500);
  EXPECT_THAT(num_total_hits_, Eq(505));
}

TEST_F(ResultStateV2Test, ShouldUpdateNumTotalHitsWhenDestructed) {
  // Create 7 ScoredDocumentHits.
  ScoredDocumentHit scored_doc_hit0 = AddScoredDocument("namespace", "uri0");
  ScoredDocumentHit scored_doc_hit1 = AddScoredDocument("namespace", "uri1");
  ScoredDocumentHit scored_doc_hit2 = AddScoredDocument("namespace", "uri2");
  ScoredDocumentHit scored_doc_hit3 = AddScoredDocument("namespace", "uri3");
  ScoredDocumentHit scored_doc_hit4 = AddScoredDocument("namespace", "uri4");
  ScoredDocumentHit scored_doc_hit5 = AddScoredDocument("namespace", "uri5");
  ScoredDocumentHit scored_doc_hit6 = AddScoredDocument("namespace", "uri6");

  // Create 2 vectors of ScoredDocumentHits and shuffle the order of the
  // ScoredDocumentHits.
  std::vector<ScoredDocumentHit> scored_document_hits1 = {
      std::move(scored_doc_hit1), std::move(scored_doc_hit0),
      std::move(scored_doc_hit2), std::move(scored_doc_hit4),
      std::move(scored_doc_hit3)};
  std::vector<ScoredDocumentHit> scored_document_hits2 = {
      std::move(scored_doc_hit6), std::move(scored_doc_hit5)};

  num_total_hits_ = 2;
  {
    // Adjustment info is not important in this test.
    // Creates a ResultState with 5 ScoredDocumentHits.
    ResultStateV2 result_state1(
        std::make_unique<
            PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
            std::move(scored_document_hits1),
            /*is_descending=*/true),
        /*parent_adjustment_info_in=*/nullptr,
        /*child_adjustment_info_in=*/nullptr,
        CreateResultSpec(/*num_per_page=*/5, ResultSpecProto::NAMESPACE),
        *schema_store_, *document_store_);

    absl_ports::unique_lock l(&result_state1.mutex);

    result_state1.RegisterNumTotalHits(&num_total_hits_);
    ASSERT_THAT(num_total_hits_, Eq(7));

    {
      // Adjustment info is not important in this test.
      // Creates another ResultState with 2 ScoredDocumentHits.
      ResultStateV2 result_state2(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits2),
              /*is_descending=*/true),
          /*parent_adjustment_info_in=*/nullptr,
          /*child_adjustment_info_in=*/nullptr,
          CreateResultSpec(/*num_per_page=*/5, ResultSpecProto::NAMESPACE),
          *schema_store_, *document_store_);

      absl_ports::unique_lock l(&result_state2.mutex);

      result_state2.RegisterNumTotalHits(&num_total_hits_);
      ASSERT_THAT(num_total_hits_, Eq(9));
    }

    EXPECT_THAT(num_total_hits_, Eq(7));
  }
  EXPECT_THAT(num_total_hits_, Eq(2));
}

TEST_F(ResultStateV2Test, ShouldNotUpdateNumTotalHitsWhenNotRegistered) {
  // Create 5 ScoredDocumentHits.
  ScoredDocumentHit scored_doc_hit0 = AddScoredDocument("namespace", "uri0");
  ScoredDocumentHit scored_doc_hit1 = AddScoredDocument("namespace", "uri1");
  ScoredDocumentHit scored_doc_hit2 = AddScoredDocument("namespace", "uri2");
  ScoredDocumentHit scored_doc_hit3 = AddScoredDocument("namespace", "uri3");
  ScoredDocumentHit scored_doc_hit4 = AddScoredDocument("namespace", "uri4");

  // Shuffle the order of the ScoredDocumentHits.
  std::vector<ScoredDocumentHit> scored_document_hits = {
      std::move(scored_doc_hit1), std::move(scored_doc_hit0),
      std::move(scored_doc_hit2), std::move(scored_doc_hit4),
      std::move(scored_doc_hit3)};

  // Creates a ResultState with 5 ScoredDocumentHits.
  {
    // Adjustment info is not important in this test.
    ResultStateV2 result_state(
        std::make_unique<
            PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
            std::move(scored_document_hits),
            /*is_descending=*/true),
        /*parent_adjustment_info_in=*/nullptr,
        /*child_adjustment_info_in=*/nullptr,
        CreateResultSpec(/*num_per_page=*/5, ResultSpecProto::NAMESPACE),
        *schema_store_, *document_store_);

    {
      absl_ports::unique_lock l(&result_state.mutex);

      EXPECT_THAT(num_total_hits_, Eq(0));
      result_state.IncrementNumTotalHits(500);
      EXPECT_THAT(num_total_hits_, Eq(0));
    }
  }
  EXPECT_THAT(num_total_hits_, Eq(0));
}

TEST_F(ResultStateV2Test, ShouldDecrementOriginalNumTotalHitsWhenReregister) {
  std::atomic<int> another_num_total_hits = 11;

  // Create 5 ScoredDocumentHits.
  ScoredDocumentHit scored_doc_hit0 = AddScoredDocument("namespace", "uri0");
  ScoredDocumentHit scored_doc_hit1 = AddScoredDocument("namespace", "uri1");
  ScoredDocumentHit scored_doc_hit2 = AddScoredDocument("namespace", "uri2");
  ScoredDocumentHit scored_doc_hit3 = AddScoredDocument("namespace", "uri3");
  ScoredDocumentHit scored_doc_hit4 = AddScoredDocument("namespace", "uri4");

  // Shuffle the order of the ScoredDocumentHits.
  std::vector<ScoredDocumentHit> scored_document_hits = {
      std::move(scored_doc_hit1), std::move(scored_doc_hit0),
      std::move(scored_doc_hit2), std::move(scored_doc_hit4),
      std::move(scored_doc_hit3)};

  // Adjustment info is not important in this test.
  // Creates a ResultState with 5 ScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits),
          /*is_descending=*/true),
      /*parent_adjustment_info_in=*/nullptr,
      /*child_adjustment_info_in=*/nullptr,
      CreateResultSpec(/*num_per_page=*/5, ResultSpecProto::NAMESPACE),
      *schema_store_, *document_store_);

  absl_ports::unique_lock l(&result_state.mutex);

  num_total_hits_ = 7;
  result_state.RegisterNumTotalHits(&num_total_hits_);
  EXPECT_THAT(num_total_hits_, Eq(12));

  result_state.RegisterNumTotalHits(&another_num_total_hits);
  // The original num_total_hits should be decremented after re-registration.
  EXPECT_THAT(num_total_hits_, Eq(7));
  // another_num_total_hits should be incremented after re-registration.
  EXPECT_THAT(another_num_total_hits, Eq(16));

  result_state.IncrementNumTotalHits(500);
  // The original num_total_hits should be unchanged.
  EXPECT_THAT(num_total_hits_, Eq(7));
  // Increment should be done on another_num_total_hits.
  EXPECT_THAT(another_num_total_hits, Eq(516));
}

TEST_F(ResultStateV2Test, Optimize_scoredDocumentHitsRankerAndNumTotalHits) {
  // Add 5 ScoredDocumentHits.
  // Intentionally set the score same as the original document id, so we can
  // easily verify the remapping is correct after optimization.
  ScoredDocumentHit scored_doc_hit0 =
      AddScoredDocument("namespace", "uri0", /*score=*/0);
  ScoredDocumentHit scored_doc_hit1 =
      AddScoredDocument("namespace", "uri1", /*score=*/1);
  ScoredDocumentHit scored_doc_hit2 =
      AddScoredDocument("namespace", "uri2", /*score=*/2);
  ScoredDocumentHit scored_doc_hit3 =
      AddScoredDocument("namespace", "uri3", /*score=*/3);
  ScoredDocumentHit scored_doc_hit4 =
      AddScoredDocument("namespace", "uri4", /*score=*/4);
  std::vector<ScoredDocumentHit> scored_document_hits = {
      std::move(scored_doc_hit1), std::move(scored_doc_hit0),
      std::move(scored_doc_hit2), std::move(scored_doc_hit4),
      std::move(scored_doc_hit3)};

  // Adjustment info optimization will be tested separately in another test, so
  // we set them to nullptr here.
  // Creates a ResultState with 5 ScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits),
          /*is_descending=*/true),
      /*parent_adjustment_info_in=*/nullptr,
      /*child_adjustment_info_in=*/nullptr,
      CreateResultSpec(/*num_per_page=*/5, ResultSpecProto::NAMESPACE),
      *schema_store_, *document_store_);

  absl_ports::unique_lock l(&result_state.mutex);

  // Set the original num_total_hits_ to 100.
  num_total_hits_ = 100;
  result_state.RegisterNumTotalHits(&num_total_hits_);
  // num_total_hits_ should be 100 + 5 after registration.
  EXPECT_THAT(num_total_hits_, Eq(105));

  // Delete document 1, 2, and optimize the document store.
  int64_t current_time_ms = clock_.GetSystemTimeMilliseconds();
  ICING_ASSERT_OK(document_store_->Delete(/*document_id=*/1, current_time_ms));
  ICING_ASSERT_OK(document_store_->Delete(/*document_id=*/2, current_time_ms));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::OptimizeResult optimize_result,
                             OptimizeDocumentStore());
  // Remapping:
  // - 0 -> 0
  // - 3 -> 1
  // - 4 -> 2
  ASSERT_THAT(optimize_result.document_id_old_to_new,
              ElementsAre(0, kInvalidDocumentId, kInvalidDocumentId, 1, 2));

  // Optimize the ResultState.
  EXPECT_THAT(result_state.Optimize(optimize_result), IsOk());

  // Verify that num_total_hits_ was updated correctly: the original 100 is
  // unchanged, the 5 hits in the original ranker are subtracted, and the 3 hits
  // in the new ranker are added back.
  EXPECT_THAT(num_total_hits_, Eq(103));

  // Pop all hits from the ranker. They should be only 3 hits and containing new
  // doc ids.
  std::vector<JoinedScoredDocumentHit> hits_after_optimization =
      RetrieveAll(result_state);
  ASSERT_THAT(hits_after_optimization, SizeIs(3));
  EXPECT_THAT(
      hits_after_optimization[0].parent_scored_document_hit(),
      EqualsScoredDocumentHit(ScoredDocumentHit(
          /*document_id=*/2, kSectionIdMaskNone, /*score=*/4)));  // 4 -> 2
  EXPECT_THAT(
      hits_after_optimization[1].parent_scored_document_hit(),
      EqualsScoredDocumentHit(ScoredDocumentHit(
          /*document_id=*/1, kSectionIdMaskNone, /*score=*/3)));  // 3 -> 1
  EXPECT_THAT(
      hits_after_optimization[2].parent_scored_document_hit(),
      EqualsScoredDocumentHit(ScoredDocumentHit(
          /*document_id=*/0, kSectionIdMaskNone, /*score=*/0)));  // 0 -> 0
}

TEST_F(ResultStateV2Test, Optimize_retrievePartiallyAndOptimize) {
  // Add 5 ScoredDocumentHits.
  // Intentionally set the score same as the original document id, so we can
  // easily verify the remapping is correct after optimization.
  ScoredDocumentHit scored_doc_hit0 =
      AddScoredDocument("namespace", "uri0", /*score=*/0);
  ScoredDocumentHit scored_doc_hit1 =
      AddScoredDocument("namespace", "uri1", /*score=*/1);
  ScoredDocumentHit scored_doc_hit2 =
      AddScoredDocument("namespace", "uri2", /*score=*/2);
  ScoredDocumentHit scored_doc_hit3 =
      AddScoredDocument("namespace", "uri3", /*score=*/3);
  ScoredDocumentHit scored_doc_hit4 =
      AddScoredDocument("namespace", "uri4", /*score=*/4);
  std::vector<ScoredDocumentHit> scored_document_hits = {
      std::move(scored_doc_hit1), std::move(scored_doc_hit0),
      std::move(scored_doc_hit2), std::move(scored_doc_hit4),
      std::move(scored_doc_hit3)};

  // Adjustment info optimization will be tested separately in another test, so
  // we set them to nullptr here.
  // Creates a ResultState with 5 ScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits),
          /*is_descending=*/true),
      /*parent_adjustment_info_in=*/nullptr,
      /*child_adjustment_info_in=*/nullptr,
      CreateResultSpec(/*num_per_page=*/5, ResultSpecProto::NAMESPACE),
      *schema_store_, *document_store_);

  absl_ports::unique_lock l(&result_state.mutex);

  // Set the original num_total_hits_ to 100.
  num_total_hits_ = 100;
  result_state.RegisterNumTotalHits(&num_total_hits_);
  // num_total_hits_ should be 100 + 5 after registration.
  EXPECT_THAT(num_total_hits_, Eq(105));

  // Retrieve 2 docs from the ResultState. Hits for document 4 and 3 are
  // returned and popped from the ranker.
  std::vector<JoinedScoredDocumentHit> retrieved_hits_before_optimization =
      RetrieveNextK(result_state, /*k=*/2);
  ASSERT_THAT(retrieved_hits_before_optimization, SizeIs(2));
  ASSERT_THAT(
      retrieved_hits_before_optimization[0].parent_scored_document_hit(),
      EqualsScoredDocumentHit(ScoredDocumentHit(
          /*document_id=*/4, kSectionIdMaskNone, /*score=*/4)));
  ASSERT_THAT(
      retrieved_hits_before_optimization[1].parent_scored_document_hit(),
      EqualsScoredDocumentHit(ScoredDocumentHit(
          /*document_id=*/3, kSectionIdMaskNone, /*score=*/3)));
  ASSERT_THAT(num_total_hits_, Eq(103));

  // Delete document 1, 4, and optimize the document store.
  int64_t current_time_ms = clock_.GetSystemTimeMilliseconds();
  ICING_ASSERT_OK(document_store_->Delete(/*document_id=*/1, current_time_ms));
  ICING_ASSERT_OK(document_store_->Delete(/*document_id=*/4, current_time_ms));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::OptimizeResult optimize_result,
                             OptimizeDocumentStore());
  // Remapping:
  // - 0 -> 0
  // - 2 -> 1
  // - 3 -> 2
  ASSERT_THAT(optimize_result.document_id_old_to_new,
              ElementsAre(0, kInvalidDocumentId, 1, 2, kInvalidDocumentId));

  // Optimize the ResultState.
  EXPECT_THAT(result_state.Optimize(optimize_result), IsOk());

  // Verify that num_total_hits_ was updated correctly:
  // - The original 100 is unchanged.
  // - Before optimization, there are only hits for doc 0, 1, 2 left in the
  //   ranker.
  // - Document 1 and 4 are deleted, but hit for doc 4 was already retrieved.
  // - So after optimization, only hits for doc 0 and 2 are kept (and remapped
  //   to 0, 1 respectively) in the ranker.
  // - Therefore, num_total_hits should become 102.
  EXPECT_THAT(num_total_hits_, Eq(102));

  // Pop all hits from the ranker. They should be only 2 hits and containing new
  // doc ids.
  std::vector<JoinedScoredDocumentHit> hits_after_optimization =
      RetrieveAll(result_state);
  ASSERT_THAT(hits_after_optimization, SizeIs(2));
  EXPECT_THAT(
      hits_after_optimization[0].parent_scored_document_hit(),
      EqualsScoredDocumentHit(ScoredDocumentHit(
          /*document_id=*/1, kSectionIdMaskNone, /*score=*/2)));  // 2 -> 1
  EXPECT_THAT(
      hits_after_optimization[1].parent_scored_document_hit(),
      EqualsScoredDocumentHit(ScoredDocumentHit(
          /*document_id=*/0, kSectionIdMaskNone, /*score=*/0)));  // 0 -> 0
}

TEST_F(ResultStateV2Test, Optimize_joinedScoredDocumentHitsRanker) {
  // Add 11 ScoredDocumentHits.
  // Intentionally set the score same as the original document id, so we can
  // easily verify the remapping is correct after optimization.
  ScoredDocumentHit scored_doc_hit0 =
      AddScoredDocument("namespace", "uri0", /*score=*/0);
  ScoredDocumentHit scored_doc_hit1 =
      AddScoredDocument("namespace", "uri1", /*score=*/1);
  ScoredDocumentHit scored_doc_hit2 =
      AddScoredDocument("namespace", "uri2", /*score=*/2);
  ScoredDocumentHit scored_doc_hit3 =
      AddScoredDocument("namespace", "uri3", /*score=*/3);
  ScoredDocumentHit scored_doc_hit4 =
      AddScoredDocument("namespace", "uri4", /*score=*/4);
  ScoredDocumentHit scored_doc_hit5 =
      AddScoredDocument("namespace", "uri5", /*score=*/5);
  ScoredDocumentHit scored_doc_hit6 =
      AddScoredDocument("namespace", "uri6", /*score=*/6);
  ScoredDocumentHit scored_doc_hit7 =
      AddScoredDocument("namespace", "uri7", /*score=*/7);
  ScoredDocumentHit scored_doc_hit8 =
      AddScoredDocument("namespace", "uri8", /*score=*/8);
  ScoredDocumentHit scored_doc_hit9 =
      AddScoredDocument("namespace", "uri9", /*score=*/9);
  ScoredDocumentHit scored_doc_hit10 =
      AddScoredDocument("namespace", "uri10", /*score=*/10);

  // Parent doc 4 -> child docs [5, 2]
  JoinedScoredDocumentHit joined_scored_document_hit1(
      /*final_score=*/123.45,
      /*parent_scored_document_hit=*/std::move(scored_doc_hit4),
      /*child_scored_document_hits=*/
      {std::move(scored_doc_hit5), std::move(scored_doc_hit2)});

  // Parent doc 3 -> child docs [6, 0, 7, 8]
  JoinedScoredDocumentHit joined_scored_document_hit2(
      /*final_score=*/67.89,
      /*parent_scored_document_hit=*/std::move(scored_doc_hit3),
      /*child_scored_document_hits=*/
      {std::move(scored_doc_hit6), std::move(scored_doc_hit0),
       std::move(scored_doc_hit7), std::move(scored_doc_hit8)});

  // Parent doc 1 -> child docs [9, 10]
  JoinedScoredDocumentHit joined_scored_document_hit3(
      /*final_score=*/39.21,
      /*parent_scored_document_hit=*/std::move(scored_doc_hit1),
      /*child_scored_document_hits=*/
      {std::move(scored_doc_hit9), std::move(scored_doc_hit10)});

  std::vector<JoinedScoredDocumentHit> joined_scored_document_hits = {
      std::move(joined_scored_document_hit1),
      std::move(joined_scored_document_hit2),
      std::move(joined_scored_document_hit3)};

  // Adjustment info optimization will be tested separately in another test, so
  // we set them to nullptr here.
  // Creates a ResultState with 3 JoinedScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<JoinedScoredDocumentHit>>(
          std::move(joined_scored_document_hits),
          /*is_descending=*/true),
      /*parent_adjustment_info_in=*/nullptr,
      /*child_adjustment_info_in=*/nullptr,
      CreateResultSpec(/*num_per_page=*/5, ResultSpecProto::NAMESPACE),
      *schema_store_, *document_store_);

  absl_ports::unique_lock l(&result_state.mutex);

  // Set the original num_total_hits_ to 100.
  num_total_hits_ = 100;
  result_state.RegisterNumTotalHits(&num_total_hits_);
  // num_total_hits_ should be 100 + 3 after registration.
  EXPECT_THAT(num_total_hits_, Eq(103));

  // Delete document 0, 2, 3, 7, 9, 10 and optimize the document store.
  int64_t current_time_ms = clock_.GetSystemTimeMilliseconds();
  ICING_ASSERT_OK(document_store_->Delete(/*document_id=*/0, current_time_ms));
  ICING_ASSERT_OK(document_store_->Delete(/*document_id=*/2, current_time_ms));
  ICING_ASSERT_OK(document_store_->Delete(/*document_id=*/3, current_time_ms));
  ICING_ASSERT_OK(document_store_->Delete(/*document_id=*/7, current_time_ms));
  ICING_ASSERT_OK(document_store_->Delete(/*document_id=*/9, current_time_ms));
  ICING_ASSERT_OK(document_store_->Delete(/*document_id=*/10, current_time_ms));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::OptimizeResult optimize_result,
                             OptimizeDocumentStore());
  // Remapping:
  // 0 -> kInvalidDocumentId
  // 1 -> 0
  // 2 -> kInvalidDocumentId
  // 3 -> kInvalidDocumentId
  // 4 -> 1
  // 5 -> 2
  // 6 -> 3
  // 7 -> kInvalidDocumentId
  // 8 -> 4
  // 9 -> kInvalidDocumentId
  // 10 -> kInvalidDocumentId
  ASSERT_THAT(optimize_result.document_id_old_to_new,
              ElementsAre(kInvalidDocumentId, 0, kInvalidDocumentId,
                          kInvalidDocumentId, 1, 2, 3, kInvalidDocumentId, 4,
                          kInvalidDocumentId, kInvalidDocumentId));

  // Optimize the ResultState.
  // - JoinedScoredDocumentHit1:
  //   - parent doc 4 is kept and remapped to 1.
  //   - child doc 5 is kept and remapped to 2.
  //   - child doc 2 is deleted.
  // - JoinedScoredDocumentHit2:
  //   - parent doc 3 is deleted, so the whole entry is deleted even though some
  //     of the child docs are kept.
  // - JoinedScoredDocumentHit3:
  //   - parent doc 1 is kept and remapped to 0.
  //   - child docs 9 and 10 are deleted.
  //
  // So there will be only 2 JoinedScoredDocumentHits left.
  EXPECT_THAT(result_state.Optimize(optimize_result), IsOk());

  // Verify that num_total_hits_ was updated correctly: the original 100 is
  // unchanged, the 3 hits in the original ranker are subtracted, and the 2 hits
  // in the new ranker are added back.
  EXPECT_THAT(num_total_hits_, Eq(102));

  // Pop all hits from the ranker. They should be only 2 hits and containing new
  // doc ids.
  std::vector<JoinedScoredDocumentHit> hits_after_optimization =
      RetrieveAll(result_state);
  ASSERT_THAT(hits_after_optimization, SizeIs(2));

  EXPECT_THAT(hits_after_optimization[0].final_score(), DoubleEq(123.45));
  EXPECT_THAT(
      hits_after_optimization[0].parent_scored_document_hit(),
      EqualsScoredDocumentHit(ScoredDocumentHit(
          /*document_id=*/1, kSectionIdMaskNone, /*score=*/4)));  // 4 -> 1
  EXPECT_THAT(hits_after_optimization[0].child_scored_document_hits(),
              ElementsAre(EqualsScoredDocumentHit(ScoredDocumentHit(
                  /*document_id=*/2, kSectionIdMaskNone, /*score=*/5)  // 5 -> 2
                                                  )));

  EXPECT_THAT(hits_after_optimization[1].final_score(), DoubleEq(39.21));
  EXPECT_THAT(
      hits_after_optimization[1].parent_scored_document_hit(),
      EqualsScoredDocumentHit(ScoredDocumentHit(
          /*document_id=*/0, kSectionIdMaskNone, /*score=*/1)));  // 1 -> 0
  EXPECT_THAT(hits_after_optimization[1].child_scored_document_hits(),
              IsEmpty());  // All child docs are deleted.
}

TEST_F(ResultStateV2Test, Optimize_resultAdjustmentInfo_parent) {
  // Add 4 ScoredDocumentHits.
  // Intentionally set the score same as the original document id, so we can
  // easily verify the remapping is correct after optimization.
  ScoredDocumentHit scored_doc_hit0 =
      AddScoredDocument("namespace", "uri0", /*score=*/0);
  ScoredDocumentHit scored_doc_hit1 =
      AddScoredDocument("namespace", "uri1", /*score=*/1);
  ScoredDocumentHit scored_doc_hit2 =
      AddScoredDocument("namespace", "uri2", /*score=*/2);
  ScoredDocumentHit scored_doc_hit3 =
      AddScoredDocument("namespace", "uri3", /*score=*/3);
  std::vector<ScoredDocumentHit> scored_document_hits = {
      std::move(scored_doc_hit1), std::move(scored_doc_hit0),
      std::move(scored_doc_hit2), std::move(scored_doc_hit3)};

  SectionRestrictQueryTermsMap query_terms_map;
  query_terms_map.emplace("term1", std::unordered_set<std::string>());

  std::vector<PropertyProto::VectorProto> embedding_query_vectors = {
      CreateVector("my_model1", {1, -2, -4}),
      CreateVector("my_model2", {1, -2, 3, -4}),
      CreateVector("my_model3", {0.1, -0.2, 0.3}),
      CreateVector("my_model1", {1, -2, -5})};

  // Search spec.
  SearchSpecProto search_spec =
      CreateSearchSpec(TermMatchType::EXACT_ONLY, embedding_query_vectors,
                       EMBEDDING_METRIC_DOT_PRODUCT);

  // Create ResultSpec with custom snippet spec and projection tree.
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/3);
  result_spec.set_max_joined_children_per_parent_to_return(
      std::numeric_limits<int32_t>::max());
  result_spec.mutable_snippet_spec()->set_num_to_snippet(5);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(5);
  result_spec.mutable_snippet_spec()->set_max_window_utf32_length(5);
  result_spec.mutable_snippet_spec()->set_get_embedding_match_info(true);
  TypePropertyMask* email_type_property_mask =
      result_spec.add_type_property_masks();
  email_type_property_mask->set_schema_type("Email");
  email_type_property_mask->add_paths("sender.name");
  email_type_property_mask->add_paths("sender.emailAddress");
  TypePropertyMask* phone_type_property_mask =
      result_spec.add_type_property_masks();
  phone_type_property_mask->set_schema_type("Phone");
  phone_type_property_mask->add_paths("caller");
  TypePropertyMask* wildcard_type_property_mask =
      result_spec.add_type_property_masks();
  wildcard_type_property_mask->set_schema_type(
      std::string(SchemaStore::kSchemaTypeWildcard));
  wildcard_type_property_mask->add_paths("wild.card");

  // Create EmbeddingQueryResults for constructing embedding match info
  // (embedding snippet).
  EmbeddingQueryResults embedding_query_results(/*num_query_vectors=*/2);
  EmbeddingMatchInfos& info_query0_doc0 =
      GetOrCreateEmbeddingMatchInfosForDocument(
          embedding_query_results, /*query_index=*/0,
          search_spec.embedding_query_metric_type(), /*doc_id=*/0);
  info_query0_doc0.AppendScore(*embedding_query_results.global_scores, 1);
  info_query0_doc0.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/0,
      /*position=*/0);
  info_query0_doc0.AppendScore(*embedding_query_results.global_scores, 1.7);
  info_query0_doc0.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/0,
      /*position=*/3);
  info_query0_doc0.AppendScore(*embedding_query_results.global_scores, 3.3);
  info_query0_doc0.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/1,
      /*position=*/1);
  EmbeddingMatchInfos& info_query1_doc0 =
      GetOrCreateEmbeddingMatchInfosForDocument(
          embedding_query_results, /*query_index=*/1,
          search_spec.embedding_query_metric_type(), /*doc_id=*/0);
  info_query1_doc0.AppendScore(*embedding_query_results.global_scores, 2);
  info_query1_doc0.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/0,
      /*position=*/0);
  info_query1_doc0.AppendScore(*embedding_query_results.global_scores, 1.7);
  info_query1_doc0.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/3,
      /*position=*/2);
  EmbeddingMatchInfos& info_query1_doc1 =
      GetOrCreateEmbeddingMatchInfosForDocument(
          embedding_query_results, /*query_index=*/1, EMBEDDING_METRIC_COSINE,
          /*doc_id=*/1);
  info_query1_doc1.AppendScore(*embedding_query_results.global_scores, 6.66);
  info_query1_doc1.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/1,
      /*position=*/0);
  EmbeddingMatchInfos& info_query0_doc2 =
      GetOrCreateEmbeddingMatchInfosForDocument(
          embedding_query_results, /*query_index=*/0, EMBEDDING_METRIC_COSINE,
          /*doc_id=*/2);
  info_query0_doc2.AppendScore(*embedding_query_results.global_scores, 5.25);
  info_query0_doc2.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/1,
      /*position=*/0);
  info_query0_doc2.AppendScore(*embedding_query_results.global_scores, 1.33);
  info_query0_doc2.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/1,
      /*position=*/4);
  EmbeddingMatchInfos& info_query1_doc3 =
      GetOrCreateEmbeddingMatchInfosForDocument(
          embedding_query_results, /*query_index=*/1, EMBEDDING_METRIC_COSINE,
          /*doc_id=*/3);
  info_query1_doc3.AppendScore(*embedding_query_results.global_scores, 3.25);
  info_query1_doc3.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/1,
      /*position=*/1);
  info_query1_doc3.AppendScore(*embedding_query_results.global_scores, 2.33);
  info_query1_doc3.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/1,
      /*position=*/2);

  // Creates a ResultState with 4 ScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits),
          /*is_descending=*/true),
      /*parent_adjustment_info_in=*/
      std::make_unique<ResultAdjustmentInfo>(
          search_spec, CreateScoringSpec(/*is_descending_order=*/false),
          result_spec, schema_store_.get(), embedding_query_results,
          /*documents_to_snippet_hint=*/
          std::unordered_set<DocumentId>{0, 1, 2, 3}, query_terms_map),
      /*child_adjustment_info_in=*/nullptr, result_spec, *schema_store_,
      *document_store_);

  absl_ports::unique_lock l(&result_state.mutex);

  ASSERT_THAT(result_state.parent_adjustment_info, NotNull());
  // Optimize the result adjustment info.
  // - Original document id 0 and 3 are deleted.
  // - Doc 1 -> doc 0.
  // - Doc 2 -> doc 1.
  DocumentStore::OptimizeResult optimize_result = {
      .document_id_old_to_new = {kInvalidDocumentId, 0, 1, kInvalidDocumentId},
      .namespace_id_old_to_new =
          {0, 1},  // namespace id remapping does not matter here.
      .should_rebuild_index = false,
      .dead_blob_handles = {}};
  EXPECT_THAT(result_state.Optimize(optimize_result), IsOk());
  EXPECT_THAT(result_state.parent_adjustment_info, NotNull());

  // Verify the snippet context.
  const SnippetContext& snippet_context =
      result_state.parent_adjustment_info->snippet_context;

  // Snippet context query terms should be unchanged.
  EXPECT_THAT(snippet_context.query_terms, Contains(Key("term1")));

  // Snippet context embedding query vector metadata map should be unchanged.
  EXPECT_THAT(snippet_context.embedding_query_vector_metadata_map,
              UnorderedElementsAre(
                  Pair(3, UnorderedElementsAre(
                              Pair("my_model1", UnorderedElementsAre(0, 3)),
                              Pair("my_model3", UnorderedElementsAre(2)))),
                  Pair(4, UnorderedElementsAre(
                              Pair("my_model2", UnorderedElementsAre(1))))));

  // Check embedding match info map -- this should contain all match infos for
  // new document ids.
  // Note: Document 0 and document 3 are deleted, so there should be only 2
  //   entries remaining in the map.
  EXPECT_THAT(snippet_context.embedding_match_info_map, SizeIs(2));
  // Document 1 (new id: 0)
  EXPECT_THAT(
      snippet_context.embedding_match_info_map,
      Contains(Pair(0, UnorderedElementsAre(EqualsEmbeddingMatchInfoEntry(
                           SnippetContext::EmbeddingMatchInfoEntry(
                               /*score=*/6.66, EMBEDDING_METRIC_COSINE,
                               /*position=*/0, /*query_vector_index=*/1,
                               /*section_id=*/1))))));
  // Document 2 (new id: 1)
  EXPECT_THAT(
      snippet_context.embedding_match_info_map,
      Contains(Pair(
          1,
          UnorderedElementsAre(
              EqualsEmbeddingMatchInfoEntry(
                  SnippetContext::EmbeddingMatchInfoEntry(
                      /*score=*/5.25, EMBEDDING_METRIC_COSINE,
                      /*position=*/0, /*query_vector_index=*/0,
                      /*section_id=*/1)),
              EqualsEmbeddingMatchInfoEntry(
                  SnippetContext::EmbeddingMatchInfoEntry(
                      /*score=*/1.33, EMBEDDING_METRIC_COSINE, /*position=*/4,
                      /*query_vector_index=*/0, /*section_id=*/1))))));

  EXPECT_THAT(snippet_context.snippet_spec,
              EqualsProto(result_spec.snippet_spec()));
  EXPECT_THAT(snippet_context.match_type, Eq(TermMatchType::EXACT_ONLY));
  EXPECT_THAT(result_state.parent_adjustment_info->remaining_num_to_snippet,
              Eq(5));

  ProjectionTree email_projection_tree =
      ProjectionTree({"Email", {"sender.name", "sender.emailAddress"}});
  ProjectionTree alternative_email_projection_tree =
      ProjectionTree({"Email", {"sender.emailAddress", "sender.name"}});
  ProjectionTree phone_projection_tree = ProjectionTree({"Phone", {"caller"}});
  ProjectionTree wildcard_projection_tree = ProjectionTree(
      {std::string(SchemaStore::kSchemaTypeWildcard), {"wild.card"}});
  // After optimization, the projection tree map should be unchanged.
  EXPECT_THAT(result_state.parent_adjustment_info->projection_tree_map,
              UnorderedElementsAre(
                  Pair("Email", AnyOf(email_projection_tree,
                                      alternative_email_projection_tree)),
                  Pair("Phone", phone_projection_tree),
                  Pair(std::string(SchemaStore::kSchemaTypeWildcard),
                       wildcard_projection_tree)));
}

TEST_F(ResultStateV2Test, Optimize_resultAdjustmentInfo_child) {
  // Add 4 ScoredDocumentHits.
  // Intentionally set the score same as the original document id, so we can
  // easily verify the remapping is correct after optimization.
  ScoredDocumentHit scored_doc_hit0 =
      AddScoredDocument("namespace", "uri0", /*score=*/0);
  ScoredDocumentHit scored_doc_hit1 =
      AddScoredDocument("namespace", "uri1", /*score=*/1);
  ScoredDocumentHit scored_doc_hit2 =
      AddScoredDocument("namespace", "uri2", /*score=*/2);
  ScoredDocumentHit scored_doc_hit3 =
      AddScoredDocument("namespace", "uri3", /*score=*/3);
  std::vector<ScoredDocumentHit> scored_document_hits = {
      std::move(scored_doc_hit1), std::move(scored_doc_hit0),
      std::move(scored_doc_hit2), std::move(scored_doc_hit3)};

  SectionRestrictQueryTermsMap query_terms_map;
  query_terms_map.emplace("term1", std::unordered_set<std::string>());

  std::vector<PropertyProto::VectorProto> embedding_query_vectors = {
      CreateVector("my_model1", {1, -2, -4}),
      CreateVector("my_model2", {1, -2, 3, -4}),
      CreateVector("my_model3", {0.1, -0.2, 0.3}),
      CreateVector("my_model1", {1, -2, -5})};

  // Search spec.
  SearchSpecProto search_spec =
      CreateSearchSpec(TermMatchType::EXACT_ONLY, embedding_query_vectors,
                       EMBEDDING_METRIC_DOT_PRODUCT);

  // Create ResultSpec with custom snippet spec and projection tree.
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/3);
  result_spec.set_max_joined_children_per_parent_to_return(
      std::numeric_limits<int32_t>::max());
  result_spec.mutable_snippet_spec()->set_num_to_snippet(5);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(5);
  result_spec.mutable_snippet_spec()->set_max_window_utf32_length(5);
  result_spec.mutable_snippet_spec()->set_get_embedding_match_info(true);
  TypePropertyMask* email_type_property_mask =
      result_spec.add_type_property_masks();
  email_type_property_mask->set_schema_type("Email");
  email_type_property_mask->add_paths("sender.name");
  email_type_property_mask->add_paths("sender.emailAddress");
  TypePropertyMask* phone_type_property_mask =
      result_spec.add_type_property_masks();
  phone_type_property_mask->set_schema_type("Phone");
  phone_type_property_mask->add_paths("caller");
  TypePropertyMask* wildcard_type_property_mask =
      result_spec.add_type_property_masks();
  wildcard_type_property_mask->set_schema_type(
      std::string(SchemaStore::kSchemaTypeWildcard));
  wildcard_type_property_mask->add_paths("wild.card");

  // Create EmbeddingQueryResults for constructing embedding match info
  // (embedding snippet).
  EmbeddingQueryResults embedding_query_results(/*num_query_vectors=*/2);
  EmbeddingMatchInfos& info_query0_doc0 =
      GetOrCreateEmbeddingMatchInfosForDocument(
          embedding_query_results, /*query_index=*/0,
          search_spec.embedding_query_metric_type(), /*doc_id=*/0);
  info_query0_doc0.AppendScore(*embedding_query_results.global_scores, 1);
  info_query0_doc0.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/0,
      /*position=*/0);
  info_query0_doc0.AppendScore(*embedding_query_results.global_scores, 1.7);
  info_query0_doc0.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/0,
      /*position=*/3);
  info_query0_doc0.AppendScore(*embedding_query_results.global_scores, 3.3);
  info_query0_doc0.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/1,
      /*position=*/1);
  EmbeddingMatchInfos& info_query1_doc0 =
      GetOrCreateEmbeddingMatchInfosForDocument(
          embedding_query_results, /*query_index=*/1,
          search_spec.embedding_query_metric_type(), /*doc_id=*/0);
  info_query1_doc0.AppendScore(*embedding_query_results.global_scores, 2);
  info_query1_doc0.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/0,
      /*position=*/0);
  info_query1_doc0.AppendScore(*embedding_query_results.global_scores, 1.7);
  info_query1_doc0.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/3,
      /*position=*/2);
  EmbeddingMatchInfos& info_query1_doc1 =
      GetOrCreateEmbeddingMatchInfosForDocument(
          embedding_query_results, /*query_index=*/1, EMBEDDING_METRIC_COSINE,
          /*doc_id=*/1);
  info_query1_doc1.AppendScore(*embedding_query_results.global_scores, 6.66);
  info_query1_doc1.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/1,
      /*position=*/0);
  EmbeddingMatchInfos& info_query0_doc2 =
      GetOrCreateEmbeddingMatchInfosForDocument(
          embedding_query_results, /*query_index=*/0, EMBEDDING_METRIC_COSINE,
          /*doc_id=*/2);
  info_query0_doc2.AppendScore(*embedding_query_results.global_scores, 5.25);
  info_query0_doc2.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/1,
      /*position=*/0);
  info_query0_doc2.AppendScore(*embedding_query_results.global_scores, 1.33);
  info_query0_doc2.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/1,
      /*position=*/4);
  EmbeddingMatchInfos& info_query1_doc3 =
      GetOrCreateEmbeddingMatchInfosForDocument(
          embedding_query_results, /*query_index=*/1, EMBEDDING_METRIC_COSINE,
          /*doc_id=*/3);
  info_query1_doc3.AppendScore(*embedding_query_results.global_scores, 3.25);
  info_query1_doc3.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/1,
      /*position=*/1);
  info_query1_doc3.AppendScore(*embedding_query_results.global_scores, 2.33);
  info_query1_doc3.AppendSectionInfo(
      *embedding_query_results.global_section_infos, /*section_id=*/1,
      /*position=*/2);

  // Creates a ResultState with 4 ScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits),
          /*is_descending=*/true),
      /*parent_adjustment_info_in=*/nullptr,
      /*child_adjustment_info_in=*/
      std::make_unique<ResultAdjustmentInfo>(
          search_spec, CreateScoringSpec(/*is_descending_order=*/false),
          result_spec, schema_store_.get(), embedding_query_results,
          /*documents_to_snippet_hint=*/
          std::unordered_set<DocumentId>{0, 1, 2, 3}, query_terms_map),
      result_spec, *schema_store_, *document_store_);

  absl_ports::unique_lock l(&result_state.mutex);

  ASSERT_THAT(result_state.child_adjustment_info, NotNull());
  // Optimize the result adjustment info.
  // - Original document id 0 and 3 are deleted.
  // - Doc 1 -> doc 0.
  // - Doc 2 -> doc 1.
  DocumentStore::OptimizeResult optimize_result = {
      .document_id_old_to_new = {kInvalidDocumentId, 0, 1, kInvalidDocumentId},
      .namespace_id_old_to_new =
          {0, 1},  // namespace id remapping does not matter here.
      .should_rebuild_index = false,
      .dead_blob_handles = {}};
  EXPECT_THAT(result_state.Optimize(optimize_result), IsOk());
  EXPECT_THAT(result_state.child_adjustment_info, NotNull());

  // Verify the snippet context.
  const SnippetContext& snippet_context =
      result_state.child_adjustment_info->snippet_context;

  // Snippet context query terms should be unchanged.
  EXPECT_THAT(snippet_context.query_terms, Contains(Key("term1")));

  // Snippet context embedding query vector metadata map should be unchanged.
  EXPECT_THAT(snippet_context.embedding_query_vector_metadata_map,
              UnorderedElementsAre(
                  Pair(3, UnorderedElementsAre(
                              Pair("my_model1", UnorderedElementsAre(0, 3)),
                              Pair("my_model3", UnorderedElementsAre(2)))),
                  Pair(4, UnorderedElementsAre(
                              Pair("my_model2", UnorderedElementsAre(1))))));

  // Check embedding match info map -- this should contain all match infos for
  // new document ids.
  // Note: Document 0 and document 3 are deleted, so there should be only 2
  //   entries remaining in the map.
  EXPECT_THAT(snippet_context.embedding_match_info_map, SizeIs(2));
  // Document 1 (new id: 0)
  EXPECT_THAT(
      snippet_context.embedding_match_info_map,
      Contains(Pair(0, UnorderedElementsAre(EqualsEmbeddingMatchInfoEntry(
                           SnippetContext::EmbeddingMatchInfoEntry(
                               /*score=*/6.66, EMBEDDING_METRIC_COSINE,
                               /*position=*/0, /*query_vector_index=*/1,
                               /*section_id=*/1))))));
  // Document 2 (new id: 1)
  EXPECT_THAT(
      snippet_context.embedding_match_info_map,
      Contains(Pair(
          1,
          UnorderedElementsAre(
              EqualsEmbeddingMatchInfoEntry(
                  SnippetContext::EmbeddingMatchInfoEntry(
                      /*score=*/5.25, EMBEDDING_METRIC_COSINE,
                      /*position=*/0, /*query_vector_index=*/0,
                      /*section_id=*/1)),
              EqualsEmbeddingMatchInfoEntry(
                  SnippetContext::EmbeddingMatchInfoEntry(
                      /*score=*/1.33, EMBEDDING_METRIC_COSINE, /*position=*/4,
                      /*query_vector_index=*/0, /*section_id=*/1))))));

  EXPECT_THAT(snippet_context.snippet_spec,
              EqualsProto(result_spec.snippet_spec()));
  EXPECT_THAT(snippet_context.match_type, Eq(TermMatchType::EXACT_ONLY));
  EXPECT_THAT(result_state.child_adjustment_info->remaining_num_to_snippet,
              Eq(5));

  ProjectionTree email_projection_tree =
      ProjectionTree({"Email", {"sender.name", "sender.emailAddress"}});
  ProjectionTree alternative_email_projection_tree =
      ProjectionTree({"Email", {"sender.emailAddress", "sender.name"}});
  ProjectionTree phone_projection_tree = ProjectionTree({"Phone", {"caller"}});
  ProjectionTree wildcard_projection_tree = ProjectionTree(
      {std::string(SchemaStore::kSchemaTypeWildcard), {"wild.card"}});
  // After optimization, the projection tree map should be unchanged.
  EXPECT_THAT(result_state.child_adjustment_info->projection_tree_map,
              UnorderedElementsAre(
                  Pair("Email", AnyOf(email_projection_tree,
                                      alternative_email_projection_tree)),
                  Pair("Phone", phone_projection_tree),
                  Pair(std::string(SchemaStore::kSchemaTypeWildcard),
                       wildcard_projection_tree)));
}

TEST_F(ResultStateV2Test,
       Optimize_namespaceIdChangedShouldRemapResultGroupingEntryIds) {
  // Add 6 ScoredDocumentHits with different namespaces.
  // Intentionally set the score same as the original document id, so we can
  // easily verify the remapping is correct after optimization.
  ScoredDocumentHit scored_doc_hit0 =
      AddScoredDocument("namespace0", "uri0", /*score=*/0);
  ScoredDocumentHit scored_doc_hit1 =
      AddScoredDocument("namespace1", "uri1", /*score=*/1);
  ScoredDocumentHit scored_doc_hit2 =
      AddScoredDocument("namespace2", "uri2", /*score=*/2);
  ScoredDocumentHit scored_doc_hit3 =
      AddScoredDocument("namespace1", "uri3", /*score=*/3);
  ScoredDocumentHit scored_doc_hit4 =
      AddScoredDocument("namespace2", "uri4", /*score=*/4);
  ScoredDocumentHit scored_doc_hit5 =
      AddScoredDocument("namespace2", "uri5", /*score=*/5);
  std::vector<ScoredDocumentHit> scored_document_hits = {
      std::move(scored_doc_hit1), std::move(scored_doc_hit0),
      std::move(scored_doc_hit2), std::move(scored_doc_hit4),
      std::move(scored_doc_hit3), std::move(scored_doc_hit5)};

  // Adjustment info optimization will be tested separately in another test, so
  // we set them to nullptr here.
  // Creates a ResultState with 6 ScoredDocumentHits and NAMESPACE result
  // grouping type.
  ResultSpecProto result_spec =
      CreateResultSpec(/*num_per_page=*/5, ResultSpecProto::NAMESPACE);

  ResultSpecProto::ResultGrouping* result_grouping1 =
      result_spec.add_result_groupings();
  ResultSpecProto::ResultGrouping::Entry* entry1 =
      result_grouping1->add_entry_groupings();
  result_grouping1->set_max_results(3);
  entry1->set_namespace_("namespace1");

  ResultSpecProto::ResultGrouping* result_grouping2 =
      result_spec.add_result_groupings();
  result_grouping2->set_max_results(100);
  ResultSpecProto::ResultGrouping::Entry* entry2 =
      result_grouping2->add_entry_groupings();
  entry2->set_namespace_("namespace2");

  ResultSpecProto::ResultGrouping* result_grouping3 =
      result_spec.add_result_groupings();
  result_grouping3->set_max_results(5);
  ResultSpecProto::ResultGrouping::Entry* entry3 =
      result_grouping3->add_entry_groupings();
  entry3->set_namespace_("namespace0");

  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits),
          /*is_descending=*/true),
      /*parent_adjustment_info_in=*/nullptr,
      /*child_adjustment_info_in=*/nullptr, result_spec, *schema_store_,
      *document_store_);

  absl_ports::unique_lock l(&result_state.mutex);

  // Retrieve 3 docs from the ResultState. Hits for document 5, 4 and 3
  // (("namespace2", "uri5"), ("namespace2", "uri4"), and ("namespace1",
  // "uri3")) are returned.
  std::vector<JoinedScoredDocumentHit> retrieved_hits =
      RetrieveNextK(result_state, /*k=*/3);
  ASSERT_THAT(retrieved_hits, SizeIs(3));
  ASSERT_THAT(retrieved_hits[0].parent_scored_document_hit(),
              EqualsScoredDocumentHit(ScoredDocumentHit(
                  /*document_id=*/5, kSectionIdMaskNone, /*score=*/5)));
  ASSERT_THAT(retrieved_hits[1].parent_scored_document_hit(),
              EqualsScoredDocumentHit(ScoredDocumentHit(
                  /*document_id=*/4, kSectionIdMaskNone, /*score=*/4)));
  ASSERT_THAT(retrieved_hits[2].parent_scored_document_hit(),
              EqualsScoredDocumentHit(ScoredDocumentHit(
                  /*document_id=*/3, kSectionIdMaskNone, /*score=*/3)));

  // Simulate the behavior of the ResultRetrieverV2: decrement the group result
  // limits for each group:
  // - namespace0: 5 -> 5
  // - namespace1: 3 -> 2
  // - namespace2: 100 -> 98
  ASSERT_THAT(document_store_->GetNamespaceId("namespace0"),
              IsOkAndHolds(Eq(0)));
  ASSERT_THAT(document_store_->GetNamespaceId("namespace1"),
              IsOkAndHolds(Eq(1)));
  ASSERT_THAT(document_store_->GetNamespaceId("namespace2"),
              IsOkAndHolds(Eq(2)));
  ICING_ASSERT_HAS_VALUE_AND_ASSIGN(
      result_utils::ResultGroupingEntryId ns0_result_grouping_entry_id,
      result_utils::EncodeResultGroupingEntryId(
          *schema_store_, *document_store_, ResultSpecProto::NAMESPACE,
          "namespace0", "SchemaType"));
  ICING_ASSERT_HAS_VALUE_AND_ASSIGN(
      result_utils::ResultGroupingEntryId ns1_result_grouping_entry_id,
      result_utils::EncodeResultGroupingEntryId(
          *schema_store_, *document_store_, ResultSpecProto::NAMESPACE,
          "namespace1", "SchemaType"));
  ICING_ASSERT_HAS_VALUE_AND_ASSIGN(
      result_utils::ResultGroupingEntryId ns2_result_grouping_entry_id,
      result_utils::EncodeResultGroupingEntryId(
          *schema_store_, *document_store_, ResultSpecProto::NAMESPACE,
          "namespace2", "SchemaType"));
  ASSERT_THAT(result_state.entry_id_group_index_map,
              UnorderedElementsAre(Key(ns0_result_grouping_entry_id),
                                   Key(ns1_result_grouping_entry_id),
                                   Key(ns2_result_grouping_entry_id)));
  int ns1_group_idx =
      result_state.entry_id_group_index_map[ns1_result_grouping_entry_id];
  int ns2_group_idx =
      result_state.entry_id_group_index_map[ns2_result_grouping_entry_id];
  result_state.group_result_limits[ns1_group_idx] -= 1;
  result_state.group_result_limits[ns2_group_idx] -= 2;

  // Delete document 0 and optimize the document store. Since "namespace0" is
  // deleted and "namespace1" is added first, namespace ids are changed.
  int64_t current_time_ms = clock_.GetSystemTimeMilliseconds();
  ICING_ASSERT_OK(document_store_->Delete(/*document_id=*/0, current_time_ms));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::OptimizeResult optimize_result,
                             OptimizeDocumentStore());
  // Remapping:
  // - Document ids:
  //   - 1 -> 0
  //   - 2 -> 1
  //   - 3 -> 2
  //   - 4 -> 3
  //   - 5 -> 4
  // - Namespace ids:
  //   - 0 -> kInvalidNamespaceId
  //   - 1 -> 0
  //   - 2 -> 1
  ASSERT_THAT(optimize_result.document_id_old_to_new,
              ElementsAre(kInvalidDocumentId, 0, 1, 2, 3, 4));
  ASSERT_THAT(optimize_result.namespace_id_old_to_new,
              ElementsAre(kInvalidNamespaceId, 0, 1));

  // Optimize the ResultState.
  EXPECT_THAT(result_state.Optimize(optimize_result), IsOk());
  ASSERT_THAT(document_store_->GetNamespaceId("namespace1"),
              IsOkAndHolds(Eq(0)));
  ASSERT_THAT(document_store_->GetNamespaceId("namespace2"),
              IsOkAndHolds(Eq(1)));

  // Verify:
  // - Result grouping entry ids are remapped to the original group indices
  //   correctly.
  // - Group result limits count are unchanged.
  ICING_ASSERT_HAS_VALUE_AND_ASSIGN(
      result_utils::ResultGroupingEntryId new_ns1_result_grouping_entry_id,
      result_utils::EncodeResultGroupingEntryId(
          *schema_store_, *document_store_, ResultSpecProto::NAMESPACE,
          "namespace1", "SchemaType"));
  ICING_ASSERT_HAS_VALUE_AND_ASSIGN(
      result_utils::ResultGroupingEntryId new_ns2_result_grouping_entry_id,
      result_utils::EncodeResultGroupingEntryId(
          *schema_store_, *document_store_, ResultSpecProto::NAMESPACE,
          "namespace2", "SchemaType"));
  EXPECT_THAT(new_ns1_result_grouping_entry_id,
              Ne(ns1_result_grouping_entry_id));
  EXPECT_THAT(new_ns2_result_grouping_entry_id,
              Ne(ns2_result_grouping_entry_id));
  EXPECT_THAT(result_state.entry_id_group_index_map,
              UnorderedElementsAre(
                  Pair(new_ns1_result_grouping_entry_id, ns1_group_idx),
                  Pair(new_ns2_result_grouping_entry_id, ns2_group_idx)));
  EXPECT_THAT(result_state.group_result_limits[ns1_group_idx], Eq(2));
  EXPECT_THAT(result_state.group_result_limits[ns2_group_idx], Eq(98));
}

TEST_F(ResultStateV2Test, Clear) {
  // Create 5 ScoredDocumentHits.
  ScoredDocumentHit scord_doc_hit0 = AddScoredDocument("namespace", "uri0");
  ScoredDocumentHit scord_doc_hit1 = AddScoredDocument("namespace", "uri1");
  ScoredDocumentHit scord_doc_hit2 = AddScoredDocument("namespace", "uri2");
  ScoredDocumentHit scord_doc_hit3 = AddScoredDocument("namespace", "uri3");
  ScoredDocumentHit scord_doc_hit4 = AddScoredDocument("namespace", "uri4");

  // Shuffle the order of the ScoredDocumentHits.
  std::vector<ScoredDocumentHit> scored_document_hits = {
      std::move(scord_doc_hit1), std::move(scord_doc_hit0),
      std::move(scord_doc_hit2), std::move(scord_doc_hit4),
      std::move(scord_doc_hit3)};

  // Adjustment info is not important in this test.
  // Creates a ResultState with 5 ScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits),
          /*is_descending=*/true),
      /*parent_adjustment_info_in=*/nullptr,
      /*child_adjustment_info_in=*/nullptr,
      CreateResultSpec(/*num_per_page=*/5, ResultSpecProto::NAMESPACE),
      *schema_store_, *document_store_);

  absl_ports::unique_lock l(&result_state.mutex);

  EXPECT_THAT(num_total_hits_, Eq(0));
  result_state.RegisterNumTotalHits(&num_total_hits_);
  EXPECT_THAT(num_total_hits_, Eq(5));

  result_state.Clear();
  EXPECT_THAT(result_state.scored_document_hits_ranker, Pointee(IsEmpty()));
  EXPECT_THAT(result_state.parent_adjustment_info, IsNull());
  EXPECT_THAT(result_state.child_adjustment_info, IsNull());
  EXPECT_THAT(result_state.entry_id_group_index_map, IsEmpty());
  EXPECT_THAT(result_state.group_result_limits, IsEmpty());

  // num_total_hits_ should be unregistered by Clear() and decremented by the
  // size.
  EXPECT_THAT(num_total_hits_, Eq(0));
}

TEST_F(ResultStateV2Test, ClearAfterPartialRetrieval) {
  // Create 5 ScoredDocumentHits.
  ScoredDocumentHit scord_doc_hit0 = AddScoredDocument("namespace", "uri0");
  ScoredDocumentHit scord_doc_hit1 = AddScoredDocument("namespace", "uri1");
  ScoredDocumentHit scord_doc_hit2 = AddScoredDocument("namespace", "uri2");
  ScoredDocumentHit scord_doc_hit3 = AddScoredDocument("namespace", "uri3");
  ScoredDocumentHit scord_doc_hit4 = AddScoredDocument("namespace", "uri4");

  // Shuffle the order of the ScoredDocumentHits.
  std::vector<ScoredDocumentHit> scored_document_hits = {
      std::move(scord_doc_hit1), std::move(scord_doc_hit0),
      std::move(scord_doc_hit2), std::move(scord_doc_hit4),
      std::move(scord_doc_hit3)};

  // Adjustment info is not important in this test.
  // Creates a ResultState with 5 ScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits),
          /*is_descending=*/true),
      /*parent_adjustment_info_in=*/nullptr,
      /*child_adjustment_info_in=*/nullptr,
      CreateResultSpec(/*num_per_page=*/5, ResultSpecProto::NAMESPACE),
      *schema_store_, *document_store_);

  absl_ports::unique_lock l(&result_state.mutex);

  // Set the original num_total_hits_ to 100.
  num_total_hits_ = 100;
  result_state.RegisterNumTotalHits(&num_total_hits_);
  // num_total_hits_ should be 100 + 5 after registration.
  ASSERT_THAT(num_total_hits_, Eq(105));

  // Retrieve 2 docs from the ResultState. Hits for document 4 and 3 are
  // returned and popped from the ranker.
  std::vector<JoinedScoredDocumentHit> retrieved_hits =
      RetrieveNextK(result_state, /*k=*/2);
  ASSERT_THAT(retrieved_hits, SizeIs(2));
  ASSERT_THAT(num_total_hits_, Eq(103));

  result_state.Clear();
  EXPECT_THAT(result_state.scored_document_hits_ranker, Pointee(IsEmpty()));
  EXPECT_THAT(result_state.parent_adjustment_info, IsNull());
  EXPECT_THAT(result_state.child_adjustment_info, IsNull());
  EXPECT_THAT(result_state.entry_id_group_index_map, IsEmpty());
  EXPECT_THAT(result_state.group_result_limits, IsEmpty());

  // num_total_hits_ should be unregistered by Clear() and decremented by the
  // remaining size.
  EXPECT_THAT(num_total_hits_, Eq(100));
}

}  // namespace
}  // namespace lib
}  // namespace icing
