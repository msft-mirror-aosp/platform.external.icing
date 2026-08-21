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
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/absl_ports/mutex.h"
#include "icing/document-builder.h"
#include "icing/feature-flags.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/portable/gzip_stream.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/document_wrapper.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/result/result-utils.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/scoring/priority-queue-scored-document-hits-ranker.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/clock.h"

namespace icing {
namespace lib {
namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

ResultSpecProto CreateResultSpec(
    int num_per_page, ResultSpecProto::ResultGroupingType result_group_type) {
  ResultSpecProto result_spec;
  result_spec.set_result_group_type(result_group_type);
  result_spec.set_num_per_page(num_per_page);
  return result_spec;
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
    SchemaProto schema;
    schema.add_types()->set_schema_type("SchemaType");
    ICING_ASSERT_OK(schema_store_->SetSchema(
        std::move(schema), /*ignore_errors_and_delete_documents=*/false));

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
  result_state.IncrementNumTotalHits(500);
  EXPECT_THAT(num_total_hits_, Eq(505));
}

TEST_F(ResultStateV2Test, ShouldUpdateNumTotalHitsWhenDestructed) {
  // Create 7 ScoredDocumentHits.
  ScoredDocumentHit scord_doc_hit0 = AddScoredDocument("namespace", "uri0");
  ScoredDocumentHit scord_doc_hit1 = AddScoredDocument("namespace", "uri1");
  ScoredDocumentHit scord_doc_hit2 = AddScoredDocument("namespace", "uri2");
  ScoredDocumentHit scord_doc_hit3 = AddScoredDocument("namespace", "uri3");
  ScoredDocumentHit scord_doc_hit4 = AddScoredDocument("namespace", "uri4");
  ScoredDocumentHit scord_doc_hit5 = AddScoredDocument("namespace", "uri5");
  ScoredDocumentHit scord_doc_hit6 = AddScoredDocument("namespace", "uri6");

  // Create 2 vectors of ScoredDocumentHits and shuffle the order of the
  // ScoredDocumentHits.
  std::vector<ScoredDocumentHit> scored_document_hits1 = {
      std::move(scord_doc_hit1), std::move(scord_doc_hit0),
      std::move(scord_doc_hit2), std::move(scord_doc_hit4),
      std::move(scord_doc_hit3)};
  std::vector<ScoredDocumentHit> scored_document_hits2 = {
      std::move(scord_doc_hit6), std::move(scord_doc_hit5)};

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

}  // namespace
}  // namespace lib
}  // namespace icing
