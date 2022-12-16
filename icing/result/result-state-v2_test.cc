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
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gtest/gtest.h"
#include "icing/absl_ports/mutex.h"
#include "icing/file/filesystem.h"
#include "icing/portable/equals-proto.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/result/projection-tree.h"
#include "icing/result/snippet-context.h"
#include "icing/schema/schema-store.h"
#include "icing/scoring/priority-queue-scored-document-hits-ranker.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/scoring/scored-document-hits-ranker.h"
#include "icing/store/document-store.h"
#include "icing/store/namespace-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/clock.h"

namespace icing {
namespace lib {
namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

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

class ResultStateV2Test : public ::testing::Test {
 protected:
  void SetUp() override {
    schema_store_base_dir_ = GetTestTempDir() + "/schema_store";
    filesystem_.CreateDirectoryRecursively(schema_store_base_dir_.c_str());
    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_,
        SchemaStore::Create(&filesystem_, schema_store_base_dir_, &clock_));
    SchemaProto schema;
    schema.add_types()->set_schema_type("Document");
    ICING_ASSERT_OK(schema_store_->SetSchema(std::move(schema)));

    doc_store_base_dir_ = GetTestTempDir() + "/document_store";
    filesystem_.CreateDirectoryRecursively(doc_store_base_dir_.c_str());
    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult result,
        DocumentStore::Create(&filesystem_, doc_store_base_dir_, &clock_,
                              schema_store_.get()));
    document_store_ = std::move(result.document_store);

    num_total_hits_ = 0;
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(doc_store_base_dir_.c_str());
    filesystem_.DeleteDirectoryRecursively(schema_store_base_dir_.c_str());
  }

  ScoredDocumentHit AddScoredDocument(DocumentId document_id) {
    DocumentProto document;
    document.set_namespace_("namespace");
    document.set_uri(std::to_string(document_id));
    document.set_schema("Document");
    document_store_->Put(std::move(document));
    return ScoredDocumentHit(document_id, kSectionIdMaskNone, /*score=*/1);
  }

  DocumentStore& document_store() { return *document_store_; }

  std::atomic<int>& num_total_hits() { return num_total_hits_; }

  const std::atomic<int>& num_total_hits() const { return num_total_hits_; }

 private:
  Filesystem filesystem_;
  std::string doc_store_base_dir_;
  std::string schema_store_base_dir_;
  Clock clock_;
  std::unique_ptr<DocumentStore> document_store_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::atomic<int> num_total_hits_;
};

TEST_F(ResultStateV2Test, ShouldInitializeValuesAccordingToSpecs) {
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/2);
  result_spec.set_num_total_bytes_per_page_threshold(4096);

  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::vector<ScoredDocumentHit>(),
          /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      document_store());

  absl_ports::shared_lock l(&result_state.mutex);

  EXPECT_THAT(result_state.num_returned, Eq(0));
  EXPECT_THAT(result_state.num_per_page(), Eq(result_spec.num_per_page()));
  EXPECT_THAT(result_state.num_total_bytes_per_page_threshold(),
              Eq(result_spec.num_total_bytes_per_page_threshold()));
}

TEST_F(ResultStateV2Test, ShouldInitializeValuesAccordingToDefaultSpecs) {
  ResultSpecProto default_result_spec = ResultSpecProto::default_instance();
  ASSERT_THAT(default_result_spec.num_per_page(), Eq(10));
  ASSERT_THAT(default_result_spec.num_total_bytes_per_page_threshold(),
              Eq(std::numeric_limits<int32_t>::max()));

  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::vector<ScoredDocumentHit>(),
          /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true), default_result_spec,
      document_store());

  absl_ports::shared_lock l(&result_state.mutex);

  EXPECT_THAT(result_state.num_returned, Eq(0));
  EXPECT_THAT(result_state.num_per_page(),
              Eq(default_result_spec.num_per_page()));
  EXPECT_THAT(result_state.num_total_bytes_per_page_threshold(),
              Eq(default_result_spec.num_total_bytes_per_page_threshold()));
}

TEST_F(ResultStateV2Test, ShouldReturnSnippetContextAccordingToSpecs) {
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/2);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(5);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(5);
  result_spec.mutable_snippet_spec()->set_max_window_utf32_length(5);

  SectionRestrictQueryTermsMap query_terms_map;
  query_terms_map.emplace("term1", std::unordered_set<std::string>());

  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::vector<ScoredDocumentHit>(),
          /*is_descending=*/true),
      query_terms_map, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      document_store());

  absl_ports::shared_lock l(&result_state.mutex);

  const SnippetContext snippet_context = result_state.snippet_context();

  // Snippet context should be derived from the specs above.
  EXPECT_TRUE(snippet_context.query_terms.find("term1") !=
              snippet_context.query_terms.end());
  EXPECT_THAT(snippet_context.snippet_spec,
              EqualsProto(result_spec.snippet_spec()));
  EXPECT_THAT(snippet_context.match_type, Eq(TermMatchType::EXACT_ONLY));

  // The same copy can be fetched multiple times.
  const SnippetContext snippet_context2 = result_state.snippet_context();
  EXPECT_TRUE(snippet_context2.query_terms.find("term1") !=
              snippet_context2.query_terms.end());
  EXPECT_THAT(snippet_context2.snippet_spec,
              EqualsProto(result_spec.snippet_spec()));
  EXPECT_THAT(snippet_context2.match_type, Eq(TermMatchType::EXACT_ONLY));
}

TEST_F(ResultStateV2Test, NoSnippetingShouldReturnNull) {
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/2);
  // Setting num_to_snippet to 0 so that snippeting info won't be
  // stored.
  result_spec.mutable_snippet_spec()->set_num_to_snippet(0);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(5);
  result_spec.mutable_snippet_spec()->set_max_window_utf32_length(5);

  SectionRestrictQueryTermsMap query_terms_map;
  query_terms_map.emplace("term1", std::unordered_set<std::string>());

  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::vector<ScoredDocumentHit>(),
          /*is_descending=*/true),
      query_terms_map, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      document_store());

  absl_ports::shared_lock l(&result_state.mutex);

  const SnippetContext snippet_context = result_state.snippet_context();
  EXPECT_THAT(snippet_context.query_terms, IsEmpty());
  EXPECT_THAT(
      snippet_context.snippet_spec,
      EqualsProto(ResultSpecProto::SnippetSpecProto::default_instance()));
  EXPECT_THAT(snippet_context.match_type, TermMatchType::UNKNOWN);
}

TEST_F(ResultStateV2Test, ShouldConstructProjectionTreeMapAccordingToSpecs) {
  // Create a ResultSpec with type property mask.
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/2);
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
      std::string(ProjectionTree::kSchemaTypeWildcard));
  wildcard_type_property_mask->add_paths("wild.card");

  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::vector<ScoredDocumentHit>(),
          /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      document_store());

  absl_ports::shared_lock l(&result_state.mutex);

  const std::unordered_map<std::string, ProjectionTree>& projection_tree_map =
      result_state.projection_tree_map();
  EXPECT_THAT(projection_tree_map,
              UnorderedElementsAre(
                  Pair("Email", ProjectionTree(*email_type_property_mask)),
                  Pair("Phone", ProjectionTree(*phone_type_property_mask)),
                  Pair(std::string(ProjectionTree::kSchemaTypeWildcard),
                       ProjectionTree(*wildcard_type_property_mask))));
}

TEST_F(ResultStateV2Test,
       ShouldConstructNamespaceGroupIdMapAndGroupResultLimitsAccordingToSpecs) {
  // Create 3 docs under namespace1, namespace2, namespace3.
  DocumentProto document1;
  document1.set_namespace_("namespace1");
  document1.set_uri("uri/1");
  document1.set_schema("Document");
  ICING_ASSERT_OK(document_store().Put(std::move(document1)));

  DocumentProto document2;
  document2.set_namespace_("namespace2");
  document2.set_uri("uri/2");
  document2.set_schema("Document");
  ICING_ASSERT_OK(document_store().Put(std::move(document2)));

  DocumentProto document3;
  document3.set_namespace_("namespace3");
  document3.set_uri("uri/3");
  document3.set_schema("Document");
  ICING_ASSERT_OK(document_store().Put(std::move(document3)));

  // Create a ResultSpec that limits "namespace1" to 3 results and limits
  // "namespace2"+"namespace3" to a total of 2 results. Also add
  // "nonexistentNamespace1" and "nonexistentNamespace2" to test the behavior.
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/5);
  ResultSpecProto::ResultGrouping* result_grouping =
      result_spec.add_result_groupings();
  result_grouping->set_max_results(3);
  result_grouping->add_namespaces("namespace1");
  result_grouping = result_spec.add_result_groupings();
  result_grouping->set_max_results(5);
  result_grouping->add_namespaces("nonexistentNamespace2");
  result_grouping = result_spec.add_result_groupings();
  result_grouping->set_max_results(2);
  result_grouping->add_namespaces("namespace2");
  result_grouping->add_namespaces("namespace3");
  result_grouping->add_namespaces("nonexistentNamespace1");

  // Get namespace ids.
  ICING_ASSERT_OK_AND_ASSIGN(NamespaceId namespace_id1,
                             document_store().GetNamespaceId("namespace1"));
  ICING_ASSERT_OK_AND_ASSIGN(NamespaceId namespace_id2,
                             document_store().GetNamespaceId("namespace2"));
  ICING_ASSERT_OK_AND_ASSIGN(NamespaceId namespace_id3,
                             document_store().GetNamespaceId("namespace3"));

  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::vector<ScoredDocumentHit>(),
          /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      document_store());

  absl_ports::shared_lock l(&result_state.mutex);

  // "namespace1" should be in group 0, and "namespace2"+"namespace3" should be
  // in group 2.
  // "nonexistentNamespace1" and "nonexistentNamespace2" shouldn't exist.
  EXPECT_THAT(
      result_state.namespace_group_id_map(),
      UnorderedElementsAre(Pair(namespace_id1, 0), Pair(namespace_id2, 2),
                           Pair(namespace_id3, 2)));

  // group_result_limits should contain 3 (at index 0 for group 0), 5 (at index
  // 1 for group 1), 2 (at index 2 for group 2), even though there is no valid
  // namespace in group 1.
  EXPECT_THAT(result_state.group_result_limits, ElementsAre(3, 5, 2));
}

TEST_F(ResultStateV2Test, ShouldUpdateNumTotalHits) {
  std::vector<ScoredDocumentHit> scored_document_hits = {
      AddScoredDocument(/*document_id=*/1),
      AddScoredDocument(/*document_id=*/0),
      AddScoredDocument(/*document_id=*/2),
      AddScoredDocument(/*document_id=*/4),
      AddScoredDocument(/*document_id=*/3)};

  // Creates a ResultState with 5 ScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits),
          /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true),
      CreateResultSpec(/*num_per_page=*/5), document_store());

  absl_ports::unique_lock l(&result_state.mutex);

  EXPECT_THAT(num_total_hits(), Eq(0));
  result_state.RegisterNumTotalHits(&num_total_hits());
  EXPECT_THAT(num_total_hits(), Eq(5));
  result_state.IncrementNumTotalHits(500);
  EXPECT_THAT(num_total_hits(), Eq(505));
}

TEST_F(ResultStateV2Test, ShouldUpdateNumTotalHitsWhenDestructed) {
  std::vector<ScoredDocumentHit> scored_document_hits1 = {
      AddScoredDocument(/*document_id=*/1),
      AddScoredDocument(/*document_id=*/0),
      AddScoredDocument(/*document_id=*/2),
      AddScoredDocument(/*document_id=*/4),
      AddScoredDocument(/*document_id=*/3)};

  std::vector<ScoredDocumentHit> scored_document_hits2 = {
      AddScoredDocument(/*document_id=*/6),
      AddScoredDocument(/*document_id=*/5)};

  num_total_hits() = 2;
  {
    // Creates a ResultState with 5 ScoredDocumentHits.
    ResultStateV2 result_state1(
        std::make_unique<
            PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
            std::move(scored_document_hits1),
            /*is_descending=*/true),
        /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
        CreateScoringSpec(/*is_descending_order=*/true),
        CreateResultSpec(/*num_per_page=*/5), document_store());

    absl_ports::unique_lock l(&result_state1.mutex);

    result_state1.RegisterNumTotalHits(&num_total_hits());
    ASSERT_THAT(num_total_hits(), Eq(7));

    {
      // Creates another ResultState with 2 ScoredDocumentHits.
      ResultStateV2 result_state2(
          std::make_unique<
              PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
              std::move(scored_document_hits2),
              /*is_descending=*/true),
          /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
          CreateScoringSpec(/*is_descending_order=*/true),
          CreateResultSpec(/*num_per_page=*/5), document_store());

      absl_ports::unique_lock l(&result_state2.mutex);

      result_state2.RegisterNumTotalHits(&num_total_hits());
      ASSERT_THAT(num_total_hits(), Eq(9));
    }

    EXPECT_THAT(num_total_hits(), Eq(7));
  }
  EXPECT_THAT(num_total_hits(), Eq(2));
}

TEST_F(ResultStateV2Test, ShouldNotUpdateNumTotalHitsWhenNotRegistered) {
  std::vector<ScoredDocumentHit> scored_document_hits = {
      AddScoredDocument(/*document_id=*/1),
      AddScoredDocument(/*document_id=*/0),
      AddScoredDocument(/*document_id=*/2),
      AddScoredDocument(/*document_id=*/4),
      AddScoredDocument(/*document_id=*/3)};

  // Creates a ResultState with 5 ScoredDocumentHits.
  {
    ResultStateV2 result_state(
        std::make_unique<
            PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
            std::move(scored_document_hits),
            /*is_descending=*/true),
        /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
        CreateScoringSpec(/*is_descending_order=*/true),
        CreateResultSpec(/*num_per_page=*/5), document_store());

    {
      absl_ports::unique_lock l(&result_state.mutex);

      EXPECT_THAT(num_total_hits(), Eq(0));
      result_state.IncrementNumTotalHits(500);
      EXPECT_THAT(num_total_hits(), Eq(0));
    }
  }
  EXPECT_THAT(num_total_hits(), Eq(0));
}

TEST_F(ResultStateV2Test, ShouldDecrementOriginalNumTotalHitsWhenReregister) {
  std::atomic<int> another_num_total_hits = 11;

  std::vector<ScoredDocumentHit> scored_document_hits = {
      AddScoredDocument(/*document_id=*/1),
      AddScoredDocument(/*document_id=*/0),
      AddScoredDocument(/*document_id=*/2),
      AddScoredDocument(/*document_id=*/4),
      AddScoredDocument(/*document_id=*/3)};

  // Creates a ResultState with 5 ScoredDocumentHits.
  ResultStateV2 result_state(
      std::make_unique<
          PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>>(
          std::move(scored_document_hits),
          /*is_descending=*/true),
      /*query_terms=*/{}, CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true),
      CreateResultSpec(/*num_per_page=*/5), document_store());

  absl_ports::unique_lock l(&result_state.mutex);

  num_total_hits() = 7;
  result_state.RegisterNumTotalHits(&num_total_hits());
  EXPECT_THAT(num_total_hits(), Eq(12));

  result_state.RegisterNumTotalHits(&another_num_total_hits);
  // The original num_total_hits should be decremented after re-registration.
  EXPECT_THAT(num_total_hits(), Eq(7));
  // another_num_total_hits should be incremented after re-registration.
  EXPECT_THAT(another_num_total_hits, Eq(16));

  result_state.IncrementNumTotalHits(500);
  // The original num_total_hits should be unchanged.
  EXPECT_THAT(num_total_hits(), Eq(7));
  // Increment should be done on another_num_total_hits.
  EXPECT_THAT(another_num_total_hits, Eq(516));
}

}  // namespace
}  // namespace lib
}  // namespace icing
