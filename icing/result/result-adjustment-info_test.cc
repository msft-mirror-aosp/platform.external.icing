// Copyright (C) 2023 Google LLC
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

#include "icing/result/result-adjustment-info.h"

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/feature-flags.h"
#include "icing/file/filesystem.h"
#include "icing/index/embed/embedding-query-results.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/query/query-terms.h"
#include "icing/result/projection-tree.h"
#include "icing/result/snippet-context.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/embedding-test-utils.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::AnyOf;
using ::testing::Contains;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::Key;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

constexpr DocumentId kDocumentId0 = 0;
constexpr DocumentId kDocumentId1 = 1;
constexpr DocumentId kDocumentId2 = 2;
constexpr DocumentId kDocumentId3 = 3;

constexpr SearchSpecProto::EmbeddingQueryMetricType::Code
    EMBEDDING_METRIC_DOT_PRODUCT =
        SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT;
constexpr SearchSpecProto::EmbeddingQueryMetricType::Code
    EMBEDDING_METRIC_COSINE = SearchSpecProto::EmbeddingQueryMetricType::COSINE;

class ResultAdjustmentInfoTest : public testing::Test {
 protected:
  ResultAdjustmentInfoTest() : test_dir_(GetTestTempDir() + "/icing") {
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());
  }

  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, test_dir_,
                                           &fake_clock_, feature_flags_.get()));

    SchemaProto schema =
        SchemaBuilder()
            .AddType(SchemaTypeConfigBuilder().SetType("Email"))
            .AddType(SchemaTypeConfigBuilder().SetType("Phone"))
            .Build();
    ASSERT_THAT(schema_store_->SetSchema(
                    schema, /*ignore_errors_and_delete_documents=*/false),
                IsOk());
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  const Filesystem filesystem_;
  const std::string test_dir_;
  std::unique_ptr<SchemaStore> schema_store_;
  FakeClock fake_clock_;
};

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

TEST_F(ResultAdjustmentInfoTest,
       ShouldConstructSnippetContextAccordingToSpecs_snippetAll) {
  ResultSpecProto result_spec =
      CreateResultSpec(/*num_per_page=*/2, ResultSpecProto::NAMESPACE);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(5);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(5);
  result_spec.mutable_snippet_spec()->set_max_window_utf32_length(5);
  result_spec.mutable_snippet_spec()->set_get_embedding_match_info(true);

  SectionRestrictQueryTermsMap query_terms_map;
  query_terms_map.emplace("term1", std::unordered_set<std::string>());

  std::vector<PropertyProto::VectorProto> embedding_query_vectors = {
      CreateVector("my_model1", {1, -2, -4}),
      CreateVector("my_model2", {1, -2, 3, -4}),
      CreateVector("my_model3", {0.1, -0.2, 0.3}),
      CreateVector("my_model1", {1, -2, -5})};
  SearchSpecProto search_spec =
      CreateSearchSpec(TermMatchType::EXACT_ONLY, embedding_query_vectors,
                       EMBEDDING_METRIC_DOT_PRODUCT);

  EmbeddingQueryResults embedding_query_results;
  EmbeddingMatchInfos& info_query0_doc0 =
      embedding_query_results
          .result_infos[/*query_index=*/0]
                       [search_spec.embedding_query_metric_type()]
                       [kDocumentId0];
  info_query0_doc0.AppendScore(1);
  info_query0_doc0.AppendScore(1.7);
  info_query0_doc0.AppendScore(3.3);
  info_query0_doc0.AppendSectionInfo(/*section_id=*/0, /*position=*/0);
  info_query0_doc0.AppendSectionInfo(/*section_id=*/0, /*position=*/3);
  info_query0_doc0.AppendSectionInfo(/*section_id=*/1, /*position=*/1);
  EmbeddingMatchInfos& info_query1_doc0 =
      embedding_query_results
          .result_infos[/*query_index=*/1]
                       [search_spec.embedding_query_metric_type()]
                       [kDocumentId0];
  info_query1_doc0.AppendScore(2);
  info_query1_doc0.AppendScore(1.7);
  info_query1_doc0.AppendSectionInfo(/*section_id=*/0, /*position=*/0);
  info_query1_doc0.AppendSectionInfo(/*section_id=*/3, /*position=*/2);
  EmbeddingMatchInfos& info_query1_doc1 =
      embedding_query_results
          .result_infos[/*query_index=*/1][EMBEDDING_METRIC_COSINE]
                       [kDocumentId1];
  info_query1_doc1.AppendScore(6.66);
  info_query1_doc1.AppendSectionInfo(/*section_id=*/1, /*position=*/0);
  EmbeddingMatchInfos& info_query0_doc2 =
      embedding_query_results
          .result_infos[/*query_index=*/0][EMBEDDING_METRIC_COSINE]
                       [kDocumentId2];
  info_query0_doc2.AppendScore(5.25);
  info_query0_doc2.AppendSectionInfo(/*section_id=*/1, /*position=*/0);
  info_query0_doc2.AppendScore(1.33);
  info_query0_doc2.AppendSectionInfo(/*section_id=*/1, /*position=*/4);
  EmbeddingMatchInfos& info_query1_doc3 =
      embedding_query_results
          .result_infos[/*query_index=*/1][EMBEDDING_METRIC_COSINE]
                       [kDocumentId3];
  info_query1_doc3.AppendScore(3.25);
  info_query1_doc3.AppendSectionInfo(/*section_id=*/1, /*position=*/1);
  info_query1_doc3.AppendScore(2.33);
  info_query1_doc3.AppendSectionInfo(/*section_id=*/1, /*position=*/2);

  ResultAdjustmentInfo result_adjustment_info(
      search_spec, CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      schema_store_.get(),
      embedding_query_results, /*documents_to_snippet_hint=*/
      {kDocumentId0, kDocumentId1, kDocumentId2, kDocumentId3},
      query_terms_map);
  const SnippetContext& snippet_context =
      result_adjustment_info.snippet_context;

  // Snippet context should be derived from the specs above.
  EXPECT_THAT(snippet_context.query_terms, Contains(Key("term1")));

  EXPECT_THAT(snippet_context.embedding_query_vector_metadata_map,
              UnorderedElementsAre(
                  Pair(3, UnorderedElementsAre(
                              Pair("my_model1", UnorderedElementsAre(0, 3)),
                              Pair("my_model3", UnorderedElementsAre(2)))),
                  Pair(4, UnorderedElementsAre(
                              Pair("my_model2", UnorderedElementsAre(1))))));

  // Check embedding match info map -- this should contain all match infos.
  // Document 0
  EXPECT_THAT(
      snippet_context.embedding_match_info_map,
      Contains(Pair(kDocumentId0,
                    UnorderedElementsAre(
                        EqualsEmbeddingMatchInfoEntry(
                            SnippetContext::EmbeddingMatchInfoEntry(
                                /*score_in=*/1, EMBEDDING_METRIC_DOT_PRODUCT,
                                /*position=*/0, /*query_vector_index=*/0,
                                /*section_id=*/0)),
                        EqualsEmbeddingMatchInfoEntry(
                            SnippetContext::EmbeddingMatchInfoEntry(
                                /*score=*/1.7, EMBEDDING_METRIC_DOT_PRODUCT,
                                /*position=*/3, /*query_vector_index=*/0,
                                /*section_id=*/0)),
                        EqualsEmbeddingMatchInfoEntry(
                            SnippetContext::EmbeddingMatchInfoEntry(
                                /*score=*/2, EMBEDDING_METRIC_DOT_PRODUCT,
                                /*position=*/0, /*query_vector_index=*/1,
                                /*section_id=*/0)),
                        EqualsEmbeddingMatchInfoEntry(
                            SnippetContext::EmbeddingMatchInfoEntry(
                                /*score=*/3.3, EMBEDDING_METRIC_DOT_PRODUCT,
                                /*position=*/1, /*query_vector_index=*/0,
                                /*section_id=*/1)),
                        EqualsEmbeddingMatchInfoEntry(
                            SnippetContext::EmbeddingMatchInfoEntry(
                                /*score=*/1.7, EMBEDDING_METRIC_DOT_PRODUCT,
                                /*position=*/2, /*query_vector_index=*/1,
                                /*section_id=*/3))))));
  // Document 1
  EXPECT_THAT(snippet_context.embedding_match_info_map,
              Contains(Pair(kDocumentId1,
                            UnorderedElementsAre(EqualsEmbeddingMatchInfoEntry(
                                SnippetContext::EmbeddingMatchInfoEntry(
                                    /*score=*/6.66, EMBEDDING_METRIC_COSINE,
                                    /*position=*/0, /*query_vector_index=*/1,
                                    /*section_id=*/1))))));
  // Document 2
  EXPECT_THAT(
      snippet_context.embedding_match_info_map,
      Contains(Pair(
          kDocumentId2,
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
  // Document 3
  EXPECT_THAT(
      snippet_context.embedding_match_info_map,
      Contains(Pair(
          kDocumentId3,
          UnorderedElementsAre(EqualsEmbeddingMatchInfoEntry(
                                   SnippetContext::EmbeddingMatchInfoEntry(
                                       /*score=*/3.25, EMBEDDING_METRIC_COSINE,
                                       /*position=*/1, /*query_vector_index=*/1,
                                       /*section_id=*/1)),
                               EqualsEmbeddingMatchInfoEntry(
                                   SnippetContext::EmbeddingMatchInfoEntry(
                                       /*score=*/2.33, EMBEDDING_METRIC_COSINE,
                                       /*position=*/2, /*query_vector_index=*/1,
                                       /*section_id=*/1))))));

  EXPECT_THAT(snippet_context.snippet_spec,
              EqualsProto(result_spec.snippet_spec()));
  EXPECT_THAT(snippet_context.match_type, Eq(TermMatchType::EXACT_ONLY));
  EXPECT_THAT(result_adjustment_info.remaining_num_to_snippet, Eq(5));
}

TEST_F(
    ResultAdjustmentInfoTest,
    ShouldConstructSnippetContextAccordingToSpecs_hitsEmbeddingSnippetLimit) {
  ResultSpecProto result_spec =
      CreateResultSpec(/*num_per_page=*/2, ResultSpecProto::NAMESPACE);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(1);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(2);
  result_spec.mutable_snippet_spec()->set_max_window_utf32_length(5);
  result_spec.mutable_snippet_spec()->set_get_embedding_match_info(true);

  SectionRestrictQueryTermsMap query_terms_map;
  query_terms_map.emplace("term1", std::unordered_set<std::string>());

  std::vector<PropertyProto::VectorProto> embedding_query_vectors = {
      CreateVector("my_model1", {1, -2, -4}),
      CreateVector("my_model2", {1, -2, 3, -4}),
      CreateVector("my_model3", {0.1, -0.2, 0.3}),
      CreateVector("my_model1", {1, -2, -5})};
  SearchSpecProto search_spec =
      CreateSearchSpec(TermMatchType::EXACT_ONLY, embedding_query_vectors,
                       EMBEDDING_METRIC_DOT_PRODUCT);

  EmbeddingQueryResults embedding_query_results;
  EmbeddingMatchInfos& info_query0_doc0 =
      embedding_query_results
          .result_infos[/*query_index=*/0]
                       [search_spec.embedding_query_metric_type()]
                       [kDocumentId0];
  info_query0_doc0.AppendScore(1);
  info_query0_doc0.AppendScore(1.7);
  info_query0_doc0.AppendScore(3.3);
  info_query0_doc0.AppendSectionInfo(/*section_id=*/0, /*position=*/0);
  info_query0_doc0.AppendSectionInfo(/*section_id=*/0, /*position=*/3);
  info_query0_doc0.AppendSectionInfo(/*section_id=*/1, /*position=*/1);
  EmbeddingMatchInfos& info_query1_doc0 =
      embedding_query_results
          .result_infos[/*query_index=*/1]
                       [search_spec.embedding_query_metric_type()]
                       [kDocumentId0];
  info_query1_doc0.AppendScore(2);
  info_query1_doc0.AppendScore(1.7);
  info_query1_doc0.AppendSectionInfo(/*section_id=*/0, /*position=*/0);
  info_query1_doc0.AppendSectionInfo(/*section_id=*/3, /*position=*/2);
  EmbeddingMatchInfos& info_query1_doc1 =
      embedding_query_results
          .result_infos[/*query_index=*/1][EMBEDDING_METRIC_COSINE]
                       [kDocumentId1];
  info_query1_doc1.AppendScore(6.66);
  info_query1_doc1.AppendSectionInfo(/*section_id=*/1, /*position=*/0);
  EmbeddingMatchInfos& info_query0_doc2 =
      embedding_query_results
          .result_infos[/*query_index=*/0][EMBEDDING_METRIC_COSINE]
                       [kDocumentId2];
  info_query0_doc2.AppendScore(5.25);
  info_query0_doc2.AppendSectionInfo(/*section_id=*/1, /*position=*/0);
  info_query0_doc2.AppendScore(1.33);
  info_query0_doc2.AppendSectionInfo(/*section_id=*/1, /*position=*/4);
  EmbeddingMatchInfos& info_query1_doc3 =
      embedding_query_results
          .result_infos[/*query_index=*/1][EMBEDDING_METRIC_COSINE]
                       [kDocumentId3];
  info_query1_doc3.AppendScore(3.25);
  info_query1_doc3.AppendSectionInfo(/*section_id=*/1, /*position=*/1);
  info_query1_doc3.AppendScore(2.33);
  info_query1_doc3.AppendSectionInfo(/*section_id=*/1, /*position=*/2);

  ResultAdjustmentInfo result_adjustment_info(
      search_spec, CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      schema_store_.get(), embedding_query_results,
      /*documents_to_snippet_hint=*/{kDocumentId0}, query_terms_map);
  const SnippetContext snippet_context = result_adjustment_info.snippet_context;

  // Snippet context should be derived from the specs above.
  EXPECT_THAT(snippet_context.query_terms, Contains(Key("term1")));

  EXPECT_THAT(snippet_context.embedding_query_vector_metadata_map,
              UnorderedElementsAre(
                  Pair(3, UnorderedElementsAre(
                              Pair("my_model1", UnorderedElementsAre(0, 3)),
                              Pair("my_model3", UnorderedElementsAre(2)))),
                  Pair(4, UnorderedElementsAre(
                              Pair("my_model2", UnorderedElementsAre(1))))));

  // Check embedding match info map
  // Should only contain Document 0, with only the top 2 matches per property.
  EXPECT_THAT(
      snippet_context.embedding_match_info_map,
      UnorderedElementsAre(Pair(
          kDocumentId0, UnorderedElementsAre(
                            EqualsEmbeddingMatchInfoEntry(
                                SnippetContext::EmbeddingMatchInfoEntry(
                                    /*score=*/1, EMBEDDING_METRIC_DOT_PRODUCT,
                                    /*position=*/0, /*query_vector_index=*/0,
                                    /*section_id=*/0)),
                            EqualsEmbeddingMatchInfoEntry(
                                SnippetContext::EmbeddingMatchInfoEntry(
                                    /*score=*/2, EMBEDDING_METRIC_DOT_PRODUCT,
                                    /*position=*/0, /*query_vector_index=*/1,
                                    /*section_id=*/0)),
                            EqualsEmbeddingMatchInfoEntry(
                                SnippetContext::EmbeddingMatchInfoEntry(
                                    /*score=*/3.3, EMBEDDING_METRIC_DOT_PRODUCT,
                                    /*position=*/1, /*query_vector_index=*/0,
                                    /*section_id=*/1)),
                            EqualsEmbeddingMatchInfoEntry(
                                SnippetContext::EmbeddingMatchInfoEntry(
                                    /*score=*/1.7, EMBEDDING_METRIC_DOT_PRODUCT,
                                    /*position=*/2, /*query_vector_index=*/1,
                                    /*section_id=*/3))))));

  EXPECT_THAT(snippet_context.snippet_spec,
              EqualsProto(result_spec.snippet_spec()));
  EXPECT_THAT(snippet_context.match_type, Eq(TermMatchType::EXACT_ONLY));
  EXPECT_THAT(result_adjustment_info.remaining_num_to_snippet, Eq(1));
}

TEST_F(
    ResultAdjustmentInfoTest,
    ShouldConstructSnippetContextAccordingToSpecs_getEmbeddingMatchInfoFalse) {
  ResultSpecProto result_spec =
      CreateResultSpec(/*num_per_page=*/2, ResultSpecProto::NAMESPACE);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(5);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(5);
  result_spec.mutable_snippet_spec()->set_max_window_utf32_length(5);
  result_spec.mutable_snippet_spec()->set_get_embedding_match_info(false);

  SectionRestrictQueryTermsMap query_terms_map;
  query_terms_map.emplace("term1", std::unordered_set<std::string>());

  std::vector<PropertyProto::VectorProto> embedding_query_vectors = {
      CreateVector("my_model1", {1, -2, -4}),
      CreateVector("my_model2", {1, -2, 3, -4})};
  SearchSpecProto search_spec =
      CreateSearchSpec(TermMatchType::EXACT_ONLY, embedding_query_vectors,
                       EMBEDDING_METRIC_DOT_PRODUCT);
  EmbeddingQueryResults embedding_query_results;
  EmbeddingMatchInfos& info_query0_doc0 =
      embedding_query_results
          .result_infos[/*query_index=*/0]
                       [search_spec.embedding_query_metric_type()]
                       [kDocumentId0];
  info_query0_doc0.AppendScore(1);
  info_query0_doc0.AppendScore(1.7);
  info_query0_doc0.AppendScore(3.3);
  info_query0_doc0.AppendSectionInfo(/*section_id=*/0, /*position=*/0);
  info_query0_doc0.AppendSectionInfo(/*section_id=*/0, /*position=*/3);
  info_query0_doc0.AppendSectionInfo(/*section_id=*/1, /*position=*/1);
  EmbeddingMatchInfos& info_query1_doc0 =
      embedding_query_results
          .result_infos[/*query_index=*/1]
                       [search_spec.embedding_query_metric_type()]
                       [kDocumentId0];
  info_query1_doc0.AppendScore(2);
  info_query1_doc0.AppendScore(1.7);
  info_query1_doc0.AppendSectionInfo(/*section_id=*/0, /*position=*/0);
  info_query1_doc0.AppendSectionInfo(/*section_id=*/3, /*position=*/2);
  EmbeddingMatchInfos& info_query1_doc1 =
      embedding_query_results
          .result_infos[/*query_index=*/1][EMBEDDING_METRIC_COSINE]
                       [kDocumentId1];
  info_query1_doc1.AppendScore(6.66);
  info_query1_doc1.AppendSectionInfo(/*section_id=*/1, /*position=*/0);

  ResultAdjustmentInfo result_adjustment_info(
      search_spec, CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      schema_store_.get(), embedding_query_results,
      /*documents_to_snippet_hint=*/{kDocumentId0, kDocumentId1},
      query_terms_map);
  const SnippetContext snippet_context = result_adjustment_info.snippet_context;

  // Snippet context should be derived from the specs above.
  EXPECT_THAT(snippet_context.query_terms, Contains(Key("term1")));
  EXPECT_THAT(snippet_context.snippet_spec,
              EqualsProto(result_spec.snippet_spec()));
  EXPECT_THAT(snippet_context.embedding_query_vector_metadata_map, IsEmpty());
  EXPECT_THAT(result_adjustment_info.snippet_context.embedding_match_info_map,
              IsEmpty());
  EXPECT_THAT(snippet_context.match_type, Eq(TermMatchType::EXACT_ONLY));
  EXPECT_THAT(result_adjustment_info.remaining_num_to_snippet, Eq(5));
}

TEST_F(
    ResultAdjustmentInfoTest,
    ShouldConstructSnippetContextAccordingToSpecs_emptyEmbeddingQueryResults) {
  ResultSpecProto result_spec =
      CreateResultSpec(/*num_per_page=*/2, ResultSpecProto::NAMESPACE);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(5);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(5);
  result_spec.mutable_snippet_spec()->set_max_window_utf32_length(5);
  result_spec.mutable_snippet_spec()->set_get_embedding_match_info(false);

  SectionRestrictQueryTermsMap query_terms_map;
  query_terms_map.emplace("term1", std::unordered_set<std::string>());

  std::vector<PropertyProto::VectorProto> embedding_query_vectors = {
      CreateVector("my_model1", {1, -2, -4}),
      CreateVector("my_model2", {1, -2, 3, -4})};
  SearchSpecProto search_spec =
      CreateSearchSpec(TermMatchType::EXACT_ONLY, embedding_query_vectors,
                       EMBEDDING_METRIC_DOT_PRODUCT);

  ResultAdjustmentInfo result_adjustment_info(
      search_spec, CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      schema_store_.get(), EmbeddingQueryResults(),
      /*documents_to_snippet_hint=*/{kDocumentId0, kDocumentId1},
      query_terms_map);
  const SnippetContext snippet_context = result_adjustment_info.snippet_context;

  // Snippet context should be derived from the specs above.
  EXPECT_THAT(snippet_context.snippet_spec,
              EqualsProto(result_spec.snippet_spec()));
  EXPECT_THAT(result_adjustment_info.snippet_context
                  .embedding_query_vector_metadata_map,
              IsEmpty());
  EXPECT_THAT(result_adjustment_info.snippet_context.embedding_match_info_map,
              IsEmpty());
  EXPECT_THAT(snippet_context.match_type, Eq(TermMatchType::EXACT_ONLY));
  EXPECT_THAT(result_adjustment_info.remaining_num_to_snippet, Eq(5));
}

TEST_F(ResultAdjustmentInfoTest, NoSnippetingShouldReturnNull) {
  ResultSpecProto result_spec =
      CreateResultSpec(/*num_per_page=*/2, ResultSpecProto::NAMESPACE);
  // Setting num_to_snippet to 0 so that snippeting info won't be
  // stored.
  result_spec.mutable_snippet_spec()->set_num_to_snippet(0);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(5);
  result_spec.mutable_snippet_spec()->set_max_window_utf32_length(5);
  result_spec.mutable_snippet_spec()->set_get_embedding_match_info(true);

  SectionRestrictQueryTermsMap query_terms_map;
  query_terms_map.emplace("term1", std::unordered_set<std::string>());

  std::vector<PropertyProto::VectorProto> embedding_query_vectors = {
      CreateVector("my_model1", {1, -2, -4}),
      CreateVector("my_model2", {1, -2, 3, -4})};
  SearchSpecProto search_spec =
      CreateSearchSpec(TermMatchType::EXACT_ONLY, embedding_query_vectors,
                       SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT);
  EmbeddingQueryResults embedding_query_results;
  EmbeddingMatchInfos& info_query0_doc0 =
      embedding_query_results
          .result_infos[/*query_index=*/0]
                       [search_spec.embedding_query_metric_type()]
                       [kDocumentId0];
  info_query0_doc0.AppendScore(1);
  info_query0_doc0.AppendScore(1.7);
  info_query0_doc0.AppendScore(3.3);
  info_query0_doc0.AppendSectionInfo(/*section_id=*/0, /*position=*/0);
  info_query0_doc0.AppendSectionInfo(/*section_id=*/0, /*position=*/3);
  info_query0_doc0.AppendSectionInfo(/*section_id=*/1, /*position=*/1);
  EmbeddingMatchInfos& info_query1_doc0 =
      embedding_query_results
          .result_infos[/*query_index=*/1]
                       [search_spec.embedding_query_metric_type()]
                       [kDocumentId0];
  info_query1_doc0.AppendScore(2);
  info_query1_doc0.AppendScore(1.7);
  info_query1_doc0.AppendSectionInfo(/*section_id=*/0, /*position=*/0);
  info_query1_doc0.AppendSectionInfo(/*section_id=*/3, /*position=*/2);
  EmbeddingMatchInfos& info_query1_doc1 =
      embedding_query_results
          .result_infos[/*query_index=*/1][EMBEDDING_METRIC_COSINE]
                       [kDocumentId1];
  info_query1_doc1.AppendScore(6.66);
  info_query1_doc1.AppendSectionInfo(/*section_id=*/1, /*position=*/0);

  ResultAdjustmentInfo result_adjustment_info(
      search_spec, CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      schema_store_.get(), embedding_query_results,
      /*documents_to_snippet_hint=*/{kDocumentId0, kDocumentId1},
      query_terms_map);

  EXPECT_THAT(result_adjustment_info.snippet_context.query_terms, IsEmpty());
  EXPECT_THAT(
      result_adjustment_info.snippet_context.snippet_spec,
      EqualsProto(ResultSpecProto::SnippetSpecProto::default_instance()));
  EXPECT_THAT(result_adjustment_info.snippet_context
                  .embedding_query_vector_metadata_map,
              IsEmpty());
  EXPECT_THAT(result_adjustment_info.snippet_context.embedding_match_info_map,
              IsEmpty());
  EXPECT_THAT(result_adjustment_info.snippet_context.match_type,
              TermMatchType::UNKNOWN);
  EXPECT_THAT(result_adjustment_info.remaining_num_to_snippet, Eq(0));
}

TEST_F(ResultAdjustmentInfoTest,
       ShouldConstructProjectionTreeMapAccordingToSpecs) {
  // Create a ResultSpec with type property mask.
  ResultSpecProto result_spec =
      CreateResultSpec(/*num_per_page=*/2, ResultSpecProto::NAMESPACE);
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

  ResultAdjustmentInfo result_adjustment_info(
      CreateSearchSpec(TermMatchType::EXACT_ONLY,
                       /*embedding_query_vectors=*/{},
                       SearchSpecProto::EmbeddingQueryMetricType::UNKNOWN),
      CreateScoringSpec(/*is_descending_order=*/true), result_spec,
      schema_store_.get(), EmbeddingQueryResults(),
      /*documents_to_snippet_hint=*/{kDocumentId0, kDocumentId1},
      /*query_terms=*/{});

  ProjectionTree email_projection_tree =
      ProjectionTree({"Email", {"sender.name", "sender.emailAddress"}});
  ProjectionTree alternative_email_projection_tree =
      ProjectionTree({"Email", {"sender.emailAddress", "sender.name"}});
  ProjectionTree phone_projection_tree = ProjectionTree({"Phone", {"caller"}});
  ProjectionTree wildcard_projection_tree = ProjectionTree(
      {std::string(SchemaStore::kSchemaTypeWildcard), {"wild.card"}});

  EXPECT_THAT(result_adjustment_info.projection_tree_map,
              UnorderedElementsAre(
                  Pair("Email", AnyOf(email_projection_tree,
                                      alternative_email_projection_tree)),
                  Pair("Phone", phone_projection_tree),
                  Pair(std::string(SchemaStore::kSchemaTypeWildcard),
                       wildcard_projection_tree)));
}

}  // namespace

}  // namespace lib
}  // namespace icing
