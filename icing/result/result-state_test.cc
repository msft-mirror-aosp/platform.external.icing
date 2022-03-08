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

#include "icing/result/result-state.h"

#include "gtest/gtest.h"
#include "icing/portable/equals-proto.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {
namespace {
using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;

ScoredDocumentHit CreateScoredDocumentHit(DocumentId document_id) {
  return ScoredDocumentHit(document_id, kSectionIdMaskNone, /*score=*/1);
}

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

// ResultState::ResultState() and ResultState::GetNextPage() are calling
// Ranker::BuildHeapInPlace() and Ranker::PopTopResultsFromHeap() directly, so
// we don't need to test much on what order is returned as that is tested in
// Ranker's tests. Here we just need one sanity test to make sure that the
// correct functions are called.
TEST(ResultStateTest, ShouldReturnNextPage) {
  std::vector<ScoredDocumentHit> scored_document_hits = {
      CreateScoredDocumentHit(/*document_id=*/2),
      CreateScoredDocumentHit(/*document_id=*/1),
      CreateScoredDocumentHit(/*document_id=*/3),
      CreateScoredDocumentHit(/*document_id=*/5),
      CreateScoredDocumentHit(/*document_id=*/4)};

  ResultState result_state(scored_document_hits, /*query_terms=*/{},
                           CreateSearchSpec(TermMatchType::EXACT_ONLY),
                           CreateScoringSpec(/*is_descending_order=*/true),
                           CreateResultSpec(/*num_per_page=*/2));

  EXPECT_THAT(
      result_state.GetNextPage(),
      ElementsAre(
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/5)),
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/4))));

  EXPECT_THAT(
      result_state.GetNextPage(),
      ElementsAre(
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/3)),
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/2))));

  EXPECT_THAT(result_state.GetNextPage(),
              ElementsAre(EqualsScoredDocumentHit(
                  CreateScoredDocumentHit(/*document_id=*/1))));
}

TEST(ResultStateTest, ShouldReturnSnippetContextAccordingToSpecs) {
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/2);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(5);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(5);
  result_spec.mutable_snippet_spec()->set_max_window_bytes(5);

  SectionRestrictQueryTermsMap query_terms_map;
  query_terms_map.emplace("term1", std::unordered_set<std::string>());

  ResultState result_state(
      /*scored_document_hits=*/{}, query_terms_map,
      CreateSearchSpec(TermMatchType::EXACT_ONLY),
      CreateScoringSpec(/*is_descending_order=*/true), result_spec);

  const SnippetContext& snippet_context = result_state.snippet_context();

  // Snippet context should be derived from the specs above.
  EXPECT_TRUE(snippet_context.query_terms.find("term1") !=
              snippet_context.query_terms.end());
  EXPECT_THAT(snippet_context.snippet_spec,
              EqualsProto(result_spec.snippet_spec()));
  EXPECT_THAT(snippet_context.match_type, Eq(TermMatchType::EXACT_ONLY));

  // The same copy can be fetched multiple times.
  const SnippetContext& snippet_context2 = result_state.snippet_context();
  EXPECT_TRUE(snippet_context2.query_terms.find("term1") !=
              snippet_context2.query_terms.end());
  EXPECT_THAT(snippet_context2.snippet_spec,
              EqualsProto(result_spec.snippet_spec()));
  EXPECT_THAT(snippet_context2.match_type, Eq(TermMatchType::EXACT_ONLY));
}

TEST(ResultStateTest, NoSnippetingShouldReturnNull) {
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/2);
  // Setting num_to_snippet to 0 so that snippeting info won't be
  // stored.
  result_spec.mutable_snippet_spec()->set_num_to_snippet(0);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(5);
  result_spec.mutable_snippet_spec()->set_max_window_bytes(5);

  SectionRestrictQueryTermsMap query_terms_map;
  query_terms_map.emplace("term1", std::unordered_set<std::string>());

  ResultState result_state(/*scored_document_hits=*/{}, query_terms_map,
                           CreateSearchSpec(TermMatchType::EXACT_ONLY),
                           CreateScoringSpec(/*is_descending_order=*/true),
                           result_spec);

  const SnippetContext& snippet_context = result_state.snippet_context();
  EXPECT_THAT(snippet_context.query_terms, IsEmpty());
  EXPECT_THAT(
      snippet_context.snippet_spec,
      EqualsProto(ResultSpecProto::SnippetSpecProto::default_instance()));
  EXPECT_THAT(snippet_context.match_type, TermMatchType::UNKNOWN);
}

TEST(ResultStateTest, ShouldTruncateToNewSize) {
  std::vector<ScoredDocumentHit> scored_document_hits = {
      CreateScoredDocumentHit(/*document_id=*/2),
      CreateScoredDocumentHit(/*document_id=*/1),
      CreateScoredDocumentHit(/*document_id=*/3),
      CreateScoredDocumentHit(/*document_id=*/5),
      CreateScoredDocumentHit(/*document_id=*/4)};

  // Creates a ResultState with 5 ScoredDocumentHits.
  ResultState result_state(scored_document_hits, /*query_terms=*/{},
                           CreateSearchSpec(TermMatchType::EXACT_ONLY),
                           CreateScoringSpec(/*is_descending_order=*/true),
                           CreateResultSpec(/*num_per_page=*/5));

  result_state.TruncateHitsTo(/*new_size=*/3);
  // The best 3 are left.
  EXPECT_THAT(
      result_state.GetNextPage(),
      ElementsAre(
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/5)),
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/4)),
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/3))));
}

TEST(ResultStateTest, ShouldTruncateToZero) {
  std::vector<ScoredDocumentHit> scored_document_hits = {
      CreateScoredDocumentHit(/*document_id=*/2),
      CreateScoredDocumentHit(/*document_id=*/1),
      CreateScoredDocumentHit(/*document_id=*/3),
      CreateScoredDocumentHit(/*document_id=*/5),
      CreateScoredDocumentHit(/*document_id=*/4)};

  // Creates a ResultState with 5 ScoredDocumentHits.
  ResultState result_state(scored_document_hits, /*query_terms=*/{},
                           CreateSearchSpec(TermMatchType::EXACT_ONLY),
                           CreateScoringSpec(/*is_descending_order=*/true),
                           CreateResultSpec(/*num_per_page=*/5));

  result_state.TruncateHitsTo(/*new_size=*/0);
  EXPECT_THAT(result_state.GetNextPage(), IsEmpty());
}

TEST(ResultStateTest, ShouldNotTruncateToNegative) {
  std::vector<ScoredDocumentHit> scored_document_hits = {
      CreateScoredDocumentHit(/*document_id=*/2),
      CreateScoredDocumentHit(/*document_id=*/1),
      CreateScoredDocumentHit(/*document_id=*/3),
      CreateScoredDocumentHit(/*document_id=*/5),
      CreateScoredDocumentHit(/*document_id=*/4)};

  // Creates a ResultState with 5 ScoredDocumentHits.
  ResultState result_state(scored_document_hits, /*query_terms=*/{},
                           CreateSearchSpec(TermMatchType::EXACT_ONLY),
                           CreateScoringSpec(/*is_descending_order=*/true),
                           CreateResultSpec(/*num_per_page=*/5));

  result_state.TruncateHitsTo(/*new_size=*/-1);
  // Results are not affected.
  EXPECT_THAT(
      result_state.GetNextPage(),
      ElementsAre(
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/5)),
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/4)),
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/3)),
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/2)),
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/1))));
}

}  // namespace
}  // namespace lib
}  // namespace icing
