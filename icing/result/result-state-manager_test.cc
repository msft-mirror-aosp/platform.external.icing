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

#include "gtest/gtest.h"
#include "icing/portable/equals-proto.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {
namespace {
using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::Gt;
using ::testing::IsEmpty;

ScoredDocumentHit CreateScoredDocumentHit(DocumentId document_id) {
  return ScoredDocumentHit(document_id, kSectionIdMaskNone, /*score=*/1);
}

ScoringSpecProto CreateScoringSpec() {
  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  return scoring_spec;
}

ResultSpecProto CreateResultSpec(int num_per_page) {
  ResultSpecProto result_spec;
  result_spec.set_num_per_page(num_per_page);
  return result_spec;
}

ResultState CreateResultState(
    const std::vector<ScoredDocumentHit>& scored_document_hits,
    int num_per_page) {
  return ResultState(scored_document_hits, /*query_terms=*/{},
                     SearchSpecProto::default_instance(), CreateScoringSpec(),
                     CreateResultSpec(num_per_page));
}

TEST(ResultStateManagerTest, ShouldRankAndPaginateOnePage) {
  ResultState original_result_state =
      CreateResultState({CreateScoredDocumentHit(/*document_id=*/1),
                         CreateScoredDocumentHit(/*document_id=*/2),
                         CreateScoredDocumentHit(/*document_id=*/3)},
                        /*num_per_page=*/10);

  ResultStateManager result_state_manager(
      /*max_hits_per_query=*/std::numeric_limits<int>::max(),
      /*max_result_states=*/std::numeric_limits<int>::max());
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultState page_result_state,
      result_state_manager.RankAndPaginate(std::move(original_result_state)));

  EXPECT_THAT(page_result_state.next_page_token, Eq(kInvalidNextPageToken));

  // Should get the original scored document hits
  EXPECT_THAT(
      page_result_state.scored_document_hits,
      ElementsAre(
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/3)),
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/2)),
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/1))));
}

TEST(ResultStateManagerTest, ShouldRankAndPaginateMultiplePages) {
  ResultState original_result_state =
      CreateResultState({CreateScoredDocumentHit(/*document_id=*/1),
                         CreateScoredDocumentHit(/*document_id=*/2),
                         CreateScoredDocumentHit(/*document_id=*/3),
                         CreateScoredDocumentHit(/*document_id=*/4),
                         CreateScoredDocumentHit(/*document_id=*/5)},
                        /*num_per_page=*/2);

  ResultStateManager result_state_manager(
      /*max_hits_per_query=*/std::numeric_limits<int>::max(),
      /*max_result_states=*/std::numeric_limits<int>::max());

  // First page, 2 results
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultState page_result_state1,
      result_state_manager.RankAndPaginate(std::move(original_result_state)));
  EXPECT_THAT(
      page_result_state1.scored_document_hits,
      ElementsAre(
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/5)),
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/4))));

  uint64_t next_page_token = page_result_state1.next_page_token;

  // Second page, 2 results
  ICING_ASSERT_OK_AND_ASSIGN(PageResultState page_result_state2,
                             result_state_manager.GetNextPage(next_page_token));
  EXPECT_THAT(
      page_result_state2.scored_document_hits,
      ElementsAre(
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/3)),
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/2))));

  // Third page, 1 result
  ICING_ASSERT_OK_AND_ASSIGN(PageResultState page_result_state3,
                             result_state_manager.GetNextPage(next_page_token));
  EXPECT_THAT(page_result_state3.scored_document_hits,
              ElementsAre(EqualsScoredDocumentHit(
                  CreateScoredDocumentHit(/*document_id=*/1))));

  // No results
  EXPECT_THAT(result_state_manager.GetNextPage(next_page_token),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST(ResultStateManagerTest, EmptyStateShouldReturnError) {
  ResultState empty_result_state = CreateResultState({}, /*num_per_page=*/1);

  ResultStateManager result_state_manager(
      /*max_hits_per_query=*/std::numeric_limits<int>::max(),
      /*max_result_states=*/std::numeric_limits<int>::max());
  EXPECT_THAT(
      result_state_manager.RankAndPaginate(std::move(empty_result_state)),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(ResultStateManagerTest, ShouldInvalidateOneToken) {
  ResultState result_state1 =
      CreateResultState({CreateScoredDocumentHit(/*document_id=*/1),
                         CreateScoredDocumentHit(/*document_id=*/2),
                         CreateScoredDocumentHit(/*document_id=*/3)},
                        /*num_per_page=*/1);
  ResultState result_state2 =
      CreateResultState({CreateScoredDocumentHit(/*document_id=*/4),
                         CreateScoredDocumentHit(/*document_id=*/5),
                         CreateScoredDocumentHit(/*document_id=*/6)},
                        /*num_per_page=*/1);

  ResultStateManager result_state_manager(
      /*max_hits_per_query=*/std::numeric_limits<int>::max(),
      /*max_result_states=*/std::numeric_limits<int>::max());
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultState page_result_state1,
      result_state_manager.RankAndPaginate(std::move(result_state1)));
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultState page_result_state2,
      result_state_manager.RankAndPaginate(std::move(result_state2)));

  result_state_manager.InvalidateResultState(
      page_result_state1.next_page_token);

  // page_result_state1.next_page_token() shouldn't be found
  EXPECT_THAT(
      result_state_manager.GetNextPage(page_result_state1.next_page_token),
      StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  // page_result_state2.next_page_token() should still exist
  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_state2,
      result_state_manager.GetNextPage(page_result_state2.next_page_token));
  EXPECT_THAT(page_result_state2.scored_document_hits,
              ElementsAre(EqualsScoredDocumentHit(
                  CreateScoredDocumentHit(/*document_id=*/5))));
}

TEST(ResultStateManagerTest, ShouldInvalidateAllTokens) {
  ResultState result_state1 =
      CreateResultState({CreateScoredDocumentHit(/*document_id=*/1),
                         CreateScoredDocumentHit(/*document_id=*/2),
                         CreateScoredDocumentHit(/*document_id=*/3)},
                        /*num_per_page=*/1);
  ResultState result_state2 =
      CreateResultState({CreateScoredDocumentHit(/*document_id=*/4),
                         CreateScoredDocumentHit(/*document_id=*/5),
                         CreateScoredDocumentHit(/*document_id=*/6)},
                        /*num_per_page=*/1);

  ResultStateManager result_state_manager(
      /*max_hits_per_query=*/std::numeric_limits<int>::max(),
      /*max_result_states=*/std::numeric_limits<int>::max());
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultState page_result_state1,
      result_state_manager.RankAndPaginate(std::move(result_state1)));
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultState page_result_state2,
      result_state_manager.RankAndPaginate(std::move(result_state2)));

  result_state_manager.InvalidateAllResultStates();

  // page_result_state1.next_page_token() shouldn't be found
  EXPECT_THAT(
      result_state_manager.GetNextPage(page_result_state1.next_page_token),
      StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  // page_result_state2.next_page_token() shouldn't be found
  EXPECT_THAT(
      result_state_manager.GetNextPage(page_result_state2.next_page_token),
      StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST(ResultStateManagerTest, ShouldRemoveOldestResultState) {
  ResultState result_state1 =
      CreateResultState({CreateScoredDocumentHit(/*document_id=*/1),
                         CreateScoredDocumentHit(/*document_id=*/2)},
                        /*num_per_page=*/1);
  ResultState result_state2 =
      CreateResultState({CreateScoredDocumentHit(/*document_id=*/3),
                         CreateScoredDocumentHit(/*document_id=*/4)},
                        /*num_per_page=*/1);
  ResultState result_state3 =
      CreateResultState({CreateScoredDocumentHit(/*document_id=*/5),
                         CreateScoredDocumentHit(/*document_id=*/6)},
                        /*num_per_page=*/1);

  ResultStateManager result_state_manager(
      /*max_hits_per_query=*/std::numeric_limits<int>::max(),
      /*max_result_states=*/2);
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultState page_result_state1,
      result_state_manager.RankAndPaginate(std::move(result_state1)));
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultState page_result_state2,
      result_state_manager.RankAndPaginate(std::move(result_state2)));
  // Adding state 3 should cause state 1 to be removed.
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultState page_result_state3,
      result_state_manager.RankAndPaginate(std::move(result_state3)));

  EXPECT_THAT(
      result_state_manager.GetNextPage(page_result_state1.next_page_token),
      StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_state2,
      result_state_manager.GetNextPage(page_result_state2.next_page_token));
  EXPECT_THAT(page_result_state2.scored_document_hits,
              ElementsAre(EqualsScoredDocumentHit(CreateScoredDocumentHit(
                  /*document_id=*/3))));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_state3,
      result_state_manager.GetNextPage(page_result_state3.next_page_token));
  EXPECT_THAT(page_result_state3.scored_document_hits,
              ElementsAre(EqualsScoredDocumentHit(CreateScoredDocumentHit(
                  /*document_id=*/5))));
}

TEST(ResultStateManagerTest,
     PreviouslyInvalidatedResultStateShouldNotBeCounted) {
  ResultState result_state1 =
      CreateResultState({CreateScoredDocumentHit(/*document_id=*/1),
                         CreateScoredDocumentHit(/*document_id=*/2)},
                        /*num_per_page=*/1);
  ResultState result_state2 =
      CreateResultState({CreateScoredDocumentHit(/*document_id=*/3),
                         CreateScoredDocumentHit(/*document_id=*/4)},
                        /*num_per_page=*/1);
  ResultState result_state3 =
      CreateResultState({CreateScoredDocumentHit(/*document_id=*/5),
                         CreateScoredDocumentHit(/*document_id=*/6)},
                        /*num_per_page=*/1);
  ResultState result_state4 =
      CreateResultState({CreateScoredDocumentHit(/*document_id=*/7),
                         CreateScoredDocumentHit(/*document_id=*/8)},
                        /*num_per_page=*/1);

  ResultStateManager result_state_manager(
      /*max_hits_per_query=*/std::numeric_limits<int>::max(),
      /*max_result_states=*/3);
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultState page_result_state1,
      result_state_manager.RankAndPaginate(std::move(result_state1)));
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultState page_result_state2,
      result_state_manager.RankAndPaginate(std::move(result_state2)));
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultState page_result_state3,
      result_state_manager.RankAndPaginate(std::move(result_state3)));

  // Invalidates state 2, so that the number of valid tokens becomes 2.
  result_state_manager.InvalidateResultState(
      page_result_state2.next_page_token);

  // Adding state 4 shouldn't affect rest of the states
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultState page_result_state4,
      result_state_manager.RankAndPaginate(std::move(result_state4)));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_state1,
      result_state_manager.GetNextPage(page_result_state1.next_page_token));
  EXPECT_THAT(page_result_state1.scored_document_hits,
              ElementsAre(EqualsScoredDocumentHit(CreateScoredDocumentHit(
                  /*document_id=*/1))));

  EXPECT_THAT(
      result_state_manager.GetNextPage(page_result_state2.next_page_token),
      StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_state3,
      result_state_manager.GetNextPage(page_result_state3.next_page_token));
  EXPECT_THAT(page_result_state3.scored_document_hits,
              ElementsAre(EqualsScoredDocumentHit(CreateScoredDocumentHit(
                  /*document_id=*/5))));

  ICING_ASSERT_OK_AND_ASSIGN(
      page_result_state4,
      result_state_manager.GetNextPage(page_result_state4.next_page_token));
  EXPECT_THAT(page_result_state4.scored_document_hits,
              ElementsAre(EqualsScoredDocumentHit(CreateScoredDocumentHit(
                  /*document_id=*/7))));
}

TEST(ResultStateManagerTest, ShouldGetSnippetContext) {
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/1);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(5);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(5);
  result_spec.mutable_snippet_spec()->set_max_window_bytes(5);

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SectionRestrictQueryTermsMap query_terms_map;
  query_terms_map.emplace("term1", std::unordered_set<std::string>());

  ResultState original_result_state = ResultState(
      /*scored_document_hits=*/{CreateScoredDocumentHit(/*document_id=*/1),
                                CreateScoredDocumentHit(/*document_id=*/2)},
      query_terms_map, search_spec, CreateScoringSpec(), result_spec);

  ResultStateManager result_state_manager(
      /*max_hits_per_query=*/std::numeric_limits<int>::max(),
      /*max_result_states=*/std::numeric_limits<int>::max());
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultState page_result_state,
      result_state_manager.RankAndPaginate(std::move(original_result_state)));

  ASSERT_THAT(page_result_state.next_page_token, Gt(kInvalidNextPageToken));

  EXPECT_THAT(page_result_state.snippet_context.match_type,
              Eq(TermMatchType::EXACT_ONLY));
  EXPECT_TRUE(page_result_state.snippet_context.query_terms.find("term1") !=
              page_result_state.snippet_context.query_terms.end());
  EXPECT_THAT(page_result_state.snippet_context.snippet_spec,
              EqualsProto(result_spec.snippet_spec()));
}

TEST(ResultStateManagerTest, ShouldGetDefaultSnippetContext) {
  ResultSpecProto result_spec = CreateResultSpec(/*num_per_page=*/1);
  // 0 indicates no snippeting
  result_spec.mutable_snippet_spec()->set_num_to_snippet(0);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(0);
  result_spec.mutable_snippet_spec()->set_max_window_bytes(0);

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SectionRestrictQueryTermsMap query_terms_map;
  query_terms_map.emplace("term1", std::unordered_set<std::string>());

  ResultState original_result_state = ResultState(
      /*scored_document_hits=*/{CreateScoredDocumentHit(/*document_id=*/1),
                                CreateScoredDocumentHit(/*document_id=*/2)},
      query_terms_map, search_spec, CreateScoringSpec(), result_spec);

  ResultStateManager result_state_manager(
      /*max_hits_per_query=*/std::numeric_limits<int>::max(),
      /*max_result_states=*/std::numeric_limits<int>::max());
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultState page_result_state,
      result_state_manager.RankAndPaginate(std::move(original_result_state)));

  ASSERT_THAT(page_result_state.next_page_token, Gt(kInvalidNextPageToken));

  EXPECT_THAT(page_result_state.snippet_context.query_terms, IsEmpty());
  EXPECT_THAT(
      page_result_state.snippet_context.snippet_spec,
      EqualsProto(ResultSpecProto::SnippetSpecProto::default_instance()));
  EXPECT_THAT(page_result_state.snippet_context.match_type,
              Eq(TermMatchType::UNKNOWN));
}

TEST(ResultStateManagerTest, ShouldGetCorrectNumPreviouslyReturned) {
  ResultState original_result_state =
      CreateResultState({CreateScoredDocumentHit(/*document_id=*/1),
                         CreateScoredDocumentHit(/*document_id=*/2),
                         CreateScoredDocumentHit(/*document_id=*/3),
                         CreateScoredDocumentHit(/*document_id=*/4),
                         CreateScoredDocumentHit(/*document_id=*/5)},
                        /*num_per_page=*/2);

  ResultStateManager result_state_manager(
      /*max_hits_per_query=*/std::numeric_limits<int>::max(),
      /*max_result_states=*/std::numeric_limits<int>::max());

  // First page, 2 results
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultState page_result_state1,
      result_state_manager.RankAndPaginate(std::move(original_result_state)));
  ASSERT_THAT(page_result_state1.scored_document_hits.size(), Eq(2));

  // No previously returned results
  EXPECT_THAT(page_result_state1.num_previously_returned, Eq(0));

  uint64_t next_page_token = page_result_state1.next_page_token;

  // Second page, 2 results
  ICING_ASSERT_OK_AND_ASSIGN(PageResultState page_result_state2,
                             result_state_manager.GetNextPage(next_page_token));
  ASSERT_THAT(page_result_state2.scored_document_hits.size(), Eq(2));

  // num_previously_returned = size of first page
  EXPECT_THAT(page_result_state2.num_previously_returned, Eq(2));

  // Third page, 1 result
  ICING_ASSERT_OK_AND_ASSIGN(PageResultState page_result_state3,
                             result_state_manager.GetNextPage(next_page_token));
  ASSERT_THAT(page_result_state3.scored_document_hits.size(), Eq(1));

  // num_previously_returned = size of first and second pages
  EXPECT_THAT(page_result_state3.num_previously_returned, Eq(4));

  // No more results
  EXPECT_THAT(result_state_manager.GetNextPage(next_page_token),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST(ResultStateManagerTest, ShouldStoreMaxNumberOfScoredDocumentHits) {
  ResultState original_result_state =
      CreateResultState({CreateScoredDocumentHit(/*document_id=*/1),
                         CreateScoredDocumentHit(/*document_id=*/2),
                         CreateScoredDocumentHit(/*document_id=*/3),
                         CreateScoredDocumentHit(/*document_id=*/4),
                         CreateScoredDocumentHit(/*document_id=*/5)},
                        /*num_per_page=*/2);

  ResultStateManager result_state_manager(
      /*max_hits_per_query=*/3,
      /*max_result_states=*/std::numeric_limits<int>::max());

  // The 5 input scored document hits will be truncated to 3.

  // First page, 2 results
  ICING_ASSERT_OK_AND_ASSIGN(
      PageResultState page_result_state1,
      result_state_manager.RankAndPaginate(std::move(original_result_state)));
  EXPECT_THAT(
      page_result_state1.scored_document_hits,
      ElementsAre(
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/5)),
          EqualsScoredDocumentHit(CreateScoredDocumentHit(/*document_id=*/4))));

  uint64_t next_page_token = page_result_state1.next_page_token;

  // Second page, 1 results.
  ICING_ASSERT_OK_AND_ASSIGN(PageResultState page_result_state2,
                             result_state_manager.GetNextPage(next_page_token));
  EXPECT_THAT(page_result_state2.scored_document_hits,
              ElementsAre(EqualsScoredDocumentHit(
                  CreateScoredDocumentHit(/*document_id=*/3))));

  // No third page.
  EXPECT_THAT(result_state_manager.GetNextPage(next_page_token),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

}  // namespace
}  // namespace lib
}  // namespace icing
