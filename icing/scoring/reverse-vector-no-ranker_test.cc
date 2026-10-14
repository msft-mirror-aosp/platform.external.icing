// Copyright (C) 2026 Google LLC
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

#include "icing/scoring/reverse-vector-no-ranker.h"

#include <memory>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/schema/section.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/scoring/scored-document-hits-ranker.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {

namespace {

using ::testing::DoubleEq;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::IsFalse;
using ::testing::IsTrue;
using ::testing::Pointee;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;

ScoredDocumentHit::Converter converter;

std::vector<JoinedScoredDocumentHit> PopAll(ScoredDocumentHitsRanker& ranker) {
  std::vector<JoinedScoredDocumentHit> hits;
  while (!ranker.empty()) {
    hits.push_back(ranker.Top());
    ranker.Pop();
  }
  return hits;
}

TEST(ReverseVectorNoRankerTest, ShouldGetCorrectSizeAndEmpty) {
  ScoredDocumentHit scored_hit_0(/*document_id=*/0, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_1(/*document_id=*/1, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_2(/*document_id=*/2, kSectionIdMaskNone,
                                 /*score=*/1);

  ReverseVectorNoRanker<ScoredDocumentHit> ranker(
      {scored_hit_1, scored_hit_0, scored_hit_2});
  EXPECT_THAT(ranker.size(), Eq(3));
  EXPECT_FALSE(ranker.empty());

  ranker.Pop();
  EXPECT_THAT(ranker.size(), Eq(2));
  EXPECT_FALSE(ranker.empty());

  ranker.Pop();
  EXPECT_THAT(ranker.size(), Eq(1));
  EXPECT_FALSE(ranker.empty());

  ranker.Pop();
  EXPECT_THAT(ranker.size(), Eq(0));
  EXPECT_TRUE(ranker.empty());
}

TEST(ReverseVectorNoRankerTest, ShouldPopHitsInReverseVectorOrder) {
  ScoredDocumentHit scored_hit_0(/*document_id=*/0, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_1(/*document_id=*/1, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_2(/*document_id=*/2, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_3(/*document_id=*/3, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_4(/*document_id=*/4, kSectionIdMaskNone,
                                 /*score=*/1);

  ReverseVectorNoRanker<ScoredDocumentHit> ranker(
      {scored_hit_1, scored_hit_0, scored_hit_2, scored_hit_4, scored_hit_3});

  EXPECT_THAT(ranker, SizeIs(5));
  std::vector<JoinedScoredDocumentHit> scored_document_hits = PopAll(ranker);
  EXPECT_THAT(
      scored_document_hits,
      ElementsAre(EqualsJoinedScoredDocumentHit(converter(scored_hit_3)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_4)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_2)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_0)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_1))));
}

TEST(ReverseVectorNoRankerTest, EmptyScoredDocumentHits) {
  ReverseVectorNoRanker<ScoredDocumentHit> ranker(
      /*scored_document_hits=*/{});
  EXPECT_THAT(ranker, IsEmpty());
}

TEST(ReverseVectorNoRankerTest, ScoredDocumentHitsGetTopKDocumentIds) {
  ScoredDocumentHit scored_hit_0(/*document_id=*/0, kSectionIdMaskNone,
                                 /*score=*/0);
  ScoredDocumentHit scored_hit_1(/*document_id=*/1, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_2(/*document_id=*/2, kSectionIdMaskNone,
                                 /*score=*/4);
  ScoredDocumentHit scored_hit_3(/*document_id=*/3, kSectionIdMaskNone,
                                 /*score=*/3);
  ScoredDocumentHit scored_hit_4(/*document_id=*/4, kSectionIdMaskNone,
                                 /*score=*/2);
  ReverseVectorNoRanker<ScoredDocumentHit> ranker(
      {scored_hit_0, scored_hit_1, scored_hit_2, scored_hit_3, scored_hit_4});

  EXPECT_THAT(ranker.GetTopKDocumentIds(2), UnorderedElementsAre(4, 3));
  EXPECT_THAT(ranker.GetTopKDocumentIds(5),
              UnorderedElementsAre(4, 3, 2, 1, 0));
  // k > size
  EXPECT_THAT(ranker.GetTopKDocumentIds(10),
              UnorderedElementsAre(4, 3, 2, 1, 0));
  // 0 and negative values should return empty.
  EXPECT_THAT(ranker.GetTopKDocumentIds(0), IsEmpty());
  EXPECT_THAT(ranker.GetTopKDocumentIds(-1), IsEmpty());

  // Check that the ranker is not affected by the call.
  EXPECT_THAT(ranker, SizeIs(5));
  std::vector<JoinedScoredDocumentHit> scored_document_hits = PopAll(ranker);
  EXPECT_THAT(
      scored_document_hits,
      ElementsAre(EqualsJoinedScoredDocumentHit(converter(scored_hit_4)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_3)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_2)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_1)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_0))));
}

TEST(ReverseVectorNoRankerTest, JoinedScoredDocumentHitsGetTopKDocumentIds) {
  ScoredDocumentHit scored_hit_0(/*document_id=*/0, kSectionIdMaskNone,
                                 /*score=*/0);
  ScoredDocumentHit scored_hit_1(/*document_id=*/1, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_2(/*document_id=*/2, kSectionIdMaskNone,
                                 /*score=*/2);
  ScoredDocumentHit scored_hit_3(/*document_id=*/3, kSectionIdMaskNone,
                                 /*score=*/3);
  ScoredDocumentHit scored_hit_4(/*document_id=*/4, kSectionIdMaskNone,
                                 /*score=*/4);
  ScoredDocumentHit scored_hit_5(/*document_id=*/5, kSectionIdMaskNone,
                                 /*score=*/5);
  ScoredDocumentHit scored_hit_6(/*document_id=*/6, kSectionIdMaskNone,
                                 /*score=*/6);
  ScoredDocumentHit scored_hit_7(/*document_id=*/7, kSectionIdMaskNone,
                                 /*score=*/7);

  JoinedScoredDocumentHit joined_scored_hit_0(
      /*final_score=*/3, /*parent_scored_document_hit=*/scored_hit_0,
      /*child_scored_document_hits=*/{scored_hit_1, scored_hit_2});
  JoinedScoredDocumentHit joined_scored_hit_1(
      /*final_score=*/4, /*parent_scored_document_hit=*/scored_hit_3,
      /*child_scored_document_hits=*/{scored_hit_4});
  JoinedScoredDocumentHit joined_scored_hit_2(
      /*final_score=*/2, /*parent_scored_document_hit=*/scored_hit_6,
      /*child_scored_document_hits=*/{scored_hit_5});
  JoinedScoredDocumentHit joined_scored_hit_3(
      /*final_score=*/1, /*parent_scored_document_hit=*/scored_hit_7,
      /*child_scored_document_hits=*/{});

  ReverseVectorNoRanker<JoinedScoredDocumentHit> ranker(
      {joined_scored_hit_0, joined_scored_hit_1, joined_scored_hit_2,
       joined_scored_hit_3});

  EXPECT_THAT(ranker.GetTopKDocumentIds(1), UnorderedElementsAre(7));
  EXPECT_THAT(ranker.GetTopKDocumentIds(2), UnorderedElementsAre(7, 6));
  // k > size
  EXPECT_THAT(ranker.GetTopKDocumentIds(5), UnorderedElementsAre(7, 6, 3, 0));
  // 0 and negative values should return empty.
  EXPECT_THAT(ranker.GetTopKDocumentIds(0), IsEmpty());
  EXPECT_THAT(ranker.GetTopKDocumentIds(-2), IsEmpty());

  // Check that the ranker is not affected by the call.
  EXPECT_THAT(ranker, SizeIs(4));
  std::vector<JoinedScoredDocumentHit> scored_document_hits = PopAll(ranker);
  EXPECT_THAT(scored_document_hits,
              ElementsAre(EqualsJoinedScoredDocumentHit(joined_scored_hit_3),
                          EqualsJoinedScoredDocumentHit(joined_scored_hit_2),
                          EqualsJoinedScoredDocumentHit(joined_scored_hit_1),
                          EqualsJoinedScoredDocumentHit(joined_scored_hit_0)));
}

TEST(ReverseVectorNoRankerTest,
     ScoredDocumentHitsGetTopKChildDocumentIds_returnsEmpty) {
  ScoredDocumentHit scored_hit_0(/*document_id=*/0, kSectionIdMaskNone,
                                 /*score=*/0);
  ScoredDocumentHit scored_hit_1(/*document_id=*/1, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_2(/*document_id=*/2, kSectionIdMaskNone,
                                 /*score=*/4);
  ScoredDocumentHit scored_hit_3(/*document_id=*/3, kSectionIdMaskNone,
                                 /*score=*/3);
  ScoredDocumentHit scored_hit_4(/*document_id=*/4, kSectionIdMaskNone,
                                 /*score=*/2);
  ReverseVectorNoRanker<ScoredDocumentHit> ranker(
      {scored_hit_0, scored_hit_1, scored_hit_2, scored_hit_3, scored_hit_4});

  EXPECT_THAT(ranker.GetTopKChildDocumentIds(2), IsEmpty());
  EXPECT_THAT(ranker.GetTopKChildDocumentIds(5), IsEmpty());
  // k > size
  EXPECT_THAT(ranker.GetTopKChildDocumentIds(10), IsEmpty());
  // 0 and negative values should return empty.
  EXPECT_THAT(ranker.GetTopKDocumentIds(0), IsEmpty());
  EXPECT_THAT(ranker.GetTopKDocumentIds(-1), IsEmpty());

  // Check that the ranker is not affected by the call.
  EXPECT_THAT(ranker, SizeIs(5));
  std::vector<JoinedScoredDocumentHit> scored_document_hits = PopAll(ranker);
  EXPECT_THAT(
      scored_document_hits,
      ElementsAre(EqualsJoinedScoredDocumentHit(converter(scored_hit_4)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_3)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_2)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_1)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_0))));
}

TEST(ReverseVectorNoRankerTest,
     JoinedScoredDocumentHitsGetTopKChildDocumentIds) {
  ScoredDocumentHit scored_hit_0(/*document_id=*/0, kSectionIdMaskNone,
                                 /*score=*/0);
  ScoredDocumentHit scored_hit_1(/*document_id=*/1, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_2(/*document_id=*/2, kSectionIdMaskNone,
                                 /*score=*/2);
  ScoredDocumentHit scored_hit_3(/*document_id=*/3, kSectionIdMaskNone,
                                 /*score=*/3);
  ScoredDocumentHit scored_hit_4(/*document_id=*/4, kSectionIdMaskNone,
                                 /*score=*/4);
  ScoredDocumentHit scored_hit_5(/*document_id=*/5, kSectionIdMaskNone,
                                 /*score=*/5);
  ScoredDocumentHit scored_hit_6(/*document_id=*/6, kSectionIdMaskNone,
                                 /*score=*/6);
  ScoredDocumentHit scored_hit_7(/*document_id=*/7, kSectionIdMaskNone,
                                 /*score=*/7);

  JoinedScoredDocumentHit joined_scored_hit_0(
      /*final_score=*/3, /*parent_scored_document_hit=*/scored_hit_0,
      /*child_scored_document_hits=*/{scored_hit_1, scored_hit_2});
  JoinedScoredDocumentHit joined_scored_hit_1(
      /*final_score=*/4, /*parent_scored_document_hit=*/scored_hit_3,
      /*child_scored_document_hits=*/{scored_hit_4});
  JoinedScoredDocumentHit joined_scored_hit_2(
      /*final_score=*/2, /*parent_scored_document_hit=*/scored_hit_6,
      /*child_scored_document_hits=*/{scored_hit_5});
  JoinedScoredDocumentHit joined_scored_hit_3(
      /*final_score=*/1, /*parent_scored_document_hit=*/scored_hit_7,
      /*child_scored_document_hits=*/{});

  ReverseVectorNoRanker<JoinedScoredDocumentHit> ranker(
      {joined_scored_hit_0, joined_scored_hit_1, joined_scored_hit_2,
       joined_scored_hit_3});

  EXPECT_THAT(ranker.GetTopKChildDocumentIds(1), UnorderedElementsAre(5, 4, 1));
  EXPECT_THAT(ranker.GetTopKChildDocumentIds(2),
              UnorderedElementsAre(5, 4, 1, 2));
  // k > size
  EXPECT_THAT(ranker.GetTopKChildDocumentIds(5),
              UnorderedElementsAre(5, 4, 1, 2));
  // 0 and negative values should return empty.
  EXPECT_THAT(ranker.GetTopKChildDocumentIds(0), IsEmpty());
  EXPECT_THAT(ranker.GetTopKChildDocumentIds(-2), IsEmpty());

  // Check that the ranker is not affected by the call.
  EXPECT_THAT(ranker, SizeIs(4));
  std::vector<JoinedScoredDocumentHit> scored_document_hits = PopAll(ranker);
  EXPECT_THAT(scored_document_hits,
              ElementsAre(EqualsJoinedScoredDocumentHit(joined_scored_hit_3),
                          EqualsJoinedScoredDocumentHit(joined_scored_hit_2),
                          EqualsJoinedScoredDocumentHit(joined_scored_hit_1),
                          EqualsJoinedScoredDocumentHit(joined_scored_hit_0)));
}

TEST(ReverseVectorNoRankerTest, ShouldTruncateToNewSize) {
  ScoredDocumentHit scored_hit_0(/*document_id=*/0, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_1(/*document_id=*/1, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_2(/*document_id=*/2, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_3(/*document_id=*/3, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_4(/*document_id=*/4, kSectionIdMaskNone,
                                 /*score=*/1);

  ReverseVectorNoRanker<ScoredDocumentHit> ranker(
      {scored_hit_1, scored_hit_0, scored_hit_2, scored_hit_4, scored_hit_3});
  ASSERT_THAT(ranker, SizeIs(5));

  ranker.TruncateHitsTo(/*new_size=*/3);
  EXPECT_THAT(ranker, SizeIs(3));
  std::vector<JoinedScoredDocumentHit> scored_document_hits = PopAll(ranker);
  EXPECT_THAT(
      scored_document_hits,
      ElementsAre(EqualsJoinedScoredDocumentHit(converter(scored_hit_3)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_4)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_2))));
}

TEST(ReverseVectorNoRankerTest, ShouldTruncateToZero) {
  ScoredDocumentHit scored_hit_0(/*document_id=*/0, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_1(/*document_id=*/1, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_2(/*document_id=*/2, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_3(/*document_id=*/3, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_4(/*document_id=*/4, kSectionIdMaskNone,
                                 /*score=*/1);

  ReverseVectorNoRanker<ScoredDocumentHit> ranker(
      {scored_hit_1, scored_hit_0, scored_hit_2, scored_hit_4, scored_hit_3});
  ASSERT_THAT(ranker, SizeIs(5));

  ranker.TruncateHitsTo(/*new_size=*/0);
  EXPECT_THAT(ranker, IsEmpty());
}

TEST(ReverseVectorNoRankerTest, ShouldNotTruncateToNegative) {
  ScoredDocumentHit scored_hit_0(/*document_id=*/0, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_1(/*document_id=*/1, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_2(/*document_id=*/2, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_3(/*document_id=*/3, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_4(/*document_id=*/4, kSectionIdMaskNone,
                                 /*score=*/1);

  ReverseVectorNoRanker<ScoredDocumentHit> ranker(
      {scored_hit_1, scored_hit_0, scored_hit_2, scored_hit_4, scored_hit_3});
  ASSERT_THAT(ranker, SizeIs(Eq(5)));

  ranker.TruncateHitsTo(/*new_size=*/-1);
  EXPECT_THAT(ranker, SizeIs(Eq(5)));
  // Contents are not affected.
  std::vector<JoinedScoredDocumentHit> scored_document_hits = PopAll(ranker);
  EXPECT_THAT(
      scored_document_hits,
      ElementsAre(EqualsJoinedScoredDocumentHit(converter(scored_hit_3)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_4)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_2)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_0)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_1))));
}

TEST(ReverseVectorNoRankerTest, OptimizeAndTransfer) {
  ScoredDocumentHit scored_hit_0(/*document_id=*/0, kSectionIdMaskNone,
                                 /*score=*/100);
  ScoredDocumentHit scored_hit_1(/*document_id=*/1, kSectionIdMaskNone,
                                 /*score=*/101);
  ScoredDocumentHit scored_hit_2(/*document_id=*/2, kSectionIdMaskNone,
                                 /*score=*/102);
  ScoredDocumentHit scored_hit_3(/*document_id=*/3, kSectionIdMaskNone,
                                 /*score=*/103);
  ScoredDocumentHit scored_hit_4(/*document_id=*/4, kSectionIdMaskNone,
                                 /*score=*/104);
  ScoredDocumentHit scored_hit_5(/*document_id=*/5, kSectionIdMaskNone,
                                 /*score=*/105);
  ScoredDocumentHit scored_hit_6(/*document_id=*/6, kSectionIdMaskNone,
                                 /*score=*/106);
  ScoredDocumentHit scored_hit_7(/*document_id=*/7, kSectionIdMaskNone,
                                 /*score=*/107);

  ReverseVectorNoRanker<ScoredDocumentHit> ranker(
      {scored_hit_6, scored_hit_1, scored_hit_0, scored_hit_7, scored_hit_2,
       scored_hit_5, scored_hit_4, scored_hit_3});

  // Create a mapping from old document ids to new document ids.
  // 0 -> kInvalidDocumentId
  // 1 -> 0
  // 2 -> kInvalidDocumentId
  // 3 -> kInvalidDocumentId
  // 4 -> 1
  // 5 -> 2
  // 6 -> 3
  // 7 -> kInvalidDocumentId
  std::vector<DocumentId> document_id_old_to_new = {
      kInvalidDocumentId, 0, kInvalidDocumentId, kInvalidDocumentId, 1, 2, 3,
      kInvalidDocumentId};

  // Optimize and transfer the ranker to a new ScoredDocumentHitsRanker.
  // - Invalid document ids are removed.
  // - Document ids are remapped according to the mapping.
  // - The order of the hits is preserved from the original ranker.
  std::unique_ptr<ScoredDocumentHitsRanker> optimized_ranker =
      std::move(ranker).OptimizeAndTransfer(document_id_old_to_new);
  EXPECT_THAT(optimized_ranker, Pointee(SizeIs(4)));
  std::vector<JoinedScoredDocumentHit> scored_document_hits =
      PopAll(*optimized_ranker);
  EXPECT_THAT(
      scored_document_hits,
      ElementsAre(EqualsJoinedScoredDocumentHit(converter(ScoredDocumentHit(
                      /*document_id=*/1, kSectionIdMaskNone,
                      /*score=*/104))),  // 4 -> 1
                  EqualsJoinedScoredDocumentHit(converter(ScoredDocumentHit(
                      /*document_id=*/2, kSectionIdMaskNone,
                      /*score=*/105))),  // 5 -> 2
                  EqualsJoinedScoredDocumentHit(converter(ScoredDocumentHit(
                      /*document_id=*/0, kSectionIdMaskNone,
                      /*score=*/101))),  // 1 -> 0
                  EqualsJoinedScoredDocumentHit(converter(ScoredDocumentHit(
                      /*document_id=*/3, kSectionIdMaskNone,
                      /*score=*/106)))  // 6 -> 3
                  ));
}

TEST(ReverseVectorNoRankerTest, OptimizeAndTransfer_allHitsDeleted) {
  ScoredDocumentHit scored_hit_0(/*document_id=*/0, kSectionIdMaskNone,
                                 /*score=*/100);
  ScoredDocumentHit scored_hit_1(/*document_id=*/1, kSectionIdMaskNone,
                                 /*score=*/101);
  ScoredDocumentHit scored_hit_2(/*document_id=*/2, kSectionIdMaskNone,
                                 /*score=*/102);
  ScoredDocumentHit scored_hit_3(/*document_id=*/3, kSectionIdMaskNone,
                                 /*score=*/103);
  ScoredDocumentHit scored_hit_4(/*document_id=*/4, kSectionIdMaskNone,
                                 /*score=*/104);
  ScoredDocumentHit scored_hit_5(/*document_id=*/5, kSectionIdMaskNone,
                                 /*score=*/105);

  ReverseVectorNoRanker<ScoredDocumentHit> ranker({scored_hit_1, scored_hit_0,
                                                   scored_hit_2, scored_hit_5,
                                                   scored_hit_4, scored_hit_3});

  // Create a mapping from old document ids to new document ids.
  // 0 -> kInvalidDocumentId
  // 1 -> kInvalidDocumentId
  // 2 -> kInvalidDocumentId
  // 3 -> kInvalidDocumentId
  // 4 -> kInvalidDocumentId
  // 5 -> kInvalidDocumentId
  std::vector<DocumentId> document_id_old_to_new = {
      kInvalidDocumentId, kInvalidDocumentId, kInvalidDocumentId,
      kInvalidDocumentId, kInvalidDocumentId, kInvalidDocumentId};

  // Optimize and transfer the ranker to a new ScoredDocumentHitsRanker. All
  // hits are deleted.
  std::unique_ptr<ScoredDocumentHitsRanker> optimized_ranker =
      std::move(ranker).OptimizeAndTransfer(document_id_old_to_new);
  EXPECT_THAT(optimized_ranker, Pointee(IsEmpty()));
  std::vector<JoinedScoredDocumentHit> scored_document_hits =
      PopAll(*optimized_ranker);
  EXPECT_THAT(scored_document_hits, IsEmpty());
}

TEST(ReverseVectorNoRankerTest, OptimizeAndTransfer_outOfBoundDocId) {
  ScoredDocumentHit scored_hit_0(/*document_id=*/0, kSectionIdMaskNone,
                                 /*score=*/100);
  ScoredDocumentHit scored_hit_1(/*document_id=*/1, kSectionIdMaskNone,
                                 /*score=*/101);

  ReverseVectorNoRanker<ScoredDocumentHit> ranker({scored_hit_1, scored_hit_0});

  // Create a mapping from old document ids to new document ids.
  // 0 -> kInvalidDocumentId
  std::vector<DocumentId> document_id_old_to_new = {0};

  // Optimize and transfer the ranker to a new ScoredDocumentHitsRanker. If
  // there is any document id out of bound from the mapping, then the hit is
  // deleted without crashing.
  std::unique_ptr<ScoredDocumentHitsRanker> optimized_ranker =
      std::move(ranker).OptimizeAndTransfer(document_id_old_to_new);
  EXPECT_THAT(optimized_ranker, Pointee(SizeIs(1)));
  std::vector<JoinedScoredDocumentHit> scored_document_hits =
      PopAll(*optimized_ranker);
  EXPECT_THAT(
      scored_document_hits,
      ElementsAre(EqualsJoinedScoredDocumentHit(converter(ScoredDocumentHit(
          /*document_id=*/0, kSectionIdMaskNone,
          /*score=*/100)))  // 0 -> 0
                  ));
}

TEST(ReverseVectorNoRankerTest, OptimizeAndTransfer_joinedScoredDocumentHit) {
  ScoredDocumentHit parent_scored_document_hit1(/*document_id=*/4,
                                                /*section_id_mask=*/49,
                                                /*score=*/104);
  std::vector<ScoredDocumentHit> child_scored_document_hits1 = {
      ScoredDocumentHit(/*document_id=*/5,
                        /*section_id_mask=*/1,
                        /*score=*/105),
      ScoredDocumentHit(/*document_id=*/2,
                        /*section_id_mask=*/2,
                        /*score=*/102)};

  ScoredDocumentHit parent_scored_document_hit2(/*document_id=*/3,
                                                /*section_id_mask=*/24,
                                                /*score=*/103);
  std::vector<ScoredDocumentHit> child_scored_document_hits2 = {
      ScoredDocumentHit(/*document_id=*/6,
                        /*section_id_mask=*/1,
                        /*score=*/106),
      ScoredDocumentHit(/*document_id=*/0,
                        /*section_id_mask=*/2,
                        /*score=*/100),
      ScoredDocumentHit(/*document_id=*/7,
                        /*section_id_mask=*/3,
                        /*score=*/107),
      ScoredDocumentHit(/*document_id=*/8,
                        /*section_id_mask=*/4,
                        /*score=*/108)};

  ScoredDocumentHit parent_scored_document_hit3(/*document_id=*/1,
                                                /*section_id_mask=*/145,
                                                /*score=*/101);
  std::vector<ScoredDocumentHit> child_scored_document_hits3 = {
      ScoredDocumentHit(/*document_id=*/9,
                        /*section_id_mask=*/1,
                        /*score=*/109),
      ScoredDocumentHit(/*document_id=*/10,
                        /*section_id_mask=*/2,
                        /*score=*/110)};

  JoinedScoredDocumentHit joined_hit1(
      /*final_score=*/123.45, std::move(parent_scored_document_hit1),
      std::move(child_scored_document_hits1));
  JoinedScoredDocumentHit joined_hit2(
      /*final_score=*/67.89, std::move(parent_scored_document_hit2),
      std::move(child_scored_document_hits2));
  JoinedScoredDocumentHit joined_hit3(
      /*final_score=*/39.21, std::move(parent_scored_document_hit3),
      std::move(child_scored_document_hits3));

  ReverseVectorNoRanker<JoinedScoredDocumentHit> ranker(
      {joined_hit1, joined_hit2, joined_hit3});

  // Create a mapping from old document ids to new document ids.
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
  std::vector<DocumentId> document_id_old_to_new = {
      kInvalidDocumentId, 0, kInvalidDocumentId, kInvalidDocumentId, 1, 2, 3,
      kInvalidDocumentId, 4, kInvalidDocumentId, kInvalidDocumentId};

  // Optimize and transfer the ranker to a new ScoredDocumentHitsRanker.
  // - Invalid document ids are removed. If a parent document is removed, the
  //   entire joined document is removed.
  // - Document ids are remapped according to the mapping.
  std::unique_ptr<ScoredDocumentHitsRanker> optimized_ranker =
      std::move(ranker).OptimizeAndTransfer(document_id_old_to_new);
  EXPECT_THAT(optimized_ranker, Pointee(SizeIs(2)));

  std::vector<JoinedScoredDocumentHit> joined_scored_document_hits =
      PopAll(*optimized_ranker);
  ASSERT_THAT(joined_scored_document_hits, SizeIs(2));

  EXPECT_THAT(joined_scored_document_hits[0].final_score(), DoubleEq(39.21));
  EXPECT_THAT(joined_scored_document_hits[0].parent_scored_document_hit(),
              EqualsScoredDocumentHit(
                  ScoredDocumentHit(/*document_id=*/0, /*section_id_mask=*/145,
                                    /*score=*/101)));  // 1 -> 0
  EXPECT_THAT(joined_scored_document_hits[0].child_scored_document_hits(),
              IsEmpty());  // All child documents are deleted.

  EXPECT_THAT(joined_scored_document_hits[1].final_score(), DoubleEq(123.45));
  EXPECT_THAT(joined_scored_document_hits[1].parent_scored_document_hit(),
              EqualsScoredDocumentHit(
                  ScoredDocumentHit(/*document_id=*/1, /*section_id_mask=*/49,
                                    /*score=*/104)));  // 4 -> 1
  EXPECT_THAT(joined_scored_document_hits[1].child_scored_document_hits(),
              ElementsAre(EqualsScoredDocumentHit(ScoredDocumentHit(
                  /*document_id=*/2, /*section_id_mask=*/1,
                  /*score=*/105))  // 5 -> 2
                          ));  // original child doc 2 is deleted, so there is
                               // only one child document hit left.

  // Since the original document 3 is deleted, joined_hit2 is deleted after
  // optimization even though there are some child documents that are not
  // deleted.
}

TEST(ReverseVectorNoRankerTest, Clear) {
  ScoredDocumentHit scored_hit_0(/*document_id=*/0, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_1(/*document_id=*/1, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_2(/*document_id=*/2, kSectionIdMaskNone,
                                 /*score=*/1);

  ReverseVectorNoRanker<ScoredDocumentHit> ranker(
      {scored_hit_1, scored_hit_0, scored_hit_2});
  ASSERT_THAT(ranker.size(), Eq(3));
  ASSERT_THAT(ranker.empty(), IsFalse());

  ranker.clear();
  EXPECT_THAT(ranker.size(), Eq(0));
  EXPECT_THAT(ranker.empty(), IsTrue());
}

}  // namespace

}  // namespace lib
}  // namespace icing
