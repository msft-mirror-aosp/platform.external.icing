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

#include "icing/scoring/priority-queue-scored-document-hits-ranker.h"

#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/schema/section.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;

class Converter {
 public:
  JoinedScoredDocumentHit operator()(ScoredDocumentHit hit) const {
    return converter_(std::move(hit));
  }

 private:
  ScoredDocumentHit::Converter converter_;
} converter;

std::vector<JoinedScoredDocumentHit> PopAll(
    PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit>& ranker) {
  std::vector<JoinedScoredDocumentHit> hits;
  while (!ranker.empty()) {
    hits.push_back(ranker.PopNext());
  }
  return hits;
}

std::vector<JoinedScoredDocumentHit> PopAll(
    PriorityQueueScoredDocumentHitsRanker<JoinedScoredDocumentHit>& ranker) {
  std::vector<JoinedScoredDocumentHit> hits;
  while (!ranker.empty()) {
    hits.push_back(ranker.PopNext());
  }
  return hits;
}

TEST(PriorityQueueScoredDocumentHitsRankerTest, ShouldGetCorrectSizeAndEmpty) {
  ScoredDocumentHit scored_hit_0(/*document_id=*/0, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_1(/*document_id=*/1, kSectionIdMaskNone,
                                 /*score=*/1);
  ScoredDocumentHit scored_hit_2(/*document_id=*/2, kSectionIdMaskNone,
                                 /*score=*/1);

  PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit> ranker(
      {scored_hit_1, scored_hit_0, scored_hit_2},
      /*is_descending=*/true);
  EXPECT_THAT(ranker.size(), Eq(3));
  EXPECT_FALSE(ranker.empty());

  ranker.PopNext();
  EXPECT_THAT(ranker.size(), Eq(2));
  EXPECT_FALSE(ranker.empty());

  ranker.PopNext();
  EXPECT_THAT(ranker.size(), Eq(1));
  EXPECT_FALSE(ranker.empty());

  ranker.PopNext();
  EXPECT_THAT(ranker.size(), Eq(0));
  EXPECT_TRUE(ranker.empty());
}

TEST(PriorityQueueScoredDocumentHitsRankerTest, ShouldRankInDescendingOrder) {
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

  PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit> ranker(
      {scored_hit_1, scored_hit_0, scored_hit_2, scored_hit_4, scored_hit_3},
      /*is_descending=*/true);

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

TEST(PriorityQueueScoredDocumentHitsRankerTest, ShouldRankInAscendingOrder) {
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

  PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit> ranker(
      {scored_hit_1, scored_hit_0, scored_hit_2, scored_hit_4, scored_hit_3},
      /*is_descending=*/false);

  EXPECT_THAT(ranker, SizeIs(5));
  std::vector<JoinedScoredDocumentHit> scored_document_hits = PopAll(ranker);
  EXPECT_THAT(
      scored_document_hits,
      ElementsAre(EqualsJoinedScoredDocumentHit(converter(scored_hit_0)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_1)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_2)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_3)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_4))));
}

TEST(PriorityQueueScoredDocumentHitsRankerTest,
     ShouldRankDuplicateScoredDocumentHits) {
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

  PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit> ranker(
      {scored_hit_2, scored_hit_4, scored_hit_1, scored_hit_0, scored_hit_2,
       scored_hit_2, scored_hit_4, scored_hit_3},
      /*is_descending=*/true);

  EXPECT_THAT(ranker, SizeIs(8));
  std::vector<JoinedScoredDocumentHit> scored_document_hits = PopAll(ranker);
  EXPECT_THAT(
      scored_document_hits,
      ElementsAre(EqualsJoinedScoredDocumentHit(converter(scored_hit_4)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_4)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_3)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_2)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_2)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_2)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_1)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_0))));
}

TEST(PriorityQueueScoredDocumentHitsRankerTest,
     ShouldRankEmptyScoredDocumentHits) {
  PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit> ranker(
      /*scored_document_hits=*/{},
      /*is_descending=*/true);
  EXPECT_THAT(ranker, IsEmpty());
}

TEST(PriorityQueueScoredDocumentHitsRankerTest,
     ScoredDocumentHitsGetTopKDocumentIds) {
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
  PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit> ranker(
      {scored_hit_0, scored_hit_1, scored_hit_2, scored_hit_3, scored_hit_4},
      /*is_descending=*/true);

  EXPECT_THAT(ranker.GetTopKDocumentIds(2), UnorderedElementsAre(2, 3));
  EXPECT_THAT(ranker.GetTopKDocumentIds(5),
              UnorderedElementsAre(4, 2, 3, 1, 0));
  // k > size
  EXPECT_THAT(ranker.GetTopKDocumentIds(10),
              UnorderedElementsAre(4, 2, 3, 1, 0));
  // 0 and negative values should return empty.
  EXPECT_THAT(ranker.GetTopKDocumentIds(0), IsEmpty());
  EXPECT_THAT(ranker.GetTopKDocumentIds(-1), IsEmpty());

  // Check that the ranker is not affected by the call.
  EXPECT_THAT(ranker, SizeIs(5));
  std::vector<JoinedScoredDocumentHit> scored_document_hits = PopAll(ranker);
  EXPECT_THAT(
      scored_document_hits,
      ElementsAre(EqualsJoinedScoredDocumentHit(converter(scored_hit_2)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_3)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_4)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_1)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_0))));
}

TEST(PriorityQueueScoredDocumentHitsRankerTest,
     JoinedScoredDocumentHitsGetTopKDocumentIds) {
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

  PriorityQueueScoredDocumentHitsRanker<JoinedScoredDocumentHit> ranker(
      {joined_scored_hit_0, joined_scored_hit_1, joined_scored_hit_2,
       joined_scored_hit_3},
      /*is_descending=*/true);

  EXPECT_THAT(ranker.GetTopKDocumentIds(1), UnorderedElementsAre(3));
  EXPECT_THAT(ranker.GetTopKDocumentIds(2), UnorderedElementsAre(3, 0));
  // k > size
  EXPECT_THAT(ranker.GetTopKDocumentIds(5), UnorderedElementsAre(3, 6, 0, 7));
  // 0 and negative values should return empty.
  EXPECT_THAT(ranker.GetTopKDocumentIds(0), IsEmpty());
  EXPECT_THAT(ranker.GetTopKDocumentIds(-2), IsEmpty());

  // Check that the ranker is not affected by the call.
  EXPECT_THAT(ranker, SizeIs(4));
  std::vector<JoinedScoredDocumentHit> scored_document_hits = PopAll(ranker);
  EXPECT_THAT(scored_document_hits,
              ElementsAre(EqualsJoinedScoredDocumentHit(joined_scored_hit_1),
                          EqualsJoinedScoredDocumentHit(joined_scored_hit_0),
                          EqualsJoinedScoredDocumentHit(joined_scored_hit_2),
                          EqualsJoinedScoredDocumentHit(joined_scored_hit_3)));
}

TEST(PriorityQueueScoredDocumentHitsRankerTest,
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
  PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit> ranker(
      {scored_hit_0, scored_hit_1, scored_hit_2, scored_hit_3, scored_hit_4},
      /*is_descending=*/true);

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
      ElementsAre(EqualsJoinedScoredDocumentHit(converter(scored_hit_2)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_3)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_4)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_1)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_0))));
}

TEST(PriorityQueueScoredDocumentHitsRankerTest,
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

  PriorityQueueScoredDocumentHitsRanker<JoinedScoredDocumentHit> ranker(
      {joined_scored_hit_0, joined_scored_hit_1, joined_scored_hit_2,
       joined_scored_hit_3},
      /*is_descending=*/true);

  EXPECT_THAT(ranker.GetTopKChildDocumentIds(1), UnorderedElementsAre(1, 4, 5));
  EXPECT_THAT(ranker.GetTopKChildDocumentIds(2),
              UnorderedElementsAre(1, 2, 4, 5));
  // k > size
  EXPECT_THAT(ranker.GetTopKChildDocumentIds(5),
              UnorderedElementsAre(1, 2, 4, 5));
  // 0 and negative values should return empty.
  EXPECT_THAT(ranker.GetTopKChildDocumentIds(0), IsEmpty());
  EXPECT_THAT(ranker.GetTopKChildDocumentIds(-2), IsEmpty());

  // Check that the ranker is not affected by the call.
  EXPECT_THAT(ranker, SizeIs(4));
  std::vector<JoinedScoredDocumentHit> scored_document_hits = PopAll(ranker);
  EXPECT_THAT(scored_document_hits,
              ElementsAre(EqualsJoinedScoredDocumentHit(joined_scored_hit_1),
                          EqualsJoinedScoredDocumentHit(joined_scored_hit_0),
                          EqualsJoinedScoredDocumentHit(joined_scored_hit_2),
                          EqualsJoinedScoredDocumentHit(joined_scored_hit_3)));
}

TEST(PriorityQueueScoredDocumentHitsRankerTest, ShouldTruncateToNewSize) {
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

  PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit> ranker(
      {scored_hit_1, scored_hit_0, scored_hit_2, scored_hit_4, scored_hit_3},
      /*is_descending=*/true);
  ASSERT_THAT(ranker, SizeIs(5));

  ranker.TruncateHitsTo(/*new_size=*/3);
  EXPECT_THAT(ranker, SizeIs(3));
  std::vector<JoinedScoredDocumentHit> scored_document_hits = PopAll(ranker);
  EXPECT_THAT(
      scored_document_hits,
      ElementsAre(EqualsJoinedScoredDocumentHit(converter(scored_hit_4)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_3)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_2))));
}

TEST(PriorityQueueScoredDocumentHitsRankerTest, ShouldTruncateToZero) {
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

  PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit> ranker(
      {scored_hit_1, scored_hit_0, scored_hit_2, scored_hit_4, scored_hit_3},
      /*is_descending=*/true);
  ASSERT_THAT(ranker, SizeIs(5));

  ranker.TruncateHitsTo(/*new_size=*/0);
  EXPECT_THAT(ranker, IsEmpty());
}

TEST(PriorityQueueScoredDocumentHitsRankerTest, ShouldNotTruncateToNegative) {
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

  PriorityQueueScoredDocumentHitsRanker<ScoredDocumentHit> ranker(
      {scored_hit_1, scored_hit_0, scored_hit_2, scored_hit_4, scored_hit_3},
      /*is_descending=*/true);
  ASSERT_THAT(ranker, SizeIs(Eq(5)));

  ranker.TruncateHitsTo(/*new_size=*/-1);
  EXPECT_THAT(ranker, SizeIs(Eq(5)));
  // Contents are not affected.
  std::vector<JoinedScoredDocumentHit> scored_document_hits = PopAll(ranker);
  EXPECT_THAT(
      scored_document_hits,
      ElementsAre(EqualsJoinedScoredDocumentHit(converter(scored_hit_4)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_3)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_2)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_1)),
                  EqualsJoinedScoredDocumentHit(converter(scored_hit_0))));
}

}  // namespace

}  // namespace lib
}  // namespace icing
