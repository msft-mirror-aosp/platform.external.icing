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

#include "icing/scoring/scored-document-hit.h"

#include <optional>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {

namespace {

using ::testing::DoubleEq;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::IsTrue;
using ::testing::Optional;

TEST(ScoredDocumentHitTest, Converter) {
  ScoredDocumentHit::Converter converter;

  double score = 2.0;
  ScoredDocumentHit scored_document_hit(/*document_id=*/5,
                                        /*section_id_mask=*/49, score);

  JoinedScoredDocumentHit joined_scored_document_hit =
      converter(ScoredDocumentHit(scored_document_hit));
  EXPECT_THAT(joined_scored_document_hit.final_score(), DoubleEq(score));
  EXPECT_THAT(joined_scored_document_hit.parent_scored_document_hit(),
              EqualsScoredDocumentHit(scored_document_hit));
  EXPECT_THAT(joined_scored_document_hit.child_scored_document_hits(),
              IsEmpty());
}

TEST(ScoredDocumentHitTest, Optimize) {
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

  // Document id 1 is mapped to 0. Other fields are unchanged.
  ScoredDocumentHit hit1(/*document_id=*/1, /*section_id_mask=*/1023,
                         /*score=*/4.0);
  EXPECT_THAT(
      std::move(hit1).Optimize(document_id_old_to_new),
      Optional(EqualsScoredDocumentHit(ScoredDocumentHit(
          /*document_id=*/0, /*section_id_mask=*/1023, /*score=*/4.0))));

  // Document id 4 is mapped to 1. Other fields are unchanged.
  ScoredDocumentHit hit2(/*document_id=*/4, /*section_id_mask=*/12,
                         /*score=*/3.0);
  EXPECT_THAT(std::move(hit2).Optimize(document_id_old_to_new),
              Optional(EqualsScoredDocumentHit(ScoredDocumentHit(
                  /*document_id=*/1, /*section_id_mask=*/12, /*score=*/3.0))));

  // Document id 5 is mapped to 2. Other fields are unchanged.
  ScoredDocumentHit hit3(/*document_id=*/5, /*section_id_mask=*/49,
                         /*score=*/2.0);
  EXPECT_THAT(std::move(hit3).Optimize(document_id_old_to_new),
              Optional(EqualsScoredDocumentHit(ScoredDocumentHit(
                  /*document_id=*/2, /*section_id_mask=*/49, /*score=*/2.0))));

  // Document id 6 is mapped to 3. Other fields are unchanged.
  ScoredDocumentHit hit4(/*document_id=*/6, /*section_id_mask=*/30,
                         /*score=*/0.0);
  EXPECT_THAT(std::move(hit4).Optimize(document_id_old_to_new),
              Optional(EqualsScoredDocumentHit(ScoredDocumentHit(
                  /*document_id=*/3, /*section_id_mask=*/30, /*score=*/0.0))));
}

TEST(ScoredDocumentHitTest, Optimize_invalidDocIdReturnsNullopt) {
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

  // Document id 0 is deleted.
  ScoredDocumentHit hit1(/*document_id=*/0, /*section_id_mask=*/1023,
                         /*score=*/4.0);
  EXPECT_THAT(std::move(hit1).Optimize(document_id_old_to_new),
              Eq(std::nullopt));

  // Document id 2 is deleted.
  ScoredDocumentHit hit2(/*document_id=*/2, /*section_id_mask=*/12,
                         /*score=*/3.0);
  EXPECT_THAT(std::move(hit2).Optimize(document_id_old_to_new),
              Eq(std::nullopt));

  // Document id 3 is deleted.
  ScoredDocumentHit hit3(/*document_id=*/3, /*section_id_mask=*/49,
                         /*score=*/2.0);
  EXPECT_THAT(std::move(hit3).Optimize(document_id_old_to_new),
              Eq(std::nullopt));

  // Document id 7 is deleted.
  ScoredDocumentHit hit4(/*document_id=*/7, /*section_id_mask=*/30,
                         /*score=*/0.0);
  EXPECT_THAT(std::move(hit4).Optimize(document_id_old_to_new),
              Eq(std::nullopt));
}

TEST(ScoredDocumentHitTest, Optimize_outOfBoundDocIdReturnsNullopt) {
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

  // Document id -2.
  ScoredDocumentHit hit1(/*document_id=*/-2, /*section_id_mask=*/1023,
                         /*score=*/4.0);
  EXPECT_THAT(std::move(hit1).Optimize(document_id_old_to_new),
              Eq(std::nullopt));

  // Document id -1.
  ScoredDocumentHit hit2(/*document_id=*/-1, /*section_id_mask=*/12,
                         /*score=*/3.0);
  EXPECT_THAT(std::move(hit2).Optimize(document_id_old_to_new),
              Eq(std::nullopt));

  // Document id 8.
  ScoredDocumentHit hit3(/*document_id=*/8, /*section_id_mask=*/49,
                         /*score=*/2.0);
  EXPECT_THAT(std::move(hit3).Optimize(document_id_old_to_new),
              Eq(std::nullopt));

  // Document id 9.
  ScoredDocumentHit hit4(/*document_id=*/9, /*section_id_mask=*/30,
                         /*score=*/0.0);
  EXPECT_THAT(std::move(hit4).Optimize(document_id_old_to_new),
              Eq(std::nullopt));

  // Document id 1000.
  ScoredDocumentHit hit5(/*document_id=*/1000, /*section_id_mask=*/60,
                         /*score=*/10.0);
  EXPECT_THAT(std::move(hit5).Optimize(document_id_old_to_new),
              Eq(std::nullopt));

  // kInvalidDocumentId.
  ScoredDocumentHit hit6(kInvalidDocumentId, /*section_id_mask=*/60,
                         /*score=*/10.0);
  EXPECT_THAT(std::move(hit6).Optimize(document_id_old_to_new),
              Eq(std::nullopt));
}

TEST(ScoredDocumentHitTest, Copyable) {
  ScoredDocumentHit hit1(/*document_id=*/5,
                         /*section_id_mask=*/49,
                         /*score=*/1.0, /*additional_scores=*/{0, 1, 2});

  ScoredDocumentHit hit2(/*document_id=*/6,
                         /*section_id_mask=*/50,
                         /*score=*/2.0, /*additional_scores=*/{3, 4, 5});

  // Copy constructor
  ScoredDocumentHit copy = hit1;
  EXPECT_THAT(copy, EqualsScoredDocumentHit(ScoredDocumentHit(
                        /*document_id=*/5,
                        /*section_id_mask=*/49,
                        /*score=*/1.0, /*additional_scores=*/{0, 1, 2})));

  // Copy assignment
  copy = hit2;
  EXPECT_THAT(copy, EqualsScoredDocumentHit(ScoredDocumentHit(
                        /*document_id=*/6,
                        /*section_id_mask=*/50,
                        /*score=*/2.0, /*additional_scores=*/{3, 4, 5})));
}

TEST(ScoredDocumentHitTest, Movable) {
  ScoredDocumentHit hit1(/*document_id=*/5,
                         /*section_id_mask=*/49,
                         /*score=*/1.0, /*additional_scores=*/{0, 1, 2});
  ScoredDocumentHit hit1_copy = hit1;

  ScoredDocumentHit hit2(/*document_id=*/6,
                         /*section_id_mask=*/50,
                         /*score=*/2.0, /*additional_scores=*/{3, 4, 5});
  ScoredDocumentHit hit2_copy = hit2;

  // Move constructor
  ScoredDocumentHit moved = std::move(hit1);
  EXPECT_THAT(moved, EqualsScoredDocumentHit(hit1_copy));

  // Move assignment
  moved = std::move(hit2);
  EXPECT_THAT(moved, EqualsScoredDocumentHit(hit2_copy));
}

TEST(ScoredDocumentHitTest, Swapable) {
  ScoredDocumentHit hit1(/*document_id=*/5,
                         /*section_id_mask=*/49,
                         /*score=*/1.0, /*additional_scores=*/{0, 1, 2});
  ScoredDocumentHit hit1_copy = hit1;

  ScoredDocumentHit hit2(/*document_id=*/6,
                         /*section_id_mask=*/50,
                         /*score=*/2.0, /*additional_scores=*/{3, 4, 5});
  ScoredDocumentHit hit2_copy = hit2;

  std::swap(hit1, hit2);
  EXPECT_THAT(hit1, EqualsScoredDocumentHit(hit2_copy));
  EXPECT_THAT(hit2, EqualsScoredDocumentHit(hit1_copy));
}

TEST(JoinedScoredDocumentHitTest, Converter) {
  JoinedScoredDocumentHit::Converter converter;

  ScoredDocumentHit parent_scored_document_hit(/*document_id=*/5,
                                               /*section_id_mask=*/49,
                                               /*score=*/1.0);
  std::vector<ScoredDocumentHit> child_scored_document_hits{
      ScoredDocumentHit(/*document_id=*/1,
                        /*section_id_mask=*/1,
                        /*score=*/2.0),
      ScoredDocumentHit(/*document_id=*/2,
                        /*section_id_mask=*/2,
                        /*score=*/3.0),
      ScoredDocumentHit(/*document_id=*/3,
                        /*section_id_mask=*/3,
                        /*score=*/4.0)};

  JoinedScoredDocumentHit joined_scored_document_hit(
      /*final_score=*/12345.6789, std::move(parent_scored_document_hit),
      std::move(child_scored_document_hits));
  EXPECT_THAT(converter(JoinedScoredDocumentHit(joined_scored_document_hit)),
              EqualsJoinedScoredDocumentHit(joined_scored_document_hit));
}

TEST(JoinedScoredDocumentHitTest, Optimize) {
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

  ScoredDocumentHit parent_scored_document_hit(/*document_id=*/4,
                                               /*section_id_mask=*/49,
                                               /*score=*/1.0);
  std::vector<ScoredDocumentHit> child_scored_document_hits{
      ScoredDocumentHit(/*document_id=*/5,
                        /*section_id_mask=*/1,
                        /*score=*/2.0),
      ScoredDocumentHit(/*document_id=*/6,
                        /*section_id_mask=*/2,
                        /*score=*/3.0),
      ScoredDocumentHit(/*document_id=*/1,
                        /*section_id_mask=*/3,
                        /*score=*/4.0)};

  JoinedScoredDocumentHit joined_hit(
      /*final_score=*/12345.6789, std::move(parent_scored_document_hit),
      std::move(child_scored_document_hits));

  std::optional<JoinedScoredDocumentHit> optimized_hit =
      std::move(joined_hit).Optimize(document_id_old_to_new);
  EXPECT_THAT(optimized_hit.has_value(), IsTrue());
  EXPECT_THAT(optimized_hit->final_score(), DoubleEq(12345.6789));
  EXPECT_THAT(optimized_hit->parent_scored_document_hit(),
              EqualsScoredDocumentHit(ScoredDocumentHit(
                  /*document_id=*/1, /*section_id_mask=*/49, /*score=*/1.0)));
  EXPECT_THAT(optimized_hit->child_scored_document_hits(),
              ElementsAre(EqualsScoredDocumentHit(ScoredDocumentHit(
                              /*document_id=*/2,
                              /*section_id_mask=*/1,
                              /*score=*/2.0)),
                          EqualsScoredDocumentHit(ScoredDocumentHit(
                              /*document_id=*/3,
                              /*section_id_mask=*/2,
                              /*score=*/3.0)),
                          EqualsScoredDocumentHit(ScoredDocumentHit(
                              /*document_id=*/0,
                              /*section_id_mask=*/3,
                              /*score=*/4.0))));
}

TEST(JoinedScoredDocumentHitTest, Optimize_skipsDeletedChildDocuments) {
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

  ScoredDocumentHit parent_scored_document_hit(/*document_id=*/4,
                                               /*section_id_mask=*/49,
                                               /*score=*/1.0);
  std::vector<ScoredDocumentHit> child_scored_document_hits{
      ScoredDocumentHit(/*document_id=*/0,
                        /*section_id_mask=*/1,
                        /*score=*/2.0),  // Deleted document.
      ScoredDocumentHit(/*document_id=*/5,
                        /*section_id_mask=*/2,
                        /*score=*/3.0),
      ScoredDocumentHit(/*document_id=*/6,
                        /*section_id_mask=*/3,
                        /*score=*/4.0),
      ScoredDocumentHit(/*document_id=*/2,
                        /*section_id_mask=*/4,
                        /*score=*/5.0),  // Deleted document.
      ScoredDocumentHit(/*document_id=*/7,
                        /*section_id_mask=*/5,
                        /*score=*/6.0),  // Deleted document.
      ScoredDocumentHit(/*document_id=*/1,
                        /*section_id_mask=*/6,
                        /*score=*/7.0),
      ScoredDocumentHit(/*document_id=*/3,
                        /*section_id_mask=*/7,
                        /*score=*/8.0)  // Deleted document.
  };

  JoinedScoredDocumentHit joined_hit(
      /*final_score=*/12345.6789, std::move(parent_scored_document_hit),
      std::move(child_scored_document_hits));

  std::optional<JoinedScoredDocumentHit> optimized_hit =
      std::move(joined_hit).Optimize(document_id_old_to_new);
  EXPECT_THAT(optimized_hit.has_value(), IsTrue());
  EXPECT_THAT(optimized_hit->final_score(), DoubleEq(12345.6789));
  EXPECT_THAT(optimized_hit->parent_scored_document_hit(),
              EqualsScoredDocumentHit(ScoredDocumentHit(
                  /*document_id=*/1, /*section_id_mask=*/49, /*score=*/1.0)));
  EXPECT_THAT(optimized_hit->child_scored_document_hits(),
              ElementsAre(EqualsScoredDocumentHit(ScoredDocumentHit(
                              /*document_id=*/2,
                              /*section_id_mask=*/2,
                              /*score=*/3.0)),
                          EqualsScoredDocumentHit(ScoredDocumentHit(
                              /*document_id=*/3,
                              /*section_id_mask=*/3,
                              /*score=*/4.0)),
                          EqualsScoredDocumentHit(ScoredDocumentHit(
                              /*document_id=*/0,
                              /*section_id_mask=*/6,
                              /*score=*/7.0))));
}

TEST(JoinedScoredDocumentHitTest, Optimize_allChildDocumentsDeleted) {
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

  ScoredDocumentHit parent_scored_document_hit(/*document_id=*/4,
                                               /*section_id_mask=*/49,
                                               /*score=*/1.0);
  std::vector<ScoredDocumentHit> child_scored_document_hits{
      ScoredDocumentHit(/*document_id=*/0,
                        /*section_id_mask=*/1,
                        /*score=*/2.0),  // Deleted document.
      ScoredDocumentHit(/*document_id=*/2,
                        /*section_id_mask=*/4,
                        /*score=*/5.0),  // Deleted document.
      ScoredDocumentHit(/*document_id=*/7,
                        /*section_id_mask=*/5,
                        /*score=*/6.0),  // Deleted document.
      ScoredDocumentHit(/*document_id=*/3,
                        /*section_id_mask=*/7,
                        /*score=*/8.0)  // Deleted document.
  };

  JoinedScoredDocumentHit joined_hit(
      /*final_score=*/12345.6789, std::move(parent_scored_document_hit),
      std::move(child_scored_document_hits));

  std::optional<JoinedScoredDocumentHit> optimized_hit =
      std::move(joined_hit).Optimize(document_id_old_to_new);
  EXPECT_THAT(optimized_hit.has_value(), IsTrue());
  EXPECT_THAT(optimized_hit->final_score(), DoubleEq(12345.6789));
  EXPECT_THAT(optimized_hit->parent_scored_document_hit(),
              EqualsScoredDocumentHit(ScoredDocumentHit(
                  /*document_id=*/1, /*section_id_mask=*/49, /*score=*/1.0)));
  EXPECT_THAT(optimized_hit->child_scored_document_hits(), IsEmpty());
}

TEST(JoinedScoredDocumentHitTest,
     Optimize_parentDocumentDeletedReturnsNullopt) {
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

  ScoredDocumentHit parent_scored_document_hit(
      /*document_id=*/3, /*section_id_mask=*/4,
      /*score=*/1.0);  // Deleted document.
  std::vector<ScoredDocumentHit> child_scored_document_hits{
      ScoredDocumentHit(/*document_id=*/0,
                        /*section_id_mask=*/1,
                        /*score=*/2.0),  // Deleted document.
      ScoredDocumentHit(/*document_id=*/5,
                        /*section_id_mask=*/2,
                        /*score=*/3.0),
      ScoredDocumentHit(/*document_id=*/6,
                        /*section_id_mask=*/3,
                        /*score=*/4.0),
      ScoredDocumentHit(/*document_id=*/2,
                        /*section_id_mask=*/4,
                        /*score=*/5.0),  // Deleted document.
      ScoredDocumentHit(/*document_id=*/7,
                        /*section_id_mask=*/5,
                        /*score=*/6.0),  // Deleted document.
      ScoredDocumentHit(/*document_id=*/1,
                        /*section_id_mask=*/6,
                        /*score=*/7.0)};

  JoinedScoredDocumentHit joined_hit(
      /*final_score=*/12345.6789, std::move(parent_scored_document_hit),
      std::move(child_scored_document_hits));

  EXPECT_THAT(std::move(joined_hit).Optimize(document_id_old_to_new),
              Eq(std::nullopt));
}

}  // namespace

}  // namespace lib
}  // namespace icing
