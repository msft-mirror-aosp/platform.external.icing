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

#include "icing/scoring/ranker.h"

#include "icing/scoring/scored-document-hit.h"
#include "icing/testing/common-matchers.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace icing {
namespace lib {

namespace {
using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::Test;

class RankerTest : public Test {
 protected:
  RankerTest()
      : test_scored_document_hit1_(/*document_id=*/3, /*hit_section_id_mask=*/3,
                                   /*score=*/1),
        test_scored_document_hit2_(/*document_id=*/1, /*hit_section_id_mask=*/1,
                                   /*score=*/2),
        test_scored_document_hit3_(/*document_id=*/2, /*hit_section_id_mask=*/2,
                                   /*score=*/3),
        test_scored_document_hit4_(/*document_id=*/5, /*hit_section_id_mask=*/5,
                                   /*score=*/4),
        test_scored_document_hit5_(/*document_id=*/4, /*hit_section_id_mask=*/4,
                                   /*score=*/5) {}

  const ScoredDocumentHit& test_scored_document_hit1() {
    return test_scored_document_hit1_;
  }

  const ScoredDocumentHit& test_scored_document_hit2() {
    return test_scored_document_hit2_;
  }

  const ScoredDocumentHit& test_scored_document_hit3() {
    return test_scored_document_hit3_;
  }

  const ScoredDocumentHit& test_scored_document_hit4() {
    return test_scored_document_hit4_;
  }

  const ScoredDocumentHit& test_scored_document_hit5() {
    return test_scored_document_hit5_;
  }

 private:
  ScoredDocumentHit test_scored_document_hit1_;
  ScoredDocumentHit test_scored_document_hit2_;
  ScoredDocumentHit test_scored_document_hit3_;
  ScoredDocumentHit test_scored_document_hit4_;
  ScoredDocumentHit test_scored_document_hit5_;
};

TEST_F(RankerTest, ShouldHandleEmpty) {
  std::vector<ScoredDocumentHit> scored_document_hits = {};

  EXPECT_THAT(
      GetTopNFromScoredDocumentHits(scored_document_hits, /*num_result=*/0,
                                    /*is_descending=*/true),
      IsEmpty());

  EXPECT_THAT(
      GetTopNFromScoredDocumentHits(scored_document_hits, /*num_result=*/3,
                                    /*is_descending=*/true),
      IsEmpty());
}

TEST_F(RankerTest, ShouldCorrectlySortResults) {
  std::vector<ScoredDocumentHit> scored_document_hits = {
      test_scored_document_hit2(), test_scored_document_hit1(),
      test_scored_document_hit5(), test_scored_document_hit4(),
      test_scored_document_hit3()};

  EXPECT_THAT(
      GetTopNFromScoredDocumentHits(scored_document_hits, /*num_result=*/5,
                                    /*is_descending=*/true),
      ElementsAre(EqualsScoredDocumentHit(test_scored_document_hit5()),
                  EqualsScoredDocumentHit(test_scored_document_hit4()),
                  EqualsScoredDocumentHit(test_scored_document_hit3()),
                  EqualsScoredDocumentHit(test_scored_document_hit2()),
                  EqualsScoredDocumentHit(test_scored_document_hit1())));
}

TEST_F(RankerTest, ShouldHandleSmallerNumResult) {
  std::vector<ScoredDocumentHit> scored_document_hits = {
      test_scored_document_hit2(), test_scored_document_hit1(),
      test_scored_document_hit5(), test_scored_document_hit4(),
      test_scored_document_hit3()};

  // num_result = 3, smaller than the size 5
  EXPECT_THAT(
      GetTopNFromScoredDocumentHits(scored_document_hits, /*num_result=*/3,
                                    /*is_descending=*/true),
      ElementsAre(EqualsScoredDocumentHit(test_scored_document_hit5()),
                  EqualsScoredDocumentHit(test_scored_document_hit4()),
                  EqualsScoredDocumentHit(test_scored_document_hit3())));
}

TEST_F(RankerTest, ShouldHandleGreaterNumResult) {
  std::vector<ScoredDocumentHit> scored_document_hits = {
      test_scored_document_hit2(), test_scored_document_hit1(),
      test_scored_document_hit5(), test_scored_document_hit4(),
      test_scored_document_hit3()};

  // num_result = 10, greater than the size 5
  EXPECT_THAT(
      GetTopNFromScoredDocumentHits(scored_document_hits, /*num_result=*/10,
                                    /*is_descending=*/true),
      ElementsAre(EqualsScoredDocumentHit(test_scored_document_hit5()),
                  EqualsScoredDocumentHit(test_scored_document_hit4()),
                  EqualsScoredDocumentHit(test_scored_document_hit3()),
                  EqualsScoredDocumentHit(test_scored_document_hit2()),
                  EqualsScoredDocumentHit(test_scored_document_hit1())));
}

TEST_F(RankerTest, ShouldHandleAcsendingOrder) {
  std::vector<ScoredDocumentHit> scored_document_hits = {
      test_scored_document_hit2(), test_scored_document_hit1(),
      test_scored_document_hit5(), test_scored_document_hit4(),
      test_scored_document_hit3()};

  EXPECT_THAT(
      GetTopNFromScoredDocumentHits(scored_document_hits, /*num_result=*/5,
                                    /*is_descending=*/false),
      ElementsAre(EqualsScoredDocumentHit(test_scored_document_hit1()),
                  EqualsScoredDocumentHit(test_scored_document_hit2()),
                  EqualsScoredDocumentHit(test_scored_document_hit3()),
                  EqualsScoredDocumentHit(test_scored_document_hit4()),
                  EqualsScoredDocumentHit(test_scored_document_hit5())));
}

TEST_F(RankerTest, ShouldRespectDocumentIdWhenScoresAreEqual) {
  ScoredDocumentHit scored_document_hit1(
      /*document_id=*/1, /*hit_section_id_mask=*/0, /*score=*/100);
  ScoredDocumentHit scored_document_hit2(
      /*document_id=*/2, /*hit_section_id_mask=*/0, /*score=*/100);
  ScoredDocumentHit scored_document_hit3(
      /*document_id=*/3, /*hit_section_id_mask=*/0, /*score=*/100);

  std::vector<ScoredDocumentHit> scored_document_hits = {
      scored_document_hit3, scored_document_hit1, scored_document_hit2};

  EXPECT_THAT(
      GetTopNFromScoredDocumentHits(scored_document_hits, /*num_result=*/3,
                                    /*is_descending=*/true),
      ElementsAre(EqualsScoredDocumentHit(scored_document_hit3),
                  EqualsScoredDocumentHit(scored_document_hit2),
                  EqualsScoredDocumentHit(scored_document_hit1)));

  EXPECT_THAT(
      GetTopNFromScoredDocumentHits(scored_document_hits, /*num_result=*/3,
                                    /*is_descending=*/false),
      ElementsAre(EqualsScoredDocumentHit(scored_document_hit1),
                  EqualsScoredDocumentHit(scored_document_hit2),
                  EqualsScoredDocumentHit(scored_document_hit3)));
}

}  // namespace

}  // namespace lib
}  // namespace icing
