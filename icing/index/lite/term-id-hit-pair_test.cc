// Copyright (C) 2024 Google LLC
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

#include "icing/index/lite/term-id-hit-pair.h"

#include <algorithm>
#include <cstdint>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/index/hit/hit.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {
namespace {

using ::testing::ElementsAre;
using ::testing::Eq;

static constexpr DocumentId kSomeDocumentId = 24;
static constexpr SectionId kSomeSectionid = 5;
static constexpr Hit::TermFrequency kSomeTermFrequency = 57;
static constexpr uint32_t kSomeTermId = 129;
static constexpr uint32_t kSomeSmallerTermId = 1;
static constexpr uint32_t kSomeLargerTermId = 0b101010101111111100000001;

TEST(TermIdHitPairTest, Accessors) {
  Hit hit1(kSomeSectionid, kSomeDocumentId, kSomeTermFrequency,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  Hit hit2(kSomeSectionid, kSomeDocumentId, kSomeTermFrequency,
           /*is_in_prefix_section=*/true, /*is_prefix_hit=*/true,
           /*is_stemmed_hit=*/false);
  Hit hit3(kSomeSectionid, kSomeDocumentId, kSomeTermFrequency,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  Hit hit4(kSomeSectionid, kSomeDocumentId, kSomeTermFrequency,
           /*is_in_prefix_section=*/true, /*is_prefix_hit=*/true,
           /*is_stemmed_hit=*/true);
  Hit invalid_hit(Hit::kInvalidValue);

  TermIdHitPair term_id_hit_pair_1(kSomeTermId, hit1);
  EXPECT_THAT(term_id_hit_pair_1.term_id(), Eq(kSomeTermId));
  EXPECT_THAT(term_id_hit_pair_1.hit(), Eq(hit1));

  TermIdHitPair term_id_hit_pair_2(kSomeLargerTermId, hit2);
  EXPECT_THAT(term_id_hit_pair_2.term_id(), Eq(kSomeLargerTermId));
  EXPECT_THAT(term_id_hit_pair_2.hit(), Eq(hit2));

  TermIdHitPair term_id_hit_pair_3(kSomeTermId, invalid_hit);
  EXPECT_THAT(term_id_hit_pair_3.term_id(), Eq(kSomeTermId));
  EXPECT_THAT(term_id_hit_pair_3.hit(), Eq(invalid_hit));

  TermIdHitPair term_id_hit_pair_4(kSomeTermId, hit4);
  EXPECT_THAT(term_id_hit_pair_4.term_id(), Eq(kSomeTermId));
  EXPECT_THAT(term_id_hit_pair_4.hit(), Eq(hit4));
}

TEST(TermIdHitPairTest, Comparison) {
  Hit hit(kSomeSectionid, kSomeDocumentId, kSomeTermFrequency,
          /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
          /*is_stemmed_hit=*/false);
  Hit smaller_hit(/*section_id=*/1, /*document_id=*/100, /*term_frequency=*/1,
                  /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
                  /*is_stemmed_hit=*/false);

  TermIdHitPair term_id_hit_pair(kSomeTermId, hit);
  TermIdHitPair term_id_hit_pair_equal(kSomeTermId, hit);
  TermIdHitPair term_id_hit_pair_smaller_hit(kSomeTermId, smaller_hit);
  TermIdHitPair term_id_hit_pair_smaller_term_id(kSomeSmallerTermId, hit);
  TermIdHitPair term_id_hit_pair_larger_term_id(kSomeLargerTermId, hit);
  TermIdHitPair term_id_hit_pair_smaller_term_id_and_hit(kSomeSmallerTermId,
                                                         smaller_hit);

  std::vector<TermIdHitPair> term_id_hit_pairs{
      term_id_hit_pair,
      term_id_hit_pair_equal,
      term_id_hit_pair_smaller_hit,
      term_id_hit_pair_smaller_term_id,
      term_id_hit_pair_larger_term_id,
      term_id_hit_pair_smaller_term_id_and_hit};
  std::sort(term_id_hit_pairs.begin(), term_id_hit_pairs.end());
  EXPECT_THAT(term_id_hit_pairs,
              ElementsAre(term_id_hit_pair_smaller_term_id_and_hit,
                          term_id_hit_pair_smaller_term_id,
                          term_id_hit_pair_smaller_hit, term_id_hit_pair_equal,
                          term_id_hit_pair, term_id_hit_pair_larger_term_id));
}

}  // namespace

}  // namespace lib
}  // namespace icing
