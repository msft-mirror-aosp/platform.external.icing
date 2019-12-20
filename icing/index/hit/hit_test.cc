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

#include "icing/index/hit/hit.h"

#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsFalse;
using ::testing::IsTrue;
using ::testing::Lt;
using ::testing::Not;

static constexpr DocumentId kSomeDocumentId = 24;
static constexpr SectionId kSomeSectionid = 5;
static constexpr Hit::Score kSomeHitScore = 57;

TEST(HitTest, HasScoreFlag) {
  Hit h1(kSomeSectionid, kSomeDocumentId, Hit::kMaxHitScore);
  EXPECT_THAT(h1.has_score(), IsFalse());
  EXPECT_THAT(h1.score(), Eq(Hit::kMaxHitScore));

  Hit h2(kSomeSectionid, kSomeDocumentId, kSomeHitScore);
  EXPECT_THAT(h2.has_score(), IsTrue());
  EXPECT_THAT(h2.score(), Eq(kSomeHitScore));
}

TEST(HitTest, IsPrefixHitFlag) {
  Hit h1(kSomeSectionid, kSomeDocumentId, Hit::kMaxHitScore);
  EXPECT_THAT(h1.is_prefix_hit(), IsFalse());

  Hit h2(kSomeSectionid, kSomeDocumentId, Hit::kMaxHitScore,
         /*in_prefix_section=*/false, /*is_prefix_hit=*/false);
  EXPECT_THAT(h2.is_prefix_hit(), IsFalse());

  Hit h3(kSomeSectionid, kSomeDocumentId, Hit::kMaxHitScore,
         /*in_prefix_section=*/false, /*is_prefix_hit=*/true);
  EXPECT_THAT(h3.is_prefix_hit(), IsTrue());
}

TEST(HitTest, IsInPrefixSectionFlag) {
  Hit h1(kSomeSectionid, kSomeDocumentId, Hit::kMaxHitScore);
  EXPECT_THAT(h1.is_in_prefix_section(), IsFalse());

  Hit h2(kSomeSectionid, kSomeDocumentId, Hit::kMaxHitScore,
         /*in_prefix_section=*/false);
  EXPECT_THAT(h2.is_in_prefix_section(), IsFalse());

  Hit h3(kSomeSectionid, kSomeDocumentId, Hit::kMaxHitScore,
         /*in_prefix_section=*/true);
  EXPECT_THAT(h3.is_in_prefix_section(), IsTrue());
}

TEST(HitTest, Accessors) {
  Hit h1(kSomeSectionid, kSomeDocumentId, Hit::kMaxHitScore);
  EXPECT_THAT(h1.document_id(), Eq(kSomeDocumentId));
  EXPECT_THAT(h1.section_id(), Eq(kSomeSectionid));
}

TEST(HitTest, Valid) {
  Hit def;
  EXPECT_THAT(def.is_valid(), IsFalse());

  Hit explicit_invalid(Hit::kInvalidValue);
  EXPECT_THAT(explicit_invalid.is_valid(), IsFalse());

  static constexpr Hit::Value kSomeValue = 65372;
  Hit explicit_valid(kSomeValue);
  EXPECT_THAT(explicit_valid.is_valid(), IsTrue());

  Hit maximum_document_id_hit(kSomeSectionid, kMaxDocumentId, kSomeHitScore);
  EXPECT_THAT(maximum_document_id_hit.is_valid(), IsTrue());

  Hit maximum_section_id_hit(kMaxSectionId, kSomeDocumentId, kSomeHitScore);
  EXPECT_THAT(maximum_section_id_hit.is_valid(), IsTrue());

  Hit minimum_document_id_hit(kSomeSectionid, 0, kSomeHitScore);
  EXPECT_THAT(minimum_document_id_hit.is_valid(), IsTrue());

  Hit minimum_section_id_hit(0, kSomeDocumentId, kSomeHitScore);
  EXPECT_THAT(minimum_section_id_hit.is_valid(), IsTrue());
}

TEST(HitTest, Comparison) {
  Hit hit(1, 243, Hit::kMaxHitScore);
  // DocumentIds are sorted in ascending order. So a hit with a lower
  // document_id should be considered greater than one with a higher
  // document_id.
  Hit higher_document_id_hit(1, 2409, Hit::kMaxHitScore);
  Hit higher_section_id_hit(15, 243, Hit::kMaxHitScore);
  // Whether or not a hit score was set is considered, but the score itself is
  // not.
  Hit hitscore_hit(1, 243, 12);
  Hit prefix_hit(1, 243, Hit::kMaxHitScore, /*in_prefix_section=*/false,
                 /*is_prefix_hit=*/true);
  Hit hit_in_prefix_section(1, 243, Hit::kMaxHitScore,
                            /*in_prefix_section=*/true,
                            /*is_prefix_hit=*/false);

  std::vector<Hit> hits{
      hit,        higher_document_id_hit, higher_section_id_hit, hitscore_hit,
      prefix_hit, hit_in_prefix_section};
  std::sort(hits.begin(), hits.end());
  EXPECT_THAT(hits,
              ElementsAre(higher_document_id_hit, hit, hit_in_prefix_section,
                          prefix_hit, hitscore_hit, higher_section_id_hit));

  Hit higher_hitscore_hit(1, 243, 108);
  // Hit score value is not considered when comparing hits.
  EXPECT_THAT(hitscore_hit, Not(Lt(higher_hitscore_hit)));
  EXPECT_THAT(higher_hitscore_hit, Not(Lt(hitscore_hit)));
}

}  // namespace

}  // namespace lib
}  // namespace icing
