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

#include <algorithm>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::Ge;
using ::testing::IsFalse;
using ::testing::IsTrue;
using ::testing::Lt;
using ::testing::Not;

static constexpr DocumentId kSomeDocumentId = 24;
static constexpr SectionId kSomeSectionid = 5;
static constexpr Hit::TermFrequency kSomeTermFrequency = 57;

TEST(BasicHitTest, Accessors) {
  BasicHit h1(kSomeSectionid, kSomeDocumentId);
  EXPECT_THAT(h1.document_id(), Eq(kSomeDocumentId));
  EXPECT_THAT(h1.section_id(), Eq(kSomeSectionid));
}

TEST(BasicHitTest, Invalid) {
  BasicHit default_invalid;
  EXPECT_THAT(default_invalid.is_valid(), IsFalse());

  // Also make sure the invalid BasicHit contains an invalid document id.
  EXPECT_THAT(default_invalid.document_id(), Eq(kInvalidDocumentId));
  EXPECT_THAT(default_invalid.section_id(), Eq(kMinSectionId));
}

TEST(BasicHitTest, Valid) {
  BasicHit maximum_document_id_hit(kSomeSectionid, kMaxDocumentId);
  EXPECT_THAT(maximum_document_id_hit.is_valid(), IsTrue());

  BasicHit maximum_section_id_hit(kMaxSectionId, kSomeDocumentId);
  EXPECT_THAT(maximum_section_id_hit.is_valid(), IsTrue());

  BasicHit minimum_document_id_hit(kSomeSectionid, kMinDocumentId);
  EXPECT_THAT(minimum_document_id_hit.is_valid(), IsTrue());

  BasicHit minimum_section_id_hit(kMinSectionId, kSomeDocumentId);
  EXPECT_THAT(minimum_section_id_hit.is_valid(), IsTrue());

  BasicHit all_maximum_hit(kMaxSectionId, kMaxDocumentId);
  EXPECT_THAT(all_maximum_hit.is_valid(), IsTrue());

  BasicHit all_minimum_hit(kMinSectionId, kMinDocumentId);
  EXPECT_THAT(all_minimum_hit.is_valid(), IsTrue());

  // We use invalid BasicHit for std::lower_bound. Verify that value of the
  // smallest valid BasicHit (which contains kMinSectionId, kMaxDocumentId) is
  // >= BasicHit::kInvalidValue.
  BasicHit smallest_hit(kMinSectionId, kMaxDocumentId);
  ASSERT_THAT(smallest_hit.is_valid(), IsTrue());
  EXPECT_THAT(smallest_hit.value(), Ge(BasicHit::kInvalidValue));
}

TEST(BasicHitTest, Comparison) {
  BasicHit hit(/*section_id=*/1, /*document_id=*/243);
  // DocumentIds are sorted in ascending order. So a hit with a lower
  // document_id should be considered greater than one with a higher
  // document_id.
  BasicHit higher_document_id_hit(/*section_id=*/1, /*document_id=*/2409);
  BasicHit higher_section_id_hit(/*section_id=*/15, /*document_id=*/243);

  std::vector<BasicHit> hits{hit, higher_document_id_hit,
                             higher_section_id_hit};
  std::sort(hits.begin(), hits.end());
  EXPECT_THAT(hits,
              ElementsAre(higher_document_id_hit, hit, higher_section_id_hit));
}

TEST(HitTest, HasTermFrequencyFlag) {
  Hit h1(kSomeSectionid, kSomeDocumentId, Hit::kDefaultTermFrequency,
         /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
         /*is_stemmed_hit=*/false);
  EXPECT_THAT(h1.has_term_frequency(), IsFalse());
  EXPECT_THAT(h1.term_frequency(), Eq(Hit::kDefaultTermFrequency));

  Hit h2(kSomeSectionid, kSomeDocumentId, kSomeTermFrequency,
         /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
         /*is_stemmed_hit=*/false);
  EXPECT_THAT(h2.has_term_frequency(), IsTrue());
  EXPECT_THAT(h2.term_frequency(), Eq(kSomeTermFrequency));
}

TEST(HitTest, IsStemmedHitFlag) {
  Hit h1(kSomeSectionid, kSomeDocumentId, Hit::kDefaultTermFrequency,
         /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
         /*is_stemmed_hit=*/false);
  EXPECT_THAT(h1.is_stemmed_hit(), IsFalse());

  Hit h2(kSomeSectionid, kSomeDocumentId, kSomeTermFrequency,
         /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
         /*is_stemmed_hit=*/true);
  EXPECT_THAT(h2.is_stemmed_hit(), IsTrue());
}

TEST(HitTest, IsPrefixHitFlag) {
  Hit h1(kSomeSectionid, kSomeDocumentId, Hit::kDefaultTermFrequency,
         /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
         /*is_stemmed_hit=*/false);
  EXPECT_THAT(h1.is_prefix_hit(), IsFalse());

  Hit h2(kSomeSectionid, kSomeDocumentId, Hit::kDefaultTermFrequency,
         /*is_in_prefix_section=*/true, /*is_prefix_hit=*/false,
         /*is_stemmed_hit=*/false);
  EXPECT_THAT(h2.is_prefix_hit(), IsFalse());

  Hit h3(kSomeSectionid, kSomeDocumentId, Hit::kDefaultTermFrequency,
         /*is_in_prefix_section=*/false, /*is_prefix_hit=*/true,
         /*is_stemmed_hit=*/false);
  EXPECT_THAT(h3.is_prefix_hit(), IsTrue());

  Hit h4(kSomeSectionid, kSomeDocumentId, kSomeTermFrequency,
         /*is_in_prefix_section=*/true, /*is_prefix_hit=*/false,
         /*is_stemmed_hit=*/false);
  EXPECT_THAT(h4.is_prefix_hit(), IsFalse());
}

TEST(HitTest, IsInPrefixSectionFlag) {
  Hit h1(kSomeSectionid, kSomeDocumentId, Hit::kDefaultTermFrequency,
         /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
         /*is_stemmed_hit=*/false);
  EXPECT_THAT(h1.is_in_prefix_section(), IsFalse());

  Hit h2(kSomeSectionid, kSomeDocumentId, Hit::kDefaultTermFrequency,
         /*is_in_prefix_section=*/false, /*is_prefix_hit=*/true,
         /*is_stemmed_hit=*/false);
  EXPECT_THAT(h2.is_in_prefix_section(), IsFalse());

  Hit h3(kSomeSectionid, kSomeDocumentId, Hit::kDefaultTermFrequency,
         /*is_in_prefix_section=*/true, /*is_prefix_hit=*/false,
         /*is_stemmed_hit=*/false);
  EXPECT_THAT(h3.is_in_prefix_section(), IsTrue());
}

TEST(HitTest, HasFlags) {
  Hit h1(kSomeSectionid, kSomeDocumentId, Hit::kDefaultTermFrequency,
         /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
         /*is_stemmed_hit=*/false);
  EXPECT_THAT(h1.has_flags(), IsFalse());

  Hit h2(kSomeSectionid, kSomeDocumentId, Hit::kDefaultTermFrequency,
         /*is_in_prefix_section=*/false, /*is_prefix_hit=*/true,
         /*is_stemmed_hit=*/false);
  EXPECT_THAT(h2.has_flags(), IsFalse());

  Hit h3(kSomeSectionid, kSomeDocumentId, Hit::kDefaultTermFrequency,
         /*is_in_prefix_section=*/true, /*is_prefix_hit=*/false,
         /*is_stemmed_hit=*/false);
  EXPECT_THAT(h3.has_flags(), IsFalse());

  Hit h4(kSomeSectionid, kSomeDocumentId, Hit::kDefaultTermFrequency,
         /*is_in_prefix_section=*/true, /*is_prefix_hit=*/false,
         /*is_stemmed_hit=*/true);
  EXPECT_THAT(h4.has_flags(), IsTrue());

  Hit h5(kSomeSectionid, kSomeDocumentId, kSomeTermFrequency,
         /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
         /*is_stemmed_hit=*/false);
  EXPECT_THAT(h5.has_flags(), IsTrue());

  Hit h6(kSomeSectionid, kSomeDocumentId, kSomeTermFrequency,
         /*is_prefix_hit=*/true, /*is_in_prefix_section=*/true,
         /*is_stemmed_hit=*/false);
  EXPECT_THAT(h6.has_flags(), IsTrue());

  Hit h7(kSomeSectionid, kSomeDocumentId, kSomeTermFrequency,
         /*is_prefix_hit=*/false, /*is_in_prefix_section=*/true,
         /*is_stemmed_hit=*/true);
  EXPECT_THAT(h7.has_flags(), IsTrue());

  Hit h8(kSomeSectionid, kSomeDocumentId, kSomeTermFrequency,
         /*is_prefix_hit=*/true, /*is_in_prefix_section=*/false,
         /*is_stemmed_hit=*/true);
  EXPECT_THAT(h8.has_flags(), IsTrue());
}

TEST(HitTest, Accessors) {
  Hit h1(kSomeSectionid, kSomeDocumentId, kSomeTermFrequency,
         /*is_in_prefix_section=*/false, /*is_prefix_hit=*/true,
         /*is_stemmed_hit=*/true);
  EXPECT_THAT(h1.document_id(), Eq(kSomeDocumentId));
  EXPECT_THAT(h1.section_id(), Eq(kSomeSectionid));
  EXPECT_THAT(h1.term_frequency(), Eq(kSomeTermFrequency));
  EXPECT_THAT(h1.is_in_prefix_section(), IsFalse());
  EXPECT_THAT(h1.is_prefix_hit(), IsTrue());
  EXPECT_THAT(h1.is_stemmed_hit(), IsTrue());
}

TEST(HitTest, Valid) {
  Hit def(Hit::kInvalidValue);
  EXPECT_THAT(def.is_valid(), IsFalse());
  Hit explicit_invalid(Hit::kInvalidValue, Hit::kNoEnabledFlags,
                       Hit::kDefaultTermFrequency);
  EXPECT_THAT(explicit_invalid.is_valid(), IsFalse());

  static constexpr Hit::Value kSomeValue = 65372;
  Hit explicit_valid(kSomeValue, Hit::kNoEnabledFlags,
                     Hit::kDefaultTermFrequency);
  EXPECT_THAT(explicit_valid.is_valid(), IsTrue());

  Hit maximum_document_id_hit(
      kSomeSectionid, kMaxDocumentId, kSomeTermFrequency,
      /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
      /*is_stemmed_hit=*/false);
  EXPECT_THAT(maximum_document_id_hit.is_valid(), IsTrue());

  Hit maximum_section_id_hit(kMaxSectionId, kSomeDocumentId, kSomeTermFrequency,
                             /*is_in_prefix_section=*/false,
                             /*is_prefix_hit=*/false, /*is_stemmed_hit=*/false);
  EXPECT_THAT(maximum_section_id_hit.is_valid(), IsTrue());

  Hit minimum_document_id_hit(kSomeSectionid, 0, kSomeTermFrequency,
                              /*is_in_prefix_section=*/false,
                              /*is_prefix_hit=*/false,
                              /*is_stemmed_hit=*/false);
  EXPECT_THAT(minimum_document_id_hit.is_valid(), IsTrue());

  Hit minimum_section_id_hit(0, kSomeDocumentId, kSomeTermFrequency,
                             /*is_in_prefix_section=*/false,
                             /*is_prefix_hit=*/false, /*is_stemmed_hit=*/false);
  EXPECT_THAT(minimum_section_id_hit.is_valid(), IsTrue());

  // We use Hit with value Hit::kMaxDocumentIdSortValue for std::lower_bound
  // in the lite index. Verify that the value of the smallest valid Hit (which
  // contains kMinSectionId, kMaxDocumentId and 3 flags = false) is >=
  // Hit::kMaxDocumentIdSortValue.
  Hit smallest_hit(kMinSectionId, kMaxDocumentId, Hit::kDefaultTermFrequency,
                   /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
                   /*is_stemmed_hit=*/false);
  ASSERT_THAT(smallest_hit.is_valid(), IsTrue());
  ASSERT_THAT(smallest_hit.has_term_frequency(), IsFalse());
  ASSERT_THAT(smallest_hit.is_stemmed_hit(), IsFalse());
  ASSERT_THAT(smallest_hit.is_prefix_hit(), IsFalse());
  ASSERT_THAT(smallest_hit.is_in_prefix_section(), IsFalse());
  EXPECT_THAT(smallest_hit.value(), Ge(Hit::kMaxDocumentIdSortValue));
}

TEST(HitTest, Comparison) {
  Hit hit(/*section_id=*/1, /*document_id=*/243, Hit::kDefaultTermFrequency,
          /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
          /*is_stemmed_hit=*/false);
  // DocumentIds are sorted in ascending order. So a hit with a lower
  // document_id should be considered greater than one with a higher
  // document_id.
  Hit higher_document_id_hit(
      /*section_id=*/1, /*document_id=*/2409, Hit::kDefaultTermFrequency,
      /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
      /*is_stemmed_hit=*/false);
  Hit higher_section_id_hit(/*section_id=*/15, /*document_id=*/243,
                            Hit::kDefaultTermFrequency,
                            /*is_in_prefix_section=*/false,
                            /*is_prefix_hit=*/false, /*is_stemmed_hit=*/false);
  // Whether or not a term frequency was set is considered, but the term
  // frequency itself is not.
  Hit term_frequency_hit(/*section_id=*/1, 243, /*term_frequency=*/12,
                         /*is_in_prefix_section=*/false,
                         /*is_prefix_hit=*/false, /*is_stemmed_hit=*/false);
  Hit prefix_hit(/*section_id=*/1, 243, Hit::kDefaultTermFrequency,
                 /*is_in_prefix_section=*/false,
                 /*is_prefix_hit=*/true, /*is_stemmed_hit=*/false);
  Hit hit_in_prefix_section(/*section_id=*/1, 243, Hit::kDefaultTermFrequency,
                            /*is_in_prefix_section=*/true,
                            /*is_prefix_hit=*/false, /*is_stemmed_hit=*/false);
  Hit hit_with_all_flags_enabled(/*section_id=*/1, 243, 56,
                                 /*is_in_prefix_section=*/true,
                                 /*is_prefix_hit=*/true,
                                 /*is_stemmed_hit=*/true);
  Hit stemmed_hit(/*section_id=*/1, 243, Hit::kDefaultTermFrequency,
                  /*is_in_prefix_section=*/false,
                  /*is_prefix_hit=*/false, /*is_stemmed_hit=*/true);

  std::vector<Hit> hits{hit,
                        higher_document_id_hit,
                        higher_section_id_hit,
                        term_frequency_hit,
                        prefix_hit,
                        hit_in_prefix_section,
                        hit_with_all_flags_enabled,
                        stemmed_hit};
  std::sort(hits.begin(), hits.end());
  EXPECT_THAT(hits,
              ElementsAre(higher_document_id_hit, hit, term_frequency_hit,
                          stemmed_hit, hit_in_prefix_section, prefix_hit,
                          hit_with_all_flags_enabled, higher_section_id_hit));

  Hit higher_term_frequency_hit(/*section_id=*/1, 243, /*term_frequency=*/108,
                                /*is_in_prefix_section=*/false,
                                /*is_prefix_hit=*/false,
                                /*is_stemmed_hit=*/false);
  // The term frequency value is not considered when comparing hits.
  EXPECT_THAT(term_frequency_hit, Not(Lt(higher_term_frequency_hit)));
  EXPECT_THAT(higher_term_frequency_hit, Not(Lt(term_frequency_hit)));
}

TEST(HitTest, CheckFlagsAreConsistent) {
  Hit::Value value_without_flags = 1 << 30;
  Hit::Value value_with_flags = value_without_flags + 1;
  Hit::Flags flags_with_term_freq = 1;

  Hit consistent_hit_no_flags(value_without_flags, Hit::kNoEnabledFlags,
                              Hit::kDefaultTermFrequency);
  Hit consistent_hit_with_term_frequency(value_with_flags, flags_with_term_freq,
                                         kSomeTermFrequency);
  EXPECT_THAT(consistent_hit_no_flags.CheckFlagsAreConsistent(), IsTrue());
  EXPECT_THAT(consistent_hit_with_term_frequency.CheckFlagsAreConsistent(),
              IsTrue());

  Hit inconsistent_hit_1(value_with_flags, Hit::kNoEnabledFlags,
                         Hit::kDefaultTermFrequency);
  Hit inconsistent_hit_2(value_with_flags, Hit::kNoEnabledFlags,
                         kSomeTermFrequency);
  Hit inconsistent_hit_3(value_with_flags, flags_with_term_freq,
                         Hit::kDefaultTermFrequency);
  EXPECT_THAT(inconsistent_hit_1.CheckFlagsAreConsistent(), IsFalse());
  EXPECT_THAT(inconsistent_hit_2.CheckFlagsAreConsistent(), IsFalse());
  EXPECT_THAT(inconsistent_hit_3.CheckFlagsAreConsistent(), IsFalse());
}

}  // namespace

}  // namespace lib
}  // namespace icing
