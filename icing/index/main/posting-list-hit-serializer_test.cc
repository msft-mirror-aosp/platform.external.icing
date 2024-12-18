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

#include "icing/index/main/posting-list-hit-serializer.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/file/posting_list/posting-list-used.h"
#include "icing/index/hit/hit.h"
#include "icing/legacy/index/icing-bit-util.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/hit-test-utils.h"
#include "icing/util/math-util.h"

using testing::ElementsAre;
using testing::Eq;
using testing::Gt;
using testing::IsEmpty;
using testing::IsFalse;
using testing::IsTrue;
using testing::Le;

namespace icing {
namespace lib {

namespace {

TEST(PostingListHitSerializerTest, PostingListUsedPrependHitNotFull) {
  PostingListHitSerializer serializer;

  static const int kNumHits = 2551;
  static const size_t kHitsSize = kNumHits * sizeof(Hit);

  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, kHitsSize));

  // Make used.
  Hit hit0(/*section_id=*/0, /*document_id=*/0, /*term_frequency=*/56,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit0));
  // Size = sizeof(uncompressed hit0::Value)
  //        + sizeof(hit0::Flags)
  //        + sizeof(hit0::TermFrequency)
  int expected_size =
      sizeof(Hit::Value) + sizeof(Hit::Flags) + sizeof(Hit::TermFrequency);
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(EqualsHit(hit0))));

  Hit hit1(/*section_id=*/0, /*document_id=*/1, Hit::kDefaultTermFrequency,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  uint8_t delta_buf[VarInt::kMaxEncodedLen64];
  size_t delta_len = PostingListHitSerializer::EncodeNextHitValue(
      /*next_hit_value=*/hit1.value(),
      /*curr_hit_value=*/hit0.value(), delta_buf);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit1));
  // Size = sizeof(uncompressed hit1::Value)
  //        + sizeof(hit0-hit1)
  //        + sizeof(hit0::Flags)
  //        + sizeof(hit0::TermFrequency)
  expected_size += delta_len;
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(EqualsHit(hit1), EqualsHit(hit0))));

  Hit hit2(/*section_id=*/0, /*document_id=*/2, /*term_frequency=*/56,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  delta_len = PostingListHitSerializer::EncodeNextHitValue(
      /*next_hit_value=*/hit2.value(),
      /*curr_hit_value=*/hit1.value(), delta_buf);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit2));
  // Size = sizeof(uncompressed hit2::Value) + sizeof(hit2::Flags)
  //        + sizeof(hit2::TermFrequency)
  //        + sizeof(hit1-hit2)
  //        + sizeof(hit0-hit1)
  //        + sizeof(hit0::flags)
  //        + sizeof(hit0::term_frequency)
  expected_size += delta_len + sizeof(Hit::Flags) + sizeof(Hit::TermFrequency);
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(EqualsHit(hit2), EqualsHit(hit1),
                                       EqualsHit(hit0))));

  Hit hit3(/*section_id=*/0, /*document_id=*/3, Hit::kDefaultTermFrequency,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  delta_len = PostingListHitSerializer::EncodeNextHitValue(
      /*next_hit_value=*/hit3.value(),
      /*curr_hit_value=*/hit2.value(), delta_buf);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit3));
  // Size = sizeof(uncompressed hit3::Value)
  //        + sizeof(hit2-hit3) + sizeof(hit2::Flags)
  //        + sizeof(hit2::TermFrequency)
  //        + sizeof(hit1-hit2)
  //        + sizeof(hit0-hit1)
  //        + sizeof(hit0::flags)
  //        + sizeof(hit0::term_frequency)
  expected_size += delta_len;
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(EqualsHit(hit3), EqualsHit(hit2),
                                       EqualsHit(hit1), EqualsHit(hit0))));
}

TEST(PostingListHitSerializerTest,
     PostingListUsedPrependHitAlmostFull_withFlags) {
  PostingListHitSerializer serializer;

  // Size = 24
  int pl_size = 2 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));

  // Fill up the compressed region.
  // Transitions:
  // Adding hit0: EMPTY -> NOT_FULL
  // Adding hit1: NOT_FULL -> NOT_FULL
  // Adding hit2: NOT_FULL -> NOT_FULL
  Hit hit0(/*section_id=*/0, /*document_id=*/0, Hit::kDefaultTermFrequency,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  Hit hit1 = CreateHit(hit0, /*desired_byte_length=*/3);
  Hit hit2 = CreateHit(hit1, /*desired_byte_length=*/2, /*term_frequency=*/57,
                       /*is_in_prefix_section=*/true,
                       /*is_prefix_hit=*/true, /*is_stemmed_hit=*/false);
  EXPECT_THAT(hit2.has_flags(), IsTrue());
  EXPECT_THAT(hit2.has_term_frequency(), IsTrue());
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit0));
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit1));
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit2));
  // Size used will be 4 (hit2::Value) + 1 (hit2::Flags) + 1
  // (hit2::TermFrequency) + 2 (hit1-hit2) + 3 (hit0-hit1) = 11 bytes
  int expected_size = sizeof(Hit::Value) + sizeof(Hit::Flags) +
                      sizeof(Hit::TermFrequency) + 2 + 3;
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(EqualsHit(hit2), EqualsHit(hit1),
                                       EqualsHit(hit0))));

  // Add one more hit to transition NOT_FULL -> ALMOST_FULL
  Hit hit3 =
      CreateHit(hit2, /*desired_byte_length=*/3, Hit::kDefaultTermFrequency,
                /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
                /*is_stemmed_hit=*/false);
  EXPECT_THAT(hit3.has_flags(), IsFalse());
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit3));
  // Storing them in the compressed region requires 4 (hit3::Value) + 3
  // (hit2-hit3) + 1 (hit2::Flags) + 1 (hit2::TermFrequency) + 2 (hit1-hit2) + 3
  // (hit0-hit1) = 14 bytes, but there are only 12 bytes in the compressed
  // region. So instead, the posting list will transition to ALMOST_FULL.
  // The in-use compressed region will actually shrink from 11 bytes to 10 bytes
  // because the uncompressed version of hit2 will be overwritten with the
  // compressed delta of hit2. hit3 will be written to one of the special hits.
  // Because we're in ALMOST_FULL, the expected size is the size of the pl minus
  // the one hit used to mark the posting list as ALMOST_FULL.
  expected_size = pl_size - sizeof(Hit);
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(EqualsHit(hit3), EqualsHit(hit2),
                                       EqualsHit(hit1), EqualsHit(hit0))));

  // Add one more hit to transition ALMOST_FULL -> ALMOST_FULL
  Hit hit4 = CreateHit(hit3, /*desired_byte_length=*/2);
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit4));
  // There are currently 10 bytes in use in the compressed region. hit3 will
  // have a 2-byte delta, which fits in the compressed region. Hit3 will be
  // moved from the special hit to the compressed region (which will have 12
  // bytes in use after adding hit3). Hit4 will be placed in one of the special
  // hits and the posting list will remain in ALMOST_FULL.
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(EqualsHit(hit4), EqualsHit(hit3),
                                       EqualsHit(hit2), EqualsHit(hit1),
                                       EqualsHit(hit0))));

  // Add one more hit to transition ALMOST_FULL -> FULL
  Hit hit5 = CreateHit(hit4, /*desired_byte_length=*/2);
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit5));
  // There are currently 12 bytes in use in the compressed region. hit4 will
  // have a 2-byte delta which will not fit in the compressed region. So hit4
  // will remain in one of the special hits and hit5 will occupy the other,
  // making the posting list FULL.
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(pl_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(EqualsHit(hit5), EqualsHit(hit4),
                                       EqualsHit(hit3), EqualsHit(hit2),
                                       EqualsHit(hit1), EqualsHit(hit0))));

  // The posting list is FULL. Adding another hit should fail.
  Hit hit6 = CreateHit(hit5, /*desired_byte_length=*/1);
  EXPECT_THAT(serializer.PrependHit(&pl_used, hit6),
              StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
}

TEST(PostingListHitSerializerTest, PostingListUsedPrependHitAlmostFull) {
  PostingListHitSerializer serializer;

  // Size = 24
  int pl_size = 2 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));

  // Fill up the compressed region.
  // Transitions:
  // Adding hit0: EMPTY -> NOT_FULL
  // Adding hit1: NOT_FULL -> NOT_FULL
  // Adding hit2: NOT_FULL -> NOT_FULL
  Hit hit0(/*section_id=*/0, /*document_id=*/0, Hit::kDefaultTermFrequency,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  Hit hit1 = CreateHit(hit0, /*desired_byte_length=*/3);
  Hit hit2 = CreateHit(hit1, /*desired_byte_length=*/3);
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit0));
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit1));
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit2));
  // Size used will be 4 (hit2::Value) + 3 (hit1-hit2) + 3 (hit0-hit1)
  // = 10 bytes
  int expected_size = sizeof(Hit::Value) + 3 + 3;
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(EqualsHit(hit2), EqualsHit(hit1),
                                       EqualsHit(hit0))));

  // Add one more hit to transition NOT_FULL -> ALMOST_FULL
  Hit hit3 = CreateHit(hit2, /*desired_byte_length=*/3);
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit3));
  // Storing them in the compressed region requires 4 (hit3::Value) + 3
  // (hit2-hit3) + 3 (hit1-hit2) + 3 (hit0-hit1) = 13 bytes, but there are only
  // 12 bytes in the compressed region. So instead, the posting list will
  // transition to ALMOST_FULL.
  // The in-use compressed region will actually shrink from 10 bytes to 9 bytes
  // because the uncompressed version of hit2 will be overwritten with the
  // compressed delta of hit2. hit3 will be written to one of the special hits.
  // Because we're in ALMOST_FULL, the expected size is the size of the pl minus
  // the one hit used to mark the posting list as ALMOST_FULL.
  expected_size = pl_size - sizeof(Hit);
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(EqualsHit(hit3), EqualsHit(hit2),
                                       EqualsHit(hit1), EqualsHit(hit0))));

  // Add one more hit to transition ALMOST_FULL -> ALMOST_FULL
  Hit hit4 = CreateHit(hit3, /*desired_byte_length=*/2);
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit4));
  // There are currently 9 bytes in use in the compressed region. Hit3 will
  // have a 2-byte delta, which fits in the compressed region. Hit3 will be
  // moved from the special hit to the compressed region (which will have 11
  // bytes in use after adding hit3). Hit4 will be placed in one of the special
  // hits and the posting list will remain in ALMOST_FULL.
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(EqualsHit(hit4), EqualsHit(hit3),
                                       EqualsHit(hit2), EqualsHit(hit1),
                                       EqualsHit(hit0))));

  // Add one more hit to transition ALMOST_FULL -> FULL
  Hit hit5 = CreateHit(hit4, /*desired_byte_length=*/2);
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit5));
  // There are currently 11 bytes in use in the compressed region. Hit4 will
  // have a 2-byte delta which will not fit in the compressed region. So hit4
  // will remain in one of the special hits and hit5 will occupy the other,
  // making the posting list FULL.
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(pl_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(EqualsHit(hit5), EqualsHit(hit4),
                                       EqualsHit(hit3), EqualsHit(hit2),
                                       EqualsHit(hit1), EqualsHit(hit0))));

  // The posting list is FULL. Adding another hit should fail.
  Hit hit6 = CreateHit(hit5, /*desired_byte_length=*/1);
  EXPECT_THAT(serializer.PrependHit(&pl_used, hit6),
              StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
}

TEST(PostingListHitSerializerTest, PrependHitsWithSameValue) {
  PostingListHitSerializer serializer;

  // Size = 24
  int pl_size = 2 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));

  // Fill up the compressed region.
  Hit hit0(/*section_id=*/0, /*document_id=*/0, Hit::kDefaultTermFrequency,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  Hit hit1 = CreateHit(hit0, /*desired_byte_length=*/3);
  Hit hit2 = CreateHit(hit1, /*desired_byte_length=*/2, /*term_frequency=*/57,
                       /*is_in_prefix_section=*/true,
                       /*is_prefix_hit=*/true, /*is_stemmed_hit=*/false);
  // Create hit3 with the same value but different flags as hit2 (hit3_flags
  // is set to have all currently-defined flags enabled)
  Hit::Flags hit3_flags = 0;
  for (int i = 0; i < Hit::kNumFlagsInFlagsField; ++i) {
    hit3_flags |= (1 << i);
  }
  Hit hit3(hit2.value(), /*term_frequency=*/hit2.term_frequency(),
           /*flags=*/hit3_flags);

  // hit3 is larger than hit2 (its flag value is larger), and so needs to be
  // prepended first
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit0));
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit1));
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit3));
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit2));
  // Posting list is now ALMOST_FULL
  // ----------------------
  // 23-21     delta(Hit #0)
  // 20-19     delta(Hit #1)
  // 18        term-frequency(Hit #2)
  // 17        flags(Hit #2)
  // 16        delta(Hit #2) = 0
  // 15-12     <unused padding = 0>
  // 11-6      Hit #3
  // 5-0       kSpecialHit
  // ----------------------
  int bytes_used = pl_size - sizeof(Hit);

  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(bytes_used));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(EqualsHit(hit2), EqualsHit(hit3),
                                       EqualsHit(hit1), EqualsHit(hit0))));
}

TEST(PostingListHitSerializerTest, PostingListUsedMinSize) {
  PostingListHitSerializer serializer;

  // Min size = 12
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used,
      PostingListUsed::CreateFromUnitializedRegion(
          &serializer, serializer.GetMinPostingListSize()));
  // PL State: EMPTY
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(0));
  EXPECT_THAT(serializer.GetHits(&pl_used), IsOkAndHolds(IsEmpty()));

  // Add a hit, PL should shift to ALMOST_FULL state
  Hit hit0(/*section_id=*/1, /*document_id=*/0, /*term_frequency=*/0,
           /*is_in_prefix_section=*/false,
           /*is_prefix_hit=*/true, /*is_stemmed_hit=*/false);
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit0));
  // Size = sizeof(uncompressed hit0)
  int expected_size = sizeof(Hit);
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Le(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(EqualsHit(hit0))));

  // Add the smallest hit possible - no term_frequency, non-prefix hit and a
  // delta of 0b10. PL should shift to FULL state.
  Hit hit1(/*section_id=*/0, /*document_id=*/0, /*term_frequency=*/0,
           /*is_in_prefix_section=*/false,
           /*is_prefix_hit=*/false, /*is_stemmed_hit=*/false);
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit1));
  // Size = sizeof(uncompressed hit1) + sizeof(uncompressed hit0)
  expected_size += sizeof(Hit);
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Le(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(EqualsHit(hit1), EqualsHit(hit0))));

  // Try to add the smallest hit possible. Should fail
  Hit hit2(/*section_id=*/0, /*document_id=*/0, /*term_frequency=*/0,
           /*is_in_prefix_section=*/false,
           /*is_prefix_hit=*/false, /*is_stemmed_hit=*/false);
  EXPECT_THAT(serializer.PrependHit(&pl_used, hit2),
              StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Le(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(EqualsHit(hit1), EqualsHit(hit0))));
}

TEST(PostingListHitSerializerTest,
     PostingListStatusJumpFromNotFullToFullAndBack) {
  PostingListHitSerializer serializer;

  // Size = 18
  const uint32_t pl_size = 3 * sizeof(Hit);
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));

  Hit max_valued_hit(kMaxSectionId, kMinDocumentId, Hit::kMaxTermFrequency,
                     /*is_in_prefix_section=*/true, /*is_prefix_hit=*/true,
                     /*is_stemmed_hit=*/false);
  ICING_ASSERT_OK(serializer.PrependHit(&pl, max_valued_hit));
  uint32_t bytes_used = serializer.GetBytesUsed(&pl);
  ASSERT_THAT(bytes_used, sizeof(Hit::Value) + sizeof(Hit::Flags) +
                              sizeof(Hit::TermFrequency));
  // Status not full.
  ASSERT_THAT(bytes_used,
              Le(pl_size - PostingListHitSerializer::kSpecialHitsSize));

  Hit min_valued_hit(kMinSectionId, kMaxDocumentId, Hit::kMaxTermFrequency,
                     /*is_in_prefix_section=*/true, /*is_prefix_hit=*/true,
                     /*is_stemmed_hit=*/false);
  uint8_t delta_buf[VarInt::kMaxEncodedLen64];
  size_t delta_len = PostingListHitSerializer::EncodeNextHitValue(
      /*next_hit_value=*/min_valued_hit.value(),
      /*curr_hit_value=*/max_valued_hit.value(), delta_buf);
  // The compressed region available is pl_size - 2 * sizeof(specialHits) = 6
  // We need to also fit max_valued_hit's flags and term-frequency fields, which
  // each take 1 byte
  // So we'll jump directly to FULL if the varint-encoded delta of the 2 hits >
  // 6 - sizeof(Hit::Flags) - sizeof(Hit::TermFrequency) = 4
  ASSERT_THAT(delta_len, Gt(4));
  ICING_ASSERT_OK(serializer.PrependHit(
      &pl, Hit(kMinSectionId, kMaxDocumentId, Hit::kMaxTermFrequency,
               /*is_in_prefix_section=*/true, /*is_prefix_hit=*/true,
               /*is_stemmed_hit=*/false)));
  // Status should jump to full directly.
  ASSERT_THAT(serializer.GetBytesUsed(&pl), Eq(pl_size));
  ICING_ASSERT_OK(serializer.PopFrontHits(&pl, 1));
  // Status should return to not full as before.
  ASSERT_THAT(serializer.GetBytesUsed(&pl), Eq(bytes_used));
}

TEST(PostingListHitSerializerTest, DeltaOverflow) {
  PostingListHitSerializer serializer;

  const uint32_t pl_size = 4 * sizeof(Hit);
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));

  static const Hit::Value kMaxHitValue = std::numeric_limits<Hit::Value>::max();
  static const Hit::Value kOverflow[4] = {
      kMaxHitValue >> 2,
      (kMaxHitValue >> 2) * 2,
      (kMaxHitValue >> 2) * 3,
      kMaxHitValue - 1,
  };

  // Fit at least 4 ordinary values.
  for (Hit::Value v = 0; v < 4; v++) {
    ICING_EXPECT_OK(serializer.PrependHit(&pl, Hit(4 - v)));
  }

  // Cannot fit 4 overflow values.
  ICING_ASSERT_OK_AND_ASSIGN(
      pl, PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  Hit::Flags has_term_frequency_flags = 0b1;
  ICING_EXPECT_OK(serializer.PrependHit(
      &pl, Hit(/*value=*/kOverflow[3], has_term_frequency_flags,
               /*term_frequency=*/8)));
  ICING_EXPECT_OK(serializer.PrependHit(
      &pl, Hit(/*value=*/kOverflow[2], has_term_frequency_flags,
               /*term_frequency=*/8)));

  // Can fit only one more.
  ICING_EXPECT_OK(serializer.PrependHit(
      &pl, Hit(/*value=*/kOverflow[1], has_term_frequency_flags,
               /*term_frequency=*/8)));
  EXPECT_THAT(serializer.PrependHit(
                  &pl, Hit(/*value=*/kOverflow[0], has_term_frequency_flags,
                           /*term_frequency=*/8)),
              StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
}

TEST(PostingListHitSerializerTest, GetMinPostingListToFitForNotFullPL) {
  PostingListHitSerializer serializer;

  // Size = 24
  int pl_size = 2 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  // Create and add some hits to make pl_used NOT_FULL
  Hit hit0(/*section_id=*/1, /*document_id=*/0, Hit::kDefaultTermFrequency,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  Hit hit1 = CreateHit(/*last_hit=*/hit0, /*desired_byte_length=*/1);
  Hit hit2 = CreateHit(/*last_hit=*/hit1, /*desired_byte_length=*/1);
  Hit hit3 = CreateHit(/*last_hit=*/hit2, /*desired_byte_length=*/1);
  Hit hit4 = CreateHit(/*last_hit=*/hit3, /*desired_byte_length=*/1);
  Hit hit5 = CreateHit(/*last_hit=*/hit4, /*desired_byte_length=*/1);
  Hit hit6 = CreateHit(/*last_hit=*/hit5, /*desired_byte_length=*/1);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit0));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit1));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit2));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit3));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit4));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit5));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit6));

  // ----------------------
  // 23     delta(Hit #0)
  // 22     delta(Hit #1)
  // 21     delta(Hit #2)
  // 20     delta(Hit #3)
  // 19     delta(Hit #4)
  // 18     delta(Hit #5)
  // 17-14  Hit #6
  // 13-12  <unused>
  // 11-6   kSpecialHit
  // 5-0    Offset=14
  // ----------------------
  int bytes_used = 10;

  // Check that all hits have been inserted
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(bytes_used));
  EXPECT_THAT(
      serializer.GetHits(&pl_used),
      IsOkAndHolds(ElementsAre(
          EqualsHit(hit6), EqualsHit(hit5), EqualsHit(hit4), EqualsHit(hit3),
          EqualsHit(hit2), EqualsHit(hit1), EqualsHit(hit0))));

  // Get the min size to fit for the hits in pl_used. Moving the hits in pl_used
  // into a posting list with this min size should make it ALMOST_FULL, which we
  // can see should have size = 18.
  // ----------------------
  // 17     delta(Hit #0)
  // 16     delta(Hit #1)
  // 15     delta(Hit #2)
  // 14     delta(Hit #3)
  // 13     delta(Hit #4)
  // 12     delta(Hit #5)
  // 11-6   Hit #6
  // 5-0    kSpecialHit
  // ----------------------
  int expected_min_size = 18;
  uint32_t min_size_to_fit = serializer.GetMinPostingListSizeToFit(&pl_used);
  EXPECT_THAT(min_size_to_fit, Eq(expected_min_size));

  // min_size_to_fit is the smallest PL size that can fit all the hits, but
  // PostingListUsed and serializer require that the PL size is a multiple of
  // sizeof(Hit). So we need to round up.
  uint32_t min_size_to_fit_rounded_up =
      math_util::RoundUpTo(min_size_to_fit, static_cast<uint32_t>(sizeof(Hit)));

  expected_min_size += sizeof(Hit);

  // Also check that this min size to fit posting list actually does fit all the
  // hits and can only hit one more hit in the ALMOST_FULL state.
  ICING_ASSERT_OK_AND_ASSIGN(PostingListUsed min_size_to_fit_pl,
                             PostingListUsed::CreateFromUnitializedRegion(
                                 &serializer, min_size_to_fit_rounded_up));
  ICING_ASSERT_OK(serializer.PrependHit(&min_size_to_fit_pl, hit0));
  ICING_ASSERT_OK(serializer.PrependHit(&min_size_to_fit_pl, hit1));
  ICING_ASSERT_OK(serializer.PrependHit(&min_size_to_fit_pl, hit2));
  ICING_ASSERT_OK(serializer.PrependHit(&min_size_to_fit_pl, hit3));
  ICING_ASSERT_OK(serializer.PrependHit(&min_size_to_fit_pl, hit4));
  ICING_ASSERT_OK(serializer.PrependHit(&min_size_to_fit_pl, hit5));
  ICING_ASSERT_OK(serializer.PrependHit(&min_size_to_fit_pl, hit6));

  // Check that all hits have been inserted and the min-fit posting list is now
  // ALMOST_FULL.
  EXPECT_THAT(serializer.GetBytesUsed(&min_size_to_fit_pl),
              Eq(min_size_to_fit_rounded_up - sizeof(Hit)));
  EXPECT_THAT(
      serializer.GetHits(&min_size_to_fit_pl),
      IsOkAndHolds(ElementsAre(
          EqualsHit(hit6), EqualsHit(hit5), EqualsHit(hit4), EqualsHit(hit3),
          EqualsHit(hit2), EqualsHit(hit1), EqualsHit(hit0))));
}

TEST(PostingListHitSerializerTest, GetMinPostingListToFitForTwoHits) {
  PostingListHitSerializer serializer;

  // Size = 36
  int pl_size = 3 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));

  // Create and add 2 hits
  Hit hit0(/*section_id=*/1, /*document_id=*/0, /*term_frequency=*/5,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  Hit hit1 = CreateHit(/*last_hit=*/hit0, /*desired_byte_length=*/4);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit0));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit1));
  // ----------------------
  // 35     term-frequency(Hit #0)
  // 34     flags(Hit #0)
  // 33-30  delta(Hit #0)
  // 29     term-frequency(Hit #1)
  // 28     flags(Hit #1)
  // 27-24  Hit #1
  // 23-12  <unused>
  // 11-6   kSpecialHit
  // 5-0    Offset=24
  // ----------------------
  int bytes_used = 12;

  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(bytes_used));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(EqualsHit(hit1), EqualsHit(hit0))));

  // GetMinPostingListSizeToFit should return min posting list size.
  EXPECT_THAT(serializer.GetMinPostingListSizeToFit(&pl_used),
              Eq(serializer.GetMinPostingListSize()));
}

TEST(PostingListHitSerializerTest, GetMinPostingListToFitForThreeSmallHits) {
  PostingListHitSerializer serializer;

  // Size = 24
  int pl_size = 2 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  // Create and add 3 small hits that fit in the size range where we should be
  // checking for whether the PL has only 2 hits
  Hit hit0(/*section_id=*/1, /*document_id=*/0, Hit::kDefaultTermFrequency,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  Hit hit1 = CreateHit(/*last_hit=*/hit0, /*desired_byte_length=*/1);
  Hit hit2 = CreateHit(/*last_hit=*/hit1, /*desired_byte_length=*/1);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit0));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit1));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit2));

  // ----------------------
  // 23     delta(Hit #0)
  // 22     delta(Hit #1)
  // 21-18  Hit #2
  // 17-12  <unused>
  // 11-6   kSpecialHit
  // 5-0    Offset=18
  // ----------------------
  int bytes_used = 6;

  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(bytes_used));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(EqualsHit(hit2), EqualsHit(hit1),
                                       EqualsHit(hit0))));

  // Get the min size to fit for the hits in pl_used. Moving the hits in pl_used
  // into a posting list with this min size should make it ALMOST_FULL, which we
  // can see should have size = 14. This should not return the min posting list
  // size.
  // ----------------------
  // 13     delta(Hit #0)
  // 12     delta(Hit #1)
  // 11-6   Hit #2
  // 5-0    kSpecialHit
  // ----------------------
  int expected_min_size = 14;

  EXPECT_THAT(serializer.GetMinPostingListSizeToFit(&pl_used),
              Gt(serializer.GetMinPostingListSize()));
  EXPECT_THAT(serializer.GetMinPostingListSizeToFit(&pl_used),
              Eq(expected_min_size));
}

TEST(PostingListHitSerializerTest,
     GetMinPostingListToFitForAlmostFullAndFullPLReturnsSameSize) {
  PostingListHitSerializer serializer;

  // Size = 24
  int pl_size = 2 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  // Create and add some hits to make pl_used ALMOST_FULL
  Hit hit0(/*section_id=*/1, /*document_id=*/0, Hit::kDefaultTermFrequency,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  Hit hit1 = CreateHit(/*last_hit=*/hit0, /*desired_byte_length=*/2);
  Hit hit2 = CreateHit(/*last_hit=*/hit1, /*desired_byte_length=*/2);
  Hit hit3 = CreateHit(/*last_hit=*/hit2, /*desired_byte_length=*/2);
  Hit hit4 = CreateHit(/*last_hit=*/hit3, /*desired_byte_length=*/2);
  Hit hit5 = CreateHit(/*last_hit=*/hit4, /*desired_byte_length=*/2);
  Hit hit6 = CreateHit(/*last_hit=*/hit5, /*desired_byte_length=*/2);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit0));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit1));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit2));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit3));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit4));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit5));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit6));

  // ----------------------
  // 23-22     delta(Hit #0)
  // 21-20     delta(Hit #1)
  // 19-18     delta(Hit #2)
  // 17-16     delta(Hit #3)
  // 15-14     delta(Hit #4)
  // 13-12     delta(Hit #5)
  // 11-6      Hit #6
  // 5-0       kSpecialHit
  // ----------------------
  int bytes_used = 18;

  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(bytes_used));
  EXPECT_THAT(
      serializer.GetHits(&pl_used),
      IsOkAndHolds(ElementsAre(
          EqualsHit(hit6), EqualsHit(hit5), EqualsHit(hit4), EqualsHit(hit3),
          EqualsHit(hit2), EqualsHit(hit1), EqualsHit(hit0))));

  // GetMinPostingListSizeToFit should return the same size as pl_used.
  EXPECT_THAT(serializer.GetMinPostingListSizeToFit(&pl_used), Eq(pl_size));

  // Add another hit to make the posting list FULL
  Hit hit7 = CreateHit(/*last_hit=*/hit6, /*desired_byte_length=*/1);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit7));
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(pl_size));
  EXPECT_THAT(
      serializer.GetHits(&pl_used),
      IsOkAndHolds(ElementsAre(
          EqualsHit(hit7), EqualsHit(hit6), EqualsHit(hit5), EqualsHit(hit4),
          EqualsHit(hit3), EqualsHit(hit2), EqualsHit(hit1), EqualsHit(hit0))));

  // GetMinPostingListSizeToFit should still be the same size as pl_used.
  EXPECT_THAT(serializer.GetMinPostingListSizeToFit(&pl_used), Eq(pl_size));
}

TEST(PostingListHitSerializerTest, MoveFrom) {
  PostingListHitSerializer serializer;

  int pl_size = 3 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used1,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  Hit hit0(/*section_id=*/1, /*document_id=*/0, Hit::kDefaultTermFrequency,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  Hit hit1 = CreateHit(/*last_hit=*/hit0, /*desired_byte_length=*/1);
  Hit hit2 = CreateHit(/*last_hit=*/hit1, /*desired_byte_length=*/1);
  Hit hit3 = CreateHit(/*last_hit=*/hit2, /*desired_byte_length=*/1);
  Hit hit4 = CreateHit(/*last_hit=*/hit3, /*desired_byte_length=*/1);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit0));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit1));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit2));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit3));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit4));
  EXPECT_THAT(serializer.GetHits(&pl_used1),
              IsOkAndHolds(ElementsAre(EqualsHit(hit4), EqualsHit(hit3),
                                       EqualsHit(hit2), EqualsHit(hit1),
                                       EqualsHit(hit0))));

  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used2,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  Hit another_hit0(/*section_id=*/1, /*document_id=*/100,
                   Hit::kDefaultTermFrequency,
                   /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
                   /*is_stemmed_hit=*/false);
  Hit another_hit1 =
      CreateHit(/*last_hit=*/another_hit0, /*desired_byte_length=*/2);
  Hit another_hit2 =
      CreateHit(/*last_hit=*/another_hit1, /*desired_byte_length=*/2);
  Hit another_hit3 =
      CreateHit(/*last_hit=*/another_hit2, /*desired_byte_length=*/2);
  Hit another_hit4 =
      CreateHit(/*last_hit=*/another_hit3, /*desired_byte_length=*/2);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, another_hit0));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, another_hit1));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, another_hit2));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, another_hit3));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, another_hit4));

  ICING_ASSERT_OK(serializer.MoveFrom(/*dst=*/&pl_used2, /*src=*/&pl_used1));
  EXPECT_THAT(serializer.GetHits(&pl_used2),
              IsOkAndHolds(ElementsAre(EqualsHit(hit4), EqualsHit(hit3),
                                       EqualsHit(hit2), EqualsHit(hit1),
                                       EqualsHit(hit0))));
  EXPECT_THAT(serializer.GetHits(&pl_used1), IsOkAndHolds(IsEmpty()));
}

TEST(PostingListHitSerializerTest, MoveFromNullArgumentReturnsInvalidArgument) {
  PostingListHitSerializer serializer;

  int pl_size = 3 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used1,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  Hit hit0(/*section_id=*/1, /*document_id=*/0, Hit::kDefaultTermFrequency,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  Hit hit1 = CreateHit(/*last_hit=*/hit0, /*desired_byte_length=*/1);
  Hit hit2 = CreateHit(/*last_hit=*/hit1, /*desired_byte_length=*/1);
  Hit hit3 = CreateHit(/*last_hit=*/hit2, /*desired_byte_length=*/1);
  Hit hit4 = CreateHit(/*last_hit=*/hit3, /*desired_byte_length=*/1);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit0));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit1));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit2));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit3));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit4));

  EXPECT_THAT(serializer.MoveFrom(/*dst=*/&pl_used1, /*src=*/nullptr),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(serializer.GetHits(&pl_used1),
              IsOkAndHolds(ElementsAre(EqualsHit(hit4), EqualsHit(hit3),
                                       EqualsHit(hit2), EqualsHit(hit1),
                                       EqualsHit(hit0))));
}

TEST(PostingListHitSerializerTest,
     MoveFromInvalidPostingListReturnsInvalidArgument) {
  PostingListHitSerializer serializer;

  int pl_size = 3 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used1,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  Hit hit0(/*section_id=*/1, /*document_id=*/0, Hit::kDefaultTermFrequency,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  Hit hit1 = CreateHit(/*last_hit=*/hit0, /*desired_byte_length=*/1);
  Hit hit2 = CreateHit(/*last_hit=*/hit1, /*desired_byte_length=*/1);
  Hit hit3 = CreateHit(/*last_hit=*/hit2, /*desired_byte_length=*/1);
  Hit hit4 = CreateHit(/*last_hit=*/hit3, /*desired_byte_length=*/1);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit0));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit1));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit2));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit3));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit4));

  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used2,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  Hit another_hit0(/*section_id=*/1, /*document_id=*/100,
                   Hit::kDefaultTermFrequency,
                   /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
                   /*is_stemmed_hit=*/false);
  Hit another_hit1 =
      CreateHit(/*last_hit=*/another_hit0, /*desired_byte_length=*/2);
  Hit another_hit2 =
      CreateHit(/*last_hit=*/another_hit1, /*desired_byte_length=*/2);
  Hit another_hit3 =
      CreateHit(/*last_hit=*/another_hit2, /*desired_byte_length=*/2);
  Hit another_hit4 =
      CreateHit(/*last_hit=*/another_hit3, /*desired_byte_length=*/2);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, another_hit0));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, another_hit1));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, another_hit2));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, another_hit3));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, another_hit4));

  // Write invalid hits to the beginning of pl_used1 to make it invalid.
  Hit invalid_hit(Hit::kInvalidValue);
  memcpy(pl_used1.posting_list_buffer(), &invalid_hit, sizeof(invalid_hit));
  memcpy(pl_used1.posting_list_buffer() + sizeof(invalid_hit), &invalid_hit,
         sizeof(invalid_hit));

  // MoveFrom should return error, and pl_used2 should be unchanged.
  EXPECT_THAT(serializer.MoveFrom(/*dst=*/&pl_used2, /*src=*/&pl_used1),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(
      serializer.GetHits(&pl_used2),
      IsOkAndHolds(ElementsAre(EqualsHit(another_hit4), EqualsHit(another_hit3),
                               EqualsHit(another_hit2), EqualsHit(another_hit1),
                               EqualsHit(another_hit0))));
}

TEST(PostingListHitSerializerTest,
     MoveToInvalidPostingListReturnsFailedPrecondition) {
  PostingListHitSerializer serializer;

  int pl_size = 3 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used1,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  Hit hit0(/*section_id=*/1, /*document_id=*/0, Hit::kDefaultTermFrequency,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  Hit hit1 = CreateHit(/*last_hit=*/hit0, /*desired_byte_length=*/1);
  Hit hit2 = CreateHit(/*last_hit=*/hit1, /*desired_byte_length=*/1);
  Hit hit3 = CreateHit(/*last_hit=*/hit2, /*desired_byte_length=*/1);
  Hit hit4 = CreateHit(/*last_hit=*/hit3, /*desired_byte_length=*/1);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit0));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit1));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit2));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit3));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit4));

  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used2,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  Hit another_hit0(/*section_id=*/1, /*document_id=*/100,
                   Hit::kDefaultTermFrequency,
                   /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
                   /*is_stemmed_hit=*/false);
  Hit another_hit1 =
      CreateHit(/*last_hit=*/another_hit0, /*desired_byte_length=*/2);
  Hit another_hit2 =
      CreateHit(/*last_hit=*/another_hit1, /*desired_byte_length=*/2);
  Hit another_hit3 =
      CreateHit(/*last_hit=*/another_hit2, /*desired_byte_length=*/2);
  Hit another_hit4 =
      CreateHit(/*last_hit=*/another_hit3, /*desired_byte_length=*/2);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, another_hit0));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, another_hit1));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, another_hit2));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, another_hit3));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, another_hit4));

  // Write invalid hits to the beginning of pl_used2 to make it invalid.
  Hit invalid_hit(Hit::kInvalidValue);
  memcpy(pl_used2.posting_list_buffer(), &invalid_hit, sizeof(invalid_hit));
  memcpy(pl_used2.posting_list_buffer() + sizeof(invalid_hit), &invalid_hit,
         sizeof(invalid_hit));

  // MoveFrom should return error, and pl_used1 should be unchanged.
  EXPECT_THAT(serializer.MoveFrom(/*dst=*/&pl_used2, /*src=*/&pl_used1),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(serializer.GetHits(&pl_used1),
              IsOkAndHolds(ElementsAre(EqualsHit(hit4), EqualsHit(hit3),
                                       EqualsHit(hit2), EqualsHit(hit1),
                                       EqualsHit(hit0))));
}

TEST(PostingListHitSerializerTest, MoveToPostingListTooSmall) {
  PostingListHitSerializer serializer;

  int pl_size = 3 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used1,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  Hit hit0(/*section_id=*/1, /*document_id=*/0, Hit::kDefaultTermFrequency,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  Hit hit1 = CreateHit(/*last_hit=*/hit0, /*desired_byte_length=*/1);
  Hit hit2 = CreateHit(/*last_hit=*/hit1, /*desired_byte_length=*/1);
  Hit hit3 = CreateHit(/*last_hit=*/hit2, /*desired_byte_length=*/1);
  Hit hit4 = CreateHit(/*last_hit=*/hit3, /*desired_byte_length=*/1);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit0));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit1));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit2));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit3));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit4));

  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used2,
      PostingListUsed::CreateFromUnitializedRegion(
          &serializer, serializer.GetMinPostingListSize()));
  Hit another_hit(/*section_id=*/1, /*document_id=*/100,
                  Hit::kDefaultTermFrequency,
                  /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
                  /*is_stemmed_hit=*/false);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, another_hit));

  EXPECT_THAT(serializer.MoveFrom(/*dst=*/&pl_used2, /*src=*/&pl_used1),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(serializer.GetHits(&pl_used1),
              IsOkAndHolds(ElementsAre(EqualsHit(hit4), EqualsHit(hit3),
                                       EqualsHit(hit2), EqualsHit(hit1),
                                       EqualsHit(hit0))));
  EXPECT_THAT(serializer.GetHits(&pl_used2),
              IsOkAndHolds(ElementsAre(EqualsHit(another_hit))));
}

TEST(PostingListHitSerializerTest, PopHitsWithTermFrequenciesAndFlags) {
  PostingListHitSerializer serializer;

  // Size = 24
  int pl_size = 2 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));

  // This posting list is 24-bytes. Create four hits that will have deltas of
  // two bytes each and all of whom will have a non-default term-frequency. This
  // posting list will be almost_full.
  //
  // ----------------------
  // 23     term-frequency(Hit #0)
  // 22     flags(Hit #0)
  // 21-20  delta(Hit #0)
  // 19     term-frequency(Hit #1)
  // 18     flags(Hit #1)
  // 17-16  delta(Hit #1)
  // 15     term-frequency(Hit #2)
  // 14     flags(Hit #2)
  // 13-12  delta(Hit #2)
  // 11-6   Hit #3
  // 5-0    kInvalidHit
  // ----------------------
  int bytes_used = 18;

  Hit hit0(/*section_id=*/0, /*document_id=*/0, /*term_frequency=*/5,
           /*is_in_prefix_section=*/false, /*is_prefix_hit=*/false,
           /*is_stemmed_hit=*/false);
  Hit hit1 = CreateHit(hit0, /*desired_byte_length=*/2);
  Hit hit2 = CreateHit(hit1, /*desired_byte_length=*/2);
  Hit hit3 = CreateHit(hit2, /*desired_byte_length=*/2);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit0));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit1));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit2));
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit3));

  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Hit> hits_out,
                             serializer.GetHits(&pl_used));
  EXPECT_THAT(hits_out, ElementsAre(EqualsHit(hit3), EqualsHit(hit2),
                                    EqualsHit(hit1), EqualsHit(hit0)));
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(bytes_used));

  // Now, pop the last hit. The posting list should contain the first three
  // hits.
  //
  // ----------------------
  // 23     term-frequency(Hit #0)
  // 22     flags(Hit #0)
  // 21-20  delta(Hit #0)
  // 19     term-frequency(Hit #1)
  // 18     flags(Hit #1)
  // 17-16  delta(Hit #1)
  // 15-12  <unused>
  // 11-6   Hit #2
  // 5-0    kInvalidHit
  // ----------------------
  ICING_ASSERT_OK(serializer.PopFrontHits(&pl_used, 1));
  ICING_ASSERT_OK_AND_ASSIGN(hits_out, serializer.GetHits(&pl_used));
  EXPECT_THAT(hits_out,
              ElementsAre(EqualsHit(hit2), EqualsHit(hit1), EqualsHit(hit0)));
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(bytes_used));
}

}  // namespace

}  // namespace lib
}  // namespace icing
