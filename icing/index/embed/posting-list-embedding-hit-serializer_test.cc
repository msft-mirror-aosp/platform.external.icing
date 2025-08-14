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

#include "icing/index/embed/posting-list-embedding-hit-serializer.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <iterator>
#include <limits>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/file/posting_list/posting-list-used.h"
#include "icing/index/embed/embedding-hit.h"
#include "icing/index/hit/hit.h"
#include "icing/legacy/index/icing-bit-util.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/hit-test-utils.h"

using testing::ElementsAre;
using testing::ElementsAreArray;
using testing::Eq;
using testing::IsEmpty;
using testing::Le;
using testing::Lt;

namespace icing {
namespace lib {

namespace {

struct HitElt {
  HitElt() = default;
  explicit HitElt(const EmbeddingHit &hit_in) : hit(hit_in) {}

  static EmbeddingHit get_hit(const HitElt &hit_elt) { return hit_elt.hit; }

  EmbeddingHit hit;
};

TEST(PostingListEmbeddingHitSerializerTest, PostingListUsedPrependHitNotFull) {
  PostingListEmbeddingHitSerializer serializer;

  static const int kNumHits = 2551;
  static const size_t kHitsSize = kNumHits * sizeof(EmbeddingHit);

  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, kHitsSize));

  // Make used.
  EmbeddingHit hit0(BasicHit(/*section_id=*/0, /*document_id=*/0),
                    /*location=*/0);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit0));
  int expected_size = sizeof(EmbeddingHit::Value);
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used), IsOkAndHolds(ElementsAre(hit0)));

  EmbeddingHit hit1(BasicHit(/*section_id=*/0, /*document_id=*/1),
                    /*location=*/1);
  uint64_t delta = hit0.value() - hit1.value();
  uint8_t delta_buf[VarInt::kMaxEncodedLen64];
  size_t delta_len = VarInt::Encode(delta, delta_buf);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit1));
  expected_size += delta_len;
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(hit1, hit0)));

  EmbeddingHit hit2(BasicHit(/*section_id=*/0, /*document_id=*/2),
                    /*location=*/2);
  delta = hit1.value() - hit2.value();
  delta_len = VarInt::Encode(delta, delta_buf);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit2));
  expected_size += delta_len;
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(hit2, hit1, hit0)));

  EmbeddingHit hit3(BasicHit(/*section_id=*/0, /*document_id=*/3),
                    /*location=*/3);
  delta = hit2.value() - hit3.value();
  delta_len = VarInt::Encode(delta, delta_buf);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit3));
  expected_size += delta_len;
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(hit3, hit2, hit1, hit0)));
}

TEST(PostingListEmbeddingHitSerializerTest,
     PostingListUsedPrependHitAlmostFull) {
  PostingListEmbeddingHitSerializer serializer;

  // Size = 32
  int pl_size = 2 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));

  // Fill up the compressed region.
  // Transitions:
  // Adding hit0: EMPTY -> NOT_FULL
  // Adding hit1: NOT_FULL -> NOT_FULL
  // Adding hit2: NOT_FULL -> NOT_FULL
  EmbeddingHit hit0(BasicHit(/*section_id=*/0, /*document_id=*/0),
                    /*location=*/1);
  EmbeddingHit hit1 = CreateEmbeddingHit(hit0, /*desired_byte_length=*/3);
  EmbeddingHit hit2 = CreateEmbeddingHit(hit1, /*desired_byte_length=*/3);
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit0));
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit1));
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit2));
  // Size used will be 8 (hit2) + 3 (hit1-hit2) + 3 (hit0-hit1) = 14 bytes
  int expected_size = sizeof(EmbeddingHit) + 3 + 3;
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(hit2, hit1, hit0)));

  // Add one more hit to transition NOT_FULL -> ALMOST_FULL
  EmbeddingHit hit3 = CreateEmbeddingHit(hit2, /*desired_byte_length=*/3);
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit3));
  // Storing them in the compressed region requires 8 (hit) + 3 (hit2-hit3) +
  // 3 (hit1-hit2) + 3 (hit0-hit1) = 17 bytes, but there are only 16 bytes in
  // the compressed region. So instead, the posting list will transition to
  // ALMOST_FULL. The in-use compressed region will actually shrink from 14
  // bytes to 9 bytes because the uncompressed version of hit2 will be
  // overwritten with the compressed delta of hit2. hit3 will be written to one
  // of the special hits. Because we're in ALMOST_FULL, the expected size is the
  // size of the pl minus the one hit used to mark the posting list as
  // ALMOST_FULL.
  expected_size = pl_size - sizeof(EmbeddingHit);
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(hit3, hit2, hit1, hit0)));

  // Add one more hit to transition ALMOST_FULL -> ALMOST_FULL
  EmbeddingHit hit4 = CreateEmbeddingHit(hit3, /*desired_byte_length=*/6);
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit4));
  // There are currently 9 bytes in use in the compressed region. Hit3 will
  // have a 6-byte delta, which fits in the compressed region. Hit3 will be
  // moved from the special hit to the compressed region (which will have 15
  // bytes in use after adding hit3). Hit4 will be placed in one of the special
  // hits and the posting list will remain in ALMOST_FULL.
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(hit4, hit3, hit2, hit1, hit0)));

  // Add one more hit to transition ALMOST_FULL -> FULL
  EmbeddingHit hit5 = CreateEmbeddingHit(hit4, /*desired_byte_length=*/2);
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit5));
  // There are currently 15 bytes in use in the compressed region. Hit4 will
  // have a 2-byte delta which will not fit in the compressed region. So hit4
  // will remain in one of the special hits and hit5 will occupy the other,
  // making the posting list FULL.
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(pl_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(hit5, hit4, hit3, hit2, hit1, hit0)));

  // The posting list is FULL. Adding another hit should fail.
  EmbeddingHit hit6 = CreateEmbeddingHit(hit5, /*desired_byte_length=*/1);
  EXPECT_THAT(serializer.PrependHit(&pl_used, hit6),
              StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
}

TEST(PostingListEmbeddingHitSerializerTest, PostingListUsedMinSize) {
  PostingListEmbeddingHitSerializer serializer;

  // Min size = 16
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used,
      PostingListUsed::CreateFromUnitializedRegion(
          &serializer, serializer.GetMinPostingListSize()));
  // PL State: EMPTY
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(0));
  EXPECT_THAT(serializer.GetHits(&pl_used), IsOkAndHolds(IsEmpty()));

  // Add a hit, PL should shift to ALMOST_FULL state
  EmbeddingHit hit0(BasicHit(/*section_id=*/1, /*document_id=*/0),
                    /*location=*/1);
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit0));
  // Size = sizeof(uncompressed hit0)
  int expected_size = sizeof(EmbeddingHit);
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used), IsOkAndHolds(ElementsAre(hit0)));

  // Add the smallest hit possible with a delta of 0b1. PL should shift to FULL
  // state.
  EmbeddingHit hit1(BasicHit(/*section_id=*/1, /*document_id=*/0),
                    /*location=*/0);
  ICING_EXPECT_OK(serializer.PrependHit(&pl_used, hit1));
  // Size = sizeof(uncompressed hit1) + sizeof(uncompressed hit0)
  expected_size += sizeof(EmbeddingHit);
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(hit1, hit0)));

  // Try to add the smallest hit possible. Should fail
  EmbeddingHit hit2(BasicHit(/*section_id=*/0, /*document_id=*/0),
                    /*location=*/0);
  EXPECT_THAT(serializer.PrependHit(&pl_used, hit2),
              StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(expected_size));
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAre(hit1, hit0)));
}

TEST(PostingListEmbeddingHitSerializerTest,
     PostingListPrependHitArrayMinSizePostingList) {
  PostingListEmbeddingHitSerializer serializer;

  // Min Size = 16
  int pl_size = serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));

  std::vector<HitElt> hits_in;
  hits_in.emplace_back(EmbeddingHit(
      BasicHit(/*section_id=*/1, /*document_id=*/0), /*location=*/1));
  hits_in.emplace_back(
      CreateEmbeddingHit(hits_in.rbegin()->hit, /*desired_byte_length=*/1));
  hits_in.emplace_back(
      CreateEmbeddingHit(hits_in.rbegin()->hit, /*desired_byte_length=*/1));
  hits_in.emplace_back(
      CreateEmbeddingHit(hits_in.rbegin()->hit, /*desired_byte_length=*/1));
  hits_in.emplace_back(
      CreateEmbeddingHit(hits_in.rbegin()->hit, /*desired_byte_length=*/1));
  std::reverse(hits_in.begin(), hits_in.end());

  // Add five hits. The PL is in the empty state and an empty min size PL can
  // only fit two hits. So PrependHitArray should fail.
  ICING_ASSERT_OK_AND_ASSIGN(
      uint32_t num_can_prepend,
      (serializer.PrependHitArray<HitElt, HitElt::get_hit>(
          &pl_used, &hits_in[0], hits_in.size(), /*keep_prepended=*/false)));
  EXPECT_THAT(num_can_prepend, Eq(2));

  int can_fit_hits = num_can_prepend;
  // The PL has room for 2 hits. We should be able to add them without any
  // problem, transitioning the PL from EMPTY -> ALMOST_FULL -> FULL
  const HitElt *hits_in_ptr = hits_in.data() + (hits_in.size() - 2);
  ICING_ASSERT_OK_AND_ASSIGN(
      num_can_prepend,
      (serializer.PrependHitArray<HitElt, HitElt::get_hit>(
          &pl_used, hits_in_ptr, can_fit_hits, /*keep_prepended=*/false)));
  EXPECT_THAT(num_can_prepend, Eq(can_fit_hits));
  EXPECT_THAT(pl_size, Eq(serializer.GetBytesUsed(&pl_used)));
  std::deque<EmbeddingHit> hits_pushed;
  std::transform(hits_in.rbegin(),
                 hits_in.rend() - hits_in.size() + can_fit_hits,
                 std::front_inserter(hits_pushed), HitElt::get_hit);
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAreArray(hits_pushed)));
}

TEST(PostingListEmbeddingHitSerializerTest,
     PostingListPrependHitArrayPostingList) {
  PostingListEmbeddingHitSerializer serializer;

  // Size = 48
  int pl_size = 3 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));

  std::vector<HitElt> hits_in;
  hits_in.emplace_back(EmbeddingHit(
      BasicHit(/*section_id=*/1, /*document_id=*/0), /*location=*/1));
  hits_in.emplace_back(
      CreateEmbeddingHit(hits_in.rbegin()->hit, /*desired_byte_length=*/1));
  hits_in.emplace_back(
      CreateEmbeddingHit(hits_in.rbegin()->hit, /*desired_byte_length=*/1));
  hits_in.emplace_back(
      CreateEmbeddingHit(hits_in.rbegin()->hit, /*desired_byte_length=*/1));
  hits_in.emplace_back(
      CreateEmbeddingHit(hits_in.rbegin()->hit, /*desired_byte_length=*/1));
  std::reverse(hits_in.begin(), hits_in.end());
  // The last hit is uncompressed and the four before it should only take one
  // byte. Total use = 8 bytes.
  // ----------------------
  // 47     delta(EmbeddingHit #0)
  // 46     delta(EmbeddingHit #1)
  // 45     delta(EmbeddingHit #2)
  // 44     delta(EmbeddingHit #3)
  // 43-36  EmbeddingHit #4
  // 35-16  <unused>
  // 15-8   kSpecialHit
  // 7-0    Offset=36
  // ----------------------
  int byte_size = sizeof(EmbeddingHit::Value) + hits_in.size() - 1;

  // Add five hits. The PL is in the empty state and should be able to fit all
  // five hits without issue, transitioning the PL from EMPTY -> NOT_FULL.
  ICING_ASSERT_OK_AND_ASSIGN(
      uint32_t num_could_fit,
      (serializer.PrependHitArray<HitElt, HitElt::get_hit>(
          &pl_used, &hits_in[0], hits_in.size(), /*keep_prepended=*/false)));
  EXPECT_THAT(num_could_fit, Eq(hits_in.size()));
  EXPECT_THAT(byte_size, Eq(serializer.GetBytesUsed(&pl_used)));
  std::deque<EmbeddingHit> hits_pushed;
  std::transform(hits_in.rbegin(), hits_in.rend(),
                 std::front_inserter(hits_pushed), HitElt::get_hit);
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAreArray(hits_pushed)));

  EmbeddingHit first_hit =
      CreateEmbeddingHit(hits_in.begin()->hit, /*desired_byte_length=*/1);
  hits_in.clear();
  hits_in.emplace_back(first_hit);
  hits_in.emplace_back(
      CreateEmbeddingHit(hits_in.rbegin()->hit, /*desired_byte_length=*/2));
  hits_in.emplace_back(
      CreateEmbeddingHit(hits_in.rbegin()->hit, /*desired_byte_length=*/1));
  hits_in.emplace_back(
      CreateEmbeddingHit(hits_in.rbegin()->hit, /*desired_byte_length=*/2));
  hits_in.emplace_back(
      CreateEmbeddingHit(hits_in.rbegin()->hit, /*desired_byte_length=*/3));
  hits_in.emplace_back(
      CreateEmbeddingHit(hits_in.rbegin()->hit, /*desired_byte_length=*/2));
  hits_in.emplace_back(
      CreateEmbeddingHit(hits_in.rbegin()->hit, /*desired_byte_length=*/3));
  std::reverse(hits_in.begin(), hits_in.end());
  // Size increased by the deltas of these hits (1+2+1+2+3+2+3) = 14 bytes
  // ----------------------
  // 47     delta(EmbeddingHit #0)
  // 46     delta(EmbeddingHit #1)
  // 45     delta(EmbeddingHit #2)
  // 44     delta(EmbeddingHit #3)
  // 43     delta(EmbeddingHit #4)
  // 42-41  delta(EmbeddingHit #5)
  // 40     delta(EmbeddingHit #6)
  // 39-38  delta(EmbeddingHit #7)
  // 37-35  delta(EmbeddingHit #8)
  // 34-33  delta(EmbeddingHit #9)
  // 32-30  delta(EmbeddingHit #10)
  // 29-22  EmbeddingHit #11
  // 21-16  <unused>
  // 15-8   kSpecialHit
  // 7-0    Offset=22
  // ----------------------
  byte_size += 14;

  // Add these 7 hits. The PL is currently in the NOT_FULL state and should
  // remain in the NOT_FULL state.
  ICING_ASSERT_OK_AND_ASSIGN(
      num_could_fit,
      (serializer.PrependHitArray<HitElt, HitElt::get_hit>(
          &pl_used, &hits_in[0], hits_in.size(), /*keep_prepended=*/false)));
  EXPECT_THAT(num_could_fit, Eq(hits_in.size()));
  EXPECT_THAT(byte_size, Eq(serializer.GetBytesUsed(&pl_used)));
  // All hits from hits_in were added.
  std::transform(hits_in.rbegin(), hits_in.rend(),
                 std::front_inserter(hits_pushed), HitElt::get_hit);
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAreArray(hits_pushed)));

  first_hit =
      CreateEmbeddingHit(hits_in.begin()->hit, /*desired_byte_length=*/8);
  hits_in.clear();
  hits_in.emplace_back(first_hit);
  // ----------------------
  // 47     delta(EmbeddingHit #0)
  // 46     delta(EmbeddingHit #1)
  // 45     delta(EmbeddingHit #2)
  // 44     delta(EmbeddingHit #3)
  // 43     delta(EmbeddingHit #4)
  // 42-41  delta(EmbeddingHit #5)
  // 40     delta(EmbeddingHit #6)
  // 39-38  delta(EmbeddingHit #7)
  // 37-35  delta(EmbeddingHit #8)
  // 34-33  delta(EmbeddingHit #9)
  // 32-30  delta(EmbeddingHit #10)
  // 29-22  delta(EmbeddingHit #11)
  // 21-16  <unused>
  // 15-8   EmbeddingHit #12
  // 7-0    kSpecialHit
  // ----------------------
  byte_size = 40;  // 48 - 8

  // Add this 1 hit. The PL is currently in the NOT_FULL state and should
  // transition to the ALMOST_FULL state - even though there is still some
  // unused space.
  ICING_ASSERT_OK_AND_ASSIGN(
      num_could_fit,
      (serializer.PrependHitArray<HitElt, HitElt::get_hit>(
          &pl_used, &hits_in[0], hits_in.size(), /*keep_prepended=*/false)));
  EXPECT_THAT(num_could_fit, Eq(hits_in.size()));
  EXPECT_THAT(byte_size, Eq(serializer.GetBytesUsed(&pl_used)));
  // All hits from hits_in were added.
  std::transform(hits_in.rbegin(), hits_in.rend(),
                 std::front_inserter(hits_pushed), HitElt::get_hit);
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAreArray(hits_pushed)));

  first_hit =
      CreateEmbeddingHit(hits_in.begin()->hit, /*desired_byte_length=*/5);
  hits_in.clear();
  hits_in.emplace_back(first_hit);
  hits_in.emplace_back(
      CreateEmbeddingHit(hits_in.rbegin()->hit, /*desired_byte_length=*/3));
  std::reverse(hits_in.begin(), hits_in.end());
  // ----------------------
  // 47     delta(EmbeddingHit #0)
  // 46     delta(EmbeddingHit #1)
  // 45     delta(EmbeddingHit #2)
  // 44     delta(EmbeddingHit #3)
  // 43     delta(EmbeddingHit #4)
  // 42-41  delta(EmbeddingHit #5)
  // 40     delta(EmbeddingHit #6)
  // 39-38  delta(EmbeddingHit #7)
  // 37-35  delta(EmbeddingHit #8)
  // 34-33  delta(EmbeddingHit #9)
  // 32-30  delta(EmbeddingHit #10)
  // 29-22  delta(EmbeddingHit #11)
  // 21-17  delta(EmbeddingHit #12)
  // 16     <unused>
  // 15-8   EmbeddingHit #13
  // 7-0    EmbeddingHit #14
  // ----------------------

  // Add these 2 hits.
  // - The PL is currently in the ALMOST_FULL state. Adding the first hit should
  //   keep the PL in ALMOST_FULL because the delta between
  //   EmbeddingHit #12 and EmbeddingHit #13 (5 byte) can fit in the unused area
  //   (6 bytes).
  // - Adding the second hit should transition to the FULL state because the
  //   delta between EmbeddingHit #13 and EmbeddingHit #14 (3 bytes) is larger
  //   than the remaining unused area (1 byte).
  ICING_ASSERT_OK_AND_ASSIGN(
      num_could_fit,
      (serializer.PrependHitArray<HitElt, HitElt::get_hit>(
          &pl_used, &hits_in[0], hits_in.size(), /*keep_prepended=*/false)));
  EXPECT_THAT(num_could_fit, Eq(hits_in.size()));
  EXPECT_THAT(pl_size, Eq(serializer.GetBytesUsed(&pl_used)));
  // All hits from hits_in were added.
  std::transform(hits_in.rbegin(), hits_in.rend(),
                 std::front_inserter(hits_pushed), HitElt::get_hit);
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAreArray(hits_pushed)));
}

TEST(PostingListEmbeddingHitSerializerTest,
     PostingListPrependHitArrayTooManyHits) {
  PostingListEmbeddingHitSerializer serializer;

  static constexpr int kNumHits = 130;
  static constexpr int kDeltaSize = 1;
  static constexpr size_t kHitsSize =
      ((kNumHits - 2) * kDeltaSize + (2 * sizeof(EmbeddingHit)));

  // Create an array with one too many hits
  std::vector<HitElt> hit_elts_in_too_many;
  hit_elts_in_too_many.emplace_back(EmbeddingHit(
      BasicHit(/*section_id=*/0, /*document_id=*/0), /*location=*/0));
  for (int i = 0; i < kNumHits; ++i) {
    hit_elts_in_too_many.emplace_back(CreateEmbeddingHit(
        hit_elts_in_too_many.back().hit, /*desired_byte_length=*/1));
  }
  // Reverse so that hits are inserted in descending order
  std::reverse(hit_elts_in_too_many.begin(), hit_elts_in_too_many.end());

  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used,
      PostingListUsed::CreateFromUnitializedRegion(
          &serializer, serializer.GetMinPostingListSize()));
  // PrependHitArray should fail because hit_elts_in_too_many is far too large
  // for the minimum size pl.
  ICING_ASSERT_OK_AND_ASSIGN(
      uint32_t num_could_fit,
      (serializer.PrependHitArray<HitElt, HitElt::get_hit>(
          &pl_used, &hit_elts_in_too_many[0], hit_elts_in_too_many.size(),
          /*keep_prepended=*/false)));
  ASSERT_THAT(num_could_fit, Eq(2));
  ASSERT_THAT(num_could_fit, Lt(hit_elts_in_too_many.size()));
  ASSERT_THAT(serializer.GetBytesUsed(&pl_used), Eq(0));
  ASSERT_THAT(serializer.GetHits(&pl_used), IsOkAndHolds(IsEmpty()));

  ICING_ASSERT_OK_AND_ASSIGN(
      pl_used,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, kHitsSize));
  // PrependHitArray should fail because hit_elts_in_too_many is one hit too
  // large for this pl.
  ICING_ASSERT_OK_AND_ASSIGN(
      num_could_fit,
      (serializer.PrependHitArray<HitElt, HitElt::get_hit>(
          &pl_used, &hit_elts_in_too_many[0], hit_elts_in_too_many.size(),
          /*keep_prepended=*/false)));
  ASSERT_THAT(num_could_fit, Eq(hit_elts_in_too_many.size() - 1));
  ASSERT_THAT(serializer.GetBytesUsed(&pl_used), Eq(0));
  ASSERT_THAT(serializer.GetHits(&pl_used), IsOkAndHolds(IsEmpty()));
}

TEST(PostingListEmbeddingHitSerializerTest,
     PostingListStatusJumpFromNotFullToFullAndBack) {
  PostingListEmbeddingHitSerializer serializer;

  // Size = 24
  const uint32_t pl_size = 3 * sizeof(EmbeddingHit);
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));

  EmbeddingHit max_valued_hit(
      BasicHit(/*section_id=*/kMaxSectionId, /*document_id=*/kMinDocumentId),
      /*location=*/std::numeric_limits<uint32_t>::max());
  ICING_ASSERT_OK(serializer.PrependHit(&pl, max_valued_hit));
  uint32_t bytes_used = serializer.GetBytesUsed(&pl);
  ASSERT_THAT(bytes_used, sizeof(EmbeddingHit));
  // Status not full.
  ASSERT_THAT(
      bytes_used,
      Le(pl_size - PostingListEmbeddingHitSerializer::kSpecialHitsSize));

  EmbeddingHit min_valued_hit(
      BasicHit(/*section_id=*/kMinSectionId, /*document_id=*/kMaxDocumentId),
      /*location=*/0);
  ICING_ASSERT_OK(serializer.PrependHit(&pl, min_valued_hit));
  EXPECT_THAT(serializer.GetHits(&pl),
              IsOkAndHolds(ElementsAre(min_valued_hit, max_valued_hit)));
  // Status should jump to full directly.
  ASSERT_THAT(serializer.GetBytesUsed(&pl), Eq(pl_size));
  ICING_ASSERT_OK(serializer.PopFrontHits(&pl, 1));
  EXPECT_THAT(serializer.GetHits(&pl),
              IsOkAndHolds(ElementsAre(max_valued_hit)));
  // Status should return to not full as before.
  ASSERT_THAT(serializer.GetBytesUsed(&pl), Eq(bytes_used));
}

TEST(PostingListEmbeddingHitSerializerTest, DeltaOverflow) {
  PostingListEmbeddingHitSerializer serializer;

  const uint32_t pl_size = 4 * sizeof(EmbeddingHit);
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));

  static const EmbeddingHit::Value kMaxHitValue =
      std::numeric_limits<EmbeddingHit::Value>::max();
  static const EmbeddingHit::Value kOverflow[4] = {
      kMaxHitValue >> 2,
      (kMaxHitValue >> 2) * 2,
      (kMaxHitValue >> 2) * 3,
      kMaxHitValue - 1,
  };

  // Fit at least 4 ordinary values.
  std::deque<EmbeddingHit> hits_pushed;
  for (EmbeddingHit::Value v = 0; v < 4; v++) {
    hits_pushed.push_front(
        EmbeddingHit(BasicHit(kMaxSectionId, kMaxDocumentId), 4 - v));
    ICING_EXPECT_OK(serializer.PrependHit(&pl, hits_pushed.front()));
    EXPECT_THAT(serializer.GetHits(&pl),
                IsOkAndHolds(ElementsAreArray(hits_pushed)));
  }

  // Cannot fit 4 overflow values.
  hits_pushed.clear();
  ICING_ASSERT_OK_AND_ASSIGN(
      pl, PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  for (int i = 3; i >= 1; i--) {
    hits_pushed.push_front(EmbeddingHit(/*value=*/kOverflow[i]));
    ICING_EXPECT_OK(serializer.PrependHit(&pl, hits_pushed.front()));
    EXPECT_THAT(serializer.GetHits(&pl),
                IsOkAndHolds(ElementsAreArray(hits_pushed)));
  }
  EXPECT_THAT(serializer.PrependHit(&pl, EmbeddingHit(/*value=*/kOverflow[0])),
              StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
}

TEST(PostingListEmbeddingHitSerializerTest,
     GetMinPostingListToFitForNotFullPL) {
  PostingListEmbeddingHitSerializer serializer;

  // Size = 64
  int pl_size = 4 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  // Create and add some hits to make pl_used NOT_FULL
  std::vector<EmbeddingHit> hits_in =
      CreateEmbeddingHits(/*num_hits=*/5, /*desired_byte_length=*/2);
  for (const EmbeddingHit &hit : hits_in) {
    ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit));
  }
  // ----------------------
  // 63-62     delta(EmbeddingHit #0)
  // 61-60     delta(EmbeddingHit #1)
  // 59-58     delta(EmbeddingHit #2)
  // 57-56     delta(EmbeddingHit #3)
  // 55-48     EmbeddingHit #5
  // 47-16     <unused>
  // 15-8      kSpecialHit
  // 7-0       Offset=48
  // ----------------------
  int bytes_used = 16;

  // Check that all hits have been inserted
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(bytes_used));
  std::deque<EmbeddingHit> hits_pushed(hits_in.rbegin(), hits_in.rend());
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAreArray(hits_pushed)));

  // Get the min size to fit for the hits in pl_used. Moving the hits in pl_used
  // into a posting list with this min size should make it ALMOST_FULL, which we
  // can see should have size = 24.
  // ----------------------
  // 23-22     delta(EmbeddingHit #0)
  // 21-20     delta(EmbeddingHit #1)
  // 19-18     delta(EmbeddingHit #2)
  // 17-16     delta(EmbeddingHit #3)
  // 15-8      EmbeddingHit #4
  // 7-0       kSpecialHit
  // ----------------------
  int expected_min_size = 24;
  uint32_t min_size_to_fit = serializer.GetMinPostingListSizeToFit(&pl_used);
  EXPECT_THAT(min_size_to_fit, Eq(expected_min_size));

  // Also check that this min size to fit posting list actually does fit all the
  // hits and can only hit one more hit in the ALMOST_FULL state.
  ICING_ASSERT_OK_AND_ASSIGN(PostingListUsed min_size_to_fit_pl,
                             PostingListUsed::CreateFromUnitializedRegion(
                                 &serializer, min_size_to_fit));
  for (const EmbeddingHit &hit : hits_in) {
    ICING_ASSERT_OK(serializer.PrependHit(&min_size_to_fit_pl, hit));
  }

  // Adding another hit to the min-size-to-fit posting list should succeed
  EmbeddingHit hit =
      CreateEmbeddingHit(hits_in.back(), /*desired_byte_length=*/1);
  ICING_ASSERT_OK(serializer.PrependHit(&min_size_to_fit_pl, hit));
  // Adding any other hits should fail with RESOURCE_EXHAUSTED error.
  EXPECT_THAT(
      serializer.PrependHit(&min_size_to_fit_pl,
                            CreateEmbeddingHit(hit, /*desired_byte_length=*/1)),
      StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));

  // Check that all hits have been inserted and the min-fit posting list is now
  // FULL.
  EXPECT_THAT(serializer.GetBytesUsed(&min_size_to_fit_pl),
              Eq(min_size_to_fit));
  hits_pushed.emplace_front(hit);
  EXPECT_THAT(serializer.GetHits(&min_size_to_fit_pl),
              IsOkAndHolds(ElementsAreArray(hits_pushed)));
}

TEST(PostingListEmbeddingHitSerializerTest,
     GetMinPostingListToFitForAlmostFullAndFullPLReturnsSameSize) {
  PostingListEmbeddingHitSerializer serializer;

  int pl_size = 24;
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  // Create and add some hits to make pl_used ALMOST_FULL
  std::vector<EmbeddingHit> hits_in =
      CreateEmbeddingHits(/*num_hits=*/5, /*desired_byte_length=*/2);
  for (const EmbeddingHit &hit : hits_in) {
    ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit));
  }
  // ----------------------
  // 23-22     delta(EmbeddingHit #0)
  // 21-20     delta(EmbeddingHit #1)
  // 19-18     delta(EmbeddingHit #2)
  // 17-16     delta(EmbeddingHit #3)
  // 15-8      EmbeddingHit #4
  // 7-0       kSpecialHit
  // ----------------------
  int bytes_used = 16;

  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(bytes_used));
  std::deque<EmbeddingHit> hits_pushed(hits_in.rbegin(), hits_in.rend());
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAreArray(hits_pushed)));

  // GetMinPostingListSizeToFit should return the same size as pl_used.
  uint32_t min_size_to_fit = serializer.GetMinPostingListSizeToFit(&pl_used);
  EXPECT_THAT(min_size_to_fit, Eq(pl_size));

  // Add another hit to make the posting list FULL
  EmbeddingHit hit =
      CreateEmbeddingHit(hits_in.back(), /*desired_byte_length=*/1);
  ICING_ASSERT_OK(serializer.PrependHit(&pl_used, hit));
  EXPECT_THAT(serializer.GetBytesUsed(&pl_used), Eq(pl_size));
  hits_pushed.emplace_front(hit);
  EXPECT_THAT(serializer.GetHits(&pl_used),
              IsOkAndHolds(ElementsAreArray(hits_pushed)));

  // GetMinPostingListSizeToFit should still be the same size as pl_used.
  min_size_to_fit = serializer.GetMinPostingListSizeToFit(&pl_used);
  EXPECT_THAT(min_size_to_fit, Eq(pl_size));
}

TEST(PostingListEmbeddingHitSerializerTest, MoveFrom) {
  PostingListEmbeddingHitSerializer serializer;

  int pl_size = 3 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used1,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  std::vector<EmbeddingHit> hits1 =
      CreateEmbeddingHits(/*num_hits=*/5, /*desired_byte_length=*/1);
  for (const EmbeddingHit &hit : hits1) {
    ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit));
  }

  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used2,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  std::vector<EmbeddingHit> hits2 =
      CreateEmbeddingHits(/*num_hits=*/5, /*desired_byte_length=*/2);
  for (const EmbeddingHit &hit : hits2) {
    ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, hit));
  }

  ICING_ASSERT_OK(serializer.MoveFrom(/*dst=*/&pl_used2, /*src=*/&pl_used1));
  EXPECT_THAT(serializer.GetHits(&pl_used2),
              IsOkAndHolds(ElementsAreArray(hits1.rbegin(), hits1.rend())));
  EXPECT_THAT(serializer.GetHits(&pl_used1), IsOkAndHolds(IsEmpty()));
}

TEST(PostingListEmbeddingHitSerializerTest,
     MoveFromNullArgumentReturnsInvalidArgument) {
  PostingListEmbeddingHitSerializer serializer;

  int pl_size = 3 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used1,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  std::vector<EmbeddingHit> hits =
      CreateEmbeddingHits(/*num_hits=*/5, /*desired_byte_length=*/1);
  for (const EmbeddingHit &hit : hits) {
    ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit));
  }

  EXPECT_THAT(serializer.MoveFrom(/*dst=*/&pl_used1, /*src=*/nullptr),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(serializer.GetHits(&pl_used1),
              IsOkAndHolds(ElementsAreArray(hits.rbegin(), hits.rend())));
}

TEST(PostingListEmbeddingHitSerializerTest,
     MoveFromInvalidPostingListReturnsInvalidArgument) {
  PostingListEmbeddingHitSerializer serializer;

  int pl_size = 3 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used1,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  std::vector<EmbeddingHit> hits1 =
      CreateEmbeddingHits(/*num_hits=*/5, /*desired_byte_length=*/1);
  for (const EmbeddingHit &hit : hits1) {
    ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit));
  }

  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used2,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  std::vector<EmbeddingHit> hits2 =
      CreateEmbeddingHits(/*num_hits=*/5, /*desired_byte_length=*/2);
  for (const EmbeddingHit &hit : hits2) {
    ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, hit));
  }

  // Write invalid hits to the beginning of pl_used1 to make it invalid.
  EmbeddingHit invalid_hit(EmbeddingHit::kInvalidValue);
  EmbeddingHit *first_hit =
      reinterpret_cast<EmbeddingHit *>(pl_used1.posting_list_buffer());
  *first_hit = invalid_hit;
  ++first_hit;
  *first_hit = invalid_hit;
  EXPECT_THAT(serializer.MoveFrom(/*dst=*/&pl_used2, /*src=*/&pl_used1),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(serializer.GetHits(&pl_used2),
              IsOkAndHolds(ElementsAreArray(hits2.rbegin(), hits2.rend())));
}

TEST(PostingListEmbeddingHitSerializerTest,
     MoveToInvalidPostingListReturnsFailedPrecondition) {
  PostingListEmbeddingHitSerializer serializer;

  int pl_size = 3 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used1,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  std::vector<EmbeddingHit> hits1 =
      CreateEmbeddingHits(/*num_hits=*/5, /*desired_byte_length=*/1);
  for (const EmbeddingHit &hit : hits1) {
    ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit));
  }

  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used2,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  std::vector<EmbeddingHit> hits2 =
      CreateEmbeddingHits(/*num_hits=*/5, /*desired_byte_length=*/2);
  for (const EmbeddingHit &hit : hits2) {
    ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, hit));
  }

  // Write invalid hits to the beginning of pl_used2 to make it invalid.
  EmbeddingHit invalid_hit(EmbeddingHit::kInvalidValue);
  EmbeddingHit *first_hit =
      reinterpret_cast<EmbeddingHit *>(pl_used2.posting_list_buffer());
  *first_hit = invalid_hit;
  ++first_hit;
  *first_hit = invalid_hit;
  EXPECT_THAT(serializer.MoveFrom(/*dst=*/&pl_used2, /*src=*/&pl_used1),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(serializer.GetHits(&pl_used1),
              IsOkAndHolds(ElementsAreArray(hits1.rbegin(), hits1.rend())));
}

TEST(PostingListEmbeddingHitSerializerTest, MoveToPostingListTooSmall) {
  PostingListEmbeddingHitSerializer serializer;

  int pl_size = 3 * serializer.GetMinPostingListSize();
  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used1,
      PostingListUsed::CreateFromUnitializedRegion(&serializer, pl_size));
  std::vector<EmbeddingHit> hits1 =
      CreateEmbeddingHits(/*num_hits=*/5, /*desired_byte_length=*/1);
  for (const EmbeddingHit &hit : hits1) {
    ICING_ASSERT_OK(serializer.PrependHit(&pl_used1, hit));
  }

  ICING_ASSERT_OK_AND_ASSIGN(
      PostingListUsed pl_used2,
      PostingListUsed::CreateFromUnitializedRegion(
          &serializer, serializer.GetMinPostingListSize()));
  std::vector<EmbeddingHit> hits2 =
      CreateEmbeddingHits(/*num_hits=*/1, /*desired_byte_length=*/2);
  for (const EmbeddingHit &hit : hits2) {
    ICING_ASSERT_OK(serializer.PrependHit(&pl_used2, hit));
  }

  EXPECT_THAT(serializer.MoveFrom(/*dst=*/&pl_used2, /*src=*/&pl_used1),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(serializer.GetHits(&pl_used1),
              IsOkAndHolds(ElementsAreArray(hits1.rbegin(), hits1.rend())));
  EXPECT_THAT(serializer.GetHits(&pl_used2),
              IsOkAndHolds(ElementsAreArray(hits2.rbegin(), hits2.rend())));
}

}  // namespace

}  // namespace lib
}  // namespace icing
