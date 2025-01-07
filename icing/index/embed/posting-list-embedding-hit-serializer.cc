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

#include <cinttypes>
#include <cstdint>
#include <cstring>
#include <limits>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/file/posting_list/posting-list-used.h"
#include "icing/index/embed/embedding-hit.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/legacy/index/icing-bit-util.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

uint32_t PostingListEmbeddingHitSerializer::GetBytesUsed(
    const PostingListUsed* posting_list_used) const {
  // The special hits will be included if they represent actual hits. If they
  // represent the hit offset or the invalid hit sentinel, they are not
  // included.
  return posting_list_used->size_in_bytes() -
         GetStartByteOffset(posting_list_used);
}

uint32_t PostingListEmbeddingHitSerializer::GetMinPostingListSizeToFit(
    const PostingListUsed* posting_list_used) const {
  if (IsFull(posting_list_used) || IsAlmostFull(posting_list_used)) {
    // If in either the FULL state or ALMOST_FULL state, this posting list *is*
    // the minimum size posting list that can fit these hits. So just return the
    // size of the posting list.
    return posting_list_used->size_in_bytes();
  }

  // - In NOT_FULL status, BytesUsed contains no special hits. For a posting
  //   list in the NOT_FULL state with n hits, we would have n-1 compressed hits
  //   and 1 uncompressed hit.
  // - The minimum sized posting list that would be guaranteed to fit these hits
  //   would be FULL, but calculating the size required for the FULL posting
  //   list would require deserializing the last two added hits, so instead we
  //   will calculate the size of an ALMOST_FULL posting list to fit.
  // - An ALMOST_FULL posting list would have kInvalidHit in special_hit(0), the
  //   full uncompressed Hit in special_hit(1), and the n-1 compressed hits in
  //   the compressed region.
  // - Currently BytesUsed contains one uncompressed Hit and n-1 compressed
  //   hits.
  // - Therefore, fitting these hits into a posting list would require
  //   BytesUsed + one extra full hit.
  return GetBytesUsed(posting_list_used) + sizeof(EmbeddingHit);
}

void PostingListEmbeddingHitSerializer::Clear(
    PostingListUsed* posting_list_used) const {
  // Safe to ignore return value because posting_list_used->size_in_bytes() is
  // a valid argument.
  SetStartByteOffset(posting_list_used,
                     /*offset=*/posting_list_used->size_in_bytes());
}

libtextclassifier3::Status PostingListEmbeddingHitSerializer::MoveFrom(
    PostingListUsed* dst, PostingListUsed* src) const {
  ICING_RETURN_ERROR_IF_NULL(dst);
  ICING_RETURN_ERROR_IF_NULL(src);
  if (GetMinPostingListSizeToFit(src) > dst->size_in_bytes()) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "src MinPostingListSizeToFit %d must be larger than size %d.",
        GetMinPostingListSizeToFit(src), dst->size_in_bytes()));
  }

  if (!IsPostingListValid(dst)) {
    return absl_ports::FailedPreconditionError(
        "Dst posting list is in an invalid state and can't be used!");
  }
  if (!IsPostingListValid(src)) {
    return absl_ports::InvalidArgumentError(
        "Cannot MoveFrom an invalid src posting list!");
  }

  // Pop just enough hits that all of src's compressed hits fit in
  // dst posting_list's compressed area. Then we can memcpy that area.
  std::vector<EmbeddingHit> hits;
  while (IsFull(src) || IsAlmostFull(src) ||
         (dst->size_in_bytes() - kSpecialHitsSize < GetBytesUsed(src))) {
    if (!GetHitsInternal(src, /*limit=*/1, /*pop=*/true, &hits).ok()) {
      return absl_ports::AbortedError(
          "Unable to retrieve hits from src posting list.");
    }
  }

  // memcpy the area and set up start byte offset.
  Clear(dst);
  memcpy(dst->posting_list_buffer() + dst->size_in_bytes() - GetBytesUsed(src),
         src->posting_list_buffer() + GetStartByteOffset(src),
         GetBytesUsed(src));
  // Because we popped all hits from src outside of the compressed area and we
  // guaranteed that GetBytesUsed(src) is less than dst->size_in_bytes() -
  // kSpecialHitSize. This is guaranteed to be a valid byte offset for the
  // NOT_FULL state, so ignoring the value is safe.
  SetStartByteOffset(dst, dst->size_in_bytes() - GetBytesUsed(src));

  // Put back remaining hits.
  for (size_t i = 0; i < hits.size(); i++) {
    const EmbeddingHit& hit = hits[hits.size() - i - 1];
    // PrependHit can return either INVALID_ARGUMENT - if hit is invalid or not
    // less than the previous hit - or RESOURCE_EXHAUSTED. RESOURCE_EXHAUSTED
    // should be impossible because we've already assured that there is enough
    // room above.
    ICING_RETURN_IF_ERROR(PrependHit(dst, hit));
  }

  Clear(src);
  return libtextclassifier3::Status::OK;
}

uint32_t PostingListEmbeddingHitSerializer::GetPadEnd(
    const PostingListUsed* posting_list_used, uint32_t offset) const {
  EmbeddingHit::Value pad;
  uint32_t pad_end = offset;
  while (pad_end < posting_list_used->size_in_bytes()) {
    size_t pad_len = VarInt::Decode(
        posting_list_used->posting_list_buffer() + pad_end, &pad);
    if (pad != 0) {
      // No longer a pad.
      break;
    }
    pad_end += pad_len;
  }
  return pad_end;
}

bool PostingListEmbeddingHitSerializer::PadToEnd(
    PostingListUsed* posting_list_used, uint32_t start, uint32_t end) const {
  if (end > posting_list_used->size_in_bytes()) {
    ICING_LOG(ERROR) << "Cannot pad a region that ends after size!";
    return false;
  }
  // In VarInt a value of 0 encodes to 0.
  memset(posting_list_used->posting_list_buffer() + start, 0, end - start);
  return true;
}

libtextclassifier3::Status
PostingListEmbeddingHitSerializer::PrependHitToAlmostFull(
    PostingListUsed* posting_list_used, const EmbeddingHit& hit) const {
  // Get delta between first hit and the new hit. Try to fit delta
  // in the padded area and put new hit at the special position 1.
  // Calling ValueOrDie is safe here because 1 < kNumSpecialData.
  EmbeddingHit cur = GetSpecialHit(posting_list_used, /*index=*/1);
  if (cur.value() <= hit.value()) {
    return absl_ports::InvalidArgumentError(
        "Hit being prepended must be strictly less than the most recent Hit");
  }
  uint64_t delta = cur.value() - hit.value();
  uint8_t delta_buf[VarInt::kMaxEncodedLen64];
  size_t delta_len = VarInt::Encode(delta, delta_buf);

  uint32_t pad_end = GetPadEnd(posting_list_used,
                               /*offset=*/kSpecialHitsSize);

  if (pad_end >= kSpecialHitsSize + delta_len) {
    // Pad area has enough space for delta of existing hit (cur). Write delta at
    // pad_end - delta_len.
    uint8_t* delta_offset =
        posting_list_used->posting_list_buffer() + pad_end - delta_len;
    memcpy(delta_offset, delta_buf, delta_len);

    // Now first hit is the new hit, at special position 1. Safe to ignore the
    // return value because 1 < kNumSpecialData.
    SetSpecialHit(posting_list_used, /*index=*/1, hit);
    // Safe to ignore the return value because sizeof(EmbeddingHit) is a valid
    // argument.
    SetStartByteOffset(posting_list_used, /*offset=*/sizeof(EmbeddingHit));
  } else {
    // No space for delta. We put the new hit at special position 0
    // and go to the full state. Safe to ignore the return value because 1 <
    // kNumSpecialData.
    SetSpecialHit(posting_list_used, /*index=*/0, hit);
  }
  return libtextclassifier3::Status::OK;
}

void PostingListEmbeddingHitSerializer::PrependHitToEmpty(
    PostingListUsed* posting_list_used, const EmbeddingHit& hit) const {
  // First hit to be added. Just add verbatim, no compression.
  if (posting_list_used->size_in_bytes() == kSpecialHitsSize) {
    // Safe to ignore the return value because 1 < kNumSpecialData
    SetSpecialHit(posting_list_used, /*index=*/1, hit);
    // Safe to ignore the return value because sizeof(EmbeddingHit) is a valid
    // argument.
    SetStartByteOffset(posting_list_used, /*offset=*/sizeof(EmbeddingHit));
  } else {
    // Since this is the first hit, size != kSpecialHitsSize and
    // size % sizeof(EmbeddingHit) == 0, we know that there is room to fit 'hit'
    // into the compressed region, so ValueOrDie is safe.
    uint32_t offset =
        PrependHitUncompressed(posting_list_used, hit,
                               /*offset=*/posting_list_used->size_in_bytes())
            .ValueOrDie();
    // Safe to ignore the return value because PrependHitUncompressed is
    // guaranteed to return a valid offset.
    SetStartByteOffset(posting_list_used, offset);
  }
}

libtextclassifier3::Status
PostingListEmbeddingHitSerializer::PrependHitToNotFull(
    PostingListUsed* posting_list_used, const EmbeddingHit& hit,
    uint32_t offset) const {
  // First hit in compressed area. It is uncompressed. See if delta
  // between the first hit and new hit will still fit in the
  // compressed area.
  if (offset + sizeof(EmbeddingHit::Value) >
      posting_list_used->size_in_bytes()) {
    // The first hit in the compressed region *should* be uncompressed, but
    // somehow there isn't enough room between offset and the end of the
    // compressed area to fit an uncompressed hit. This should NEVER happen.
    return absl_ports::FailedPreconditionError(
        "Posting list is in an invalid state.");
  }
  EmbeddingHit::Value cur_value;
  memcpy(&cur_value, posting_list_used->posting_list_buffer() + offset,
         sizeof(EmbeddingHit::Value));
  if (cur_value <= hit.value()) {
    return absl_ports::InvalidArgumentError(
        IcingStringUtil::StringPrintf("EmbeddingHit %" PRId64
                                      " being prepended must be "
                                      "strictly less than the most recent "
                                      "EmbeddingHit %" PRId64,
                                      hit.value(), cur_value));
  }
  uint64_t delta = cur_value - hit.value();
  uint8_t delta_buf[VarInt::kMaxEncodedLen64];
  size_t delta_len = VarInt::Encode(delta, delta_buf);

  // offset now points to one past the end of the first hit.
  offset += sizeof(EmbeddingHit::Value);
  if (kSpecialHitsSize + sizeof(EmbeddingHit::Value) + delta_len <= offset) {
    // Enough space for delta in compressed area.

    // Prepend delta.
    offset -= delta_len;
    memcpy(posting_list_used->posting_list_buffer() + offset, delta_buf,
           delta_len);

    // Prepend new hit. We know that there is room for 'hit' because of the if
    // statement above, so calling ValueOrDie is safe.
    offset =
        PrependHitUncompressed(posting_list_used, hit, offset).ValueOrDie();
    // offset is guaranteed to be valid here. So it's safe to ignore the return
    // value. The if above will guarantee that offset >= kSpecialHitSize and <
    // posting_list_used->size_in_bytes() because the if ensures that there is
    // enough room between offset and kSpecialHitSize to fit the delta of the
    // previous hit and the uncompressed hit.
    SetStartByteOffset(posting_list_used, offset);
  } else if (kSpecialHitsSize + delta_len <= offset) {
    // Only have space for delta. The new hit must be put in special
    // position 1.

    // Prepend delta.
    offset -= delta_len;
    memcpy(posting_list_used->posting_list_buffer() + offset, delta_buf,
           delta_len);

    // Prepend pad. Safe to ignore the return value of PadToEnd because offset
    // must be less than posting_list_used->size_in_bytes(). Otherwise, this
    // function already would have returned FAILED_PRECONDITION.
    PadToEnd(posting_list_used, /*start=*/kSpecialHitsSize,
             /*end=*/offset);

    // Put new hit in special position 1. Safe to ignore return value because 1
    // < kNumSpecialData.
    SetSpecialHit(posting_list_used, /*index=*/1, hit);

    // State almost_full. Safe to ignore the return value because
    // sizeof(EmbeddingHit) is a valid argument.
    SetStartByteOffset(posting_list_used, /*offset=*/sizeof(EmbeddingHit));
  } else {
    // Very rare case where delta is larger than sizeof(EmbeddingHit::Value)
    // (i.e. varint delta encoding expanded required storage). We
    // move first hit to special position 1 and put new hit in
    // special position 0.
    EmbeddingHit cur(cur_value);
    // Safe to ignore the return value of PadToEnd because offset must be less
    // than posting_list_used->size_in_bytes(). Otherwise, this function
    // already would have returned FAILED_PRECONDITION.
    PadToEnd(posting_list_used, /*start=*/kSpecialHitsSize,
             /*end=*/offset);
    // Safe to ignore the return value here because 0 and 1 < kNumSpecialData.
    SetSpecialHit(posting_list_used, /*index=*/1, cur);
    SetSpecialHit(posting_list_used, /*index=*/0, hit);
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status PostingListEmbeddingHitSerializer::PrependHit(
    PostingListUsed* posting_list_used, const EmbeddingHit& hit) const {
  static_assert(
      sizeof(EmbeddingHit::Value) <= sizeof(uint64_t),
      "EmbeddingHit::Value cannot be larger than 8 bytes because the delta "
      "must be able to fit in 8 bytes.");
  if (!hit.is_valid()) {
    return absl_ports::InvalidArgumentError("Cannot prepend an invalid hit!");
  }
  if (!IsPostingListValid(posting_list_used)) {
    return absl_ports::FailedPreconditionError(
        "This PostingListUsed is in an invalid state and can't add any hits!");
  }

  if (IsFull(posting_list_used)) {
    // State full: no space left.
    return absl_ports::ResourceExhaustedError("No more room for hits");
  } else if (IsAlmostFull(posting_list_used)) {
    return PrependHitToAlmostFull(posting_list_used, hit);
  } else if (IsEmpty(posting_list_used)) {
    PrependHitToEmpty(posting_list_used, hit);
    return libtextclassifier3::Status::OK;
  } else {
    uint32_t offset = GetStartByteOffset(posting_list_used);
    return PrependHitToNotFull(posting_list_used, hit, offset);
  }
}

libtextclassifier3::StatusOr<std::vector<EmbeddingHit>>
PostingListEmbeddingHitSerializer::GetHits(
    const PostingListUsed* posting_list_used) const {
  std::vector<EmbeddingHit> hits_out;
  ICING_RETURN_IF_ERROR(GetHits(posting_list_used, &hits_out));
  return hits_out;
}

libtextclassifier3::Status PostingListEmbeddingHitSerializer::GetHits(
    const PostingListUsed* posting_list_used,
    std::vector<EmbeddingHit>* hits_out) const {
  return GetHitsInternal(posting_list_used,
                         /*limit=*/std::numeric_limits<uint32_t>::max(),
                         /*pop=*/false, hits_out);
}

libtextclassifier3::Status PostingListEmbeddingHitSerializer::PopFrontHits(
    PostingListUsed* posting_list_used, uint32_t num_hits) const {
  if (num_hits == 1 && IsFull(posting_list_used)) {
    // The PL is in full status which means that we save 2 uncompressed hits in
    // the 2 special postions. But full status may be reached by 2 different
    // statuses.
    // (1) In "almost full" status
    // +-----------------+----------------+-------+-----------------+
    // |Hit::kInvalidVal |1st hit         |(pad)  |(compressed) hits|
    // +-----------------+----------------+-------+-----------------+
    // When we prepend another hit, we can only put it at the special
    // position 0. And we get a full PL
    // +-----------------+----------------+-------+-----------------+
    // |new 1st hit      |original 1st hit|(pad)  |(compressed) hits|
    // +-----------------+----------------+-------+-----------------+
    // (2) In "not full" status
    // +-----------------+----------------+------+-------+------------------+
    // |hits-start-offset|Hit::kInvalidVal|(pad) |1st hit|(compressed) hits |
    // +-----------------+----------------+------+-------+------------------+
    // When we prepend another hit, we can reach any of the 3 following
    // scenarios:
    // (2.1) not full
    // if the space of pad and original 1st hit can accommodate the new 1st hit
    // and the encoded delta value.
    // +-----------------+----------------+------+-----------+-----------------+
    // |hits-start-offset|Hit::kInvalidVal|(pad) |new 1st hit|(compressed) hits|
    // +-----------------+----------------+------+-----------+-----------------+
    // (2.2) almost full
    // If the space of pad and original 1st hit cannot accommodate the new 1st
    // hit and the encoded delta value but can accommodate the encoded delta
    // value only. We can put the new 1st hit at special position 1.
    // +-----------------+----------------+-------+-----------------+
    // |Hit::kInvalidVal |new 1st hit     |(pad)  |(compressed) hits|
    // +-----------------+----------------+-------+-----------------+
    // (2.3) full
    // In very rare case, it cannot even accommodate only the encoded delta
    // value. we can move the original 1st hit into special position 1 and the
    // new 1st hit into special position 0. This may happen because we use
    // VarInt encoding method which may make the encoded value longer (about
    // 4/3 times of original)
    // +-----------------+----------------+-------+-----------------+
    // |new 1st hit      |original 1st hit|(pad)  |(compressed) hits|
    // +-----------------+----------------+-------+-----------------+
    // Suppose now the PL is full. But we don't know whether it arrived to
    // this status from "not full" like (2.3) or from "almost full" like (1).
    // We'll return to "almost full" status like (1) if we simply pop the new
    // 1st hit but we want to make the prepending operation "reversible". So
    // there should be some way to return to "not full" if possible. A simple
    // way to do it is to pop 2 hits out of the PL to status "almost full" or
    // "not full".  And add the original 1st hit back. We can return to the
    // correct original statuses of (2.1) or (1). This makes our prepending
    // operation reversible.
    std::vector<EmbeddingHit> out;

    // Popping 2 hits should never fail because we've just ensured that the
    // posting list is in the FULL state.
    ICING_RETURN_IF_ERROR(
        GetHitsInternal(posting_list_used, /*limit=*/2, /*pop=*/true, &out));

    // PrependHit should never fail because out[1] is a valid hit less than
    // previous hits in the posting list and because there's no way that the
    // posting list could run out of room because it previously stored this hit
    // AND another hit.
    ICING_RETURN_IF_ERROR(PrependHit(posting_list_used, out[1]));
  } else if (num_hits > 0) {
    return GetHitsInternal(posting_list_used, /*limit=*/num_hits, /*pop=*/true,
                           nullptr);
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status PostingListEmbeddingHitSerializer::GetHitsInternal(
    const PostingListUsed* posting_list_used, uint32_t limit, bool pop,
    std::vector<EmbeddingHit>* out) const {
  // Put current uncompressed val here.
  EmbeddingHit::Value val = EmbeddingHit::kInvalidValue;
  uint32_t offset = GetStartByteOffset(posting_list_used);
  uint32_t count = 0;

  // First traverse the first two special positions.
  while (count < limit && offset < kSpecialHitsSize) {
    // Calling ValueOrDie is safe here because offset / sizeof(EmbeddingHit) <
    // kNumSpecialData because of the check above.
    EmbeddingHit hit = GetSpecialHit(posting_list_used,
                                     /*index=*/offset / sizeof(EmbeddingHit));
    val = hit.value();
    if (out != nullptr) {
      out->push_back(hit);
    }
    offset += sizeof(EmbeddingHit);
    count++;
  }

  // If special position 1 was set then we need to skip padding.
  if (val != EmbeddingHit::kInvalidValue && offset == kSpecialHitsSize) {
    offset = GetPadEnd(posting_list_used, offset);
  }

  while (count < limit && offset < posting_list_used->size_in_bytes()) {
    if (val == EmbeddingHit::kInvalidValue) {
      // First hit is in compressed area. Put that in val.
      memcpy(&val, posting_list_used->posting_list_buffer() + offset,
             sizeof(EmbeddingHit::Value));
      offset += sizeof(EmbeddingHit::Value);
    } else {
      // Now we have delta encoded subsequent hits. Decode and push.
      uint64_t delta;
      offset += VarInt::Decode(
          posting_list_used->posting_list_buffer() + offset, &delta);
      val += delta;
    }
    EmbeddingHit hit(val);
    if (out != nullptr) {
      out->push_back(hit);
    }
    count++;
  }

  if (pop) {
    PostingListUsed* mutable_posting_list_used =
        const_cast<PostingListUsed*>(posting_list_used);
    // Modify the posting list so that we pop all hits actually
    // traversed.
    if (offset >= kSpecialHitsSize &&
        offset < posting_list_used->size_in_bytes()) {
      // In the compressed area. Pop and reconstruct. offset/val is
      // the last traversed hit, which we must discard. So move one
      // more forward.
      uint64_t delta;
      offset += VarInt::Decode(
          posting_list_used->posting_list_buffer() + offset, &delta);
      val += delta;

      // Now val is the first hit of the new posting list.
      if (kSpecialHitsSize + sizeof(EmbeddingHit::Value) <= offset) {
        // val fits in compressed area. Simply copy.
        offset -= sizeof(EmbeddingHit::Value);
        memcpy(mutable_posting_list_used->posting_list_buffer() + offset, &val,
               sizeof(EmbeddingHit::Value));
      } else {
        // val won't fit in compressed area.
        EmbeddingHit hit(val);
        // Okay to ignore the return value here because 1 < kNumSpecialData.
        SetSpecialHit(mutable_posting_list_used, /*index=*/1, hit);

        // Prepend pad. Safe to ignore the return value of PadToEnd because
        // offset must be less than posting_list_used->size_in_bytes() thanks to
        // the if above.
        PadToEnd(mutable_posting_list_used,
                 /*start=*/kSpecialHitsSize,
                 /*end=*/offset);
        offset = sizeof(EmbeddingHit);
      }
    }
    // offset is guaranteed to be valid so ignoring the return value of
    // set_start_byte_offset is safe. It falls into one of four scenarios:
    // Scenario 1: the above if was false because offset is not <
    //             posting_list_used->size_in_bytes()
    //   In this case, offset must be == posting_list_used->size_in_bytes()
    //   because we reached offset by unwinding hits on the posting list.
    // Scenario 2: offset is < kSpecialHitSize
    //   In this case, offset is guaranteed to be either 0 or
    //   sizeof(EmbeddingHit) because offset is incremented by
    //   sizeof(EmbeddingHit) within the first while loop.
    // Scenario 3: offset is within the compressed region and the new first hit
    //   in the posting list (the value that 'val' holds) will fit as an
    //   uncompressed hit in the compressed region. The resulting offset from
    //   decompressing val must be >= kSpecialHitSize because otherwise we'd be
    //   in Scenario 4
    // Scenario 4: offset is within the compressed region, but the new first hit
    //   in the posting list is too large to fit as an uncompressed hit in the
    //   in the compressed region. Therefore, it must be stored in a special hit
    //   and offset will be sizeof(EmbeddingHit).
    SetStartByteOffset(mutable_posting_list_used, offset);
  }

  return libtextclassifier3::Status::OK;
}

EmbeddingHit PostingListEmbeddingHitSerializer::GetSpecialHit(
    const PostingListUsed* posting_list_used, uint32_t index) const {
  static_assert(sizeof(EmbeddingHit::Value) >= sizeof(uint32_t), "HitTooSmall");
  EmbeddingHit val(EmbeddingHit::kInvalidValue);
  memcpy(&val, posting_list_used->posting_list_buffer() + index * sizeof(val),
         sizeof(val));
  return val;
}

void PostingListEmbeddingHitSerializer::SetSpecialHit(
    PostingListUsed* posting_list_used, uint32_t index,
    const EmbeddingHit& val) const {
  memcpy(posting_list_used->posting_list_buffer() + index * sizeof(val), &val,
         sizeof(val));
}

bool PostingListEmbeddingHitSerializer::IsPostingListValid(
    const PostingListUsed* posting_list_used) const {
  if (IsAlmostFull(posting_list_used)) {
    // Special Hit 1 should hold a Hit. Calling ValueOrDie is safe because we
    // know that 1 < kNumSpecialData.
    if (!GetSpecialHit(posting_list_used, /*index=*/1).is_valid()) {
      ICING_LOG(ERROR)
          << "Both special hits cannot be invalid at the same time.";
      return false;
    }
  } else if (!IsFull(posting_list_used)) {
    // NOT_FULL. Special Hit 0 should hold a valid offset. Calling ValueOrDie is
    // safe because we know that 0 < kNumSpecialData.
    if (GetSpecialHit(posting_list_used, /*index=*/0).value() >
            posting_list_used->size_in_bytes() ||
        GetSpecialHit(posting_list_used, /*index=*/0).value() <
            kSpecialHitsSize) {
      ICING_LOG(ERROR) << "EmbeddingHit: "
                       << GetSpecialHit(posting_list_used, /*index=*/0).value()
                       << " size: " << posting_list_used->size_in_bytes()
                       << " sp size: " << kSpecialHitsSize;
      return false;
    }
  }
  return true;
}

uint32_t PostingListEmbeddingHitSerializer::GetStartByteOffset(
    const PostingListUsed* posting_list_used) const {
  if (IsFull(posting_list_used)) {
    return 0;
  } else if (IsAlmostFull(posting_list_used)) {
    return sizeof(EmbeddingHit);
  } else {
    // NOT_FULL, calling ValueOrDie is safe because we know that 0 <
    // kNumSpecialData.
    return GetSpecialHit(posting_list_used, /*index=*/0).value();
  }
}

bool PostingListEmbeddingHitSerializer::SetStartByteOffset(
    PostingListUsed* posting_list_used, uint32_t offset) const {
  if (offset > posting_list_used->size_in_bytes()) {
    ICING_LOG(ERROR) << "offset cannot be a value greater than size "
                     << posting_list_used->size_in_bytes() << ". offset is "
                     << offset << ".";
    return false;
  }
  if (offset < kSpecialHitsSize && offset > sizeof(EmbeddingHit)) {
    ICING_LOG(ERROR) << "offset cannot be a value between ("
                     << sizeof(EmbeddingHit) << ", " << kSpecialHitsSize
                     << "). offset is " << offset << ".";
    return false;
  }
  if (offset < sizeof(EmbeddingHit) && offset != 0) {
    ICING_LOG(ERROR) << "offset cannot be a value between (0, "
                     << sizeof(EmbeddingHit) << "). offset is " << offset
                     << ".";
    return false;
  }
  if (offset >= kSpecialHitsSize) {
    // not_full state. Safe to ignore the return value because 0 and 1 are both
    // < kNumSpecialData.
    SetSpecialHit(posting_list_used, /*index=*/0, EmbeddingHit(offset));
    SetSpecialHit(posting_list_used, /*index=*/1,
                  EmbeddingHit(EmbeddingHit::kInvalidValue));
  } else if (offset == sizeof(EmbeddingHit)) {
    // almost_full state. Safe to ignore the return value because 1 is both <
    // kNumSpecialData.
    SetSpecialHit(posting_list_used, /*index=*/0,
                  EmbeddingHit(EmbeddingHit::kInvalidValue));
  }
  // Nothing to do for the FULL state - the offset isn't actually stored
  // anywhere and both special hits hold valid hits.
  return true;
}

libtextclassifier3::StatusOr<uint32_t>
PostingListEmbeddingHitSerializer::PrependHitUncompressed(
    PostingListUsed* posting_list_used, const EmbeddingHit& hit,
    uint32_t offset) const {
  if (offset < kSpecialHitsSize + sizeof(EmbeddingHit::Value)) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "Not enough room to prepend EmbeddingHit::Value at offset %d.",
        offset));
  }
  offset -= sizeof(EmbeddingHit::Value);
  EmbeddingHit::Value val = hit.value();
  memcpy(posting_list_used->posting_list_buffer() + offset, &val,
         sizeof(EmbeddingHit::Value));
  return offset;
}

}  // namespace lib
}  // namespace icing
