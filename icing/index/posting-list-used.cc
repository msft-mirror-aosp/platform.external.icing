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

#include "icing/index/posting-list-used.h"

#include <algorithm>
#include <cinttypes>
#include <cstdint>
#include <limits>

#include "icing/absl_ports/canonical_errors.h"
#include "icing/index/posting-list-utils.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/legacy/index/icing-bit-util.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

uint32_t GetScoreByteSize(const Hit &hit) {
  return hit.has_score() ? sizeof(Hit::Score) : 0;
}

}  // namespace

libtextclassifier3::StatusOr<PostingListUsed>
PostingListUsed::CreateFromPreexistingPostingListUsedRegion(
    void *posting_list_buffer, uint32_t size_in_bytes) {
  ICING_RETURN_ERROR_IF_NULL(posting_list_buffer);
  if (!posting_list_utils::IsValidPostingListSize(size_in_bytes)) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "Requested posting list size %d is invalid!", size_in_bytes));
  }
  return PostingListUsed(posting_list_buffer, size_in_bytes);
}

libtextclassifier3::StatusOr<PostingListUsed>
PostingListUsed::CreateFromUnitializedRegion(void *posting_list_buffer,
                                             uint32_t size_in_bytes) {
  ICING_ASSIGN_OR_RETURN(PostingListUsed posting_list_used,
                         CreateFromPreexistingPostingListUsedRegion(
                             posting_list_buffer, size_in_bytes));
  posting_list_used.Clear();
  return posting_list_used;
}

void PostingListUsed::Clear() { set_start_byte_offset(size_in_bytes_); }

libtextclassifier3::Status PostingListUsed::MoveFrom(PostingListUsed *other) {
  ICING_RETURN_ERROR_IF_NULL(other);
  if (other->MinPostingListSizeToFit() > size_in_bytes_) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "other->MinPostingListSizeToFit %d must be larger than size %d.",
        other->MinPostingListSizeToFit(), size_in_bytes_));
  }

  if (!IsPostingListValid()) {
    return absl_ports::FailedPreconditionError(
        "This posting list is in an invalid state and can't be used!");
  }
  if (other->IsPostingListValid()) {
    return absl_ports::InvalidArgumentError(
        "Cannot MoveFrom an invalid posting list!");
  }

  // Pop just enough hits that all of other's compressed hits fit in
  // this posting_list's compressed area. Then we can memcpy that area.
  std::vector<Hit> hits;
  while (other->full() || other->almost_full() ||
         (size_in_bytes_ - posting_list_utils::kSpecialHitsSize <
          other->BytesUsed())) {
    if (other->GetHitsInternal(/*limit=*/1, /*pop=*/true, &hits) != 1) {
      return absl_ports::AbortedError(
          "Unable to retrieve hits from other posting list.");
    }
  }

  // memcpy the area and set up start byte offset.
  Clear();
  memcpy(posting_list_buffer_ + size_in_bytes_ - other->BytesUsed(),
         other->posting_list_buffer_ + other->get_start_byte_offset(),
         other->BytesUsed());
  // Because we popped all hits from other outside of the compressed area and we
  // guaranteed that other->BytesUsed is less than size_in_bytes_ -
  // kSpecialHitSize. This is guaranteed to be a valid byte offset for the
  // NOT_FULL state.
  set_start_byte_offset(size_in_bytes_ - other->BytesUsed());

  // Put back remaining hits.
  for (size_t i = 0; i < hits.size(); i++) {
    const Hit &hit = hits[hits.size() - i - 1];
    // PrependHit can return either INVALID_ARGUMENT - if hit is invalid or not
    // less than the previous hit - or RESOURCE_EXHAUSTED. RESOURCE_EXHAUSTED
    // should be impossible because we've already assured that there is enough
    // room above.
    ICING_RETURN_IF_ERROR(PrependHit(hit));
  }

  other->Clear();
  return libtextclassifier3::Status::OK;
}

uint32_t PostingListUsed::GetPadEnd(uint32_t offset) const {
  Hit::Value pad;
  uint32_t pad_end = offset;
  while (pad_end < size_in_bytes_) {
    size_t pad_len = VarInt::Decode(posting_list_buffer_ + pad_end, &pad);
    if (pad != 0) {
      // No longer a pad.
      break;
    }
    pad_end += pad_len;
  }
  return pad_end;
}

void PostingListUsed::PadToEnd(uint32_t start, uint32_t end) {
  if (end > size_in_bytes_) {
    ICING_LOG(ERROR) << "Cannot pad a region that ends after size!";
    return;
  }
  // In VarInt a value of 0 encodes to 0.
  memset(posting_list_buffer_ + start, 0, end - start);
}

libtextclassifier3::Status PostingListUsed::PrependHitToAlmostFull(
    const Hit &hit) {
  // Get delta between first hit and the new hit. Try to fit delta
  // in the padded area and put new hit at the special position 1.
  Hit cur = get_special_hit(1);
  if (cur.value() <= hit.value()) {
    return absl_ports::InvalidArgumentError(
        "Hit being prepended must be strictly less than the most recent Hit");
  }
  uint64_t delta = cur.value() - hit.value();
  uint8_t delta_buf[VarInt::kMaxEncodedLen64];
  size_t delta_len = VarInt::Encode(delta, delta_buf);
  uint32_t cur_score_bytes = GetScoreByteSize(cur);

  uint32_t pad_end = GetPadEnd(posting_list_utils::kSpecialHitsSize);

  if (pad_end >=
      posting_list_utils::kSpecialHitsSize + delta_len + cur_score_bytes) {
    // Pad area has enough space for delta and score of existing hit
    // (cur). Write delta at pad_end - delta_len - cur_score_bytes.
    uint8_t *delta_offset =
        posting_list_buffer_ + pad_end - delta_len - cur_score_bytes;
    memcpy(delta_offset, delta_buf, delta_len);
    // Now copy score.
    Hit::Score score = cur.score();
    uint8_t *score_offset = delta_offset + delta_len;
    memcpy(score_offset, &score, cur_score_bytes);

    // Now first hit is the new hit, at special position 1.
    set_special_hit(1, hit);
    set_start_byte_offset(sizeof(Hit));
  } else {
    // No space for delta. We put the new hit at special position 0
    // and go to the full state.
    set_special_hit(0, hit);
  }
  return libtextclassifier3::Status::OK;
}

void PostingListUsed::PrependHitToEmpty(const Hit &hit) {
  // First hit to be added. Just add verbatim, no compression.
  if (size_in_bytes_ == posting_list_utils::kSpecialHitsSize) {
    set_special_hit(1, hit);
    set_start_byte_offset(sizeof(Hit));
  } else {
    // Since this is the first hit, size != kSpecialHitsSize and
    // size % sizeof(Hit) == 0, we know that there is room to fit 'hit' into
    // the compressed region.
    uint32_t offset = PrependHitUncompressed(hit, size_in_bytes_);
    set_start_byte_offset(offset);
  }
}

libtextclassifier3::Status PostingListUsed::PrependHitToNotFull(
    const Hit &hit, uint32_t offset) {
  // First hit in compressed area. It is uncompressed. See if delta
  // between the first hit and new hit will still fit in the
  // compressed area.
  if (offset + sizeof(Hit::Value) > size_in_bytes_) {
    // The first hit in the compressed region *should* be uncompressed, but
    // somehow there isn't enough room between offset and the end of the
    // compressed area to fit an uncompressed hit. This should NEVER happen.
    return absl_ports::FailedPreconditionError(
        "Posting list is in an invalid state.");
  }
  Hit::Value cur_value;
  memcpy(&cur_value, posting_list_buffer_ + offset, sizeof(Hit::Value));
  if (cur_value <= hit.value()) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "Hit %d being prepended must be strictly less than the most recent "
        "Hit %d",
        hit.value(), cur_value));
  }
  uint64_t delta = cur_value - hit.value();
  uint8_t delta_buf[VarInt::kMaxEncodedLen64];
  size_t delta_len = VarInt::Encode(delta, delta_buf);
  uint32_t hit_score_bytes = GetScoreByteSize(hit);

  // offset now points to one past the end of the first hit.
  offset += sizeof(Hit::Value);
  if (posting_list_utils::kSpecialHitsSize + sizeof(Hit::Value) + delta_len +
          hit_score_bytes <=
      offset) {
    // Enough space for delta in compressed area.

    // Prepend delta.
    offset -= delta_len;
    memcpy(posting_list_buffer_ + offset, delta_buf, delta_len);

    // Prepend new hit with (possibly) its score. We know that there is room
    // for 'hit' because of the if statement above.
    offset = PrependHitUncompressed(hit, offset);
    // offset is guaranteed to be valid here. The if above will guarantee that
    // offset >= kSpecialHitSize and < size_in_bytes_ because the if ensures
    // that there is enough room between offset and kSpecialHitSize to fit the
    // delta of the previous hit, any score and the uncompressed hit.
    set_start_byte_offset(offset);
  } else if (posting_list_utils::kSpecialHitsSize + delta_len <= offset) {
    // Only have space for delta. The new hit must be put in special
    // position 1.

    // Prepend delta.
    offset -= delta_len;
    memcpy(posting_list_buffer_ + offset, delta_buf, delta_len);

    // Prepend pad.
    PadToEnd(posting_list_utils::kSpecialHitsSize, offset);

    // Put new hit in special position 1.
    set_special_hit(1, hit);

    // State almost_full.
    set_start_byte_offset(sizeof(Hit));
  } else {
    // Very rare case where delta is larger than sizeof(Hit::Value)
    // (i.e. varint delta encoding expanded required storage). We
    // move first hit to special position 1 and put new hit in
    // special position 0.
    Hit cur(cur_value);
    if (cur.has_score()) {
      cur = Hit(cur_value, ReadScore(offset));
      offset += sizeof(Hit::Score);
    }
    PadToEnd(posting_list_utils::kSpecialHitsSize, offset);
    set_special_hit(1, cur);
    set_special_hit(0, hit);
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status PostingListUsed::PrependHit(const Hit &hit) {
  static_assert(sizeof(Hit::Value) <= sizeof(uint64_t),
                "Hit::Value cannot be larger than 8 bytes because the delta "
                "must be able to fit in 8 bytes.");
  if (!hit.is_valid()) {
    return absl_ports::InvalidArgumentError("Cannot prepend an invalid hit!");
  }
  if (!IsPostingListValid()) {
    return absl_ports::FailedPreconditionError(
        "This PostingListUsed is in an invalid state and can't add any hits!");
  }

  if (full()) {
    // State full: no space left.
    return absl_ports::ResourceExhaustedError("No more room for hits");
  } else if (almost_full()) {
    return PrependHitToAlmostFull(hit);
  } else if (empty()) {
    PrependHitToEmpty(hit);
    return libtextclassifier3::Status::OK;
  } else {
    uint32_t offset = get_start_byte_offset();
    return PrependHitToNotFull(hit, offset);
  }
}

std::vector<Hit> PostingListUsed::GetHits() const {
  std::vector<Hit> hits_out;
  GetHits(&hits_out);
  return hits_out;
}

void PostingListUsed::GetHits(std::vector<Hit> *hits_out) const {
  GetHitsInternal(/*limit=*/std::numeric_limits<uint32_t>::max(), /*pop=*/false,
                  hits_out);
}

void PostingListUsed::PopFrontHits(uint32_t num_hits) {
  if (num_hits == 1 && full()) {
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
    std::vector<Hit> out;

    // Popping 2 hits should never fail because we've just ensured that the
    // posting list is in the FULL state.
    GetHitsInternal(/*limit=*/2, /*pop=*/true, &out);

    // PrependHit should never fail because out[1] is a valid hit less than
    // previous hits in the posting list and because there's no way that the
    // posting list could run out of room because it previously stored this hit
    // AND another hit.
    PrependHit(out[1]);
  } else if (num_hits > 0) {
    GetHitsInternal(/*limit=*/num_hits, /*pop=*/true, nullptr);
  }
}

uint32_t PostingListUsed::GetHitsInternal(uint32_t limit, bool pop,
                                          std::vector<Hit> *out) const {
  // Put current uncompressed val here.
  Hit::Value val = Hit::kInvalidValue;
  uint32_t offset = get_start_byte_offset();
  uint32_t count = 0;

  // First traverse the first two special positions.
  while (count < limit && offset < posting_list_utils::kSpecialHitsSize) {
    Hit hit = get_special_hit(offset / sizeof(Hit));
    val = hit.value();
    if (out != nullptr) {
      out->push_back(hit);
    }
    offset += sizeof(Hit);
    count++;
  }

  // If special position 1 was set then we need to skip padding.
  if (val != Hit::kInvalidValue &&
      offset == posting_list_utils::kSpecialHitsSize) {
    offset = GetPadEnd(offset);
  }

  while (count < limit && offset < size_in_bytes_) {
    if (val == Hit::kInvalidValue) {
      // First hit is in compressed area. Put that in val.
      memcpy(&val, posting_list_buffer_ + offset, sizeof(Hit::Value));
      offset += sizeof(Hit::Value);
    } else {
      // Now we have delta encoded subsequent hits. Decode and push.
      uint64_t delta;
      offset += VarInt::Decode(posting_list_buffer_ + offset, &delta);
      val += delta;
    }
    Hit hit(val);
    if (hit.has_score()) {
      hit = Hit(val, ReadScore(offset));
      offset += sizeof(Hit::Score);
    }
    if (out != nullptr) {
      out->push_back(hit);
    }
    count++;
  }

  if (pop) {
    PostingListUsed *mutable_this = const_cast<PostingListUsed *>(this);
    // Modify the posting list so that we pop all hits actually
    // traversed.
    if (offset >= posting_list_utils::kSpecialHitsSize &&
        offset < size_in_bytes_) {
      // In the compressed area. Pop and reconstruct. offset/val is
      // the last traversed hit, which we must discard. So move one
      // more forward.
      uint64_t delta;
      offset += VarInt::Decode(posting_list_buffer_ + offset, &delta);
      val += delta;

      // Now val is the first hit of the new posting list.
      if (posting_list_utils::kSpecialHitsSize + sizeof(Hit::Value) <= offset) {
        // val fits in compressed area. Simply copy.
        offset -= sizeof(Hit::Value);
        memcpy(posting_list_buffer_ + offset, &val, sizeof(Hit::Value));
      } else {
        // val won't fit in compressed area. Also see if there is a
        // score.
        Hit hit(val);
        if (hit.has_score()) {
          hit = Hit(val, ReadScore(offset));
        }
        mutable_this->set_special_hit(1, hit);
        mutable_this->PadToEnd(posting_list_utils::kSpecialHitsSize, offset);
        offset = sizeof(Hit);
      }
    }
    // offset is guaranteed to be valid. It falls into one of four scenarios:
    // Scenario 1: the above if was false because offset is not < size_in_bytes_
    //   In this case, offset must be == size_in_bytes_ because we reached
    //   offset by unwinding hits on the posting list.
    // Scenario 2: offset is < kSpecialHitSize
    //   In this case, offset is guaranteed to be either 0 or sizeof(Hit)
    //   because offset is incremented by sizeof(Hit) within the first while
    //   loop.
    // Scenario 3: offset is within the compressed region and the new first hit
    //   in the posting list (the value that 'val' holds) will fit as an
    //   uncompressed hit in the compressed region. The resulting offset from
    //   decompressing val must be >= kSpecialHitSize because otherwise we'd be
    //   in Scenario 4
    // Scenario 4: offset is within the compressed region, but the new first hit
    //   in the posting list is too large to fit as an uncompressed hit in the
    //   in the compressed region. Therefore, it must be stored in a special hit
    //   and offset will be sizeof(Hit).
    mutable_this->set_start_byte_offset(offset);
  }

  return count;
}

Hit PostingListUsed::get_special_hit(uint32_t index) const {
  static_assert(sizeof(Hit::Value) >= sizeof(uint32_t), "HitTooSmall");
  if (index >= posting_list_utils::kSpecialHitsSize / sizeof(Hit)) {
    ICING_LOG(ERROR) << "Special hits only exist at indices 0 and 1";
    return Hit();
  }
  Hit val;
  memcpy(&val, posting_list_buffer_ + index * sizeof(val), sizeof(val));
  return val;
}

void PostingListUsed::set_special_hit(uint32_t index, const Hit &val) {
  if (index >= posting_list_utils::kSpecialHitsSize / sizeof(Hit)) {
    ICING_LOG(ERROR) << "Special hits only exist at indices 0 and 1";
    return;
  }
  memcpy(posting_list_buffer_ + index * sizeof(val), &val, sizeof(val));
}

uint32_t PostingListUsed::BytesUsed() const {
  // The special hits will be included if they represent actual hits. If they
  // represent the hit offset or the invalid hit sentinel, they are not
  // included.
  return size_in_bytes_ - get_start_byte_offset();
}

uint32_t PostingListUsed::MinPostingListSizeToFit() const {
  if (full() || almost_full()) {
    // If in either the FULL state or ALMOST_FULL state, this posting list *is*
    // the minimum size posting list that can fit these hits. So just return the
    // size of the posting list.
    return size_in_bytes_;
  }

  // In NOT_FULL status BytesUsed contains no special hits. The minimum sized
  // posting list that would be guaranteed to fit these hits would be
  // ALMOST_FULL, with kInvalidHit in special_hit(0), the uncompressed Hit in
  // special_hit(1) and the n compressed hits in the compressed region.
  // BytesUsed contains one uncompressed Hit and n compressed hits. Therefore,
  // fitting these hits into a posting list would require BytesUsed plus one
  // extra hit.
  return BytesUsed() + sizeof(Hit);
}

bool PostingListUsed::IsPostingListValid() const {
  if (almost_full()) {
    // Special Hit 1 should hold a Hit.
    if (!get_special_hit(1).is_valid()) {
      ICING_LOG(ERROR)
          << "Both special hits cannot be invalid at the same time.";
      return false;
    }
  } else if (!full()) {
    // NOT_FULL. Special Hit 0 should hold a valid offset.
    if (get_special_hit(0).value() > size_in_bytes_ ||
        get_special_hit(0).value() < posting_list_utils::kSpecialHitsSize) {
      ICING_LOG(ERROR) << "Hit: " << get_special_hit(0).value()
                       << " size: " << size_in_bytes_
                       << " sp size: " << posting_list_utils::kSpecialHitsSize;
      return false;
    }
  }
  return true;
}

uint32_t PostingListUsed::get_start_byte_offset() const {
  if (full()) {
    return 0;
  } else if (almost_full()) {
    return sizeof(Hit);
  } else {
    // NOT_FULL
    return get_special_hit(0).value();
  }
}

void PostingListUsed::set_start_byte_offset(uint32_t offset) {
  if (offset > size_in_bytes_) {
    ICING_LOG(ERROR) << "offset cannot be a value greater than size "
                     << size_in_bytes_ << ". offset is " << offset << ".";
    return;
  }
  if (offset < posting_list_utils::kSpecialHitsSize && offset > sizeof(Hit)) {
    ICING_LOG(ERROR) << "offset cannot be a value between (" << sizeof(Hit)
                     << ", " << posting_list_utils::kSpecialHitsSize
                     << "). offset is " << offset << ".";
    return;
  }
  if (offset < sizeof(Hit) && offset != 0) {
    ICING_LOG(ERROR) << "offset cannot be a value between (0, " << sizeof(Hit)
                     << "). offset is " << offset << ".";
    return;
  }
  if (offset >= posting_list_utils::kSpecialHitsSize) {
    // not_full state.
    set_special_hit(0, Hit(offset));
    set_special_hit(1, Hit());
  } else if (offset == sizeof(Hit)) {
    // almost_full state.
    set_special_hit(0, Hit());
  }
  // Nothing to do for the FULL state - the offset isn't actually stored
  // anywhere and both special hits hold valid hits.
}

uint32_t PostingListUsed::PrependHitUncompressed(const Hit &hit,
                                                 uint32_t offset) {
  if (hit.has_score()) {
    if (offset < posting_list_utils::kSpecialHitsSize + sizeof(Hit)) {
      ICING_LOG(ERROR) << "Not enough room to prepend Hit at offset " << offset
                       << ".";
      return offset;
    }
    offset -= sizeof(Hit);
    memcpy(posting_list_buffer_ + offset, &hit, sizeof(Hit));
  } else {
    if (offset < posting_list_utils::kSpecialHitsSize + sizeof(Hit::Value)) {
      ICING_LOG(ERROR) << "Not enough room to prepend Hit::Value at offset "
                       << offset << ".";
      return offset;
    }
    offset -= sizeof(Hit::Value);
    Hit::Value val = hit.value();
    memcpy(posting_list_buffer_ + offset, &val, sizeof(Hit::Value));
  }
  return offset;
}

Hit::Score PostingListUsed::ReadScore(uint32_t offset) const {
  if (offset + sizeof(Hit::Score) > size_in_bytes_) {
    ICING_LOG(FATAL)
        << "offset " << offset
        << " must not point past the end of the posting list of size "
        << size_in_bytes_ << ".";
  }
  Hit::Score score;
  memcpy(&score, posting_list_buffer_ + offset, sizeof(Hit::Score));
  return score;
}

}  // namespace lib
}  // namespace icing
