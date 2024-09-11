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

#ifndef ICING_INDEX_MAIN_POSTING_LIST_HIT_SERIALIZER_H_
#define ICING_INDEX_MAIN_POSTING_LIST_HIT_SERIALIZER_H_

#include <cstddef>
#include <cstdint>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/file/posting_list/posting-list-common.h"
#include "icing/file/posting_list/posting-list-used.h"
#include "icing/index/hit/hit.h"
#include "icing/legacy/index/icing-bit-util.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

// A serializer class to serialize hits to PostingListUsed. Layout described in
// comments in posting-list-hit-serializer.cc.
class PostingListHitSerializer : public PostingListSerializer {
 public:
  static constexpr uint32_t kSpecialHitsSize = kNumSpecialData * sizeof(Hit);

  struct DecodedHitInfo {
    // The decoded hit value.
    Hit::Value hit_value;

    // Size of the encoded hit in bytes.
    size_t encoded_size;
  };

  // Given the current hit value, encodes the next hit value for serialization
  // in the posting list.
  //
  // The encoded value is the varint-encoded delta between next_hit_value and
  // curr_hit_value.
  // - We add 1 to this delta so as to avoid getting a delta value of 0.
  // - This allows us to add duplicate hits with the same value, which is a
  //   valid case if we need to store hits with different flags that belong in
  //   the same section-id/doc-id combo.
  // - We cannot have an encoded hit delta with a value of 0 as 0 is currently
  //   used for padding the unused region in the posting list.
  //
  // REQUIRES: next_hit_value <= curr_hit_value AND
  //           curr_hit_value - next_hit_value <
  //              std::numeric_limits<Hit::Value>::max()
  //
  // RETURNS: next_hit_value's encoded length in bytes and writes the encoded
  //          value directly into encoded_buf_out.
  static size_t EncodeNextHitValue(Hit::Value next_hit_value,
                                   Hit::Value curr_hit_value,
                                   uint8_t* encoded_buf_out) {
    uint64_t delta = curr_hit_value - next_hit_value + 1;
    return VarInt::Encode(delta, encoded_buf_out);
  }

  // Given the current hit value, decodes the next hit value from an encoded
  // byte array buffer.
  //
  // RETURNS: DecodedHitInfo containing the decoded hit value and the value's
  //          encoded size in bytes.
  static DecodedHitInfo DecodeNextHitValue(const uint8_t* encoded_buf_in,
                                           Hit::Value curr_hit_value) {
    uint64_t delta;
    size_t delta_size = VarInt::Decode(encoded_buf_in, &delta);
    Hit::Value hit_value = curr_hit_value + delta - 1;
    return {hit_value, delta_size};
  }

  uint32_t GetDataTypeBytes() const override { return sizeof(Hit); }

  uint32_t GetMinPostingListSize() const override {
    static constexpr uint32_t kMinPostingListSize = kSpecialHitsSize;
    static_assert(sizeof(PostingListIndex) <= kMinPostingListSize,
                  "PostingListIndex must be small enough to fit in a "
                  "minimum-sized Posting List.");

    return kMinPostingListSize;
  }

  uint32_t GetMinPostingListSizeToFit(
      const PostingListUsed* posting_list_used) const override;

  uint32_t GetBytesUsed(
      const PostingListUsed* posting_list_used) const override;

  void Clear(PostingListUsed* posting_list_used) const override;

  libtextclassifier3::Status MoveFrom(PostingListUsed* dst,
                                      PostingListUsed* src) const override;

  // Prepend a hit to the posting list.
  //
  // RETURNS:
  //   - INVALID_ARGUMENT if !hit.is_valid() or if hit is not less than the
  //       previously added hit.
  //   - RESOURCE_EXHAUSTED if there is no more room to add hit to the posting
  //       list.
  libtextclassifier3::Status PrependHit(PostingListUsed* posting_list_used,
                                        const Hit& hit) const;

  // Retrieves the hits stored in the posting list.
  //
  // RETURNS:
  //   - On success, a vector of hits sorted by the reverse order of prepending.
  //   - INTERNAL_ERROR if the posting list has been corrupted somehow.
  libtextclassifier3::StatusOr<std::vector<Hit>> GetHits(
      const PostingListUsed* posting_list_used) const;

  // Same as GetHits but appends hits to hits_out.
  //
  // RETURNS:
  //   - On success, a vector of hits sorted by the reverse order of prepending.
  //   - INTERNAL_ERROR if the posting list has been corrupted somehow.
  libtextclassifier3::Status GetHits(const PostingListUsed* posting_list_used,
                                     std::vector<Hit>* hits_out) const;

  // Undo the last num_hits hits prepended. If num_hits > number of
  // hits we clear all hits.
  //
  // RETURNS:
  //   - OK on success
  //   - INTERNAL_ERROR if the posting list has been corrupted somehow.
  libtextclassifier3::Status PopFrontHits(PostingListUsed* posting_list_used,
                                          uint32_t num_hits) const;

 private:
  // Posting list layout formats:
  //
  // not_full
  //
  // +-----------------+----------------+-------+-----------------+
  // |hits-start-offset|Hit::kInvalidVal|xxxxxxx|(compressed) hits|
  // +-----------------+----------------+-------+-----------------+
  //
  // almost_full
  //
  // +-----------------+----------------+-------+-----------------+
  // |Hit::kInvalidVal |1st hit         |(pad)  |(compressed) hits|
  // +-----------------+----------------+-------+-----------------+
  //
  // full()
  //
  // +-----------------+----------------+-------+-----------------+
  // |1st hit          |2nd hit         |(pad)  |(compressed) hits|
  // +-----------------+----------------+-------+-----------------+
  //
  // The first two uncompressed hits also implicitly encode information about
  // the size of the compressed hits region.
  //
  // 1. If the posting list is NOT_FULL, then
  // posting_list_buffer_[0] contains the byte offset of the start of the
  // compressed hits - and, thus, the size of the compressed hits region is
  // size_in_bytes - posting_list_buffer_[0].
  //
  // 2. If posting list is ALMOST_FULL or FULL, then the compressed hits region
  // starts somewhere between [kSpecialHitsSize, kSpecialHitsSize + sizeof(Hit)
  // - 1] and ends at size_in_bytes - 1.
  //
  // Hit term frequencies are stored after the hit value, compressed or
  // uncompressed. For the first two special hits, we always have a
  // space for the term frequency. For hits in the compressed area, we only have
  // the term frequency following the hit value of hit.has_term_frequency() is
  // true. This allows good compression in the common case where hits don't have
  // a valid term frequency.
  //
  // EXAMPLE
  // Posting list storage. Posting list size: 20 bytes
  // EMPTY!
  // +--bytes 0-4--+----- 5-9 ------+---------------- 10-19 -----------------+
  // |     20      |Hit::kInvalidVal|                 0x000                  |
  // +-------------+----------------+----------------+-----------------------+
  //
  // Add Hit 0x07FFF998 (DocumentId = 12, SectionId = 3, Flags = 0)
  // NOT FULL!
  // +--bytes 0-4--+----- 5-9 ------+----- 10-15 -----+-------- 16-19 -------+
  // |     16      |Hit::kInvalidVal|      0x000      |       0x07FFF998     |
  // +-------------+----------------+-----------------+----------------------+
  //
  // Add Hit 0x07FFF684 (DocumentId = 18, SectionId = 0, Flags = 4,
  // TermFrequency=125)
  // (Hit 0x07FFF998 - Hit 0x07FFF684 = 788)
  // +--bytes 0-4--+----- 5-9 ------+-- 10-12 --+-- 13-16 --+- 17 -+-- 18-19 --+
  // |      13     |Hit::kInvalidVal|   0x000   | 0x07FFF684| 125  |    788    |
  // +-------------+----------------+-----------+-----------+------+-----------+
  //
  // Add Hit 0x07FFF4D2 (DocumentId = 22, SectionId = 10, Flags = 2)
  // (Hit 0x07FFF684 - Hit 0x07FFF4D2 = 434)
  // +--bytes 0-4--+--- 5-9 ----+-- 10 --+-- 11-14 -+- 15-16 -+- 17 -+- 18-19 -+
  // |      9      |Hit::kInvVal|  0x00  |0x07FFF4D2|   434   | 125  |   788   |
  // +-------------+------------+--------+----------+---------+------+---------+
  //
  // Add Hit 0x07FFF40E (DocumentId = 23, SectionId = 1, Flags = 6,
  // TermFrequency = 87)
  // (Hit 0x07FFF684 - Hit 0x07FFF4D2 = 196) ALMOST FULL!
  // +--bytes 0-4-+---- 5-9 ----+- 10-12 -+- 13-14 -+- 15-16 -+- 17 -+- 18-19 -+
  // |Hit::kInvVal|0x07FFF40E,87|  0x000  |    196  |   434   |  125 |   788   |
  // +-------------+------------+---------+---------+---------+------+---------+
  //
  // Add Hit 0x07FFF320 (DocumentId = 27, SectionId = 4, Flags = 0)
  // FULL!
  // +--bytes 0-4--+---- 5-9 ----+- 10-13 -+-- 14-15 -+- 16-17 -+- 18 -+- 19-20
  // -+ | 0x07FFF320  |0x07FFF40E,87|  0x000  |    196   |   434   |  125 | 788
  // |
  // +-------------+-------------+---------+----------+---------+------+---------+

  // Helpers to determine what state the posting list is in.
  bool IsFull(const PostingListUsed* posting_list_used) const {
    return GetSpecialHit(posting_list_used, /*index=*/0)
               .ValueOrDie()
               .is_valid() &&
           GetSpecialHit(posting_list_used, /*index=*/1)
               .ValueOrDie()
               .is_valid();
  }

  bool IsAlmostFull(const PostingListUsed* posting_list_used) const {
    return !GetSpecialHit(posting_list_used, /*index=*/0)
                .ValueOrDie()
                .is_valid();
  }

  bool IsEmpty(const PostingListUsed* posting_list_used) const {
    return GetSpecialHit(posting_list_used, /*index=*/0).ValueOrDie().value() ==
               posting_list_used->size_in_bytes() &&
           !GetSpecialHit(posting_list_used, /*index=*/1)
                .ValueOrDie()
                .is_valid();
  }

  // Returns false if both special hits are invalid or if the offset value
  // stored in the special hit is less than kSpecialHitsSize or greater than
  // posting_list_used->size_in_bytes(). Returns true, otherwise.
  bool IsPostingListValid(const PostingListUsed* posting_list_used) const;

  // Prepend hit to a posting list that is in the ALMOST_FULL state.
  // RETURNS:
  //  - OK, if successful
  //  - INVALID_ARGUMENT if hit is not less than the previously added hit.
  libtextclassifier3::Status PrependHitToAlmostFull(
      PostingListUsed* posting_list_used, const Hit& hit) const;

  // Prepend hit to a posting list that is in the EMPTY state. This will always
  // succeed because there are no pre-existing hits and no validly constructed
  // posting list could fail to fit one hit.
  void PrependHitToEmpty(PostingListUsed* posting_list_used,
                         const Hit& hit) const;

  // Prepend hit to a posting list that is in the NOT_FULL state.
  // RETURNS:
  //  - OK, if successful
  //  - INVALID_ARGUMENT if hit is not less than the previously added hit.
  libtextclassifier3::Status PrependHitToNotFull(
      PostingListUsed* posting_list_used, const Hit& hit,
      uint32_t offset) const;

  // Returns either 0 (full state), sizeof(Hit) (almost_full state) or
  // a byte offset between kSpecialHitsSize and
  // posting_list_used->size_in_bytes() (inclusive) (not_full state).
  uint32_t GetStartByteOffset(const PostingListUsed* posting_list_used) const;

  // Sets the special hits to properly reflect what offset is (see layout
  // comment for further details).
  //
  // Returns false if offset > posting_list_used->size_in_bytes() or offset is
  // (kSpecialHitsSize, sizeof(Hit)) or offset is (sizeof(Hit), 0). True,
  // otherwise.
  bool SetStartByteOffset(PostingListUsed* posting_list_used,
                          uint32_t offset) const;

  // Manipulate padded areas. We never store the same hit value twice
  // so a delta of 0 is a pad byte.

  // Returns offset of first non-pad byte.
  uint32_t GetPadEnd(const PostingListUsed* posting_list_used,
                     uint32_t offset) const;

  // Fill padding between offset start and offset end with 0s.
  // Returns false if end > posting_list_used->size_in_bytes(). True,
  // otherwise.
  bool PadToEnd(PostingListUsed* posting_list_used, uint32_t start,
                uint32_t end) const;

  // Helper for AppendHits/PopFrontHits. Adds limit number of hits to out or all
  // hits in the posting list if the posting list contains less than limit
  // number of hits. out can be NULL.
  //
  // NOTE: If called with limit=1, pop=true on a posting list that transitioned
  // from NOT_FULL directly to FULL, GetHitsInternal will not return the posting
  // list to NOT_FULL. Instead it will leave it in a valid state, but it will be
  // ALMOST_FULL.
  //
  // RETURNS:
  //   - OK on success
  //   - INTERNAL_ERROR if the posting list has been corrupted somehow.
  libtextclassifier3::Status GetHitsInternal(
      const PostingListUsed* posting_list_used, uint32_t limit, bool pop,
      std::vector<Hit>* out) const;

  // Retrieves the value stored in the index-th special hit.
  //
  // RETURNS:
  //   - A valid Hit, on success
  //   - INVALID_ARGUMENT if index is not less than kNumSpecialData
  libtextclassifier3::StatusOr<Hit> GetSpecialHit(
      const PostingListUsed* posting_list_used, uint32_t index) const;

  // Sets the value stored in the index-th special hit to val. If index is not
  // less than kSpecialHitSize / sizeof(Hit), this has no effect.
  bool SetSpecialHit(PostingListUsed* posting_list_used, uint32_t index,
                     const Hit& val) const;

  // Prepends hit to the memory region [offset - sizeof(Hit), offset] and
  // returns the new beginning of the padded region.
  //
  // RETURNS:
  //   - The new beginning of the padded region, if successful.
  //   - INVALID_ARGUMENT if hit will not fit (uncompressed) between offset and
  // kSpecialHitsSize
  libtextclassifier3::StatusOr<uint32_t> PrependHitUncompressed(
      PostingListUsed* posting_list_used, const Hit& hit,
      uint32_t offset) const;

  // If hit has the flags and/or term frequency field, consumes the flags and/or
  // term frequency at offset, updates hit to include the flag and/or term
  // frequency and updates offset to reflect that the flag and/or term frequency
  // fields have been consumed.
  //
  // RETURNS:
  //   - OK, if successful
  //   - INVALID_ARGUMENT if hit has a flags and/or term frequency field and
  //     offset + sizeof(Hit's flag) + sizeof(Hit's tf) >=
  //     posting_list_used->size_in_bytes()
  //     i.e. the posting list is not large enough to consume the hit's flags
  //     and term frequency fields
  libtextclassifier3::Status ConsumeFlagsAndTermFrequencyIfPresent(
      const PostingListUsed* posting_list_used, Hit* hit,
      uint32_t* offset) const;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_MAIN_POSTING_LIST_HIT_SERIALIZER_H_
