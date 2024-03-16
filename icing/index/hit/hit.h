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

#ifndef ICING_INDEX_HIT_HIT_H_
#define ICING_INDEX_HIT_HIT_H_

#include <array>
#include <cstdint>
#include <cstring>
#include <limits>

#include "icing/legacy/core/icing-packed-pod.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

// BasicHit is a specific encoding that refers to content within a document. A
// basic hit consists of:
// - a DocumentId
// - a SectionId
// referring to the document and section that the hit corresponds to.
//
// The hit is the most basic unit of the index and, when grouped together by
// term, can be used to encode what terms appear in what documents.
//
// BasicHit is for indices (e.g. numeric index) that don't require term
// frequency.
class BasicHit {
 public:
  // The datatype used to encode BasicHit information: the document_id and
  // section_id.
  using Value = uint32_t;

  // WARNING: Changing this value will invalidate any pre-existing posting lists
  // on user devices.
  //
  // kInvalidValue contains:
  // - 0 for unused bits. Note that unused bits are always 0 for both valid and
  //   invalid BasicHit values.
  // - Inverted kInvalidDocumentId
  // - SectionId 0 (valid), which is ok because inverted kInvalidDocumentId has
  //   already invalidated the value. In fact, we currently use all 2^6 section
  //   ids and there is no "invalid section id", so it doesn't matter what
  //   SectionId we set for kInvalidValue.
  static constexpr Value kInvalidValue = 0;

  explicit BasicHit(SectionId section_id, DocumentId document_id);

  explicit BasicHit() : value_(kInvalidValue) {}

  bool is_valid() const { return value_ != kInvalidValue; }
  Value value() const { return value_; }
  DocumentId document_id() const;
  SectionId section_id() const;

  bool operator<(const BasicHit& h2) const { return value_ < h2.value_; }
  bool operator==(const BasicHit& h2) const { return value_ == h2.value_; }

 private:
  // Value bits layout: 4 unused + 22 document_id + 6 section id.
  //
  // The Value is guaranteed to be an unsigned integer, but its size and the
  // information it stores may change if the Hit's encoding format is changed.
  Value value_;
} __attribute__((packed));
static_assert(sizeof(BasicHit) == 4, "");

// Hit is a specific encoding that refers to content within a document. A hit
// consists of:
// - a DocumentId
// - a SectionId
//   - referring to the document and section that the hit corresponds to
// - Metadata about the hit:
//   - whether the Hit does not appear exactly in the document, but instead
//     represents a term that is a prefix of a term in the document
//     (is_prefix_hit)
//   - whether the Hit came from a section that has prefix expansion enabled
//     (is_in_prefix_section)
//   - whether the Hit has set any bitmask flags (has_flags)
//   - bitmasks in flags fields:
//     - whether the Hit has a TermFrequency other than the default value
//       (has_term_frequency)
//   - a term frequency for the hit
//
// The hit is the most basic unit of the index and, when grouped together by
// term, can be used to encode what terms appear in what documents.
class Hit {
 public:
  // The datatype used to encode Hit information: the document_id, section_id,
  // and 3 flags: is_prefix_hit, is_hit_in_prefix_section and has_flags flag.
  //
  // The Value is guaranteed to be an unsigned integer, but its size and the
  // information it stores may change if the Hit's encoding format is changed.
  using Value = uint32_t;

  // WARNING: Changing this value will invalidate any pre-existing posting lists
  // on user devices.
  //
  // kInvalidValue contains:
  // - 0 for unused bits. Note that unused bits are always 0 for both valid and
  //   invalid Hit values.
  // - Inverted kInvalidDocumentId
  // - SectionId 0 (valid), which is ok because inverted kInvalidDocumentId has
  //   already invalidated the value. In fact, we currently use all 2^6 section
  //   ids and there is no "invalid section id", so it doesn't matter what
  //   SectionId we set for kInvalidValue.
  static constexpr Value kInvalidValue = 0;
  // Docs are sorted in reverse, and 0 is never used as the inverted
  // DocumentId (because it is the inverse of kInvalidValue), so it is always
  // the max in a descending sort.
  static constexpr Value kMaxDocumentIdSortValue = 0;

  enum FlagOffsetsInFlagsField {
    // Whether or not the hit has a term_frequency other than
    // kDefaultTermFrequency.
    kHasTermFrequency = 0,
    kNumFlagsInFlagsField = 1,
  };

  enum FlagOffsetsInValueField {
    // Whether or not the hit has a flags value other than kNoEnabledFlags (i.e.
    // it has flags enabled in the flags field)
    kHasFlags = 0,
    // This hit, whether exact or not, came from a prefixed section and will
    // need to be backfilled into branching posting lists if/when those are
    // created.
    kInPrefixSection = 1,
    // This hit represents a prefix of a longer term. If exact matches are
    // required, then this hit should be ignored.
    kPrefixHit = 2,
    kNumFlagsInValueField = 3,
  };
  static_assert(kDocumentIdBits + kSectionIdBits + kNumFlagsInValueField <=
                    sizeof(Hit::Value) * 8,
                "HitOverflow");
  static_assert(kDocumentIdBits == 22, "");
  static_assert(kSectionIdBits == 6, "");
  static_assert(kNumFlagsInValueField == 3, "");

  // The datatype used to encode additional bit-flags in the Hit.
  // This is guaranteed to be an unsigned integer, but its size may change if
  // more flags are introduced in the future and require more bits to encode.
  using Flags = uint8_t;
  static constexpr Flags kNoEnabledFlags = 0;

  // The Term Frequency of a Hit.
  // This is guaranteed to be an unsigned integer, but its size may change if we
  // need to expand the max term-frequency.
  using TermFrequency = uint8_t;
  using TermFrequencyArray = std::array<Hit::TermFrequency, kTotalNumSections>;
  // Max TermFrequency is 255.
  static constexpr TermFrequency kMaxTermFrequency =
      std::numeric_limits<TermFrequency>::max();
  static constexpr TermFrequency kDefaultTermFrequency = 1;
  static constexpr TermFrequency kNoTermFrequency = 0;

  explicit Hit(Value value)
      : Hit(value, kNoEnabledFlags, kDefaultTermFrequency) {}
  explicit Hit(Value value, Flags flags, TermFrequency term_frequency);
  explicit Hit(SectionId section_id, DocumentId document_id,
               TermFrequency term_frequency, bool is_in_prefix_section,
               bool is_prefix_hit);

  bool is_valid() const { return value() != kInvalidValue; }

  Value value() const {
    Value value;
    memcpy(&value, value_.data(), sizeof(value));
    return value;
  }

  DocumentId document_id() const;
  SectionId section_id() const;
  bool is_prefix_hit() const;
  bool is_in_prefix_section() const;
  // Whether or not the hit has any flags set to true.
  bool has_flags() const;

  Flags flags() const { return flags_; }
  // Whether or not the hit contains a valid term frequency.
  bool has_term_frequency() const;

  TermFrequency term_frequency() const { return term_frequency_; }

  // Returns true if the flags values across the Hit's value_, term_frequency_
  // and flags_ fields are consistent.
  bool CheckFlagsAreConsistent() const;

  // Creates a new hit based on old_hit but with new_document_id set.
  static Hit TranslateHit(Hit old_hit, DocumentId new_document_id);

  bool operator<(const Hit& h2) const {
    if (value() != h2.value()) {
      return value() < h2.value();
    }
    return flags() < h2.flags();
  }
  bool operator==(const Hit& h2) const {
    return value() == h2.value() && flags() == h2.flags();
  }

  struct EqualsDocumentIdAndSectionId {
    bool operator()(const Hit& hit1, const Hit& hit2) const;
  };

 private:
  // Value, Flags and TermFrequency must be in this order.
  // Value bits layout: 1 unused + 22 document_id + 6 section_id + 1
  // is_prefix_hit + 1 is_in_prefix_section + 1 has_flags.
  std::array<char, sizeof(Value)> value_;
  // Flags bits layout: 1 reserved + 6 unused + 1 has_term_frequency.
  // The left-most bit is reserved for chaining additional fields in case of
  // future hit expansions.
  Flags flags_;
  TermFrequency term_frequency_;
};
static_assert(sizeof(Hit) == 6, "");
// TODO(b/138991332) decide how to remove/replace all is_packed_pod assertions.
static_assert(icing_is_packed_pod<Hit>::value, "go/icing-ubsan");

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_HIT_HIT_H_
