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

#include <cstdint>
#include <limits>

#include "icing/legacy/core/icing-packed-pod.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

// Hit is a specific encoding that refers to content within a document. A hit
// consists of:
// - a DocumentId
// - a SectionId
// referring to the document and section that the hit corresponds to, as well as
// metadata about the hit:
// - whether the Hit has a Score other than the default value
// - whether the Hit does not appear exactly in the document, but instead
//   represents a term that is a prefix of a term in the document
// - whether the Hit came from a section that has prefix expansion enabled
// and a score for the hit. Ranging from [0,255] a higher score indicates a
// higher quality hit.
// The hit is the most basic unit of the index and, when grouped together by
// term, can be used to encode what terms appear in what documents.
class Hit {
 public:
  // The datatype used to encode Hit information: the document_id, section_id
  // and the has_score, prefix hit and in prefix section flags.
  using Value = uint32_t;

  // WARNING: Changing this value will invalidate any pre-existing posting lists
  // on user devices.
  static constexpr Value kInvalidValue = std::numeric_limits<Value>::max();
  // Docs are sorted in reverse, and 0 is never used as the inverted
  // DocumentId (because it is the inverse of kInvalidValue), so it is always
  // the max in a descending sort.
  static constexpr Value kMaxDocumentIdSortValue = 0;

  // A score reflecting the "quality" of this hit. The higher the score, the
  // higher quality the hit.
  using Score = uint8_t;
  // By default, hits are given the highest possible score.
  static constexpr Score kMaxHitScore = std::numeric_limits<Score>::max();

  explicit Hit(Value value = kInvalidValue, Score score = kMaxHitScore)
      : value_(value), score_(score) {}
  Hit(SectionId section_id, DocumentId document_id, Score score,
      bool in_prefix_section = false, bool is_prefix_hit = false);

  bool is_valid() const { return value() != kInvalidValue; }
  Value value() const { return value_; }
  DocumentId document_id() const;
  SectionId section_id() const;
  // Whether or not the hit contains a non-default score. Hits with non-default
  // score are considered to be of lower quality.
  bool has_score() const;
  Score score() const { return score_; }
  bool is_prefix_hit() const;
  bool is_in_prefix_section() const;

  bool operator<(const Hit& h2) const { return value() < h2.value(); }
  bool operator==(const Hit& h2) const { return value() == h2.value(); }

  struct EqualsDocumentIdAndSectionId {
    bool operator()(const Hit& hit1, const Hit& hit2) const;
  };

 private:
  // Value and score must be in this order.
  // Value bits layout: 5 unused + 20 document_id + 4 section id + 3 flags.
  Value value_;
  Score score_;
} __attribute__((packed));
static_assert(sizeof(Hit) == 5, "");
// TODO(b/138991332) decide how to remove/replace all is_packed_pod assertions.
static_assert(icing_is_packed_pod<Hit>::value, "go/icing-ubsan");

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_HIT_HIT_H_
