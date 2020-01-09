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

#include "icing/index/hit/doc-hit-info.h"

#include "icing/legacy/core/icing-string-util.h"

namespace icing {
namespace lib {

bool DocHitInfo::operator<(const DocHitInfo& other) const {
  if (document_id() != other.document_id()) {
    // Sort by document_id descending. This mirrors how the individual hits that
    // are collapsed into this DocHitInfo would sort with other hits -
    // document_ids are inverted when encoded in hits. Hits are encoded this way
    // because they are appended to posting lists and the most recent value
    // appended to a posting list must have the smallest encoded value of any
    // hit on the posting list.
    return document_id() > other.document_id();
  }
  if (hit_section_ids_mask() != other.hit_section_ids_mask()) {
    return hit_section_ids_mask() < other.hit_section_ids_mask();
  }
  // Doesn't matter which way we compare this array, as long as
  // DocHitInfo is unequal when it is unequal.
  return memcmp(max_hit_score_, other.max_hit_score_, sizeof(max_hit_score_)) <
         0;
}

void DocHitInfo::UpdateSection(SectionId section_id, Hit::Score hit_score) {
  SectionIdMask section_id_mask = (1u << section_id);
  if (hit_section_ids_mask() & section_id_mask) {
    max_hit_score_[section_id] =
        std::max(max_hit_score_[section_id], hit_score);
  } else {
    max_hit_score_[section_id] = hit_score;
    hit_section_ids_mask_ |= section_id_mask;
  }
}

void DocHitInfo::MergeSectionsFrom(const DocHitInfo& other) {
  SectionIdMask other_mask = other.hit_section_ids_mask();
  while (other_mask) {
    SectionId section_id = __builtin_ctz(other_mask);
    UpdateSection(section_id, other.max_hit_score(section_id));
    other_mask &= ~(1u << section_id);
  }
}

}  // namespace lib
}  // namespace icing
