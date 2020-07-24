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

#ifndef ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_TERM_H_
#define ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_TERM_H_

#include <cstdint>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/index/lite/lite-index.h"
#include "icing/index/term-id-codec.h"
#include "icing/schema/section.h"

namespace icing {
namespace lib {

class DocHitInfoIteratorTerm : public DocHitInfoIterator {
 public:
  explicit DocHitInfoIteratorTerm(const TermIdCodec* term_id_codec,
                                  LiteIndex* lite_index, const std::string term,
                                  SectionIdMask section_restrict_mask)
      : term_(term),
        lite_index_(lite_index),
        cached_hits_idx_(-1),
        term_id_codec_(term_id_codec),
        num_advance_calls_(0),
        section_restrict_mask_(section_restrict_mask) {}

  libtextclassifier3::Status Advance() override;

  int32_t GetNumBlocksInspected() const override {
    // TODO(b/137862424): Implement this once the main index is added.
    return 0;
  }
  int32_t GetNumLeafAdvanceCalls() const override { return num_advance_calls_; }

 protected:
  // Add DocHitInfos corresponding to term_ to cached_hits_.
  virtual libtextclassifier3::Status RetrieveMoreHits() = 0;

  const std::string term_;
  LiteIndex* const lite_index_;
  // Stores hits retrieved from the index. This may only be a subset of the hits
  // that are present in the index. Current value pointed to by the Iterator is
  // tracked by cached_hits_idx_.
  std::vector<DocHitInfo> cached_hits_;
  int cached_hits_idx_;
  const TermIdCodec* term_id_codec_;
  int num_advance_calls_;
  // Mask indicating which sections hits should be considered for.
  // Ex. 0000 0000 0000 0010 means that only hits from section 1 are desired.
  const SectionIdMask section_restrict_mask_;
};

class DocHitInfoIteratorTermExact : public DocHitInfoIteratorTerm {
 public:
  explicit DocHitInfoIteratorTermExact(const TermIdCodec* term_id_codec,
                                       LiteIndex* lite_index,
                                       const std::string& term,
                                       SectionIdMask section_id_mask)
      : DocHitInfoIteratorTerm(term_id_codec, lite_index, term,
                               section_id_mask) {}

  std::string ToString() const override;

 protected:
  libtextclassifier3::Status RetrieveMoreHits() override;
};

class DocHitInfoIteratorTermPrefix : public DocHitInfoIteratorTerm {
 public:
  explicit DocHitInfoIteratorTermPrefix(const TermIdCodec* term_id_codec,
                                        LiteIndex* lite_index,
                                        const std::string& term,
                                        SectionIdMask section_id_mask)
      : DocHitInfoIteratorTerm(term_id_codec, lite_index, term,
                               section_id_mask) {}

  std::string ToString() const override;

 protected:
  libtextclassifier3::Status RetrieveMoreHits() override;

 private:
  // After retrieving DocHitInfos from the index, a DocHitInfo for docid 1 and
  // "foo" and a DocHitInfo for docid 1 and "fool". These DocHitInfos should be
  // merged.
  void SortAndDedupeDocumentIds();
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_TERM_H_
