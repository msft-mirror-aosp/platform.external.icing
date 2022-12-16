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

#ifndef ICING_INDEX_NUMERIC_DOC_HIT_INFO_ITERATOR_NUMERIC_H_
#define ICING_INDEX_NUMERIC_DOC_HIT_INFO_ITERATOR_NUMERIC_H_

#include <memory>
#include <string>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/index/numeric/numeric-index.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

template <typename T>
class DocHitInfoIteratorNumeric : public DocHitInfoIterator {
 public:
  explicit DocHitInfoIteratorNumeric(
      std::unique_ptr<typename NumericIndex<T>::Iterator> numeric_index_iter)
      : numeric_index_iter_(std::move(numeric_index_iter)) {}

  libtextclassifier3::Status Advance() override {
    ICING_RETURN_IF_ERROR(numeric_index_iter_->Advance());

    doc_hit_info_ = numeric_index_iter_->GetDocHitInfo();
    return libtextclassifier3::Status::OK;
  }

  int32_t GetNumBlocksInspected() const override { return 0; }

  int32_t GetNumLeafAdvanceCalls() const override { return 0; }

  std::string ToString() const override { return "test"; }

  void PopulateMatchedTermsStats(
      std::vector<TermMatchInfo>* matched_terms_stats,
      SectionIdMask filtering_section_mask = kSectionIdMaskAll) const override {
    // For numeric hit iterator, this should do nothing since there is no term.
  }

 private:
  std::unique_ptr<typename NumericIndex<T>::Iterator> numeric_index_iter_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_NUMERIC_DOC_HIT_INFO_ITERATOR_NUMERIC_H_