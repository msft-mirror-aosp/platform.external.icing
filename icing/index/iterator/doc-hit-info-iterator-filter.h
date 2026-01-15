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

#ifndef ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_FILTER_H_
#define ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_FILTER_H_

#include <memory>
#include <string>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/index/iterator/document-filter-predicate.h"
#include "icing/schema/section.h"

namespace icing {
namespace lib {

// A iterator that helps filter out DocHitInfos by a given predicate.
//
// To maintain the correct semantics of section restrictions, it implements
// DocHitInfoIteratorSectionRestrictionApplyToChildren to pass down section
// restrictions to child iterators.
class DocHitInfoIteratorFilter
    : public DocHitInfoIteratorSectionRestrictionApplyToChildren {
 public:
  static std::unique_ptr<DocHitInfoIterator> ApplyFilter(
      std::unique_ptr<DocHitInfoIterator> iterator,
      const DocumentFilterPredicate* predicate,
      bool enable_passing_filter_to_children);

  libtextclassifier3::Status Advance() override;

  libtextclassifier3::StatusOr<TrimmedNode> TrimRightMostNode() && override;

  std::vector<std::unique_ptr<DocHitInfoIterator>*> GetChildren() override {
    return {&delegate_};
  }

  CallStats GetCallStats() const override { return delegate_->GetCallStats(); }

  std::string ToString() const override;

  void PopulateMatchedTermsStats(
      std::vector<TermMatchInfo>* matched_terms_stats,
      SectionIdMask filtering_section_mask = kSectionIdMaskAll) const override {
    delegate_->PopulateMatchedTermsStats(matched_terms_stats,
                                         filtering_section_mask);
  }

 private:
  explicit DocHitInfoIteratorFilter(
      std::unique_ptr<DocHitInfoIterator> delegate,
      const DocumentFilterPredicate* predicate)
      : delegate_(std::move(delegate)), predicate_(predicate) {}

  std::unique_ptr<DocHitInfoIterator> delegate_;
  const DocumentFilterPredicate* predicate_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_FILTER_H_
