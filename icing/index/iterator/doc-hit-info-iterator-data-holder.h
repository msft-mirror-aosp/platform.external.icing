// Copyright (C) 2025 Google LLC
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

#ifndef ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_DATA_HOLDER_H_
#define ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_DATA_HOLDER_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/schema/section.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

// An iterator that simply takes ownership of an object.
template <typename T>
class DocHitInfoIteratorDataHolder : public DocHitInfoIterator {
 public:
  explicit DocHitInfoIteratorDataHolder(
      std::unique_ptr<DocHitInfoIterator> delegate, std::unique_ptr<T> data)
      : delegate_(std::move(delegate)), data_(std::move(data)) {}

  libtextclassifier3::Status Advance() override {
    auto result = delegate_->Advance();
    doc_hit_info_ = delegate_->doc_hit_info();
    return result;
  }

  libtextclassifier3::StatusOr<TrimmedNode> TrimRightMostNode() && override {
    ICING_ASSIGN_OR_RETURN(TrimmedNode trimmed_delegate,
                           std::move(*delegate_).TrimRightMostNode());
    if (trimmed_delegate.iterator_ != nullptr) {
      trimmed_delegate.iterator_ =
          std::make_unique<DocHitInfoIteratorDataHolder>(
              std::move(trimmed_delegate.iterator_), std::move(data_));
    }
    return trimmed_delegate;
  }

  void MapChildren(const ChildrenMapper& mapper) override {
    delegate_ = mapper(std::move(delegate_));
  }

  CallStats GetCallStats() const override { return delegate_->GetCallStats(); }

  std::string ToString() const override { return delegate_->ToString(); }

  void PopulateMatchedTermsStats(
      std::vector<TermMatchInfo>* matched_terms_stats,
      SectionIdMask filtering_section_mask) const override {
    return delegate_->PopulateMatchedTermsStats(matched_terms_stats,
                                                filtering_section_mask);
  }

 private:
  std::unique_ptr<DocHitInfoIterator> delegate_;
  std::unique_ptr<T> data_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_DATA_HOLDER_H_
