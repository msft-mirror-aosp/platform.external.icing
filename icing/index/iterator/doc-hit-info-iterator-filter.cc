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

#include "icing/index/iterator/doc-hit-info-iterator-filter.h"

#include <memory>
#include <string>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/index/iterator/document-filter-predicate.h"
#include "icing/store/document-id.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

// Some children iterators (currently only DocHitInfoIteratorEmbedding) can
// internally handle the filter predicate to accelerate the search. Let's dfs to
// all children iterators to try to provide the filter predicate.
//
// Returns true if all children iterators have accepted the filter predicate, in
// which case we no longer need to apply the filter at the top level.
bool PassFilterPredicateToChildrenAndHandle(
    DocHitInfoIterator* iterator, const DocumentFilterPredicate* predicate) {
  // If the predicate is accepted, we can stop here.
  if (iterator->HandleFilter(predicate)) {
    return true;
  }

  // Now, we know that the iterator cannot internally handle the filter
  // predicate.
  // If it has no children or the iterator cannot pass the filter predicate
  // through (e.g., NOT iterator), return false to indicate that we should apply
  // the filter at the top level.
  if (iterator->GetChildren().empty() ||
      !iterator->CanPassFilterPredicateThrough()) {
    return false;
  }

  // Continue to pass the filter predicate to children iterators.
  bool all_children_accepted = true;
  for (std::unique_ptr<DocHitInfoIterator>* child : iterator->GetChildren()) {
    all_children_accepted &=
        PassFilterPredicateToChildrenAndHandle(child->get(), predicate);
  }
  return all_children_accepted;
}

/* static */ std::unique_ptr<DocHitInfoIterator>
DocHitInfoIteratorFilter::ApplyFilter(
    std::unique_ptr<DocHitInfoIterator> iterator,
    const DocumentFilterPredicate* predicate,
    bool enable_passing_filter_to_children) {
  if (enable_passing_filter_to_children &&
      PassFilterPredicateToChildrenAndHandle(iterator.get(), predicate)) {
    return iterator;
  }
  return std::unique_ptr<DocHitInfoIteratorFilter>(
      new DocHitInfoIteratorFilter(std::move(iterator), predicate));
}

libtextclassifier3::Status DocHitInfoIteratorFilter::Advance() {
  while (delegate_->Advance().ok()) {
    if (!(*predicate_)(delegate_->doc_hit_info().document_id())) {
      continue;
    }
    // Satisfied all our specified filters
    doc_hit_info_ = delegate_->doc_hit_info();
    return libtextclassifier3::Status::OK;
  }

  // Didn't find anything on the delegate iterator.
  doc_hit_info_ = DocHitInfo(kInvalidDocumentId);
  return absl_ports::ResourceExhaustedError("No more DocHitInfos in iterator");
}

libtextclassifier3::StatusOr<DocHitInfoIterator::TrimmedNode>
DocHitInfoIteratorFilter::TrimRightMostNode() && {
  ICING_ASSIGN_OR_RETURN(TrimmedNode trimmed_delegate,
                         std::move(*delegate_).TrimRightMostNode());
  if (trimmed_delegate.iterator_ != nullptr) {
    trimmed_delegate.iterator_ =
        std::unique_ptr<DocHitInfoIteratorFilter>(new DocHitInfoIteratorFilter(
            std::move(trimmed_delegate.iterator_), predicate_));
  }
  return trimmed_delegate;
}

std::string DocHitInfoIteratorFilter::ToString() const {
  return delegate_->ToString();
}

}  // namespace lib
}  // namespace icing
