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

#ifndef ICING_INDEX_ITERATOR_DOCUMENT_FILTER_PREDICATE_H_
#define ICING_INDEX_ITERATOR_DOCUMENT_FILTER_PREDICATE_H_

#include <vector>

#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

// An interface for a predicate that determines whether a document, identified
// by its DocumentId, should be included in a result set.
class DocumentFilterPredicate {
 public:
  virtual ~DocumentFilterPredicate() = default;

  // Evaluates whether the given document_id satisfies the predicate. Returns
  // true if the document satisfies the predicate and should be included, false
  // otherwise.
  virtual bool operator()(DocumentId document_id) const = 0;
};

// Indicate that the iterator can internally handle filtering logic by itself.
//
// This is helpful when some iterators want to have better control for
// optimization. For example, embedding iterator will be able to filter out
// embedding hits from unwanted documents to avoid retrieving unnecessary
// vectors and calculate scores for them.
class DocHitInfoIteratorHandlingFilter : virtual public DocHitInfoIterator {
 protected:
  // After accepting a filter predicate, the iterator will behave equivalently
  // as if we had applied a filter with this predicate at the top of the
  // iterator.
  bool HandleFilter(const DocumentFilterPredicate* predicate) override {
    document_filter_predicates_.push_back(predicate);
    return true;
  }

  bool DoesDocumentPassAllFilters(DocumentId document_id) const {
    for (const DocumentFilterPredicate* predicate :
         document_filter_predicates_) {
      if (!(*predicate)(document_id)) {
        return false;
      }
    }
    return true;
  }

  // Does not own the pointers.
  std::vector<const DocumentFilterPredicate*> document_filter_predicates_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_ITERATOR_DOCUMENT_FILTER_PREDICATE_H_
