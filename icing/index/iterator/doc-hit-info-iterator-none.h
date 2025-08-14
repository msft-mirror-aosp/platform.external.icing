// Copyright (C) 2023 Google LLC
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

#ifndef THIRD_PARTY_ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_NONE_H_
#define THIRD_PARTY_ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_NONE_H_

#include <memory>
#include <string>
#include <vector>

#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/status.h"
#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/statusor.h"
#include "third_party/icing/absl_ports/canonical_errors.h"
#include "third_party/icing/index/iterator/doc-hit-info-iterator.h"

namespace icing {
namespace lib {

// Iterator that will return no results.
class DocHitInfoIteratorNone
    : public DocHitInfoIteratorSectionRestrictionNotApplicable {
 public:
  libtextclassifier3::Status Advance() override {
    return absl_ports::ResourceExhaustedError(
        "DocHitInfoIterator NONE has no hits.");
  }

  libtextclassifier3::StatusOr<TrimmedNode> TrimRightMostNode() && override {
    TrimmedNode node = {nullptr, /*term=*/"", /*term_start_index_=*/0,
                        /*unnormalized_term_length_=*/0};
    return node;
  }

  std::vector<std::unique_ptr<DocHitInfoIterator>*> GetChildren() override {
    return {};
  }

  CallStats GetCallStats() const override { return CallStats(); }

  std::string ToString() const override { return "(NONE)"; }
};

}  // namespace lib
}  // namespace icing

#endif  // THIRD_PARTY_ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_NONE_H_
