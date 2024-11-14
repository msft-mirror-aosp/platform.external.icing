// Copyright (C) 2024 Google LLC
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

#ifndef ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_BY_URI_H_
#define ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_BY_URI_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/proto/search.pb.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"

namespace icing {
namespace lib {

class DocHitInfoIteratorByUri : public DocHitInfoIterator {
 public:
  // Creates a DocHitInfoIteratorByUri based on the given search_spec.
  //
  // Returns:
  //   - A DocHitInfoIteratorByUri instance on success.
  //   - INVALID_ARGUMENT error if the search_spec's document_uri_filters is
  //     empty or if any namespace in the document_uri_filters has no document
  //     URIs specified.
  static libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIteratorByUri>>
  Create(const DocumentStore* document_store,
         const SearchSpecProto& search_spec);

  libtextclassifier3::Status Advance() override;

  libtextclassifier3::StatusOr<TrimmedNode> TrimRightMostNode() && override {
    return absl_ports::InvalidArgumentError(
        "DocHitInfoIteratorByUri should not be used in suggestion.");
  }

  void MapChildren(const ChildrenMapper& mapper) override {}

  CallStats GetCallStats() const override {
    return CallStats(
        /*num_leaf_advance_calls_lite_index_in=*/0,
        /*num_leaf_advance_calls_main_index_in=*/0,
        /*num_leaf_advance_calls_integer_index_in=*/0,
        /*num_leaf_advance_calls_no_index_in=*/num_advance_calls_,
        /*num_blocks_inspected_in=*/0);
  }

  std::string ToString() const override { return "uri_iterator"; }

 private:
  explicit DocHitInfoIteratorByUri(std::vector<DocumentId> target_document_ids)
      : target_document_ids_(std::move(target_document_ids)),
        current_document_id_index_(-1),
        num_advance_calls_(0) {}

  std::vector<DocumentId> target_document_ids_;
  int current_document_id_index_;

  int num_advance_calls_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_BY_URI_H_
