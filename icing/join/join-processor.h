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

#ifndef ICING_JOIN_JOIN_PROCESSOR_H_
#define ICING_JOIN_JOIN_PROCESSOR_H_

#include <string>
#include <string_view>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/proto/search.pb.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/store/document-store.h"

namespace icing {
namespace lib {

class JoinProcessor {
 public:
  static constexpr std::string_view kQualifiedIdExpr = "this.qualifiedId()";

  explicit JoinProcessor(const DocumentStore* doc_store)
      : doc_store_(doc_store) {}

  libtextclassifier3::StatusOr<std::vector<JoinedScoredDocumentHit>> Join(
      const JoinSpecProto& join_spec,
      std::vector<ScoredDocumentHit>&& parent_scored_document_hits,
      std::vector<ScoredDocumentHit>&& child_scored_document_hits);

 private:
  // Loads a document and uses a property expression to fetch the value of the
  // property from the document. The property expression may refer to nested
  // document properties.
  // Note: currently we only support single joining, so we use the first element
  // (index 0) for any repeated values.
  //
  // TODO(b/256022027): validate joinable property (and its upper-level) should
  //                    not have REPEATED cardinality.
  //
  // Returns:
  //   "" on document load error.
  //   "" if the property path is not found in the document.
  std::string FetchPropertyExpressionValue(
      const DocumentId& document_id,
      const std::string& property_expression) const;

  const DocumentStore* doc_store_;  // Does not own.
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_JOIN_JOIN_PROCESSOR_H_
