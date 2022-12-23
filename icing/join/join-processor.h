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

#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/proto/search.pb.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/store/document-store.h"

namespace icing {
namespace lib {

class JoinProcessor {
 public:
  static constexpr std::string_view kFullyQualifiedIdExpr =
      "this.fullyQualifiedId()";

  explicit JoinProcessor(const DocumentStore* doc_store)
      : doc_store_(doc_store) {}

  libtextclassifier3::StatusOr<std::vector<JoinedScoredDocumentHit>> Join(
      const JoinSpecProto& join_spec,
      std::vector<ScoredDocumentHit>&& parent_scored_document_hits,
      std::vector<ScoredDocumentHit>&& child_scored_document_hits);

 private:
  std::string FetchPropertyExpressionValue(
      const DocumentId& document_id,
      const std::string& property_expression) const;

  void UnescapeSeparator(std::string& property, const std::string& separator);

  std::vector<int> GetSeparatorLocations(const std::string& content,
                                         const std::string& separator) const;

  const DocumentStore* doc_store_;  // Does not own.
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_JOIN_JOIN_PROCESSOR_H_
