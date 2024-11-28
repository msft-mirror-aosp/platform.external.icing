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

#include "icing/join/join-children-fetcher-impl-deprecated.h"

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

/* static */ libtextclassifier3::StatusOr<
    std::unique_ptr<JoinChildrenFetcherImplDeprecated>>
JoinChildrenFetcherImplDeprecated::Create(
    const JoinSpecProto& join_spec,
    std::unordered_map<DocumentId, std::vector<ScoredDocumentHit>>&&
        map_joinable_qualified_id) {
  // TODO(b/256022027): So far we only support kQualifiedIdExpr for
  // parent_property_expression, we could support more.
  if (join_spec.parent_property_expression() != kQualifiedIdExpr) {
    return absl_ports::UnimplementedError(absl_ports::StrCat(
        "Parent property expression must be ", kQualifiedIdExpr));
  }

  return std::unique_ptr<JoinChildrenFetcherImplDeprecated>(
      new JoinChildrenFetcherImplDeprecated(
          join_spec, std::move(map_joinable_qualified_id)));
}

libtextclassifier3::StatusOr<std::vector<ScoredDocumentHit>>
JoinChildrenFetcherImplDeprecated::GetChildren(DocumentId parent_doc_id) const {
  if (auto iter = map_joinable_qualified_id_.find(parent_doc_id);
      iter != map_joinable_qualified_id_.end()) {
    return iter->second;
  }
  return std::vector<ScoredDocumentHit>();
}

}  // namespace lib
}  // namespace icing
