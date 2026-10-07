// Copyright (C) 2026 Google LLC
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

#include "icing/monkey_test/abstract_query_tree/monkey-or-query-node.h"

#include <cassert>
#include <cstddef>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-nary-node.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-query-node.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/proto/term.pb.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

MonkeyOrQueryNode::MonkeyOrQueryNode(
    std::vector<std::unique_ptr<MonkeyAbstractQueryNode>> child_nodes)
    : MonkeyAbstractNaryQueryNode(std::move(child_nodes)) {}

libtextclassifier3::StatusOr<std::vector<DocumentId>>
MonkeyOrQueryNode::ProcessMatchedDocIds(
    const InMemoryIcingSearchEngine* engine,
    const std::vector<std::unordered_set<DocumentId>>& child_matched_doc_ids)
    const {
  if (child_matched_doc_ids.empty()) {
    return std::vector<DocumentId>();
  }

  std::vector<DocumentId> results;
  std::unordered_set<DocumentId> union_result;

  for (const auto& child_matched_doc_id_set : child_matched_doc_ids) {
    for (DocumentId doc_id : child_matched_doc_id_set) {
      if (union_result.insert(doc_id).second) {
        results.push_back(doc_id);
      }
    }
  }
  return results;
}

std::string MonkeyOrQueryNode::GenerateQueryString() const {
  std::string query_string = "(";
  for (size_t i = 0; i < child_nodes_.size(); ++i) {
    if (i > 0) {
      absl_ports::StrAppend(&query_string, " OR ");
    }
    absl_ports::StrAppend(&query_string, "(",
                          child_nodes_[i]->GenerateQueryString(), ")");
  }
  absl_ports::StrAppend(&query_string, ")");
  return query_string;
}

}  // namespace lib
}  // namespace icing
