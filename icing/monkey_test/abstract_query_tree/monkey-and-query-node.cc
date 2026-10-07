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

#include "icing/monkey_test/abstract_query_tree/monkey-and-query-node.h"

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

MonkeyAndQueryNode::MonkeyAndQueryNode(
    std::vector<std::unique_ptr<MonkeyAbstractQueryNode>> child_nodes)
    : MonkeyAbstractNaryQueryNode(std::move(child_nodes)) {}

libtextclassifier3::StatusOr<std::vector<DocumentId>>
MonkeyAndQueryNode::ProcessMatchedDocIds(
    const InMemoryIcingSearchEngine* engine,
    const std::vector<std::unordered_set<DocumentId>>& child_matched_doc_ids)
    const {
  if (child_matched_doc_ids.empty()) {
    return std::vector<DocumentId>();
  }

  std::vector<DocumentId> intersection_results;
  const std::unordered_set<DocumentId>& first_set = child_matched_doc_ids[0];

  for (DocumentId doc_id : first_set) {
    bool in_all_sets = true;
    for (size_t i = 1; i < child_matched_doc_ids.size(); ++i) {
      if (child_matched_doc_ids[i].find(doc_id) ==
          child_matched_doc_ids[i].end()) {
        in_all_sets = false;
        break;
      }
    }
    if (in_all_sets) {
      intersection_results.push_back(doc_id);
    }
  }
  return intersection_results;
}

std::string MonkeyAndQueryNode::GenerateQueryString() const {
  std::string query_string = "(";
  for (size_t i = 0; i < child_nodes_.size(); ++i) {
    if (i > 0) {
      absl_ports::StrAppend(&query_string, " AND ");
    }
    absl_ports::StrAppend(&query_string, "(",
                          child_nodes_[i]->GenerateQueryString(), ")");
  }
  absl_ports::StrAppend(&query_string, ")");
  return query_string;
}

}  // namespace lib
}  // namespace icing
