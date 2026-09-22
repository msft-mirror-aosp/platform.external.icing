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

#ifndef ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_ABSTRACT_NARY_NODE_H_
#define ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_ABSTRACT_NARY_NODE_H_

#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-query-node.h"
#include "icing/store/document-id.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

// Representation of n-ary nodes in the abstract query tree.
//
// N-ary nodes have at least one child node and perform operations over
// the results of their child nodes.
class MonkeyAbstractNaryQueryNode : public MonkeyAbstractQueryNode {
 public:
  explicit MonkeyAbstractNaryQueryNode(
      std::vector<std::unique_ptr<MonkeyAbstractQueryNode>> child_nodes)
      : child_nodes_(std::move(child_nodes)) {}

  libtextclassifier3::StatusOr<std::vector<DocumentId>> EvaluateQuery(
      const InMemoryIcingSearchEngine* engine) const override {
    std::vector<std::unordered_set<DocumentId>> child_matched_doc_ids;
    for (const auto& child_node : child_nodes_) {
      ICING_ASSIGN_OR_RETURN(std::vector<DocumentId> matched_doc_ids,
                             child_node->EvaluateQuery(engine));
      child_matched_doc_ids.push_back(std::unordered_set<DocumentId>(
          matched_doc_ids.begin(), matched_doc_ids.end()));
    }
    return ProcessMatchedDocIds(engine, child_matched_doc_ids);
  }

 protected:
  // Processes the matched document IDs from all child nodes and returns the
  // result.
  virtual libtextclassifier3::StatusOr<std::vector<DocumentId>>
  ProcessMatchedDocIds(const InMemoryIcingSearchEngine* engine,
                       const std::vector<std::unordered_set<DocumentId>>&
                           child_matched_doc_ids) const = 0;

  std::vector<std::unique_ptr<MonkeyAbstractQueryNode>> child_nodes_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_ABSTRACT_NARY_NODE_H_
