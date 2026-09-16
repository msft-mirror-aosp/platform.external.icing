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

#ifndef ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_OR_QUERY_NODE_H_
#define ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_OR_QUERY_NODE_H_

#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-nary-node.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-query-node.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

class InMemoryIcingSearchEngine;

// MonkeyNode that represents logical OR over subqueries. This node is
// equivalent to androidx.appsearch.ast.OrNode in the AppSearch AST library.
class MonkeyOrQueryNode : public MonkeyAbstractNaryQueryNode {
 public:
  explicit MonkeyOrQueryNode(
      std::vector<std::unique_ptr<MonkeyAbstractQueryNode>> child_nodes);

  std::string GenerateQueryString() const override;

 private:
  libtextclassifier3::StatusOr<std::vector<DocumentId>> ProcessMatchedDocIds(
      const InMemoryIcingSearchEngine* engine,
      const std::vector<std::unordered_set<DocumentId>>& child_matched_doc_ids)
      const override;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_MONKEY_TEST_ABSTRACT_QUERY_TREE_MONKEY_OR_QUERY_NODE_H_
