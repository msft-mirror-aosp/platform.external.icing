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

#include "icing/monkey_test/abstract_query_tree/monkey-not-query-node.h"

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-nary-node.h"
#include "icing/monkey_test/abstract_query_tree/monkey-abstract-query-node.h"
#include "icing/monkey_test/monkey-test-util.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/term.pb.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

MonkeyNotQueryNode::MonkeyNotQueryNode(
    std::unique_ptr<MonkeyAbstractQueryNode> value_node,
    std::vector<std::string> document_namespaces,
    std::vector<std::string> document_schema_types)
    : MonkeyAbstractNaryQueryNode([&value_node] {
        std::vector<std::unique_ptr<MonkeyAbstractQueryNode>> child_nodes;
        child_nodes.push_back(std::move(value_node));
        return child_nodes;
      }()),
      document_namespaces_(std::move(document_namespaces)),
      document_schema_types_(std::move(document_schema_types)) {}

MonkeyNotQueryNode::MonkeyNotQueryNode(
    std::unique_ptr<MonkeyAbstractQueryNode> value_node)
    : MonkeyNotQueryNode(std::move(value_node), /*document_namespaces=*/{},
                         /*document_schema_types=*/{}) {}

libtextclassifier3::StatusOr<std::vector<DocumentId>>
MonkeyNotQueryNode::ProcessMatchedDocIds(
    const InMemoryIcingSearchEngine* engine,
    const std::vector<std::unordered_set<DocumentId>>& combined_matched_doc_ids)
    const {
  std::vector<DocumentId> results;
  const std::unordered_set<DocumentId>& matched_doc_ids =
      combined_matched_doc_ids[0];
  const std::vector<DocumentId>& all_doc_ids = engine->GetExistingDocumentIds();
  for (DocumentId doc_id : all_doc_ids) {
    if (!matched_doc_ids.contains(doc_id)) {
      const MonkeyTokenizedDocument& document = engine->GetDocumentById(doc_id);
      if (DoesNamespaceMatch(document, document_namespaces_) &&
          DoesSchemaTypeMatch(document, document_schema_types_)) {
        results.push_back(doc_id);
      }
    }
  }
  return results;
}

std::string MonkeyNotQueryNode::GenerateQueryString() const {
  std::string result =
      absl_ports::StrCat("NOT (", child_nodes_[0]->GenerateQueryString(), ")");
  return result;
}

}  // namespace lib
}  // namespace icing
