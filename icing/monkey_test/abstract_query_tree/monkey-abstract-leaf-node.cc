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

#include "icing/monkey_test/abstract_query_tree/monkey-abstract-leaf-node.h"

#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/monkey_test/monkey-test-util.h"
#include "icing/monkey_test/in-memory-icing-search-engine.h"
#include "icing/monkey_test/monkey-tokenized-document.h"
#include "icing/proto/schema.pb.h"
#include "icing/store/document-id.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<std::vector<DocumentId>>
MonkeyAbstractLeafQueryNode::EvaluateQuery(
    const InMemoryIcingSearchEngine* search_engine) const {
  std::vector<DocumentId> result;
  const std::vector<DocumentId>& doc_ids =
      search_engine->GetExistingDocumentIds();
  for (DocumentId doc_id : doc_ids) {
    const MonkeyTokenizedDocument& document =
        search_engine->GetDocumentById(doc_id);
    // Skip documents that don't match the namespace or schema type filters
    // (if any).
    if (!DoesNamespaceMatch(document, document_namespaces_) ||
        !DoesSchemaTypeMatch(document, document_schema_types_)) {
      continue;
    }
    ICING_ASSIGN_OR_RETURN(bool matches,
                           DoesDocumentMatchQuery(search_engine, document));
    if (matches) {
      result.push_back(doc_id);
    }
  }
  return result;
}

}  // namespace lib
}  // namespace icing
