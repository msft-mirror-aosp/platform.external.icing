// Copyright (C) 2025 Google LLC
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

#ifndef ICING_JOIN_EXPIRATION_TIMESTAMP_UTIL_H_
#define ICING_JOIN_EXPIRATION_TIMESTAMP_UTIL_H_

#include <cstdint>
#include <unordered_set>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/join/qualified-id-join-index.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"

namespace icing {
namespace lib {

class ExpirationTimestampUtil {
 public:
  // Updates the given single document's expiration timestamp by its
  // dependencies. Also propagates the expiration timestamp to all of its
  // "non-deleted" dependents (and their dependents, and so on), based on the
  // dependent graph stored in the join index.
  //
  // Note:
  // - The implementation is designed for updating a single document in (single)
  //   Put API.
  // - To make it efficient, currently it only allows a smaller expiration
  //   timestamp to be propagated down. IOW, if the new expiration timestamp is
  //   larger than the existing one, the existing one will be kept and stop
  //   propagation.
  // - Therefore, we can run simple BFS instead of Bellman-Ford algorithm, and
  //   the time complexity is better.
  // - In the future, if we decide to support updating (1) multiple documents on
  //   the subgraph for batch API (2) larger expiration timestamp, then:
  //   - Running K times BFS (K = number of documents to update) is not
  //     efficient.
  //   - Bellman-Ford algorithm is not efficient enough either, especially when
  //     there are cycles in the dependent graph.
  //   - We should consider running the SCC + topological sort algorithm to
  //     propagate the expiration timestamp in linear time complexity.
  //   - For optimization, subgraph SCC and topological sort can be performed,
  //     but this requires to store the reverse dependent edges for determining
  //     the subgraph that requires updating.
  //
  // Parameters:
  //   - document_id: the single document id to update.
  //   - dependency_doc_ids: dependencies of the document_id.
  //   - schema_store: the schema store.
  //   - qualified_id_join_index: the qualified id join index.
  //   - document_store.
  //   - current_time_ms.
  //
  // Returns:
  //   - OK on success, and the expiration timestamps (which are stored in
  //     DocumentStore -> DocumentFilterData cache) of the document and its
  //     dependents are updated.
  //   - Any error from document store, schema store, or join index.
  static libtextclassifier3::Status SingleDocumentPropagation(
      DocumentId document_id,
      const std::unordered_set<DocumentId>& dependency_doc_ids,
      const SchemaStore& schema_store,
      const QualifiedIdJoinIndex& qualified_id_join_index,
      DocumentStore& document_store, int64_t current_time_ms);
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_JOIN_EXPIRATION_TIMESTAMP_UTIL_H_
