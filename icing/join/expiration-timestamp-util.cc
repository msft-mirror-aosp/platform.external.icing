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

#include "third_party/icing/join/expiration-timestamp-util.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <queue>
#include <unordered_set>
#include <utility>

#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/status.h"
#include "third_party/icing/absl_ports/canonical_errors.h"
#include "third_party/icing/graph/graph-interface.h"
#include "third_party/icing/join/document-dependent-graph.h"
#include "third_party/icing/join/qualified-id-join-index.h"
#include "third_party/icing/schema/schema-store.h"
#include "third_party/icing/store/document-filter-data.h"
#include "third_party/icing/store/document-id.h"
#include "third_party/icing/store/document-store.h"
#include "third_party/icing/util/logging.h"
#include "third_party/icing/util/status-macros.h"

namespace icing {
namespace lib {

/* static */ libtextclassifier3::Status
ExpirationTimestampUtil::SingleDocumentPropagation(
    DocumentId document_id,
    const std::unordered_set<DocumentId>& dependency_doc_ids,
    const SchemaStore& schema_store,
    const QualifiedIdJoinIndex& qualified_id_join_index,
    DocumentStore& document_store, int64_t current_time_ms) {
  // This function implements the algorithm to:
  // - Update the given document's expiration timestamp by its given
  //   dependencies. Mostly its dependencies are determined by the dependency
  //   evaluation. See DocumentDependencyProcessor for more details.
  // - Propagate the expiration timestamp to all of the given document's
  //   non-deleted dependents (and their dependents, and so on), based on the
  //   dependent graph.
  //
  // For example:
  // - dependency_doc_ids = {depcy1, depcy2}
  // - dependent_graph contains the edges of the following graph.
  //
  //                 +--------> dept1 ------+
  //                 |                      |
  //                 |                      v
  // depcy1 --> document_id --> dept2 --> dept3
  //                 ^            |
  //                 |            v
  // depcy2 ---------+          dept4
  //
  // The algorithm will:
  // - Update document_id's expiration timestamp by min(depcy1_exp_ts,
  //   depcy2_exp_ts) if this min value is smaller than the document's current
  //   expiration timestamp.
  // - BFS traverse: propagate the expiration timestamp to dept1, dept2, dept3,
  //   and dept4. Early stop if any of the dependents' expiration timestamp is
  //   not smaller than the propagated value.

  // Step 1.0: get the min expiration timestamp of the dependencies.
  int64_t min_depcy_exp_ts_ms = std::numeric_limits<int64_t>::max();
  for (DocumentId depcy_doc_id : dependency_doc_ids) {
    std::optional<DocumentFilterData> doc_filter_data =
        document_store.GetAliveDocumentFilterData(depcy_doc_id,
                                                  current_time_ms);
    if (!doc_filter_data.has_value()) {
      // This really shouldn't happen since they were validated in the caller
      // side by DocumentDependencyProcessor.
      ICING_LOG(ERROR) << "A dependency document is not alive after dependency "
                          "evaluation. This should never happen.";
      return absl_ports::InternalError(
          "A dependency document is not alive after dependency evaluation.");
    }
    min_depcy_exp_ts_ms = std::min(min_depcy_exp_ts_ms,
                                   doc_filter_data->expiration_timestamp_ms());
  }

  // Step 1.1: update min expiration timestamp from dependencies to the
  //           document.
  //
  // Note:
  // - UpdateDocumentExpirationTimestamp only overwrites the new expiration
  //   timestamp if the new value is smaller than the existing one, so it is
  //   safe to call this method directly.
  // - final_exp_ts is the final expiration timestamp of the document.
  //   - If min_depcy_exp_ts_ms is smaller than the document's current (raw)
  //     expiration timestamp, final_exp_ts will be min_depcy_exp_ts_ms.
  //   - Otherwise, final_exp_ts will be the document's (raw) expiration
  //     timestamp.
  ICING_ASSIGN_OR_RETURN(
      DocumentStore::UpdateDocumentExpirationTimestampResult update_result,
      document_store.UpdateDocumentExpirationTimestamp(document_id,
                                                       min_depcy_exp_ts_ms));
  int64_t final_exp_ts = update_result.final_expiration_timestamp_ms;

  // Step 2: run BFS to propagate final_exp_ts to all dependents. In most cases,
  //         a new document won't have any dependents at this point. Only alive
  //         document replacement will have dependents to update.
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<DocumentDependentGraph> dependent_graph,
      DocumentDependentGraph::Create(&schema_store, &document_store,
                                     &qualified_id_join_index));

  std::unordered_set<DocumentId> visited_doc_ids;
  std::queue<DocumentId> que;
  visited_doc_ids.insert(document_id);
  que.push(document_id);
  while (!que.empty()) {
    DocumentId curr_doc_id = que.front();
    que.pop();

    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<graph::GraphInterface<DocumentId>::EdgeIteratorIf> itr,
        dependent_graph->GetEdgesIterator(curr_doc_id));
    while (itr->Advance().ok()) {
      DocumentId next_doc_id = itr->Get();
      if (!visited_doc_ids.insert(next_doc_id).second) {
        // Already visited.
        continue;
      }

      // Update the next document's expiration timestamp. Push into the queue
      // only if the expiration timestamp is updated.
      //
      // Note: it is safe to call UpdateDocumentExpirationTimestamp directly and
      //   return if getting an error here, because DocumentDependentGraph
      //   returns edges to non deleted documents and at this point next_doc_id
      //   is guaranteed to be valid and non-deleted.
      ICING_ASSIGN_OR_RETURN(
          DocumentStore::UpdateDocumentExpirationTimestampResult
              dependent_update_result,
          document_store.UpdateDocumentExpirationTimestamp(next_doc_id,
                                                           final_exp_ts));
      if (dependent_update_result.was_updated) {
        que.push(next_doc_id);
      }
    }
  }
  return libtextclassifier3::Status::OK;
}

}  // namespace lib
}  // namespace icing
