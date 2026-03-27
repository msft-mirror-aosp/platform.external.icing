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

#include "icing/join/delete-propagation-handler.h"

#include <cstdint>
#include <deque>
#include <optional>
#include <queue>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/join/document-join-id-pair.h"
#include "icing/join/qualified-id-join-index.h"
#include "icing/schema/joinable-property.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

/* static */ libtextclassifier3::StatusOr<DeletePropagationHandler>
DeletePropagationHandler::Create(
    const SchemaStore* schema_store,
    const QualifiedIdJoinIndex* qualified_id_join_index,
    DocumentStore* document_store, int64_t current_time_ms) {
  ICING_RETURN_ERROR_IF_NULL(schema_store);
  ICING_RETURN_ERROR_IF_NULL(qualified_id_join_index);
  ICING_RETURN_ERROR_IF_NULL(document_store);

  if (qualified_id_join_index->version() !=
      QualifiedIdJoinIndex::Version::kV3) {
    return absl_ports::FailedPreconditionError(
        "Delete propagation is enabled but qualified id join index v3 is not "
        "used.");
  }

  return DeletePropagationHandler(schema_store, qualified_id_join_index,
                                  document_store, current_time_ms);
}

libtextclassifier3::StatusOr<std::vector<DocumentStore::DocumentMetadata>>
DeletePropagationHandler::Handle(
    const std::unordered_set<DocumentId>& parent_doc_ids) {
  ICING_ASSIGN_OR_RETURN(
      std::unordered_set<DocumentId> propagated_child_doc_ids,
      GetPropagatedChildDocumentIds(parent_doc_ids));

  // Delete all propagated child documents.
  std::vector<DocumentStore::DocumentMetadata> deleted_doc_metadata_list;
  for (DocumentId child_doc_id : propagated_child_doc_ids) {
    auto deleted_doc_metadata_or = document_store_.ForceDelete(child_doc_id);
    if (!deleted_doc_metadata_or.ok()) {
      if (absl_ports::IsNotFound(deleted_doc_metadata_or.status())) {
        // The child document has already been deleted or expired, so skip the
        // error. This should not happen, but let's check and skip it just in
        // case.
        continue;
      }

      // Real error.
      return std::move(deleted_doc_metadata_or).status();
    }
    deleted_doc_metadata_list.push_back(
        std::move(deleted_doc_metadata_or).ValueOrDie());
  }

  return deleted_doc_metadata_list;
}

libtextclassifier3::StatusOr<std::unordered_set<DocumentId>>
DeletePropagationHandler::GetPropagatedChildDocumentIds(
    const std::unordered_set<DocumentId>& parent_doc_ids) {
  std::unordered_set<DocumentId> propagated_child_doc_ids;

  // BFS traverse to find all propagated child documents.
  std::queue<DocumentId> que(
      std::deque(parent_doc_ids.begin(), parent_doc_ids.end()));
  while (!que.empty()) {
    DocumentId doc_id_to_expand = que.front();
    que.pop();

    ICING_ASSIGN_OR_RETURN(
        QualifiedIdJoinIndex::DocumentJoinIdPairArrayView
            child_join_id_pairs_array_view,
        qualified_id_join_index_.GetDocumentJoinIdPairArrayView(
            doc_id_to_expand));
    for (const DocumentJoinIdPair& child_join_id_pair :
         child_join_id_pairs_array_view) {
      if (propagated_child_doc_ids.find(child_join_id_pair.document_id()) !=
              propagated_child_doc_ids.end() ||
          parent_doc_ids.find(child_join_id_pair.document_id()) !=
              parent_doc_ids.end()) {
        // Already added into the propagated set or in the parent set (happens
        // only when there is a cycle back to the parent or traversed document
        // in the join relation). Skip it.
        continue;
      }

      // Get DocumentFilterData of the child document to look up its schema type
      // id.
      // - Skip if the child document has been deleted, since delete propagation
      //   should've been done to all its children when deleting it previously.
      // - Otherwise, we have to handle this child document and propagate to the
      //   grandchildren when it is alive OR expired but not deleted.
      std::optional<DocumentFilterData> child_filter_data =
          document_store_.GetNonDeletedDocumentFilterData(
              child_join_id_pair.document_id());
      if (!child_filter_data) {
        // The child document has been deleted. Skip.
        continue;
      }

      auto metadata_or = schema_store_.GetJoinablePropertyMetadata(
          child_filter_data->schema_type_id(),
          child_join_id_pair.joinable_property_id());
      if (!metadata_or.ok() || metadata_or.ValueOrDie() == nullptr) {
        // This shouldn't happen because we've validated it during indexing and
        // only put valid DocumentJoinIdPair into qualified id join index.
        // Log and skip it.
        ICING_LOG(ERROR) << "Failed to get metadata for schema type id "
                         << child_filter_data->schema_type_id()
                         << ", joinable property id "
                         << static_cast<int>(
                                child_join_id_pair.joinable_property_id());
        continue;
      }
      const JoinablePropertyMetadata* metadata = metadata_or.ValueOrDie();

      if (metadata->value_type == JoinableConfig::ValueType::QUALIFIED_ID &&
          metadata->delete_propagation_type ==
              JoinableConfig::DeletePropagationType::PROPAGATE_FROM) {
        propagated_child_doc_ids.insert(child_join_id_pair.document_id());
        que.push(child_join_id_pair.document_id());
      }
    }
  }

  return propagated_child_doc_ids;
}

}  // namespace lib
}  // namespace icing
