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

#include "icing/join/document-dependent-graph.h"

#include <memory>
#include <optional>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/graph/graph-interface.h"
#include "icing/join/qualified-id-join-index.h"
#include "icing/schema/joinable-property.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

/* static */ libtextclassifier3::StatusOr<
    std::unique_ptr<DocumentDependentGraph>>
DocumentDependentGraph::Create(const SchemaStore* schema_store,
                               const DocumentStore* doc_store,
                               const QualifiedIdJoinIndex* join_index) {
  ICING_RETURN_ERROR_IF_NULL(schema_store);
  ICING_RETURN_ERROR_IF_NULL(doc_store);
  ICING_RETURN_ERROR_IF_NULL(join_index);

  if (join_index->version() != QualifiedIdJoinIndex::Version::kV3) {
    return absl_ports::InvalidArgumentError(
        "DocumentDependentGraph only supports QualifiedIdJoinIndex version "
        "V3.");
  }

  return std::unique_ptr<DocumentDependentGraph>(
      new DocumentDependentGraph(schema_store, doc_store, join_index));
}

int DocumentDependentGraph::GetNumNodes() const {
  DocumentId last_stored_doc_id = doc_store_.last_added_document_id();
  if (last_stored_doc_id == kInvalidDocumentId) {
    return 0;
  }
  // There are documents from 0 to last_stored_doc_id, so num nodes
  // should be last_stored_doc_id + 1.
  return last_stored_doc_id + 1;
}

libtextclassifier3::StatusOr<
    std::unique_ptr<typename graph::GraphInterface<DocumentId>::EdgeIteratorIf>>
DocumentDependentGraph::GetEdgesIterator(int node_id) const {
  if (node_id < 0 || node_id >= GetNumNodes()) {
    return absl_ports::InvalidArgumentError("Invalid node id.");
  }

  DocumentId doc_id = static_cast<DocumentId>(node_id);
  if (!doc_store_.GetNonDeletedDocumentFilterData(doc_id).has_value()) {
    // Return an iterator with no edge to advance to for a deleted document.
    return std::make_unique<EdgeIterator>(
        schema_store_, doc_store_,
        QualifiedIdJoinIndex::DocumentJoinIdPairArrayView(/*data=*/nullptr,
                                                          /*size=*/0));
  }

  ICING_ASSIGN_OR_RETURN(
      QualifiedIdJoinIndex::DocumentJoinIdPairArrayView data_array_view,
      join_index_.GetDocumentJoinIdPairArrayView(doc_id));
  return std::make_unique<EdgeIterator>(schema_store_, doc_store_,
                                        std::move(data_array_view));
}

libtextclassifier3::Status DocumentDependentGraph::EdgeIterator::Advance() {
  while (++curr_idx_ < join_data_array_view_.size()) {
    DocumentId next_doc_id = join_data_array_view_[curr_idx_].document_id();
    JoinablePropertyId next_joinable_property_id =
        join_data_array_view_[curr_idx_].joinable_property_id();

    // Dedupe document id.
    if (next_doc_id == curr_) {
      continue;
    }

    // Lookup the schema type id of the next document.
    std::optional<DocumentFilterData> next_doc_filter_data =
        doc_store_.GetNonDeletedDocumentFilterData(next_doc_id);
    if (!next_doc_filter_data.has_value() ||
        next_doc_filter_data->schema_type_id() == kInvalidSchemaTypeId) {
      continue;
    }

    // Get the joinable property metadata.
    ICING_ASSIGN_OR_RETURN(
        const JoinablePropertyMetadata* metadata,
        schema_store_.GetJoinablePropertyMetadata(
            next_doc_filter_data->schema_type_id(), next_joinable_property_id));

    if (metadata->delete_propagation_type ==
        JoinableConfig::DeletePropagationType::PROPAGATE_FROM) {
      // Found a joinable property hit with delete propagation PROPAGATE_FROM.
      // It means the next document is a dependent of the current document, so
      // stop here, record the doc id and return success.
      curr_ = next_doc_id;
      return libtextclassifier3::Status::OK;
    }
  }

  return absl_ports::ResourceExhaustedError("No more edges to advance to.");
}

}  // namespace lib
}  // namespace icing
