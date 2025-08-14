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

#include "third_party/icing/join/document-dependency-processor.h"

#include <cstdint>
#include <optional>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/status.h"
#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/statusor.h"
#include "third_party/icing/absl_ports/canonical_errors.h"
#include "third_party/icing/join/qualified-id.h"
#include "third_party/icing/proto/document.proto.h"
#include "third_party/icing/schema/joinable-property.h"
#include "third_party/icing/store/document-filter-data.h"
#include "third_party/icing/store/document-id.h"
#include "third_party/icing/store/document-store.h"
#include "third_party/icing/util/status-macros.h"
#include "third_party/icing/util/timestamp-util.h"
#include "third_party/icing/util/tokenized-document.h"

namespace icing {
namespace lib {

/* static */ libtextclassifier3::StatusOr<DocumentDependencyProcessor>
DocumentDependencyProcessor::Create(
    const DocumentStore* document_store,
    const std::vector<TokenizedDocument>& batch_documents_to_add,
    int64_t current_time_ms) {
  ICING_RETURN_ERROR_IF_NULL(document_store);

  std::unordered_map<QualifiedId, int, QualifiedId::Hasher>
      qualified_id_to_batch_idx;
  for (int i = 0; i < batch_documents_to_add.size(); ++i) {
    const TokenizedDocument& tokenized_document = batch_documents_to_add[i];

    // Ensure that the new document is not expired.
    int64_t expiration_timestamp_ms =
        timestamp_util::CalculateRawExpirationTimestampMs(
            tokenized_document.document_wrapper()
                .document()
                .creation_timestamp_ms(),
            tokenized_document.document_wrapper().document().ttl_ms());
    if (expiration_timestamp_ms <= current_time_ms) {
      return absl_ports::InvalidArgumentError("The new document is expired.");
    }

    QualifiedId qualified_id(
        tokenized_document.document_wrapper().document().namespace_(),
        tokenized_document.document_wrapper().document().uri());
    qualified_id_to_batch_idx.insert({std::move(qualified_id), i});
  }

  return DocumentDependencyProcessor(document_store, batch_documents_to_add,
                                     std::move(qualified_id_to_batch_idx),
                                     current_time_ms);
}

libtextclassifier3::StatusOr<DocumentDependencyProcessor::EvaluateResult>
DocumentDependencyProcessor::Evaluate() {
  EvaluateResult result;
  result.outer_dependency_document_ids.resize(batch_documents_to_add_.size());

  // Validate the dependencies and construct the dependent graph.
  for (int i = 0; i < batch_documents_to_add_.size(); ++i) {
    const TokenizedDocument& tokenized_document = batch_documents_to_add_[i];

    // Iterate through all qualified id joinable properties of the tokenized
    // document.
    for (const JoinableProperty<std::string_view>& dep_qualified_id_prop :
         tokenized_document.qualified_id_join_properties()) {
      if (dep_qualified_id_prop.metadata.delete_propagation_type ==
          JoinableConfig::DeletePropagationType::NONE) {
        // If delete propagation is NONE, then the referenced documents are
        // not required to be present, so skip the check for this joinable
        // property.
        continue;
      }

      // Otherwise, the referenced documents are required to be present.
      // For each of the qualified id string:
      // - Validate and check it should match a document in either the same
      //   batch of new documents to add or the document store.
      // - Add the dependency document id (out of the batch) into result.
      for (std::string_view dep_qualified_id_str :
           dep_qualified_id_prop.values) {
        ICING_RETURN_IF_ERROR(ValidateDependency(
            dep_qualified_id_str, result.outer_dependency_document_ids[i]));
      }
    }

    // Check if this document to add is replacing an expired document.
    auto existing_doc_id_or = document_store_.GetDocumentId(
        tokenized_document.document_wrapper().document().namespace_(),
        tokenized_document.document_wrapper().document().uri());
    if (existing_doc_id_or.ok()) {
      DocumentId existing_doc_id = existing_doc_id_or.ValueOrDie();
      std::optional<DocumentFilterData> filter_data =
          document_store_.GetNonDeletedDocumentFilterData(existing_doc_id);
      if (filter_data.has_value() &&
          filter_data->expiration_timestamp_ms() <= current_time_ms_) {
        result.existing_expired_doc_ids_to_replace.insert(existing_doc_id);
      }
    }
  }
  return result;
}

libtextclassifier3::Status DocumentDependencyProcessor::ValidateDependency(
    std::string_view dep_qualified_id_str,
    std::unordered_set<DocumentId>& outer_dep_doc_ids) const {
  if (dep_qualified_id_str.empty()) {
    // Allow empty qualified id.
    return libtextclassifier3::Status::OK;
  }

  // Attempt to parse the qualified id string.
  auto dep_qualified_id_or = QualifiedId::Parse(dep_qualified_id_str);
  if (!dep_qualified_id_or.ok()) {
    // Incorrect format of qualified id string. Return INVALID_ARGUMENT_ERROR
    // for unsatisfied dependency.
    return absl_ports::InvalidArgumentError("Invalid qualified id string.");
  }
  QualifiedId dep_qualified_id = std::move(dep_qualified_id_or).ValueOrDie();

  // Case 1: check if the dependency document is in the same batch of new
  //         documents.
  auto itr = qualified_id_to_batch_idx_.find(dep_qualified_id);
  if (itr != qualified_id_to_batch_idx_.end()) {
    // We've already validated that the document in the batch is not expired, so
    // we don't need to check it again here.
    return libtextclassifier3::Status::OK;
  }

  // Case 2: check if the dependency document is alive in the document store.
  auto dep_doc_id_or = document_store_.GetDocumentId(
      dep_qualified_id.name_space(), dep_qualified_id.uri());
  if (!dep_doc_id_or.ok()) {
    if (absl_ports::IsNotFound(dep_doc_id_or.status())) {
      // Document not found in the document store. Return INVALID_ARGUMENT_ERROR
      // for unsatisfied dependency.
      return absl_ports::InvalidArgumentError(
          "A dependency document is not found.");
    }
    // Real error.
    return std::move(dep_doc_id_or).status();
  }
  DocumentId dep_doc_id = dep_doc_id_or.ValueOrDie();

  if (!document_store_.IsDocumentAlive(dep_doc_id, current_time_ms_)) {
    return absl_ports::InvalidArgumentError(
        "A dependency document is not alive.");
  }

  outer_dep_doc_ids.insert(dep_doc_id);

  return libtextclassifier3::Status::OK;
}

}  // namespace lib
}  // namespace icing
