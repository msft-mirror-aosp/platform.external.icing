// Copyright (C) 2023 Google LLC
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

#include "icing/join/qualified-id-join-indexing-handler.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/join/document-join-id-pair.h"
#include "icing/join/qualified-id-join-index.h"
#include "icing/join/qualified-id.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/proto/logging.pb.h"
#include "icing/schema/joinable-property.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/store/namespace-id-fingerprint.h"
#include "icing/store/namespace-id.h"
#include "icing/util/clock.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"
#include "icing/util/tokenized-document.h"

namespace icing {
namespace lib {

/* static */ libtextclassifier3::StatusOr<
    std::unique_ptr<QualifiedIdJoinIndexingHandler>>
QualifiedIdJoinIndexingHandler::Create(
    const Clock* clock, const DocumentStore* doc_store,
    QualifiedIdJoinIndex* qualified_id_join_index) {
  ICING_RETURN_ERROR_IF_NULL(clock);
  ICING_RETURN_ERROR_IF_NULL(doc_store);
  ICING_RETURN_ERROR_IF_NULL(qualified_id_join_index);

  return std::unique_ptr<QualifiedIdJoinIndexingHandler>(
      new QualifiedIdJoinIndexingHandler(clock, doc_store,
                                         qualified_id_join_index));
}

libtextclassifier3::Status QualifiedIdJoinIndexingHandler::Handle(
    const TokenizedDocument& tokenized_document, DocumentId document_id,
    DocumentId old_document_id, bool recovery_mode,
    PutDocumentStatsProto* put_document_stats) {
  std::unique_ptr<Timer> index_timer = clock_.GetNewTimer();

  if (!IsDocumentIdValid(document_id)) {
    return absl_ports::InvalidArgumentError(
        IcingStringUtil::StringPrintf("Invalid DocumentId %d", document_id));
  }

  if (qualified_id_join_index_.last_added_document_id() != kInvalidDocumentId &&
      document_id <= qualified_id_join_index_.last_added_document_id()) {
    if (recovery_mode) {
      // Skip the document if document_id <= last_added_document_id in recovery
      // mode without returning an error.
      return libtextclassifier3::Status::OK;
    }
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "DocumentId %d must be greater than last added document_id %d",
        document_id, qualified_id_join_index_.last_added_document_id()));
  }
  qualified_id_join_index_.set_last_added_document_id(document_id);

  switch (qualified_id_join_index_.version()) {
    case QualifiedIdJoinIndex::Version::kV1:
      ICING_RETURN_IF_ERROR(HandleV1(tokenized_document, document_id));
      break;
    case QualifiedIdJoinIndex::Version::kV2:
      ICING_RETURN_IF_ERROR(HandleV2(tokenized_document, document_id));
      break;
    case QualifiedIdJoinIndex::Version::kV3:
      ICING_RETURN_IF_ERROR(
          HandleV3(tokenized_document, document_id, old_document_id));
      break;
  }

  if (put_document_stats != nullptr) {
    put_document_stats->set_qualified_id_join_index_latency_ms(
        index_timer->GetElapsedMilliseconds());
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status QualifiedIdJoinIndexingHandler::HandleV1(
    const TokenizedDocument& tokenized_document, DocumentId document_id) {
  for (const JoinableProperty<std::string_view>& qualified_id_property :
       tokenized_document.qualified_id_join_properties()) {
    if (qualified_id_property.values.empty()) {
      continue;
    }

    DocumentJoinIdPair document_join_id_pair(document_id,
                                             qualified_id_property.metadata.id);
    // Currently we only support single (non-repeated) joinable value under a
    // property.
    std::string_view ref_qualified_id_str = qualified_id_property.values[0];

    // Attempt to parse qualified id string to make sure the format is
    // correct.
    if (!QualifiedId::Parse(ref_qualified_id_str).ok()) {
      // Skip incorrect format of qualified id string to save disk space.
      continue;
    }

    libtextclassifier3::Status status = qualified_id_join_index_.Put(
        document_join_id_pair, ref_qualified_id_str);
    if (!status.ok()) {
      ICING_LOG(WARNING)
          << "Failed to add data into qualified id join index due to: "
          << status.error_message();
      return status;
    }
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status QualifiedIdJoinIndexingHandler::HandleV2(
    const TokenizedDocument& tokenized_document, DocumentId document_id) {
  std::optional<DocumentFilterData> filter_data =
      doc_store_.GetAliveDocumentFilterData(
          document_id,
          /*current_time_ms=*/std::numeric_limits<int64_t>::min());
  if (!filter_data) {
    // This should not happen.
    return absl_ports::InternalError(
        "Failed to get alive document filter data when indexing");
  }

  for (const JoinableProperty<std::string_view>& qualified_id_property :
       tokenized_document.qualified_id_join_properties()) {
    // Parse all qualified id strings and convert them to
    // NamespaceIdFingerprint.
    std::vector<NamespaceIdFingerprint> ref_doc_nsid_uri_fingerprints;
    for (std::string_view ref_qualified_id_str : qualified_id_property.values) {
      // Attempt to parse qualified id string to make sure the format is
      // correct.
      auto ref_qualified_id_or = QualifiedId::Parse(ref_qualified_id_str);
      if (!ref_qualified_id_or.ok()) {
        // Skip incorrect format of qualified id string.
        continue;
      }

      QualifiedId ref_qualified_id =
          std::move(ref_qualified_id_or).ValueOrDie();
      auto ref_namespace_id_or =
          doc_store_.GetNamespaceId(ref_qualified_id.name_space());
      if (!ref_namespace_id_or.ok()) {
        // Skip invalid namespace id.
        continue;
      }
      NamespaceId ref_namespace_id =
          std::move(ref_namespace_id_or).ValueOrDie();

      ref_doc_nsid_uri_fingerprints.push_back(
          NamespaceIdFingerprint(ref_namespace_id, ref_qualified_id.uri()));
    }

    // Batch add all join data of this (schema_type_id, joinable_property_id)
    // into to the index.
    libtextclassifier3::Status status = qualified_id_join_index_.Put(
        filter_data->schema_type_id(), qualified_id_property.metadata.id,
        document_id, std::move(ref_doc_nsid_uri_fingerprints));
    if (!status.ok()) {
      ICING_LOG(WARNING)
          << "Failed to add data into qualified id join index v2 due to: "
          << status.error_message();
      return status;
    }
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status QualifiedIdJoinIndexingHandler::HandleV3(
    const TokenizedDocument& tokenized_document, DocumentId document_id,
    DocumentId old_document_id) {
  // (Parent perspective)
  // When replacement, if there were any existing child documents joining to it,
  // then we need to migrate the old document id to the new document id.
  if (IsDocumentIdValid(old_document_id)) {
    ICING_RETURN_IF_ERROR(
        qualified_id_join_index_.MigrateParent(old_document_id, document_id));
  }

  // (Child perspective)
  // Add child join data.
  for (const JoinableProperty<std::string_view>& qualified_id_property :
       tokenized_document.qualified_id_join_properties()) {
    if (qualified_id_property.values.empty()) {
      continue;
    }

    DocumentJoinIdPair child_doc_join_id_pair(
        document_id, qualified_id_property.metadata.id);

    // Extract parent qualified ids and lookup their corresponding document ids.
    std::vector<DocumentId> parent_doc_ids;
    parent_doc_ids.reserve(qualified_id_property.values.size());
    for (std::string_view parent_qualified_id_str :
         qualified_id_property.values) {
      libtextclassifier3::StatusOr<QualifiedId> parent_qualified_id_or =
          QualifiedId::Parse(parent_qualified_id_str);
      if (!parent_qualified_id_or.ok()) {
        // Skip incorrect format of qualified id string.
        continue;
      }
      QualifiedId parent_qualified_id =
          std::move(parent_qualified_id_or).ValueOrDie();

      // Lookup document store to get the parent document id.
      libtextclassifier3::StatusOr<DocumentId> parent_doc_id_or =
          doc_store_.GetDocumentId(parent_qualified_id.name_space(),
                                   parent_qualified_id.uri());
      if (!parent_doc_id_or.ok() ||
          parent_doc_id_or.ValueOrDie() == kInvalidDocumentId) {
        // Skip invalid parent document id or parent document does not exist.
        continue;
      }
      parent_doc_ids.push_back(parent_doc_id_or.ValueOrDie());
    }

    // Add all parent document ids to the index.
    libtextclassifier3::Status status = qualified_id_join_index_.Put(
        child_doc_join_id_pair, std::move(parent_doc_ids));
    if (!status.ok()) {
      ICING_LOG(WARNING)
          << "Failed to add data into qualified id join index due to: "
          << status.error_message();
      return status;
    }
  }
  return libtextclassifier3::Status::OK;
}

}  // namespace lib
}  // namespace icing
