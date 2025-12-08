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

#ifndef ICING_JOIN_DELETE_PROPAGATION_HANDLER_H_
#define ICING_JOIN_DELETE_PROPAGATION_HANDLER_H_

#include <cstdint>
#include <unordered_set>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/join/qualified-id-join-index.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"

namespace icing {
namespace lib {

// A class to handle delete propagation related logics.
class DeletePropagationHandler {
 public:
  // Creates a DeletePropagationHandler.
  //
  // Returns:
  //   - DeletePropagationHandler on success.
  //   - FAILED_PRECONDITION_ERROR if any pointer is nullptr or qualified id
  //     join index version is not v3.
  static libtextclassifier3::StatusOr<DeletePropagationHandler> Create(
      const SchemaStore* schema_store,
      const QualifiedIdJoinIndex* qualified_id_join_index,
      DocumentStore* document_store, int64_t current_time_ms);

  // Handles delete propagation for the given parent document ids.
  //
  // Note: this function DOES NOT handle parent_doc_ids' deletion. Instead, it
  //   only deletes the propagated child documents.
  //
  // Returns:
  //   - A vector of deleted child document metadata on success.
  //   - INTERNAL_ERROR on any I/O errors.
  libtextclassifier3::StatusOr<std::vector<DocumentStore::DocumentMetadata>>
  Handle(const std::unordered_set<DocumentId>& parent_doc_ids);

 private:
  explicit DeletePropagationHandler(
      const SchemaStore* schema_store,
      const QualifiedIdJoinIndex* qualified_id_join_index,
      DocumentStore* document_store, int64_t current_time_ms)
      : schema_store_(*schema_store),
        qualified_id_join_index_(*qualified_id_join_index),
        document_store_(*document_store),
        current_time_ms_(current_time_ms) {}

  // Helper function to get all child document ids propagated from the given
  // parent document ids via join relations with delete propagation enabled.
  //
  // Returns:
  //   - A set of propagated child document ids on success.
  //   - INTERNAL_ERROR on any I/O errors.
  libtextclassifier3::StatusOr<std::unordered_set<DocumentId>>
  GetPropagatedChildDocumentIds(
      const std::unordered_set<DocumentId>& parent_doc_ids);

  const SchemaStore& schema_store_;                      // Does not own.
  const QualifiedIdJoinIndex& qualified_id_join_index_;  // Does not own.
  DocumentStore& document_store_;                        // Does not own.
  int64_t current_time_ms_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_JOIN_DELETE_PROPAGATION_HANDLER_H_
