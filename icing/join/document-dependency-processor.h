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

#ifndef THIRD_PARTY_ICING_JOIN_DOCUMENT_DEPENDENCY_PROCESSOR_H_
#define THIRD_PARTY_ICING_JOIN_DOCUMENT_DEPENDENCY_PROCESSOR_H_

#include <cstdint>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/status.h"
#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/statusor.h"
#include "third_party/icing/join/qualified-id.h"
#include "third_party/icing/proto/document.proto.h"
#include "third_party/icing/store/document-id.h"
#include "third_party/icing/store/document-store.h"
#include "third_party/icing/util/tokenized-document.h"

namespace icing {
namespace lib {

// This class evaluates the dependency of the documents to be added. Currently,
// dependencies are defined solely by delete propagation.
class DocumentDependencyProcessor {
 public:
  // Creates a DocumentDependencyProcessor for the given batch of documents to
  // add.
  //
  // Returns:
  //   - A DocumentDependencyProcessor on success.
  //   - INVALID_ARGUMENT_ERROR if any of the document in the batch is expired.
  static libtextclassifier3::StatusOr<DocumentDependencyProcessor> Create(
      const DocumentStore* document_store,
      const std::vector<TokenizedDocument>& batch_documents_to_add,
      int64_t current_time_ms);

  // Evaluates the document dependencies:
  // - Validates the dependencies. For each document in the batch, its
  //   dependencies (parent documents with delete propagation enabled in the
  //   schema) must be present in either the same batch of new documents or the
  //   document store.
  // - Attaches some additional information to the result for the caller.
  //
  // Returns:
  //   - An EvaluateResult object containing essential evaluation result
  //     information on success.
  //   - INVALID_ARGUMENT_ERROR if the validation fails, e.g. any of the
  //     dependencies (referenced parent documents) are not present.
  //   - Any error from document store or schema store.
  struct EvaluateResult {
    // A vector of sets to store dependency document ids out of the batch, for
    // each document in batch_documents_to_add_. Note that the index of the
    // vector corresponds to the index of the document in
    // batch_documents_to_add_.
    //
    // Note: only dependency documents out of the batch are included. IOW the
    //   relations between documents in the batch are not included.
    //
    // The caller must propagate (std::min) the expiration timestamps of the
    // dependency documents down to the batch documents to add.
    std::vector<std::unordered_set<DocumentId>> outer_dependency_document_ids;

    // A set of existing document ids that are expired and will be replaced by
    // the new documents in the batch.
    //
    // The caller must run delete propagation against these documents to remove
    // their (expired) children from ground truth. Otherwise, it is possible
    // that when Icing rebuilds derived files, an already expired child document
    // becomes alive again. For example, consider a parent document A and child
    // document B:
    // - t = 0: put A with raw expiration timestamp 100.
    // - t = 10: put B with raw expiration timestamp 1000. Its final expiration
    //   timestamp is min(100, 1000) = 100.
    // - t = 200: both A and B are expired.
    // - t = 300: replace (expired) A with raw expiration timestamp 2000.
    // - t = 500: the device reboots.
    //   - When initializing Icing, derived files are discarded and rebuilt.
    //   - Since we lost the previously propagated expiration timestamp of B
    //     (100), when recomputing from ground truth, it becomes min(1000, 2000)
    //     = 1000 and B becomes alive again.
    //   - This causes privacy issue since the replaced A may have unaware child
    //     documents.
    std::unordered_set<DocumentId> existing_expired_doc_ids_to_replace;
  };
  libtextclassifier3::StatusOr<EvaluateResult> Evaluate();

 private:
  explicit DocumentDependencyProcessor(
      const DocumentStore* document_store,
      const std::vector<TokenizedDocument>& batch_documents_to_add,
      std::unordered_map<QualifiedId, int, QualifiedId::Hasher>
          qualified_id_to_batch_idx,
      int64_t current_time_ms)
      : document_store_(*document_store),
        batch_documents_to_add_(batch_documents_to_add),
        qualified_id_to_batch_idx_(std::move(qualified_id_to_batch_idx)),
        current_time_ms_(current_time_ms) {}

  // Helper function to validate a dependency's qualified id string:
  // - Is valid or not. Note that empty qualified id string is allowed and will
  //   be skipped.
  // - Satisfies the dependency: matches a document in either the same batch of
  //   new documents to add or the document store.
  //
  // Also add the dependency document id (out of the batch) into
  // outer_dep_doc_ids.
  //
  // Returns:
  //   - OK on success.
  //   - INVALID_ARGUMENT_ERROR if dep_qualified_id_str is invalid or the
  //     document referenced by the qualified id is not present.
  //   - Any error from document store.
  libtextclassifier3::Status ValidateDependency(
      std::string_view dep_qualified_id_str,
      std::unordered_set<DocumentId>& outer_dep_doc_ids) const;

  const DocumentStore& document_store_;
  const std::vector<TokenizedDocument>& batch_documents_to_add_;

  // A map for mapping qualified id to the index of batch_documents_to_add_.
  std::unordered_map<QualifiedId, int, QualifiedId::Hasher>
      qualified_id_to_batch_idx_;

  int64_t current_time_ms_;
};

}  // namespace lib
}  // namespace icing

#endif  // THIRD_PARTY_ICING_JOIN_DOCUMENT_DEPENDENCY_PROCESSOR_H_
