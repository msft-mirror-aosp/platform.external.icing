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

#ifndef ICING_JOIN_DOCUMENT_DEPENDENCY_PROCESSOR_H_
#define ICING_JOIN_DOCUMENT_DEPENDENCY_PROCESSOR_H_

#include <cstdint>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/join/qualified-id.h"
#include "icing/proto/document.pb.h"
#include "icing/store/document-store.h"
#include "icing/util/tokenized-document.h"

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

  // Evaluates the document dependencies. For each document in the batch, its
  // dependencies (parent documents with delete propagation enabled in the
  // schema) must be alive in either the same batch of new documents or the
  // document store.
  //
  // Returns:
  //   - OK on success.
  //   - INVALID_ARGUMENT_ERROR if the validation fails, e.g. any of the
  //     dependencies (referenced parent documents) are not present.
  //   - Any error from document store or schema store.
  libtextclassifier3::Status Evaluate();

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
  // Returns:
  //   - OK on success.
  //   - INVALID_ARGUMENT_ERROR if dep_qualified_id_str is invalid or the
  //     document referenced by the qualified id is not present.
  //   - Any error from document store.
  libtextclassifier3::Status ValidateDependency(
      std::string_view dep_qualified_id_str) const;

  const DocumentStore& document_store_;
  const std::vector<TokenizedDocument>& batch_documents_to_add_;

  // A map for mapping qualified id to the index of batch_documents_to_add_.
  std::unordered_map<QualifiedId, int, QualifiedId::Hasher>
      qualified_id_to_batch_idx_;

  int64_t current_time_ms_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_JOIN_DOCUMENT_DEPENDENCY_PROCESSOR_H_
