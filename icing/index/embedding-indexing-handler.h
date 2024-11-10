// Copyright (C) 2024 Google LLC
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

#ifndef ICING_INDEX_EMBEDDING_INDEXING_HANDLER_H_
#define ICING_INDEX_EMBEDDING_INDEXING_HANDLER_H_

#include <memory>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/index/data-indexing-handler.h"
#include "icing/index/embed/embedding-index.h"
#include "icing/store/document-id.h"
#include "icing/util/clock.h"
#include "icing/util/tokenized-document.h"

namespace icing {
namespace lib {

class EmbeddingIndexingHandler : public DataIndexingHandler {
 public:
  ~EmbeddingIndexingHandler() override = default;

  // Creates an EmbeddingIndexingHandler instance which does not take
  // ownership of any input components. All pointers must refer to valid objects
  // that outlive the created EmbeddingIndexingHandler instance.
  //
  // Returns:
  //   - An EmbeddingIndexingHandler instance on success
  //   - FAILED_PRECONDITION_ERROR if any of the input pointer is null
  static libtextclassifier3::StatusOr<std::unique_ptr<EmbeddingIndexingHandler>>
  Create(const Clock* clock, EmbeddingIndex* embedding_index,
         bool enable_embedding_index);

  // Handles the embedding indexing process: add hits into the embedding index
  // for all contents in tokenized_document.vector_sections.
  //
  // Returns:
  //   - OK on success.
  //   - INVALID_ARGUMENT_ERROR if document_id is invalid OR document_id is less
  //     than or equal to the document_id of a previously indexed document in
  //     non recovery mode.
  //   - INTERNAL_ERROR if any other errors occur.
  //   - Any embedding index errors.
  libtextclassifier3::Status Handle(
      const TokenizedDocument& tokenized_document, DocumentId document_id,
      bool recovery_mode, PutDocumentStatsProto* put_document_stats) override;

 private:
  explicit EmbeddingIndexingHandler(const Clock* clock,
                                    EmbeddingIndex* embedding_index,
                                    bool enable_embedding_index)
      : DataIndexingHandler(clock),
        embedding_index_(*embedding_index),
        enable_embedding_index_(enable_embedding_index) {}

  EmbeddingIndex& embedding_index_;
  bool enable_embedding_index_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_EMBEDDING_INDEXING_HANDLER_H_
