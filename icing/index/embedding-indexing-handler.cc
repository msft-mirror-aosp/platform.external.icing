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

#include "icing/index/embedding-indexing-handler.h"

#include <memory>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/index/embed/embedding-index.h"
#include "icing/index/hit/hit.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/util/clock.h"
#include "icing/util/status-macros.h"
#include "icing/util/tokenized-document.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<std::unique_ptr<EmbeddingIndexingHandler>>
EmbeddingIndexingHandler::Create(const Clock* clock,
                                 EmbeddingIndex* embedding_index) {
  ICING_RETURN_ERROR_IF_NULL(clock);
  ICING_RETURN_ERROR_IF_NULL(embedding_index);

  return std::unique_ptr<EmbeddingIndexingHandler>(
      new EmbeddingIndexingHandler(clock, embedding_index));
}

libtextclassifier3::Status EmbeddingIndexingHandler::Handle(
    const TokenizedDocument& tokenized_document, DocumentId document_id,
    bool recovery_mode, PutDocumentStatsProto* put_document_stats) {
  std::unique_ptr<Timer> index_timer = clock_.GetNewTimer();

  if (!IsDocumentIdValid(document_id)) {
    return absl_ports::InvalidArgumentError(
        IcingStringUtil::StringPrintf("Invalid DocumentId %d", document_id));
  }

  if (embedding_index_.last_added_document_id() != kInvalidDocumentId &&
      document_id <= embedding_index_.last_added_document_id()) {
    if (recovery_mode) {
      // Skip the document if document_id <= last_added_document_id in
      // recovery mode without returning an error.
      return libtextclassifier3::Status::OK;
    }
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "DocumentId %d must be greater than last added document_id %d",
        document_id, embedding_index_.last_added_document_id()));
  }
  embedding_index_.set_last_added_document_id(document_id);

  for (const Section<PropertyProto::VectorProto>& vector_section :
       tokenized_document.vector_sections()) {
    BasicHit hit(/*section_id=*/vector_section.metadata.id, document_id);
    for (const PropertyProto::VectorProto& vector : vector_section.content) {
      ICING_RETURN_IF_ERROR(embedding_index_.BufferEmbedding(hit, vector));
    }
  }
  ICING_RETURN_IF_ERROR(embedding_index_.CommitBufferToIndex());

  if (put_document_stats != nullptr) {
    put_document_stats->set_embedding_index_latency_ms(
        index_timer->GetElapsedMilliseconds());
  }

  return libtextclassifier3::Status::OK;
}

}  // namespace lib
}  // namespace icing
