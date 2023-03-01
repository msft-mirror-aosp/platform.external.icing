// Copyright (C) 2019 Google LLC
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

#ifndef ICING_INDEX_INDEX_PROCESSOR_H_
#define ICING_INDEX_INDEX_PROCESSOR_H_

#include <cstdint>
#include <memory>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/index/index.h"
#include "icing/index/numeric/numeric-index.h"
#include "icing/index/section-indexing-handler.h"
#include "icing/proto/logging.pb.h"
#include "icing/store/document-id.h"
#include "icing/transform/normalizer.h"
#include "icing/util/tokenized-document.h"

namespace icing {
namespace lib {

class IndexProcessor {
 public:
  // Factory function to create an IndexProcessor which does not take ownership
  // of any input components, and all pointers must refer to valid objects that
  // outlive the created IndexProcessor instance.
  //
  // - recovery_mode: a flag indicates that if IndexProcessor is used to restore
  //   index. Since there are several indices (term, integer) being restored at
  //   the same time, we start with the minimum last added DocumentId of all
  //   indices and replay documents to re-index, so it is possible to get some
  //   previously indexed documents in the recovery mode. Therefore, we should
  //   skip them without returning an error in recovery mode.
  //
  // Returns:
  //   An IndexProcessor on success
  //   FAILED_PRECONDITION if any of the pointers is null.
  static libtextclassifier3::StatusOr<std::unique_ptr<IndexProcessor>> Create(
      const Normalizer* normalizer, Index* index,
      NumericIndex<int64_t>* integer_index_, const Clock* clock,
      bool recovery_mode = false);

  // Add tokenized document to the index, associated with document_id. If the
  // number of tokens in the document exceeds max_tokens_per_document, then only
  // the first max_tokens_per_document will be added to the index. All tokens of
  // length exceeding max_token_length will be shortened to max_token_length.
  //
  // Indexing a document *may* trigger an index merge. If a merge fails, then
  // all content in the index will be lost.
  //
  // If put_document_stats is present, the fields related to indexing will be
  // populated.
  //
  // Returns:
  //   - OK on success.
  //   - Any SectionIndexingHandler errors.
  libtextclassifier3::Status IndexDocument(
      const TokenizedDocument& tokenized_document, DocumentId document_id,
      PutDocumentStatsProto* put_document_stats = nullptr);

 private:
  explicit IndexProcessor(std::vector<std::unique_ptr<SectionIndexingHandler>>&&
                              section_indexing_handlers,
                          const Clock* clock, bool recovery_mode)
      : section_indexing_handlers_(std::move(section_indexing_handlers)),
        clock_(*clock),
        recovery_mode_(recovery_mode) {}

  std::vector<std::unique_ptr<SectionIndexingHandler>>
      section_indexing_handlers_;
  const Clock& clock_;
  bool recovery_mode_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_INDEX_PROCESSOR_H_
