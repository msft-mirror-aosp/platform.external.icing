// Copyright (C) 2022 Google LLC
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

#ifndef ICING_INDEX_STRING_SECTION_INDEXING_HANDLER_H_
#define ICING_INDEX_STRING_SECTION_INDEXING_HANDLER_H_

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/index/index.h"
#include "icing/index/section-indexing-handler.h"
#include "icing/proto/logging.pb.h"
#include "icing/store/document-id.h"
#include "icing/transform/normalizer.h"
#include "icing/util/clock.h"
#include "icing/util/tokenized-document.h"

namespace icing {
namespace lib {

class StringSectionIndexingHandler : public SectionIndexingHandler {
 public:
  explicit StringSectionIndexingHandler(const Clock* clock,
                                        const Normalizer* normalizer,
                                        Index* index)
      : SectionIndexingHandler(clock),
        normalizer_(*normalizer),
        index_(*index) {}

  ~StringSectionIndexingHandler() override = default;

  // Handles the string term indexing process: add hits into the lite index for
  // all contents in tokenized_document.tokenized_string_sections and merge lite
  // index into main index if necessary.
  //
  /// Returns:
  //   - OK on success
  //   - INVALID_ARGUMENT_ERROR if document_id is less than or equal to the
  //     document_id of a previously indexed document in non recovery mode.
  //   - RESOURCE_EXHAUSTED_ERROR if the index is full and can't add anymore
  //     content.
  //   - DATA_LOSS_ERROR if an attempt to merge the index fails and both indices
  //     are cleared as a result.
  //   - INTERNAL_ERROR if any other errors occur.
  //   - Any main/lite index errors.
  libtextclassifier3::Status Handle(
      const TokenizedDocument& tokenized_document, DocumentId document_id,
      bool recovery_mode, PutDocumentStatsProto* put_document_stats) override;

 private:
  const Normalizer& normalizer_;
  Index& index_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_STRING_SECTION_INDEXING_HANDLER_H_
