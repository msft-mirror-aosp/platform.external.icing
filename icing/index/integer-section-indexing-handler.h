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

#ifndef ICING_INDEX_INTEGER_SECTION_INDEXING_HANDLER_H_
#define ICING_INDEX_INTEGER_SECTION_INDEXING_HANDLER_H_

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/index/numeric/numeric-index.h"
#include "icing/index/section-indexing-handler.h"
#include "icing/store/document-id.h"
#include "icing/util/clock.h"
#include "icing/util/tokenized-document.h"

namespace icing {
namespace lib {

class IntegerSectionIndexingHandler : public SectionIndexingHandler {
 public:
  explicit IntegerSectionIndexingHandler(const Clock* clock,
                                         NumericIndex<int64_t>* integer_index)
      : SectionIndexingHandler(clock), integer_index_(*integer_index) {}

  ~IntegerSectionIndexingHandler() override = default;

  // Handles the integer indexing process: add hits into the integer index for
  // all contents in tokenized_document.integer_sections.
  //
  // Returns:
  //   - OK on success
  //   - INVALID_ARGUMENT_ERROR if document_id is less than or equal to the
  //     document_id of a previously indexed document in non recovery mode.
  //   - Any NumericIndex<int64_t>::Editor errors.
  libtextclassifier3::Status Handle(
      const TokenizedDocument& tokenized_document, DocumentId document_id,
      bool recovery_mode, PutDocumentStatsProto* put_document_stats) override;

 private:
  NumericIndex<int64_t>& integer_index_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_INTEGER_SECTION_INDEXING_HANDLER_H_
