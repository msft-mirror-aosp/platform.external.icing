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

#ifndef ICING_INDEX_SECTION_INDEXING_HANDLER_H_
#define ICING_INDEX_SECTION_INDEXING_HANDLER_H_

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/proto/logging.pb.h"
#include "icing/store/document-id.h"
#include "icing/util/clock.h"
#include "icing/util/tokenized-document.h"

namespace icing {
namespace lib {

// Parent class for indexing different types of sections in TokenizedDocument.
class SectionIndexingHandler {
 public:
  explicit SectionIndexingHandler(const Clock* clock) : clock_(*clock) {}

  virtual ~SectionIndexingHandler() = default;

  // Handles the indexing process: add data (hits) into the specific type index
  // (e.g. string index, integer index) for all contents in the corresponding
  // type of sections in tokenized_document.
  // For example, IntegerSectionIndexingHandler::Handle should add data into
  // integer index for all contents in tokenized_document.integer_sections.
  //
  // tokenized_document: document object with different types of tokenized
  //                     sections.
  // document_id:        id of the document.
  // put_document_stats: object for collecting stats during indexing. It can be
  //                     nullptr.
  //
  /// Returns:
  //   - OK on success
  //   - Any other errors. It depends on each implementation.
  virtual libtextclassifier3::Status Handle(
      const TokenizedDocument& tokenized_document, DocumentId document_id,
      PutDocumentStatsProto* put_document_stats) = 0;

 protected:
  const Clock& clock_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_SECTION_INDEXING_HANDLER_H_
