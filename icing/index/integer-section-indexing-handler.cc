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

#include "icing/index/integer-section-indexing-handler.h"

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/schema/section-manager.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/util/logging.h"
#include "icing/util/tokenized-document.h"

namespace icing {
namespace lib {

libtextclassifier3::Status IntegerSectionIndexingHandler::Handle(
    const TokenizedDocument& tokenized_document, DocumentId document_id,
    PutDocumentStatsProto* put_document_stats) {
  // TODO(b/259744228):
  // 1. Resolve last_added_document_id for index rebuilding before rollout
  // 2. Set integer indexing latency and other stats

  libtextclassifier3::Status status;
  // We have to add integer sections into integer index in reverse order because
  // sections are sorted by SectionId in ascending order, but BasicHit should be
  // added in descending order of SectionId (posting list requirement).
  for (auto riter = tokenized_document.integer_sections().rbegin();
       riter != tokenized_document.integer_sections().rend(); ++riter) {
    const Section<int64_t>& section = *riter;
    std::unique_ptr<NumericIndex<int64_t>::Editor> editor = integer_index_.Edit(
        section.metadata.path, document_id, section.metadata.id);

    for (int64_t key : section.content) {
      status = editor->BufferKey(key);
      if (!status.ok()) {
        ICING_LOG(WARNING)
            << "Failed to buffer keys into integer index due to: "
            << status.error_message();
        break;
      }
    }
    if (!status.ok()) {
      break;
    }

    // Add all the seen keys to the integer index.
    status = editor->IndexAllBufferedKeys();
    if (!status.ok()) {
      ICING_LOG(WARNING) << "Failed to add keys into integer index due to: "
                         << status.error_message();
      break;
    }
  }

  return status;
}

}  // namespace lib
}  // namespace icing
