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

#include "icing/index/index-processor.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/index/index.h"
#include "icing/index/integer-section-indexing-handler.h"
#include "icing/index/numeric/numeric-index.h"
#include "icing/index/string-section-indexing-handler.h"
#include "icing/proto/logging.pb.h"
#include "icing/store/document-id.h"
#include "icing/transform/normalizer.h"
#include "icing/util/status-macros.h"
#include "icing/util/tokenized-document.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<std::unique_ptr<IndexProcessor>>
IndexProcessor::Create(const Normalizer* normalizer, Index* index,
                       NumericIndex<int64_t>* integer_index,
                       const Clock* clock) {
  ICING_RETURN_ERROR_IF_NULL(normalizer);
  ICING_RETURN_ERROR_IF_NULL(index);
  ICING_RETURN_ERROR_IF_NULL(integer_index);
  ICING_RETURN_ERROR_IF_NULL(clock);

  std::vector<std::unique_ptr<SectionIndexingHandler>> handlers;
  handlers.push_back(
      std::make_unique<StringSectionIndexingHandler>(clock, normalizer, index));
  handlers.push_back(
      std::make_unique<IntegerSectionIndexingHandler>(clock, integer_index));

  return std::unique_ptr<IndexProcessor>(
      new IndexProcessor(std::move(handlers), clock));
}

libtextclassifier3::Status IndexProcessor::IndexDocument(
    const TokenizedDocument& tokenized_document, DocumentId document_id,
    PutDocumentStatsProto* put_document_stats) {
  // TODO(b/259744228): set overall index latency.
  for (auto& section_indexing_handler : section_indexing_handlers_) {
    ICING_RETURN_IF_ERROR(section_indexing_handler->Handle(
        tokenized_document, document_id, put_document_stats));
  }

  return libtextclassifier3::Status::OK;
}

}  // namespace lib
}  // namespace icing
