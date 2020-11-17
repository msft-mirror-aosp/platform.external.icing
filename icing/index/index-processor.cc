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
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/index/index.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/schema/section-manager.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/tokenization/token.h"
#include "icing/tokenization/tokenizer-factory.h"
#include "icing/tokenization/tokenizer.h"
#include "icing/transform/normalizer.h"
#include "icing/util/status-macros.h"
#include "icing/util/timer.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<std::unique_ptr<IndexProcessor>>
IndexProcessor::Create(const SchemaStore* schema_store,
                       const LanguageSegmenter* lang_segmenter,
                       const Normalizer* normalizer, Index* index,
                       const IndexProcessor::Options& options) {
  ICING_RETURN_ERROR_IF_NULL(schema_store);
  ICING_RETURN_ERROR_IF_NULL(lang_segmenter);
  ICING_RETURN_ERROR_IF_NULL(normalizer);
  ICING_RETURN_ERROR_IF_NULL(index);

  return std::unique_ptr<IndexProcessor>(new IndexProcessor(
      schema_store, lang_segmenter, normalizer, index, options));
}

libtextclassifier3::Status IndexProcessor::IndexDocument(
    const DocumentProto& document, DocumentId document_id,
    NativePutDocumentStats* put_document_stats) {
  Timer index_timer;

  if (index_->last_added_document_id() != kInvalidDocumentId &&
      document_id <= index_->last_added_document_id()) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "DocumentId %d must be greater than last added document_id %d",
        document_id, index_->last_added_document_id()));
  }
  ICING_ASSIGN_OR_RETURN(std::vector<Section> sections,
                         schema_store_.ExtractSections(document));
  uint32_t num_tokens = 0;
  libtextclassifier3::Status overall_status;
  for (const Section& section : sections) {
    // TODO(b/152934343): pass real namespace ids in
    Index::Editor editor =
        index_->Edit(document_id, section.metadata.id,
                     section.metadata.term_match_type, /*namespace_id=*/0);
    for (std::string_view subcontent : section.content) {
      ICING_ASSIGN_OR_RETURN(std::unique_ptr<Tokenizer> tokenizer,
                             tokenizer_factory::CreateIndexingTokenizer(
                                 section.metadata.tokenizer, &lang_segmenter_));
      ICING_ASSIGN_OR_RETURN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer->Tokenize(subcontent));
      while (itr->Advance()) {
        if (++num_tokens > options_.max_tokens_per_document) {
          if (put_document_stats != nullptr) {
            put_document_stats->mutable_tokenization_stats()
                ->set_exceeded_max_token_num(true);
            put_document_stats->mutable_tokenization_stats()
                ->set_num_tokens_indexed(options_.max_tokens_per_document);
          }
          switch (options_.token_limit_behavior) {
            case Options::TokenLimitBehavior::kReturnError:
              return absl_ports::ResourceExhaustedError(
                  "Max number of tokens reached!");
            case Options::TokenLimitBehavior::kSuppressError:
              return libtextclassifier3::Status::OK;
          }
        }
        std::string term = normalizer_.NormalizeTerm(itr->GetToken().text);
        // Add this term to the index. Even if adding this hit fails, we keep
        // trying to add more hits because it's possible that future hits could
        // still be added successfully. For instance if the lexicon is full, we
        // might fail to add a hit for a new term, but should still be able to
        // add hits for terms that are already in the index.
        auto status = editor.AddHit(term.c_str());
        if (overall_status.ok() && !status.ok()) {
          // If we've succeeded to add everything so far, set overall_status to
          // represent this new failure. If we've already failed, no need to
          // update the status - we're already going to return a resource
          // exhausted error.
          overall_status = status;
        }
      }
    }
  }

  if (put_document_stats != nullptr) {
    put_document_stats->set_index_latency_ms(
        index_timer.GetElapsedMilliseconds());
    put_document_stats->mutable_tokenization_stats()->set_num_tokens_indexed(
        num_tokens);
  }

  // Merge if necessary.
  if (overall_status.ok() && index_->WantsMerge()) {
    ICING_VLOG(1) << "Merging the index at docid " << document_id << ".";

    Timer merge_timer;
    libtextclassifier3::Status merge_status = index_->Merge();

    if (!merge_status.ok()) {
      ICING_LOG(ERROR) << "Index merging failed. Clearing index.";
      if (!index_->Reset().ok()) {
        return absl_ports::InternalError(IcingStringUtil::StringPrintf(
            "Unable to reset to clear index after merge failure. Merge "
            "failure=%d:%s",
            merge_status.error_code(), merge_status.error_message().c_str()));
      } else {
        return absl_ports::DataLossError(IcingStringUtil::StringPrintf(
            "Forced to reset index after merge failure. Merge failure=%d:%s",
            merge_status.error_code(), merge_status.error_message().c_str()));
      }
    }

    if (put_document_stats != nullptr) {
      put_document_stats->set_index_merge_latency_ms(
          merge_timer.GetElapsedMilliseconds());
    }
  }

  return overall_status;
}

}  // namespace lib
}  // namespace icing
