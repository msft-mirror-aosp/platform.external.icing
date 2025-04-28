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

#include "icing/index/embed/doc-hit-info-iterator-embedding.h"

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/index/embed/embedding-hit.h"
#include "icing/index/embed/embedding-index.h"
#include "icing/index/embed/embedding-query-results.h"
#include "icing/index/embed/embedding-scorer.h"
#include "icing/index/embed/posting-list-embedding-hit-accessor.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/hit/hit.h"
#include "icing/proto/search.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIteratorEmbedding>>
DocHitInfoIteratorEmbedding::Create(
    const PropertyProto::VectorProto* query,
    SearchSpecProto::EmbeddingQueryMetricType::Code metric_type,
    double score_low, double score_high, bool get_embedding_match_info,
    EmbeddingQueryResults::EmbeddingQueryMatchInfoMap* info_map,
    const EmbeddingIndex* embedding_index, const DocumentStore* document_store,
    const SchemaStore* schema_store, int64_t current_time_ms) {
  ICING_RETURN_ERROR_IF_NULL(query);
  ICING_RETURN_ERROR_IF_NULL(embedding_index);
  ICING_RETURN_ERROR_IF_NULL(info_map);
  ICING_RETURN_ERROR_IF_NULL(document_store);
  ICING_RETURN_ERROR_IF_NULL(schema_store);

  libtextclassifier3::StatusOr<std::unique_ptr<PostingListEmbeddingHitAccessor>>
      pl_accessor_or = embedding_index->GetAccessorForVector(*query);
  std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor;
  if (pl_accessor_or.ok()) {
    pl_accessor = std::move(pl_accessor_or).ValueOrDie();
  } else if (absl_ports::IsNotFound(pl_accessor_or.status())) {
    // A not-found error should be fine, since that means there is no matching
    // embedding hits in the index.
    pl_accessor = nullptr;
  } else {
    // Otherwise, return the error as is.
    return pl_accessor_or.status();
  }

  ICING_ASSIGN_OR_RETURN(std::unique_ptr<EmbeddingScorer> embedding_scorer,
                         EmbeddingScorer::Create(metric_type));

  return std::unique_ptr<DocHitInfoIteratorEmbedding>(
      new DocHitInfoIteratorEmbedding(
          query, metric_type, std::move(embedding_scorer), score_low,
          score_high, get_embedding_match_info, info_map, embedding_index,
          std::move(pl_accessor), document_store, schema_store,
          current_time_ms));
}

libtextclassifier3::StatusOr<const EmbeddingHit*>
DocHitInfoIteratorEmbedding::AdvanceToNextEmbeddingHit() {
  if (cached_embedding_hits_idx_ == cached_embedding_hits_.size()) {
    ICING_ASSIGN_OR_RETURN(cached_embedding_hits_,
                           posting_list_accessor_->GetNextHitsBatch());
    cached_embedding_hits_idx_ = 0;
    if (cached_embedding_hits_.empty()) {
      no_more_hit_ = true;
      return nullptr;
    }
  }
  const EmbeddingHit& embedding_hit =
      cached_embedding_hits_[cached_embedding_hits_idx_];
  if (doc_hit_info_.document_id() == kInvalidDocumentId) {
    doc_hit_info_.set_document_id(embedding_hit.basic_hit().document_id());
    current_allowed_sections_mask_ =
        ComputeAllowedSectionsMask(doc_hit_info_.document_id());

    schema_type_id_ = document_store_.GetSchemaTypeId(
        doc_hit_info_.document_id(), current_time_ms_);
    if (schema_type_id_ == kInvalidSchemaTypeId) {
      // This means that the document is deleted or expired, so update
      // current_allowed_sections_mask_ to skip the document.
      current_allowed_sections_mask_ = kSectionIdMaskNone;
    }
  } else if (doc_hit_info_.document_id() !=
             embedding_hit.basic_hit().document_id()) {
    return nullptr;
  }
  ++cached_embedding_hits_idx_;
  return &embedding_hit;
}

libtextclassifier3::Status
DocHitInfoIteratorEmbedding::AdvanceToNextUnfilteredDocument() {
  if (no_more_hit_ || posting_list_accessor_ == nullptr) {
    return absl_ports::ResourceExhaustedError(
        "No more DocHitInfos in iterator");
  }

  doc_hit_info_ = DocHitInfo(kInvalidDocumentId, kSectionIdMaskNone);
  schema_type_id_ = kInvalidSchemaTypeId;
  EmbeddingMatchInfos* matched_infos = nullptr;
  current_allowed_sections_mask_ = kSectionIdMaskAll;
  SectionId current_section_id = kInvalidSectionId;
  EmbeddingIndexingConfig::QuantizationType::Code quantization_type =
      EmbeddingIndexingConfig::QuantizationType::NONE;
  int current_section_match_count = 0;

  while (true) {
    ICING_ASSIGN_OR_RETURN(const EmbeddingHit* embedding_hit,
                           AdvanceToNextEmbeddingHit());
    if (embedding_hit == nullptr) {
      // No more hits for the current document.
      break;
    }

    // Filter out the embedding hit according to the section restriction.
    if (((UINT64_C(1) << embedding_hit->basic_hit().section_id()) &
         current_allowed_sections_mask_) == 0) {
      continue;
    }

    // We've reached a new section. Reset the match count and retrieve the
    // quantization type for the new section.
    if (current_section_id != embedding_hit->basic_hit().section_id()) {
      current_section_match_count = 0;
      current_section_id = embedding_hit->basic_hit().section_id();
      // The schema type id is guaranteed to be valid here. Otherwise,
      // current_allowed_sections_mask_ should be assigned to kSectionIdMaskNone
      // by AdvanceToNextEmbeddingHit, and the embedding hit should have been
      // skipped above.
      ICING_ASSIGN_OR_RETURN(
          quantization_type,
          schema_store_.GetQuantizationType(
              schema_type_id_, current_section_id));
    }

    // Calculate the semantic score.
    ICING_ASSIGN_OR_RETURN(
        float semantic_score,
        embedding_index_.ScoreEmbeddingHit(*embedding_scorer_, query_,
                                           *embedding_hit, quantization_type));

    // If the semantic score is within the desired score range, update
    // doc_hit_info_ and info_map_.
    if (score_low_ <= semantic_score && semantic_score <= score_high_) {
      doc_hit_info_.UpdateSection(embedding_hit->basic_hit().section_id());
      if (matched_infos == nullptr) {
        matched_infos = &(info_map_[doc_hit_info_.document_id()]);
      }
      matched_infos->AppendScore(semantic_score);
      if (get_embedding_match_info_) {
        // Add the section info for this embedding match.
        matched_infos->AppendSectionInfo(current_section_id,
                                         current_section_match_count);
      }
    }
    ++current_section_match_count;
  }

  if (doc_hit_info_.document_id() == kInvalidDocumentId) {
    return absl_ports::ResourceExhaustedError(
        "No more DocHitInfos in iterator");
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status DocHitInfoIteratorEmbedding::Advance() {
  do {
    ICING_RETURN_IF_ERROR(AdvanceToNextUnfilteredDocument());
  } while (doc_hit_info_.hit_section_ids_mask() == kSectionIdMaskNone);
  ++num_advance_calls_;
  return libtextclassifier3::Status::OK;
}

}  // namespace lib
}  // namespace icing
