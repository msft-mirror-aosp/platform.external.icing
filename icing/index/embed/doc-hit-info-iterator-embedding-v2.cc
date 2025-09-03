// Copyright (C) 2025 Google LLC
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

#include "icing/index/embed/doc-hit-info-iterator-embedding-v2.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <numeric>
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

libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIteratorEmbeddingV2>>
DocHitInfoIteratorEmbeddingV2::Create(
    const PropertyProto::VectorProto* query,
    SearchSpecProto::EmbeddingQueryMetricType::Code metric_type,
    double score_low, double score_high,
    EmbeddingQueryResults::EmbeddingQueryMatchInfoMap* info_map,
    std::vector<double>* global_scores,
    std::vector<EmbeddingMatchInfos::EmbeddingMatchSectionInfo>*
        global_section_infos,
    const EmbeddingIndex* embedding_index, const DocumentStore* document_store,
    const SchemaStore* schema_store, int64_t current_time_ms) {
  ICING_RETURN_ERROR_IF_NULL(query);
  ICING_RETURN_ERROR_IF_NULL(embedding_index);
  ICING_RETURN_ERROR_IF_NULL(info_map);
  ICING_RETURN_ERROR_IF_NULL(global_scores);
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

  return std::unique_ptr<DocHitInfoIteratorEmbeddingV2>(
      new DocHitInfoIteratorEmbeddingV2(
          query, metric_type, std::move(embedding_scorer), score_low,
          score_high, info_map, global_scores, global_section_infos,
          embedding_index, std::move(pl_accessor), document_store, schema_store,
          current_time_ms));
}

libtextclassifier3::Status
DocHitInfoIteratorEmbeddingV2::RetrieveNextHitsBatch() {
  ICING_ASSIGN_OR_RETURN(std::vector<EmbeddingHit> embedding_hits,
                         posting_list_accessor_->GetNextHitsBatch());
  if (embedding_hits.empty()) {
    no_more_hit_ = true;
  }

  cached_hit_scores_idx_ = 0;
  cached_hit_scores_.clear();
  cached_hit_scores_.resize(embedding_hits.size());

  // Create a sequential access order of the embedding hits for optimized
  // memory access for embedding data.
  std::vector<int> access_order(embedding_hits.size());
  std::iota(access_order.begin(), access_order.end(), 0);
  std::sort(access_order.begin(), access_order.end(),
            [&embedding_hits](int i, int j) {
              return embedding_hits[i].location() <
                     embedding_hits[j].location();
            });

  DocumentId document_id = kInvalidDocumentId;
  SchemaTypeId schema_type_id = kInvalidSchemaTypeId;
  SectionIdMask allowed_sections_mask = kSectionIdMaskAll;
  for (int i = 0; i < access_order.size(); ++i) {
    const EmbeddingHit& embedding_hit = embedding_hits[access_order[i]];
    // Update document id, schema type id, and allowed sections mask for the
    // new document.
    if (embedding_hit.basic_hit().document_id() != document_id) {
      document_id = embedding_hit.basic_hit().document_id();
      schema_type_id = kInvalidSchemaTypeId;
      allowed_sections_mask = kSectionIdMaskAll;

      if (DoesDocumentPassAllFilters(document_id)) {
        allowed_sections_mask = ComputeAllowedSectionsMask(document_id);

        schema_type_id =
            document_store_.GetSchemaTypeId(document_id, current_time_ms_);
        if (schema_type_id == kInvalidSchemaTypeId) {
          // This means that the document is deleted or expired, so update
          // allowed_sections_mask to skip the document.
          allowed_sections_mask = kSectionIdMaskNone;
        }
      } else {
        // This means that the document is filtered out by the document filter
        // predicate, so update allowed_sections_mask to skip the
        // document.
        allowed_sections_mask = kSectionIdMaskNone;
      }
    }

    SectionId section_id = embedding_hit.basic_hit().section_id();

    // Filter out the embedding hit according to the section restriction.
    if (((UINT64_C(1) << section_id) & allowed_sections_mask) == 0) {
      continue;
    }

    // The schema type id is guaranteed to be valid here. Otherwise,
    // allowed_sections_mask should be assigned to kSectionIdMaskNone, and the
    // embedding hit should have been skipped above.
    ICING_ASSIGN_OR_RETURN(
        EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
        schema_store_.GetQuantizationType(schema_type_id, section_id));

    // Calculate the semantic score.
    ICING_ASSIGN_OR_RETURN(
        float semantic_score,
        embedding_index_.ScoreEmbeddingHit(*embedding_scorer_, query_,
                                           embedding_hit, quantization_type));
    cached_hit_scores_[access_order[i]] = {embedding_hit.basic_hit(),
                                           semantic_score};
  }

  // Remove the embedding hits that are filtered out.
  int hit_cnt = 0;
  for (const HitWithScore& hit_with_score : cached_hit_scores_) {
    if (hit_with_score.hit.is_valid()) {
      cached_hit_scores_[hit_cnt++] = hit_with_score;
    }
  }
  cached_hit_scores_.resize(hit_cnt);
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<const DocHitInfoIteratorEmbeddingV2::HitWithScore*>
DocHitInfoIteratorEmbeddingV2::AdvanceToNextEmbeddingHit() {
  while (cached_hit_scores_idx_ == cached_hit_scores_.size()) {
    ICING_RETURN_IF_ERROR(RetrieveNextHitsBatch());
    if (no_more_hit_) {
      return nullptr;
    }
  }
  const HitWithScore& embedding_hit_score =
      cached_hit_scores_[cached_hit_scores_idx_];
  DocumentId document_id = embedding_hit_score.hit.document_id();
  if (doc_hit_info_.document_id() == kInvalidDocumentId) {
    doc_hit_info_.set_document_id(document_id);
  } else if (doc_hit_info_.document_id() != document_id) {
    return nullptr;
  }
  ++cached_hit_scores_idx_;
  return &embedding_hit_score;
}

libtextclassifier3::Status
DocHitInfoIteratorEmbeddingV2::AdvanceToNextUnfilteredDocument() {
  if (no_more_hit_ || posting_list_accessor_ == nullptr) {
    return absl_ports::ResourceExhaustedError(
        "No more DocHitInfos in iterator");
  }

  doc_hit_info_ = DocHitInfo(kInvalidDocumentId, kSectionIdMaskNone);
  EmbeddingMatchInfos* matched_infos = nullptr;
  SectionId current_section_id = kInvalidSectionId;
  int current_section_match_count = 0;

  while (true) {
    ICING_ASSIGN_OR_RETURN(const HitWithScore* embedding_hit_score,
                           AdvanceToNextEmbeddingHit());
    if (embedding_hit_score == nullptr) {
      // No more hits for the current document.
      break;
    }

    // We've reached a new section. Reset the match count and retrieve the
    // quantization type for the new section.
    if (current_section_id != embedding_hit_score->hit.section_id()) {
      current_section_match_count = 0;
      current_section_id = embedding_hit_score->hit.section_id();
    }

    float semantic_score = embedding_hit_score->score;
    // If the semantic score is within the desired score range, update
    // doc_hit_info_ and info_map_.
    if (score_low_ <= semantic_score && semantic_score <= score_high_) {
      doc_hit_info_.UpdateSection(embedding_hit_score->hit.section_id());
      if (matched_infos == nullptr) {
        matched_infos = &(info_map_[doc_hit_info_.document_id()]);
      }
      ICING_RETURN_IF_ERROR(
          matched_infos->AppendScore(global_scores_, semantic_score));
      if (global_section_infos_ != nullptr) {
        // Add the section info for this embedding match.
        ICING_RETURN_IF_ERROR(matched_infos->AppendSectionInfo(
            *global_section_infos_, current_section_id,
            current_section_match_count));
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

libtextclassifier3::Status DocHitInfoIteratorEmbeddingV2::Advance() {
  do {
    ICING_RETURN_IF_ERROR(AdvanceToNextUnfilteredDocument());
  } while (doc_hit_info_.hit_section_ids_mask() == kSectionIdMaskNone);
  ++num_advance_calls_;
  return libtextclassifier3::Status::OK;
}

}  // namespace lib
}  // namespace icing
