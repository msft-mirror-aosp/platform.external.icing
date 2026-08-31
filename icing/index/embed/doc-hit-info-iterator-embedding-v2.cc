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
    const std::vector<uint32_t>& cluster_ids,
    const EmbeddingIndex* embedding_index, const DocumentStore* document_store,
    const SchemaStore* schema_store, int64_t current_time_ms) {
  ICING_RETURN_ERROR_IF_NULL(query);
  ICING_RETURN_ERROR_IF_NULL(embedding_index);
  ICING_RETURN_ERROR_IF_NULL(info_map);
  ICING_RETURN_ERROR_IF_NULL(global_scores);
  ICING_RETURN_ERROR_IF_NULL(document_store);
  ICING_RETURN_ERROR_IF_NULL(schema_store);

  libtextclassifier3::StatusOr<
      std::unique_ptr<EmbeddingIndex::EmbeddingHitAccessor>>
      embedding_hit_accessor_or =
          embedding_index->GetAccessorForVector(*query, cluster_ids);
  std::unique_ptr<EmbeddingIndex::EmbeddingHitAccessor> embedding_hit_accessor;
  if (embedding_hit_accessor_or.ok()) {
    embedding_hit_accessor = std::move(embedding_hit_accessor_or).ValueOrDie();
  } else if (absl_ports::IsNotFound(embedding_hit_accessor_or.status())) {
    // A not-found error should be fine, since that means there is no matching
    // embedding hits in the index.
    embedding_hit_accessor = nullptr;
  } else {
    // Otherwise, return the error as is.
    return embedding_hit_accessor_or.status();
  }
  ICING_ASSIGN_OR_RETURN(int dimension, embedding_util::GetDimension(*query));
  std::vector<float> query_floats;
  if (!query->quantized_values().empty()) {
    query_floats.resize(dimension);
    embedding_util::Dequantize(query->quantized_values().data(), dimension,
                               query_floats.data());
  } else if (!query->values().empty()) {
    query_floats.assign(query->values().begin(), query->values().end());
  } else {
    return absl_ports::InvalidArgumentError("Query vector is empty");
  }

  ICING_ASSIGN_OR_RETURN(std::unique_ptr<EmbeddingScorer> embedding_scorer,
                         EmbeddingScorer::Create(metric_type));

  return std::unique_ptr<DocHitInfoIteratorEmbeddingV2>(
      new DocHitInfoIteratorEmbeddingV2(
          std::move(query_floats), metric_type, std::move(embedding_scorer),
          score_low, score_high, info_map, global_scores, global_section_infos,
          embedding_index, std::move(embedding_hit_accessor), document_store,
          schema_store, current_time_ms));
}

libtextclassifier3::Status
DocHitInfoIteratorEmbeddingV2::RetrieveNextHitsBatch() {
  ICING_RETURN_IF_ERROR(embedding_hit_accessor_->AssertSharedLockHeld());

  ICING_ASSIGN_OR_RETURN(
      std::vector<EmbeddingIndex::EmbeddingHitAccessor::HitInfo>
          embedding_hit_infos,
      embedding_hit_accessor_->GetNextHitsBatch());
  if (embedding_hit_infos.empty()) {
    no_more_hit_ = true;
  }

  // Create a sequential access order of the embedding hits for optimized
  // memory access for embedding data.
  cached_hit_scores_idx_ = 0;
  cached_hit_scores_.clear();
  cached_hit_scores_.resize(embedding_hit_infos.size());
  cached_delegate_matches_.clear();
  if (delegate_ != nullptr) {
    cached_delegate_matches_.resize(embedding_hit_infos.size());
  }

  std::vector<int> access_order(embedding_hit_infos.size());
  std::iota(access_order.begin(), access_order.end(), 0);
  std::sort(access_order.begin(), access_order.end(),
            [&embedding_hit_infos](int i, int j) {
              return embedding_hit_infos[i].hit.location() <
                     embedding_hit_infos[j].hit.location();
            });
  if (delegate_ != nullptr) {
    // Add to access_order and filter any docids that don't match the delegate.
    for (int i = 0; i < embedding_hit_infos.size(); ++i) {
      while (delegate_->doc_hit_info().document_id() == kInvalidDocumentId ||
             delegate_->doc_hit_info().document_id() >
                 embedding_hit_infos[i].hit.basic_hit().document_id()) {
        if (!delegate_->Advance().ok()) {
          break;
        }
      }
      if (delegate_->doc_hit_info().document_id() ==
          embedding_hit_infos[i].hit.basic_hit().document_id()) {
        cached_delegate_matches_[i] = {
            delegate_->doc_hit_info().hit_section_ids_mask(),
            /*is_matched=*/true};
      } else {
        cached_delegate_matches_[i] = {kSectionIdMaskNone,
                                       /*is_matched=*/false};
      }
    }
  }

  DocumentId document_id = kInvalidDocumentId;
  SchemaTypeId schema_type_id = kInvalidSchemaTypeId;
  uint32_t schema_name_hash = 0;

  SectionIdMask allowed_sections_mask = kSectionIdMaskNone;
  for (int i = 0; i < access_order.size(); ++i) {
    const EmbeddingIndex::EmbeddingHitAccessor::HitInfo& hit_info =
        embedding_hit_infos[access_order[i]];
    const EmbeddingHit& embedding_hit = hit_info.hit;
    // Update document id, schema type id, and allowed sections mask for the
    // new document.
    if (embedding_hit.basic_hit().document_id() != document_id) {
      document_id = embedding_hit.basic_hit().document_id();
      schema_type_id = kInvalidSchemaTypeId;
      schema_name_hash = 0;
      allowed_sections_mask = kSectionIdMaskNone;

      if (!DoesDocumentPassAllFilters(document_id)) {
        // This means that the document is filtered out by the document filter
        // predicate. Just skip the document.
        continue;
      }
      if (delegate_ != nullptr &&
          !cached_delegate_matches_[access_order[i]].is_matched) {
        // The document has been filtered out by the delegate. Therefore, there
        // is no need to calculate the embedding score for this document.
        continue;
      }
      schema_type_id =
          document_store_.GetSchemaTypeId(document_id, current_time_ms_);
      if (schema_type_id == kInvalidSchemaTypeId) {
        // This means that the document is deleted or expired.
        // No need to update allowed_sections_mask back to kSectionIdMaskNone,
        // since we will always check schema_type_id itself.
        continue;
      }
      ICING_ASSIGN_OR_RETURN(schema_name_hash,
                             schema_store_.GetSchemaNameHash(schema_type_id));
      allowed_sections_mask = ComputeAllowedSectionsMask(document_id);
    }

    SectionId section_id = embedding_hit.basic_hit().section_id();

    // Filter out the embedding hit according to the section restriction.
    if (((UINT64_C(1) << section_id) & allowed_sections_mask) == 0) {
      continue;
    }

    // Filter out the embedding hit if the schema type id is invalid.
    if (schema_type_id == kInvalidSchemaTypeId) {
      continue;
    }

    // The schema type id is guaranteed to be valid here. Otherwise,
    // allowed_sections_mask should be assigned to kSectionIdMaskNone, and the
    // embedding hit should have been skipped above.
    ICING_ASSIGN_OR_RETURN(
        const SectionMetadata* section_metadata,
        schema_store_.GetSectionMetadata(schema_type_id, section_id));
    bool is_ann = section_metadata->embedding_indexing_type ==
                  EmbeddingIndexingConfig::EmbeddingIndexingType::
                      APPROXIMATE_NEAREST_NEIGHBOR;

    ICING_ASSIGN_OR_RETURN(
        float semantic_score,
        embedding_hit_accessor_->ScoreEmbeddingHit(
            *embedding_scorer_, query_floats_, hit_info,
            section_metadata->quantization_type, schema_name_hash, is_ann));
    cached_hit_scores_[access_order[i]] = {embedding_hit.basic_hit(),
                                           semantic_score, is_ann};
  }

  // Remove the embedding hits that are filtered out.
  int hit_cnt = 0;
  for (int i = 0; i < cached_hit_scores_.size(); ++i) {
    const HitWithScore& hit_with_score = cached_hit_scores_[i];
    if (hit_with_score.hit.is_valid()) {
      cached_hit_scores_[hit_cnt] = hit_with_score;
      if (delegate_ != nullptr && i < cached_delegate_matches_.size()) {
        cached_delegate_matches_[hit_cnt] = cached_delegate_matches_[i];
      }
      ++hit_cnt;
    }
  }
  cached_hit_scores_.resize(hit_cnt);
  if (!cached_delegate_matches_.empty()) {
    cached_delegate_matches_.resize(hit_cnt);
  }
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
  if (no_more_hit_ || embedding_hit_accessor_ == nullptr) {
    return absl_ports::ResourceExhaustedError("");
  }

  doc_hit_info_ = DocHitInfo(kInvalidDocumentId, kSectionIdMaskNone);
  EmbeddingMatchInfos* matched_infos = nullptr;
  SectionId current_section_id = kInvalidSectionId;
  int current_section_match_count = 0;

  SectionIdMask delegate_section_id_mask = kSectionIdMaskNone;
  std::vector<HitWithScore> current_doc_hit_scores;
  while (true) {
    ICING_ASSIGN_OR_RETURN(const HitWithScore* embedding_hit_score,
                           AdvanceToNextEmbeddingHit());
    if (embedding_hit_score == nullptr) {
      // No more hits for the current document.
      break;
    }
    if (delegate_ != nullptr) {
      delegate_section_id_mask |=
          cached_delegate_matches_[cached_hit_scores_idx_ - 1].section_id_mask;
    }
    current_doc_hit_scores.push_back(*embedding_hit_score);
  }

  for (const HitWithScore& embedding_hit_score : current_doc_hit_scores) {
    // We've reached a new section. Reset the match count and retrieve the
    // quantization type for the new section.
    if (current_section_id != embedding_hit_score.hit.section_id()) {
      current_section_match_count = 0;
      current_section_id = embedding_hit_score.hit.section_id();
    }

    float semantic_score = embedding_hit_score.score;
    // If the semantic score is within the desired score range, update
    // doc_hit_info_ and info_map_.
    if (score_low_ <= semantic_score && semantic_score <= score_high_) {
      doc_hit_info_.UpdateSection(embedding_hit_score.hit.section_id());
      if (matched_infos == nullptr) {
        matched_infos = &(info_map_[doc_hit_info_.document_id()]);
      }
      ICING_RETURN_IF_ERROR(
          matched_infos->AppendScore(global_scores_, semantic_score));
      if (global_section_infos_ != nullptr) {
        // Add the section info for this embedding match.
        // For ANN, we can't support retrieving the specific vector position yet
        // because cluster hits for the same document and property may not be
        // contiguous in the posting list, thus breaking the sequential
        // current_section_match_count trick used by snippeting. Therefore, we
        // use -1 as the position for ANN to indicate that the specific vector
        // position is not supported.
        int position =
            embedding_hit_score.is_ann ? -1 : current_section_match_count;
        ICING_RETURN_IF_ERROR(matched_infos->AppendSectionInfo(
            *global_section_infos_, current_section_id, position));
      }
    }
    ++current_section_match_count;
  }
  if (doc_hit_info_.hit_section_ids_mask() != kSectionIdMaskNone) {
    // The hit sections id mask is used by the caller of this function
    // (DocHitInfoIteratorEmbeddingV2::Advance) to determine whether
    // the current document has any hits meeting our score thresholds.
    // Therefore, we can only merge here so long as at least one embedding hit
    // has passed the score threshold.
    doc_hit_info_.MergeSectionsFrom(delegate_section_id_mask);
  }

  if (doc_hit_info_.document_id() == kInvalidDocumentId) {
    return absl_ports::ResourceExhaustedError("");
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status DocHitInfoIteratorEmbeddingV2::Advance() {
  do {
    libtextclassifier3::Status status = AdvanceToNextUnfilteredDocument();
    if (!status.ok()) {
      doc_hit_info_ = DocHitInfo(kInvalidDocumentId, kSectionIdMaskNone);
      return status;
    }
  } while (doc_hit_info_.hit_section_ids_mask() == kSectionIdMaskNone);
  ++num_advance_calls_;
  return libtextclassifier3::Status::OK;
}

}  // namespace lib
}  // namespace icing
