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

#ifndef ICING_INDEX_EMBED_DOC_HIT_INFO_ITERATOR_EMBEDDING_V2_H_
#define ICING_INDEX_EMBED_DOC_HIT_INFO_ITERATOR_EMBEDDING_V2_H_

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/index/embed/embedding-index.h"
#include "icing/index/embed/embedding-query-results.h"
#include "icing/index/embed/embedding-scorer.h"
#include "icing/index/hit/hit.h"
#include "icing/index/iterator/doc-hit-info-iterator-section-restrict.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/index/iterator/document-filter-predicate.h"
#include "icing/index/iterator/section-restrict-data.h"
#include "icing/proto/search.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/store/document-store.h"

namespace icing {
namespace lib {

class DocHitInfoIteratorEmbeddingV2
    : public DocHitInfoIteratorHandlingSectionRestrict,
      public DocHitInfoIteratorHandlingFilter {
 public:
  // Create a DocHitInfoIterator for iterating through all docs which have an
  // embedding matched with the provided query with a score in the range of
  // [score_low, score_high], using the provided metric_type.
  //
  // The iterator will store the matched embedding scores in info_map to
  // prepare for scoring and snippeting.
  //
  // The iterator will handle the section restriction logic internally with the
  // help of DocHitInfoIteratorHandlingSectionRestrict.
  //
  // Returns:
  //   - a DocHitInfoIteratorEmbeddingV2 instance on success.
  //   - Any error from posting lists.
  static libtextclassifier3::StatusOr<
      std::unique_ptr<DocHitInfoIteratorEmbeddingV2>>
  Create(const PropertyProto::VectorProto* query,
         SearchSpecProto::EmbeddingQueryMetricType::Code metric_type,
         double score_low, double score_high,
         EmbeddingQueryResults::EmbeddingQueryMatchInfoMap* info_map,
         std::vector<double>* global_scores,
         std::vector<EmbeddingMatchInfos::EmbeddingMatchSectionInfo>*
             global_section_infos,
         const EmbeddingIndex* embedding_index,
         const DocumentStore* document_store, const SchemaStore* schema_store,
         int64_t current_time_ms);

  void AdoptDelegate(std::unique_ptr<DocHitInfoIterator> delegate,
                     bool delegate_node_is_right_most) {
    delegate_ = std::move(delegate);
    delegate_node_is_right_most_ = delegate_node_is_right_most;
  }

  bool HandleSectionRestriction(SectionRestrictData* other_data) override {
    // Apply section restriction to delegate if we have one.
    if (delegate_ != nullptr) {
      delegate_ = DocHitInfoIteratorSectionRestrict::ApplyRestrictions(
          std::move(delegate_), other_data);
    }
    return DocHitInfoIteratorHandlingSectionRestrict::HandleSectionRestriction(
        other_data);
  }

  libtextclassifier3::Status Advance() override;

  libtextclassifier3::StatusOr<TrimmedNode> TrimRightMostNode() && override {
    if (delegate_ != nullptr && !delegate_node_is_right_most_) {
      return std::move(*delegate_).TrimRightMostNode();
    }
    return absl_ports::InvalidArgumentError(
        "Query suggestions for the semanticSearch function are not supported");
  }

  std::vector<std::unique_ptr<DocHitInfoIterator>*> GetChildren() override {
    if (delegate_ != nullptr) {
      return {&delegate_};
    }
    return {};
  }

  CallStats GetCallStats() const override {
    CallStats call_stats(
        /*num_leaf_advance_calls_lite_index_in=*/num_advance_calls_,
        /*num_leaf_advance_calls_main_index_in=*/0,
        /*num_leaf_advance_calls_integer_index_in=*/0,
        /*num_leaf_advance_calls_no_index_in=*/0,
        /*num_blocks_inspected_in=*/0,
        embedding_hit_accessor_ != nullptr
            ? embedding_hit_accessor_->GetEmbeddingStats()
            : CallStats::EmbeddingStats{});
    if (delegate_ != nullptr) {
      call_stats += delegate_->GetCallStats();
    }
    return call_stats;
  }

  std::string ToString() const override {
    if (delegate_ != nullptr) {
      return absl_ports::StrCat("embedding_iterator with delegate (",
                                delegate_->ToString(), ")");
    }
    return "embedding_iterator";
  }

  // PopulateMatchedTermsStats is not applicable to embedding search.
  void PopulateMatchedTermsStats(
      std::vector<TermMatchInfo>* matched_terms_stats,
      SectionIdMask filtering_section_mask) const override {
    if (delegate_ != nullptr) {
      delegate_->PopulateMatchedTermsStats(matched_terms_stats,
                                           filtering_section_mask);
    }
  }

 private:
  struct HitWithScore {
    BasicHit hit;
    float score;
  };

  explicit DocHitInfoIteratorEmbeddingV2(
      const PropertyProto::VectorProto* query,
      SearchSpecProto::EmbeddingQueryMetricType::Code metric_type,
      std::unique_ptr<EmbeddingScorer> embedding_scorer, double score_low,
      double score_high,
      EmbeddingQueryResults::EmbeddingQueryMatchInfoMap* info_map,
      std::vector<double>* global_scores,
      std::vector<EmbeddingMatchInfos::EmbeddingMatchSectionInfo>*
          global_section_infos,
      const EmbeddingIndex* embedding_index,
      std::unique_ptr<EmbeddingIndex::EmbeddingHitAccessor>
          embedding_hit_accessor,
      const DocumentStore* document_store, const SchemaStore* schema_store,
      int64_t current_time_ms)
      : query_(*query),
        metric_type_(metric_type),
        embedding_scorer_(std::move(embedding_scorer)),
        score_low_(score_low),
        score_high_(score_high),
        info_map_(*info_map),
        global_scores_(*global_scores),
        global_section_infos_(global_section_infos),
        embedding_index_(*embedding_index),
        embedding_hit_accessor_(std::move(embedding_hit_accessor)),
        cached_hit_scores_idx_(0),
        no_more_hit_(false),
        document_store_(*document_store),
        schema_store_(*schema_store),
        current_time_ms_(current_time_ms),
        num_advance_calls_(0) {}

  // Retrieve the next batch of embedding hits from the posting list.
  //
  // Hits that do not pass section restriction or document filter will be
  // filtered out. Otherwise, the hits will be scored and added to
  // cached_hit_scores_.
  //
  // Returns:
  //   - OK, if it is able to retrieve the next batch of embedding hits.
  //   - Any error from posting lists.
  libtextclassifier3::Status RetrieveNextHitsBatch();

  // Advance to the next embedding hit of the current document. If the current
  // document id is kInvalidDocumentId, the method will advance to the first
  // embedding hit of the next document and update doc_hit_info_.
  //
  // This method also properly updates cached_hit_scores_,
  // cached_hit_scores_idx_, and no_more_hit_ to reflect the current
  // state.
  //
  // Returns:
  //   - a const pointer to the next embedding hit on success.
  //   - nullptr, if there is no more hit for the current document, or no more
  //     hit in general if the current document id is kInvalidDocumentId.
  //   - Any error from posting lists.
  libtextclassifier3::StatusOr<const HitWithScore*> AdvanceToNextEmbeddingHit();

  // Similar to Advance(), this method advances the iterator to the next
  // document, but it does not guarantee that the next document will have
  // a matched embedding hit within the score range.
  //
  // Returns:
  //   - OK, if it is able to advance to a new document_id.
  //   - RESOUCE_EXHAUSTED, if we have run out of document_ids to iterate over.
  //   - Any error from posting lists.
  libtextclassifier3::Status AdvanceToNextUnfilteredDocument();

  std::unique_ptr<DocHitInfoIterator> delegate_;
  // Whether the delegate is a node to the right of the current node. This
  // affects the behavior of TrimRightMostNode.
  bool delegate_node_is_right_most_;

  // Query information
  const PropertyProto::VectorProto& query_;  // Does not own

  // Scoring arguments
  SearchSpecProto::EmbeddingQueryMetricType::Code metric_type_;
  std::unique_ptr<EmbeddingScorer> embedding_scorer_;
  double score_low_;
  double score_high_;

  // MatchInfo map
  EmbeddingQueryResults::EmbeddingQueryMatchInfoMap& info_map_;  // Does not own
  std::vector<double>& global_scores_;                           // Does not own
  // Nullable, and does not own. If null, section info will not be populated.
  std::vector<EmbeddingMatchInfos::EmbeddingMatchSectionInfo>*
      global_section_infos_;

  // Access to embeddings index data
  const EmbeddingIndex& embedding_index_;
  std::unique_ptr<EmbeddingIndex::EmbeddingHitAccessor>
      embedding_hit_accessor_;  // Nullable.

  // Cached data from the embeddings index
  std::vector<HitWithScore> cached_hit_scores_;
  std::vector<SectionIdMask> cached_section_id_masks_;
  int cached_hit_scores_idx_;
  bool no_more_hit_;

  const DocumentStore& document_store_;
  const SchemaStore& schema_store_;
  int64_t current_time_ms_;
  int num_advance_calls_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_EMBED_DOC_HIT_INFO_ITERATOR_EMBEDDING_V2_H_
