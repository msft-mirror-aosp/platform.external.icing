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

#ifndef ICING_INDEX_EMBED_DOC_HIT_INFO_ITERATOR_EMBEDDING_H_
#define ICING_INDEX_EMBED_DOC_HIT_INFO_ITERATOR_EMBEDDING_H_

#include <memory>
#include <string>
#include <string_view>
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
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/index/iterator/section-restrict-data.h"
#include "icing/proto/search.pb.h"
#include "icing/schema/section.h"

namespace icing {
namespace lib {

class DocHitInfoIteratorEmbedding : public DocHitInfoLeafIterator {
 public:
  // Create a DocHitInfoIterator for iterating through all docs which have an
  // embedding matched with the provided query with a score in the range of
  // [score_low, score_high], using the provided metric_type.
  //
  // The iterator will store the matched embedding scores in score_map to
  // prepare for scoring.
  //
  // The iterator will handle the section restriction logic internally by the
  // provided section_restrict_data.
  //
  // Returns:
  //   - a DocHitInfoIteratorEmbedding instance on success.
  //   - Any error from posting lists.
  static libtextclassifier3::StatusOr<
      std::unique_ptr<DocHitInfoIteratorEmbedding>>
  Create(const PropertyProto::VectorProto* query,
         std::unique_ptr<SectionRestrictData> section_restrict_data,
         SearchSpecProto::EmbeddingQueryMetricType::Code metric_type,
         double score_low, double score_high,
         EmbeddingQueryResults::EmbeddingQueryScoreMap* score_map,
         const EmbeddingIndex* embedding_index);

  libtextclassifier3::Status Advance() override;

  // The iterator will internally handle the section restriction logic by itself
  // to have better control, so that it is able to filter out embedding hits
  // from unwanted sections to avoid retrieving unnecessary vectors and
  // calculate scores for them.
  bool full_section_restriction_applied() const override { return true; }

  libtextclassifier3::StatusOr<TrimmedNode> TrimRightMostNode() && override {
    return absl_ports::InvalidArgumentError(
        "Query suggestions for the semanticSearch function are not supported");
  }

  CallStats GetCallStats() const override {
    return CallStats(
        /*num_leaf_advance_calls_lite_index_in=*/num_advance_calls_,
        /*num_leaf_advance_calls_main_index_in=*/0,
        /*num_leaf_advance_calls_integer_index_in=*/0,
        /*num_leaf_advance_calls_no_index_in=*/0,
        /*num_blocks_inspected_in=*/0);
  }

  std::string ToString() const override { return "embedding_iterator"; }

  // PopulateMatchedTermsStats is not applicable to embedding search.
  void PopulateMatchedTermsStats(
      std::vector<TermMatchInfo>* matched_terms_stats,
      SectionIdMask filtering_section_mask) const override {}

 private:
  explicit DocHitInfoIteratorEmbedding(
      const PropertyProto::VectorProto* query,
      std::unique_ptr<SectionRestrictData> section_restrict_data,
      SearchSpecProto::EmbeddingQueryMetricType::Code metric_type,
      std::unique_ptr<EmbeddingScorer> embedding_scorer, double score_low,
      double score_high,
      EmbeddingQueryResults::EmbeddingQueryScoreMap* score_map,
      const EmbeddingIndex* embedding_index,
      std::unique_ptr<PostingListEmbeddingHitAccessor> posting_list_accessor)
      : query_(*query),
        section_restrict_data_(std::move(section_restrict_data)),
        metric_type_(metric_type),
        embedding_scorer_(std::move(embedding_scorer)),
        score_low_(score_low),
        score_high_(score_high),
        score_map_(*score_map),
        embedding_index_(*embedding_index),
        posting_list_accessor_(std::move(posting_list_accessor)),
        cached_embedding_hits_idx_(0),
        current_allowed_sections_mask_(kSectionIdMaskAll),
        no_more_hit_(false),
        num_advance_calls_(0) {}

  // Advance to the next embedding hit of the current document. If the current
  // document id is kInvalidDocumentId, the method will advance to the first
  // embedding hit of the next document and update doc_hit_info_.
  //
  // This method also properly updates cached_embedding_hits_,
  // cached_embedding_hits_idx_, current_allowed_sections_mask_, and
  // no_more_hit_ to reflect the current state.
  //
  // Returns:
  //   - a const pointer to the next embedding hit on success.
  //   - nullptr, if there is no more hit for the current document, or no more
  //     hit in general if the current document id is kInvalidDocumentId.
  //   - Any error from posting lists.
  libtextclassifier3::StatusOr<const EmbeddingHit*> AdvanceToNextEmbeddingHit();

  // Query information
  const PropertyProto::VectorProto& query_;                     // Does not own
  std::unique_ptr<SectionRestrictData> section_restrict_data_;  // Nullable.

  // Scoring arguments
  SearchSpecProto::EmbeddingQueryMetricType::Code metric_type_;
  std::unique_ptr<EmbeddingScorer> embedding_scorer_;
  double score_low_;
  double score_high_;

  // Score map
  EmbeddingQueryResults::EmbeddingQueryScoreMap& score_map_;  // Does not own

  // Access to embeddings index data
  const EmbeddingIndex& embedding_index_;
  std::unique_ptr<PostingListEmbeddingHitAccessor> posting_list_accessor_;

  // Cached data from the embeddings index
  std::vector<EmbeddingHit> cached_embedding_hits_;
  int cached_embedding_hits_idx_;
  SectionIdMask current_allowed_sections_mask_;
  bool no_more_hit_;

  int num_advance_calls_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_EMBED_DOC_HIT_INFO_ITERATOR_EMBEDDING_H_
