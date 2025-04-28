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

#include <cstdint>
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
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-store.h"

namespace icing {
namespace lib {

class DocHitInfoIteratorEmbedding
    : public DocHitInfoIteratorHandlingSectionRestrict {
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
  //   - a DocHitInfoIteratorEmbedding instance on success.
  //   - Any error from posting lists.
  static libtextclassifier3::StatusOr<
      std::unique_ptr<DocHitInfoIteratorEmbedding>>
  Create(const PropertyProto::VectorProto* query,
         SearchSpecProto::EmbeddingQueryMetricType::Code metric_type,
         double score_low, double score_high, bool get_embedding_match_info,
         EmbeddingQueryResults::EmbeddingQueryMatchInfoMap* info_map,
         const EmbeddingIndex* embedding_index,
         const DocumentStore* document_store, const SchemaStore* schema_store,
         int64_t current_time_ms);

  libtextclassifier3::Status Advance() override;

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
      SearchSpecProto::EmbeddingQueryMetricType::Code metric_type,
      std::unique_ptr<EmbeddingScorer> embedding_scorer, double score_low,
      double score_high, bool get_embedding_match_info,
      EmbeddingQueryResults::EmbeddingQueryMatchInfoMap* info_map,
      const EmbeddingIndex* embedding_index,
      std::unique_ptr<PostingListEmbeddingHitAccessor> posting_list_accessor,
      const DocumentStore* document_store, const SchemaStore* schema_store,
      int64_t current_time_ms)
      : query_(*query),
        metric_type_(metric_type),
        embedding_scorer_(std::move(embedding_scorer)),
        score_low_(score_low),
        score_high_(score_high),
        get_embedding_match_info_(get_embedding_match_info),
        info_map_(*info_map),
        embedding_index_(*embedding_index),
        posting_list_accessor_(std::move(posting_list_accessor)),
        cached_embedding_hits_idx_(0),
        current_allowed_sections_mask_(kSectionIdMaskAll),
        no_more_hit_(false),
        schema_type_id_(kInvalidSchemaTypeId),
        document_store_(*document_store),
        schema_store_(*schema_store),
        current_time_ms_(current_time_ms),
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

  // Similar to Advance(), this method advances the iterator to the next
  // document, but it does not guarantee that the next document will have
  // a matched embedding hit within the score range.
  //
  // Returns:
  //   - OK, if it is able to advance to a new document_id.
  //   - RESOUCE_EXHAUSTED, if we have run out of document_ids to iterate over.
  //   - Any error from posting lists.
  libtextclassifier3::Status AdvanceToNextUnfilteredDocument();

  // Query information
  const PropertyProto::VectorProto& query_;  // Does not own

  // Scoring arguments
  SearchSpecProto::EmbeddingQueryMetricType::Code metric_type_;
  std::unique_ptr<EmbeddingScorer> embedding_scorer_;
  double score_low_;
  double score_high_;

  // Snippet arguments
  bool get_embedding_match_info_;

  // MatchInfo map
  EmbeddingQueryResults::EmbeddingQueryMatchInfoMap& info_map_;  // Does not own

  // Access to embeddings index data
  const EmbeddingIndex& embedding_index_;
  std::unique_ptr<PostingListEmbeddingHitAccessor> posting_list_accessor_;

  // Cached data from the embeddings index
  std::vector<EmbeddingHit> cached_embedding_hits_;
  int cached_embedding_hits_idx_;
  SectionIdMask current_allowed_sections_mask_;
  bool no_more_hit_;
  SchemaTypeId schema_type_id_;  // The schema type id for the current document.

  const DocumentStore& document_store_;
  const SchemaStore& schema_store_;
  int64_t current_time_ms_;
  int num_advance_calls_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_EMBED_DOC_HIT_INFO_ITERATOR_EMBEDDING_H_
