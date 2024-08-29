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

#ifndef ICING_INDEX_EMBED_EMBEDDING_QUERY_RESULTS_H_
#define ICING_INDEX_EMBED_EMBEDDING_QUERY_RESULTS_H_

#include <unordered_map>
#include <vector>

#include "icing/proto/search.pb.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

// A class to store results generated from embedding queries.
struct EmbeddingQueryResults {
  // Maps from DocumentId to the list of matched embedding scores for that
  // document, which will be used in the advanced scoring language to
  // determine the results for the "this.matchedSemanticScores(...)" function.
  using EmbeddingQueryScoreMap =
      std::unordered_map<DocumentId, std::vector<double>>;

  // Maps from (query_vector_index, metric_type) to EmbeddingQueryScoreMap.
  std::unordered_map<
      int, std::unordered_map<SearchSpecProto::EmbeddingQueryMetricType::Code,
                              EmbeddingQueryScoreMap>>
      result_scores;

  // Get the score map for the given query_vector_index and metric_type. Returns
  // nullptr if (query_vector_index, metric_type) does not exist in the
  // result_scores map.
  const EmbeddingQueryScoreMap* GetScoreMap(
      int query_vector_index,
      SearchSpecProto::EmbeddingQueryMetricType::Code metric_type) const {
    // Check if a mapping exists for the query_vector_index
    auto outer_it = result_scores.find(query_vector_index);
    if (outer_it == result_scores.end()) {
      return nullptr;
    }
    // Check if a mapping exists for the metric_type
    auto inner_it = outer_it->second.find(metric_type);
    if (inner_it == outer_it->second.end()) {
      return nullptr;
    }
    return &inner_it->second;
  }

  // Returns the matched scores for the given query_vector_index, metric_type,
  // and doc_id. Returns nullptr if (query_vector_index, metric_type, doc_id)
  // does not exist in the result_scores map.
  const std::vector<double>* GetMatchedScoresForDocument(
      int query_vector_index,
      SearchSpecProto::EmbeddingQueryMetricType::Code metric_type,
      DocumentId doc_id) const {
    const EmbeddingQueryScoreMap* score_map =
        GetScoreMap(query_vector_index, metric_type);
    if (score_map == nullptr) {
      return nullptr;
    }
    // Check if the doc_id exists in the score_map
    auto scores_it = score_map->find(doc_id);
    if (scores_it == score_map->end()) {
      return nullptr;
    }
    return &scores_it->second;
  }
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_EMBED_EMBEDDING_QUERY_RESULTS_H_
