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

#include <memory>
#include <unordered_map>
#include <vector>

#include "icing/legacy/core/icing-packed-pod.h"
#include "icing/proto/search.pb.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

struct EmbeddingMatchInfos {
  // A vector of semantic scores of matched embeddings.
  std::vector<double> scores;

  struct EmbeddingMatchSectionInfo {
    // The position of the matched embedding vector in a section relative to
    // other vectors with the same (dimension, signature) combination. Note that
    // this is not the universal position of the vector in the section.
    //
    // E.g. If a repeated vector property contains the following vectors:
    // - vector1: [1, 2, 3] (signature = "signature1", dimension = 3)
    // - vector2: [7, 8, 9] (signature = "signature1", dimension = 3)
    // - vector3: [4, 5, 6, 8] (signature = "signature2", dimension = 4)
    // - vector4: [10, 11, 12] (signature = "signature1", dimension = 3)
    //
    // Then the position values for each vector would be:
    // - vector1: 0
    // - vector2: 1
    // - vector3: 0
    // - vector4: 2
    int position;

    // The section id of an embedding vector.
    SectionId section_id;
  } __attribute__((packed));
  static_assert(sizeof(EmbeddingMatchSectionInfo) == 5, "");
  static_assert(icing_is_packed_pod<EmbeddingMatchSectionInfo>::value,
                "go/icing-ubsan");

  // A vector of section infos on the matched embeddings. This will be nullptr
  // if embedding match info is not enabled for this query.
  //
  // When non-null, section_infos must have a 1:1 mapping with the scores
  // vector.
  std::unique_ptr<std::vector<EmbeddingMatchSectionInfo>> section_infos;

  EmbeddingMatchInfos() = default;

  EmbeddingMatchInfos(const EmbeddingMatchInfos& other) = delete;
  EmbeddingMatchInfos& operator=(const EmbeddingMatchInfos& other) = delete;

  // Appends a score to the scores vector.
  void AppendScore(double score) { scores.push_back(score); }

  // Appends a section info to the section_infos vector, allocating if needed.
  void AppendSectionInfo(SectionId section_id, int position) {
    if (!section_infos) {
      section_infos =
          std::make_unique<std::vector<EmbeddingMatchSectionInfo>>();
    }
    section_infos->push_back({.position = position, .section_id = section_id});
  }
};

// A class to store results generated from embedding queries.
struct EmbeddingQueryResults {
  // Maps from DocumentId to matched embedding infos for that document.
  // For each document, its embedding match info consists of two vectors:
  // - The scores vector, which will be used in the advanced scoring language
  //   to determine the results for the "this.matchedSemanticScores(...)"
  //   function.
  // - The section infos vector, which will be used to retrieve snippeting
  //   MatchInfo for the embedding query.
  using EmbeddingQueryMatchInfoMap =
      std::unordered_map<DocumentId, EmbeddingMatchInfos>;

  // Maps from (query_vector_index, metric_type) to EmbeddingQueryMatchInfoMap.
  std::unordered_map<
      int, std::unordered_map<SearchSpecProto::EmbeddingQueryMetricType::Code,
                              EmbeddingQueryMatchInfoMap>>
      result_infos;

  // Get the MatchedInfo map for the given query_vector_index and metric_type.
  // Returns nullptr if (query_vector_index, metric_type) does not exist in the
  // result_scores map.
  const EmbeddingQueryMatchInfoMap* GetMatchInfoMap(
      int query_vector_index,
      SearchSpecProto::EmbeddingQueryMetricType::Code metric_type) const {
    // Check if a mapping exists for the query_vector_index
    auto outer_it = result_infos.find(query_vector_index);
    if (outer_it == result_infos.end()) {
      return nullptr;
    }
    // Check if a mapping exists for the metric_type
    auto inner_it = outer_it->second.find(metric_type);
    if (inner_it == outer_it->second.end()) {
      return nullptr;
    }
    return &inner_it->second;
  }

  // Returns the matched infos for the given query_vector_index, metric_type,
  // and doc_id. Returns nullptr if (query_vector_index, metric_type, doc_id)
  // does not exist in the result_scores map.
  const EmbeddingMatchInfos* GetMatchedInfosForDocument(
      int query_vector_index,
      SearchSpecProto::EmbeddingQueryMetricType::Code metric_type,
      DocumentId doc_id) const {
    const EmbeddingQueryMatchInfoMap* info_map =
        GetMatchInfoMap(query_vector_index, metric_type);
    if (info_map == nullptr) {
      return nullptr;
    }
    // Check if the doc_id exists in the info_map
    auto info_it = info_map->find(doc_id);
    if (info_it == info_map->end()) {
      return nullptr;
    }
    return &info_it->second;
  }

  // Returns the matched scores for the given query_vector_index, metric_type,
  // and doc_id. Returns nullptr if (query_vector_index, metric_type, doc_id)
  // does not exist in the result_scores map.
  const std::vector<double>* GetMatchedScoresForDocument(
      int query_vector_index,
      SearchSpecProto::EmbeddingQueryMetricType::Code metric_type,
      DocumentId doc_id) const {
    const EmbeddingMatchInfos* match_infos =
        GetMatchedInfosForDocument(query_vector_index, metric_type, doc_id);
    if (match_infos == nullptr) {
      return nullptr;
    }
    return &match_infos->scores;
  };
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_EMBED_EMBEDDING_QUERY_RESULTS_H_
