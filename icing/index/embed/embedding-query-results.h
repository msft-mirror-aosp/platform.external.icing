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

#include <cstdint>
#include <memory>
#include <optional>
#include <unordered_map>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/legacy/core/icing-packed-pod.h"
#include "icing/proto/search.pb.h"
#include "icing/schema/section.h"
#include "icing/scoring/advanced_scoring/double-list.h"
#include "icing/store/document-id.h"
#include "icing/util/embedding-util.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

// Stores the matched embedding infos for a single document.
struct EmbeddingMatchInfos {
  // [score_start_index, score_end_index) is the range of score indexes in the
  // global_scores vector. If embedding section info is enabled, the same range
  // will be used for section infos in global_section_infos as well.
  int32_t score_start_index = 0;
  int32_t score_end_index = 0;

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

  EmbeddingMatchInfos() = default;
  EmbeddingMatchInfos(const EmbeddingMatchInfos& other) = delete;
  EmbeddingMatchInfos& operator=(const EmbeddingMatchInfos& other) = delete;

  // Appends a score to the scores vector, which is stored in the global_scores
  // vector. score_start_index and score_end_index will be updated accordingly.
  //
  // Returns:
  //   - OK, if the score is appended successfully.
  //   - FailedPreconditionError, if the score is not contiguous with the
  //     existing scores.
  libtextclassifier3::Status AppendScore(std::vector<double>& global_scores,
                                         double score) {
    if (score_end_index == 0) {
      score_start_index = score_end_index = global_scores.size();
    }
    if (score_end_index != global_scores.size()) {
      return absl_ports::FailedPreconditionError(
          "Scores for the same document should be contiguous.");
    }
    global_scores.push_back(score);
    score_end_index += 1;
    return libtextclassifier3::Status::OK;
  }

  // Appends a section info to the section info vector, which is stored in the
  // global_section_infos vector.
  //
  // Returns:
  //   - OK, if the section info is appended successfully.
  //   - FailedPreconditionError, if the section info is not appended
  //     immediately after AppendScore.
  libtextclassifier3::Status AppendSectionInfo(
      std::vector<EmbeddingMatchInfos::EmbeddingMatchSectionInfo>&
          global_section_infos,
      SectionId section_id, int position) {
    if (score_end_index != global_section_infos.size() + 1) {
      return absl_ports::FailedPreconditionError(
          "Section infos must be appended immediately after AppendScore.");
    }
    global_section_infos.push_back(
        {.position = position, .section_id = section_id});
    return libtextclassifier3::Status::OK;
  }
};

// A class to store results generated from embedding queries.
class EmbeddingQueryResults {
 public:
  // Creates an empty EmbeddingQueryResults instance.
  EmbeddingQueryResults() : EmbeddingQueryResults(/*num_query_vectors=*/0) {}

  // Creates an EmbeddingQueryResults instance with the given number of query
  // vectors.
  EmbeddingQueryResults(int num_query_vectors)
      : result_infos_size_(embedding_util::kEmbeddingQueryMetricTypes.size() *
                           num_query_vectors),
        result_infos_(
            std::make_unique<std::optional<EmbeddingQueryMatchInfoMap>[]>(
                result_infos_size_)) {}

  int GetNumQueryVectors() const {
    return result_infos_size_ /
           embedding_util::kEmbeddingQueryMetricTypes.size();
  }

  // Maps from DocumentId to matched embedding infos for that document.
  // For each document, its embedding match info consists of two vectors:
  // - The scores vector, which will be used in the advanced scoring language
  //   to determine the results for the "this.matchedSemanticScores(...)"
  //   function.
  // - The section infos vector, which will be used to retrieve snippeting
  //   MatchInfo for the embedding query.
  using EmbeddingQueryMatchInfoMap =
      std::unordered_map<DocumentId, EmbeddingMatchInfos>;

  // A centralized vector of scores for all documents. This is used to store the
  // scores for the "this.matchedSemanticScores(...)" function.
  std::unique_ptr<std::vector<double>> global_scores =
      std::make_unique<std::vector<double>>();

  // A centralized vector of EmbeddingMatchSectionInfo for all documents. This
  // is used to store the section infos for the embedding query.
  std::unique_ptr<std::vector<EmbeddingMatchInfos::EmbeddingMatchSectionInfo>>
      global_section_infos = std::make_unique<
          std::vector<EmbeddingMatchInfos::EmbeddingMatchSectionInfo>>();

  // Get or create the MatchedInfo map for the given query_vector_index and
  // metric_type.
  //
  // Returns:
  //   - The pointer to the EmbeddingQueryMatchInfoMap map, if the map is found
  //     or created.
  //   - InvalidArgumentError, if the index is out of bounds.
  libtextclassifier3::StatusOr<EmbeddingQueryMatchInfoMap*>
  GetOrCreateMatchInfoMap(
      int query_vector_index,
      SearchSpecProto::EmbeddingQueryMetricType::Code metric_type) const {
    ICING_ASSIGN_OR_RETURN(int index,
                           GetResultInfoIndex(query_vector_index, metric_type));
    if (!result_infos_[index].has_value()) {
      result_infos_[index] = EmbeddingQueryMatchInfoMap();
    }
    return &result_infos_[index].value();
  }

  // Get the MatchedInfo map for the given query_vector_index and metric_type.
  // Returns nullptr if (query_vector_index, metric_type) does not exist in the
  // result_scores map.
  const EmbeddingQueryMatchInfoMap* GetMatchInfoMap(
      int query_vector_index,
      SearchSpecProto::EmbeddingQueryMetricType::Code metric_type) const {
    libtextclassifier3::StatusOr<int> index =
        GetResultInfoIndex(query_vector_index, metric_type);
    if (!index.ok() || !result_infos_[index.ValueOrDie()].has_value()) {
      return nullptr;
    }
    return &result_infos_[index.ValueOrDie()].value();
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
  // and doc_id. Returns an empty DoubleList if (query_vector_index,
  // metric_type, doc_id) does not exist in the result_scores map.
  //
  // The returned DoubleList is a non-owning view of the scores vector stored
  // within the EmbeddingQueryResults instance. The caller must ensure the
  // lifetime of the EmbeddingQueryResults exceeds the lifetime of the returned
  // DoubleList.
  DoubleList GetMatchedScoresForDocument(
      int query_vector_index,
      SearchSpecProto::EmbeddingQueryMetricType::Code metric_type,
      DocumentId doc_id) const {
    const EmbeddingMatchInfos* match_infos =
        GetMatchedInfosForDocument(query_vector_index, metric_type, doc_id);
    if (match_infos == nullptr) {
      return DoubleList();
    }
    return GetMatchedScoresFromEmbeddingMatchInfos(*match_infos);
  };

  // Returns the matched scores for the given EmbeddingMatchInfos, which stores
  // the match infos for a single document.
  //
  // The returned DoubleList is a non-owning view of the scores vector stored
  // within the EmbeddingQueryResults instance. The caller must ensure the
  // lifetime of the EmbeddingQueryResults exceeds the lifetime of the returned
  // DoubleList.
  DoubleList GetMatchedScoresFromEmbeddingMatchInfos(
      const EmbeddingMatchInfos& match_infos) const {
    return DoubleList(
        global_scores->data() + match_infos.score_start_index,
        match_infos.score_end_index - match_infos.score_start_index);
  }

 private:
  // Maps from (query_vector_index, metric_type) to EmbeddingQueryMatchInfoMap.
  int result_infos_size_;
  std::unique_ptr<std::optional<EmbeddingQueryMatchInfoMap>[]> result_infos_;

  // Returns the index of the result info for the given query_vector_index and
  // metric_type.
  //
  // Returns:
  //   - The index of the result info, if the index is valid.
  //   - InvalidArgumentError, if the index is out of bounds.
  libtextclassifier3::StatusOr<int> GetResultInfoIndex(
      int query_vector_index,
      SearchSpecProto::EmbeddingQueryMetricType::Code metric_type) const {
    int index =
        query_vector_index * embedding_util::kEmbeddingQueryMetricTypes.size() +
        (metric_type - embedding_util::kEmbeddingQueryMetricTypes[0]);
    if (result_infos_ == nullptr || index < 0 || index >= result_infos_size_) {
      return absl_ports::InvalidArgumentError(
          "result_infos_ index out of bounds.");
    }
    return index;
  }
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_EMBED_EMBEDDING_QUERY_RESULTS_H_
