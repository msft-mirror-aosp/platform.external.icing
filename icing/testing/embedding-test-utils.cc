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

#include "icing/testing/embedding-test-utils.h"

#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/index/embed/embedding-hit.h"
#include "icing/index/embed/embedding-index.h"
#include "icing/index/embed/embedding-query-results.h"
#include "icing/index/embed/quantizer.h"
#include "icing/proto/document.pb.h"
#include "icing/store/document-id.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<std::vector<EmbeddingHit>>
GetEmbeddingHitsFromIndex(const EmbeddingIndex* embedding_index,
                          uint32_t dimension, std::string_view model_signature,
                          const std::vector<uint32_t>& cluster_ids) {
  std::vector<EmbeddingHit> hits;

  libtextclassifier3::StatusOr<
      std::unique_ptr<EmbeddingIndex::EmbeddingHitAccessor>>
      embedding_hit_accessor_or =
          embedding_index->GetAccessor(dimension, model_signature, cluster_ids);
  if (absl_ports::IsNotFound(embedding_hit_accessor_or.status())) {
    return hits;
  }
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<EmbeddingIndex::EmbeddingHitAccessor>
                             embedding_hit_accessor,
                         std::move(embedding_hit_accessor_or));

  while (true) {
    ICING_ASSIGN_OR_RETURN(auto batch,
                           embedding_hit_accessor->GetNextHitsBatch());
    if (batch.empty()) {
      return hits;
    }
    for (const auto& hit_info : batch) {
      hits.push_back(hit_info.hit);
    }
  }
}

std::vector<float> GetRawEmbeddingDataFromIndex(
    const EmbeddingIndex* embedding_index, uint32_t shard_id) {
  ICING_ASSIGN_OR_RETURN(const float* data,
                         embedding_index->GetRawEmbeddingData(shard_id),
                         std::vector<float>());
  return std::vector<float>(
      data, data + embedding_index->GetTotalVectorSize(shard_id));
}

libtextclassifier3::StatusOr<std::vector<float>>
GetAndRestoreQuantizedEmbeddingVectorFromIndex(
    const EmbeddingIndex* embedding_index, const EmbeddingHit& hit,
    uint32_t dimension, std::string_view model_signature,
    std::string_view schema_name, uint32_t cluster_id) {
  uint32_t shard_id = GetShardId(embedding_index, dimension, model_signature,
                                 schema_name, cluster_id);
  ICING_ASSIGN_OR_RETURN(
      const char* data,
      embedding_index->GetQuantizedEmbeddingVector(hit, dimension, shard_id));
  Quantizer quantizer(data);
  const uint8_t* quantized_vector =
      reinterpret_cast<const uint8_t*>(data + sizeof(Quantizer));
  std::vector<float> result;
  result.reserve(dimension);
  for (int i = 0; i < dimension; ++i) {
    result.push_back(quantizer.Dequantize(quantized_vector[i]));
  }
  return result;
}

EmbeddingMatchInfos& GetOrCreateEmbeddingMatchInfosForDocument(
    EmbeddingQueryResults& embedding_query_results, int query_vector_index,
    SearchSpecProto::EmbeddingQueryMetricType::Code metric_type,
    DocumentId doc_id) {
  EmbeddingQueryResults::EmbeddingQueryMatchInfoMap* info_map =
      embedding_query_results
          .GetOrCreateMatchInfoMap(query_vector_index, metric_type)
          .ValueOrDie();
  return (*info_map)[doc_id];
}

}  // namespace lib
}  // namespace icing
