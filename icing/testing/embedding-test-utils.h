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

#ifndef ICING_TESTING_EMBEDDING_TEST_UTILS_H_
#define ICING_TESTING_EMBEDDING_TEST_UTILS_H_

#include <cstdint>
#include <initializer_list>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/index/embed/embedding-hit.h"
#include "icing/index/embed/embedding-index.h"
#include "icing/index/embed/embedding-query-results.h"
#include "icing/proto/document.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-id.h"
#include "icing/util/embedding-util.h"

namespace icing {
namespace lib {

inline PropertyProto::VectorProto CreateVector(
    const std::string& model_signature, std::initializer_list<float> values) {
  PropertyProto::VectorProto vector;
  vector.set_model_signature(model_signature);
  for (float value : values) {
    vector.add_values(value);
  }
  return vector;
}

template <typename... V>
inline PropertyProto::VectorProto CreateVector(
    const std::string& model_signature, V&&... values) {
  return CreateVector(model_signature, values...);
}

template <typename RandomEngine>
inline PropertyProto::VectorProto GetRandomVector(
    RandomEngine& random, std::string_view model_signature,
    uint32_t dimension) {
  PropertyProto::VectorProto vector;
  vector.set_model_signature(std::string(model_signature));
  std::uniform_real_distribution<float> value_dist(-10.0, 10.0);
  for (uint32_t i = 0; i < dimension; ++i) {
    vector.add_values(value_dist(random));
  }
  return vector;
}

libtextclassifier3::StatusOr<std::vector<EmbeddingHit>>
GetEmbeddingHitsFromIndex(const EmbeddingIndex* embedding_index,
                          uint32_t dimension, std::string_view model_signature,
                          const std::vector<uint32_t>& cluster_ids = {
                              embedding_util::kLinearSearchClusterId});

std::vector<float> GetRawEmbeddingDataFromIndex(
    const EmbeddingIndex* embedding_index, uint32_t shard_id);

// Get the shard id according to the given information.
// If cluster_id is kLinearSearchClusterId, this is the shard id for the base
// linear search index. Otherwise, this is the shard id according to the
// given IVF cluster.
inline uint32_t GetShardId(
    const EmbeddingIndex* embedding_index, uint32_t dimension,
    std::string_view model_signature, std::string_view schema_name,
    uint32_t cluster_id = embedding_util::kLinearSearchClusterId) {
  std::string key;
  if (cluster_id != embedding_util::kLinearSearchClusterId) {
    key = EmbeddingIndex::IvfContextManager(dimension, model_signature)
              .GetPostingListKey(cluster_id);
  } else {
    key = embedding_util::GetPostingListKey(dimension, model_signature);
  }
  return embedding_index->GetShardId(
      embedding_util::GetPostingListKeyHash(key),
      SchemaStore::GetSchemaNameHash(schema_name));
}

// Gets the quantized embedding vector from the index based on the given hit,
// and returns the dequantized version of the vector.
libtextclassifier3::StatusOr<std::vector<float>>
GetAndRestoreQuantizedEmbeddingVectorFromIndex(
    const EmbeddingIndex* embedding_index, const EmbeddingHit& hit,
    uint32_t dimension, std::string_view model_signature,
    std::string_view schema_name,
    uint32_t cluster_id = embedding_util::kLinearSearchClusterId);

// Gets or creates the EmbeddingMatchInfos in embedding_query_results for the
// given query_vector_index, metric_type, and document.
EmbeddingMatchInfos& GetOrCreateEmbeddingMatchInfosForDocument(
    EmbeddingQueryResults& embedding_query_results, int query_vector_index,
    SearchSpecProto::EmbeddingQueryMetricType::Code metric_type,
    DocumentId doc_id);

}  // namespace lib
}  // namespace icing

#endif  // ICING_TESTING_EMBEDDING_TEST_UTILS_H_
