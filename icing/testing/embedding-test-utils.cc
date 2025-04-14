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
#include "icing/index/embed/posting-list-embedding-hit-accessor.h"
#include "icing/index/embed/quantizer.h"
#include "icing/proto/document.pb.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<std::vector<EmbeddingHit>>
GetEmbeddingHitsFromIndex(const EmbeddingIndex* embedding_index,
                          uint32_t dimension,
                          std::string_view model_signature) {
  std::vector<EmbeddingHit> hits;

  libtextclassifier3::StatusOr<std::unique_ptr<PostingListEmbeddingHitAccessor>>
      pl_accessor_or = embedding_index->GetAccessor(dimension, model_signature);
  if (absl_ports::IsNotFound(pl_accessor_or.status())) {
    return hits;
  }
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor,
      std::move(pl_accessor_or));

  while (true) {
    ICING_ASSIGN_OR_RETURN(std::vector<EmbeddingHit> batch,
                           pl_accessor->GetNextHitsBatch());
    if (batch.empty()) {
      return hits;
    }
    hits.insert(hits.end(), batch.begin(), batch.end());
  }
}

std::vector<float> GetRawEmbeddingDataFromIndex(
    const EmbeddingIndex* embedding_index) {
  ICING_ASSIGN_OR_RETURN(const float* data,
                         embedding_index->GetRawEmbeddingData(),
                         std::vector<float>());
  return std::vector<float>(data, data + embedding_index->GetTotalVectorSize());
}

libtextclassifier3::StatusOr<std::vector<float>>
GetAndRestoreQuantizedEmbeddingVectorFromIndex(
    const EmbeddingIndex* embedding_index, const EmbeddingHit& hit,
    uint32_t dimension) {
  ICING_ASSIGN_OR_RETURN(
      const char* data,
      embedding_index->GetQuantizedEmbeddingVector(hit, dimension));
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

}  // namespace lib
}  // namespace icing
