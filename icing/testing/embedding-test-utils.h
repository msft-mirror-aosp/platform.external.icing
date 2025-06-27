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
#include <string>
#include <string_view>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/index/embed/embedding-hit.h"
#include "icing/index/embed/embedding-index.h"
#include "icing/proto/document.pb.h"

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

libtextclassifier3::StatusOr<std::vector<EmbeddingHit>>
GetEmbeddingHitsFromIndex(const EmbeddingIndex* embedding_index,
                          uint32_t dimension, std::string_view model_signature);

std::vector<float> GetRawEmbeddingDataFromIndex(
    const EmbeddingIndex* embedding_index);

// Gets the quantized embedding vector from the index based on the given hit,
// and returns the dequantized version of the vector.
libtextclassifier3::StatusOr<std::vector<float>>
GetAndRestoreQuantizedEmbeddingVectorFromIndex(
    const EmbeddingIndex* embedding_index, const EmbeddingHit& hit,
    uint32_t dimension);

}  // namespace lib
}  // namespace icing

#endif  // ICING_TESTING_EMBEDDING_TEST_UTILS_H_
