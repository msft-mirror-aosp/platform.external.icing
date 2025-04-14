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

#include "icing/index/embed/embedding-scorer.h"

#include <cmath>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/index/embed/quantizer.h"
#include "icing/proto/search.pb.h"

namespace icing {
namespace lib {

namespace {

template <typename T>
inline std::enable_if_t<std::is_same<T, float>::value, float> ToFloat(T value) {
  return value;
}

template <typename T>
inline std::enable_if_t<std::is_same<T, float>::value, float> ToFloat(
    T value, const Quantizer&) {
  return value;
}

template <typename T>
inline std::enable_if_t<std::is_same<T, uint8_t>::value, float> ToFloat(
    T quantized, const Quantizer& quantizer) {
  return quantizer.Dequantize(quantized);
}

template <typename T1, typename T2, typename... Args>
float CalculateDotProduct(int dimension, const T1* v1, const T2* v2,
                          const Args&... args) {
  float dot_product = 0.0;
  for (int i = 0; i < dimension; ++i) {
    dot_product += ToFloat(v1[i], args...) * ToFloat(v2[i], args...);
  }
  return dot_product;
}

template <typename T, typename... Args>
float CalculateNorm2(int dimension, const T* v, const Args&... args) {
  return std::sqrt(CalculateDotProduct(dimension, v, v, args...));
}

template <typename T1, typename T2, typename... Args>
float CalculateCosine(int dimension, const T1* v1, const T2* v2,
                      const Args&... args) {
  float divisor = CalculateNorm2(dimension, v1, args...) *
                  CalculateNorm2(dimension, v2, args...);
  if (divisor == 0.0) {
    return 0.0;
  }
  return CalculateDotProduct(dimension, v1, v2, args...) / divisor;
}

template <typename T1, typename T2, typename... Args>
float CalculateEuclideanDistance(int dimension, const T1* v1, const T2* v2,
                                 const Args&... args) {
  float result = 0.0;
  for (int i = 0; i < dimension; ++i) {
    float diff = ToFloat(v1[i], args...) - ToFloat(v2[i], args...);
    result += diff * diff;
  }
  return std::sqrt(result);
}

}  // namespace

libtextclassifier3::StatusOr<std::unique_ptr<EmbeddingScorer>>
EmbeddingScorer::Create(
    SearchSpecProto::EmbeddingQueryMetricType::Code metric_type) {
  switch (metric_type) {
    case SearchSpecProto::EmbeddingQueryMetricType::COSINE:
      return std::make_unique<CosineEmbeddingScorer>();
    case SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT:
      return std::make_unique<DotProductEmbeddingScorer>();
    case SearchSpecProto::EmbeddingQueryMetricType::EUCLIDEAN:
      return std::make_unique<EuclideanDistanceEmbeddingScorer>();
    default:
      return absl_ports::InvalidArgumentError(absl_ports::StrCat(
          "Invalid EmbeddingQueryMetricType: ", std::to_string(metric_type)));
  }
}

float CosineEmbeddingScorer::Score(int dimension, const float* v1,
                                   const float* v2) const {
  return CalculateCosine(dimension, v1, v2);
}

float DotProductEmbeddingScorer::Score(int dimension, const float* v1,
                                       const float* v2) const {
  return CalculateDotProduct(dimension, v1, v2);
}

float EuclideanDistanceEmbeddingScorer::Score(int dimension, const float* v1,
                                              const float* v2) const {
  return CalculateEuclideanDistance(dimension, v1, v2);
}

float CosineEmbeddingScorer::Score(int dimension, const float* v1,
                                   const uint8_t* v2,
                                   const Quantizer& quantizer) const {
  return CalculateCosine(dimension, v1, v2, quantizer);
}

float DotProductEmbeddingScorer::Score(int dimension, const float* v1,
                                       const uint8_t* v2,
                                       const Quantizer& quantizer) const {
  return CalculateDotProduct(dimension, v1, v2, quantizer);
}

float EuclideanDistanceEmbeddingScorer::Score(
    int dimension, const float* v1, const uint8_t* v2,
    const Quantizer& quantizer) const {
  return CalculateEuclideanDistance(dimension, v1, v2, quantizer);
}

}  // namespace lib
}  // namespace icing
