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
#include <memory>
#include <string>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/proto/search.pb.h"

namespace icing {
namespace lib {

namespace {

float CalculateDotProduct(int dimension, const float* v1, const float* v2) {
  float dot_product = 0.0;
  for (int i = 0; i < dimension; ++i) {
    dot_product += v1[i] * v2[i];
  }
  return dot_product;
}

float CalculateNorm2(int dimension, const float* v) {
  return std::sqrt(CalculateDotProduct(dimension, v, v));
}

float CalculateCosine(int dimension, const float* v1, const float* v2) {
  float divisor = CalculateNorm2(dimension, v1) * CalculateNorm2(dimension, v2);
  if (divisor == 0.0) {
    return 0.0;
  }
  return CalculateDotProduct(dimension, v1, v2) / divisor;
}

float CalculateEuclideanDistance(int dimension, const float* v1,
                                 const float* v2) {
  float result = 0.0;
  for (int i = 0; i < dimension; ++i) {
    float diff = v1[i] - v2[i];
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

}  // namespace lib
}  // namespace icing
