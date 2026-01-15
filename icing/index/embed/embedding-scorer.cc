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

#ifndef ICING_DISABLE_EIGEN
#include <Eigen/Core>
#include <Eigen/Dense>
#endif  // ICING_DISABLE_EIGEN

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

#ifndef ICING_DISABLE_EIGEN
// Returns a lazily-evaluated (due to the return type "auto") expression that
// dequantizes the quantized vector.
//
// It requires that the scale factor is not 0. If it is 0, the caller should
// handle the case specifically.
inline auto DequantizeEigenVector(
    const Quantizer& quantizer,
    Eigen::Ref<const Eigen::VectorX<uint8_t>> quantized_vec) {
  return (quantized_vec.cast<float>().array() / quantizer.scale_factor() +
          quantizer.float_min())
      .matrix();
}

float EigenCosine(Eigen::Ref<const Eigen::VectorXf> v1,
                  Eigen::Ref<const Eigen::VectorXf> v2) {
  float divisor = v1.norm() * v2.norm();
  if (divisor == 0.0) {
    return 0.0;
  }
  return v1.dot(v2) / divisor;
}
#endif  // ICING_DISABLE_EIGEN

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

#ifndef ICING_DISABLE_EIGEN
float CosineEmbeddingScorer::EigenScore(int dimension, const float* v1,
                                        const float* v2) const {
  Eigen::Map<const Eigen::VectorXf> vec1(v1, dimension);
  Eigen::Map<const Eigen::VectorXf> vec2(v2, dimension);
  return EigenCosine(vec1, vec2);
}

float DotProductEmbeddingScorer::EigenScore(int dimension, const float* v1,
                                            const float* v2) const {
  Eigen::Map<const Eigen::VectorXf> vec1(v1, dimension);
  Eigen::Map<const Eigen::VectorXf> vec2(v2, dimension);
  return vec1.dot(vec2);
}

float EuclideanDistanceEmbeddingScorer::EigenScore(int dimension,
                                                   const float* v1,
                                                   const float* v2) const {
  Eigen::Map<const Eigen::VectorXf> vec1(v1, dimension);
  Eigen::Map<const Eigen::VectorXf> vec2(v2, dimension);
  return (vec1 - vec2).norm();
}

float CosineEmbeddingScorer::EigenScore(int dimension, const float* v1,
                                        const uint8_t* v2,
                                        const Quantizer& quantizer) const {
  Eigen::Map<const Eigen::VectorXf> vec1(v1, dimension);
  Eigen::Map<const Eigen::Matrix<uint8_t, Eigen::Dynamic, 1>> vec2(v2,
                                                                   dimension);
  if (quantizer.scale_factor() == 0.0) {
    return EigenCosine(
        vec1, Eigen::VectorXf::Constant(dimension, quantizer.float_min()));
  }
  return EigenCosine(vec1, DequantizeEigenVector(quantizer, vec2));
}

float DotProductEmbeddingScorer::EigenScore(int dimension, const float* v1,
                                            const uint8_t* v2,
                                            const Quantizer& quantizer) const {
  Eigen::Map<const Eigen::VectorXf> vec1(v1, dimension);
  Eigen::Map<const Eigen::Matrix<uint8_t, Eigen::Dynamic, 1>> vec2(v2,
                                                                   dimension);
  if (quantizer.scale_factor() == 0.0) {
    return vec1.dot(
        Eigen::VectorXf::Constant(dimension, quantizer.float_min()));
  }
  return vec1.dot(DequantizeEigenVector(quantizer, vec2));
}

float EuclideanDistanceEmbeddingScorer::EigenScore(
    int dimension, const float* v1, const uint8_t* v2,
    const Quantizer& quantizer) const {
  Eigen::Map<const Eigen::VectorXf> vec1(v1, dimension);
  Eigen::Map<const Eigen::Matrix<uint8_t, Eigen::Dynamic, 1>> vec2(v2,
                                                                   dimension);
  if (quantizer.scale_factor() == 0.0) {
    return (vec1.array() - quantizer.float_min()).matrix().norm();
  }
  return (vec1 - DequantizeEigenVector(quantizer, vec2)).norm();
}
#else   // ICING_DISABLE_EIGEN
// If Eigen is disabled, just fall back to the regular Score() function.

float CosineEmbeddingScorer::EigenScore(int dimension, const float* v1,
                                        const float* v2) const {
  return Score(dimension, v1, v2);
}

float DotProductEmbeddingScorer::EigenScore(int dimension, const float* v1,
                                            const float* v2) const {
  return Score(dimension, v1, v2);
}

float EuclideanDistanceEmbeddingScorer::EigenScore(int dimension,
                                                   const float* v1,
                                                   const float* v2) const {
  return Score(dimension, v1, v2);
}

float CosineEmbeddingScorer::EigenScore(int dimension, const float* v1,
                                        const uint8_t* v2,
                                        const Quantizer& quantizer) const {
  return Score(dimension, v1, v2, quantizer);
}

float DotProductEmbeddingScorer::EigenScore(int dimension, const float* v1,
                                            const uint8_t* v2,
                                            const Quantizer& quantizer) const {
  return Score(dimension, v1, v2, quantizer);
}

float EuclideanDistanceEmbeddingScorer::EigenScore(
    int dimension, const float* v1, const uint8_t* v2,
    const Quantizer& quantizer) const {
  return Score(dimension, v1, v2, quantizer);
}
#endif  // ICING_DISABLE_EIGEN

}  // namespace lib
}  // namespace icing
