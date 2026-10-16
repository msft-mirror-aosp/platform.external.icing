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

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/index/embed/quantizer.h"
#include "icing/proto/search.pb.h"

#ifdef ICING_DISABLE_EIGEN
#include <type_traits>
#else
#include <Eigen/Core>
#endif  // ICING_DISABLE_EIGEN

namespace icing {
namespace lib {

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

namespace {

// ---------------------------------------------------------------------------
// 8-bit x 8-bit quantized scoring.
//
// When both operands are 8-bit quantized, the score is reconstructed from
// exact integer sums without dequantizing individual elements.
//
// A quantizer maps a byte v to a float via v/s + m, where s is
// Quantizer::scale_factor() and m is Quantizer::float_min(). Expanding the dot
// product of two such vectors ("FOIL") gives:
//
//   <Q, D> = (1 / (s_q * s_d)) * sum(q[i] * d[i])
//          + (m_d / s_q)       * sum(q[i])
//          + (m_q / s_d)       * sum(d[i])
//          + dimension * m_q * m_d
//
// and, for a single vector,
//
//   ||V||^2 = (1 / s^2)   * sum(v[i]^2)
//           + (2 * m / s) * sum(v[i])
//           + dimension * m^2
//
// Query-dependent terms are precomputed once in EmbeddingQuery, so the
// per-document loop accumulates only document-dependent integer sums.
// ---------------------------------------------------------------------------

// Integer sums produced by the per-document accumulation loop.
struct QuantizedAccumulators {
  uint32_t dot = 0;         // sum(q[i] * d[i])
  uint32_t doc_sum = 0;     // sum(d[i])
  uint32_t doc_sum_sq = 0;  // sum(d[i] * d[i]); populated when kNeedDocSumSq.
};

// Returns 1/scale_factor, or 0 when the quantizer is degenerate.
//
// Quantizer::scale_factor() is 0 when the source vector has zero dynamic range
// (float_max - float_min <= 1e-6), in which case Dequantize() returns
// float_min for every byte. Returning 0 reproduces that behavior in the
// `byte * inv_scale + float_min` reconstruction.
inline double InvScale(const Quantizer& quantizer) {
  return quantizer.scale_factor() == 0.0f
             ? 0.0
             : 1.0 / static_cast<double>(quantizer.scale_factor());
}

// Reconstructs the dequantized dot product from the integer sums.
inline double FoilDotProduct(int dimension, const QuantizedAccumulators& acc,
                             const EmbeddingQuery& query,
                             const Quantizer& doc_quantizer,
                             double inv_doc_scale) {
  const double inv_query_scale = query.inv_scale();
  const double query_min = static_cast<double>(query.quantizer().float_min());
  const double doc_min = static_cast<double>(doc_quantizer.float_min());
  return (static_cast<double>(acc.dot) * inv_query_scale * inv_doc_scale) +
         (static_cast<double>(query.quantized_sum()) * doc_min *
          inv_query_scale) +
         (static_cast<double>(acc.doc_sum) * query_min * inv_doc_scale) +
         (static_cast<double>(dimension) * query_min * doc_min);
}

// Reconstructs the dequantized squared L2 norm of the document from integer
// sums.
inline double FoilDocNormSq(int dimension, const QuantizedAccumulators& acc,
                            const Quantizer& doc_quantizer,
                            double inv_doc_scale) {
  const double doc_min = static_cast<double>(doc_quantizer.float_min());
  return (static_cast<double>(acc.doc_sum_sq) * inv_doc_scale * inv_doc_scale) +
         (2.0 * doc_min * static_cast<double>(acc.doc_sum) * inv_doc_scale) +
         (static_cast<double>(dimension) * doc_min * doc_min);
}

inline float FinishDotProduct(int dimension, const QuantizedAccumulators& acc,
                              const EmbeddingQuery& query,
                              const Quantizer& doc_quantizer) {
  return static_cast<float>(FoilDotProduct(dimension, acc, query, doc_quantizer,
                                           InvScale(doc_quantizer)));
}

inline double NonNegativeOrNan(double x) { return x < 0.0 ? 0.0 : x; }

inline float FinishCosine(int dimension, const QuantizedAccumulators& acc,
                          const EmbeddingQuery& query,
                          const Quantizer& doc_quantizer) {
  const double inv_doc_scale = InvScale(doc_quantizer);
  const double dot_product =
      FoilDotProduct(dimension, acc, query, doc_quantizer, inv_doc_scale);
  const double doc_norm_sq =
      FoilDocNormSq(dimension, acc, doc_quantizer, inv_doc_scale);
  const double divisor =
      query.norm() * std::sqrt(NonNegativeOrNan(doc_norm_sq));
  if (divisor == 0.0) {
    return 0.0f;
  }
  // Clamp to [-1, 1] to guard against floating-point rounding on unit vectors.
  return std::clamp(static_cast<float>(dot_product / divisor), -1.0f, 1.0f);
}

inline float FinishEuclidean(int dimension, const QuantizedAccumulators& acc,
                             const EmbeddingQuery& query,
                             const Quantizer& doc_quantizer) {
  const double inv_doc_scale = InvScale(doc_quantizer);
  const double dot_product =
      FoilDotProduct(dimension, acc, query, doc_quantizer, inv_doc_scale);
  const double doc_norm_sq =
      FoilDocNormSq(dimension, acc, doc_quantizer, inv_doc_scale);
  // ||q - d||^2 = ||q||^2 + ||d||^2 - 2<q, d>
  const double dist_sq = query.norm_sq() + doc_norm_sq - 2.0 * dot_product;
  return static_cast<float>(std::sqrt(NonNegativeOrNan(dist_sq)));
}

// Beyond this dimension, the uint32_t accumulators in AccumulateQuantized()
// can wrap (maximum per-element contribution is 255 * 255 = 65,025).
constexpr size_t kMaxQuantizedScoringDimension =
    std::numeric_limits<uint32_t>::max() / (255u * 255u);  // 66,051

// Accumulates integer dot-product and document sums across `dimension` bytes.
//
// kNeedDocSumSq is a compile-time template parameter rather than a runtime
// branch so the DotProduct instantiation maintains only two live accumulator
// chains. On ARM64, a third live accumulator chain exceeds Clang's
// vectorization register pressure threshold, spilling accumulators to
// general-purpose registers and introducing cross-register-file transfers in
// the vectorized loop body.
//
// The uint32_t accumulators do not overflow for any dimension <=
// kMaxQuantizedScoringDimension (maximum per-element contribution is
// 255 * 255 = 65,025).
template <bool kNeedDocSumSq>
QuantizedAccumulators AccumulateQuantized(
    int dimension, const uint8_t* __restrict__ query_values,
    const uint8_t* __restrict__ doc_values) {
  uint32_t dot0 = 0, dot1 = 0, dot2 = 0, dot3 = 0;
  uint32_t doc_sum0 = 0, doc_sum1 = 0, doc_sum2 = 0, doc_sum3 = 0;
  uint32_t doc_sum_sq0 = 0, doc_sum_sq1 = 0, doc_sum_sq2 = 0, doc_sum_sq3 = 0;

  int i = 0;
  for (; i + 4 <= dimension; i += 4) {
    dot0 += static_cast<uint32_t>(query_values[i]) * doc_values[i];
    dot1 += static_cast<uint32_t>(query_values[i + 1]) * doc_values[i + 1];
    dot2 += static_cast<uint32_t>(query_values[i + 2]) * doc_values[i + 2];
    dot3 += static_cast<uint32_t>(query_values[i + 3]) * doc_values[i + 3];

    doc_sum0 += doc_values[i];
    doc_sum1 += doc_values[i + 1];
    doc_sum2 += doc_values[i + 2];
    doc_sum3 += doc_values[i + 3];

    if (kNeedDocSumSq) {
      doc_sum_sq0 += static_cast<uint32_t>(doc_values[i]) * doc_values[i];
      doc_sum_sq1 +=
          static_cast<uint32_t>(doc_values[i + 1]) * doc_values[i + 1];
      doc_sum_sq2 +=
          static_cast<uint32_t>(doc_values[i + 2]) * doc_values[i + 2];
      doc_sum_sq3 +=
          static_cast<uint32_t>(doc_values[i + 3]) * doc_values[i + 3];
    }
  }

  uint32_t dot = (dot0 + dot1) + (dot2 + dot3);
  uint32_t doc_sum = (doc_sum0 + doc_sum1) + (doc_sum2 + doc_sum3);
  uint32_t doc_sum_sq = 0;
  if (kNeedDocSumSq) {
    doc_sum_sq = (doc_sum_sq0 + doc_sum_sq1) + (doc_sum_sq2 + doc_sum_sq3);
  }

  for (; i < dimension; ++i) {
    dot += static_cast<uint32_t>(query_values[i]) * doc_values[i];
    doc_sum += doc_values[i];
    if (kNeedDocSumSq) {
      doc_sum_sq += static_cast<uint32_t>(doc_values[i]) * doc_values[i];
    }
  }
  return {dot, doc_sum, doc_sum_sq};
}

}  // namespace

libtextclassifier3::StatusOr<EmbeddingQuery>
EmbeddingQuery::CreateFromQuantized(
    std::string_view quantized_values,
    bool enable_quantization_scoring_optimization) {
  if (quantized_values.size() <= sizeof(Quantizer)) {
    return absl_ports::InvalidArgumentError(
        "Quantized query vector is too small to hold a Quantizer header and "
        "at least one dimension.");
  }

  Quantizer quantizer(quantized_values.data());
  const size_t dimension = quantized_values.size() - sizeof(Quantizer);
  const uint8_t* bytes = reinterpret_cast<const uint8_t*>(
      quantized_values.data() + sizeof(Quantizer));

  std::vector<float> floats(dimension);
  for (size_t i = 0; i < dimension; ++i) {
    floats[i] = quantizer.Dequantize(bytes[i]);
  }
  if (!enable_quantization_scoring_optimization ||
      dimension > kMaxQuantizedScoringDimension) {
    return EmbeddingQuery(std::move(floats));
  }

  std::vector<uint8_t> values(bytes, bytes + dimension);
  uint32_t quantized_sum = 0;
  uint32_t sum_sq = 0;
  for (size_t i = 0; i < dimension; ++i) {
    quantized_sum += values[i];
    sum_sq += static_cast<uint32_t>(values[i]) * values[i];
  }

  // Reconstruct the dequantized query squared L2 norm using the same algebraic
  // expansion as FoilDocNormSq so both operands share identical floating-point
  // rounding behavior in Euclidean distance calculations.
  const double inv_scale = InvScale(quantizer);
  const double query_min = static_cast<double>(quantizer.float_min());
  const double norm_sq =
      static_cast<double>(sum_sq) * inv_scale * inv_scale +
      2.0 * query_min * static_cast<double>(quantized_sum) * inv_scale +
      static_cast<double>(dimension) * query_min * query_min;

  return EmbeddingQuery(std::move(floats), std::move(values), quantizer,
                        quantized_sum, std::sqrt(NonNegativeOrNan(norm_sq)),
                        norm_sq, inv_scale);
}

libtextclassifier3::StatusOr<EmbeddingQuery> EmbeddingQuery::CreateFromFloats(
    std::vector<float> floats) {
  if (floats.empty()) {
    return absl_ports::InvalidArgumentError("Query vector is empty");
  }
  return EmbeddingQuery(std::move(floats));
}

float CosineEmbeddingScorer::Score(int dimension, const EmbeddingQuery& v1,
                                   const uint8_t* v2,
                                   const Quantizer& quantizer) const {
  if (!v1.is_quantized()) {
    return Score(dimension, v1.floats().data(), v2, quantizer);
  }
  return FinishCosine(dimension,
                      AccumulateQuantized</*kNeedDocSumSq=*/true>(
                          dimension, v1.quantized_data(), v2),
                      v1, quantizer);
}

float DotProductEmbeddingScorer::Score(int dimension, const EmbeddingQuery& v1,
                                       const uint8_t* v2,
                                       const Quantizer& quantizer) const {
  if (!v1.is_quantized()) {
    return Score(dimension, v1.floats().data(), v2, quantizer);
  }
  return FinishDotProduct(dimension,
                          AccumulateQuantized</*kNeedDocSumSq=*/false>(
                              dimension, v1.quantized_data(), v2),
                          v1, quantizer);
}

float EuclideanDistanceEmbeddingScorer::Score(
    int dimension, const EmbeddingQuery& v1, const uint8_t* v2,
    const Quantizer& quantizer) const {
  if (!v1.is_quantized()) {
    return Score(dimension, v1.floats().data(), v2, quantizer);
  }
  return FinishEuclidean(dimension,
                         AccumulateQuantized</*kNeedDocSumSq=*/true>(
                             dimension, v1.quantized_data(), v2),
                         v1, quantizer);
}

#ifdef ICING_DISABLE_EIGEN

// Scalar implementations, used when Eigen is not available.

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

#else  // ICING_DISABLE_EIGEN

namespace {

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

}  // namespace

float CosineEmbeddingScorer::Score(int dimension, const float* v1,
                                   const float* v2) const {
  Eigen::Map<const Eigen::VectorXf> vec1(v1, dimension);
  Eigen::Map<const Eigen::VectorXf> vec2(v2, dimension);
  return EigenCosine(vec1, vec2);
}

float DotProductEmbeddingScorer::Score(int dimension, const float* v1,
                                       const float* v2) const {
  Eigen::Map<const Eigen::VectorXf> vec1(v1, dimension);
  Eigen::Map<const Eigen::VectorXf> vec2(v2, dimension);
  return vec1.dot(vec2);
}

float EuclideanDistanceEmbeddingScorer::Score(int dimension, const float* v1,
                                              const float* v2) const {
  Eigen::Map<const Eigen::VectorXf> vec1(v1, dimension);
  Eigen::Map<const Eigen::VectorXf> vec2(v2, dimension);
  return (vec1 - vec2).norm();
}

float CosineEmbeddingScorer::Score(int dimension, const float* v1,
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

float DotProductEmbeddingScorer::Score(int dimension, const float* v1,
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

float EuclideanDistanceEmbeddingScorer::Score(
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

#endif  // ICING_DISABLE_EIGEN

}  // namespace lib
}  // namespace icing
