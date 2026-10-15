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

#ifndef ICING_INDEX_EMBED_EMBEDDING_SCORER_H_
#define ICING_INDEX_EMBED_EMBEDDING_SCORER_H_

#include <cstdint>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/index/embed/quantizer.h"
#include "icing/proto/search.pb.h"

namespace icing {
namespace lib {

// Holds an embedding query vector and precomputed query-side statistics
// (byte sum, L2 norm, squared L2 norm, and inverse scale) that remain
// invariant across candidate documents during scoring.
//
// Always retains the float32 representation of the query (`floats()`,
// dequantized once on construction when the caller supplies a quantized query)
// for scoring against unquantized sections or when quantized scoring
// optimization is disabled. When constructed from a quantized query with
// `enable_quantization_scoring_optimization` enabled, also retains the raw
// quantized bytes and precomputed statistics for direct 8-bit x 8-bit scoring.
class EmbeddingQuery {
 public:
  // Creates an EmbeddingQuery from an already-quantized query vector encoded as
  // an 8-byte Quantizer header followed by one byte per dimension (matching
  // PropertyProto::VectorProto::quantized_values).
  //
  // Returns:
  //   - An EmbeddingQuery on success.
  //   - INVALID_ARGUMENT_ERROR if `quantized_values` is too small to hold a
  //     Quantizer header plus at least one dimension.
  static libtextclassifier3::StatusOr<EmbeddingQuery> CreateFromQuantized(
      std::string_view quantized_values,
      bool enable_quantization_scoring_optimization = true);

  // Creates an EmbeddingQuery from an unquantized float32 query vector.
  //
  // Returns:
  //   - An EmbeddingQuery on success.
  //   - INVALID_ARGUMENT_ERROR if `floats` is empty.
  static libtextclassifier3::StatusOr<EmbeddingQuery> CreateFromFloats(
      std::vector<float> floats);

  int dimension() const { return static_cast<int>(floats_.size()); }

  // The float32 query vector (original floats for unquantized queries, or
  // dequantized floats for quantized queries). Size equals `dimension()`.
  const std::vector<float>& floats() const { return floats_; }

  // Whether this query carries a quantized representation and precomputed
  // statistics for direct 8-bit x 8-bit scoring.
  bool is_quantized() const { return !quantized_values_.empty(); }

  // The quantized query bytes. Size equals `dimension()` when `is_quantized()`
  // is true, or 0 otherwise.
  //
  // REQUIRES: is_quantized()
  const uint8_t* quantized_data() const { return quantized_values_.data(); }

  // The Quantizer used to quantize the query vector.
  //
  // REQUIRES: is_quantized()
  const Quantizer& quantizer() const { return *quantizer_; }

  // Sum of the raw quantized bytes. Used by the FOIL cross terms. Returns 0 if
  // !is_quantized().
  uint32_t quantized_sum() const { return quantized_sum_; }
  // L2 norm of the query. Used by Cosine. Returns 0 if !is_quantized().
  double norm() const { return norm_; }
  // Squared L2 norm of the query. Used by Euclidean. Returns 0 if
  // !is_quantized().
  //
  // Reconstructed in double precision from exact integer sums to match the
  // document-side norm computation and prevent catastrophic cancellation when
  // evaluating Euclidean distance (||q||^2 + ||d||^2 - 2<q, d>) on
  // near-identical vectors.
  double norm_sq() const { return norm_sq_; }
  // 1 / quantizer().scale_factor(), hoisted out of the inner loop. Zero when
  // the quantizer is degenerate (scale_factor() == 0), or if !is_quantized().
  double inv_scale() const { return inv_scale_; }

 private:
  explicit EmbeddingQuery(std::vector<float> floats)
      : floats_(std::move(floats)) {}

  EmbeddingQuery(std::vector<float> floats,
                 std::vector<uint8_t> quantized_values, Quantizer quantizer,
                 uint32_t quantized_sum, double norm, double norm_sq,
                 double inv_scale)
      : floats_(std::move(floats)),
        quantized_values_(std::move(quantized_values)),
        quantizer_(quantizer),
        quantized_sum_(quantized_sum),
        norm_(norm),
        norm_sq_(norm_sq),
        inv_scale_(inv_scale) {}

  std::vector<float> floats_;
  std::vector<uint8_t> quantized_values_;
  std::optional<Quantizer> quantizer_;
  uint32_t quantized_sum_ = 0;
  double norm_ = 0.0;
  double norm_sq_ = 0.0;
  double inv_scale_ = 0.0;
};

class EmbeddingScorer {
 public:
  static libtextclassifier3::StatusOr<std::unique_ptr<EmbeddingScorer>> Create(
      SearchSpecProto::EmbeddingQueryMetricType::Code metric_type);

  virtual float Score(int dimension, const float* v1,
                      const float* v2) const = 0;
  virtual float Score(int dimension, const float* v1, const uint8_t* v2,
                      const Quantizer& quantizer) const = 0;

  // Scores an already-quantized query against an already-quantized document.
  // Neither operand is dequantized per element; the score is reconstructed
  // from integer sums (see the FOIL derivation in embedding-scorer.cc).
  //
  // Behaviorally equivalent to dequantizing `v1` and calling the
  // Score(dimension, const float*, const uint8_t*, const Quantizer&) overload,
  // up to floating-point rounding. Nothing is requantized, so this introduces
  // no additional quantization error.
  //
  // `v1` must have been built from a quantized query vector of length
  // `dimension` (`v1.is_quantized()` is true), and `v2` must point to at least
  // `dimension` bytes.
  //
  // Unlike the float overloads above, this has a single integer implementation
  // shared by the Eigen and ICING_DISABLE_EIGEN builds.
  virtual float Score(int dimension, const EmbeddingQuery& v1,
                      const uint8_t* v2, const Quantizer& quantizer) const = 0;

  virtual ~EmbeddingScorer() = default;
};

class CosineEmbeddingScorer : public EmbeddingScorer {
 public:
  float Score(int dimension, const float* v1, const float* v2) const override;
  float Score(int dimension, const float* v1, const uint8_t* v2,
              const Quantizer& quantizer) const override;
  float Score(int dimension, const EmbeddingQuery& v1, const uint8_t* v2,
              const Quantizer& quantizer) const override;
};

class DotProductEmbeddingScorer : public EmbeddingScorer {
 public:
  float Score(int dimension, const float* v1, const float* v2) const override;
  float Score(int dimension, const float* v1, const uint8_t* v2,
              const Quantizer& quantizer) const override;
  float Score(int dimension, const EmbeddingQuery& v1, const uint8_t* v2,
              const Quantizer& quantizer) const override;
};

class EuclideanDistanceEmbeddingScorer : public EmbeddingScorer {
 public:
  float Score(int dimension, const float* v1, const float* v2) const override;
  float Score(int dimension, const float* v1, const uint8_t* v2,
              const Quantizer& quantizer) const override;
  float Score(int dimension, const EmbeddingQuery& v1, const uint8_t* v2,
              const Quantizer& quantizer) const override;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_EMBED_EMBEDDING_SCORER_H_
