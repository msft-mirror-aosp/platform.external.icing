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
#include <cstring>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <tuple>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/index/embed/quantizer.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {

namespace {

std::vector<uint8_t> QuantizeVector(std::vector<float> v,
                                    const Quantizer& quantizer) {
  std::vector<uint8_t> quantized;
  quantized.reserve(v.size());
  for (float value : v) {
    quantized.push_back(quantizer.Quantize(value));
  }
  return quantized;
}

// Packs `values` the way a caller supplies a quantized query vector: an
// 8-byte Quantizer header followed by one byte per dimension. This mirrors
// PropertyProto::VectorProto::quantized_values.
std::string PackQuantizedVector(const std::vector<float>& values) {
  auto minmax = std::minmax_element(values.begin(), values.end());
  Quantizer quantizer =
      Quantizer::Create(*minmax.first, *minmax.second).ValueOrDie();
  std::string packed(sizeof(Quantizer), '\0');
  memcpy(packed.data(), &quantizer, sizeof(Quantizer));
  for (float value : values) {
    packed.push_back(static_cast<char>(quantizer.Quantize(value)));
  }
  return packed;
}

// Absolute tolerance for comparing two implementations that consume *identical*
// float inputs (Eigen vs. scalar). The only difference between them is
// floating-point association order, so a fixed offset is the right bound.
constexpr float kEps = 0.001f;

// Relative tolerance for comparing quantized-query scoring against a 64-bit
// double ground-truth score. Because the score is reconstructed in double
// precision before narrowing to float, the error scales with the magnitude of
// the result (~1 ULP of float32).
constexpr double kScoreEps = 1e-5;

TEST(EmbeddingScorerTest, DotProduct) {
  constexpr float eps_quantized = 0.01f;

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingScorer> embedding_scorer,
      EmbeddingScorer::Create(
          SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT));
  ICING_ASSERT_OK_AND_ASSIGN(
      Quantizer quantizer,
      Quantizer::Create(/*float_min=*/-1.0f, /*float_max=*/1.0f));

  int dimension = 3;
  std::vector<float> v1 = {0.1f, 0.2f, 0.3f};
  std::vector<float> v2 = {0.5f, 0.5f, 0.6f};
  std::vector<uint8_t> v2_quantized = QuantizeVector(v2, quantizer);
  float expected_dot_product = 0.1f * 0.5f + 0.2f * 0.5f + 0.3f * 0.6f;

  // Test float computation
  EXPECT_FLOAT_EQ(embedding_scorer->Score(dimension, v1.data(), v2.data()),
                  expected_dot_product);

  // Test quantization
  EXPECT_NEAR(embedding_scorer->Score(dimension, v1.data(), v2_quantized.data(),
                                      quantizer),
              expected_dot_product, eps_quantized);

  // Test 8-bit x 8-bit quantization
  ICING_ASSERT_OK_AND_ASSIGN(
      EmbeddingQuery query,
      EmbeddingQuery::CreateFromQuantized(PackQuantizedVector(v1)));
  EXPECT_NEAR(
      embedding_scorer->Score(dimension, query, v2_quantized.data(), quantizer),
      expected_dot_product, eps_quantized);
}

TEST(EmbeddingScorerTest, Cosine) {
  constexpr float eps = 0.001f;
  constexpr float eps_quantized = 0.01f;

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingScorer> embedding_scorer,
      EmbeddingScorer::Create(
          SearchSpecProto::EmbeddingQueryMetricType::COSINE));
  ICING_ASSERT_OK_AND_ASSIGN(
      Quantizer quantizer,
      Quantizer::Create(/*float_min=*/-1.0f, /*float_max=*/1.0f));

  int dimension = 3;
  std::vector<float> v1 = {0.7f, -0.3f, -0.6f};
  std::vector<float> v2 = {-0.5f, 0.1f, -0.2f};
  std::vector<uint8_t> v2_quantized = QuantizeVector(v2, quantizer);
  float expected_cosine = -0.4896f;

  // Test float computation
  EXPECT_NEAR(embedding_scorer->Score(dimension, v1.data(), v2.data()),
              expected_cosine, eps);

  // Test quantization
  EXPECT_NEAR(embedding_scorer->Score(dimension, v1.data(), v2_quantized.data(),
                                      quantizer),
              expected_cosine, eps_quantized);

  // Test 8-bit x 8-bit quantization
  ICING_ASSERT_OK_AND_ASSIGN(
      EmbeddingQuery query,
      EmbeddingQuery::CreateFromQuantized(PackQuantizedVector(v1)));
  EXPECT_NEAR(
      embedding_scorer->Score(dimension, query, v2_quantized.data(), quantizer),
      expected_cosine, eps_quantized);
}

TEST(EmbeddingScorerTest, CosineWithZeroVector) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingScorer> embedding_scorer,
      EmbeddingScorer::Create(
          SearchSpecProto::EmbeddingQueryMetricType::COSINE));
  ICING_ASSERT_OK_AND_ASSIGN(
      Quantizer quantizer,
      Quantizer::Create(/*float_min=*/-1.0f, /*float_max=*/1.0f));
  ICING_ASSERT_OK_AND_ASSIGN(
      Quantizer zero_quantizer,
      Quantizer::Create(/*float_min=*/0.0f, /*float_max=*/0.0f));

  int dimension = 3;
  std::vector<float> zero_vec = {0.0f, 0.0f, 0.0f};
  std::vector<float> non_zero_vec = {0.7f, -0.3f, -0.6f};
  std::vector<uint8_t> zero_vec_quantized =
      QuantizeVector(zero_vec, zero_quantizer);
  std::vector<uint8_t> non_zero_vec_quantized =
      QuantizeVector(non_zero_vec, quantizer);

  // Case 1: v1 is zero vector, v2 is non-zero vector
  EXPECT_FLOAT_EQ(
      embedding_scorer->Score(dimension, zero_vec.data(), non_zero_vec.data()),
      0.0f);
  EXPECT_FLOAT_EQ(
      embedding_scorer->Score(dimension, zero_vec.data(),
                              non_zero_vec_quantized.data(), quantizer),
      0.0f);

  // Case 2: v1 is non-zero vector, v2 is zero vector
  EXPECT_FLOAT_EQ(
      embedding_scorer->Score(dimension, non_zero_vec.data(), zero_vec.data()),
      0.0f);
  EXPECT_FLOAT_EQ(
      embedding_scorer->Score(dimension, non_zero_vec.data(),
                              zero_vec_quantized.data(), zero_quantizer),
      0.0f);

  // Case 3: Both v1 and v2 are zero vectors
  EXPECT_FLOAT_EQ(
      embedding_scorer->Score(dimension, zero_vec.data(), zero_vec.data()),
      0.0f);
  EXPECT_FLOAT_EQ(
      embedding_scorer->Score(dimension, zero_vec.data(),
                              zero_vec_quantized.data(), zero_quantizer),
      0.0f);
}

TEST(EmbeddingScorerTest, Euclidean) {
  constexpr float eps = 0.001f;
  constexpr float eps_quantized = 0.01f;

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<EmbeddingScorer> embedding_scorer,
      EmbeddingScorer::Create(
          SearchSpecProto::EmbeddingQueryMetricType::EUCLIDEAN));
  ICING_ASSERT_OK_AND_ASSIGN(
      Quantizer quantizer,
      Quantizer::Create(/*float_min=*/-1.0f, /*float_max=*/1.0f));

  int dimension = 3;
  std::vector<float> v1 = {0.6f, -0.2f, 0.9f};
  std::vector<float> v2 = {-0.8f, -0.4f, 0.2f};
  std::vector<uint8_t> v2_quantized = QuantizeVector(v2, quantizer);
  float expected_euclidean = 1.5780f;

  // Test float computation
  EXPECT_NEAR(embedding_scorer->Score(dimension, v1.data(), v2.data()),
              expected_euclidean, eps);

  // Test quantization
  EXPECT_NEAR(embedding_scorer->Score(dimension, v1.data(), v2_quantized.data(),
                                      quantizer),
              expected_euclidean, eps_quantized);

  // Test 8-bit x 8-bit quantization
  ICING_ASSERT_OK_AND_ASSIGN(
      EmbeddingQuery query,
      EmbeddingQuery::CreateFromQuantized(PackQuantizedVector(v1)));
  EXPECT_NEAR(
      embedding_scorer->Score(dimension, query, v2_quantized.data(), quantizer),
      expected_euclidean, eps_quantized);
}

TEST(EmbeddingScorerTest, UnquantizedEmbeddingQueryDispatchesToFloatOverload) {
  std::vector<float> v1 = {0.7f, -0.3f, 0.5f};
  std::vector<float> v2 = {-0.2f, 0.4f, 0.8f};
  int dimension = 3;

  ICING_ASSERT_OK_AND_ASSIGN(
      Quantizer quantizer,
      Quantizer::Create(/*float_min=*/-1.0f, /*float_max=*/1.0f));
  std::vector<uint8_t> v2_quantized = QuantizeVector(v2, quantizer);

  // Test both ways of obtaining an unquantized EmbeddingQuery:
  // 1. Direct from vector<float>
  ICING_ASSERT_OK_AND_ASSIGN(EmbeddingQuery query_from_floats,
                             EmbeddingQuery::CreateFromFloats(v1));
  EXPECT_FALSE(query_from_floats.is_quantized());

  // 2. From quantized bytes with optimization disabled
  ICING_ASSERT_OK_AND_ASSIGN(
      EmbeddingQuery query_unoptimized,
      EmbeddingQuery::CreateFromQuantized(
          PackQuantizedVector(v1),
          /*enable_quantization_scoring_optimization=*/false));
  EXPECT_FALSE(query_unoptimized.is_quantized());

  for (auto metric : {SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT,
                      SearchSpecProto::EmbeddingQueryMetricType::COSINE,
                      SearchSpecProto::EmbeddingQueryMetricType::EUCLIDEAN}) {
    ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EmbeddingScorer> scorer,
                               EmbeddingScorer::Create(metric));
    float expected_score =
        scorer->Score(dimension, v1.data(), v2_quantized.data(), quantizer);

    EXPECT_FLOAT_EQ(scorer->Score(dimension, query_from_floats,
                                  v2_quantized.data(), quantizer),
                    expected_score);
    EXPECT_FLOAT_EQ(scorer->Score(dimension, query_unoptimized,
                                  v2_quantized.data(), quantizer),
                    expected_score);
  }
}

TEST(EmbeddingScorerTest,
     DimensionExceedingMaxQuantizedScoringFallsBackToUnquantized) {
  // kMaxQuantizedScoringDimension is 66,051. Beyond this dimension,
  // CreateFromQuantized must return an unquantized EmbeddingQuery to prevent
  // uint32_t accumulator overflow.
  constexpr size_t kDimension = 66052;
  ICING_ASSERT_OK_AND_ASSIGN(
      Quantizer quantizer,
      Quantizer::Create(/*float_min=*/-1.0f, /*float_max=*/1.0f));
  std::string packed(sizeof(Quantizer) + kDimension, '\0');
  std::memcpy(packed.data(), &quantizer, sizeof(Quantizer));

  ICING_ASSERT_OK_AND_ASSIGN(EmbeddingQuery query,
                             EmbeddingQuery::CreateFromQuantized(packed));
  EXPECT_FALSE(query.is_quantized());
  EXPECT_EQ(query.dimension(), kDimension);
}

TEST(EmbeddingScorerTest, MalformedNanQuantizerPropagatesNan) {
  int dimension = 3;
  ICING_ASSERT_OK_AND_ASSIGN(
      Quantizer doc_quantizer,
      Quantizer::Create(/*float_min=*/-1.0f, /*float_max=*/1.0f));
  std::vector<uint8_t> v2_quantized = {10, 20, 30};

  // Construct a malformed quantizer with NaN float_min and scale_factor.
  alignas(Quantizer) char nan_quantizer_bytes[sizeof(Quantizer)];
  float nan_val = std::numeric_limits<float>::quiet_NaN();
  std::memcpy(nan_quantizer_bytes, &nan_val, sizeof(float));
  std::memcpy(nan_quantizer_bytes + sizeof(float), &nan_val, sizeof(float));
  Quantizer nan_doc_quantizer(nan_quantizer_bytes);

  // Packed query with normal quantizer.
  std::vector<float> v1 = {0.5f, 0.5f, 0.5f};
  ICING_ASSERT_OK_AND_ASSIGN(
      EmbeddingQuery query,
      EmbeddingQuery::CreateFromQuantized(PackQuantizedVector(v1)));

  // Packed query with NaN quantizer header.
  std::string nan_query_packed(sizeof(Quantizer) + dimension, '\0');
  std::memcpy(nan_query_packed.data(), nan_quantizer_bytes, sizeof(Quantizer));
  ICING_ASSERT_OK_AND_ASSIGN(
      EmbeddingQuery nan_query,
      EmbeddingQuery::CreateFromQuantized(nan_query_packed));

  for (auto metric : {SearchSpecProto::EmbeddingQueryMetricType::COSINE,
                      SearchSpecProto::EmbeddingQueryMetricType::EUCLIDEAN}) {
    ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<EmbeddingScorer> scorer,
                               EmbeddingScorer::Create(metric));

    // When the document quantizer is NaN, scores must be NaN, not 0.0f.
    EXPECT_TRUE(std::isnan(scorer->Score(dimension, query, v2_quantized.data(),
                                         nan_doc_quantizer)));

    // When the query quantizer is NaN, scores must be NaN, not 0.0f.
    EXPECT_TRUE(std::isnan(scorer->Score(dimension, nan_query,
                                         v2_quantized.data(), doc_quantizer)));
  }
}

TEST(EmbeddingScorerTest, CreateWithUnknownMetricTypeReturnsError) {
  EXPECT_THAT(EmbeddingScorer::Create(
                  SearchSpecProto::EmbeddingQueryMetricType::UNKNOWN),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

// A straightforward, independent implementation of the supported metrics, used
// as the ground truth to validate EmbeddingScorer against.
float ReferenceScore(SearchSpecProto::EmbeddingQueryMetricType::Code metric,
                     const std::vector<float>& v1,
                     const std::vector<float>& v2) {
  float dot_product = 0.0f;
  float squared_norm1 = 0.0f;
  float squared_norm2 = 0.0f;
  float squared_distance = 0.0f;
  for (size_t i = 0; i < v1.size(); ++i) {
    dot_product += v1[i] * v2[i];
    squared_norm1 += v1[i] * v1[i];
    squared_norm2 += v2[i] * v2[i];
    float diff = v1[i] - v2[i];
    squared_distance += diff * diff;
  }

  switch (metric) {
    case SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT:
      return dot_product;
    case SearchSpecProto::EmbeddingQueryMetricType::COSINE: {
      float divisor = std::sqrt(squared_norm1) * std::sqrt(squared_norm2);
      return divisor == 0.0f ? 0.0f : dot_product / divisor;
    }
    case SearchSpecProto::EmbeddingQueryMetricType::EUCLIDEAN:
      return std::sqrt(squared_distance);
    default:
      ADD_FAILURE() << "Unsupported metric type: " << metric;
      return 0.0f;
  }
}

std::vector<float> DequantizeVector(const std::vector<uint8_t>& quantized,
                                    const Quantizer& quantizer) {
  std::vector<float> dequantized;
  dequantized.reserve(quantized.size());
  for (uint8_t value : quantized) {
    dequantized.push_back(quantizer.Dequantize(value));
  }
  return dequantized;
}

class EmbeddingScorerReferenceTest
    : public testing::TestWithParam<
          std::tuple<SearchSpecProto::EmbeddingQueryMetricType::Code, int>> {
 protected:
  void SetUp() override {
    metric_ = std::get<0>(GetParam());
    dimension_ = std::get<1>(GetParam());
    ICING_ASSERT_OK_AND_ASSIGN(embedding_scorer_,
                               EmbeddingScorer::Create(metric_));

    // Initialize random number generator
    random_ = std::default_random_engine(std::random_device()());
    dist_ = std::uniform_real_distribution<float>(-3.0f, 3.0f);
  }

  // Generates a random vector of the specified dimension.
  std::vector<float> GenerateRandomVector() {
    std::vector<float> vec(dimension_);
    for (int i = 0; i < dimension_; ++i) {
      vec[i] = dist_(random_);
    }
    return vec;
  }

  std::vector<float> GenerateRandomConstantVector() {
    float value = dist_(random_);
    std::vector<float> vec(dimension_, value);
    return vec;
  }

  const int kNumRandomPairs = 1000;

  SearchSpecProto::EmbeddingQueryMetricType::Code metric_;
  int dimension_;
  std::unique_ptr<EmbeddingScorer> embedding_scorer_;
  std::default_random_engine random_;
  std::uniform_real_distribution<float> dist_;
};

// Test that the Score function matches the reference implementation for a
// variety of random vectors.
TEST_P(EmbeddingScorerReferenceTest, ScoreMatchesReference) {
  for (int i = 0; i < kNumRandomPairs; ++i) {
    std::vector<float> v1 = GenerateRandomVector();
    std::vector<float> v2 = GenerateRandomVector();

    // Compare scores
    float score_val =
        embedding_scorer_->Score(dimension_, v1.data(), v2.data());
    ASSERT_NEAR(score_val, ReferenceScore(metric_, v1, v2), kEps);
  }
}

// Test that the Score function matches the reference implementation for a
// variety of random quantized vectors.
TEST_P(EmbeddingScorerReferenceTest, ScoreMatchesReferenceForQuantizedVectors) {
  for (int i = 0; i < kNumRandomPairs; ++i) {
    std::vector<float> v1 = GenerateRandomVector();
    std::vector<float> v2 = GenerateRandomVector();

    // Quantize v2
    auto v2_minmax_pair = std::minmax_element(v2.begin(), v2.end());
    ICING_ASSERT_OK_AND_ASSIGN(
        Quantizer quantizer,
        Quantizer::Create(*v2_minmax_pair.first, *v2_minmax_pair.second));
    std::vector<uint8_t> v2_quantized = QuantizeVector(v2, quantizer);

    // Compare scores
    float score_val = embedding_scorer_->Score(dimension_, v1.data(),
                                               v2_quantized.data(), quantizer);
    ASSERT_NEAR(
        score_val,
        ReferenceScore(metric_, v1, DequantizeVector(v2_quantized, quantizer)),
        kEps);
  }
}

// Test that the Score function matches the reference implementation for
// constant vectors (i.e. all values are the same) to be quantized.
TEST_P(EmbeddingScorerReferenceTest,
       ScoreMatchesReferenceForQuantizedConstantVectors) {
  for (int i = 0; i < kNumRandomPairs; ++i) {
    std::vector<float> v1 = GenerateRandomVector();
    std::vector<float> v2 = GenerateRandomConstantVector();

    // Check that v2 is constant.
    auto v2_minmax_pair = std::minmax_element(v2.begin(), v2.end());
    ASSERT_TRUE(*v2_minmax_pair.first == *v2_minmax_pair.second);
    // Quantize v2
    ICING_ASSERT_OK_AND_ASSIGN(
        Quantizer quantizer,
        Quantizer::Create(*v2_minmax_pair.first, *v2_minmax_pair.second));
    ASSERT_EQ(quantizer.scale_factor(), 0.f);
    std::vector<uint8_t> v2_quantized = QuantizeVector(v2, quantizer);

    // Compare scores
    float score_val = embedding_scorer_->Score(dimension_, v1.data(),
                                               v2_quantized.data(), quantizer);
    ASSERT_NEAR(
        score_val,
        ReferenceScore(metric_, v1, DequantizeVector(v2_quantized, quantizer)),
        kEps);
  }
}

// Unpacks a 1-byte value in 64-bit `double` precision without the per-element
// 32-bit `float` rounding of `Quantizer::Dequantize()`.
double DequantizeDouble(const Quantizer& quantizer, uint8_t quantized) {
  if (quantizer.scale_factor() == 0.0f) {
    return static_cast<double>(quantizer.float_min());
  }
  return (static_cast<double>(quantized) /
          static_cast<double>(quantizer.scale_factor())) +
         static_cast<double>(quantizer.float_min());
}

// Computes the exact mathematical score in 64-bit `double` precision over the
// unpacked 8-bit values of `query` and `v2_quantized`, without 32-bit
// per-element or accumulation rounding.
double DoubleGroundTruthScore(
    SearchSpecProto::EmbeddingQueryMetricType::Code metric, int dimension,
    const EmbeddingQuery& query, const uint8_t* v2_quantized,
    const Quantizer& doc_quantizer) {
  double dot = 0.0;
  double q_sq_norm = 0.0;
  double d_sq_norm = 0.0;
  double diff_sq_sum = 0.0;
  for (int j = 0; j < dimension; ++j) {
    double q = DequantizeDouble(query.quantizer(), query.quantized_data()[j]);
    double d = DequantizeDouble(doc_quantizer, v2_quantized[j]);
    dot += q * d;
    q_sq_norm += q * q;
    d_sq_norm += d * d;
    double diff = q - d;
    diff_sq_sum += diff * diff;
  }
  switch (metric) {
    case SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT:
      return dot;
    case SearchSpecProto::EmbeddingQueryMetricType::COSINE: {
      double denom = std::sqrt(q_sq_norm) * std::sqrt(d_sq_norm);
      return denom > 0.0 ? dot / denom : 0.0;
    }
    case SearchSpecProto::EmbeddingQueryMetricType::EUCLIDEAN:
      return std::sqrt(diff_sq_sum);
    default:
      return 0.0;
  }
}

void ExpectQuantizedQueryScoreMatchesGroundTruth(
    const EmbeddingScorer& scorer,
    SearchSpecProto::EmbeddingQueryMetricType::Code metric, int dimension,
    const std::vector<float>& query_floats,
    const std::vector<float>& doc_floats) {
  ICING_ASSERT_OK_AND_ASSIGN(
      EmbeddingQuery query,
      EmbeddingQuery::CreateFromQuantized(PackQuantizedVector(query_floats)));

  auto doc_minmax = std::minmax_element(doc_floats.begin(), doc_floats.end());
  ICING_ASSERT_OK_AND_ASSIGN(
      Quantizer doc_quantizer,
      Quantizer::Create(*doc_minmax.first, *doc_minmax.second));
  std::vector<uint8_t> v2_quantized = QuantizeVector(doc_floats, doc_quantizer);

  double ground_truth = DoubleGroundTruthScore(
      metric, dimension, query, v2_quantized.data(), doc_quantizer);
  double quantized_query_score = static_cast<double>(
      scorer.Score(dimension, query, v2_quantized.data(), doc_quantizer));

  double max_error = std::max(1.0, std::abs(ground_truth)) * kScoreEps;
  EXPECT_NEAR(quantized_query_score, ground_truth, max_error);
}

TEST_P(EmbeddingScorerReferenceTest, ScoreMatchesGroundTruthForQuantizedQuery) {
  for (int i = 0; i < kNumRandomPairs; ++i) {
    ExpectQuantizedQueryScoreMatchesGroundTruth(
        *embedding_scorer_, metric_, dimension_, GenerateRandomVector(),
        GenerateRandomVector());
  }
}

// Both operands degenerate: Quantizer::scale_factor() is 0 on each side, so
// every byte unpacks to float_min.
TEST_P(EmbeddingScorerReferenceTest,
       ScoreMatchesGroundTruthForQuantizedQueryForConstantVectors) {
  for (int i = 0; i < kNumRandomPairs; ++i) {
    std::vector<float> query = GenerateRandomConstantVector();
    std::vector<float> doc = GenerateRandomConstantVector();

    auto query_minmax = std::minmax_element(query.begin(), query.end());
    ICING_ASSERT_OK_AND_ASSIGN(
        Quantizer query_quantizer,
        Quantizer::Create(*query_minmax.first, *query_minmax.second));
    ASSERT_EQ(query_quantizer.scale_factor(), 0.0f);

    auto doc_minmax = std::minmax_element(doc.begin(), doc.end());
    ICING_ASSERT_OK_AND_ASSIGN(
        Quantizer doc_quantizer,
        Quantizer::Create(*doc_minmax.first, *doc_minmax.second));
    ASSERT_EQ(doc_quantizer.scale_factor(), 0.0f);

    ExpectQuantizedQueryScoreMatchesGroundTruth(*embedding_scorer_, metric_,
                                                dimension_, query, doc);
  }
}

// Exercises asymmetric degenerate quantizer cases (scale_factor() == 0) where
// only one operand is constant.
TEST_P(EmbeddingScorerReferenceTest,
       ScoreMatchesGroundTruthForQuantizedQueryForConstantQuery) {
  for (int i = 0; i < kNumRandomPairs; ++i) {
    ExpectQuantizedQueryScoreMatchesGroundTruth(
        *embedding_scorer_, metric_, dimension_, GenerateRandomConstantVector(),
        GenerateRandomVector());
  }
}

TEST_P(EmbeddingScorerReferenceTest,
       ScoreMatchesGroundTruthForQuantizedQueryForConstantDocument) {
  for (int i = 0; i < kNumRandomPairs; ++i) {
    ExpectQuantizedQueryScoreMatchesGroundTruth(
        *embedding_scorer_, metric_, dimension_, GenerateRandomVector(),
        GenerateRandomConstantVector());
  }
}

// Exercises catastrophic cancellation in Euclidean distance
// (||q||^2 + ||d||^2 - 2<q, d>) when query and document vectors are identical.
TEST_P(EmbeddingScorerReferenceTest,
       ScoreMatchesGroundTruthForQuantizedQueryForDuplicateVectors) {
  for (int i = 0; i < kNumRandomPairs; ++i) {
    std::vector<float> v = GenerateRandomVector();
    ExpectQuantizedQueryScoreMatchesGroundTruth(*embedding_scorer_, metric_,
                                                dimension_, v, v);
  }
}

INSTANTIATE_TEST_SUITE_P(
    ScoreVsReferenceComparison, EmbeddingScorerReferenceTest,
    testing::Combine(
        testing::Values(SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT,
                        SearchSpecProto::EmbeddingQueryMetricType::COSINE,
                        SearchSpecProto::EmbeddingQueryMetricType::EUCLIDEAN),
        // Includes a non-multiple-of-4 dimension (130) to exercise both the
        // 4-wide unrolled loop and scalar tail.
        testing::Values(128, 130, 512, 768, 1024)));

}  // namespace

}  // namespace lib
}  // namespace icing
