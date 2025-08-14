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

#include "third_party/icing/index/embed/embedding-scorer.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <random>
#include <tuple>
#include <vector>

#include "testing/base/public/gunit.h"
#include "third_party/icing/index/embed/quantizer.h"
#include "third_party/icing/testing/common-matchers.h"

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
}

class EmbeddingScorerEigenTest
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
  const float kEps = 0.001f;

  SearchSpecProto::EmbeddingQueryMetricType::Code metric_;
  int dimension_;
  std::unique_ptr<EmbeddingScorer> embedding_scorer_;
  std::default_random_engine random_;
  std::uniform_real_distribution<float> dist_;
};

// Test that the EigenScore function matches the Score function for a variety
// of random vectors.
TEST_P(EmbeddingScorerEigenTest, EigenScoreMatchesScore) {
  for (int i = 0; i < kNumRandomPairs; ++i) {
    std::vector<float> v1 = GenerateRandomVector();
    std::vector<float> v2 = GenerateRandomVector();

    // Compare scores
    float score_val =
        embedding_scorer_->Score(dimension_, v1.data(), v2.data());
    float eigen_score_val =
        embedding_scorer_->EigenScore(dimension_, v1.data(), v2.data());
    ASSERT_NEAR(score_val, eigen_score_val, kEps);
  }
}

// Test that the EigenScore function matches the Score function for a variety
// of random quantized vectors.
TEST_P(EmbeddingScorerEigenTest, EigenScoreMatchesScoreForQuantizedVectors) {
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
    float eigen_score_val = embedding_scorer_->EigenScore(
        dimension_, v1.data(), v2_quantized.data(), quantizer);
    ASSERT_NEAR(score_val, eigen_score_val, kEps);
  }
}

// Test that the EigenScore function matches the Score function for constant
// vectors (i.e. all values are the same) to be quantized.
TEST_P(EmbeddingScorerEigenTest,
       EigenScoreMatchesScoreForQuantizedConstantVectors) {
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
    float eigen_score_val = embedding_scorer_->EigenScore(
        dimension_, v1.data(), v2_quantized.data(), quantizer);
    ASSERT_NEAR(score_val, eigen_score_val, kEps);
  }
}

INSTANTIATE_TEST_SUITE_P(
    EigenVsScoreComparison, EmbeddingScorerEigenTest,
    testing::Combine(
        testing::Values(SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT,
                        SearchSpecProto::EmbeddingQueryMetricType::COSINE,
                        SearchSpecProto::EmbeddingQueryMetricType::EUCLIDEAN),
        testing::Values(128, 512, 768, 1024)));

}  // namespace

}  // namespace lib
}  // namespace icing
