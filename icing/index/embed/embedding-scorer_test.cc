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

#include <cstdint>
#include <memory>
#include <vector>

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

}  // namespace

}  // namespace lib
}  // namespace icing
