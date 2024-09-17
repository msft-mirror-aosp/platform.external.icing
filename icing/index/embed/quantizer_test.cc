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

#include "icing/index/embed/quantizer.h"

#include <cstdint>
#include <limits>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {

namespace {

using ::testing::AnyOf;
using ::testing::Eq;

constexpr float kFloatEps = 1e-5f;

constexpr float GetMaximumErrorForQuantization(float float_min,
                                               float float_max) {
  return (float_max - float_min) / 255 / 2 + kFloatEps;
}

TEST(QuantizerTest, CreateFailure) {
  EXPECT_THAT(Quantizer::Create(/*float_min=*/1.0f, /*float_max=*/0.0f),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(Quantizer::Create(/*float_min=*/0.0f, /*float_max=*/0.0f),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(QuantizerTest, QuantizeAndDequantize) {
  constexpr float float_min = -1.0f;
  constexpr float float_max = 1.0f;
  constexpr float eps = GetMaximumErrorForQuantization(float_min, float_max);

  ICING_ASSERT_OK_AND_ASSIGN(Quantizer quantizer,
                             Quantizer::Create(float_min, float_max));

  // float_min
  EXPECT_EQ(quantizer.Quantize(float_min), 0);
  EXPECT_FLOAT_EQ(quantizer.Dequantize(0), float_min);

  // float_max
  EXPECT_EQ(quantizer.Quantize(float_max), 255);
  EXPECT_FLOAT_EQ(quantizer.Dequantize(255), float_max);

  // Midpoint
  // After scaling the midpoint value, we should get 127.5. Due to float
  // precision, the quantized value can be either 127 or 128.
  float original_value = 0.0f;
  uint8_t quantized_value = quantizer.Quantize(original_value);
  EXPECT_THAT(quantized_value, AnyOf(Eq(127), Eq(128)));
  EXPECT_NEAR(quantizer.Dequantize(quantized_value), original_value, eps);

  // Other values
  original_value = -0.5f;
  quantized_value = quantizer.Quantize(original_value);
  EXPECT_EQ(quantized_value, 64);
  EXPECT_NEAR(quantizer.Dequantize(quantized_value), original_value, eps);
  original_value = 0.5f;
  quantized_value = quantizer.Quantize(original_value);
  EXPECT_EQ(quantized_value, 191);
  EXPECT_NEAR(quantizer.Dequantize(quantized_value), original_value, eps);

  // Out of range values
  EXPECT_EQ(quantizer.Quantize(-2.0f), 0);
  EXPECT_EQ(quantizer.Quantize(2.0f), 255);
  EXPECT_EQ(quantizer.Quantize(std::numeric_limits<float>::lowest()), 0);
  EXPECT_EQ(quantizer.Quantize(std::numeric_limits<float>::max()), 255);
}

TEST(QuantizerTest, QuantizeAndDequantizeLargerRange) {
  constexpr float float_min = -100.0f;
  constexpr float float_max = 100.0f;
  constexpr float eps = GetMaximumErrorForQuantization(float_min, float_max);

  ICING_ASSERT_OK_AND_ASSIGN(Quantizer quantizer,
                             Quantizer::Create(float_min, float_max));

  // float_min
  EXPECT_EQ(quantizer.Quantize(float_min), 0);
  EXPECT_FLOAT_EQ(quantizer.Dequantize(0), float_min);

  // float_max
  EXPECT_EQ(quantizer.Quantize(float_max), 255);
  EXPECT_FLOAT_EQ(quantizer.Dequantize(255), float_max);

  // Midpoint
  // After scaling the midpoint value, we should get 127.5. Due to float
  // precision, the quantized value can be either 127 or 128.
  float original_value = 0.0f;
  uint8_t quantized_value = quantizer.Quantize(original_value);
  EXPECT_THAT(quantized_value, AnyOf(Eq(127), Eq(128)));
  EXPECT_NEAR(quantizer.Dequantize(quantized_value), original_value, eps);

  // Other values
  original_value = -50.0f;
  quantized_value = quantizer.Quantize(original_value);
  EXPECT_EQ(quantized_value, 64);
  EXPECT_NEAR(quantizer.Dequantize(quantized_value), original_value, eps);
  original_value = 50.0f;
  quantized_value = quantizer.Quantize(original_value);
  EXPECT_EQ(quantized_value, 191);
  EXPECT_NEAR(quantizer.Dequantize(quantized_value), original_value, eps);

  // Out of range values
  EXPECT_EQ(quantizer.Quantize(-150.0f), 0);
  EXPECT_EQ(quantizer.Quantize(150.0f), 255);
  EXPECT_EQ(quantizer.Quantize(std::numeric_limits<float>::lowest()), 0);
  EXPECT_EQ(quantizer.Quantize(std::numeric_limits<float>::max()), 255);
}

// Test when the float range is equal to the uint8_t range. In this case,
// Quantize and Dequantize should be identity functions.
TEST(QuantizerTest, QuantizeAndDequantizeEqualRange) {
  ICING_ASSERT_OK_AND_ASSIGN(
      Quantizer quantizer,
      Quantizer::Create(/*float_min=*/0.0f, /*float_max=*/255.0f));

  for (int i = 0; i < 256; ++i) {
    EXPECT_EQ(quantizer.Quantize(static_cast<float>(i)), i);
    EXPECT_FLOAT_EQ(quantizer.Dequantize(i), static_cast<float>(i));
  }

  // Out of range values
  EXPECT_EQ(quantizer.Quantize(-300.0f), 0);
  EXPECT_EQ(quantizer.Quantize(300.0f), 255);
  EXPECT_EQ(quantizer.Quantize(std::numeric_limits<float>::lowest()), 0);
  EXPECT_EQ(quantizer.Quantize(std::numeric_limits<float>::max()), 255);
}

}  // namespace

}  // namespace lib
}  // namespace icing
