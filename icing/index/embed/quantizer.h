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

#ifndef ICING_INDEX_EMBED_QUANTIZER_H_
#define ICING_INDEX_EMBED_QUANTIZER_H_

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"

namespace icing {
namespace lib {

// A class for quantizing and dequantizing floating-point values to and from
// 8-bit unsigned integers. The maximum quantization error is
// (float_max - float_min) / 255 / 2.
class Quantizer {
 public:
  // Creates a new Quantizer instance based on the specified range. Values
  // outside this range will be quantized to the closest boundary.
  //
  // Returns:
  //   - An Quantizer instance on success.
  //   - INVALID_ARGUMENT_ERROR if float_min is greater than or equal to
  //     float_max.
  static libtextclassifier3::StatusOr<Quantizer> Create(float float_min,
                                                        float float_max) {
    if (float_min >= float_max) {
      return absl_ports::InvalidArgumentError(
          "float_min must be less than float_max.");
    }
    float scale_factor =
        static_cast<float>(kMaxQuantizedValue) / (float_max - float_min);
    return Quantizer(float_min, scale_factor);
  }

  uint8_t Quantize(float value) const {
    double normalized =
        (static_cast<double>(value) - float_min_) * scale_factor_;
    double quantized = std::round(normalized);
    quantized =
        std::clamp(quantized, 0.0, static_cast<double>(kMaxQuantizedValue));
    return static_cast<uint8_t>(quantized);
  }

  float Dequantize(uint8_t quantized) const {
    return (quantized / scale_factor_) + float_min_;
  }

 private:
  static constexpr uint8_t kMaxQuantizedValue =
      std::numeric_limits<uint8_t>::max();

  Quantizer(float float_min, float scale_factor)
      : float_min_(float_min), scale_factor_(scale_factor) {}
  float float_min_;
  float scale_factor_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_EMBED_QUANTIZER_H_
