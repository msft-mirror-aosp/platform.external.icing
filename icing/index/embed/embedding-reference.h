// Copyright (C) 2026 Google LLC
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

#ifndef ICING_INDEX_EMBED_EMBEDDING_REFERENCE_H_
#define ICING_INDEX_EMBED_EMBEDDING_REFERENCE_H_

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/absl_ports/canonical_errors.h"

namespace icing {
namespace lib {

struct EmbeddingReference {
  // If not null, use this (unquantized).
  const float* float_vector = nullptr;

  // If float_vector is null, use this (quantized).
  // The Quantizer object is stored at the beginning of the array,
  // followed immediately by the quantized vector data.
  const char* quantized_vector = nullptr;

  libtextclassifier3::Status Validate() const {
    bool has_float = (float_vector != nullptr);
    bool has_quantized = (quantized_vector != nullptr);

    if (has_float == has_quantized) {
      return absl_ports::InvalidArgumentError(
          "EmbeddingReference must have exactly one of float_vector or "
          "quantized_vector set.");
    }
    return libtextclassifier3::Status::OK;
  }
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_EMBED_EMBEDDING_REFERENCE_H_
