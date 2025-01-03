// Copyright (C) 2019 Google LLC
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

#include <memory>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/transform/map/map-normalizer.h"
#include "icing/transform/normalizer-options.h"
#include "icing/transform/normalizer.h"

namespace icing {
namespace lib {

namespace normalizer_factory {

// Creates a map-based normalizer.
//
// Returns:
//   A normalizer on success
//   INVALID_ARGUMENT_ERROR if options.max_term_byte_size <= 0
//   INTERNAL_ERROR on errors
libtextclassifier3::StatusOr<std::unique_ptr<Normalizer>> Create(
    const NormalizerOptions& options) {
  if (options.max_term_byte_size <= 0) {
    return absl_ports::InvalidArgumentError(
        "normalizer_max_term_byte_size must be greater than zero.");
  }

  return std::make_unique<MapNormalizer>(
      options.max_term_byte_size);
}

}  // namespace normalizer_factory

}  // namespace lib
}  // namespace icing
