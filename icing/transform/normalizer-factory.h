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

#ifndef THIRD_PARTY_ICING_TRANSFORM_NORMALIZER_FACTORY_H_
#define THIRD_PARTY_ICING_TRANSFORM_NORMALIZER_FACTORY_H_

#include <memory>

#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/statusor.h"
#include "third_party/icing/transform/normalizer-options.h"
#include "third_party/icing/transform/normalizer.h"

namespace icing {
namespace lib {

namespace normalizer_factory {

// Creates a normalizer.
//
// Returns:
//   A normalizer on success
//   INVALID_ARGUMENT if options.max_term_byte_size <= 0
//   INTERNAL_ERROR on errors
libtextclassifier3::StatusOr<std::unique_ptr<Normalizer>> Create(
    const NormalizerOptions& options);

}  // namespace normalizer_factory

}  // namespace lib
}  // namespace icing

#endif  // THIRD_PARTY_ICING_TRANSFORM_NORMALIZER_FACTORY_H_
