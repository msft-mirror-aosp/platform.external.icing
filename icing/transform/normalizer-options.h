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

#ifndef ICING_TRANSFORM_NORMALIZER_OPTIONS_H_
#define ICING_TRANSFORM_NORMALIZER_OPTIONS_H_

namespace icing {
namespace lib {

// TODO: b/332382299 - Avoid using default values in the NormalizerOptions
// constructor. This can lead to unexpected behavior.
struct NormalizerOptions {
  explicit NormalizerOptions(int max_term_byte_size,
                             bool enable_icu_normalizer = false)
      : max_term_byte_size(max_term_byte_size),
        enable_icu_normalizer(enable_icu_normalizer) {}

  // max_term_byte_size enforces the max size of text after normalization,
  // text will be truncated if exceeds the max size.
  int max_term_byte_size;

  // Determines whether to use an ICU based normalizer
  // in icu-with-map-normalizer-factory or not.
  // The default value is false, which means that the fallback option of a
  // map based normalizer will be used.
  //
  // This flag is a no-op for all other normalizer factories because they
  // only support one normalizer type.
  bool enable_icu_normalizer;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_TRANSFORM_NORMALIZER_OPTIONS_H_
