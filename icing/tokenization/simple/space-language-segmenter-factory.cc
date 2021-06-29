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

#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/tokenization/simple/space-language-segmenter.h"
#include "icing/util/logging.h"

namespace icing {
namespace lib {

namespace language_segmenter_factory {

// Creates a language segmenter with the given locale.
//
// Returns:
//   A LanguageSegmenter on success
//   INVALID_ARGUMENT if locale string is invalid
//
// TODO(b/156383798): Figure out if we want to verify locale strings and notify
// users. Right now illegal locale strings will be ignored by ICU. ICU
// components will be created with its default locale.
libtextclassifier3::StatusOr<std::unique_ptr<LanguageSegmenter>> Create(
    SegmenterOptions) {
  return std::make_unique<SpaceLanguageSegmenter>();
}

}  // namespace language_segmenter_factory

}  // namespace lib
}  // namespace icing