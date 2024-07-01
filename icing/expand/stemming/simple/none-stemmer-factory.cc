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

#include <memory>
#include <string>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/expand/stemming/simple/none-stemmer.h"
#include "icing/expand/stemming/stemmer.h"

namespace icing {
namespace lib {

namespace stemmer_factory {

// Creates a dummy stemmer that returns the input word as the stem.
// The language_code is ignored in this implementation.
libtextclassifier3::StatusOr<std::unique_ptr<Stemmer>> Create(
    std::string language_code) {
  return std::make_unique<NoneStemmer>();
}

}  // namespace stemmer_factory

}  // namespace lib
}  // namespace icing
