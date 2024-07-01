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

#ifndef ICING_EXPAND_STEMMING_STEMMER_FACTORY_H_
#define ICING_EXPAND_STEMMING_STEMMER_FACTORY_H_

#include <memory>
#include <string>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/expand/stemming/stemmer.h"

namespace icing {
namespace lib {

namespace stemmer_factory {

// Creates a stemmer for the given language code.
//
// The language code should be a IETF BCP 47 language tag.
//   - E.g. "en" for English, "fr" for French, etc.
//   - See https://en.wikipedia.org/wiki/IETF_language_tag for more
//     information.
//
// This is the header file for the factory function. Implementations are in the
// .cc files, and we select which stemmer and .cc file to build with in each
// build rule.
//
// Returns:
//  - A stemmer on success
//  - INVALID_ARGUMENT_ERROR if the language code is invalid or not supported.
libtextclassifier3::StatusOr<std::unique_ptr<Stemmer>> Create(
    std::string language_code);

}  // namespace stemmer_factory

}  // namespace lib
}  // namespace icing

#endif  // ICING_EXPAND_STEMMING_STEMMER_FACTORY_H_
