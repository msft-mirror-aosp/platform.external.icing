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

#include "icing/tokenization/tokenizer-factory.h"

#include "utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/tokenization/plain-tokenizer.h"

namespace icing {
namespace lib {

namespace tokenizer_factory {

libtextclassifier3::StatusOr<std::unique_ptr<Tokenizer>>
CreateIndexingTokenizer(IndexingConfig::TokenizerType::Code type,
                        const LanguageSegmenter* lang_segmenter) {
  switch (type) {
    case IndexingConfig::TokenizerType::PLAIN:
      return std::make_unique<PlainTokenizer>(lang_segmenter);
    case IndexingConfig::TokenizerType::NONE:
      U_FALLTHROUGH;
    default:
      // This should never happen.
      return absl_ports::InvalidArgumentError(
          "Invalid tokenizer type for an indexed section");
  }
}

}  // namespace tokenizer_factory

}  // namespace lib
}  // namespace icing
