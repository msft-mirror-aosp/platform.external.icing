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

#ifndef ICING_TOKENIZATION_TRIGRAM_TOKENIZER_H_
#define ICING_TOKENIZATION_TRIGRAM_TOKENIZER_H_

#include <memory>
#include <string_view>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/tokenization/token.h"
#include "icing/tokenization/tokenizer.h"

namespace icing {
namespace lib {

// A tokenizer that splits the input text into trigrams. Trigrams are useful for
// several features, e.g. approximate substring matching, spell checking, etc.
//
// Trigram definition: 3 consecutive characters in a text.
//
// Note: TrigramTokenizer does not do any additional normalization or
// segmentation. It simply yields trigram tokens from the input text without
// skipping any special characters (e.g. punctuations, whitespaces). It is up to
// the caller to handle normalization and segmentation before applying this
// tokenizer.
// - The caller can use PlainTokenizer to normalize and segment a text to
//   several "term tokens" first, and then apply TrigramTokenizer to each of
//   them to split the normalized and segmented term into trigrams.
// - On the other hand, the caller can use VerbatimTokenizer instead to avoid
//   normalization and segmentation, and then apply TrigramTokenizer to the
//   verbatim text.
//
// Example:
//
// Input text: "abcdefg"
// Output tokens: ["abc", "bcd", "cde", "def", "efg"]
//
// Input text: "我每天走路去上班"
// Output tokens: ["我每天", "每天走", "天走路", "走路去", "去上班"]
//
// Input text: "foo bar"
// Output tokens: ["foo", "oo ", "o b", " ba", "bar"]
class TrigramTokenizer : public Tokenizer {
 public:
  libtextclassifier3::StatusOr<std::unique_ptr<Tokenizer::Iterator>> Tokenize(
      std::string_view text) const override;

  libtextclassifier3::StatusOr<std::vector<Token>> TokenizeAll(
      std::string_view text) const override;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_TOKENIZATION_TRIGRAM_TOKENIZER_H_
