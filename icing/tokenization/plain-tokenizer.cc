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

#include "icing/tokenization/plain-tokenizer.h"

#include <cstdint>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/status_macros.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/util/i18n-utils.h"
#include "unicode/umachine.h"

namespace icing {
namespace lib {

namespace {
// Helper function to validate a term.
// A term is valid if:
//   1. it's not empty
//   2. it's not a whitespace
//   3. it's not a punctuation mark
//
// TODO(b/141007791): figure out how we'd like to support special characters
// like "+", "&", "@", "#" in indexing and query tokenizers.
bool IsValidTerm(std::string_view term) {
  if (term.empty()) {
    return false;
  }
  // Gets the first unicode character. We can know what the whole term is by
  // checking only the first character.
  UChar32 uchar32 = i18n_utils::GetUChar32At(term.data(), term.length(), 0);
  return !u_isUWhiteSpace(uchar32) && !u_ispunct(uchar32);
}
}  // namespace

// Plain tokenizer applies its rules to the results from language segmenter. It
// simply filters out invalid terms from language segmenter and returns
// everything else as tokens. Please refer to IsValidTerm() above for what terms
// are valid.
class PlainTokenIterator : public Tokenizer::Iterator {
 public:
  explicit PlainTokenIterator(
      std::unique_ptr<LanguageSegmenter::Iterator> base_iterator)
      : base_iterator_(std::move(base_iterator)) {}

  bool Advance() override {
    bool found_next_valid_term = false;
    while (!found_next_valid_term && base_iterator_->Advance()) {
      current_term_ = base_iterator_->GetTerm();
      found_next_valid_term = IsValidTerm(current_term_);
    }
    return found_next_valid_term;
  }

  Token GetToken() const override {
    if (current_term_.empty()) {
      return Token(Token::INVALID);
    }
    return Token(Token::REGULAR, current_term_);
  }

  bool ResetToTokenAfter(int32_t offset) override {
    if (!base_iterator_->ResetToTermStartingAfter(offset).ok()) {
      return false;
    }
    current_term_ = base_iterator_->GetTerm();
    if (!IsValidTerm(current_term_)) {
      // If the current value isn't valid, advance to the next valid value.
      return Advance();
    }
    return true;
  }

  bool ResetToTokenBefore(int32_t offset) override {
    TC3_ASSIGN_OR_RETURN(
        offset, base_iterator_->ResetToTermEndingBefore(offset), false);
    current_term_ = base_iterator_->GetTerm();
    while (!IsValidTerm(current_term_)) {
      // Haven't found a valid term yet. Retrieve the term prior to this one
      // from the segmenter.
      TC3_ASSIGN_OR_RETURN(
          offset, base_iterator_->ResetToTermEndingBefore(offset), false);
      current_term_ = base_iterator_->GetTerm();
    }
    return true;
  }

 private:
  std::unique_ptr<LanguageSegmenter::Iterator> base_iterator_;
  std::string_view current_term_;
};

libtextclassifier3::StatusOr<std::unique_ptr<Tokenizer::Iterator>>
PlainTokenizer::Tokenize(std::string_view text) const {
  TC3_ASSIGN_OR_RETURN(
      std::unique_ptr<LanguageSegmenter::Iterator> base_iterator,
      language_segmenter_.Segment(text));
  return std::make_unique<PlainTokenIterator>(std::move(base_iterator));
}

libtextclassifier3::StatusOr<std::vector<Token>> PlainTokenizer::TokenizeAll(
    std::string_view text) const {
  TC3_ASSIGN_OR_RETURN(std::unique_ptr<Tokenizer::Iterator> iterator,
                         Tokenize(text));
  std::vector<Token> tokens;
  while (iterator->Advance()) {
    tokens.push_back(iterator->GetToken());
  }
  return tokens;
}

}  // namespace lib
}  // namespace icing
