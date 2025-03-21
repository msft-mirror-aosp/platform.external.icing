// Copyright (C) 2021 Google LLC
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

#include "icing/tokenization/verbatim-tokenizer.h"

#include <cstdint>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/tokenization/token.h"
#include "icing/tokenization/tokenizer.h"
#include "icing/util/character-iterator.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

class VerbatimTokenIterator : public Tokenizer::Iterator {
 public:
  explicit VerbatimTokenIterator(std::string_view text)
      : term_(std::move(text)), has_advanced_to_end_(false) {}

  bool Advance() override {
    if (term_.empty() || has_advanced_to_end_) {
      return false;
    }

    has_advanced_to_end_ = true;
    return true;
  }

  std::vector<Token> GetTokens() const override {
    std::vector<Token> result;

    if (!term_.empty() && has_advanced_to_end_) {
      result.push_back(Token(Token::Type::VERBATIM, term_));
    }

    return result;
  }

  libtextclassifier3::StatusOr<CharacterIterator> CalculateTokenStart()
      override {
    if (term_.empty()) {
      return absl_ports::AbortedError(
          "Could not calculate start of empty token.");
    }

    return CharacterIterator(term_);
  }

  libtextclassifier3::StatusOr<CharacterIterator> CalculateTokenEndExclusive()
      override {
    if (term_.empty()) {
      return absl_ports::AbortedError(
          "Could not calculate end of empty token.");
    }

    CharacterIterator token_end_iterator = GetTokenEndIterator();
    if (!token_end_iterator.is_valid()) {
      return absl_ports::AbortedError("Could not move to end of token.");
    }
    return token_end_iterator;
  }

  bool ResetToTokenStartingAfter(int32_t utf32_offset) override {
    // We can only reset to the sole verbatim token, so we must have a negative
    // offset for it to be considered the token after.
    if (utf32_offset < 0) {
      // Because we are now at the sole verbatim token, we should ensure we can
      // no longer advance past it.
      has_advanced_to_end_ = true;
      return true;
    }
    return false;
  }

  bool ResetToTokenEndingBefore(int32_t utf32_offset) override {
    // We can only reset to the sole verbatim token, so we must have an offset
    // after the end of the token for the reset to be valid. This means the
    // provided utf-32 offset must be equal to or greater than the utf-32 length
    // of the token.
    if (utf32_offset >= GetTokenEndIterator().utf32_index()) {
      has_advanced_to_end_ = true;
      return true;
    }
    return false;
  }

  bool ResetToStart() override {
    has_advanced_to_end_ = true;
    return true;
  }

 private:
  // Returns the end of the token, caching the result if it is not already
  // cached.
  //
  // RETURNS:
  //   - A valid CharacterIterator instance pointing to end of the token if
  //     succeeded to move to the end of the token. Also caches the end
  //     iterator.
  //   - An invalid character iterator if failed to move to the end of the
  //     token.
  CharacterIterator GetTokenEndIterator() const {
    if (cached_token_end_iterator_.is_valid()) {
      return cached_token_end_iterator_;
    }

    CharacterIterator token_end_iterator(term_);
    if (token_end_iterator.MoveToUtf8(term_.length())) {
      cached_token_end_iterator_ = std::move(token_end_iterator);
    }

    return cached_token_end_iterator_;
  }

  std::string_view term_;
  // Used to determine whether we have advanced on the sole verbatim token
  bool has_advanced_to_end_;

  mutable CharacterIterator
      cached_token_end_iterator_;  // initially invalid. Lazy update.
};

libtextclassifier3::StatusOr<std::unique_ptr<Tokenizer::Iterator>>
VerbatimTokenizer::Tokenize(std::string_view text) const {
  return std::make_unique<VerbatimTokenIterator>(text);
}

libtextclassifier3::StatusOr<std::vector<Token>> VerbatimTokenizer::TokenizeAll(
    std::string_view text) const {
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<Tokenizer::Iterator> iterator,
                         Tokenize(text));
  std::vector<Token> tokens;
  while (iterator->Advance()) {
    std::vector<Token> batch = iterator->GetTokens();
    tokens.insert(tokens.end(), batch.begin(), batch.end());
  }
  return tokens;
}

}  // namespace lib
}  // namespace icing
