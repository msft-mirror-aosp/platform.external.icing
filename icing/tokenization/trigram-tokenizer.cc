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

#include "icing/tokenization/trigram-tokenizer.h"

#include <cstdint>
#include <deque>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/tokenization/token.h"
#include "icing/tokenization/tokenizer.h"
#include "icing/util/character-iterator.h"
#include "icing/util/i18n-utils.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

class TrigramTokenizerIterator : public Tokenizer::Iterator {
 public:
  explicit TrigramTokenizerIterator(std::string_view text) : text_(text) {}

  bool Advance() override {
    if (utf8_char_itrs_.empty()) {
      // First call to Advance().
      return Initialize();
    }

    // Derive the next CharacterIterator pointing to the next utf8 character.
    CharacterIterator next_char_itr = utf8_char_itrs_.back();
    if (!next_char_itr.AdvanceToUtf32(next_char_itr.utf32_index() + 1)) {
      return false;
    }

    // We reach the end of the text, so there is no more trigram token.
    if (next_char_itr.utf8_index() >= text_.length()) {
      return false;
    }

    // Update the deque.
    utf8_char_itrs_.pop_front();
    utf8_char_itrs_.push_back(std::move(next_char_itr));

    return true;
  }

  std::vector<Token> GetTokens() const override {
    int start_byte_idx = utf8_char_itrs_.front().utf8_index();
    int end_byte_idx =
        utf8_char_itrs_.back().utf8_index() +
        i18n_utils::GetUtf8Length(utf8_char_itrs_.back().GetCurrentChar());

    return {Token(/*type_in=*/Token::Type::TRIGRAM,
                  /*text_in=*/text_.substr(start_byte_idx,
                                           end_byte_idx - start_byte_idx))};
  };

  // Returns a character iterator to the start of the current trigram token.
  //
  // REQUIRES: the preceding Advance() call returned true.
  libtextclassifier3::StatusOr<CharacterIterator> CalculateTokenStart()
      override {
    // The start index of the current trigram is utf8_char_itrs_.front().
    return utf8_char_itrs_.front();
  }

  // Returns a character iterator to right after the end of the current trigram
  // token.
  //
  // REQUIRES: the preceding Advance() call returned true.
  libtextclassifier3::StatusOr<CharacterIterator> CalculateTokenEndExclusive()
      override {
    // Derive the end exclusive CharacterIterator by advancing
    // utf8_char_itrs_.back() once.
    CharacterIterator end_exclusive_itr = utf8_char_itrs_.back();
    if (!end_exclusive_itr.AdvanceToUtf32(end_exclusive_itr.utf32_index() +
                                          1)) {
      return absl_ports::OutOfRangeError(
          "Unable to create the end exclusive iterator of the current trigram. "
          "This should not happen.");
    }
    return end_exclusive_itr;
  }

  // Resets the iterator to the leftmost trigram token starting after
  // utf32_offset. The iterator state is undefined if the reset fails.
  //
  // Note: utf32_offset can be negative, which means it will be reset to the
  // first trigram token.
  //
  // Returns: true if successfully resets.
  bool ResetToTokenStartingAfter(int32_t utf32_offset) override {
    // Initialize if necessary.
    if (utf8_char_itrs_.empty() && !Initialize()) {
      return false;
    }

    // Step 1: rewind iterators until utf8_char_itrs_.front() points to a utf8
    //         character that starts before or at utf32_offset.
    while (utf8_char_itrs_.front().utf32_index() > utf32_offset) {
      if (utf8_char_itrs_.front().utf32_index() == 0) {
        // Unable to rewind to the previous trigram token since we were already
        // at the first trigram token.
        //
        // It means utf32_offset is too small and all trigram tokens start after
        // it, so simply return true here since the first trigram token is the
        // leftmost trigram token starting after utf32_offset.
        return true;
      }

      // Attempt to rewind to the previous trigram token.
      // This should always succeed since we checked the condition above, unless
      // there is any invalid state or encoded bytes in text_.
      if (!Rewind()) {
        return false;
      }
    }

    // Step 2: advance iterators until utf8_char_itrs_.front() points to a utf8
    //         character that starts after utf32_offset.
    //
    // Note: if rewinded in step 1, then it is guaranteed to run only one
    //       iteration here.
    while (utf8_char_itrs_.front().utf32_index() <= utf32_offset) {
      if (!Advance()) {
        // It means we reached the end of the text but still cannot find a
        // trigram token starting after utf32_offset.
        return false;
      }
    }

    return true;
  }

  // Resets the iterator to the rightmost trigram token ending before
  // utf32_offset. The iterator state is undefined if the reset fails.
  //
  // Returns: true if successfully resets.
  bool ResetToTokenEndingBefore(int32_t utf32_offset) override {
    // Initialize if necessary.
    if (utf8_char_itrs_.empty() && !Initialize()) {
      return false;
    }

    // Step 1: advance iterators until utf8_char_itrs_.back() points to a utf8
    //         character that ends after or at utf32_offset.
    while (utf8_char_itrs_.back().utf32_index() < utf32_offset) {
      // Derive the next CharacterIterator pointing to the next utf8 character.
      CharacterIterator next_char_itr = utf8_char_itrs_.back();
      if (!next_char_itr.AdvanceToUtf32(next_char_itr.utf32_index() + 1)) {
        return false;
      }

      if (next_char_itr.utf8_index() >= text_.length()) {
        // The next character is out of the text, so we're already at the last
        // trigram token but it is still not ending after or at utf32_offset.
        //
        // It means utf32_offset is too large and all trigram tokens end before
        // it, so simply return true here since the last trigram token is the
        // rightmost trigram token ending before utf32_offset.
        return true;
      }

      // Update the deque.
      utf8_char_itrs_.pop_front();
      utf8_char_itrs_.push_back(std::move(next_char_itr));
    }

    // Step 2: rewind iterators until utf8_char_itrs_.back() points to a utf8
    //         character that ends before utf32_offset.
    //
    // Note: if advanced in step 1, then it is guaranteed to run only one
    //       iteration here.
    while (utf8_char_itrs_.back().utf32_index() >= utf32_offset) {
      if (!Rewind()) {
        // It means we reached the start of the text but still cannot find a
        // trigram token ending before utf32_offset.
        return false;
      }
    }

    return true;
  }

  bool ResetToStart() override {
    // Don't rewind it since it potentially calls RewindToUtf8() and rewinds
    // characters one by one, which is not necessary because we know we want to
    // start from the beginning of the text.
    utf8_char_itrs_.clear();
    return Initialize();
  }

 private:
  static constexpr int kNgramLength = 3;

  // Initializes utf8_char_itrs_ with 3 CharacterIterators pointing to 3 valid
  // utf8 characters of the first trigram.
  //
  // REQUIRES: utf8_char_itrs_.empty()
  bool Initialize() {
    bool result = true;

    CharacterIterator iterator(text_);
    for (int i = 0; i < kNgramLength; ++i) {
      if (iterator.utf8_index() >= text_.length()) {
        // It means the text is too short (with # of characters < 3) to form a
        // trigram.
        result = false;
        break;
      }

      utf8_char_itrs_.push_back(iterator);
      // Advance to the next character.
      if (!iterator.AdvanceToUtf32(iterator.utf32_index() + 1)) {
        result = false;
        break;
      }
    }

    if (!result) {
      // If failed to initialize, then clear the deque to avoid the next
      // Advance() call to generate invalid CharacterIterators.
      utf8_char_itrs_.clear();
    }
    return result;
  }

  // Rewinds utf8_char_itrs_ to point to the previous trigram token. Fails if
  // it is already at the first trigram token.
  //
  // REQUIRES: !utf8_char_itrs_.empty()
  bool Rewind() {
    // Derive the CharacterIterator pointing to the previous character.
    CharacterIterator prev_char_itr = utf8_char_itrs_.front();
    if (!prev_char_itr.RewindToUtf32(prev_char_itr.utf32_index() - 1)) {
      return false;
    }

    utf8_char_itrs_.pop_back();
    utf8_char_itrs_.push_front(std::move(prev_char_itr));
    return true;
  }

  std::string_view text_;  // Does not own the actual string data.

  // This deque maintains 3 CharacterIterators pointing to 3 utf8 characters of
  // the current trigram.
  // - Starting byte indices of the current trigram are
  //   utf8_char_itrs_[0].utf8_index(), utf8_char_itrs_[1].utf8_index(), and
  //   utf8_char_itrs_[2].utf8_index().
  // - When advancing or rewinding, we can derive the next or previous
  //   CharacterIterators by the back or front element of this deque.
  std::deque<CharacterIterator> utf8_char_itrs_;
};

libtextclassifier3::StatusOr<std::unique_ptr<Tokenizer::Iterator>>
TrigramTokenizer::Tokenize(std::string_view text) const {
  return std::make_unique<TrigramTokenizerIterator>(text);
}

libtextclassifier3::StatusOr<std::vector<Token>> TrigramTokenizer::TokenizeAll(
    std::string_view text) const {
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<Tokenizer::Iterator> iterator,
                         Tokenize(text));
  std::vector<Token> tokens;
  while (iterator->Advance()) {
    std::vector<Token> batch_tokens = iterator->GetTokens();
    tokens.insert(tokens.end(), batch_tokens.begin(), batch_tokens.end());
  }
  return tokens;
}

}  // namespace lib
}  // namespace icing
