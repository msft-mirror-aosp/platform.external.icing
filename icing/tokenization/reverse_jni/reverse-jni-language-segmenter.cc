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

#include "icing/tokenization/reverse_jni/reverse-jni-language-segmenter.h"

#include <cctype>
#include <memory>
#include <string>
#include <string_view>

#include "icing/jni/reverse-jni-break-iterator.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/util/character-iterator.h"
#include "icing/util/i18n-utils.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

class ReverseJniLanguageSegmenterIterator : public LanguageSegmenter::Iterator {
 public:
  explicit ReverseJniLanguageSegmenterIterator(
      std::string_view text,
      std::unique_ptr<ReverseJniBreakIterator> break_iterator)
      : break_iterator_(std::move(break_iterator)),
        text_(text),
        term_start_(text),
        term_end_exclusive_(text) {}

  // Advances to the next term. Returns false if it has reached the end.
  bool Advance() override {
    // Prerequisite check
    if (term_end_exclusive_.utf16_index() == ReverseJniBreakIterator::kDone) {
      return false;
    }

    if (term_end_exclusive_.utf16_index() == 0) {
      int first = break_iterator_->First();
      if (!term_start_.AdvanceToUtf16(first)) {
        // First is guaranteed to succeed and return a position within bonds. So
        // the only possible failure could be an invalid sequence. Mark as DONE
        // and return.
        MarkAsDone();
        return false;
      }
    } else {
      term_start_ = term_end_exclusive_;
    }

    int next_utf16_index_exclusive = break_iterator_->Next();
    // Reached the end
    if (next_utf16_index_exclusive == ReverseJniBreakIterator::kDone) {
      MarkAsDone();
      return false;
    }
    if (!term_end_exclusive_.AdvanceToUtf16(next_utf16_index_exclusive)) {
      // next_utf16_index_exclusive is guaranteed to be within bonds thanks to
      // the check for kDone above. So the only possible failure could be an
      // invalid sequence. Mark as DONE and return.
      MarkAsDone();
      return false;
    }

    // Check if the current term is valid. We consider any term valid if its
    // first character is valid. If it's not valid, then we need to advance to
    // the next term.
    if (IsValidTerm()) {
      return true;
    }
    return Advance();
  }

  // Returns the current term. It can be called only when Advance() returns
  // true.
  std::string_view GetTerm() const override {
    int term_length =
        term_end_exclusive_.utf8_index() - term_start_.utf8_index();
    if (term_length > 0 && std::isspace(text_[term_start_.utf8_index()])) {
      // Rule 3: multiple continuous whitespaces are treated as one.
      term_length = 1;
    }
    return text_.substr(term_start_.utf8_index(), term_length);
  }

  // Resets the iterator to point to the first term that starts after offset.
  // GetTerm will now return that term.
  //
  // Returns:
  //   On success, the starting position of the first term that starts after
  //   offset.
  //   NOT_FOUND if an error occurred or there are no terms that start after
  //   offset.
  //   INVALID_ARGUMENT if offset is out of bounds for the provided text.
  //   ABORTED if an invalid unicode character is encountered while
  //   traversing the text.
  libtextclassifier3::StatusOr<int32_t> ResetToTermStartingAfter(
      int32_t offset) override {
    if (offset < 0 || offset >= text_.length()) {
      return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
          "Illegal offset provided! Offset %d is not within bounds of string "
          "of length %zu",
          offset, text_.length()));
    }
    if (term_end_exclusive_.utf16_index() == ReverseJniBreakIterator::kDone) {
      // We're done. Need to start from the beginning if we're going to reset
      // properly.
      term_start_ = CharacterIterator(text_);
      term_end_exclusive_ = CharacterIterator(text_);
    }

    // 1. Find the unicode character that contains the byte at offset.
    CharacterIterator offset_iterator = term_end_exclusive_;
    bool success = (offset > offset_iterator.utf8_index())
                       ? offset_iterator.AdvanceToUtf8(offset)
                       : offset_iterator.RewindToUtf8(offset);
    if (!success) {
      // Offset is guaranteed to be within bounds thanks to the check above. So
      // the only possible failure could be an invalid sequence. Mark as DONE
      // and return.
      MarkAsDone();
      return absl_ports::AbortedError("Encountered invalid UTF sequence!");
    }

    // 2. We've got the unicode character containing byte offset. Now, we need
    // to point to the segment that starts after this character.
    int following_utf16_index =
        break_iterator_->Following(offset_iterator.utf16_index());
    if (following_utf16_index == ReverseJniBreakIterator::kDone) {
      MarkAsDone();
      return absl_ports::NotFoundError(IcingStringUtil::StringPrintf(
          "No segments begin after provided offset %d.", offset));
    }
    if (!offset_iterator.AdvanceToUtf16(following_utf16_index)) {
      // following_utf16_index is guaranteed to be within bonds thanks to the
      // check for kDone above. So the only possible failure could be an invalid
      // sequence. Mark as DONE and return.
      MarkAsDone();
      return absl_ports::AbortedError("Encountered invalid UTF sequence!");
    }
    term_end_exclusive_ = offset_iterator;

    // 3. The term_end_exclusive_ points to the term that we want to return. We
    // need to Advance so that term_start_ will now point to this term.
    if (!Advance()) {
      return absl_ports::NotFoundError(IcingStringUtil::StringPrintf(
          "No segments begin after provided offset %d.", offset));
    }
    return term_start_.utf8_index();
  }

  // Resets the iterator to point to the first term that ends before offset.
  // GetTerm will now return that term.
  //
  // Returns:
  //   On success, the starting position of the first term that ends before
  //   offset.
  //   NOT_FOUND if an error occurred or there are no terms that end before
  //   offset.
  //   INVALID_ARGUMENT if offset is out of bounds for the provided text.
  //   ABORTED if an invalid unicode character is encountered while
  //   traversing the text.
  libtextclassifier3::StatusOr<int32_t> ResetToTermEndingBefore(
      int32_t offset) override {
    if (offset < 0 || offset >= text_.length()) {
      return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
          "Illegal offset provided! Offset %d is not within bounds of string "
          "of length %zu",
          offset, text_.length()));
    }
    if (term_end_exclusive_.utf16_index() == ReverseJniBreakIterator::kDone) {
      // We're done. Need to start from the beginning if we're going to reset
      // properly.
      term_start_ = CharacterIterator(text_);
      term_end_exclusive_ = CharacterIterator(text_);
    }

    // 1. Find the unicode character that contains the byte at offset.
    CharacterIterator offset_iterator = term_end_exclusive_;
    bool success = (offset > offset_iterator.utf8_index())
                       ? offset_iterator.AdvanceToUtf8(offset)
                       : offset_iterator.RewindToUtf8(offset);
    if (!success) {
      // Offset is guaranteed to be within bounds thanks to the check above. So
      // the only possible failure could be an invalid sequence. Mark as DONE
      // and return.
      MarkAsDone();
      return absl_ports::AbortedError(
          "Could not retrieve valid utf8 character!");
    }

    // 2. We've got the unicode character containing byte offset. Now, we need
    // to point to the segment that starts before this character.
    int starting_utf16_index =
        break_iterator_->Preceding(offset_iterator.utf16_index());
    if (starting_utf16_index == ReverseJniBreakIterator::kDone) {
      // Rewind the end indices.
      MarkAsDone();
      return absl_ports::NotFoundError(IcingStringUtil::StringPrintf(
          "No segments end before provided offset %d.", offset));
    }
    if (!offset_iterator.RewindToUtf16(starting_utf16_index)) {
      // starting_utf16_index is guaranteed to be within bonds thanks to the
      // check for kDone above. So the only possible failure could be an invalid
      // sequence. Mark as DONE and return.
      MarkAsDone();
      return absl_ports::AbortedError("Encountered invalid UTF sequence!");
    }
    term_start_ = offset_iterator;

    // 3. We've correctly set the start index and the iterator currently points
    // to that position. Now we need to find the correct end position and
    // advance the iterator to that position.
    int end_utf16_index = break_iterator_->Next();
    term_end_exclusive_ = term_start_;
    term_end_exclusive_.AdvanceToUtf16(end_utf16_index);

    // 4. The start and end indices point to a segment, but we need to ensure
    // that this segment is 1) valid and 2) ends before offset. Otherwise, we'll
    // need a segment prior to this one.
    if (term_end_exclusive_.utf8_index() > offset || !IsValidTerm()) {
      return ResetToTermEndingBefore(term_start_.utf8_index());
    }
    return term_start_.utf8_index();
  }

  libtextclassifier3::StatusOr<int32_t> ResetToStart() override {
    term_start_ = CharacterIterator(text_);
    term_end_exclusive_ = CharacterIterator(text_);
    if (!Advance()) {
      return absl_ports::NotFoundError("");
    }
    return term_start_.utf8_index();
  }

 private:
  // Ensures that all members are consistent with the 'Done' state.
  // In the 'Done' state, both term_start_.utf8_index() and
  // term_end_exclusive_.utf8_index() will point to the same character, causing
  // GetTerm() to return an empty string and term_start_.utf16_index() and
  // term_end_exclusive_.utf16_index() will be marked with the kDone value.
  // break_iterator_ may be in any state.
  void MarkAsDone() {
    term_start_ =
        CharacterIterator(text_, /*utf8_index=*/0,
                          /*utf16_index=*/ReverseJniBreakIterator::kDone);
    term_end_exclusive_ =
        CharacterIterator(text_, /*utf8_index=*/0,
                          /*utf16_index=*/ReverseJniBreakIterator::kDone);
  }

  bool IsValidTerm() const {
    // Rule 1: all ASCII terms will be returned.
    // We know it's a ASCII term by checking the first char.
    if (i18n_utils::IsAscii(text_[term_start_.utf8_index()])) {
      return true;
    }

    // Rule 2: for non-ASCII terms, only the alphabetic terms are returned.
    // We know it's an alphabetic term by checking the first unicode character.
    if (i18n_utils::IsAlphabeticAt(text_, term_start_.utf8_index())) {
      return true;
    }
    return false;
  }

  // All of ReverseJniBreakIterator's functions return UTF-16 boundaries. So
  // this class needs to maintain state to convert between UTF-16 and UTF-8.
  std::unique_ptr<ReverseJniBreakIterator> break_iterator_;

  // Text to be segmented
  std::string_view text_;

  // Index used to track the start position of current term.
  CharacterIterator term_start_;

  // Index used to track the end position of current term.
  CharacterIterator term_end_exclusive_;
};

libtextclassifier3::StatusOr<std::unique_ptr<LanguageSegmenter::Iterator>>
ReverseJniLanguageSegmenter::Segment(const std::string_view text) const {
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<ReverseJniBreakIterator> break_iterator,
      ReverseJniBreakIterator::Create(jni_cache_, text, locale_));
  return std::make_unique<ReverseJniLanguageSegmenterIterator>(
      text, std::move(break_iterator));
}

libtextclassifier3::StatusOr<std::vector<std::string_view>>
ReverseJniLanguageSegmenter::GetAllTerms(const std::string_view text) const {
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<LanguageSegmenter::Iterator> iterator,
                         Segment(text));
  std::vector<std::string_view> terms;
  while (iterator->Advance()) {
    terms.push_back(iterator->GetTerm());
  }
  return terms;
}

}  // namespace lib
}  // namespace icing
