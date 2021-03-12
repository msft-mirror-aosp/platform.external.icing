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

#include "icing/tokenization/icu/icu-language-segmenter.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/util/i18n-utils.h"
#include "icing/util/status-macros.h"
#include "unicode/ubrk.h"
#include "unicode/uchar.h"
#include "unicode/umachine.h"

namespace icing {
namespace lib {

namespace {
constexpr char kASCIISpace = ' ';
}  // namespace

class IcuLanguageSegmenterIterator : public LanguageSegmenter::Iterator {
 public:
  // Factory function to create a segment iterator based on the given locale.
  //
  // Returns:
  //   An iterator on success
  //   INTERNAL_ERROR if unable to create
  static libtextclassifier3::StatusOr<
      std::unique_ptr<LanguageSegmenter::Iterator>>
  Create(std::string_view text, std::string_view locale) {
    std::unique_ptr<IcuLanguageSegmenterIterator> iterator(
        new IcuLanguageSegmenterIterator(text, locale));
    if (iterator->Initialize()) {
      return iterator;
    }
    return absl_ports::InternalError("Unable to create a term iterator");
  }

  ~IcuLanguageSegmenterIterator() {
    ubrk_close(break_iterator_);
    utext_close(&u_text_);
  }

  // Advances to the next term. Returns false if it has reached the end.
  bool Advance() override {
    // Prerequisite check
    if (term_end_index_exclusive_ == UBRK_DONE) {
      return false;
    }

    if (term_end_index_exclusive_ == 0) {
      // First Advance() call
      term_start_index_ = ubrk_first(break_iterator_);
    } else {
      term_start_index_ = term_end_index_exclusive_;
    }
    term_end_index_exclusive_ = ubrk_next(break_iterator_);

    // Reached the end
    if (term_end_index_exclusive_ == UBRK_DONE) {
      MarkAsDone();
      return false;
    }

    if (!IsValidSegment()) {
      return Advance();
    }
    return true;
  }

  // Returns the current term. It can be called only when Advance() returns
  // true.
  std::string_view GetTerm() const override {
    int term_length = term_end_index_exclusive_ - term_start_index_;
    if (term_end_index_exclusive_ == UBRK_DONE) {
      term_length = 0;
    } else if (text_[term_start_index_] == kASCIISpace) {
      // Rule 3: multiple continuous whitespaces are treated as one.
      term_length = 1;
    }
    return text_.substr(term_start_index_, term_length);
  }

  libtextclassifier3::StatusOr<int32_t> ResetToTermStartingAfter(
      int32_t offset) override {
    if (offset < 0 || offset >= text_.length()) {
      return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
          "Illegal offset provided! Offset %d is not within bounds of string "
          "of length %zu",
          offset, text_.length()));
    }
    term_start_index_ = ubrk_following(break_iterator_, offset);
    if (term_start_index_ == UBRK_DONE) {
      MarkAsDone();
      return absl_ports::NotFoundError(IcingStringUtil::StringPrintf(
          "No segments begin after provided offset %d.", offset));
    }
    term_end_index_exclusive_ = ubrk_next(break_iterator_);
    if (term_end_index_exclusive_ == UBRK_DONE) {
      MarkAsDone();
      return absl_ports::NotFoundError(IcingStringUtil::StringPrintf(
          "No segments begin after provided offset %d.", offset));
    }
    if (!IsValidSegment()) {
      if (!Advance()) {
        return absl_ports::NotFoundError(IcingStringUtil::StringPrintf(
            "No segments begin after provided offset %d.", offset));
      }
    }
    return term_start_index_;
  }

  libtextclassifier3::StatusOr<int32_t> ResetToTermEndingBefore(
      int32_t offset) override {
    if (offset < 0 || offset >= text_.length()) {
      return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
          "Illegal offset provided! Offset %d is not within bounds of string "
          "of length %zu",
          offset, text_.length()));
    }
    ICING_RETURN_IF_ERROR(ResetToTermStartingBefore(offset));
    if (term_end_index_exclusive_ > offset) {
      // This term ends after offset. So we need to get the term just before
      // this one.
      ICING_RETURN_IF_ERROR(ResetToTermStartingBefore(term_start_index_));
    }
    return term_start_index_;
  }

  libtextclassifier3::StatusOr<int32_t> ResetToStart() override {
    term_start_index_ = 0;
    term_end_index_exclusive_ = 0;
    if (!Advance()) {
      return absl_ports::NotFoundError("");
    }
    return term_start_index_;
  }

 private:
  explicit IcuLanguageSegmenterIterator(std::string_view text,
                                        std::string_view locale)
      : break_iterator_(nullptr),
        text_(text),
        locale_(locale),
        u_text_(UTEXT_INITIALIZER),
        term_start_index_(0),
        term_end_index_exclusive_(0) {}

  // Returns true on success
  bool Initialize() {
    UErrorCode status = U_ZERO_ERROR;
    utext_openUTF8(&u_text_, text_.data(), text_.length(), &status);
    break_iterator_ = ubrk_open(UBRK_WORD, locale_.data(), /*text=*/nullptr,
                                /*textLength=*/0, &status);
    ubrk_setUText(break_iterator_, &u_text_, &status);
    return !U_FAILURE(status);
  }

  libtextclassifier3::Status ResetToTermStartingBefore(int32_t offset) {
    term_start_index_ = ubrk_preceding(break_iterator_, offset);
    if (term_start_index_ == UBRK_DONE) {
      MarkAsDone();
      return absl_ports::NotFoundError("");
    }
    term_end_index_exclusive_ = ubrk_next(break_iterator_);
    if (term_end_index_exclusive_ == UBRK_DONE) {
      MarkAsDone();
      return absl_ports::NotFoundError("");
    }
    return libtextclassifier3::Status::OK;
  }

  // Ensures that all members are consistent with the 'Done' state.
  // In the 'Done' state, term_start_index_ will point to the first character
  // and term_end_index_exclusive_ will be marked with the kDone value.
  // break_iterator_ may be in any state.
  void MarkAsDone() {
    term_end_index_exclusive_ = UBRK_DONE;
    term_start_index_ = 0;
  }

  bool IsValidSegment() const {
    // Rule 1: all ASCII terms will be returned.
    // We know it's a ASCII term by checking the first char.
    if (i18n_utils::IsAscii(text_[term_start_index_])) {
      return true;
    }

    UChar32 uchar32 = i18n_utils::GetUChar32At(text_.data(), text_.length(),
                                               term_start_index_);
    // Rule 2: for non-ASCII terms, only the alphabetic terms are returned.
    // We know it's an alphabetic term by checking the first unicode character.
    if (u_isUAlphabetic(uchar32)) {
      return true;
    }
    return false;
  }

  // The underlying class that does the segmentation, ubrk_close() must be
  // called after using.
  UBreakIterator* break_iterator_;

  // Text to be segmented
  std::string_view text_;

  // Locale of the input text, used to help segment more accurately. If a
  // wrong locale is set, text could probably still be segmented correctly
  // because the default break iterator behavior is used for most locales.
  std::string_view locale_;

  // A thin wrapper around the input UTF8 text, needed by break_iterator_.
  // utext_close() must be called after using.
  UText u_text_;

  // The start and end indices are used to track the positions of current
  // term.
  int term_start_index_;
  int term_end_index_exclusive_;
};

IcuLanguageSegmenter::IcuLanguageSegmenter(std::string locale)
    : locale_(std::move(locale)) {}

libtextclassifier3::StatusOr<std::unique_ptr<LanguageSegmenter::Iterator>>
IcuLanguageSegmenter::Segment(const std::string_view text) const {
  return IcuLanguageSegmenterIterator::Create(text, locale_);
}

libtextclassifier3::StatusOr<std::vector<std::string_view>>
IcuLanguageSegmenter::GetAllTerms(const std::string_view text) const {
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
