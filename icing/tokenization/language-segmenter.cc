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

#include "icing/tokenization/language-segmenter.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/util/i18n-utils.h"
#include "icing/util/status-macros.h"
#include "unicode/ubrk.h"
#include "unicode/uchar.h"
#include "unicode/umachine.h"

namespace icing {
namespace lib {

namespace {
constexpr char kASCIISpace = ' ';
constexpr char kLocaleAmericanEnglishComputer[] = "en_US_POSIX";
}  // namespace

LanguageSegmenter::LanguageSegmenter(std::string locale)
    : locale_(std::move(locale)) {}

libtextclassifier3::StatusOr<std::unique_ptr<LanguageSegmenter>>
LanguageSegmenter::Create(const std::string& locale) {
  // TODO(samzheng): Figure out if we want to verify locale strings and notify
  // users. Right now illegal locale strings will be ignored by ICU. ICU
  // components will be created with its default locale.\

  // Word connector rules for "en_US_POSIX" (American English (Computer)) are
  // different from other locales. E.g. "email.subject" will be split into 3
  // terms in "en_US_POSIX": "email", ".", and "subject", while it's just one
  // term in other locales. Our current LanguageSegmenter doesn't handle this
  // special rule, so we replace it with "en_US".
  if (locale == kLocaleAmericanEnglishComputer) {
    return std::unique_ptr<LanguageSegmenter>(new LanguageSegmenter(ULOC_US));
  }
  return std::unique_ptr<LanguageSegmenter>(new LanguageSegmenter(locale));
}

libtextclassifier3::StatusOr<std::unique_ptr<LanguageSegmenter::Iterator>>
LanguageSegmenter::Segment(const std::string_view text) const {
  return LanguageSegmenter::Iterator::Create(text, locale_);
}

libtextclassifier3::StatusOr<std::vector<std::string_view>>
LanguageSegmenter::GetAllTerms(const std::string_view text) const {
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<Iterator> iterator, Segment(text));
  std::vector<std::string_view> terms;
  while (iterator->Advance()) {
    terms.push_back(iterator->GetTerm());
  }
  return terms;
}

libtextclassifier3::StatusOr<std::unique_ptr<LanguageSegmenter::Iterator>>
LanguageSegmenter::Iterator::Create(std::string_view text,
                                    const std::string locale) {
  std::unique_ptr<Iterator> iterator(new Iterator(text, std::move(locale)));
  if (iterator->Initialize()) {
    return iterator;
  }
  return absl_ports::InternalError("Unable to create a term iterator");
}

LanguageSegmenter::Iterator::Iterator(const std::string_view text,
                                      const std::string&& locale)
    : break_iterator_(nullptr),
      text_(text),
      locale_(std::move(locale)),
      u_text_(UTEXT_INITIALIZER),
      term_start_index_(0),
      term_end_index_exclusive_(0) {}

LanguageSegmenter::Iterator::~Iterator() {
  ubrk_close(break_iterator_);
  utext_close(&u_text_);
}

bool LanguageSegmenter::Iterator::Initialize() {
  UErrorCode status = U_ZERO_ERROR;
  utext_openUTF8(&u_text_, text_.data(), /*length=*/-1, &status);
  break_iterator_ = ubrk_open(UBRK_WORD, locale_.c_str(), /*text=*/nullptr,
                              /*textLength=*/0, &status);
  ubrk_setUText(break_iterator_, &u_text_, &status);
  return !U_FAILURE(status);
}

bool LanguageSegmenter::Iterator::Advance() {
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
    return false;
  }

  // Rule 1: all ASCII terms will be returned.
  // We know it's a ASCII term by checking the first char.
  if (i18n_utils::IsAscii(text_[term_start_index_])) {
    return true;
  }

  UChar32 uchar32 =
      i18n_utils::GetUChar32At(text_.data(), text_.length(), term_start_index_);
  // Rule 2: for non-ASCII terms, only the alphabetic terms are returned.
  // We know it's an alphabetic term by checking the first unicode character.
  if (u_isUAlphabetic(uchar32)) {
    return true;
  } else {
    return Advance();
  }
}

std::string_view LanguageSegmenter::Iterator::GetTerm() const {
  if (text_[term_start_index_] == kASCIISpace) {
    // Rule 3: multiple continuous whitespaces are treated as one.
    return std::string_view(&text_[term_start_index_], 1);
  }
  return text_.substr(term_start_index_,
                      term_end_index_exclusive_ - term_start_index_);
}

libtextclassifier3::StatusOr<int32_t>
LanguageSegmenter::Iterator::ResetToTermStartingAfter(int32_t offset) {
  term_start_index_ = ubrk_following(break_iterator_, offset);
  if (term_start_index_ == UBRK_DONE) {
    return absl_ports::NotFoundError("");
  }
  term_end_index_exclusive_ = ubrk_next(break_iterator_);
  if (term_end_index_exclusive_ == UBRK_DONE) {
    return absl_ports::NotFoundError("");
  }
  return term_start_index_;
}

libtextclassifier3::Status
LanguageSegmenter::Iterator::ResetToTermStartingBefore(int32_t offset) {
  term_start_index_ = ubrk_preceding(break_iterator_, offset);
  if (term_start_index_ == UBRK_DONE) {
    return absl_ports::NotFoundError("");
  }
  term_end_index_exclusive_ = ubrk_next(break_iterator_);
  if (term_end_index_exclusive_ == UBRK_DONE) {
    return absl_ports::NotFoundError("");
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<int32_t>
LanguageSegmenter::Iterator::ResetToTermEndingBefore(int32_t offset) {
  ICING_RETURN_IF_ERROR(ResetToTermStartingBefore(offset));
  if (term_end_index_exclusive_ > offset) {
    // This term ends after offset. So we need to get the term just before this
    // one.
    ICING_RETURN_IF_ERROR(ResetToTermStartingBefore(term_start_index_));
  }
  return term_start_index_;
}

}  // namespace lib
}  // namespace icing
