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

#include "icing/tokenization/simple/space-language-segmenter.h"

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
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {
constexpr char kASCIISpace = ' ';
}  // namespace

class SpaceLanguageSegmenterIterator : public LanguageSegmenter::Iterator {
 public:
  SpaceLanguageSegmenterIterator(std::string_view text)
      : text_(text), term_start_index_(0), term_end_index_exclusive_(0) {}

  // Advances to the next term. Returns false if it has reached the end.
  bool Advance() override {
    if (term_end_index_exclusive_ >= text_.size() ||
        term_start_index_ >= text_.size()) {
      // Reached the end
      return false;
    }

    // Next term starts where we left off.
    term_start_index_ = term_end_index_exclusive_;

    // We know a term is at least one length, so we can +1 first.
    term_end_index_exclusive_++;

    // We alternate terms between space and non-space. Figure out what type of
    // term we're currently on so we know how to stop.
    bool is_space = text_[term_start_index_] == kASCIISpace;

    while (term_end_index_exclusive_ < text_.size()) {
      bool end_is_space = text_[term_end_index_exclusive_] == kASCIISpace;
      if (is_space != end_is_space) {
        // We finally see a different type of character, reached the end.
        break;
      }
      // We're still seeing the same types of characters (saw a space and
      // still seeing spaces, or saw a non-space and still seeing non-spaces).
      // Haven't reached the next term yet, keep advancing.
      term_end_index_exclusive_++;
    }

    return true;
  }

  // Returns the current term. It can be called only when Advance() returns
  // true.
  std::string_view GetTerm() const override {
    if (text_[term_start_index_] == kASCIISpace) {
      // Rule: multiple continuous whitespaces are treated as one.
      return std::string_view(&text_[term_start_index_], 1);
    }
    return text_.substr(term_start_index_,
                        term_end_index_exclusive_ - term_start_index_);
  }

  libtextclassifier3::StatusOr<int32_t> ResetToTermStartingAfter(
      int32_t offset) override {
    if (offset < 0) {
      // Start over from the beginning to find the first term.
      term_start_index_ = 0;
      term_end_index_exclusive_ = 0;
    } else {
      // Offset points to a term right now. Advance to get past the current
      // term.
      term_end_index_exclusive_ = offset;
      if (!Advance()) {
        return absl_ports::NotFoundError(IcingStringUtil::StringPrintf(
            "No term found in '%s' that starts after offset %d",
            std::string(text_).c_str(), offset));
      }
    }

    // Advance again so we can point to the next term.
    if (!Advance()) {
      return absl_ports::NotFoundError(IcingStringUtil::StringPrintf(
          "No term found in '%s' that starts after offset %d",
          std::string(text_).c_str(), offset));
    }

    return term_start_index_;
  }

  libtextclassifier3::StatusOr<int32_t> ResetToTermEndingBefore(
      int32_t offset) override {
    if (offset <= 0 || offset > text_.size()) {
      return absl_ports::NotFoundError(IcingStringUtil::StringPrintf(
          "No term found in '%s' that ends before offset %d",
          std::string(text_).c_str(), offset));
    }

    if (offset == text_.size()) {
      // Special-case if the offset is the text length, this is the last term in
      // the text, which is also considered to be "ending before" the offset.
      term_end_index_exclusive_ = offset;
      ICING_ASSIGN_OR_RETURN(term_start_index_, GetTermStartingBefore(offset));
      return term_start_index_;
    }

    // Otherwise, this is just the end of the previous term and we still need to
    // find the start of the previous term.
    ICING_ASSIGN_OR_RETURN(term_end_index_exclusive_,
                           GetTermStartingBefore(offset));

    if (term_end_index_exclusive_ == 0) {
      // The current term starts at the beginning of the underlying text_.
      // There is no term before this.
      return absl_ports::NotFoundError(IcingStringUtil::StringPrintf(
          "No term found in '%s' that ends before offset %d",
          std::string(text_).c_str(), offset));
    }

    // Reset ourselves to find the term before the end.
    ICING_ASSIGN_OR_RETURN(
        term_start_index_,
        GetTermStartingBefore(term_end_index_exclusive_ - 1));
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
  // Return the start offset of the term starting right before the given offset.
  libtextclassifier3::StatusOr<int32_t> GetTermStartingBefore(int32_t offset) {
    bool is_space = text_[offset] == kASCIISpace;

    // Special-case that if offset was the text length, then we're already at
    // the "end" of our current term.
    if (offset == text_.size()) {
      is_space = text_[--offset] == kASCIISpace;
    }

    // While it's the same type of character (space vs non-space), we're in the
    // same term. So keep iterating backwards until we see a change.
    while (offset >= 0 && (text_[offset] == kASCIISpace) == is_space) {
      --offset;
    }

    // +1 is because offset was off-by-one to exit the while-loop.
    return ++offset;
  }

  // Text to be segmented
  std::string_view text_;

  // The start and end indices are used to track the positions of current
  // term.
  int term_start_index_;
  int term_end_index_exclusive_;
};

libtextclassifier3::StatusOr<std::unique_ptr<LanguageSegmenter::Iterator>>
SpaceLanguageSegmenter::Segment(const std::string_view text) const {
  return std::make_unique<SpaceLanguageSegmenterIterator>(text);
}

libtextclassifier3::StatusOr<std::vector<std::string_view>>
SpaceLanguageSegmenter::GetAllTerms(const std::string_view text) const {
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
