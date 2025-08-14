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

#include "icing/testing/iterator-test-utils.h"

#include <string_view>
#include <vector>

#include "icing/tokenization/language-segmenter.h"
#include "icing/util/character-iterator.h"

namespace icing {
namespace lib {

// Returns a vector containing all terms retrieved by Advancing on the iterator.
std::vector<std::string_view> GetAllTermsAdvance(
    LanguageSegmenter::Iterator* itr) {
  std::vector<std::string_view> terms;
  while (itr->Advance()) {
    terms.push_back(itr->GetTerm());
  }
  return terms;
}

// Returns a vector containing all terms retrieved by calling ResetAfter with
// the UTF-32 position of the current term start to simulate Advancing on the
// iterator.
std::vector<std::string_view> GetAllTermsResetAfterUtf32(
    LanguageSegmenter::Iterator* itr) {
  std::vector<std::string_view> terms;
  // Calling ResetToTermStartingAfterUtf32 with -1 should get the first term in
  // the sequence.
  bool is_ok = itr->ResetToTermStartingAfterUtf32(-1).ok();
  while (is_ok) {
    terms.push_back(itr->GetTerm());
    // Calling ResetToTermStartingAfterUtf32 with the current position should
    // get the very next term in the sequence.
    CharacterIterator char_itr = itr->CalculateTermStart().ValueOrDie();
    is_ok = itr->ResetToTermStartingAfterUtf32(char_itr.utf32_index()).ok();
  }
  return terms;
}

// Returns a vector containing all terms retrieved by alternating calls to
// Advance and calls to ResetAfter with the UTF-32 position of the current term
// start to simulate Advancing.
std::vector<std::string_view> GetAllTermsAdvanceAndResetAfterUtf32(
    LanguageSegmenter::Iterator* itr) {
  std::vector<std::string_view> terms;
  bool is_ok = itr->Advance();
  while (is_ok) {
    terms.push_back(itr->GetTerm());
    // Alternate between using Advance and ResetToTermAfter.
    if (terms.size() % 2 == 0) {
      is_ok = itr->Advance();
    } else {
      // Calling ResetToTermStartingAfterUtf32 with the current position should
      // get the very next term in the sequence.
      CharacterIterator char_itr = itr->CalculateTermStart().ValueOrDie();
      is_ok = itr->ResetToTermStartingAfterUtf32(char_itr.utf32_index()).ok();
    }
  }
  return terms;
}

// Returns a vector containing all terms retrieved by calling ResetBefore with
// the UTF-32 position of the current term start, starting at the end of the
// text. This vector should be in reverse order of GetAllTerms and missing the
// last term.
std::vector<std::string_view> GetAllTermsResetBeforeUtf32(
    LanguageSegmenter::Iterator* itr) {
  std::vector<std::string_view> terms;
  bool is_ok = itr->ResetToTermEndingBeforeUtf32(1000).ok();
  while (is_ok) {
    terms.push_back(itr->GetTerm());
    // Calling ResetToTermEndingBeforeUtf32 with the current position should get
    // the previous term in the sequence.
    CharacterIterator char_itr = itr->CalculateTermStart().ValueOrDie();
    is_ok = itr->ResetToTermEndingBeforeUtf32(char_itr.utf32_index()).ok();
  }
  return terms;
}

}  // namespace lib
}  // namespace icing
