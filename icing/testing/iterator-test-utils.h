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

#ifndef ICING_TESTING_ITERATOR_TEST_UTILS_H_
#define ICING_TESTING_ITERATOR_TEST_UTILS_H_

#include <string_view>
#include <vector>

#include "icing/tokenization/language-segmenter.h"

namespace icing {
namespace lib {

// Returns a vector containing all terms retrieved by Advancing on the iterator.
std::vector<std::string_view> GetAllTermsAdvance(
    LanguageSegmenter::Iterator* itr);

// Returns a vector containing all terms retrieved by calling ResetAfter with
// the UTF-32 position of the current term start to simulate Advancing on the
// iterator.
std::vector<std::string_view> GetAllTermsResetAfterUtf32(
    LanguageSegmenter::Iterator* itr);

// Returns a vector containing all terms retrieved by alternating calls to
// Advance and calls to ResetAfter with the UTF-32 position of the current term
// start to simulate Advancing.
std::vector<std::string_view> GetAllTermsAdvanceAndResetAfterUtf32(
    LanguageSegmenter::Iterator* itr);

// Returns a vector containing all terms retrieved by calling ResetBefore with
// the UTF-32 position of the current term start, starting at the end of the
// text. This vector should be in reverse order of GetAllTerms and missing the
// last term.
std::vector<std::string_view> GetAllTermsResetBeforeUtf32(
    LanguageSegmenter::Iterator* itr);

}  // namespace lib
}  // namespace icing

#endif  // ICING_TESTING_ITERATOR_TEST_UTILS_H_
