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

#include "icing/util/i18n-utils.h"

#include <cctype>
#include <string_view>

namespace icing {
namespace lib {
namespace i18n_utils {

namespace {

// All ASCII punctuation that's also in a Unicode Punctuation category
// (https://www.fileformat.info/info/unicode/category/index.htm). The set of
// characters that are regarded as punctuation is not the same for std::ispunct
// and u_ispunct.
const std::string ascii_icu_punctuation = "!\"#%&'*,./:;?@\\_-([{}])";

}  // namespace

UChar32 GetUChar32At(const char* data, int length, int position) {
  // We don't handle Unicode, i.e. anything more than 1 byte.
  return data[position];
}

bool IsAscii(char c) { return (c & 0x80) == 0; }

bool IsPunctuationAt(std::string_view input, int position, int* char_len_out) {
  if (IsAscii(input[position])) {
    if (char_len_out != nullptr) {
      *char_len_out = 1;
    }
    return ascii_icu_punctuation.find(input[position]) != std::string::npos;
  }

  // If it's not ASCII, we can't process Unicode so we don't know.
  return false;
}

bool IsWhitespaceAt(std::string_view input, int position) {
  if (IsAscii(input[position])) {
    return std::isspace(input[position]);
  }

  // If it's not ASCII, we can't process Unicode so we don't know.
  return false;
}

}  // namespace i18n_utils
}  // namespace lib
}  // namespace icing
