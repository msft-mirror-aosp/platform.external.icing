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

#ifndef ICING_UTIL_I18N_UTILS_H_
#define ICING_UTIL_I18N_UTILS_H_

#include <string>
#include <string_view>

namespace icing {
namespace lib {

// These are included for uses when we don't have access to ICU.
//
// Defined in ICU;
// https://unicode-org.github.io/icu-docs/apidoc/released/icu4c/umachine_8h.html#a09fff5c3b5a5b015324dc3ec3cf92809
using UChar32 = int32_t;

// Defined in ICU:
// https://unicode-org.github.io/icu-docs/apidoc/released/icu4c/utf8_8h.html#aa2298b48749d9f45772c8f5a6885464a
#define U8_MAX_LENGTH 4

// Defined in ICU:
// https://unicode-org.github.io/icu-docs/apidoc/released/icu4c/uloc_8h.html#aa55404d3c725af4e05e65e5b40a6e13d
#define ULOC_US "en_US"

// Internationalization utils that use standard utilities or custom code. Does
// not require any special dependencies, i.e. for use when the library is NOT
// guaranteed to have access to ICU.
//
// Note: This does not handle Unicode.
//
// TODO(cassiewang): Figure out if we want to keep this file as a non-ICU
// solution long-term, or if we'll do something along the lines of reverse-jni,
// etc.
namespace i18n_utils {

// An invalid value defined by Unicode.
static constexpr UChar32 kInvalidUChar32 = 0xFFFD;

// Returns the char at the given position.
UChar32 GetUChar32At(const char* data, int length, int position);

// Checks if the single char is within ASCII range.
bool IsAscii(char c);

// Checks if the character at position is punctuation. Assigns the length of the
// character at position to *char_len_out if the character at position is valid
// punctuation and char_len_out is not null.
bool IsPunctuationAt(std::string_view input, int position,
                     int* char_len_out = nullptr);

// Checks if the character at position is a whitespace.
bool IsWhitespaceAt(std::string_view input, int position);

}  // namespace i18n_utils
}  // namespace lib
}  // namespace icing

#endif  // ICING_UTIL_I18N_UTILS_H_
