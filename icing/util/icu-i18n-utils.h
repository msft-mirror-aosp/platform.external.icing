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

#ifndef ICING_UTIL_ICU_I18N_UTILS_H_
#define ICING_UTIL_ICU_I18N_UTILS_H_

#include <string>
#include <string_view>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "unicode/umachine.h"
#include "unicode/unorm2.h"

// Rely on this to transitively have access to U8_MAX_LENGTH, so all users can
// depend on either icu-i18n-utils or i18n-utils.
#include "unicode/utf8.h"

// Rely on this to transitively have access to ULOC_US, so all users can depend
// on either icu-i18n-utils or i18n-utils.
#include "unicode/uloc.h"

namespace icing {
namespace lib {

// Internationalization utils that use ICU methods under the hood. For use when
// the library is guaranteed to have access to ICU.
namespace icu_i18n_utils {

// An invalid value defined by Unicode.
static constexpr UChar32 kInvalidUChar32 = 0xFFFD;

// Converts a UTF16 string to a UTF8 string.
//
// Returns:
//   A UTF8 string on success
//   INTERNAL_ERROR on any failures
libtextclassifier3::StatusOr<std::string> Utf16ToUtf8(
    const std::u16string& utf16_string);

// Converts a UTF8 string to a UTF16 string.
//
// Returns:
//   A UTF16 string on success
//   INTERNAL_ERROR on any failures
libtextclassifier3::StatusOr<std::u16string> Utf8ToUtf16(
    std::string_view utf8_string);

// Returns the Unicode char at the given position. If anything wrong happens, an
// invalid value 0xFFFD is returned.
UChar32 GetUChar32At(const char* data, int length, int position);

// Safely truncates a UTF8 string so that multi-byte UTF8 characters are not cut
// in the middle. The string will be truncated in place.
void SafeTruncateUtf8(std::string* str, int truncate_to_length);

// Checks if the single char is within ASCII range.
bool IsAscii(char c);

// Checks if the Unicode char is within ASCII range.
bool IsAscii(UChar32 c);

// Returns how many code units (bytes) are used for the UTF-8 encoding of this
// Unicode character. Returns 0 if not valid.
int GetUtf8Length(UChar32 c);

// Checks if the single char is the first byte of a UTF8 character, note
// that a single ASCII char is also considered a lead byte.
bool IsLeadUtf8Byte(char c);

// Checks if the character at position is punctuation. Assigns the length of the
// character at position to *char_len_out if the character at position is valid
// punctuation and char_len_out is not null.
bool IsPunctuationAt(std::string_view input, int position,
                     int* char_len_out = nullptr);

// Checks if the character at position is a whitespace.
bool IsWhitespaceAt(std::string_view input, int position);

// Transforms a Unicode character with diacritics to its counterpart in ASCII
// range. E.g. "ü" -> "u". Result will be set to char_out. Returns true if
// the transformation is successful.
//
// NOTE: According to our convention this function should have returned
// StatusOr<char>. However, this function is performance-sensitive because is
// could be called on every Latin character in normalization, so we make it
// return a bool here to save a bit more time and memory.
bool DiacriticCharToAscii(const UNormalizer2* normalizer2, UChar32 uchar32_in,
                          char* char_out);

}  // namespace icu_i18n_utils
}  // namespace lib
}  // namespace icing

#endif  // ICING_UTIL_ICU_I18N_UTILS_H_
