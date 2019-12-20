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

#include <sys/types.h>

#include <cctype>
#include <string>
#include <string_view>

#include "utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "unicode/umachine.h"
#include "unicode/unorm2.h"
#include "unicode/ustring.h"
#include "unicode/utf8.h"

namespace icing {
namespace lib {
namespace i18n_utils {

libtextclassifier3::StatusOr<std::string> Utf16ToUtf8(
    const std::u16string& utf16_string) {
  std::string utf8_string;
  // Allocates the maximum possible UTF8 string length:
  // 3 UTF-8 bytes per UTF16 code unit, plus one for the terminating NUL.
  //
  // NOTE: we need to call resize() but not reserve() because values can't be
  // set at positions after length().
  utf8_string.resize(utf16_string.length() * 3 + 1);

  int result_length = 0;
  UErrorCode status = U_ZERO_ERROR;
  u_strToUTF8(&utf8_string[0], utf8_string.length(), &result_length,
              utf16_string.data(), utf16_string.length(), &status);
  // Corrects the length
  utf8_string.resize(result_length);

  if (U_FAILURE(status)) {
    return absl_ports::InternalError("Failed to convert UTF16 string to UTF8");
  }
  return utf8_string;
}

libtextclassifier3::StatusOr<std::u16string> Utf8ToUtf16(
    std::string_view utf8_string) {
  std::u16string utf16_result;
  // The UTF16 string won't be longer than its UTF8 format
  //
  // NOTE: we need to call resize() but not reserve() because values can't be
  // set at positions after length().
  utf16_result.resize(utf8_string.length());

  int result_length = 0;
  UErrorCode status = U_ZERO_ERROR;
  u_strFromUTF8(&utf16_result[0], utf16_result.length(), &result_length,
                utf8_string.data(), utf8_string.length(), &status);
  // Corrects the length
  utf16_result.resize(result_length);

  if (U_FAILURE(status)) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Failed to convert UTF8 string '", utf8_string, "' to UTF16"));
  }
  return utf16_result;
}

UChar32 GetUChar32At(const char* data, int length, int position) {
  UChar32 uchar32;
  U8_NEXT_OR_FFFD(data, position, length, uchar32);
  return uchar32;
}

void SafeTruncateUtf8(std::string* str, int truncate_to_length) {
  if (str == nullptr || truncate_to_length >= str->length()) {
    return;
  }

  while (truncate_to_length > 0) {
    if (IsLeadUtf8Byte(str->at(truncate_to_length))) {
      str->resize(truncate_to_length);
      return;
    }
    truncate_to_length--;
  }

  // Truncates to an empty string
  str->resize(0);
}

bool IsAscii(char c) { return U8_IS_SINGLE((u_int8_t)c); }

bool IsAscii(UChar32 c) { return U8_LENGTH(c) == 1; }

int GetUtf8Length(UChar32 c) { return U8_LENGTH(c); }

bool IsLeadUtf8Byte(char c) { return IsAscii(c) || U8_IS_LEAD((u_int8_t)c); }

bool IsPunctuationAt(std::string_view input, int position, int* char_len_out) {
  if (IsAscii(input[position])) {
    if (char_len_out != nullptr) {
      *char_len_out = 1;
    }
    return std::ispunct(input[position]);
  }
  UChar32 c = GetUChar32At(input.data(), input.length(), position);
  if (char_len_out != nullptr) {
    *char_len_out = U8_LENGTH(c);
  }
  return u_ispunct(c);
}

bool DiacriticCharToAscii(const UNormalizer2* normalizer2, UChar32 uchar32_in,
                          char* char_out) {
  if (IsAscii(uchar32_in)) {
    // The Unicode character is within ASCII range
    if (char_out != nullptr) {
      *char_out = uchar32_in;
    }
    return true;
  }

  // Maximum number of pieces a Unicode character can be decomposed into.
  // TODO(samzheng) figure out if this number is proper.
  constexpr int kDecompositionBufferCapacity = 5;

  // A buffer used to store Unicode decomposition mappings of only one
  // character.
  UChar decomposition_buffer[kDecompositionBufferCapacity];

  // Decomposes the Unicode character, trying to get an ASCII char and some
  // diacritic chars.
  UErrorCode status = U_ZERO_ERROR;
  if (unorm2_getDecomposition(normalizer2, uchar32_in, &decomposition_buffer[0],
                              kDecompositionBufferCapacity, &status) > 0 &&
      !U_FAILURE(status) && i18n_utils::IsAscii(decomposition_buffer[0])) {
    if (char_out != nullptr) {
      *char_out = decomposition_buffer[0];
    }
    return true;
  }
  return false;
}

}  // namespace i18n_utils
}  // namespace lib
}  // namespace icing
