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

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "unicode/uchar.h"

namespace icing {
namespace lib {
namespace {

using ::testing::Eq;

TEST(IcuI18nUtilsTest, IsPunctuationAtSameAsIcuIsPunct) {
  // Iterate through ASCII values
  for (int i = 0; i <= 127; ++i) {
    char ascii = i;

    std::string ascii_string = "";
    ascii_string.push_back(ascii);

    EXPECT_EQ(i18n_utils::IsPunctuationAt(ascii_string, /*position=*/0),

              u_ispunct(ascii));
  }
}

TEST(IcuI18nUtilsTest, IsAlphabeticAt) {
  // Test alphabetic and non-alphabetic ascii characters
  constexpr std::string_view kSomeAscii = "iJ?9";
  EXPECT_TRUE(i18n_utils::IsAlphabeticAt(kSomeAscii, /*position=*/0));   // 'i'
  EXPECT_TRUE(i18n_utils::IsAlphabeticAt(kSomeAscii, /*position=*/1));   // 'J'
  EXPECT_FALSE(i18n_utils::IsAlphabeticAt(kSomeAscii, /*position=*/2));  // '?'
  EXPECT_FALSE(i18n_utils::IsAlphabeticAt(kSomeAscii, /*position=*/3));  // '9'

  constexpr std::string_view kSomeNonAscii = "👏ñ①カ";
  EXPECT_FALSE(
      i18n_utils::IsAlphabeticAt(kSomeNonAscii, /*position=*/0));  // '👏'
  EXPECT_EQ(i18n_utils::GetUtf8Length(i18n_utils::GetUChar32At(
                kSomeNonAscii.data(), kSomeNonAscii.length(), 0)),
            4);
  EXPECT_TRUE(
      i18n_utils::IsAlphabeticAt(kSomeNonAscii, /*position=*/4));  // 'ñ'
  EXPECT_EQ(i18n_utils::GetUtf8Length(i18n_utils::GetUChar32At(
                kSomeNonAscii.data(), kSomeNonAscii.length(), 4)),
            2);
  EXPECT_FALSE(
      i18n_utils::IsAlphabeticAt(kSomeNonAscii, /*position=*/6));  // '①'
  EXPECT_EQ(i18n_utils::GetUtf8Length(i18n_utils::GetUChar32At(
                kSomeNonAscii.data(), kSomeNonAscii.length(), 6)),
            3);
  EXPECT_TRUE(
      i18n_utils::IsAlphabeticAt(kSomeNonAscii, /*position=*/9));  // 'カ'
  EXPECT_EQ(i18n_utils::GetUtf8Length(i18n_utils::GetUChar32At(
                kSomeNonAscii.data(), kSomeNonAscii.length(), 9)),
            3);
}

TEST(IcuI18nUtilsTest, IsWhitespaceAt) {
  // Test ASCII and non-ASCII whitespaces

  // ASCII whitespaces:
  EXPECT_TRUE(i18n_utils::IsWhitespaceAt("\u0009",
                                         /*position=*/0));  // horizontal tab
  EXPECT_TRUE(i18n_utils::IsWhitespaceAt("\u000a",
                                         /*position=*/0));  // line feed '\n'
  EXPECT_TRUE(
      i18n_utils::IsWhitespaceAt("\u000b", /*position=*/0));  // vertical tab
  EXPECT_TRUE(
      i18n_utils::IsWhitespaceAt("\u000c", /*position=*/0));  // form feed
  EXPECT_TRUE(i18n_utils::IsWhitespaceAt("\u000d",
                                         /*position=*/0));  // carriage return
  EXPECT_TRUE(
      i18n_utils::IsWhitespaceAt("\u0020", /*position=*/0));  // space ' '

  // Non-ASCII whitespaces:
  EXPECT_TRUE(i18n_utils::IsWhitespaceAt(
      "\u00a0", /*position=*/0));  // Non-breaking space
}

TEST(IcuI18nUtilsTest, GetUtf8Length) {
  // Test alphabetic and non-alphabetic ascii characters
  constexpr std::string_view kSomeAscii = "iJ?9";
  EXPECT_EQ(i18n_utils::GetUtf8Length(i18n_utils::GetUChar32At(
                kSomeAscii.data(), kSomeAscii.length(), 0)),
            1);  // 'i'
  EXPECT_EQ(i18n_utils::GetUtf8Length(i18n_utils::GetUChar32At(
                kSomeAscii.data(), kSomeAscii.length(), 1)),
            1);  // 'J'
  EXPECT_EQ(i18n_utils::GetUtf8Length(i18n_utils::GetUChar32At(
                kSomeAscii.data(), kSomeAscii.length(), 2)),
            1);  // '?'
  EXPECT_EQ(i18n_utils::GetUtf8Length(i18n_utils::GetUChar32At(
                kSomeAscii.data(), kSomeAscii.length(), 3)),
            1);  // '9'

  constexpr std::string_view kSomeNonAscii = "👏ñ①カ";
  EXPECT_EQ(i18n_utils::GetUtf8Length(i18n_utils::GetUChar32At(
                kSomeNonAscii.data(), kSomeNonAscii.length(), 0)),
            4);  // '👏'
  EXPECT_EQ(i18n_utils::GetUtf8Length(i18n_utils::GetUChar32At(
                kSomeNonAscii.data(), kSomeNonAscii.length(), 4)),
            2);  // 'ñ'
  EXPECT_EQ(i18n_utils::GetUtf8Length(i18n_utils::GetUChar32At(
                kSomeNonAscii.data(), kSomeNonAscii.length(), 6)),
            3);  // '①'
  EXPECT_EQ(i18n_utils::GetUtf8Length(i18n_utils::GetUChar32At(
                kSomeNonAscii.data(), kSomeNonAscii.length(), 9)),
            3);  // 'カ'
}

TEST(IcuI18nUtilsTest, SafeTruncate) {
  // Test alphabetic and non-alphabetic ascii characters
  constexpr std::string_view kSomeAscii = "iJ?9";
  std::string truncated(kSomeAscii);
  i18n_utils::SafeTruncateUtf8(&truncated, kSomeAscii.length() + 1);
  EXPECT_THAT(truncated, Eq("iJ?9"));
  truncated = kSomeAscii;
  i18n_utils::SafeTruncateUtf8(&truncated, kSomeAscii.length());
  EXPECT_THAT(truncated, Eq("iJ?9"));
  truncated = kSomeAscii;
  i18n_utils::SafeTruncateUtf8(&truncated, kSomeAscii.length() - 1);
  EXPECT_THAT(truncated, Eq("iJ?"));

  constexpr std::string_view kSomeNonAscii = "👏ñ①カ";
  truncated = kSomeNonAscii;
  i18n_utils::SafeTruncateUtf8(&truncated, kSomeNonAscii.length() + 1);
  EXPECT_THAT(truncated, Eq("👏ñ①カ"));
  truncated = kSomeNonAscii;
  i18n_utils::SafeTruncateUtf8(&truncated, kSomeNonAscii.length());
  EXPECT_THAT(truncated, Eq("👏ñ①カ"));
  truncated = kSomeNonAscii;
  i18n_utils::SafeTruncateUtf8(&truncated, kSomeNonAscii.length() - 1);
  EXPECT_THAT(truncated, Eq("👏ñ①"));
  truncated = kSomeNonAscii;
  i18n_utils::SafeTruncateUtf8(&truncated, kSomeNonAscii.length() - 2);
  EXPECT_THAT(truncated, Eq("👏ñ①"));
  truncated = kSomeNonAscii;
  i18n_utils::SafeTruncateUtf8(&truncated, kSomeNonAscii.length() - 3);
  EXPECT_THAT(truncated, Eq("👏ñ①"));
  truncated = kSomeNonAscii;
  i18n_utils::SafeTruncateUtf8(&truncated, kSomeNonAscii.length() - 4);
  EXPECT_THAT(truncated, Eq("👏ñ"));
}

TEST(IcuI18nUtilsTest, GetUChar32AtReplacementCharacterAndInvalidUtf8) {
  // Valid U+FFFD (Unicode replacement character) should return 0xFFFD.
  constexpr std::string_view kValidReplacementChar = "\xEF\xBF\xBD";
  EXPECT_EQ(i18n_utils::GetUChar32At(kValidReplacementChar.data(),
                                     kValidReplacementChar.length(), 0),
            0xFFFD);

  // Ill-formed UTF-8 sequence should return kInvalidUChar32 (-1).
  constexpr std::string_view kIllFormedUtf8 = "\xFF";
  EXPECT_EQ(i18n_utils::GetUChar32At(kIllFormedUtf8.data(),
                                     kIllFormedUtf8.length(), 0),
            i18n_utils::kInvalidUChar32);
}

}  // namespace
}  // namespace lib
}  // namespace icing
