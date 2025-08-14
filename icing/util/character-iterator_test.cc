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

#include "icing/util/character-iterator.h"

#include <cstring>
#include <string_view>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/icu-i18n-test-utils.h"

namespace icing {
namespace lib {

namespace {

using ::testing::Eq;
using ::testing::IsFalse;
using ::testing::IsTrue;

TEST(CharacterIteratorTest, DefaultInstanceShouldBeInvalid) {
  CharacterIterator iterator;
  EXPECT_THAT(iterator.is_valid(), IsFalse());
}

TEST(CharacterIteratorTest, EmptyText) {
  constexpr std::string_view kText = "¿Dónde está la biblioteca?";
  std::string_view empty_text(kText.data(), 0);

  CharacterIterator iterator(empty_text);
  EXPECT_THAT(iterator.is_valid(), IsTrue());
  EXPECT_THAT(iterator.GetCurrentChar(), Eq(0));
}

TEST(CharacterIteratorTest, BasicUtf8) {
  constexpr std::string_view kText = "¿Dónde está la biblioteca?";

  CharacterIterator iterator(kText);
  EXPECT_THAT(iterator.is_valid(), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("¿"));

  EXPECT_THAT(iterator.AdvanceToUtf8(4), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("ó"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/3,
                                      /*expected_utf16_index=*/2,
                                      /*expected_utf32_index=*/2));

  EXPECT_THAT(iterator.AdvanceToUtf8(18), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("b"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/18,
                                      /*expected_utf16_index=*/15,
                                      /*expected_utf32_index=*/15));

  EXPECT_THAT(iterator.AdvanceToUtf8(28), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("?"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/28,
                                      /*expected_utf16_index=*/25,
                                      /*expected_utf32_index=*/25));

  // Advance to the end of the string. This is allowed and we should get null
  // character.
  EXPECT_THAT(iterator.AdvanceToUtf8(29), IsTrue());
  EXPECT_THAT(iterator.GetCurrentChar(), Eq(0));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/29,
                                      /*expected_utf16_index=*/26,
                                      /*expected_utf32_index=*/26));

  EXPECT_THAT(iterator.RewindToUtf8(28), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("?"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/28,
                                      /*expected_utf16_index=*/25,
                                      /*expected_utf32_index=*/25));

  EXPECT_THAT(iterator.RewindToUtf8(18), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("b"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/18,
                                      /*expected_utf16_index=*/15,
                                      /*expected_utf32_index=*/15));

  EXPECT_THAT(iterator.RewindToUtf8(4), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("ó"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/3,
                                      /*expected_utf16_index=*/2,
                                      /*expected_utf32_index=*/2));

  EXPECT_THAT(iterator.RewindToUtf8(0), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("¿"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/0,
                                      /*expected_utf16_index=*/0,
                                      /*expected_utf32_index=*/0));
}

TEST(CharacterIteratorTest, BasicUtf16) {
  constexpr std::string_view kText = "¿Dónde está la biblioteca?";

  CharacterIterator iterator(kText);
  EXPECT_THAT(iterator.is_valid(), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("¿"));

  EXPECT_THAT(iterator.AdvanceToUtf16(2), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("ó"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/3,
                                      /*expected_utf16_index=*/2,
                                      /*expected_utf32_index=*/2));

  EXPECT_THAT(iterator.AdvanceToUtf16(15), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("b"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/18,
                                      /*expected_utf16_index=*/15,
                                      /*expected_utf32_index=*/15));

  EXPECT_THAT(iterator.AdvanceToUtf16(25), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("?"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/28,
                                      /*expected_utf16_index=*/25,
                                      /*expected_utf32_index=*/25));

  // Advance to the end of the string. This is allowed and we should get null
  // character.
  EXPECT_THAT(iterator.AdvanceToUtf16(26), IsTrue());
  EXPECT_THAT(iterator.GetCurrentChar(), Eq(0));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/29,
                                      /*expected_utf16_index=*/26,
                                      /*expected_utf32_index=*/26));

  EXPECT_THAT(iterator.RewindToUtf16(25), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("?"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/28,
                                      /*expected_utf16_index=*/25,
                                      /*expected_utf32_index=*/25));

  EXPECT_THAT(iterator.RewindToUtf16(15), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("b"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/18,
                                      /*expected_utf16_index=*/15,
                                      /*expected_utf32_index=*/15));

  EXPECT_THAT(iterator.RewindToUtf16(2), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("ó"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/3,
                                      /*expected_utf16_index=*/2,
                                      /*expected_utf32_index=*/2));

  EXPECT_THAT(iterator.RewindToUtf16(0), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("¿"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/0,
                                      /*expected_utf16_index=*/0,
                                      /*expected_utf32_index=*/0));
}

TEST(CharacterIteratorTest, BasicUtf32) {
  constexpr std::string_view kText = "¿Dónde está la biblioteca?";

  CharacterIterator iterator(kText);
  EXPECT_THAT(iterator.is_valid(), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("¿"));

  EXPECT_THAT(iterator.AdvanceToUtf32(2), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("ó"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/3,
                                      /*expected_utf16_index=*/2,
                                      /*expected_utf32_index=*/2));

  EXPECT_THAT(iterator.AdvanceToUtf32(15), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("b"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/18,
                                      /*expected_utf16_index=*/15,
                                      /*expected_utf32_index=*/15));

  EXPECT_THAT(iterator.AdvanceToUtf32(25), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("?"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/28,
                                      /*expected_utf16_index=*/25,
                                      /*expected_utf32_index=*/25));

  // Advance to the end of the string. This is allowed and we should get null
  // character.
  EXPECT_THAT(iterator.AdvanceToUtf32(26), IsTrue());
  EXPECT_THAT(iterator.GetCurrentChar(), Eq(0));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/29,
                                      /*expected_utf16_index=*/26,
                                      /*expected_utf32_index=*/26));

  EXPECT_THAT(iterator.RewindToUtf32(25), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("?"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/28,
                                      /*expected_utf16_index=*/25,
                                      /*expected_utf32_index=*/25));

  EXPECT_THAT(iterator.RewindToUtf32(15), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("b"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/18,
                                      /*expected_utf16_index=*/15,
                                      /*expected_utf32_index=*/15));

  EXPECT_THAT(iterator.RewindToUtf32(2), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("ó"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/3,
                                      /*expected_utf16_index=*/2,
                                      /*expected_utf32_index=*/2));

  EXPECT_THAT(iterator.RewindToUtf32(0), IsTrue());
  EXPECT_THAT(UCharToString(iterator.GetCurrentChar()), Eq("¿"));
  EXPECT_THAT(iterator,
              EqualsCharacterIterator(kText, /*expected_utf8_index=*/0,
                                      /*expected_utf16_index=*/0,
                                      /*expected_utf32_index=*/0));
}

TEST(CharacterIteratorTest, InvalidUtf) {
  // "\255" is an invalid sequence.
  constexpr std::string_view kText = "foo \255 bar";
  CharacterIterator iterator(kText);
  EXPECT_THAT(iterator.is_valid(), IsTrue());

  // Try to advance to the 'b' in 'bar'. This will fail. Also the iterator will
  // be in an undefined state, so no need to verify the state or
  // GetCurrentChar().
  EXPECT_THAT(iterator.AdvanceToUtf8(6), IsFalse());
  EXPECT_THAT(iterator.AdvanceToUtf16(6), IsFalse());
  EXPECT_THAT(iterator.AdvanceToUtf32(6), IsFalse());
}

TEST(CharacterIteratorTest, AdvanceToUtf8_emptyText) {
  // Create an uninitialized buffer.
  char buf[30];

  // Create a string_view that points to the 10-th byte of the buffer with
  // length 0.
  std::string_view text(buf + 10, 0);

  CharacterIterator iter0(text);
  // Advance to utf8 index 0. This should succeed without memory or
  // use-of-uninitialized-value errors (tested with "--config=msan").
  EXPECT_THAT(iter0.AdvanceToUtf8(0), IsTrue());
  // We should get null character after succeeding.
  EXPECT_THAT(iter0.GetCurrentChar(), Eq(0));

  // Advance to utf8 indices with positive values. This should fail successfully
  // without memory or use-of-uninitialized-value errors (tested with
  // "--config=msan").
  CharacterIterator iter1(text);
  EXPECT_THAT(iter1.AdvanceToUtf8(1), IsFalse());

  CharacterIterator iter2(text);
  EXPECT_THAT(iter2.AdvanceToUtf8(2), IsFalse());

  // Advance to utf8 indices with negative values. This should fail successfully
  // without memory or use-of-uninitialized-value errors (tested with
  // "--config=msan").
  CharacterIterator iter3(text);
  EXPECT_THAT(iter1.AdvanceToUtf8(-1), IsFalse());

  CharacterIterator iter4(text);
  EXPECT_THAT(iter2.AdvanceToUtf8(-2), IsFalse());
}

TEST(CharacterIteratorTest, AdvanceToUtf8_negativeIndex) {
  constexpr std::string_view kText = "abcdefghijklmnopqrstuvwxyz";
  // Create a buffer with extra 4 bytes. Copy kText to the last 26 bytes and
  // intentionally leave the first 4 bytes uninitialized.
  char buf[30];
  memcpy(buf + 4, kText.data(), kText.size());

  std::string_view text(buf + 4, kText.size());

  // Advance to negative utf8 indices. This should fail successfully without
  // memory or use-of-uninitialized-value errors (tested with "--config=msan").

  CharacterIterator iter0(text);
  EXPECT_THAT(iter0.AdvanceToUtf8(-1), IsFalse());

  CharacterIterator iter1(text);
  EXPECT_THAT(iter1.AdvanceToUtf8(-2), IsFalse());
}

TEST(CharacterIteratorTest, AdvanceToUtf8_indexEqCharLength) {
  constexpr std::string_view kText = "abcdefghijklmnopqrstuvwxyz";
  // Create a buffer with extra 4 bytes. Copy kText to the first 26 bytes and
  // intentionally leave the last 4 bytes uninitialized.
  char buf[30];
  memcpy(buf, kText.data(), kText.size());

  std::string_view text(buf, kText.size());

  CharacterIterator iter0(text);
  // Advance to utf8 index == kText.size(). This should succeed without memory
  // or use-of-uninitialized-value errors (tested with "--config=msan").
  EXPECT_THAT(iter0.AdvanceToUtf8(kText.size()), IsTrue());
  // We should get null character after succeeding.
  EXPECT_THAT(iter0.GetCurrentChar(), Eq(0));
}

TEST(CharacterIteratorTest, AdvanceToUtf8_indexGtCharLength) {
  constexpr std::string_view kText = "abcdefghijklmnopqrstuvwxyz";
  // Create a buffer with extra 4 bytes. Copy kText to the first 26 bytes and
  // intentionally leave the last 4 bytes uninitialized.
  char buf[30];
  memcpy(buf, kText.data(), kText.size());

  std::string_view text(buf, kText.size());

  // Advance to utf8 index greater than the length of the string. This should
  // fail successfully without memory or use-of-uninitialized-value errors
  // (tested with "--config=msan").

  CharacterIterator iter0(text);
  EXPECT_THAT(iter0.AdvanceToUtf8(kText.size() + 1), IsFalse());

  CharacterIterator iter1(text);
  EXPECT_THAT(iter0.AdvanceToUtf8(kText.size() + 2), IsFalse());
}

}  // namespace

}  // namespace lib
}  // namespace icing
