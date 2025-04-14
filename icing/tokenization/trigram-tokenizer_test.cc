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

#include "icing/tokenization/trigram-tokenizer.h"

#include <memory>
#include <string_view>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/testing/common-matchers.h"
#include "icing/tokenization/token.h"
#include "icing/tokenization/tokenizer.h"
#include "icing/util/character-iterator.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::IsFalse;
using ::testing::IsTrue;

TEST(TrigramTokenizerTest, TokenizeAll_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s("abcdefg");
  EXPECT_THAT(
      tokenizer.TokenizeAll(s),
      IsOkAndHolds(ElementsAre(EqualsToken(Token::Type::TRIGRAM, "abc"),
                               EqualsToken(Token::Type::TRIGRAM, "bcd"),
                               EqualsToken(Token::Type::TRIGRAM, "cde"),
                               EqualsToken(Token::Type::TRIGRAM, "def"),
                               EqualsToken(Token::Type::TRIGRAM, "efg"))));
}

TEST(TrigramTokenizerTest, TokenizeAll_CJKT) {
  TrigramTokenizer tokenizer;

  std::string_view s("我每天走路去上班");
  EXPECT_THAT(
      tokenizer.TokenizeAll(s),
      IsOkAndHolds(ElementsAre(EqualsToken(Token::Type::TRIGRAM, "我每天"),
                               EqualsToken(Token::Type::TRIGRAM, "每天走"),
                               EqualsToken(Token::Type::TRIGRAM, "天走路"),
                               EqualsToken(Token::Type::TRIGRAM, "走路去"),
                               EqualsToken(Token::Type::TRIGRAM, "路去上"),
                               EqualsToken(Token::Type::TRIGRAM, "去上班"))));
}

TEST(TrigramTokenizerTest, TokenizeAll_latinLettersWithAccents) {
  TrigramTokenizer tokenizer;

  std::string_view s("āăąḃḅḇčćç");
  EXPECT_THAT(
      tokenizer.TokenizeAll(s),
      IsOkAndHolds(ElementsAre(EqualsToken(Token::Type::TRIGRAM, "āăą"),
                               EqualsToken(Token::Type::TRIGRAM, "ăąḃ"),
                               EqualsToken(Token::Type::TRIGRAM, "ąḃḅ"),
                               EqualsToken(Token::Type::TRIGRAM, "ḃḅḇ"),
                               EqualsToken(Token::Type::TRIGRAM, "ḅḇč"),
                               EqualsToken(Token::Type::TRIGRAM, "ḇčć"),
                               EqualsToken(Token::Type::TRIGRAM, "čćç"))));
}

TEST(TrigramTokenizerTest, TokenizeAll_whitespaceShouldBeTrigrams) {
  TrigramTokenizer tokenizer;

  std::string_view s("foo bar");
  EXPECT_THAT(
      tokenizer.TokenizeAll(s),
      IsOkAndHolds(ElementsAre(EqualsToken(Token::Type::TRIGRAM, "foo"),
                               EqualsToken(Token::Type::TRIGRAM, "oo "),
                               EqualsToken(Token::Type::TRIGRAM, "o b"),
                               EqualsToken(Token::Type::TRIGRAM, " ba"),
                               EqualsToken(Token::Type::TRIGRAM, "bar"))));
}

TEST(TrigramTokenizerTest, TokenizeAll_punctuationShouldBeTrigrams) {
  TrigramTokenizer tokenizer;

  std::string_view s("foo`~!@#$%^&*()-+=[]{}\\|;:'\",./<>?bar");
  EXPECT_THAT(
      tokenizer.TokenizeAll(s),
      IsOkAndHolds(ElementsAre(EqualsToken(Token::Type::TRIGRAM, "foo"),
                               EqualsToken(Token::Type::TRIGRAM, "oo`"),
                               EqualsToken(Token::Type::TRIGRAM, "o`~"),
                               EqualsToken(Token::Type::TRIGRAM, "`~!"),
                               EqualsToken(Token::Type::TRIGRAM, "~!@"),
                               EqualsToken(Token::Type::TRIGRAM, "!@#"),
                               EqualsToken(Token::Type::TRIGRAM, "@#$"),
                               EqualsToken(Token::Type::TRIGRAM, "#$%"),
                               EqualsToken(Token::Type::TRIGRAM, "$%^"),
                               EqualsToken(Token::Type::TRIGRAM, "%^&"),
                               EqualsToken(Token::Type::TRIGRAM, "^&*"),
                               EqualsToken(Token::Type::TRIGRAM, "&*("),
                               EqualsToken(Token::Type::TRIGRAM, "*()"),
                               EqualsToken(Token::Type::TRIGRAM, "()-"),
                               EqualsToken(Token::Type::TRIGRAM, ")-+"),
                               EqualsToken(Token::Type::TRIGRAM, "-+="),
                               EqualsToken(Token::Type::TRIGRAM, "+=["),
                               EqualsToken(Token::Type::TRIGRAM, "=[]"),
                               EqualsToken(Token::Type::TRIGRAM, "[]{"),
                               EqualsToken(Token::Type::TRIGRAM, "]{}"),
                               EqualsToken(Token::Type::TRIGRAM, "{}\\"),
                               EqualsToken(Token::Type::TRIGRAM, "}\\|"),
                               EqualsToken(Token::Type::TRIGRAM, "\\|;"),
                               EqualsToken(Token::Type::TRIGRAM, "|;:"),
                               EqualsToken(Token::Type::TRIGRAM, ";:'"),
                               EqualsToken(Token::Type::TRIGRAM, ":'\""),
                               EqualsToken(Token::Type::TRIGRAM, "'\","),
                               EqualsToken(Token::Type::TRIGRAM, "\",."),
                               EqualsToken(Token::Type::TRIGRAM, ",./"),
                               EqualsToken(Token::Type::TRIGRAM, "./<"),
                               EqualsToken(Token::Type::TRIGRAM, "/<>"),
                               EqualsToken(Token::Type::TRIGRAM, "<>?"),
                               EqualsToken(Token::Type::TRIGRAM, ">?b"),
                               EqualsToken(Token::Type::TRIGRAM, "?ba"),
                               EqualsToken(Token::Type::TRIGRAM, "bar"))));
}

TEST(TrigramTokenizerTest, TokenizeAll_emptyStringShouldReturnEmpty) {
  TrigramTokenizer tokenizer;

  std::string_view s("");
  EXPECT_THAT(tokenizer.TokenizeAll(s), IsOkAndHolds(IsEmpty()));
}

TEST(TrigramTokenizerTest, TokenizeAll_tooShortStringShouldReturnEmpty_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s1("a");
  EXPECT_THAT(tokenizer.TokenizeAll(s1), IsOkAndHolds(IsEmpty()));

  std::string_view s2("ab");
  EXPECT_THAT(tokenizer.TokenizeAll(s2), IsOkAndHolds(IsEmpty()));
}

TEST(TrigramTokenizerTest, TokenizeAll_tooShortStringShouldReturnEmpty_utf8) {
  TrigramTokenizer tokenizer;

  std::string_view s1("我");
  EXPECT_THAT(tokenizer.TokenizeAll(s1), IsOkAndHolds(IsEmpty()));

  std::string_view s2("我每");
  EXPECT_THAT(tokenizer.TokenizeAll(s2), IsOkAndHolds(IsEmpty()));
}

TEST(TrigramTokenizerTest,
     TokenizeAll_exactThreeCharsShouldReturnOneTrigram_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s("abc");
  EXPECT_THAT(
      tokenizer.TokenizeAll(s),
      IsOkAndHolds(ElementsAre(EqualsToken(Token::Type::TRIGRAM, "abc"))));
}

TEST(TrigramTokenizerTest,
     TokenizeAll_exactThreeCharsShouldReturnOneTrigram_utf8) {
  TrigramTokenizer tokenizer;

  std::string_view s("我每天");
  EXPECT_THAT(
      tokenizer.TokenizeAll(s),
      IsOkAndHolds(ElementsAre(EqualsToken(Token::Type::TRIGRAM, "我每天"))));
}

TEST(TrigramTokenizerTest, CalculateTokenStartAndEndExclusive_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s("abcd");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance to "abc".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "abc")));
  ICING_ASSERT_OK_AND_ASSIGN(CharacterIterator start_itr0,
                             itr->CalculateTokenStart());
  ICING_ASSERT_OK_AND_ASSIGN(CharacterIterator end_itr0,
                             itr->CalculateTokenEndExclusive());
  EXPECT_THAT(start_itr0.utf8_index(), Eq(0));
  EXPECT_THAT(end_itr0.utf8_index(), Eq(3));

  // Advance to "bcd".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "bcd")));
  ICING_ASSERT_OK_AND_ASSIGN(CharacterIterator start_itr1,
                             itr->CalculateTokenStart());
  ICING_ASSERT_OK_AND_ASSIGN(CharacterIterator end_itr1,
                             itr->CalculateTokenEndExclusive());
  EXPECT_THAT(start_itr1.utf8_index(), Eq(1));
  EXPECT_THAT(end_itr1.utf8_index(), Eq(4));

  ASSERT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest, CalculateTokenStartAndEndExclusive_utf8) {
  TrigramTokenizer tokenizer;

  std::string_view s("我每天走");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance to "我每天".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "我每天")));
  ICING_ASSERT_OK_AND_ASSIGN(CharacterIterator start_itr0,
                             itr->CalculateTokenStart());
  ICING_ASSERT_OK_AND_ASSIGN(CharacterIterator end_itr0,
                             itr->CalculateTokenEndExclusive());
  EXPECT_THAT(start_itr0.utf8_index(), Eq(0));
  EXPECT_THAT(end_itr0.utf8_index(), Eq(s.find("走")));

  // Advance to "每天走".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "每天走")));
  ICING_ASSERT_OK_AND_ASSIGN(CharacterIterator start_itr1,
                             itr->CalculateTokenStart());
  ICING_ASSERT_OK_AND_ASSIGN(CharacterIterator end_itr1,
                             itr->CalculateTokenEndExclusive());
  EXPECT_THAT(start_itr1.utf8_index(), Eq(s.find("每")));
  EXPECT_THAT(end_itr1.utf8_index(), Eq(s.length()));

  ASSERT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest, ResetToTokenStartingAfter_resetBackward_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s("abcdefgh");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance 5 times to "efg".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "efg")));

  // Reset backward to after utf32_offset 1.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/1), IsTrue());

  // Verify the iterator is pointing to "cde" since it is the leftmost trigram
  // token after utf32_offset 1.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "cde")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "def".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "def")));
  // Advance to "efg".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "efg")));
  // Advance to "fgh".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "fgh")));

  EXPECT_THAT(itr->Advance(), IsFalse());

  // Reset again.
  // Reset backward to after utf32_offset 0.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/0), IsTrue());

  // Verify the iterator is pointing to "bcd" since it is the leftmost trigram
  // token after utf32_offset 0.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "bcd")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "cde".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "cde")));
  // Advance to "def".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "def")));
  // Advance to "efg".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "efg")));
  // Advance to "fgh".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "fgh")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest, ResetToTokenStartingAfter_resetBackward_utf8) {
  TrigramTokenizer tokenizer;

  std::string_view s("我每天走路去上班");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance 5 times to "路去上".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "路去上")));

  // Reset backward to after "每". Note that the utf32_offset of "每" is 1.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/1), IsTrue());

  // Verify the iterator is pointing to "天走路" since it is the leftmost
  // trigram token after "每".
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "天走路")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "走路去".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "走路去")));
  // Advance to "路去上".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "路去上")));
  // Advance to "去上班".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "去上班")));

  EXPECT_THAT(itr->Advance(), IsFalse());

  // Reset again.
  // Reset backward to after utf32_offset 0.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/0), IsTrue());

  // Verify the iterator is pointing to "每天走" since it is the leftmost
  // trigram token after utf32_offset 0.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "每天走")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "天走路".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "天走路")));
  // Advance to "走路去".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "走路去")));
  // Advance to "路去上".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "路去上")));
  // Advance to "去上班".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "去上班")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest, ResetToTokenStartingAfter_resetForward_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s("abcdefgh");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance 1 time to "abc".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "abc")));

  // Reset forward to after utf32_offset 2.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/2), IsTrue());

  // Verify the iterator is pointing to "def" since it is the leftmost trigram
  // token after utf32_offset 2.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "def")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "efg".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "efg")));
  // Advance to "fgh".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "fgh")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest, ResetToTokenStartingAfter_resetForward_utf8) {
  TrigramTokenizer tokenizer;

  std::string_view s("我每天走路去上班");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance 1 time to "我每天".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "我每天")));

  // Reset forward to after "天". Note that the utf32_offset of "天" is 2.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/2), IsTrue());

  // Verify the iterator is pointing to "走路去" since it is the leftmost
  // trigram token after "天".
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "走路去")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "路去上".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "路去上")));
  // Advance to "去上班".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "去上班")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenStartingAfter_negativeUtf32OffsetShouldResetToStart) {
  TrigramTokenizer tokenizer;

  std::string_view s("abcde");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance twice to "bcd".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "bcd")));

  // Reset to after utf32_offset -1.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/-1), IsTrue());

  // Verify the iterator is pointing to "abc" since it is the leftmost trigram
  // token after utf32_offset -1.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "abc")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "bcd".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "bcd")));
  // Advance to "cde".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "cde")));

  EXPECT_THAT(itr->Advance(), IsFalse());

  // Reset to after utf32_offset -2.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/-2), IsTrue());

  // Verify the iterator is pointing to "abc" since it is the leftmost trigram
  // token after utf32_offset -2.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "abc")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "bcd".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "bcd")));
  // Advance to "cde".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "cde")));
}

TEST(TrigramTokenizerTest,
     ResetToTokenStartingAfter_utf32OffsetAfterOrAtLastTokenShouldFail_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s("abcde");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance twice to "bcd".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "bcd")));

  // Reset to after utf32_offset 2. Since the last token is "cde" and the
  // starting utf32_offset of it is 2, ResetToTokenStartingAfter() should fail
  // with utf32_offset >= 2 since there is no token after it.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/2), IsFalse());

  // Reset to after utf32_offset 3.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/3), IsFalse());

  // Reset to after utf32_offset 4.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/4), IsFalse());

  // Reset to after utf32_offset 5.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/5), IsFalse());

  // Reset to after utf32_offset 6.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/6), IsFalse());

  // Reset to after utf32_offset 7.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/7), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenStartingAfter_utf32OffsetAfterOrAtLastTokenShouldFail_utf8) {
  TrigramTokenizer tokenizer;

  std::string_view s("我每天走路");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance once to "我每天".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "我每天")));

  // Reset to after "天" (with utf32_offset 2). Since the last token is
  // "天走路", ResetToTokenStartingAfter() should fail with utf32_offset >= 2
  // since there is no token after it.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/2), IsFalse());

  // Reset to after utf32_offset 3.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/3), IsFalse());

  // Reset to after utf32_offset 4.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/4), IsFalse());

  // Reset to after utf32_offset 5.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/5), IsFalse());

  // Reset to after utf32_offset 6.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/6), IsFalse());

  // Reset to after utf32_offset 7.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/7), IsFalse());
}

TEST(
    TrigramTokenizerTest,
    ResetToTokenStartingAfter_fromInitStateWithValidStringShouldSucceed_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s("abcdef");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Call ResetToTokenStartingAfter() with a valid utf32_offset without any
  // Advance().
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/1), IsTrue());

  // Verify the iterator is pointing to "cde" since it is the leftmost trigram
  // token after utf32_offset 1.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "cde")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "def".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "def")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenStartingAfter_fromInitStateWithValidStringShouldSucceed_utf8) {
  TrigramTokenizer tokenizer;

  std::string_view s("我每天走路去");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Call ResetToTokenStartingAfter() with a valid utf32_offset without any
  // Advance().
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/1), IsTrue());

  // Verify the iterator is pointing to "天走路" since it is the leftmost
  // trigram token after utf32_offset 1.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "天走路")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "走路去".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "走路去")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenStartingAfter_fromInitStateWithEmptyStringShouldFail) {
  TrigramTokenizer tokenizer;

  std::string_view s("");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Call ResetToTokenStartingAfter() without any Advance(). This should fail
  // since the text is too short to form a trigram.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/-1), IsFalse());
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/0), IsFalse());
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/1), IsFalse());
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/2), IsFalse());
}

TEST(
    TrigramTokenizerTest,
    ResetToTokenStartingAfter_fromInitStateWithTooShortStringShouldFail_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s("ab");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Call ResetToTokenStartingAfter() without any Advance(). This should fail
  // since the text is too short to form a trigram.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/-1), IsFalse());
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/0), IsFalse());
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/1), IsFalse());
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/2), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenStartingAfter_fromInitStateWithTooShortStringShouldFail_utf8) {
  TrigramTokenizer tokenizer;

  std::string_view s("我每");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Call ResetToTokenStartingAfter() without any Advance(). This should fail
  // since the text is too short to form a trigram.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/-1), IsFalse());
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/0), IsFalse());
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/1), IsFalse());
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/2), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenStartingAfter_fromEndStateWithValidStringShouldSucceed_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s("abcdef");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance to the end.
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsFalse());

  // Call ResetToTokenStartingAfter() with a valid utf32_offset when reaching
  // the end.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/1), IsTrue());

  // Verify the iterator is pointing to "cde" since it is the leftmost trigram
  // token after utf32_offset 1.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "cde")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "def".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "def")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenStartingAfter_fromEndStateWithValidStringShouldSucceed_utf8) {
  TrigramTokenizer tokenizer;

  std::string_view s("我每天走路去");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance to the end.
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsFalse());

  // Call ResetToTokenStartingAfter() with a valid utf32_offset when reaching
  // the end.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/1), IsTrue());

  // Verify the iterator is pointing to "天走路" since it is the leftmost
  // trigram token after utf32_offset 1.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "天走路")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "走路去".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "走路去")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenStartingAfter_fromEndStateWithEmptyStringShouldFail) {
  TrigramTokenizer tokenizer;

  std::string_view s("");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance to the end.
  ASSERT_THAT(itr->Advance(), IsFalse());

  // Call ResetToTokenStartingAfter() when reaching the end. This should fail
  // since the text is too short to form a trigram.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/-1), IsFalse());
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/0), IsFalse());
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/1), IsFalse());
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/2), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenStartingAfter_fromEndStateWithTooShortStringShouldFail_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s("ab");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance to the end.
  ASSERT_THAT(itr->Advance(), IsFalse());

  // Call ResetToTokenStartingAfter() when reaching the end. This should fail
  // since the text is too short to form a trigram.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/-1), IsFalse());
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/0), IsFalse());
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/1), IsFalse());
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/2), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenStartingAfter_fromEndStateWithTooShortStringShouldFail_utf8) {
  TrigramTokenizer tokenizer;

  std::string_view s("我每");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance to the end.
  ASSERT_THAT(itr->Advance(), IsFalse());

  // Call ResetToTokenStartingAfter() when reaching the end. This should fail
  // since the text is too short to form a trigram.
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/-1), IsFalse());
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/0), IsFalse());
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/1), IsFalse());
  EXPECT_THAT(itr->ResetToTokenStartingAfter(/*utf32_offset=*/2), IsFalse());
}

TEST(TrigramTokenizerTest, ResetToTokenEndingBefore_resetBackward_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s("abcdefgh");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance 5 times to "efg".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "efg")));

  // Reset backward to before utf32_offset 5.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/5), IsTrue());

  // Verify the iterator is pointing to "cde" since it is the rightmost trigram
  // token before utf32_offset 5.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "cde")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "def".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "def")));
  // Advance to "efg".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "efg")));
  // Advance to "fgh".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "fgh")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest, ResetToTokenEndingBefore_resetBackward_utf8) {
  TrigramTokenizer tokenizer;

  std::string_view s("我每天走路去上班");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance 5 times to "路去上".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "路去上")));

  // Reset backward to before "去". Note that the utf32_offset of "去" is 5.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/5), IsTrue());

  // Verify the iterator is pointing to "天走路" since it is the rightmost
  // trigram token before "每".
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "天走路")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "走路去".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "走路去")));
  // Advance to "路去上".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "路去上")));
  // Advance to "去上班".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "去上班")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest, ResetToTokenEndingBefore_resetForward_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s("abcdefgh");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance 1 time to "abc".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "abc")));

  // Reset forward to before utf32_offset 6.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/6), IsTrue());

  // Verify the iterator is pointing to "def" since it is the rightmost trigram
  // token before utf32_offset 6.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "def")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "efg".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "efg")));
  // Advance to "fgh".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "fgh")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest, ResetToTokenEndingBefore_resetForward_utf8) {
  TrigramTokenizer tokenizer;

  std::string_view s("我每天走路去上班");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance 1 time to "我每天".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "我每天")));

  // Reset forward to before "上". Note that the utf32_offset of "上" is 6.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/6), IsTrue());

  // Verify the iterator is pointing to "走路去" since it is the rightmost
  // trigram token before "上".
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "走路去")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "路去上".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "路去上")));
  // Advance to "去上班".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "去上班")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(
    TrigramTokenizerTest,
    ResetToTokenEndingBefore_utf32OffsetAfterOrAtLastTokenShouldResetToLast_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s("abcde");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance once to "abc".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "abc")));

  // Reset to before utf32_offset 5. Since the last token "cde" ends at
  // utf32_offset 4, the iterator should be reset to "cde" which is the
  // rightmost trigram token before utf32_offset 5.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/5), IsTrue());

  // Verify the iterator is pointing to "cde" since it is the rightmost trigram
  // token before utf32_offset 5.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "cde")));

  // Since it is the last token, the next Advance() should fail.
  EXPECT_THAT(itr->Advance(), IsFalse());

  // Reset to before utf32_offset 6. Since the last token "cde" ends at
  // utf32_offset 4, the iterator should be reset to "cde" which is the
  // rightmost trigram token before utf32_offset 6.
  //
  // ResetToTokenEndingBefore should still work even if utf32_offset 6 is
  // greater than # of characters in the text.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/6), IsTrue());

  // Verify the iterator is pointing to "cde" since it is the rightmost trigram
  // token before utf32_offset 6.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "cde")));

  // Since it is the last token, the next Advance() should fail.
  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(
    TrigramTokenizerTest,
    ResetToTokenEndingBefore_utf32OffsetAfterOrAtLastTokenShouldResetToLast_utf8) {
  TrigramTokenizer tokenizer;

  std::string_view s("我每天走路");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance once to "我每天".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "我每天")));

  // Reset to before utf32_offset 5. Since the last token "天走路" ends at
  // utf32_offset 4, the iterator should be reset to "天走路" which is the
  // rightmost trigram token before utf32_offset 5.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/5), IsTrue());

  // Verify the iterator is pointing to "天走路" since it is the rightmost
  // trigram token before utf32_offset 5.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "天走路")));

  // Since it is the last token, the next Advance() should fail.
  EXPECT_THAT(itr->Advance(), IsFalse());

  // Reset to before utf32_offset 6. Since the last token "天走路" ends at
  // utf32_offset 4, the iterator should be reset to "天走路" which is the
  // rightmost trigram token before utf32_offset 6.
  //
  // ResetToTokenEndingBefore should still work even if utf32_offset 6 is
  // greater than # of characters in the text.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/6), IsTrue());

  // Verify the iterator is pointing to "天走路" since it is the rightmost
  // trigram token before utf32_offset 6.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "天走路")));

  // Since it is the last token, the next Advance() should fail.
  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenEndingBefore_utf32OffsetBeforeOrAtFirstTokenShouldFail_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s("abcde");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance twice to "bcd".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "bcd")));

  // Reset to before utf32_offset 2. Since the first token is "abc" and the
  // ending utf32_offset of it is 2, ResetToTokenEndingBefore() should fail with
  // utf32_offset <= 2 since there is no token before it.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/2), IsFalse());

  // Reset to before utf32_offset 1.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/1), IsFalse());

  // Reset to before utf32_offset 0.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/0), IsFalse());

  // Reset to before utf32_offset -1.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/-1), IsFalse());

  // Reset to before utf32_offset -2.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/-2), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenEndingBefore_utf32OffsetBeforeOrAtFirstTokenShouldFail_utf8) {
  TrigramTokenizer tokenizer;

  std::string_view s("我每天走路");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance once to "我每天".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "我每天")));

  // Reset to before "天" (with utf32_offset 2). Since the first token is
  // "天走路", ResetToTokenEndingBefore() should fail with utf32_offset <= 2
  // since there is no token before it.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/2), IsFalse());

  // Reset to before utf32_offset 1.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/1), IsFalse());

  // Reset to before utf32_offset 0.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/0), IsFalse());

  // Reset to before utf32_offset -1.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/-1), IsFalse());

  // Reset to before utf32_offset -2.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/-2), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenEndingBefore_fromInitStateWithValidStringShouldSucceed_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s("abcdef");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Call ResetToTokenEndingBefore() with a valid utf32_offset without any
  // Advance().
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/5), IsTrue());

  // Verify the iterator is pointing to "cde" since it is the rightmost trigram
  // token before utf32_offset 5.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "cde")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "def".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "def")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenEndingBefore_fromInitStateWithValidStringShouldSucceed_utf8) {
  TrigramTokenizer tokenizer;

  std::string_view s("我每天走路去");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Call ResetToTokenEndingBefore() with a valid utf32_offset without any
  // Advance().
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/5), IsTrue());

  // Verify the iterator is pointing to "天走路" since it is the rightmost
  // trigram token before utf32_offset 5.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "天走路")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "走路去".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "走路去")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenEndingBefore_fromInitStateWithEmptyStringShouldFail) {
  TrigramTokenizer tokenizer;

  std::string_view s("");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Call ResetToTokenEndingBefore() without any Advance(). This should fail
  // since the text is too short to form a trigram.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/0), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/1), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/2), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/3), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/4), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/5), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/6), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenEndingBefore_fromInitStateWithTooShortStringShouldFail_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s("ab");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Call ResetToTokenEndingBefore() without any Advance(). This should fail
  // since the text is too short to form a trigram.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/2), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/3), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/4), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/5), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/6), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenEndingBefore_fromInitStateWithTooShortStringShouldFail_utf8) {
  TrigramTokenizer tokenizer;

  std::string_view s("我每");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Call ResetToTokenEndingBefore() without any Advance(). This should fail
  // since the text is too short to form a trigram.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/2), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/3), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/4), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/5), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/6), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenEndingBefore_fromEndStateWithValidStringShouldSucceed_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s("abcdef");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance to the end.
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsFalse());

  // Call ResetToTokenEndingBefore() with a valid utf32_offset when reaching the
  // end.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/5), IsTrue());

  // Verify the iterator is pointing to "cde" since it is the rightmost trigram
  // token before utf32_offset 5.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "cde")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "def".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "def")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenEndingBefore_fromEndStateWithValidStringShouldSucceed_utf8) {
  TrigramTokenizer tokenizer;

  std::string_view s("我每天走路去");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance to the end.
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsFalse());

  // Call ResetToTokenEndingBefore() with a valid utf32_offset when reaching the
  // end.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/5), IsTrue());

  // Verify the iterator is pointing to "天走路" since it is the rightmost
  // trigram token before utf32_offset 5.
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "天走路")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "走路去".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "走路去")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenEndingBefore_fromEndStateWithEmptyStringShouldFail) {
  TrigramTokenizer tokenizer;

  std::string_view s("");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance to the end.
  ASSERT_THAT(itr->Advance(), IsFalse());

  // Call ResetToTokenEndingBefore() when reaching the end. This should fail
  // since the text is too short to form a trigram.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/0), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/1), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/2), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/3), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/4), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/5), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/6), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenEndingBefore_fromEndStateWithTooShortStringShouldFail_ascii) {
  TrigramTokenizer tokenizer;

  std::string_view s("ab");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance to the end.
  ASSERT_THAT(itr->Advance(), IsFalse());

  // Call ResetToTokenEndingBefore() when reaching the end. This should fail
  // since the text is too short to form a trigram.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/2), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/3), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/4), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/5), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/6), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToTokenEndingBefore_fromEndStateWithTooShortStringShouldFail_utf8) {
  TrigramTokenizer tokenizer;

  std::string_view s("我每");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance to the end.
  ASSERT_THAT(itr->Advance(), IsFalse());

  // Call ResetToTokenEndingBefore() when reaching the end. This should fail
  // since the text is too short to form a trigram.
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/2), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/3), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/4), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/5), IsFalse());
  EXPECT_THAT(itr->ResetToTokenEndingBefore(/*utf32_offset=*/6), IsFalse());
}

TEST(TrigramTokenizerTest, ResetToStart_ascii) {
  TrigramTokenizer tokenizer = TrigramTokenizer();

  std::string_view s("abcdef");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance 3 times to "cde".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "cde")));

  // Call ResetToStart().
  EXPECT_THAT(itr->ResetToStart(), IsTrue());

  // Verify the iterator is pointing to the start trigram "abc".
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "abc")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "bcd".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "bcd")));
  // Advance to "cde".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "cde")));
  // Advance to "def".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "def")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest, ResetToStart_utf8) {
  TrigramTokenizer tokenizer = TrigramTokenizer();

  std::string_view s("我每天走路去");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance 3 times to "天走路".
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "天走路")));

  // Call ResetToStart().
  EXPECT_THAT(itr->ResetToStart(), IsTrue());

  // Verify the iterator is pointing to the start trigram "我每天".
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "我每天")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "每天走".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "每天走")));
  // Advance to "天走路".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "天走路")));
  // Advance to "走路去".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "走路去")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToStart_fromInitStateWithValidStringShouldSucceed_ascii) {
  TrigramTokenizer tokenizer = TrigramTokenizer();

  std::string_view s("abcde");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Call ResetToStart() without any Advance().
  EXPECT_THAT(itr->ResetToStart(), IsTrue());

  // Verify the iterator is pointing to "abc".
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "abc")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "bcd".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "bcd")));
  // Advance to "cde".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "cde")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToStart_fromInitStateWithValidStringShouldSucceed_utf8) {
  TrigramTokenizer tokenizer = TrigramTokenizer();

  std::string_view s("我每天走路");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Call ResetToStart() without any Advance().
  EXPECT_THAT(itr->ResetToStart(), IsTrue());

  // Verify the iterator is pointing to "我每天".
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "我每天")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "每天走".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "每天走")));
  // Advance to "天走路".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "天走路")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToStart_fromInitStateWithEmptyStringShouldFail) {
  TrigramTokenizer tokenizer = TrigramTokenizer();

  std::string_view s("");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Call ResetToStart() without any Advance(). This should fail since the text
  // is too short to form a trigram.
  EXPECT_THAT(itr->ResetToStart(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToStart_fromInitStateWithTooShortStringShouldFail_ascii) {
  TrigramTokenizer tokenizer = TrigramTokenizer();

  std::string_view s("ab");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Call ResetToStart() without any Advance(). This should fail since the text
  // is too short to form a trigram.
  EXPECT_THAT(itr->ResetToStart(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToStart_fromInitStateWithTooShortStringShouldFail_utf8) {
  TrigramTokenizer tokenizer = TrigramTokenizer();

  std::string_view s("我每");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Call ResetToStart() without any Advance(). This should fail since the text
  // is too short to form a trigram.
  EXPECT_THAT(itr->ResetToStart(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToStart_fromEndStateWithValidStringShouldSucceed_ascii) {
  TrigramTokenizer tokenizer = TrigramTokenizer();

  std::string_view s("abcdef");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance to the end.
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsFalse());

  // Call ResetToStart() when reaching the end.
  EXPECT_THAT(itr->ResetToStart(), IsTrue());

  // Verify the iterator is pointing to the start trigram "abc".
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "abc")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "bcd".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "bcd")));
  // Advance to "cde".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "cde")));
  // Advance to "def".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "def")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToStart_fromEndStateWithValidStringShouldSucceed_utf8) {
  TrigramTokenizer tokenizer = TrigramTokenizer();

  std::string_view s("我每天走路去");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance to the end.
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsTrue());
  ASSERT_THAT(itr->Advance(), IsFalse());

  // Call ResetToStart() when reaching the end.
  EXPECT_THAT(itr->ResetToStart(), IsTrue());

  // Verify the iterator is pointing to the start trigram "我每天".
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "我每天")));

  // Verify the iterator is still valid and able to advance to the rest of the
  // tokens.
  // Advance to "每天走".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "每天走")));
  // Advance to "天走路".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "天走路")));
  // Advance to "走路去".
  EXPECT_THAT(itr->Advance(), IsTrue());
  EXPECT_THAT(itr->GetTokens(),
              ElementsAre(EqualsToken(Token::Type::TRIGRAM, "走路去")));

  EXPECT_THAT(itr->Advance(), IsFalse());
}

TEST(TrigramTokenizerTest, ResetToStart_fromEndStateWithEmptyStringShouldFail) {
  TrigramTokenizer tokenizer = TrigramTokenizer();

  std::string_view s("");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance to the end.
  ASSERT_THAT(itr->Advance(), IsFalse());

  // Call ResetToStart() when reaching the end. This should fail since the text
  // is too short to form a trigram.
  EXPECT_THAT(itr->ResetToStart(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToStart_fromEndStateWithTooShortStringShouldFail_ascii) {
  TrigramTokenizer tokenizer = TrigramTokenizer();

  std::string_view s("ab");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance to the end.
  ASSERT_THAT(itr->Advance(), IsFalse());

  // Call ResetToStart() when reaching the end. This should fail since the text
  // is too short to form a trigram.
  EXPECT_THAT(itr->ResetToStart(), IsFalse());
}

TEST(TrigramTokenizerTest,
     ResetToStart_fromEndStateWithTooShortStringShouldFail_utf8) {
  TrigramTokenizer tokenizer = TrigramTokenizer();

  std::string_view s("我每");
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Tokenizer::Iterator> itr,
                             tokenizer.Tokenize(s));

  // Advance to the end.
  ASSERT_THAT(itr->Advance(), IsFalse());

  // Call ResetToStart() when reaching the end. This should fail since the text
  // is too short to form a trigram.
  EXPECT_THAT(itr->ResetToStart(), IsFalse());
}

}  // namespace

}  // namespace lib
}  // namespace icing
