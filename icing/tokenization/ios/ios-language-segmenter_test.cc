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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/icu-i18n-test-utils.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/tokenization/language-segmenter.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {
namespace {
using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::IsEmpty;

// Returns a vector containing all terms retrieved by Advancing on the iterator.
std::vector<std::string_view> GetAllTermsAdvance(
    LanguageSegmenter::Iterator* itr) {
  std::vector<std::string_view> terms;
  while (itr->Advance()) {
    terms.push_back(itr->GetTerm());
  }
  return terms;
}

// Returns a vector containing all terms retrieved by calling
// ResetToStart/ResetAfter with the current position to simulate Advancing on
// the iterator.
std::vector<std::string_view> GetAllTermsResetAfter(
    LanguageSegmenter::Iterator* itr) {
  std::vector<std::string_view> terms;
  if (!itr->ResetToStart().ok()) {
    return terms;
  }
  terms.push_back(itr->GetTerm());
  const char* text_begin = itr->GetTerm().data();
  // Calling ResetToTermStartingAfter with the current position should get the
  // very next term in the sequence.
  for (int current_pos = 0; itr->ResetToTermStartingAfter(current_pos).ok();
       current_pos = itr->GetTerm().data() - text_begin) {
    terms.push_back(itr->GetTerm());
  }
  return terms;
}

// Returns a vector containing all terms retrieved by alternating calls to
// Advance and calls to ResetAfter with the current position to simulate
// Advancing.
std::vector<std::string_view> GetAllTermsAdvanceAndResetAfter(
    LanguageSegmenter::Iterator* itr) {
  const char* text_begin = itr->GetTerm().data();
  std::vector<std::string_view> terms;

  bool is_ok = true;
  int current_pos = 0;
  while (is_ok) {
    // Alternate between using Advance and ResetToTermAfter.
    if (terms.size() % 2 == 0) {
      is_ok = itr->Advance();
    } else {
      // Calling ResetToTermStartingAfter with the current position should get
      // the very next term in the sequence.
      current_pos = itr->GetTerm().data() - text_begin;
      is_ok = itr->ResetToTermStartingAfter(current_pos).ok();
    }
    if (is_ok) {
      terms.push_back(itr->GetTerm());
    }
  }
  return terms;
}

// Returns a vector containing all terms retrieved by calling ResetBefore with
// the current position, starting at the end of the text. This vector should be
// in reverse order of GetAllTerms and missing the last term.
std::vector<std::string_view> GetAllTermsResetBefore(
    LanguageSegmenter::Iterator* itr) {
  const char* text_begin = itr->GetTerm().data();
  int last_pos = 0;
  while (itr->Advance()) {
    last_pos = itr->GetTerm().data() - text_begin;
  }
  std::vector<std::string_view> terms;
  // Calling ResetToTermEndingBefore with the current position should get the
  // previous term in the sequence.
  for (int current_pos = last_pos;
       itr->ResetToTermEndingBefore(current_pos).ok();
       current_pos = itr->GetTerm().data() - text_begin) {
    terms.push_back(itr->GetTerm());
  }
  return terms;
}

class IosLanguageSegmenterAllLocalesTest
    : public testing::TestWithParam<const char*> {
 protected:
  static std::string GetLocale() { return GetParam(); }
  static language_segmenter_factory::SegmenterOptions GetOptions() {
    return language_segmenter_factory::SegmenterOptions(GetLocale());
  }
};

TEST_P(IosLanguageSegmenterAllLocalesTest, EmptyText) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  EXPECT_THAT(language_segmenter->GetAllTerms(""), IsOkAndHolds(IsEmpty()));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, SimpleText) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  EXPECT_THAT(language_segmenter->GetAllTerms("Hello World"),
              IsOkAndHolds(ElementsAre("Hello", " ", "World")));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, ASCII_Punctuation) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  // ASCII punctuation marks are kept
  EXPECT_THAT(
      language_segmenter->GetAllTerms("Hello, World!!!"),
      IsOkAndHolds(ElementsAre("Hello", ",", " ", "World", "!", "!", "!")));
  EXPECT_THAT(language_segmenter->GetAllTerms("Open-source project"),
              IsOkAndHolds(ElementsAre("Open", "-", "source", " ", "project")));
  EXPECT_THAT(language_segmenter->GetAllTerms("100%"),
              IsOkAndHolds(ElementsAre("100", "%")));
  EXPECT_THAT(language_segmenter->GetAllTerms("A&B"),
              IsOkAndHolds(ElementsAre("A", "&", "B")));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, ASCII_SpecialCharacter) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  // ASCII special characters are kept
  EXPECT_THAT(language_segmenter->GetAllTerms("Pay $1000"),
              IsOkAndHolds(ElementsAre("Pay", " ", "$", "1000")));
  EXPECT_THAT(language_segmenter->GetAllTerms("A+B"),
              IsOkAndHolds(ElementsAre("A", "+", "B")));
  // 0x0009 is the unicode for tab (within ASCII range).
  std::string text_with_tab = absl_ports::StrCat(
      "Hello", UCharToString(0x0009), UCharToString(0x0009), "World");
  EXPECT_THAT(language_segmenter->GetAllTerms(text_with_tab),
              IsOkAndHolds(ElementsAre("Hello", UCharToString(0x0009),
                                       UCharToString(0x0009), "World")));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, Non_ASCII_Non_Alphabetic) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  // Full-width (non-ASCII) punctuation marks and special characters are left
  // out.
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<std::string_view> terms,
                             language_segmenter->GetAllTerms("。？·Hello！×"));
  EXPECT_THAT(terms, ElementsAre("Hello"));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, Acronym) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  // LOCALE DEVIATION!! When the locale is Japanese, internal periods are
  // considered word breaks.
  std::vector<std::string> exp_terms;
  if (GetOptions().locale == ULOC_JAPAN) {
    exp_terms = {"U", ".", "S", ".", " ", "Bank"};
  } else {
    exp_terms = {"U.S", ".", " ", "Bank"};
  }
  EXPECT_THAT(language_segmenter->GetAllTerms("U.S. Bank"),
              IsOkAndHolds(ElementsAreArray(exp_terms)));

  // LOCALE DEVIATION!! When the locale is Japanese, internal periods are
  // considered word breaks.
  if (GetOptions().locale == ULOC_JAPAN) {
    exp_terms = {"I", ".", "B", ".", "M", "."};
  } else {
    exp_terms = {"I.B.M", "."};
  }
  EXPECT_THAT(language_segmenter->GetAllTerms("I.B.M."),
              IsOkAndHolds(ElementsAreArray(exp_terms)));

  EXPECT_THAT(language_segmenter->GetAllTerms("I,B,M"),
              IsOkAndHolds(ElementsAre("I", ",", "B", ",", "M")));
  EXPECT_THAT(language_segmenter->GetAllTerms("I B M"),
              IsOkAndHolds(ElementsAre("I", " ", "B", " ", "M")));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, WordConnector) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  // According to unicode word break rules
  // WB6(https://unicode.org/reports/tr29/#WB6),
  // WB7(https://unicode.org/reports/tr29/#WB7), and a few others, some
  // punctuation characters are used as word connecters. That is, words don't
  // break before and after them. Here we just test some that we care about.

  // Word connecters
  EXPECT_THAT(language_segmenter->GetAllTerms("com.google.android"),
              IsOkAndHolds(ElementsAre("com.google.android")));
  // DIFFERENCE!! iOS doesn't agree that ':' is a word connector
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<std::string_view> term,
      language_segmenter->GetAllTerms("com:google:android"));
  EXPECT_THAT(term, ElementsAre("com", ":", "google", ":", "android"));
  EXPECT_THAT(language_segmenter->GetAllTerms("com'google'android"),
              IsOkAndHolds(ElementsAre("com'google'android")));
  EXPECT_THAT(language_segmenter->GetAllTerms("com_google_android"),
              IsOkAndHolds(ElementsAre("com_google_android")));

  // Word connecters can be mixed
  // DIFFERENCE!! iOS doesn't agree that ':' is a word connector
  // TODO(b/157565185) resolve the handling of ':' as a connector.
  EXPECT_THAT(language_segmenter->GetAllTerms("com.google.android:icing"),
              IsOkAndHolds(ElementsAre("com.google.android", ":", "icing")));

  // Any heading and trailing characters are not connecters
  EXPECT_THAT(language_segmenter->GetAllTerms(".com.google.android."),
              IsOkAndHolds(ElementsAre(".", "com.google.android", ".")));

  // Not word connecters
  EXPECT_THAT(language_segmenter->GetAllTerms("com,google,android"),
              IsOkAndHolds(ElementsAre("com", ",", "google", ",", "android")));
  EXPECT_THAT(language_segmenter->GetAllTerms("com-google-android"),
              IsOkAndHolds(ElementsAre("com", "-", "google", "-", "android")));
  EXPECT_THAT(language_segmenter->GetAllTerms("com+google+android"),
              IsOkAndHolds(ElementsAre("com", "+", "google", "+", "android")));
  EXPECT_THAT(language_segmenter->GetAllTerms("com*google*android"),
              IsOkAndHolds(ElementsAre("com", "*", "google", "*", "android")));
  EXPECT_THAT(language_segmenter->GetAllTerms("com@google@android"),
              IsOkAndHolds(ElementsAre("com", "@", "google", "@", "android")));
  EXPECT_THAT(language_segmenter->GetAllTerms("com^google^android"),
              IsOkAndHolds(ElementsAre("com", "^", "google", "^", "android")));
  EXPECT_THAT(language_segmenter->GetAllTerms("com&google&android"),
              IsOkAndHolds(ElementsAre("com", "&", "google", "&", "android")));
  EXPECT_THAT(language_segmenter->GetAllTerms("com|google|android"),
              IsOkAndHolds(ElementsAre("com", "|", "google", "|", "android")));
  EXPECT_THAT(language_segmenter->GetAllTerms("com/google/android"),
              IsOkAndHolds(ElementsAre("com", "/", "google", "/", "android")));
  EXPECT_THAT(language_segmenter->GetAllTerms("com;google;android"),
              IsOkAndHolds(ElementsAre("com", ";", "google", ";", "android")));
  EXPECT_THAT(
      language_segmenter->GetAllTerms("com\"google\"android"),
      IsOkAndHolds(ElementsAre("com", "\"", "google", "\"", "android")));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, Apostrophes) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  EXPECT_THAT(language_segmenter->GetAllTerms("It's ok."),
              IsOkAndHolds(ElementsAre("It's", " ", "ok", ".")));
  EXPECT_THAT(language_segmenter->GetAllTerms("He'll be back."),
              IsOkAndHolds(ElementsAre("He'll", " ", "be", " ", "back", ".")));
  EXPECT_THAT(language_segmenter->GetAllTerms("'Hello 'World."),
              IsOkAndHolds(ElementsAre("'", "Hello", " ", "'", "World", ".")));
  EXPECT_THAT(language_segmenter->GetAllTerms("The dogs' bone"),
              IsOkAndHolds(ElementsAre("The", " ", "dogs", "'", " ", "bone")));
  // 0x2019 is the single right quote, should be treated the same as "'"
  std::string token_with_quote =
      absl_ports::StrCat("He", UCharToString(0x2019), "ll");
  std::string text_with_quote =
      absl_ports::StrCat(token_with_quote, " be back.");
  EXPECT_THAT(
      language_segmenter->GetAllTerms(text_with_quote),
      IsOkAndHolds(ElementsAre(token_with_quote, " ", "be", " ", "back", ".")));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, Parentheses) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));

  EXPECT_THAT(language_segmenter->GetAllTerms("(Hello)"),
              IsOkAndHolds(ElementsAre("(", "Hello", ")")));

  EXPECT_THAT(language_segmenter->GetAllTerms(")Hello("),
              IsOkAndHolds(ElementsAre(")", "Hello", "(")));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, Quotes) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));

  EXPECT_THAT(language_segmenter->GetAllTerms("\"Hello\""),
              IsOkAndHolds(ElementsAre("\"", "Hello", "\"")));

  EXPECT_THAT(language_segmenter->GetAllTerms("'Hello'"),
              IsOkAndHolds(ElementsAre("'", "Hello", "'")));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, Alphanumeric) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));

  // Alphanumeric terms are allowed
  EXPECT_THAT(language_segmenter->GetAllTerms("Se7en A4 3a"),
              IsOkAndHolds(ElementsAre("Se7en", " ", "A4", " ", "3a")));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, Number) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));

  // Alphanumeric terms are allowed
  EXPECT_THAT(
      language_segmenter->GetAllTerms("3.141592653589793238462643383279"),
      IsOkAndHolds(ElementsAre("3.141592653589793238462643383279")));

  EXPECT_THAT(language_segmenter->GetAllTerms("3,456.789"),
              IsOkAndHolds(ElementsAre("3,456.789")));

  EXPECT_THAT(language_segmenter->GetAllTerms("-123"),
              IsOkAndHolds(ElementsAre("-", "123")));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, ContinuousWhitespaces) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  // Multiple continuous whitespaces are treated as one.
  const int kNumSeparators = 256;
  std::string text_with_spaces =
      absl_ports::StrCat("Hello", std::string(kNumSeparators, ' '), "World");
  EXPECT_THAT(language_segmenter->GetAllTerms(text_with_spaces),
              IsOkAndHolds(ElementsAre("Hello", " ", "World")));

  // Multiple continuous whitespaces are treated as one. Whitespace at the
  // beginning of the text doesn't affect the results of GetTerm() after the
  // iterator is done.
  text_with_spaces = absl_ports::StrCat(std::string(kNumSeparators, ' '),
                                        "Hello", " ", "World");
  ICING_ASSERT_OK_AND_ASSIGN(auto itr,
                             language_segmenter->Segment(text_with_spaces));
  std::vector<std::string_view> terms;
  while (itr->Advance()) {
    terms.push_back(itr->GetTerm());
  }
  EXPECT_THAT(terms, ElementsAre(" ", "Hello", " ", "World"));
  EXPECT_THAT(itr->GetTerm(), IsEmpty());
}

TEST_P(IosLanguageSegmenterAllLocalesTest, CJKT) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  // CJKT (Chinese, Japanese, Khmer, Thai) are the 4 main languages that don't
  // have whitespaces as word delimiter.

  // Chinese
  // DIFFERENCE/LOCALE DEVIATION!! SIMPLISTIC_CHINESE agrees with ICU that
  // "每天" should be treated as a single token. All other locales split it into
  // two tokens.
  std::vector<std::string> exp_terms;
  if (GetOptions().locale == ULOC_SIMPLIFIED_CHINESE) {
    exp_terms = {"我", "每天", "走路", "去", "上班"};
  } else if (GetOptions().locale == ULOC_JAPAN) {
    // LOCALE DEVIATION!! JAPANESE groups "去上" and leaves "班" on its own.
    // All other locales which, like ICU, breaks the text into "去" and "上班".
    exp_terms = {"我", "每", "天", "走路", "去上", "班"};
  } else {
    exp_terms = {"我", "每", "天", "走路", "去", "上班"};
  }
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<std::string_view> terms,
      language_segmenter->GetAllTerms("我每天走路去上班。"));
  EXPECT_THAT(terms, ElementsAreArray(exp_terms));

  // Japanese
  // DIFFERENCE!! Disagreement over how to segment "歩い" (iOS groups) and
  // "てい" (iOS splits). This difference persists even when locale is set to
  // JAPAN.
  if (GetOptions().locale == ULOC_SIMPLIFIED_CHINESE ||
      GetOptions().locale == ULOC_TRADITIONAL_CHINESE) {
    // LOCALE DEVIATION!! There is also disagreement when locale is CHINESE
    // about how to tokenize "毎日", "仕事", "歩い", which are all split, and
    // "てい" which is grouped.
    exp_terms = {"私", "は", "毎", "日",   "仕",  "事",
                 "に", "歩", "い", "てい", "ます"};
  } else {
    exp_terms = {"私", "は", "毎日", "仕事", "に", "歩い", "て", "い", "ます"};
  }
  ICING_ASSERT_OK_AND_ASSIGN(
      terms, language_segmenter->GetAllTerms("私は毎日仕事に歩いています。"));
  EXPECT_THAT(terms, ElementsAreArray(exp_terms));

  // Khmer
  ICING_ASSERT_OK_AND_ASSIGN(
      terms, language_segmenter->GetAllTerms("ញុំដើរទៅធ្វើការរាល់ថ្ងៃ។"));
  EXPECT_THAT(terms, ElementsAre("ញុំ", "ដើរទៅ", "ធ្វើការ", "រាល់ថ្ងៃ"));

  // Thai
  // DIFFERENCE!! Disagreement over how to segment "ทุกวัน" (iOS groups).
  // This difference persists even when locale is set to THAI
  ICING_ASSERT_OK_AND_ASSIGN(
      terms, language_segmenter->GetAllTerms("ฉันเดินไปทำงานทุกวัน"));
  EXPECT_THAT(terms, ElementsAre("ฉัน", "เดิน", "ไป", "ทำงาน", "ทุกวัน"));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, LatinLettersWithAccents) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  EXPECT_THAT(language_segmenter->GetAllTerms("āăąḃḅḇčćç"),
              IsOkAndHolds(ElementsAre("āăąḃḅḇčćç")));
}

// TODO(samzheng): test cases for more languages (e.g. top 20 in the world)
TEST_P(IosLanguageSegmenterAllLocalesTest, WhitespaceSplitLanguages) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  // Turkish
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<std::string_view> terms,
                             language_segmenter->GetAllTerms("merhaba dünya"));
  EXPECT_THAT(terms, ElementsAre("merhaba", " ", "dünya"));
  // Korean
  ICING_ASSERT_OK_AND_ASSIGN(
      terms, language_segmenter->GetAllTerms("나는 매일 출근합니다."));
  EXPECT_THAT(terms, ElementsAre("나는", " ", "매일", " ", "출근합니다", "."));
}

// TODO(samzheng): more mixed languages test cases
TEST_P(IosLanguageSegmenterAllLocalesTest, MixedLanguages) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  // DIFFERENCE/LOCALE DEVIATION!! JAPANESE agrees with ICU that "你好" should
  // be treated as a single token. All other locales other than
  // SIMPLIFIED_CHINESE split it into two tokens.
  std::vector<std::string> exp_terms;
  if (GetOptions().locale == ULOC_JAPAN) {
    exp_terms = {"How", " ",  "are",  " ",    "you", "你好",
                 "吗",  "お", "元気", "です", "か"};
  } else if (GetOptions().locale == ULOC_TRADITIONAL_CHINESE) {
    // LOCALE DEVIATION!! TRADITIONAL_CHINESE disagrees over tokenization of
    // "你好" and "元気", both of which it breaks up.
    exp_terms = {"How", " ",  "are", " ",  "you",  "你", "好",
                 "吗",  "お", "元",  "気", "です", "か"};
  } else if (GetOptions().locale == ULOC_SIMPLIFIED_CHINESE) {
    // LOCALE DEVIATION!! SIMPLIFIED_CHINESE disagrees over tokenization of
    // "元気", which it breaks up.
    exp_terms = {"How", " ",  "are", " ",  "you",  "你好",
                 "吗",  "お", "元",  "気", "です", "か"};
  } else {
    // LOCALE DEVIATION!! All other locales disagree over the tokenization of
    // "你好", which it breaks up.
    exp_terms = {"How", " ",  "are", " ",    "you",  "你",
                 "好",  "吗", "お",  "元気", "です", "か"};
  }
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<std::string_view> terms,
      language_segmenter->GetAllTerms("How are you你好吗お元気ですか"));
  EXPECT_THAT(terms, ElementsAreArray(exp_terms));

  ICING_ASSERT_OK_AND_ASSIGN(
      terms, language_segmenter->GetAllTerms("나는 California에 산다"));
  EXPECT_THAT(terms, ElementsAre("나는", " ", "California", "에", " ", "산다"));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, NotCopyStrings) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  // Validates that the input strings are not copied
  const std::string text = "Hello World";
  const char* word1_address = text.c_str();
  const char* word2_address = text.c_str() + 6;
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<std::string_view> terms,
                             language_segmenter->GetAllTerms(text));
  ASSERT_THAT(terms, ElementsAre("Hello", " ", "World"));
  const char* word1_result_address = terms.at(0).data();
  const char* word2_result_address = terms.at(2).data();

  // The underlying char* should be the same
  EXPECT_THAT(word1_address, Eq(word1_result_address));
  EXPECT_THAT(word2_address, Eq(word2_result_address));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, NewIteratorResetToStart) {
  ICING_ASSERT_OK_AND_ASSIGN(auto segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  constexpr std::string_view kText = "How are you你好吗お元気ですか";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<LanguageSegmenter::Iterator> itr,
                             segmenter->Segment(kText));

  // String: "How are you你好吗お元気ですか"
  //          ^  ^^  ^^  ^  ^ ^ ^  ^  ^
  // Bytes:   0  3 4 7 8 11 172023 29 35
  EXPECT_THAT(itr->ResetToStart(), IsOkAndHolds(Eq(0)));
  EXPECT_THAT(itr->GetTerm(), Eq("How"));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, IteratorOneAdvanceResetToStart) {
  ICING_ASSERT_OK_AND_ASSIGN(auto segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  constexpr std::string_view kText = "How are you你好吗お元気ですか";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<LanguageSegmenter::Iterator> itr,
                             segmenter->Segment(kText));

  // String: "How are you你好吗お元気ですか"
  //          ^  ^^  ^^  ^  ^ ^ ^  ^  ^
  // Bytes:   0  3 4 7 8 11 172023 29 35
  ASSERT_TRUE(itr->Advance());  // itr points to 'How'
  EXPECT_THAT(itr->ResetToStart(), IsOkAndHolds(Eq(0)));
  EXPECT_THAT(itr->GetTerm(), Eq("How"));
}

TEST_P(IosLanguageSegmenterAllLocalesTest,
       IteratorMultipleAdvancesResetToStart) {
  ICING_ASSERT_OK_AND_ASSIGN(auto segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  constexpr std::string_view kText = "How are you你好吗お元気ですか";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<LanguageSegmenter::Iterator> itr,
                             segmenter->Segment(kText));

  // String: "How are you你好吗お元気ですか"
  //          ^  ^^  ^^  ^  ^ ^ ^  ^  ^
  // Bytes:   0  3 4 7 8 11 172023 29 35
  ASSERT_TRUE(itr->Advance());
  ASSERT_TRUE(itr->Advance());
  ASSERT_TRUE(itr->Advance());
  ASSERT_TRUE(itr->Advance());  // itr points to ' '
  EXPECT_THAT(itr->ResetToStart(), IsOkAndHolds(Eq(0)));
  EXPECT_THAT(itr->GetTerm(), Eq("How"));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, IteratorDoneResetToStart) {
  ICING_ASSERT_OK_AND_ASSIGN(auto segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  constexpr std::string_view kText = "How are you你好吗お元気ですか";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<LanguageSegmenter::Iterator> itr,
                             segmenter->Segment(kText));

  // String: "How are you你好吗お元気ですか"
  //          ^  ^^  ^^  ^  ^ ^ ^  ^  ^
  // Bytes:   0  3 4 7 8 11 172023 29 35
  while (itr->Advance()) {
    // Do nothing.
  }
  EXPECT_THAT(itr->ResetToStart(), IsOkAndHolds(Eq(0)));
  EXPECT_THAT(itr->GetTerm(), Eq("How"));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, ResetToTermAfterOutOfBounds) {
  ICING_ASSERT_OK_AND_ASSIGN(auto segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  constexpr std::string_view kText = "How are you你好吗お元気ですか";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<LanguageSegmenter::Iterator> itr,
                             segmenter->Segment(kText));

  // String: "How are you你好吗お元気ですか"
  //          ^  ^^  ^^  ^  ^ ^ ^  ^  ^
  // Bytes:   0  3 4 7 8 11 172023 29 35
  auto position_or = itr->ResetToTermStartingAfter(7);
  EXPECT_THAT(position_or, IsOk());
  EXPECT_THAT(position_or.ValueOrDie(), Eq(8));
  ASSERT_THAT(itr->GetTerm(), Eq("you"));

  EXPECT_THAT(itr->ResetToTermStartingAfter(-1),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(itr->GetTerm(), Eq("you"));

  EXPECT_THAT(itr->ResetToTermStartingAfter(kText.length()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(itr->GetTerm(), Eq("you"));
}

// Tests that ResetToTermAfter and Advance produce the same output. With the
// exception of the first term which is inacessible via ResetToTermAfter,
// the stream of terms produced by Advance calls should exacly match the
// terms produced by ResetToTermAfter calls with the current position
// provided as the argument.
TEST_P(IosLanguageSegmenterAllLocalesTest,
       MixedLanguagesResetToTermAfterEquivalentToAdvance) {
  ICING_ASSERT_OK_AND_ASSIGN(auto segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  constexpr std::string_view kText = "How are𡔖 you你好吗お元気ですか";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> advance_itr,
      segmenter->Segment(kText));
  std::vector<std::string_view> advance_terms =
      GetAllTermsAdvance(advance_itr.get());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> reset_to_term_itr,
      segmenter->Segment(kText));
  std::vector<std::string_view> reset_terms =
      GetAllTermsResetAfter(reset_to_term_itr.get());

  EXPECT_THAT(reset_terms, testing::ElementsAreArray(advance_terms));
  EXPECT_THAT(reset_to_term_itr->GetTerm(), Eq(advance_itr->GetTerm()));
}

TEST_P(IosLanguageSegmenterAllLocalesTest,
       ThaiResetToTermAfterEquivalentToAdvance) {
  ICING_ASSERT_OK_AND_ASSIGN(auto segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  constexpr std::string_view kThai = "ฉันเดินไปทำงานทุกวัน";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> advance_itr,
      segmenter->Segment(kThai));
  std::vector<std::string_view> advance_terms =
      GetAllTermsAdvance(advance_itr.get());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> reset_to_term_itr,
      segmenter->Segment(kThai));
  std::vector<std::string_view> reset_terms =
      GetAllTermsResetAfter(reset_to_term_itr.get());

  EXPECT_THAT(reset_terms, testing::ElementsAreArray(advance_terms));
  EXPECT_THAT(reset_to_term_itr->GetTerm(), Eq(advance_itr->GetTerm()));
}

TEST_P(IosLanguageSegmenterAllLocalesTest,
       KoreanResetToTermAfterEquivalentToAdvance) {
  ICING_ASSERT_OK_AND_ASSIGN(auto segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  constexpr std::string_view kKorean = "나는 매일 출근합니다.";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> advance_itr,
      segmenter->Segment(kKorean));
  std::vector<std::string_view> advance_terms =
      GetAllTermsAdvance(advance_itr.get());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> reset_to_term_itr,
      segmenter->Segment(kKorean));
  std::vector<std::string_view> reset_terms =
      GetAllTermsResetAfter(reset_to_term_itr.get());

  EXPECT_THAT(reset_terms, testing::ElementsAreArray(advance_terms));
  EXPECT_THAT(reset_to_term_itr->GetTerm(), Eq(advance_itr->GetTerm()));
}

// Tests that ResetToTermAfter and Advance can be used in conjunction. Just as
// ResetToTermAfter(current_position) can be used to simulate Advance, users
// should be able to mix ResetToTermAfter(current_position) calls and Advance
// calls to mimic calling Advance.
TEST_P(IosLanguageSegmenterAllLocalesTest,
       MixedLanguagesResetToTermAfterInteroperableWithAdvance) {
  ICING_ASSERT_OK_AND_ASSIGN(auto segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  constexpr std::string_view kText = "How are𡔖 you你好吗お元気ですか";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> advance_itr,
      segmenter->Segment(kText));
  std::vector<std::string_view> advance_terms =
      GetAllTermsAdvance(advance_itr.get());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> advance_and_reset_itr,
      segmenter->Segment(kText));
  std::vector<std::string_view> advance_and_reset_terms =
      GetAllTermsAdvanceAndResetAfter(advance_and_reset_itr.get());

  EXPECT_THAT(advance_and_reset_terms,
              testing::ElementsAreArray(advance_terms));
  EXPECT_THAT(advance_and_reset_itr->GetTerm(), Eq(advance_itr->GetTerm()));
}

TEST_P(IosLanguageSegmenterAllLocalesTest,
       ThaiResetToTermAfterInteroperableWithAdvance) {
  ICING_ASSERT_OK_AND_ASSIGN(auto segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  constexpr std::string_view kThai = "ฉันเดินไปทำงานทุกวัน";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> advance_itr,
      segmenter->Segment(kThai));
  std::vector<std::string_view> advance_terms =
      GetAllTermsAdvance(advance_itr.get());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> advance_and_reset_itr,
      segmenter->Segment(kThai));
  std::vector<std::string_view> advance_and_reset_terms =
      GetAllTermsAdvanceAndResetAfter(advance_and_reset_itr.get());

  EXPECT_THAT(advance_and_reset_terms,
              testing::ElementsAreArray(advance_terms));
  EXPECT_THAT(advance_and_reset_itr->GetTerm(), Eq(advance_itr->GetTerm()));
}

TEST_P(IosLanguageSegmenterAllLocalesTest,
       KoreanResetToTermAfterInteroperableWithAdvance) {
  ICING_ASSERT_OK_AND_ASSIGN(auto segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  constexpr std::string_view kKorean = "나는 매일 출근합니다.";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> advance_itr,
      segmenter->Segment(kKorean));
  std::vector<std::string_view> advance_terms =
      GetAllTermsAdvance(advance_itr.get());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> advance_and_reset_itr,
      segmenter->Segment(kKorean));
  std::vector<std::string_view> advance_and_reset_terms =
      GetAllTermsAdvanceAndResetAfter(advance_and_reset_itr.get());

  EXPECT_THAT(advance_and_reset_terms,
              testing::ElementsAreArray(advance_terms));
  EXPECT_THAT(advance_and_reset_itr->GetTerm(), Eq(advance_itr->GetTerm()));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, MixedLanguagesResetToTermAfter) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> itr,
      language_segmenter->Segment("How are you你好吗お元気ですか"));

  // String: "How are you你好吗お元気ですか"
  //          ^  ^^  ^^  ^ ^^ ^ ^  ^  ^
  // Bytes:   0  3 4 78 1114172023 29 35
  EXPECT_THAT(itr->ResetToTermStartingAfter(2), IsOkAndHolds(Eq(3)));
  EXPECT_THAT(itr->GetTerm(), Eq(" "));

  // DIFFERENCE/LOCALE DEVIATION!! JAPANESE and SIMPLIFIED_CHINESE agrees with
  // ICU that "你好" should be treated as a single token. All other locales
  // other than SIMPLIFIED_CHINESE split it into two tokens.
  std::string exp_token;
  if (GetLocale() == ULOC_JAPAN || GetLocale() == ULOC_SIMPLIFIED_CHINESE) {
    exp_token = "你好";
  } else {
    exp_token = "你";
  }
  EXPECT_THAT(itr->ResetToTermStartingAfter(10), IsOkAndHolds(Eq(11)));
  EXPECT_THAT(itr->GetTerm(), Eq(exp_token));

  EXPECT_THAT(itr->ResetToTermStartingAfter(7), IsOkAndHolds(Eq(8)));
  EXPECT_THAT(itr->GetTerm(), Eq("you"));

  EXPECT_THAT(itr->ResetToTermStartingAfter(32), IsOkAndHolds(Eq(35)));
  EXPECT_THAT(itr->GetTerm(), Eq("か"));

  EXPECT_THAT(itr->ResetToTermStartingAfter(14), IsOkAndHolds(Eq(17)));
  EXPECT_THAT(itr->GetTerm(), Eq("吗"));

  EXPECT_THAT(itr->ResetToTermStartingAfter(0), IsOkAndHolds(Eq(3)));
  EXPECT_THAT(itr->GetTerm(), Eq(" "));

  EXPECT_THAT(itr->ResetToTermStartingAfter(35),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(itr->GetTerm(), IsEmpty());
}

TEST_P(IosLanguageSegmenterAllLocalesTest,
       ContinuousWhitespacesResetToTermAfter) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  // Multiple continuous whitespaces are treated as one.
  constexpr std::string_view kTextWithSpace = "Hello          World";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<LanguageSegmenter::Iterator> itr,
                             language_segmenter->Segment(kTextWithSpace));

  // String: "Hello          World"
  //          ^    ^         ^
  // Bytes:   0    5         15
  auto offset_or = itr->ResetToTermStartingAfter(0);
  EXPECT_THAT(offset_or.status(), IsOk());
  EXPECT_THAT(offset_or.ValueOrDie(), Eq(5));
  EXPECT_THAT(itr->GetTerm(), Eq(" "));

  EXPECT_THAT(itr->ResetToTermStartingAfter(2), IsOkAndHolds(Eq(5)));
  EXPECT_THAT(itr->GetTerm(), Eq(" "));

  EXPECT_THAT(itr->ResetToTermStartingAfter(10), IsOkAndHolds(Eq(15)));
  EXPECT_THAT(itr->GetTerm(), Eq("World"));

  EXPECT_THAT(itr->ResetToTermStartingAfter(5), IsOkAndHolds(Eq(15)));
  EXPECT_THAT(itr->GetTerm(), Eq("World"));

  EXPECT_THAT(itr->ResetToTermStartingAfter(15),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(itr->GetTerm(), IsEmpty());

  EXPECT_THAT(itr->ResetToTermStartingAfter(17),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(itr->GetTerm(), IsEmpty());

  EXPECT_THAT(itr->ResetToTermStartingAfter(19),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(itr->GetTerm(), IsEmpty());
}

TEST_P(IosLanguageSegmenterAllLocalesTest, ChineseResetToTermAfter) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  // CJKT (Chinese, Japanese, Khmer, Thai) are the 4 main languages that
  // don't have whitespaces as word delimiter. Chinese
  constexpr std::string_view kChinese = "我每天走路去上班。";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<LanguageSegmenter::Iterator> itr,
                             language_segmenter->Segment(kChinese));
  // String: "我每天走路去上班。"
  //          ^ ^^ ^   ^^
  // Bytes:   0 3 6 9 15 18
  std::string exp_token;
  // DIFFERENCE/LOCALE DEVIATION!! SIMPLISTIC_CHINESE agrees with ICU that
  // "每天" should be treated as a single token. All other locales split it into
  // two tokens.
  if (GetLocale() == ULOC_SIMPLIFIED_CHINESE) {
    exp_token = "每天";
  } else {
    exp_token = "每";
  }
  EXPECT_THAT(itr->ResetToTermStartingAfter(0), IsOkAndHolds(Eq(3)));
  EXPECT_THAT(itr->GetTerm(), Eq(exp_token));

  EXPECT_THAT(itr->ResetToTermStartingAfter(7), IsOkAndHolds(Eq(9)));
  EXPECT_THAT(itr->GetTerm(), Eq("走路"));

  EXPECT_THAT(itr->ResetToTermStartingAfter(21),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(itr->GetTerm(), IsEmpty());
}

TEST_P(IosLanguageSegmenterAllLocalesTest, JapaneseResetToTermAfter) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  // Japanese
  constexpr std::string_view kJapanese = "私は毎日仕事に歩いています。";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<LanguageSegmenter::Iterator> itr,
                             language_segmenter->Segment(kJapanese));
  // String: "私は毎日仕事に歩いています。"
  //          ^ ^ ^  ^  ^ ^ ^ ^  ^
  // Bytes:   0 3 6  12 18212427 33
  EXPECT_THAT(itr->ResetToTermStartingAfter(0), IsOkAndHolds(Eq(3)));
  EXPECT_THAT(itr->GetTerm(), Eq("は"));

  EXPECT_THAT(itr->ResetToTermStartingAfter(33),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(itr->GetTerm(), IsEmpty());

  // LOCALE DEVIATION!! There is disagreement when locale is CHINESE about how
  // to tokenize "毎日", "仕事", "歩い", which are all split, and "てい" which
  // is grouped.
  std::string exp_term;
  int exp_offset;
  if (GetLocale() == ULOC_SIMPLIFIED_CHINESE ||
      GetLocale() == ULOC_TRADITIONAL_CHINESE) {
    // Since "毎日" is broken up when the locale is CHINESE, ResetAfter(7) will
    // point to "日" instead of the next segment ("仕事") like other locales.
    exp_term = "日";
    exp_offset = 9;
  } else {
    exp_term = "仕事";
    exp_offset = 12;
  }
  EXPECT_THAT(itr->ResetToTermStartingAfter(7), IsOkAndHolds(Eq(exp_offset)));
  EXPECT_THAT(itr->GetTerm(), Eq(exp_term));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, KhmerResetToTermAfter) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  constexpr std::string_view kKhmer = "ញុំដើរទៅធ្វើការរាល់ថ្ងៃ។";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<LanguageSegmenter::Iterator> itr,
                             language_segmenter->Segment(kKhmer));
  // String: "ញុំដើរទៅធ្វើការរាល់ថ្ងៃ។"
  //          ^ ^   ^   ^
  // Bytes:   0 9   24  45
  EXPECT_THAT(itr->ResetToTermStartingAfter(0), IsOkAndHolds(Eq(9)));
  EXPECT_THAT(itr->GetTerm(), Eq("ដើរទៅ"));

  EXPECT_THAT(itr->ResetToTermStartingAfter(47),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(itr->GetTerm(), IsEmpty());

  EXPECT_THAT(itr->ResetToTermStartingAfter(14), IsOkAndHolds(Eq(24)));
  EXPECT_THAT(itr->GetTerm(), Eq("ធ្វើការ"));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, ThaiResetToTermAfter) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  // Thai
  constexpr std::string_view kThai = "ฉันเดินไปทำงานทุกวัน";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<LanguageSegmenter::Iterator> itr,
                             language_segmenter->Segment(kThai));
  // String: "ฉันเดินไปทำงานทุกวัน"
  //          ^ ^  ^ ^    ^
  // Bytes:   0 9 21 27  42
  EXPECT_THAT(itr->ResetToTermStartingAfter(0), IsOkAndHolds(Eq(9)));
  EXPECT_THAT(itr->GetTerm(), Eq("เดิน"));

  EXPECT_THAT(itr->ResetToTermStartingAfter(51),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(itr->GetTerm(), IsEmpty());

  EXPECT_THAT(itr->ResetToTermStartingAfter(13), IsOkAndHolds(Eq(21)));
  EXPECT_THAT(itr->GetTerm(), Eq("ไป"));

  // DIFFERENCE!! Disagreement over how to segment "ทุกวัน" (iOS groups).
  // This difference persists even when locale is set to THAI
  EXPECT_THAT(itr->ResetToTermStartingAfter(34), IsOkAndHolds(Eq(42)));
  EXPECT_THAT(itr->GetTerm(), Eq("ทุกวัน"));
}
TEST_P(IosLanguageSegmenterAllLocalesTest, ResetToTermBeforeOutOfBounds) {
  ICING_ASSERT_OK_AND_ASSIGN(auto segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  constexpr std::string_view kText = "How are you你好吗お元気ですか";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<LanguageSegmenter::Iterator> itr,
                             segmenter->Segment(kText));

  // String: "How are you你好吗お元気ですか"
  //          ^  ^^  ^^  ^  ^ ^ ^  ^  ^
  // Bytes:   0  3 4 7 8 11 172023 29 35
  ASSERT_THAT(itr->ResetToTermEndingBefore(7), IsOkAndHolds(Eq(4)));
  ASSERT_THAT(itr->GetTerm(), Eq("are"));

  EXPECT_THAT(itr->ResetToTermEndingBefore(-1),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(itr->GetTerm(), Eq("are"));

  EXPECT_THAT(itr->ResetToTermEndingBefore(kText.length()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(itr->GetTerm(), Eq("are"));
}

// Tests that ResetToTermBefore and Advance produce the same output. With the
// exception of the last term which is inacessible via ResetToTermBefore,
// the stream of terms produced by Advance calls should exacly match the
// terms produced by ResetToTermBefore calls with the current position
// provided as the argument (after their order has been reversed).
TEST_P(IosLanguageSegmenterAllLocalesTest,
       MixedLanguagesResetToTermBeforeEquivalentToAdvance) {
  ICING_ASSERT_OK_AND_ASSIGN(auto segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  constexpr std::string_view kText = "How are𡔖 you你好吗お元気ですか";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> advance_itr,
      segmenter->Segment(kText));
  std::vector<std::string_view> advance_terms =
      GetAllTermsAdvance(advance_itr.get());
  // Can't produce the last term via calls to ResetToTermBefore. So skip
  // past that one.
  auto itr = advance_terms.begin();
  std::advance(itr, advance_terms.size() - 1);
  advance_terms.erase(itr);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> reset_to_term_itr,
      segmenter->Segment(kText));
  std::vector<std::string_view> reset_terms =
      GetAllTermsResetBefore(reset_to_term_itr.get());
  std::reverse(reset_terms.begin(), reset_terms.end());

  EXPECT_THAT(reset_terms, testing::ElementsAreArray(advance_terms));
  EXPECT_THAT(reset_to_term_itr->GetTerm(), IsEmpty());
  EXPECT_THAT(advance_itr->GetTerm(), IsEmpty());
}

TEST_P(IosLanguageSegmenterAllLocalesTest,
       ThaiResetToTermBeforeEquivalentToAdvance) {
  ICING_ASSERT_OK_AND_ASSIGN(auto segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  constexpr std::string_view kThai = "ฉันเดินไปทำงานทุกวัน";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> advance_itr,
      segmenter->Segment(kThai));
  std::vector<std::string_view> advance_terms =
      GetAllTermsAdvance(advance_itr.get());
  // Can't produce the last term via calls to ResetToTermBefore. So skip
  // past that one.
  auto itr = advance_terms.begin();
  std::advance(itr, advance_terms.size() - 1);
  advance_terms.erase(itr);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> reset_to_term_itr,
      segmenter->Segment(kThai));
  std::vector<std::string_view> reset_terms =
      GetAllTermsResetBefore(reset_to_term_itr.get());
  std::reverse(reset_terms.begin(), reset_terms.end());

  EXPECT_THAT(reset_terms, testing::ElementsAreArray(advance_terms));
  EXPECT_THAT(reset_to_term_itr->GetTerm(), Eq(advance_itr->GetTerm()));
}

TEST_P(IosLanguageSegmenterAllLocalesTest,
       KoreanResetToTermBeforeEquivalentToAdvance) {
  ICING_ASSERT_OK_AND_ASSIGN(auto segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  constexpr std::string_view kKorean = "나는 매일 출근합니다.";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> advance_itr,
      segmenter->Segment(kKorean));
  std::vector<std::string_view> advance_terms =
      GetAllTermsAdvance(advance_itr.get());
  // Can't produce the last term via calls to ResetToTermBefore. So skip
  // past that one.
  auto itr = advance_terms.begin();
  std::advance(itr, advance_terms.size() - 1);
  advance_terms.erase(itr);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> reset_to_term_itr,
      segmenter->Segment(kKorean));
  std::vector<std::string_view> reset_terms =
      GetAllTermsResetBefore(reset_to_term_itr.get());
  std::reverse(reset_terms.begin(), reset_terms.end());

  EXPECT_THAT(reset_terms, testing::ElementsAreArray(advance_terms));
  EXPECT_THAT(reset_to_term_itr->GetTerm(), Eq(advance_itr->GetTerm()));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, MixedLanguagesResetToTermBefore) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<LanguageSegmenter::Iterator> itr,
      language_segmenter->Segment("How are you你好吗お元気ですか"));

  // String: "How are you你好吗お元気ですか"
  //          ^  ^^  ^^  ^  ^ ^ ^  ^  ^
  // Bytes:   0  3 4 7 8 11 172023 29 35
  EXPECT_THAT(itr->ResetToTermEndingBefore(2),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(itr->GetTerm(), IsEmpty());

  EXPECT_THAT(itr->ResetToTermEndingBefore(10), IsOkAndHolds(Eq(7)));
  EXPECT_THAT(itr->GetTerm(), Eq(" "));

  EXPECT_THAT(itr->ResetToTermEndingBefore(7), IsOkAndHolds(Eq(4)));
  EXPECT_THAT(itr->GetTerm(), Eq("are"));

  std::string exp_token;
  int exp_offset;
  if (GetOptions().locale == ULOC_TRADITIONAL_CHINESE ||
      GetOptions().locale == ULOC_SIMPLIFIED_CHINESE) {
    // LOCALE DEVIATION!! SIMPLIFIED_CHINESE disagrees over tokenization of
    // "元気", which it breaks up.
    exp_offset = 26;
    exp_token = "気";
  } else {
    exp_offset = 23;
    exp_token = "元気";
  }
  EXPECT_THAT(itr->ResetToTermEndingBefore(32), IsOkAndHolds(Eq(exp_offset)));
  EXPECT_THAT(itr->GetTerm(), Eq(exp_token));

  // DIFFERENCE/LOCALE DEVIATION!! JAPANESE and SIMPLIFIED_CHINESE agrees with
  // ICU that "你好" should be treated as a single token. All other locales
  // split it into two tokens.
  if (GetLocale() == ULOC_JAPAN || GetLocale() == ULOC_SIMPLIFIED_CHINESE) {
    exp_offset = 8;
    exp_token = "you";
  } else {
    exp_offset = 11;
    exp_token = "你";
  }
  EXPECT_THAT(itr->ResetToTermEndingBefore(14), IsOkAndHolds(Eq(exp_offset)));
  EXPECT_THAT(itr->GetTerm(), Eq(exp_token));

  EXPECT_THAT(itr->ResetToTermEndingBefore(0),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(itr->GetTerm(), IsEmpty());

  EXPECT_THAT(itr->ResetToTermEndingBefore(35), IsOkAndHolds(Eq(29)));
  EXPECT_THAT(itr->GetTerm(), Eq("です"));
}

TEST_P(IosLanguageSegmenterAllLocalesTest,
       ContinuousWhitespacesResetToTermBefore) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  // Multiple continuous whitespaces are treated as one.
  constexpr std::string_view kTextWithSpace = "Hello          World";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<LanguageSegmenter::Iterator> itr,
                             language_segmenter->Segment(kTextWithSpace));

  // String: "Hello          World"
  //          ^    ^         ^
  // Bytes:   0    5         15
  EXPECT_THAT(itr->ResetToTermEndingBefore(0),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(itr->GetTerm(), IsEmpty());

  EXPECT_THAT(itr->ResetToTermEndingBefore(2),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(itr->GetTerm(), IsEmpty());

  EXPECT_THAT(itr->ResetToTermEndingBefore(10), IsOkAndHolds(Eq(0)));
  EXPECT_THAT(itr->GetTerm(), Eq("Hello"));

  EXPECT_THAT(itr->ResetToTermEndingBefore(5), IsOkAndHolds(Eq(0)));
  EXPECT_THAT(itr->GetTerm(), Eq("Hello"));

  EXPECT_THAT(itr->ResetToTermEndingBefore(15), IsOkAndHolds(Eq(5)));
  EXPECT_THAT(itr->GetTerm(), Eq(" "));

  EXPECT_THAT(itr->ResetToTermEndingBefore(17), IsOkAndHolds(Eq(5)));
  EXPECT_THAT(itr->GetTerm(), Eq(" "));

  EXPECT_THAT(itr->ResetToTermEndingBefore(19), IsOkAndHolds(Eq(5)));
  EXPECT_THAT(itr->GetTerm(), Eq(" "));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, ChineseResetToTermBefore) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  // CJKT (Chinese, Japanese, Khmer, Thai) are the 4 main languages that
  // don't have whitespaces as word delimiter. Chinese
  constexpr std::string_view kChinese = "我每天走路去上班。";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<LanguageSegmenter::Iterator> itr,
                             language_segmenter->Segment(kChinese));
  // String: "我每天走路去上班。"
  //          ^ ^^ ^  ^ ^
  // Bytes:   0 3 6 9 15 18
  EXPECT_THAT(itr->ResetToTermEndingBefore(0),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(itr->GetTerm(), IsEmpty());

  std::string exp_token;
  int exp_offset;
  // DIFFERENCE/LOCALE DEVIATION!! SIMPLISTIC_CHINESE agrees with ICU that
  // "每天" should be treated as a single token. All other locales split it into
  // two tokens.
  if (GetLocale() == ULOC_SIMPLIFIED_CHINESE) {
    exp_offset = 0;
    exp_token = "我";
  } else {
    exp_offset = 3;
    exp_token = "每";
  }
  EXPECT_THAT(itr->ResetToTermEndingBefore(7), IsOkAndHolds(Eq(exp_offset)));
  EXPECT_THAT(itr->GetTerm(), Eq(exp_token));

  if (GetOptions().locale == ULOC_JAPAN) {
    // LOCALE DEVIATION!! JAPANESE groups "去上" and leaves "班" on its own.
    // All other locales which, like ICU, breaks the text into "去" and "上班".
    exp_offset = 9;
    exp_token = "走路";
  } else {
    exp_offset = 15;
    exp_token = "去";
  }
  EXPECT_THAT(itr->ResetToTermEndingBefore(19), IsOkAndHolds(Eq(exp_offset)));
  EXPECT_THAT(itr->GetTerm(), Eq(exp_token));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, JapaneseResetToTermBefore) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  // Japanese
  constexpr std::string_view kJapanese = "私は毎日仕事に歩いています。";
  // String: "私は毎日仕事に歩いています。"
  //          ^ ^ ^  ^  ^ ^ ^ ^  ^
  // Bytes:   0 3 6  12 18212427 33
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<LanguageSegmenter::Iterator> itr,
                             language_segmenter->Segment(kJapanese));
  EXPECT_THAT(itr->ResetToTermEndingBefore(0),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(itr->GetTerm(), IsEmpty());

  // LOCALE DEVIATION!! There is disagreement when locale is CHINESE about how
  // to tokenize "毎日", "仕事", "歩い", which are all split, and "てい" which
  // is grouped.
  std::string exp_term;
  int exp_offset;
  if (GetLocale() == ULOC_SIMPLIFIED_CHINESE ||
      GetLocale() == ULOC_TRADITIONAL_CHINESE) {
    // TODO(b/157565185) For some reason, CFStringTokenizerGoToTokenAtIndex
    // believes that "いています" is one token when locale is
    // SIMPLIFIED/TRADITIONAL CHINESE, but CFStringTokenizerAdvanceToNextToken
    // thinks that it is three: "い" "てい", "ます". Other locales and ICU agree
    // that that segment should be "歩い", "て", "い", "ます".
    // This is the only case where CFStringTokenizerGoToTokenAtIndex and
    // CFStringTokenizerAdvanceToNextToken disagree. Find a way around this
    // (such as rewinding past the desired segment and then advancing to it) if
    // this is still an issue after adding language detection.
    exp_term = "歩";
    exp_offset = 21;
  } else {
    // Since "てい" is broken up when the locale is not CHINESE,
    // ResetBefore(33) will point to "い" at offset 30.
    exp_term = "い";
    exp_offset = 30;
  }
  auto offset_or = itr->ResetToTermEndingBefore(33);
  EXPECT_THAT(offset_or, IsOk());
  EXPECT_THAT(offset_or.ValueOrDie(), Eq(exp_offset));
  EXPECT_THAT(itr->GetTerm(), Eq(exp_term));

  EXPECT_THAT(itr->ResetToTermEndingBefore(7), IsOkAndHolds(Eq(3)));
  EXPECT_THAT(itr->GetTerm(), Eq("は"));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, KhmerResetToTermBefore) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  constexpr std::string_view kKhmer = "ញុំដើរទៅធ្វើការរាល់ថ្ងៃ។";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<LanguageSegmenter::Iterator> itr,
                             language_segmenter->Segment(kKhmer));
  // String: "ញុំដើរទៅធ្វើការរាល់ថ្ងៃ។"
  //          ^ ^   ^   ^
  // Bytes:   0 9   24  45
  EXPECT_THAT(itr->ResetToTermEndingBefore(0),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(itr->GetTerm(), IsEmpty());

  EXPECT_THAT(itr->ResetToTermEndingBefore(47), IsOkAndHolds(Eq(24)));
  EXPECT_THAT(itr->GetTerm(), Eq("ធ្វើការ"));

  EXPECT_THAT(itr->ResetToTermEndingBefore(14), IsOkAndHolds(Eq(0)));
  EXPECT_THAT(itr->GetTerm(), Eq("ញុំ"));
}

TEST_P(IosLanguageSegmenterAllLocalesTest, ThaiResetToTermBefore) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetOptions()));
  // Thai
  constexpr std::string_view kThai = "ฉันเดินไปทำงานทุกวัน";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<LanguageSegmenter::Iterator> itr,
                             language_segmenter->Segment(kThai));
  // String: "ฉันเดินไปทำงานทุกวัน"
  //          ^ ^  ^ ^    ^
  // Bytes:   0 9 21 27  42
  EXPECT_THAT(itr->ResetToTermEndingBefore(0),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(itr->GetTerm(), IsEmpty());

  // DIFFERENCE!! Disagreement over how to segment "ทุกวัน" (iOS groups).
  // This difference persists even when locale is set to THAI
  EXPECT_THAT(itr->ResetToTermEndingBefore(51), IsOkAndHolds(Eq(27)));
  EXPECT_THAT(itr->GetTerm(), Eq("ทำงาน"));

  EXPECT_THAT(itr->ResetToTermEndingBefore(13), IsOkAndHolds(Eq(0)));
  EXPECT_THAT(itr->GetTerm(), Eq("ฉัน"));

  EXPECT_THAT(itr->ResetToTermEndingBefore(34), IsOkAndHolds(Eq(21)));
  EXPECT_THAT(itr->GetTerm(), Eq("ไป"));
}

INSTANTIATE_TEST_SUITE_P(
    LocaleName, IosLanguageSegmenterAllLocalesTest,
    testing::Values(ULOC_US, ULOC_UK, ULOC_CANADA, ULOC_CANADA_FRENCH,
                    ULOC_FRANCE, ULOC_GERMANY, ULOC_ITALY, ULOC_JAPAN,
                    ULOC_KOREA,
                    ULOC_SIMPLIFIED_CHINESE,
                    ULOC_TRADITIONAL_CHINESE,
                     "es_ES",        // Spanish
                     "hi_IN",        // Hindi
                     "th_TH",        // Thai
                     "lo_LA",        // Lao
                     "km_KH",        // Khmer
                     "ar_DZ",        // Arabic
                     "ru_RU",        // Russian
                     "pt_PT",        // Portuguese
                     "en_US_POSIX"   // American English (Computer)
                     "wrong_locale"  // Will fall back to ICU default locale
                     ""              // Will fall back to ICU default locale
                    ));

}  // namespace
}  // namespace lib
}  // namespace icing
