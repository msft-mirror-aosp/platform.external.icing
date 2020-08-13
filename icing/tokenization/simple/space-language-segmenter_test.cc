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
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/tokenization/language-segmenter.h"

namespace icing {
namespace lib {
namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;

TEST(SpaceLanguageSegmenterTest, EmptyText) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create());
  EXPECT_THAT(language_segmenter->GetAllTerms(""), IsOkAndHolds(IsEmpty()));
}

TEST(SpaceLanguageSegmenterTest, SimpleText) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create());
  EXPECT_THAT(language_segmenter->GetAllTerms("Hello World"),
              IsOkAndHolds(ElementsAre("Hello", " ", "World")));
}

TEST(SpaceLanguageSegmenterTest, Punctuation) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create());

  EXPECT_THAT(language_segmenter->GetAllTerms("Hello, World!!!"),
              IsOkAndHolds(ElementsAre("Hello,", " ", "World!!!")));
  EXPECT_THAT(language_segmenter->GetAllTerms("Open-source project"),
              IsOkAndHolds(ElementsAre("Open-source", " ", "project")));
  EXPECT_THAT(language_segmenter->GetAllTerms("100%"),
              IsOkAndHolds(ElementsAre("100%")));
  EXPECT_THAT(language_segmenter->GetAllTerms("(A&B)"),
              IsOkAndHolds(ElementsAre("(A&B)")));
}

TEST(SpaceLanguageSegmenterTest, Alphanumeric) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create());

  // Alphanumeric terms are allowed
  EXPECT_THAT(language_segmenter->GetAllTerms("Se7en A4 3a"),
              IsOkAndHolds(ElementsAre("Se7en", " ", "A4", " ", "3a")));
}

TEST(SpaceLanguageSegmenterTest, Number) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create());

  // Alphanumeric terms are allowed
  EXPECT_THAT(
      language_segmenter->GetAllTerms("3.141592653589793238462643383279"),
      IsOkAndHolds(ElementsAre("3.141592653589793238462643383279")));

  EXPECT_THAT(language_segmenter->GetAllTerms("3,456.789"),
              IsOkAndHolds(ElementsAre("3,456.789")));

  EXPECT_THAT(language_segmenter->GetAllTerms("-123"),
              IsOkAndHolds(ElementsAre("-123")));
}

TEST(SpaceLanguageSegmenterTest, ContinuousWhitespaces) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create());

  // Multiple continuous whitespaces are treated as one.
  const int kNumSeparators = 256;
  const std::string text_with_spaces =
      absl_ports::StrCat("Hello", std::string(kNumSeparators, ' '), "World");
  EXPECT_THAT(language_segmenter->GetAllTerms(text_with_spaces),
              IsOkAndHolds(ElementsAre("Hello", " ", "World")));
}

TEST(SpaceLanguageSegmenterTest, NotCopyStrings) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create());
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

}  // namespace
}  // namespace lib
}  // namespace icing
