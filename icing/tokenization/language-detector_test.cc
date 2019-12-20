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

#include "icing/tokenization/language-detector.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/file/filesystem.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/test-data.h"

namespace icing {
namespace lib {
namespace {
using ::testing::Eq;

TEST(LanguageDetectorTest, BadFilePath) {
  EXPECT_THAT(LanguageDetector::CreateWithLangId("Bad file path"),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

// TODO(samzheng): more tests for other languages and mixed languages
TEST(LanguageDetectorTest, DetectLanguage) {
  ICING_ASSERT_OK_AND_ASSIGN(
      auto language_detector,
      LanguageDetector::CreateWithLangId(GetLangIdModelPath()));

  EXPECT_THAT(language_detector->DetectLanguage(" , "),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  EXPECT_THAT(language_detector->DetectLanguage("hello world"),
              IsOkAndHolds(Eq("en")));  // English

  EXPECT_THAT(language_detector->DetectLanguage("Selam Dünya"),
              IsOkAndHolds(Eq("tr")));  // Turkish

  EXPECT_THAT(language_detector->DetectLanguage("Bonjour le monde"),
              IsOkAndHolds(Eq("fr")));  // French

  EXPECT_THAT(language_detector->DetectLanguage("你好世界"),
              IsOkAndHolds(Eq("zh")));  // Chinese

  EXPECT_THAT(language_detector->DetectLanguage("こんにちは世界"),
              IsOkAndHolds(Eq("ja")));  // Japanese

  EXPECT_THAT(language_detector->DetectLanguage("สวัสดีชาวโลก"),
              IsOkAndHolds(Eq("th")));  // Thai

  EXPECT_THAT(language_detector->DetectLanguage("안녕 세상"),
              IsOkAndHolds(Eq("ko")));  // Korean

  EXPECT_THAT(language_detector->DetectLanguage("Hallo Wereld"),
              IsOkAndHolds(Eq("nl")));  // Dutch

  EXPECT_THAT(language_detector->DetectLanguage("Hola Mundo"),
              IsOkAndHolds(Eq("es")));  // Spanish

  EXPECT_THAT(language_detector->DetectLanguage("नमस्ते दुनिया"),
              IsOkAndHolds(Eq("hi")));  // Hindi

  EXPECT_THAT(language_detector->DetectLanguage("مرحبا بالعالم"),
              IsOkAndHolds(Eq("ar")));  // Arabic

  EXPECT_THAT(language_detector->DetectLanguage("Привет, мир"),
              IsOkAndHolds(Eq("ru")));  // Russian
}

}  // namespace
}  // namespace lib
}  // namespace icing
