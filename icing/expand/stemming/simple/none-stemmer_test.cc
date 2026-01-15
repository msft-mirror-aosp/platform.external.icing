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
// See the License for the specific language governing permissions and∂∂
// limitations under the License.

#include <memory>
#include <string>
#include <string_view>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/expand/stemming/stemmer-factory.h"
#include "icing/expand/stemming/stemmer.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {

namespace {

using ::testing::Eq;

constexpr std::string_view kEnglishLanguageCode = "en";
constexpr std::string_view kFrenchLanguageCode = "fr";

constexpr std::string_view kUnsupportedLanguageCode = "unsupported";

TEST(NoneStemmerTest, Creation) {
  EXPECT_THAT(stemmer_factory::Create(std::string(kEnglishLanguageCode)),
              IsOk());
  EXPECT_THAT(stemmer_factory::Create(std::string(kFrenchLanguageCode)),
              IsOk());
  EXPECT_THAT(stemmer_factory::Create(std::string(kUnsupportedLanguageCode)),
              IsOk());
}

TEST(NoneStemmerTest, NoStemmingDone) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Stemmer> english_stemmer,
      stemmer_factory::Create(
          /*language_code=*/std::string(kEnglishLanguageCode)));
  EXPECT_THAT(english_stemmer->Stem(""), Eq(""));
  EXPECT_THAT(english_stemmer->Stem("  "), Eq("  "));
  EXPECT_THAT(english_stemmer->Stem("....."), Eq("....."));
  EXPECT_THAT(english_stemmer->Stem("test"), Eq("test"));
  EXPECT_THAT(english_stemmer->Stem("tests"), Eq("tests"));
  EXPECT_THAT(english_stemmer->Stem("testing"), Eq("testing"));
  EXPECT_THAT(english_stemmer->Stem("majestueuse"), Eq("majestueuse"));
  EXPECT_THAT(english_stemmer->Stem("你好"), Eq("你好"));
  EXPECT_THAT(english_stemmer->Stem("валя"), Eq("валя"));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Stemmer> french_stemmer,
      stemmer_factory::Create(
          /*language_code=*/std::string(kFrenchLanguageCode)));
  EXPECT_THAT(french_stemmer->Stem(""), Eq(""));
  EXPECT_THAT(french_stemmer->Stem("  "), Eq("  "));
  EXPECT_THAT(french_stemmer->Stem("....."), Eq("....."));
  EXPECT_THAT(french_stemmer->Stem("test"), Eq("test"));
  EXPECT_THAT(french_stemmer->Stem("tests"), Eq("tests"));
  EXPECT_THAT(french_stemmer->Stem("testing"), Eq("testing"));
  EXPECT_THAT(french_stemmer->Stem("majestueuse"), Eq("majestueuse"));
  EXPECT_THAT(french_stemmer->Stem("你好"), Eq("你好"));
  EXPECT_THAT(french_stemmer->Stem("валя"), Eq("валя"));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Stemmer> unsupported_stemmer,
      stemmer_factory::Create(
          /*language_code=*/std::string(kUnsupportedLanguageCode)));
  EXPECT_THAT(unsupported_stemmer->Stem(""), Eq(""));
  EXPECT_THAT(unsupported_stemmer->Stem("  "), Eq("  "));
  EXPECT_THAT(unsupported_stemmer->Stem("....."), Eq("....."));
  EXPECT_THAT(unsupported_stemmer->Stem("test"), Eq("test"));
  EXPECT_THAT(unsupported_stemmer->Stem("tests"), Eq("tests"));
  EXPECT_THAT(unsupported_stemmer->Stem("testing"), Eq("testing"));
  EXPECT_THAT(unsupported_stemmer->Stem("majestueuse"), Eq("majestueuse"));
  EXPECT_THAT(unsupported_stemmer->Stem("你好"), Eq("你好"));
  EXPECT_THAT(unsupported_stemmer->Stem("валя"), Eq("валя"));
}

}  // namespace

}  // namespace lib
}  // namespace icing
