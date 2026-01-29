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

#include "icing/expand/expander-manager.h"

#include <array>
#include <memory>
#include <string>
#include <string_view>
#include <thread>  // NOLINT
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/expand/expander.h"
#include "icing/portable/platform.h"
#include "icing/testing/common-matchers.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;

constexpr std::string_view kRussianLocale = "ru-RU";
constexpr std::string_view kTamilLocale = "ta-IN";
constexpr std::string_view kUnsupportedLocale = "unsupported_locale";

TEST(ExpanderManagerTest, CreateWithInvalidMaxTermsShouldFail) {
  EXPECT_THAT(ExpanderManager::Create(ULOC_US, /*max_terms_per_expander=*/-1),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(ExpanderManager::Create(ULOC_US, /*max_terms_per_expander=*/1),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(ExpanderManagerTest, CreateWithAnyLocaleShouldSucceed) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ExpanderManager> expander_manager,
      ExpanderManager::Create(ULOC_US, /*max_terms_per_expander=*/2));
  EXPECT_THAT(expander_manager->default_locale(), ULOC_US);

  ICING_ASSERT_OK_AND_ASSIGN(
      expander_manager, ExpanderManager::Create(ULOC_FRENCH,
                                                /*max_terms_per_expander=*/2));
  EXPECT_THAT(expander_manager->default_locale(), ULOC_FRENCH);

  ICING_ASSERT_OK_AND_ASSIGN(
      expander_manager, ExpanderManager::Create(std::string(kUnsupportedLocale),
                                                /*max_terms_per_expander=*/2));
  if (IsStemmingEnabled()) {
    EXPECT_THAT(expander_manager->default_locale(),
                ExpanderManager::kDefaultEnglishLocale);
  } else {
    EXPECT_THAT(expander_manager->default_locale(),
                std::string(kUnsupportedLocale));
  }
}

TEST(ExpanderManagerTest, ProcessTerm_exactMatchReturnsOriginalTerm) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ExpanderManager> expander_manager,
      ExpanderManager::Create(ULOC_US, /*max_terms_per_expander=*/3));

  std::vector<ExpandedTerm> expanded_terms = expander_manager->ProcessTerm(
      "running", TermMatchType::EXACT_ONLY, ULOC_US);
  EXPECT_THAT(expander_manager->default_locale(), ULOC_US);
  EXPECT_THAT(expanded_terms, ElementsAre(ExpandedTerm(
                                  "running", /*is_stemmed_term_in=*/false)));
}

TEST(ExpanderManagerTest, ProcessTerm_prefixMatchReturnsOriginalTerm) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ExpanderManager> expander_manager,
      ExpanderManager::Create(ULOC_US, /*max_terms_per_expander=*/3));

  std::vector<ExpandedTerm> expanded_terms =
      expander_manager->ProcessTerm("running", TermMatchType::PREFIX, ULOC_US);
  EXPECT_THAT(expander_manager->default_locale(), ULOC_US);
  EXPECT_THAT(expanded_terms, ElementsAre(ExpandedTerm(
                                  "running", /*is_stemmed_term_in=*/false)));
}

TEST(ExpanderManagerTest, ProcessTerm_stemmingMatchWithDefaultLocale) {
  if (!IsStemmingEnabled()) {
    GTEST_SKIP() << "Skipping test because stemming is not enabled.";
  }

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ExpanderManager> expander_manager,
      ExpanderManager::Create(ULOC_US, /*max_terms_per_expander=*/3));

  std::vector<ExpandedTerm> expanded_terms = expander_manager->ProcessTerm(
      "running", TermMatchType::STEMMING, ULOC_US);
  EXPECT_THAT(expander_manager->default_locale(), ULOC_US);
  EXPECT_THAT(expanded_terms,
              ElementsAre(ExpandedTerm("running", /*is_stemmed_term_in=*/false),
                          ExpandedTerm("run", /*is_stemmed_term_in=*/true)));

  expanded_terms =
      expander_manager->ProcessTerm("tests", TermMatchType::STEMMING, ULOC_US);
  EXPECT_THAT(expander_manager->default_locale(), ULOC_US);
  EXPECT_THAT(expanded_terms,
              ElementsAre(ExpandedTerm("tests", /*is_stemmed_term_in=*/false),
                          ExpandedTerm("test", /*is_stemmed_term_in=*/true)));
}

TEST(ExpanderManagerTest, ProcessTerm_stemmingMatchWithNonDefaultLocale) {
  if (!IsStemmingEnabled()) {
    GTEST_SKIP() << "Skipping test because stemming is not enabled.";
  }

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ExpanderManager> expander_manager,
      ExpanderManager::Create(ULOC_FRENCH, /*max_terms_per_expander=*/3));

  std::vector<ExpandedTerm> expanded_terms = expander_manager->ProcessTerm(
      "running", TermMatchType::STEMMING, ULOC_US);
  EXPECT_THAT(expander_manager->default_locale(), ULOC_FRENCH);
  EXPECT_THAT(expanded_terms,
              ElementsAre(ExpandedTerm("running", /*is_stemmed_term_in=*/false),
                          ExpandedTerm("run", /*is_stemmed_term_in=*/true)));

  expanded_terms = expander_manager->ProcessTerm(
      "torpedearon", TermMatchType::STEMMING, "es_ES");
  EXPECT_THAT(expander_manager->default_locale(), ULOC_FRENCH);
  EXPECT_THAT(
      expanded_terms,
      ElementsAre(ExpandedTerm("torpedearon", /*is_stemmed_term_in=*/false),
                  ExpandedTerm("torped", /*is_stemmed_term_in=*/true)));
}

TEST(ExpanderManagerTest,
     ProcessTerm_stemmingMatchWithUnsupportedLocaleUsesDefault) {
  if (!IsStemmingEnabled()) {
    GTEST_SKIP() << "Skipping test because stemming is not enabled.";
  }

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ExpanderManager> expander_manager,
      ExpanderManager::Create(ULOC_FRENCH, /*max_terms_per_expander=*/3));

  std::string unsupported_locale_str = std::string(kUnsupportedLocale);
  std::vector<ExpandedTerm> expanded_terms = expander_manager->ProcessTerm(
      "running", TermMatchType::STEMMING, unsupported_locale_str);
  EXPECT_THAT(expander_manager->default_locale(), ULOC_FRENCH);
  EXPECT_THAT(expanded_terms, ElementsAre(ExpandedTerm(
                                  "running", /*is_stemmed_term_in=*/false)));

  expanded_terms = expander_manager->ProcessTerm(
      "majestueuse", TermMatchType::STEMMING, unsupported_locale_str);
  EXPECT_THAT(expander_manager->default_locale(), ULOC_FRENCH);
  EXPECT_THAT(
      expanded_terms,
      ElementsAre(ExpandedTerm("majestueuse", /*is_stemmed_term_in=*/false),
                  ExpandedTerm("majestu", /*is_stemmed_term_in=*/true)));
}

TEST(ExpanderManagerTest,
     ProcessTerm_stemmingMatchWithUnsupportedDefaultLocaleUsesEnglish) {
  if (!IsStemmingEnabled()) {
    GTEST_SKIP() << "Skipping test because stemming is not enabled.";
  }

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ExpanderManager> expander_manager,
      ExpanderManager::Create(std::string(kUnsupportedLocale),
                              /*max_terms_per_expander=*/3));

  std::string unsupported_locale_str = std::string(kUnsupportedLocale);
  std::vector<ExpandedTerm> expanded_terms = expander_manager->ProcessTerm(
      "running", TermMatchType::STEMMING, unsupported_locale_str);
  EXPECT_THAT(expander_manager->default_locale(),
              ExpanderManager::kDefaultEnglishLocale);
  EXPECT_THAT(expanded_terms,
              ElementsAre(ExpandedTerm("running", /*is_stemmed_term_in=*/false),
                          ExpandedTerm("run", /*is_stemmed_term_in=*/true)));

  expanded_terms = expander_manager->ProcessTerm(
      "majestueuse", TermMatchType::STEMMING, unsupported_locale_str);
  EXPECT_THAT(expander_manager->default_locale(),
              ExpanderManager::kDefaultEnglishLocale);
  EXPECT_THAT(
      expanded_terms,
      ElementsAre(ExpandedTerm("majestueuse", /*is_stemmed_term_in=*/false),
                  ExpandedTerm("majestueus", /*is_stemmed_term_in=*/true)));
}

TEST(ExpanderManagerTest, ThreadSafety) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ExpanderManager> expander_manager,
      ExpanderManager::Create(ULOC_US,
                              /*max_terms_per_expander=*/1000));

  constexpr int kNumTerms = 10;
  constexpr int kNumLocales = 5;
  constexpr std::array<std::string_view, kNumLocales> kLocales = {
      ULOC_US, ULOC_FRENCH, kRussianLocale, kTamilLocale, kUnsupportedLocale};
  constexpr std::array<std::string_view, kNumTerms> kTerms = {
      "running", "majestueuse", "валяется", "இக்கதையின்", "testing",
      "test",    "running",     "говорить", "அக்கரையில்", "manager"};

  std::array<std::string_view, kNumTerms> kStems;
  if (IsStemmingEnabled()) {
    kStems = {"run",  "majestu", "валя",  "கதை", "test",
              "test", "running", "говор", "கரை", "manag"};
  } else {
    // Stemming is not enabled, so the stemmed terms are the same as the
    // original terms.
    kStems = kTerms;
  }

  // Create kNumThreads threads. Call ProcessTerm() from each thread in
  // parallel using different locales. There should be no crashes.
  constexpr int kNumThreads = 50;
  std::vector<std::vector<ExpandedTerm>> expanded_terms(kNumThreads);
  auto callable = [&](int thread_id) {
    std::string locale = std::string(kLocales[thread_id % kNumLocales]);
    expanded_terms[thread_id] = expander_manager->ProcessTerm(
        kTerms[thread_id % kNumTerms], TermMatchType::STEMMING, locale);
  };

  // Spawn threads to call ProcessTerm() in parallel.
  std::vector<std::thread> thread_objs;
  for (int i = 0; i < kNumThreads; ++i) {
    thread_objs.emplace_back(callable, i);
  }

  // Join threads and verify results
  for (int i = 0; i < kNumThreads; ++i) {
    thread_objs[i].join();

    int term_number = i % kNumTerms;
    if (kTerms[term_number] == kStems[term_number]) {
      // No stemmed term generated after expansion.
      EXPECT_THAT(expanded_terms[i],
                  ElementsAre(ExpandedTerm(std::string(kTerms[term_number]),
                                           /*is_stemmed_term_in=*/false)));
    } else {
      EXPECT_THAT(expanded_terms[i],
                  ElementsAre(ExpandedTerm(std::string(kTerms[term_number]),
                                           /*is_stemmed_term_in=*/false),
                              ExpandedTerm(std::string(kStems[term_number]),
                                           /*is_stemmed_term_in=*/true)));
    }
  }
}

}  // namespace

}  // namespace lib
}  // namespace icing
