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

#include <array>
#include <memory>
#include <string>
#include <string_view>
#include <thread>  // NOLINT
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/expand/expander.h"
#include "icing/expand/stemming/stemming-expander.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::SizeIs;

constexpr std::string_view kEnglishLanguageCode = "en";
constexpr std::string_view kRandomLanguageCode = "random";

TEST(NoneStemmingExpanderTest, EmptyTerm) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Expander> expander,
      StemmingExpander::Create(std::string(kEnglishLanguageCode)));

  std::vector<ExpandedTerm> expanded_terms = expander->Expand("");
  EXPECT_THAT(expanded_terms, SizeIs(1));
  EXPECT_THAT(expanded_terms[0].text, Eq(""));
  EXPECT_FALSE(expanded_terms[0].is_stemmed_term);

  expanded_terms = expander->Expand("  ");
  EXPECT_THAT(expanded_terms, SizeIs(1));
  EXPECT_THAT(expanded_terms[0].text, Eq("  "));
  EXPECT_FALSE(expanded_terms[0].is_stemmed_term);
}

TEST(NoneStemmingExpanderTest, NonAlphabetSymbols) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Expander> expander,
      StemmingExpander::Create(std::string(kEnglishLanguageCode)));

  std::vector<ExpandedTerm> expanded_terms = expander->Expand("....");
  EXPECT_THAT(expanded_terms, SizeIs(1));
  EXPECT_THAT(expanded_terms[0].text, Eq("...."));
  EXPECT_FALSE(expanded_terms[0].is_stemmed_term);

  expanded_terms = expander->Expand("928347");
  EXPECT_THAT(expanded_terms, SizeIs(1));
  EXPECT_THAT(expanded_terms[0].text, Eq("928347"));
  EXPECT_FALSE(expanded_terms[0].is_stemmed_term);
}

TEST(NoneStemmingExpanderTest, ExpandTermReturnsOriginalTerm) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Expander> expander,
      StemmingExpander::Create(std::string(kEnglishLanguageCode)));

  std::vector<ExpandedTerm> expanded_terms = expander->Expand("running");
  EXPECT_THAT(expanded_terms, SizeIs(1));
  EXPECT_THAT(expanded_terms[0].text, Eq("running"));
  EXPECT_FALSE(expanded_terms[0].is_stemmed_term);

  expanded_terms = expander->Expand("abattement");
  EXPECT_THAT(expanded_terms, SizeIs(1));
  EXPECT_THAT(expanded_terms[0].text, Eq("abattement"));
  EXPECT_FALSE(expanded_terms[0].is_stemmed_term);
}

TEST(NoneStemmingExpanderTest, LanguageCodeDoesNotMatter) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Expander> english_expander,
      StemmingExpander::Create(std::string(kEnglishLanguageCode)));

  std::vector<ExpandedTerm> expanded_terms =
      english_expander->Expand("running");
  EXPECT_THAT(expanded_terms, SizeIs(1));
  EXPECT_THAT(expanded_terms[0].text, Eq("running"));
  EXPECT_FALSE(expanded_terms[0].is_stemmed_term);

  expanded_terms = english_expander->Expand("abattement");
  EXPECT_THAT(expanded_terms, SizeIs(1));
  EXPECT_THAT(expanded_terms[0].text, Eq("abattement"));
  EXPECT_FALSE(expanded_terms[0].is_stemmed_term);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Expander> random_expander,
      StemmingExpander::Create(std::string(kRandomLanguageCode)));

  expanded_terms = random_expander->Expand("running");
  EXPECT_THAT(expanded_terms, SizeIs(1));
  EXPECT_THAT(expanded_terms[0].text, Eq("running"));
  EXPECT_FALSE(expanded_terms[0].is_stemmed_term);

  expanded_terms = random_expander->Expand("abattement");
  EXPECT_THAT(expanded_terms, SizeIs(1));
  EXPECT_THAT(expanded_terms[0].text, Eq("abattement"));
  EXPECT_FALSE(expanded_terms[0].is_stemmed_term);
}

TEST(NoneStemmingExpanderTest, Utf8Characters) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Expander> expander,
      StemmingExpander::Create(std::string(kEnglishLanguageCode)));

  std::vector<ExpandedTerm> expanded_terms = expander->Expand("我们");
  EXPECT_THAT(expanded_terms, SizeIs(1));
  EXPECT_THAT(expanded_terms[0].text, Eq("我们"));
  EXPECT_FALSE(expanded_terms[0].is_stemmed_term);

  expanded_terms = expander->Expand("இக்கதையின்");
  EXPECT_THAT(expanded_terms, SizeIs(1));
  EXPECT_THAT(expanded_terms[0].text, Eq("இக்கதையின்"));
  EXPECT_FALSE(expanded_terms[0].is_stemmed_term);
}

TEST(StemmingExpanderTest, ThreadSafety) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<Expander> expander,
      StemmingExpander::Create(std::string(kEnglishLanguageCode)));

  constexpr std::array<std::string_view, 5> kTerms = {
      "running", "management", "tests", "asdfjgjjh", "!!!))))"};

  // Create kNumThreads threads. Call Expand() from each thread in
  // parallel using different locales. There should be no crashes.
  constexpr int kNumThreads = 50;
  std::vector<std::vector<ExpandedTerm>> expanded_terms(kNumThreads);
  auto callable = [&](int thread_id) {
    expanded_terms[thread_id] =
        expander->Expand(kTerms[thread_id % kTerms.size()]);
  };

  // Spawn threads to call Expand() in parallel.
  std::vector<std::thread> thread_objs;
  for (int i = 0; i < kNumThreads; ++i) {
    thread_objs.emplace_back(callable, i);
  }

  // Join threads and verify results
  for (int i = 0; i < kNumThreads; ++i) {
    thread_objs[i].join();
    EXPECT_THAT(expanded_terms[i],
                ElementsAre(ExpandedTerm(std::string(kTerms[i % kTerms.size()]),
                                         /*is_stemmed_term_in=*/false)));
  }
}

}  // namespace

}  // namespace lib
}  // namespace icing
