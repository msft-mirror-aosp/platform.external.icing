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
#include "icing/icu-data-file-helper.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/test-data.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/tokenization/language-segmenter.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {
namespace {

using ::testing::Eq;

// We have a separate test just for the language segmenter iterators because we
// don't need to stress test the implementation's definition of a term. These
// test that it advances and traverses through simple terms consistently between
// all the implementations.
class LanguageSegmenterIteratorTest
    : public testing::TestWithParam<language_segmenter_factory::SegmenterType> {
 protected:
  void SetUp() override {
    ICING_ASSERT_OK(
        // File generated via icu_data_file rule in //icing/BUILD.
        icu_data_file_helper::SetUpICUDataFile(
            GetTestFilePath("icing/icu.dat")));
  }

  static language_segmenter_factory::SegmenterType GetType() {
    return GetParam();
  }
};

TEST_P(LanguageSegmenterIteratorTest, AdvanceAndGetTerm) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetType()));
  ICING_ASSERT_OK_AND_ASSIGN(auto iterator,
                             language_segmenter->Segment("foo bar"));

  EXPECT_TRUE(iterator->Advance());
  EXPECT_THAT(iterator->GetTerm(), Eq("foo"));

  EXPECT_TRUE(iterator->Advance());
  EXPECT_THAT(iterator->GetTerm(), Eq(" "));

  EXPECT_TRUE(iterator->Advance());
  EXPECT_THAT(iterator->GetTerm(), Eq("bar"));

  EXPECT_FALSE(iterator->Advance());
}

TEST_P(LanguageSegmenterIteratorTest,
       ResetToTermStartingAfterWithOffsetInText) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetType()));
  ICING_ASSERT_OK_AND_ASSIGN(auto iterator,
                             language_segmenter->Segment("foo bar"));

  EXPECT_THAT(iterator->ResetToTermStartingAfter(/*offset=*/0),
              IsOkAndHolds(3));  // The term " "
  EXPECT_THAT(iterator->ResetToTermStartingAfter(/*offset=*/3),
              IsOkAndHolds(4));  // The term "bar"
  EXPECT_THAT(iterator->ResetToTermStartingAfter(/*offset=*/4),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_P(LanguageSegmenterIteratorTest,
       ResetToTermStartingAfterWithNegativeOffsetOk) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetType()));
  ICING_ASSERT_OK_AND_ASSIGN(auto iterator,
                             language_segmenter->Segment("foo bar"));

  EXPECT_THAT(iterator->ResetToTermStartingAfter(/*offset=*/-1),
              IsOkAndHolds(0));  // The term "foo"

  EXPECT_THAT(iterator->ResetToTermStartingAfter(/*offset=*/-100),
              IsOkAndHolds(0));  // The term "foo"
}

TEST_P(LanguageSegmenterIteratorTest,
       ResetToTermStartingAfterWithTextLengthOffsetNotFound) {
  std::string text = "foo bar";
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetType()));
  ICING_ASSERT_OK_AND_ASSIGN(auto iterator, language_segmenter->Segment(text));

  EXPECT_THAT(iterator->ResetToTermStartingAfter(/*offset=*/text.size()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_P(LanguageSegmenterIteratorTest,
       ResetToTermStartingAfterWithOffsetPastTextLengthNotFound) {
  std::string text = "foo bar";
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetType()));
  ICING_ASSERT_OK_AND_ASSIGN(auto iterator, language_segmenter->Segment(text));

  EXPECT_THAT(iterator->ResetToTermStartingAfter(/*offset=*/100),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_P(LanguageSegmenterIteratorTest, ResetToTermEndingBeforeWithOffsetInText) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetType()));
  ICING_ASSERT_OK_AND_ASSIGN(auto iterator,
                             language_segmenter->Segment("foo bar"));

  EXPECT_THAT(iterator->ResetToTermEndingBefore(/*offset=*/6),
              IsOkAndHolds(3));  // The term " "
  EXPECT_THAT(iterator->ResetToTermEndingBefore(/*offset=*/3),
              IsOkAndHolds(0));  // The term "foo"
  EXPECT_THAT(iterator->ResetToTermEndingBefore(/*offset=*/2),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_P(LanguageSegmenterIteratorTest,
       ResetToTermEndingBeforeWithZeroOrNegativeOffsetNotFound) {
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetType()));
  ICING_ASSERT_OK_AND_ASSIGN(auto iterator,
                             language_segmenter->Segment("foo bar"));

  EXPECT_THAT(iterator->ResetToTermEndingBefore(/*offset=*/0),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  EXPECT_THAT(iterator->ResetToTermEndingBefore(/*offset=*/-1),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  EXPECT_THAT(iterator->ResetToTermEndingBefore(/*offset=*/-100),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_P(LanguageSegmenterIteratorTest,
       ResetToTermEndingBeforeWithTextLengthOffsetOk) {
  std::string text = "foo bar";
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetType()));
  ICING_ASSERT_OK_AND_ASSIGN(auto iterator, language_segmenter->Segment(text));

  EXPECT_THAT(iterator->ResetToTermEndingBefore(/*offset=*/text.length()),
              IsOkAndHolds(4));  // The term "bar"
}

TEST_P(LanguageSegmenterIteratorTest,
       ResetToTermEndingBeforeWithOffsetPastTextLengthNotFound) {
  std::string text = "foo bar";
  ICING_ASSERT_OK_AND_ASSIGN(auto language_segmenter,
                             language_segmenter_factory::Create(GetType()));
  ICING_ASSERT_OK_AND_ASSIGN(auto iterator, language_segmenter->Segment(text));

  EXPECT_THAT(iterator->ResetToTermEndingBefore(/*offset=*/text.length() + 1),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

INSTANTIATE_TEST_SUITE_P(
    SegmenterType, LanguageSegmenterIteratorTest,
    testing::Values(language_segmenter_factory::SegmenterType::ICU4C,
                    language_segmenter_factory::SegmenterType::SPACE));

}  // namespace
}  // namespace lib
}  // namespace icing
