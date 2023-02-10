// Copyright (C) 2022 Google LLC
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

#include "icing/index/numeric/numeric-index.h"

#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/file/filesystem.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/index/numeric/dummy-numeric-index.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::IsTrue;
using ::testing::NotNull;

constexpr static std::string_view kDefaultTestPropertyName = "test";

constexpr SectionId kDefaultSectionId = 0;

template <typename T>
class NumericIndexTest : public ::testing::Test {
 protected:
  using INDEX_IMPL_TYPE = T;

  void SetUp() override {
    base_dir_ = GetTestTempDir() + "/icing";
    ASSERT_THAT(filesystem_.CreateDirectoryRecursively(base_dir_.c_str()),
                IsTrue());

    working_path_ = base_dir_ + "/numeric_index_integer_test";

    if (std::is_same_v<
            INDEX_IMPL_TYPE,
            DummyNumericIndex<typename INDEX_IMPL_TYPE::value_type>>) {
      ICING_ASSERT_OK_AND_ASSIGN(
          numeric_index_,
          DummyNumericIndex<typename INDEX_IMPL_TYPE::value_type>::Create(
              filesystem_, working_path_));
    }

    ASSERT_THAT(numeric_index_, NotNull());
  }

  void TearDown() override {
    numeric_index_.reset();
    filesystem_.DeleteDirectoryRecursively(base_dir_.c_str());
  }

  void Index(std::string_view property_name, DocumentId document_id,
             SectionId section_id,
             std::vector<typename INDEX_IMPL_TYPE::value_type> keys) {
    std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
        this->numeric_index_->Edit(property_name, document_id, section_id);

    for (const auto& key : keys) {
      ICING_EXPECT_OK(editor->BufferKey(key));
    }
    ICING_EXPECT_OK(editor->IndexAllBufferedKeys());
  }

  libtextclassifier3::StatusOr<std::vector<DocHitInfo>> Query(
      std::string_view property_name,
      typename INDEX_IMPL_TYPE::value_type key_lower,
      typename INDEX_IMPL_TYPE::value_type key_upper) {
    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<DocHitInfoIterator> iter,
        this->numeric_index_->GetIterator(property_name, key_lower, key_upper));

    std::vector<DocHitInfo> result;
    while (iter->Advance().ok()) {
      result.push_back(iter->doc_hit_info());
    }
    return result;
  }

  Filesystem filesystem_;
  std::string base_dir_;
  std::string working_path_;
  std::unique_ptr<NumericIndex<typename INDEX_IMPL_TYPE::value_type>>
      numeric_index_;
};

using TestTypes = ::testing::Types<DummyNumericIndex<int64_t>>;
TYPED_TEST_SUITE(NumericIndexTest, TestTypes);

TYPED_TEST(NumericIndexTest, SingleKeyExactQuery) {
  this->Index(kDefaultTestPropertyName, /*document_id=*/0, kDefaultSectionId,
              /*keys=*/{1});
  this->Index(kDefaultTestPropertyName, /*document_id=*/1, kDefaultSectionId,
              /*keys=*/{3});
  this->Index(kDefaultTestPropertyName, /*document_id=*/2, kDefaultSectionId,
              /*keys=*/{2});
  this->Index(kDefaultTestPropertyName, /*document_id=*/3, kDefaultSectionId,
              /*keys=*/{0});
  this->Index(kDefaultTestPropertyName, /*document_id=*/4, kDefaultSectionId,
              /*keys=*/{4});
  this->Index(kDefaultTestPropertyName, /*document_id=*/5, kDefaultSectionId,
              /*keys=*/{2});

  int64_t query_key = 2;
  std::vector<SectionId> expected_sections{kDefaultSectionId};
  EXPECT_THAT(this->Query(kDefaultTestPropertyName, /*key_lower=*/query_key,
                          /*key_upper=*/query_key),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/5, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/2, expected_sections))));
}

TYPED_TEST(NumericIndexTest, SingleKeyRangeQuery) {
  this->Index(kDefaultTestPropertyName, /*document_id=*/0, kDefaultSectionId,
              /*keys=*/{1});
  this->Index(kDefaultTestPropertyName, /*document_id=*/1, kDefaultSectionId,
              /*keys=*/{3});
  this->Index(kDefaultTestPropertyName, /*document_id=*/2, kDefaultSectionId,
              /*keys=*/{2});
  this->Index(kDefaultTestPropertyName, /*document_id=*/3, kDefaultSectionId,
              /*keys=*/{0});
  this->Index(kDefaultTestPropertyName, /*document_id=*/4, kDefaultSectionId,
              /*keys=*/{4});
  this->Index(kDefaultTestPropertyName, /*document_id=*/5, kDefaultSectionId,
              /*keys=*/{2});

  std::vector<SectionId> expected_sections{kDefaultSectionId};
  EXPECT_THAT(this->Query(kDefaultTestPropertyName, /*key_lower=*/1,
                          /*key_upper=*/3),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/5, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/2, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
}

TYPED_TEST(NumericIndexTest, EmptyResult) {
  this->Index(kDefaultTestPropertyName, /*document_id=*/0, kDefaultSectionId,
              /*keys=*/{1});
  this->Index(kDefaultTestPropertyName, /*document_id=*/1, kDefaultSectionId,
              /*keys=*/{3});
  this->Index(kDefaultTestPropertyName, /*document_id=*/2, kDefaultSectionId,
              /*keys=*/{2});
  this->Index(kDefaultTestPropertyName, /*document_id=*/3, kDefaultSectionId,
              /*keys=*/{0});
  this->Index(kDefaultTestPropertyName, /*document_id=*/4, kDefaultSectionId,
              /*keys=*/{4});
  this->Index(kDefaultTestPropertyName, /*document_id=*/5, kDefaultSectionId,
              /*keys=*/{2});

  EXPECT_THAT(this->Query(kDefaultTestPropertyName, /*key_lower=*/100,
                          /*key_upper=*/200),
              IsOkAndHolds(IsEmpty()));
}

TYPED_TEST(NumericIndexTest, MultipleKeysShouldMergeAndDedupeDocHitInfo) {
  // Construct several documents with mutiple keys under the same section.
  // Range query [1, 3] will find hits with same (DocumentId, SectionId) for
  // mutiple times. For example, (2, kDefaultSectionId) will be found twice
  // (once for key = 1 and once for key = 3).
  // Test if the iterator dedupes correctly.
  this->Index(kDefaultTestPropertyName, /*document_id=*/0, kDefaultSectionId,
              /*keys=*/{-1000, 0});
  this->Index(kDefaultTestPropertyName, /*document_id=*/1, kDefaultSectionId,
              /*keys=*/{-100, 0, 1, 2, 3, 4, 5});
  this->Index(kDefaultTestPropertyName, /*document_id=*/2, kDefaultSectionId,
              /*keys=*/{3, 1});
  this->Index(kDefaultTestPropertyName, /*document_id=*/3, kDefaultSectionId,
              /*keys=*/{4, 1});
  this->Index(kDefaultTestPropertyName, /*document_id=*/4, kDefaultSectionId,
              /*keys=*/{1, 6});
  this->Index(kDefaultTestPropertyName, /*document_id=*/5, kDefaultSectionId,
              /*keys=*/{2, 100});
  this->Index(kDefaultTestPropertyName, /*document_id=*/6, kDefaultSectionId,
              /*keys=*/{1000, 2});
  this->Index(kDefaultTestPropertyName, /*document_id=*/7, kDefaultSectionId,
              /*keys=*/{4, -1000});

  std::vector<SectionId> expected_sections{kDefaultSectionId};
  EXPECT_THAT(this->Query(kDefaultTestPropertyName, /*key_lower=*/1,
                          /*key_upper=*/3),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/6, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/5, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/4, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/3, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/2, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections))));
}

TYPED_TEST(NumericIndexTest, EdgeNumericValues) {
  this->Index(kDefaultTestPropertyName, /*document_id=*/0, kDefaultSectionId,
              /*keys=*/{0});
  this->Index(kDefaultTestPropertyName, /*document_id=*/1, kDefaultSectionId,
              /*keys=*/{-100});
  this->Index(kDefaultTestPropertyName, /*document_id=*/2, kDefaultSectionId,
              /*keys=*/{-80});
  this->Index(
      kDefaultTestPropertyName, /*document_id=*/3, kDefaultSectionId,
      /*keys=*/{std::numeric_limits<typename TypeParam::value_type>::max()});
  this->Index(
      kDefaultTestPropertyName, /*document_id=*/4, kDefaultSectionId,
      /*keys=*/{std::numeric_limits<typename TypeParam::value_type>::min()});
  this->Index(kDefaultTestPropertyName, /*document_id=*/5, kDefaultSectionId,
              /*keys=*/{200});
  this->Index(kDefaultTestPropertyName, /*document_id=*/6, kDefaultSectionId,
              /*keys=*/{100});
  this->Index(
      kDefaultTestPropertyName, /*document_id=*/7, kDefaultSectionId,
      /*keys=*/{std::numeric_limits<typename TypeParam::value_type>::max()});
  this->Index(kDefaultTestPropertyName, /*document_id=*/8, kDefaultSectionId,
              /*keys=*/{0});
  this->Index(
      kDefaultTestPropertyName, /*document_id=*/9, kDefaultSectionId,
      /*keys=*/{std::numeric_limits<typename TypeParam::value_type>::min()});

  std::vector<SectionId> expected_sections{kDefaultSectionId};

  // Negative key
  EXPECT_THAT(this->Query(kDefaultTestPropertyName, /*key_lower=*/-100,
                          /*key_upper=*/-70),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/2, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections))));

  // value_type max key
  EXPECT_THAT(
      this->Query(kDefaultTestPropertyName, /*key_lower=*/
                  std::numeric_limits<typename TypeParam::value_type>::max(),
                  /*key_upper=*/
                  std::numeric_limits<typename TypeParam::value_type>::max()),
      IsOkAndHolds(
          ElementsAre(EqualsDocHitInfo(/*document_id=*/7, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/3, expected_sections))));

  // value_type min key
  EXPECT_THAT(
      this->Query(kDefaultTestPropertyName, /*key_lower=*/
                  std::numeric_limits<typename TypeParam::value_type>::min(),
                  /*key_upper=*/
                  std::numeric_limits<typename TypeParam::value_type>::min()),
      IsOkAndHolds(
          ElementsAre(EqualsDocHitInfo(/*document_id=*/9, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/4, expected_sections))));

  // Key = 0
  EXPECT_THAT(
      this->Query(kDefaultTestPropertyName, /*key_lower=*/0, /*key_upper=*/0),
      IsOkAndHolds(
          ElementsAre(EqualsDocHitInfo(/*document_id=*/8, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/0, expected_sections))));

  // All keys from value_type min to value_type max
  EXPECT_THAT(
      this->Query(kDefaultTestPropertyName, /*key_lower=*/
                  std::numeric_limits<typename TypeParam::value_type>::min(),
                  /*key_upper=*/
                  std::numeric_limits<typename TypeParam::value_type>::max()),
      IsOkAndHolds(
          ElementsAre(EqualsDocHitInfo(/*document_id=*/9, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/8, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/7, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/6, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/5, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/4, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/3, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/2, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/1, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
}

TYPED_TEST(NumericIndexTest,
           MultipleSectionsShouldMergeSectionsAndDedupeDocHitInfo) {
  // Construct several documents with mutiple numeric sections.
  // Range query [1, 3] will find hits with same DocumentIds but multiple
  // different SectionIds. For example, there will be 2 hits (1, 0), (1, 1) for
  // DocumentId=1.
  // Test if the iterator merges multiple sections into a single SectionIdMask
  // correctly.
  this->Index(kDefaultTestPropertyName, /*document_id=*/0, /*section_id=*/0,
              /*keys=*/{0});
  this->Index(kDefaultTestPropertyName, /*document_id=*/0, /*section_id=*/1,
              /*keys=*/{1});
  this->Index(kDefaultTestPropertyName, /*document_id=*/0, /*section_id=*/2,
              /*keys=*/{-1});
  this->Index(kDefaultTestPropertyName, /*document_id=*/1, /*section_id=*/0,
              /*keys=*/{2});
  this->Index(kDefaultTestPropertyName, /*document_id=*/1, /*section_id=*/1,
              /*keys=*/{1});
  this->Index(kDefaultTestPropertyName, /*document_id=*/1, /*section_id=*/2,
              /*keys=*/{4});
  this->Index(kDefaultTestPropertyName, /*document_id=*/2, /*section_id=*/3,
              /*keys=*/{3});
  this->Index(kDefaultTestPropertyName, /*document_id=*/2, /*section_id=*/4,
              /*keys=*/{2});
  this->Index(kDefaultTestPropertyName, /*document_id=*/2, /*section_id=*/5,
              /*keys=*/{5});

  EXPECT_THAT(
      this->Query(kDefaultTestPropertyName, /*key_lower=*/1,
                  /*key_upper=*/3),
      IsOkAndHolds(ElementsAre(
          EqualsDocHitInfo(/*document_id=*/2, std::vector<SectionId>{3, 4}),
          EqualsDocHitInfo(/*document_id=*/1, std::vector<SectionId>{0, 1}),
          EqualsDocHitInfo(/*document_id=*/0, std::vector<SectionId>{1}))));
}

TYPED_TEST(NumericIndexTest, NonRelevantPropertyShouldNotBeIncluded) {
  constexpr std::string_view kNonRelevantProperty = "non_relevant_property";
  this->Index(kDefaultTestPropertyName, /*document_id=*/0, kDefaultSectionId,
              /*keys=*/{1});
  this->Index(kDefaultTestPropertyName, /*document_id=*/1, kDefaultSectionId,
              /*keys=*/{3});
  this->Index(kNonRelevantProperty, /*document_id=*/2, kDefaultSectionId,
              /*keys=*/{2});
  this->Index(kDefaultTestPropertyName, /*document_id=*/3, kDefaultSectionId,
              /*keys=*/{0});
  this->Index(kNonRelevantProperty, /*document_id=*/4, kDefaultSectionId,
              /*keys=*/{4});
  this->Index(kDefaultTestPropertyName, /*document_id=*/5, kDefaultSectionId,
              /*keys=*/{2});

  std::vector<SectionId> expected_sections{kDefaultSectionId};
  EXPECT_THAT(this->Query(kDefaultTestPropertyName, /*key_lower=*/1,
                          /*key_upper=*/3),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/5, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
}

TYPED_TEST(NumericIndexTest,
           RangeQueryKeyLowerGreaterThanKeyUpperShouldReturnError) {
  this->Index(kDefaultTestPropertyName, /*document_id=*/0, kDefaultSectionId,
              /*keys=*/{1});
  this->Index(kDefaultTestPropertyName, /*document_id=*/1, kDefaultSectionId,
              /*keys=*/{3});
  this->Index(kDefaultTestPropertyName, /*document_id=*/2, kDefaultSectionId,
              /*keys=*/{2});
  this->Index(kDefaultTestPropertyName, /*document_id=*/3, kDefaultSectionId,
              /*keys=*/{0});
  this->Index(kDefaultTestPropertyName, /*document_id=*/4, kDefaultSectionId,
              /*keys=*/{4});
  this->Index(kDefaultTestPropertyName, /*document_id=*/5, kDefaultSectionId,
              /*keys=*/{2});

  EXPECT_THAT(this->Query(kDefaultTestPropertyName, /*key_lower=*/3,
                          /*key_upper=*/1),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

}  // namespace

}  // namespace lib
}  // namespace icing
