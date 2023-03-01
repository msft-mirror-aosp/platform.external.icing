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

#include "icing/index/numeric/integer-index.h"

#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/file/filesystem.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/index/numeric/dummy-numeric-index.h"
#include "icing/index/numeric/integer-index-storage.h"
#include "icing/index/numeric/numeric-index.h"
#include "icing/index/numeric/posting-list-integer-index-serializer.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::IsFalse;
using ::testing::IsTrue;
using ::testing::Lt;

using Crcs = PersistentStorage::Crcs;
using Info = IntegerIndex::Info;

static constexpr int32_t kCorruptedValueOffset = 3;
constexpr static std::string_view kDefaultTestPropertyPath = "test.property";

constexpr SectionId kDefaultSectionId = 0;

template <typename T>
class NumericIndexIntegerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    base_dir_ = GetTestTempDir() + "/icing";
    ASSERT_THAT(filesystem_.CreateDirectoryRecursively(base_dir_.c_str()),
                IsTrue());

    working_path_ = base_dir_ + "/numeric_index_integer_test";
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(base_dir_.c_str());
  }

  template <typename UnknownIntegerIndexType>
  libtextclassifier3::StatusOr<std::unique_ptr<NumericIndex<int64_t>>>
  CreateIntegerIndex() {
    return absl_ports::InvalidArgumentError("Unknown type");
  }

  template <>
  libtextclassifier3::StatusOr<std::unique_ptr<NumericIndex<int64_t>>>
  CreateIntegerIndex<DummyNumericIndex<int64_t>>() {
    return DummyNumericIndex<int64_t>::Create(filesystem_, working_path_);
  }

  template <>
  libtextclassifier3::StatusOr<std::unique_ptr<NumericIndex<int64_t>>>
  CreateIntegerIndex<IntegerIndex>() {
    return IntegerIndex::Create(filesystem_, working_path_);
  }

  Filesystem filesystem_;
  std::string base_dir_;
  std::string working_path_;
};

void Index(NumericIndex<int64_t>* integer_index, std::string_view property_path,
           DocumentId document_id, SectionId section_id,
           std::vector<int64_t> keys) {
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      integer_index->Edit(property_path, document_id, section_id);

  for (const auto& key : keys) {
    ICING_EXPECT_OK(editor->BufferKey(key));
  }
  ICING_EXPECT_OK(std::move(*editor).IndexAllBufferedKeys());
}

libtextclassifier3::StatusOr<std::vector<DocHitInfo>> Query(
    const NumericIndex<int64_t>* integer_index, std::string_view property_path,
    int64_t key_lower, int64_t key_upper) {
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<DocHitInfoIterator> iter,
      integer_index->GetIterator(property_path, key_lower, key_upper));

  std::vector<DocHitInfo> result;
  while (iter->Advance().ok()) {
    result.push_back(iter->doc_hit_info());
  }
  return result;
}

using TestTypes = ::testing::Types<DummyNumericIndex<int64_t>, IntegerIndex>;
TYPED_TEST_SUITE(NumericIndexIntegerTest, TestTypes);

TYPED_TEST(NumericIndexIntegerTest, SetLastAddedDocumentId) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumericIndex<int64_t>> integer_index,
      this->template CreateIntegerIndex<TypeParam>());

  EXPECT_THAT(integer_index->last_added_document_id(), Eq(kInvalidDocumentId));

  constexpr DocumentId kDocumentId = 100;
  integer_index->set_last_added_document_id(kDocumentId);
  EXPECT_THAT(integer_index->last_added_document_id(), Eq(kDocumentId));

  constexpr DocumentId kNextDocumentId = 123;
  integer_index->set_last_added_document_id(kNextDocumentId);
  EXPECT_THAT(integer_index->last_added_document_id(), Eq(kNextDocumentId));
}

TYPED_TEST(
    NumericIndexIntegerTest,
    SetLastAddedDocumentIdShouldIgnoreNewDocumentIdNotGreaterThanTheCurrent) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumericIndex<int64_t>> integer_index,
      this->template CreateIntegerIndex<TypeParam>());

  constexpr DocumentId kDocumentId = 123;
  integer_index->set_last_added_document_id(kDocumentId);
  ASSERT_THAT(integer_index->last_added_document_id(), Eq(kDocumentId));

  constexpr DocumentId kNextDocumentId = 100;
  ASSERT_THAT(kNextDocumentId, Lt(kDocumentId));
  integer_index->set_last_added_document_id(kNextDocumentId);
  // last_added_document_id() should remain unchanged.
  EXPECT_THAT(integer_index->last_added_document_id(), Eq(kDocumentId));
}

TYPED_TEST(NumericIndexIntegerTest, SingleKeyExactQuery) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumericIndex<int64_t>> integer_index,
      this->template CreateIntegerIndex<TypeParam>());

  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/0,
        kDefaultSectionId, /*keys=*/{1});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/1,
        kDefaultSectionId, /*keys=*/{3});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/2,
        kDefaultSectionId, /*keys=*/{2});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/3,
        kDefaultSectionId, /*keys=*/{0});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/4,
        kDefaultSectionId, /*keys=*/{4});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/5,
        kDefaultSectionId, /*keys=*/{2});

  int64_t query_key = 2;
  std::vector<SectionId> expected_sections = {kDefaultSectionId};
  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/query_key, /*key_upper=*/query_key),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/5, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/2, expected_sections))));
}

TYPED_TEST(NumericIndexIntegerTest, SingleKeyRangeQuery) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumericIndex<int64_t>> integer_index,
      this->template CreateIntegerIndex<TypeParam>());

  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/0,
        kDefaultSectionId, /*keys=*/{1});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/1,
        kDefaultSectionId, /*keys=*/{3});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/2,
        kDefaultSectionId, /*keys=*/{2});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/3,
        kDefaultSectionId, /*keys=*/{0});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/4,
        kDefaultSectionId, /*keys=*/{4});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/5,
        kDefaultSectionId, /*keys=*/{2});

  std::vector<SectionId> expected_sections = {kDefaultSectionId};
  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/1, /*key_upper=*/3),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/5, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/2, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
}

TYPED_TEST(NumericIndexIntegerTest, EmptyResult) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumericIndex<int64_t>> integer_index,
      this->template CreateIntegerIndex<TypeParam>());

  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/0,
        kDefaultSectionId, /*keys=*/{1});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/1,
        kDefaultSectionId, /*keys=*/{3});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/2,
        kDefaultSectionId, /*keys=*/{2});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/3,
        kDefaultSectionId, /*keys=*/{0});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/4,
        kDefaultSectionId, /*keys=*/{4});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/5,
        kDefaultSectionId, /*keys=*/{2});

  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/10, /*key_upper=*/10),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/100, /*key_upper=*/200),
              IsOkAndHolds(IsEmpty()));
}

TYPED_TEST(NumericIndexIntegerTest,
           NonExistingPropertyPathShouldReturnEmptyResult) {
  constexpr std::string_view kAnotherPropertyPath = "another_property";

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumericIndex<int64_t>> integer_index,
      this->template CreateIntegerIndex<TypeParam>());

  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/0,
        kDefaultSectionId, /*keys=*/{1});

  EXPECT_THAT(Query(integer_index.get(), kAnotherPropertyPath,
                    /*key_lower=*/100, /*key_upper=*/200),
              IsOkAndHolds(IsEmpty()));
}

TYPED_TEST(NumericIndexIntegerTest,
           MultipleKeysShouldMergeAndDedupeDocHitInfo) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumericIndex<int64_t>> integer_index,
      this->template CreateIntegerIndex<TypeParam>());

  // Construct several documents with mutiple keys under the same section.
  // Range query [1, 3] will find hits with same (DocumentId, SectionId) for
  // mutiple times. For example, (2, kDefaultSectionId) will be found twice
  // (once for key = 1 and once for key = 3).
  // Test if the iterator dedupes correctly.
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/0,
        kDefaultSectionId, /*keys=*/{-1000, 0});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/1,
        kDefaultSectionId, /*keys=*/{-100, 0, 1, 2, 3, 4, 5});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/2,
        kDefaultSectionId, /*keys=*/{3, 1});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/3,
        kDefaultSectionId, /*keys=*/{4, 1});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/4,
        kDefaultSectionId, /*keys=*/{1, 6});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/5,
        kDefaultSectionId, /*keys=*/{2, 100});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/6,
        kDefaultSectionId, /*keys=*/{1000, 2});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/7,
        kDefaultSectionId, /*keys=*/{4, -1000});

  std::vector<SectionId> expected_sections = {kDefaultSectionId};
  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/1, /*key_upper=*/3),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/6, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/5, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/4, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/3, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/2, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections))));
}

TYPED_TEST(NumericIndexIntegerTest, EdgeNumericValues) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumericIndex<int64_t>> integer_index,
      this->template CreateIntegerIndex<TypeParam>());

  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/0,
        kDefaultSectionId, /*keys=*/{0});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/1,
        kDefaultSectionId, /*keys=*/{-100});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/2,
        kDefaultSectionId, /*keys=*/{-80});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/3,
        kDefaultSectionId, /*keys=*/{std::numeric_limits<int64_t>::max()});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/4,
        kDefaultSectionId, /*keys=*/{std::numeric_limits<int64_t>::min()});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/5,
        kDefaultSectionId, /*keys=*/{200});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/6,
        kDefaultSectionId, /*keys=*/{100});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/7,
        kDefaultSectionId, /*keys=*/{std::numeric_limits<int64_t>::max()});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/8,
        kDefaultSectionId, /*keys=*/{0});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/9,
        kDefaultSectionId, /*keys=*/{std::numeric_limits<int64_t>::min()});

  std::vector<SectionId> expected_sections = {kDefaultSectionId};

  // Negative key
  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/-100, /*key_upper=*/-70),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/2, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections))));

  // INT64_MAX key
  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/std::numeric_limits<int64_t>::max(),
                    /*key_upper=*/std::numeric_limits<int64_t>::max()),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/7, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/3, expected_sections))));

  // INT64_MIN key
  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/std::numeric_limits<int64_t>::min(),
                    /*key_upper=*/std::numeric_limits<int64_t>::min()),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/9, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/4, expected_sections))));

  // Key = 0
  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/0, /*key_upper=*/0),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/8, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));

  // All keys from INT64_MIN to INT64_MAX
  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/std::numeric_limits<int64_t>::min(),
                    /*key_upper=*/std::numeric_limits<int64_t>::max()),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/9, expected_sections),
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

TYPED_TEST(NumericIndexIntegerTest,
           MultipleSectionsShouldMergeSectionsAndDedupeDocHitInfo) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumericIndex<int64_t>> integer_index,
      this->template CreateIntegerIndex<TypeParam>());

  // Construct several documents with mutiple numeric sections.
  // Range query [1, 3] will find hits with same DocumentIds but multiple
  // different SectionIds. For example, there will be 2 hits (1, 0), (1, 1) for
  // DocumentId=1.
  // Test if the iterator merges multiple sections into a single SectionIdMask
  // correctly.
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/0,
        /*section_id=*/2, /*keys=*/{0});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/0,
        /*section_id=*/1, /*keys=*/{1});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/0,
        /*section_id=*/0, /*keys=*/{-1});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/1,
        /*section_id=*/2, /*keys=*/{2});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/1,
        /*section_id=*/1, /*keys=*/{1});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/1,
        /*section_id=*/0, /*keys=*/{4});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/2,
        /*section_id=*/5, /*keys=*/{3});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/2,
        /*section_id=*/4, /*keys=*/{2});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/2,
        /*section_id=*/3, /*keys=*/{5});

  EXPECT_THAT(
      Query(integer_index.get(), kDefaultTestPropertyPath, /*key_lower=*/1,
            /*key_upper=*/3),
      IsOkAndHolds(ElementsAre(
          EqualsDocHitInfo(/*document_id=*/2, std::vector<SectionId>{4, 5}),
          EqualsDocHitInfo(/*document_id=*/1, std::vector<SectionId>{1, 2}),
          EqualsDocHitInfo(/*document_id=*/0, std::vector<SectionId>{1}))));
}

TYPED_TEST(NumericIndexIntegerTest, NonRelevantPropertyShouldNotBeIncluded) {
  constexpr std::string_view kNonRelevantProperty = "non_relevant_property";

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumericIndex<int64_t>> integer_index,
      this->template CreateIntegerIndex<TypeParam>());

  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/0,
        kDefaultSectionId, /*keys=*/{1});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/1,
        kDefaultSectionId, /*keys=*/{3});
  Index(integer_index.get(), kNonRelevantProperty, /*document_id=*/2,
        kDefaultSectionId, /*keys=*/{2});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/3,
        kDefaultSectionId, /*keys=*/{0});
  Index(integer_index.get(), kNonRelevantProperty, /*document_id=*/4,
        kDefaultSectionId, /*keys=*/{4});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/5,
        kDefaultSectionId, /*keys=*/{2});

  std::vector<SectionId> expected_sections = {kDefaultSectionId};
  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/1, /*key_upper=*/3),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/5, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
}

TYPED_TEST(NumericIndexIntegerTest,
           RangeQueryKeyLowerGreaterThanKeyUpperShouldReturnError) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumericIndex<int64_t>> integer_index,
      this->template CreateIntegerIndex<TypeParam>());

  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/0,
        kDefaultSectionId, /*keys=*/{1});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/1,
        kDefaultSectionId, /*keys=*/{3});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/2,
        kDefaultSectionId, /*keys=*/{2});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/3,
        kDefaultSectionId, /*keys=*/{0});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/4,
        kDefaultSectionId, /*keys=*/{4});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/5,
        kDefaultSectionId, /*keys=*/{2});

  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/3, /*key_upper=*/1),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TYPED_TEST(NumericIndexIntegerTest, Optimize) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumericIndex<int64_t>> integer_index,
      this->template CreateIntegerIndex<TypeParam>());

  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/1,
        kDefaultSectionId, /*keys=*/{1});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/2,
        kDefaultSectionId, /*keys=*/{3});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/3,
        kDefaultSectionId, /*keys=*/{2});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/5,
        kDefaultSectionId, /*keys=*/{0});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/8,
        kDefaultSectionId, /*keys=*/{4});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/13,
        kDefaultSectionId, /*keys=*/{2});

  // Delete doc id = 3, 5, compress and keep the rest.
  std::vector<DocumentId> document_id_old_to_new(14, kInvalidDocumentId);
  document_id_old_to_new[1] = 0;
  document_id_old_to_new[2] = 1;
  document_id_old_to_new[8] = 2;
  document_id_old_to_new[13] = 3;

  DocumentId new_last_added_document_id = 3;
  EXPECT_THAT(integer_index->Optimize(document_id_old_to_new,
                                      new_last_added_document_id),
              IsOk());
  EXPECT_THAT(integer_index->last_added_document_id(),
              Eq(new_last_added_document_id));

  // Verify index and query API still work normally after Optimize().
  std::vector<SectionId> expected_sections = {kDefaultSectionId};
  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/1, /*key_upper=*/1),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/3, /*key_upper=*/3),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections))));
  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/0, /*key_upper=*/0),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/4, /*key_upper=*/4),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/2, expected_sections))));
  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/2, /*key_upper=*/2),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/3, expected_sections))));

  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/5,
        kDefaultSectionId, /*keys=*/{123});
  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/123, /*key_upper=*/123),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/5, expected_sections))));
}

TYPED_TEST(NumericIndexIntegerTest, OptimizeMultiplePropertyPaths) {
  constexpr std::string_view kPropertyPath1 = "prop1";
  constexpr SectionId kSectionId1 = 0;
  constexpr std::string_view kPropertyPath2 = "prop2";
  constexpr SectionId kSectionId2 = 1;

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumericIndex<int64_t>> integer_index,
      this->template CreateIntegerIndex<TypeParam>());

  // Doc id = 1: insert 2 data for "prop1", "prop2"
  Index(integer_index.get(), kPropertyPath2, /*document_id=*/1, kSectionId2,
        /*keys=*/{1});
  Index(integer_index.get(), kPropertyPath1, /*document_id=*/1, kSectionId1,
        /*keys=*/{2});

  // Doc id = 2: insert 1 data for "prop1".
  Index(integer_index.get(), kPropertyPath1, /*document_id=*/2, kSectionId1,
        /*keys=*/{3});

  // Doc id = 3: insert 2 data for "prop2"
  Index(integer_index.get(), kPropertyPath2, /*document_id=*/3, kSectionId2,
        /*keys=*/{4});

  // Doc id = 5: insert 3 data for "prop1", "prop2"
  Index(integer_index.get(), kPropertyPath2, /*document_id=*/5, kSectionId2,
        /*keys=*/{1});
  Index(integer_index.get(), kPropertyPath1, /*document_id=*/5, kSectionId1,
        /*keys=*/{2});

  // Doc id = 8: insert 1 data for "prop2".
  Index(integer_index.get(), kPropertyPath2, /*document_id=*/8, kSectionId2,
        /*keys=*/{3});

  // Doc id = 13: insert 1 data for "prop1".
  Index(integer_index.get(), kPropertyPath1, /*document_id=*/13, kSectionId1,
        /*keys=*/{4});

  // Delete doc id = 3, 5, compress and keep the rest.
  std::vector<DocumentId> document_id_old_to_new(14, kInvalidDocumentId);
  document_id_old_to_new[1] = 0;
  document_id_old_to_new[2] = 1;
  document_id_old_to_new[8] = 2;
  document_id_old_to_new[13] = 3;

  DocumentId new_last_added_document_id = 3;
  EXPECT_THAT(integer_index->Optimize(document_id_old_to_new,
                                      new_last_added_document_id),
              IsOk());
  EXPECT_THAT(integer_index->last_added_document_id(),
              Eq(new_last_added_document_id));

  // Verify index and query API still work normally after Optimize().
  // Key = 1
  EXPECT_THAT(Query(integer_index.get(), kPropertyPath1, /*key_lower=*/1,
                    /*key_upper=*/1),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(Query(integer_index.get(), kPropertyPath2, /*key_lower=*/1,
                    /*key_upper=*/1),
              IsOkAndHolds(ElementsAre(EqualsDocHitInfo(
                  /*document_id=*/0, std::vector<SectionId>{kSectionId2}))));

  // key = 2
  EXPECT_THAT(Query(integer_index.get(), kPropertyPath1, /*key_lower=*/2,
                    /*key_upper=*/2),
              IsOkAndHolds(ElementsAre(EqualsDocHitInfo(
                  /*document_id=*/0, std::vector<SectionId>{kSectionId1}))));
  EXPECT_THAT(Query(integer_index.get(), kPropertyPath2, /*key_lower=*/2,
                    /*key_upper=*/2),
              IsOkAndHolds(IsEmpty()));

  // key = 3
  EXPECT_THAT(Query(integer_index.get(), kPropertyPath1, /*key_lower=*/3,
                    /*key_upper=*/3),
              IsOkAndHolds(ElementsAre(EqualsDocHitInfo(
                  /*document_id=*/1, std::vector<SectionId>{kSectionId1}))));
  EXPECT_THAT(Query(integer_index.get(), kPropertyPath2, /*key_lower=*/3,
                    /*key_upper=*/3),
              IsOkAndHolds(ElementsAre(EqualsDocHitInfo(
                  /*document_id=*/2, std::vector<SectionId>{kSectionId2}))));

  // key = 4
  EXPECT_THAT(Query(integer_index.get(), kPropertyPath1, /*key_lower=*/4,
                    /*key_upper=*/4),
              IsOkAndHolds(ElementsAre(EqualsDocHitInfo(
                  /*document_id=*/3, std::vector<SectionId>{kSectionId1}))));
  EXPECT_THAT(Query(integer_index.get(), kPropertyPath2, /*key_lower=*/4,
                    /*key_upper=*/4),
              IsOkAndHolds(IsEmpty()));
}

TYPED_TEST(NumericIndexIntegerTest, OptimizeShouldDiscardEmptyPropertyStorage) {
  constexpr std::string_view kPropertyPath1 = "prop1";
  constexpr SectionId kSectionId1 = 0;
  constexpr std::string_view kPropertyPath2 = "prop2";
  constexpr SectionId kSectionId2 = 1;

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumericIndex<int64_t>> integer_index,
      this->template CreateIntegerIndex<TypeParam>());

  // Doc id = 1: insert 2 data for "prop1", "prop2"
  Index(integer_index.get(), kPropertyPath2, /*document_id=*/1, kSectionId2,
        /*keys=*/{1});
  Index(integer_index.get(), kPropertyPath1, /*document_id=*/1, kSectionId1,
        /*keys=*/{2});

  // Doc id = 2: insert 1 data for "prop1".
  Index(integer_index.get(), kPropertyPath1, /*document_id=*/2, kSectionId1,
        /*keys=*/{3});

  // Doc id = 3: insert 2 data for "prop2"
  Index(integer_index.get(), kPropertyPath2, /*document_id=*/3, kSectionId2,
        /*keys=*/{4});

  // Delete doc id = 1, 3, compress and keep the rest.
  std::vector<DocumentId> document_id_old_to_new(4, kInvalidDocumentId);
  document_id_old_to_new[2] = 0;

  DocumentId new_last_added_document_id = 0;
  EXPECT_THAT(integer_index->Optimize(document_id_old_to_new,
                                      new_last_added_document_id),
              IsOk());
  EXPECT_THAT(integer_index->last_added_document_id(),
              Eq(new_last_added_document_id));

  // All data in "prop2" as well as the underlying storage should be deleted, so
  // when querying "prop2", we should get empty result.
  EXPECT_THAT(Query(integer_index.get(), kPropertyPath2,
                    /*key_lower=*/std::numeric_limits<int64_t>::min(),
                    /*key_upper=*/std::numeric_limits<int64_t>::max()),
              IsOkAndHolds(IsEmpty()));
  if (std::is_same_v<IntegerIndex, TypeParam>) {
    std::string prop2_storage_working_path =
        absl_ports::StrCat(this->working_path_, "/", kPropertyPath2);
    EXPECT_THAT(
        this->filesystem_.DirectoryExists(prop2_storage_working_path.c_str()),
        IsFalse());
  }

  // Verify we can still index and query for "prop2".
  Index(integer_index.get(), kPropertyPath2, /*document_id=*/100, kSectionId2,
        /*keys=*/{123});
  EXPECT_THAT(Query(integer_index.get(), kPropertyPath2,
                    /*key_lower=*/123, /*key_upper=*/123),
              IsOkAndHolds(ElementsAre(EqualsDocHitInfo(
                  /*document_id=*/100, std::vector<SectionId>{kSectionId2}))));
}

TYPED_TEST(NumericIndexIntegerTest, OptimizeOutOfRangeDocumentId) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumericIndex<int64_t>> integer_index,
      this->template CreateIntegerIndex<TypeParam>());

  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/1,
        kDefaultSectionId, /*keys=*/{1});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/2,
        kDefaultSectionId, /*keys=*/{3});

  // Create document_id_old_to_new with size = 2. Optimize should handle out of
  // range DocumentId properly.
  std::vector<DocumentId> document_id_old_to_new(2, kInvalidDocumentId);

  EXPECT_THAT(integer_index->Optimize(
                  document_id_old_to_new,
                  /*new_last_added_document_id=*/kInvalidDocumentId),
              IsOk());
  EXPECT_THAT(integer_index->last_added_document_id(), Eq(kInvalidDocumentId));

  // Verify all data are discarded after Optimize().
  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/std::numeric_limits<int64_t>::min(),
                    /*key_upper=*/std::numeric_limits<int64_t>::max()),
              IsOkAndHolds(IsEmpty()));
}

TYPED_TEST(NumericIndexIntegerTest, OptimizeDeleteAll) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumericIndex<int64_t>> integer_index,
      this->template CreateIntegerIndex<TypeParam>());

  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/1,
        kDefaultSectionId, /*keys=*/{1});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/2,
        kDefaultSectionId, /*keys=*/{3});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/3,
        kDefaultSectionId, /*keys=*/{2});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/5,
        kDefaultSectionId, /*keys=*/{0});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/8,
        kDefaultSectionId, /*keys=*/{4});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/13,
        kDefaultSectionId, /*keys=*/{2});

  // Delete all documents.
  std::vector<DocumentId> document_id_old_to_new(14, kInvalidDocumentId);

  EXPECT_THAT(integer_index->Optimize(
                  document_id_old_to_new,
                  /*new_last_added_document_id=*/kInvalidDocumentId),
              IsOk());
  EXPECT_THAT(integer_index->last_added_document_id(), Eq(kInvalidDocumentId));

  // Verify all data are discarded after Optimize().
  EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/std::numeric_limits<int64_t>::min(),
                    /*key_upper=*/std::numeric_limits<int64_t>::max()),
              IsOkAndHolds(IsEmpty()));
}

TYPED_TEST(NumericIndexIntegerTest, Clear) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<NumericIndex<int64_t>> integer_index,
      this->template CreateIntegerIndex<TypeParam>());

  Index(integer_index.get(), /*property_path=*/"A", /*document_id=*/0,
        kDefaultSectionId, /*keys=*/{1});
  Index(integer_index.get(), /*property_path=*/"B", /*document_id=*/1,
        kDefaultSectionId, /*keys=*/{3});
  integer_index->set_last_added_document_id(1);

  ASSERT_THAT(integer_index->last_added_document_id(), Eq(1));
  ASSERT_THAT(
      Query(integer_index.get(), /*property_path=*/"A", /*key_lower=*/1,
            /*key_upper=*/1),
      IsOkAndHolds(ElementsAre(EqualsDocHitInfo(
          /*document_id=*/0, std::vector<SectionId>{kDefaultSectionId}))));
  ASSERT_THAT(
      Query(integer_index.get(), /*property_path=*/"B", /*key_lower=*/3,
            /*key_upper=*/3),
      IsOkAndHolds(ElementsAre(EqualsDocHitInfo(
          /*document_id=*/1, std::vector<SectionId>{kDefaultSectionId}))));

  // After resetting, last_added_document_id should be set to
  // kInvalidDocumentId, and the previous added keys should be deleted.
  ICING_ASSERT_OK(integer_index->Clear());
  EXPECT_THAT(integer_index->last_added_document_id(), Eq(kInvalidDocumentId));
  EXPECT_THAT(Query(integer_index.get(), /*property_path=*/"A", /*key_lower=*/1,
                    /*key_upper=*/1),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(Query(integer_index.get(), /*property_path=*/"B", /*key_lower=*/3,
                    /*key_upper=*/3),
              IsOkAndHolds(IsEmpty()));

  // Integer index should be able to work normally after Clear().
  Index(integer_index.get(), /*property_path=*/"A", /*document_id=*/3,
        kDefaultSectionId, /*keys=*/{123});
  Index(integer_index.get(), /*property_path=*/"B", /*document_id=*/4,
        kDefaultSectionId, /*keys=*/{456});
  integer_index->set_last_added_document_id(4);

  EXPECT_THAT(integer_index->last_added_document_id(), Eq(4));
  EXPECT_THAT(
      Query(integer_index.get(), /*property_path=*/"A", /*key_lower=*/123,
            /*key_upper=*/123),
      IsOkAndHolds(ElementsAre(EqualsDocHitInfo(
          /*document_id=*/3, std::vector<SectionId>{kDefaultSectionId}))));
  EXPECT_THAT(
      Query(integer_index.get(), /*property_path=*/"B", /*key_lower=*/456,
            /*key_upper=*/456),
      IsOkAndHolds(ElementsAre(EqualsDocHitInfo(
          /*document_id=*/4, std::vector<SectionId>{kDefaultSectionId}))));
}

// Tests for persistent integer index only
class IntegerIndexTest : public NumericIndexIntegerTest<IntegerIndex> {};

TEST_F(IntegerIndexTest, InvalidWorkingPath) {
  EXPECT_THAT(IntegerIndex::Create(filesystem_, "/dev/null/integer_index_test"),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL));
}

TEST_F(IntegerIndexTest, InitializeNewFiles) {
  {
    ASSERT_FALSE(filesystem_.DirectoryExists(working_path_.c_str()));
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<IntegerIndex> integer_index,
        IntegerIndex::Create(filesystem_, working_path_));

    ICING_ASSERT_OK(integer_index->PersistToDisk());
  }

  // Metadata file should be initialized correctly for both info and crcs
  // sections.
  const std::string metadata_file_path =
      absl_ports::StrCat(working_path_, "/", IntegerIndex::kFilePrefix, ".m");
  ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
  ASSERT_TRUE(metadata_sfd.is_valid());

  // Check info section
  Info info;
  ASSERT_TRUE(filesystem_.PRead(metadata_sfd.get(), &info, sizeof(Info),
                                IntegerIndex::kInfoMetadataFileOffset));
  EXPECT_THAT(info.magic, Eq(Info::kMagic));
  EXPECT_THAT(info.last_added_document_id, Eq(kInvalidDocumentId));

  // Check crcs section
  Crcs crcs;
  ASSERT_TRUE(filesystem_.PRead(metadata_sfd.get(), &crcs, sizeof(Crcs),
                                IntegerIndex::kCrcsMetadataFileOffset));
  // There are no storages initially, so storages_crc should be 0.
  EXPECT_THAT(crcs.component_crcs.storages_crc, Eq(0));
  EXPECT_THAT(crcs.component_crcs.info_crc,
              Eq(Crc32(std::string_view(reinterpret_cast<const char*>(&info),
                                        sizeof(Info)))
                     .Get()));
  EXPECT_THAT(crcs.all_crc,
              Eq(Crc32(std::string_view(
                           reinterpret_cast<const char*>(&crcs.component_crcs),
                           sizeof(Crcs::ComponentCrcs)))
                     .Get()));
}

TEST_F(IntegerIndexTest,
       InitializationShouldFailWithoutPersistToDiskOrDestruction) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<IntegerIndex> integer_index,
                             IntegerIndex::Create(filesystem_, working_path_));

  // Insert some data.
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/0,
        /*section_id=*/20, /*keys=*/{0, 100, -100});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/1,
        /*section_id=*/2, /*keys=*/{3, -1000, 500});
  Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/2,
        /*section_id=*/15, /*keys=*/{-6, 321, 98});

  // Without calling PersistToDisk, checksums will not be recomputed or synced
  // to disk, so initializing another instance on the same files should fail.
  EXPECT_THAT(IntegerIndex::Create(filesystem_, working_path_),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(IntegerIndexTest, InitializationShouldSucceedWithPersistToDisk) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<IntegerIndex> integer_index1,
                             IntegerIndex::Create(filesystem_, working_path_));

  // Insert some data.
  Index(integer_index1.get(), kDefaultTestPropertyPath, /*document_id=*/0,
        /*section_id=*/20, /*keys=*/{0, 100, -100});
  Index(integer_index1.get(), kDefaultTestPropertyPath, /*document_id=*/1,
        /*section_id=*/2, /*keys=*/{3, -1000, 500});
  Index(integer_index1.get(), kDefaultTestPropertyPath, /*document_id=*/2,
        /*section_id=*/15, /*keys=*/{-6, 321, 98});
  integer_index1->set_last_added_document_id(2);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<DocHitInfo> doc_hit_info_vec,
      Query(integer_index1.get(), kDefaultTestPropertyPath,
            /*key_lower=*/std::numeric_limits<int64_t>::min(),
            /*key_upper=*/std::numeric_limits<int64_t>::max()));

  // After calling PersistToDisk, all checksums should be recomputed and synced
  // correctly to disk, so initializing another instance on the same files
  // should succeed, and we should be able to get the same contents.
  ICING_EXPECT_OK(integer_index1->PersistToDisk());

  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<IntegerIndex> integer_index2,
                             IntegerIndex::Create(filesystem_, working_path_));
  EXPECT_THAT(integer_index2->last_added_document_id(), Eq(2));
  EXPECT_THAT(Query(integer_index2.get(), kDefaultTestPropertyPath,
                    /*key_lower=*/std::numeric_limits<int64_t>::min(),
                    /*key_upper=*/std::numeric_limits<int64_t>::max()),
              IsOkAndHolds(ElementsAreArray(doc_hit_info_vec.begin(),
                                            doc_hit_info_vec.end())));
}

TEST_F(IntegerIndexTest, InitializationShouldSucceedAfterDestruction) {
  std::vector<DocHitInfo> doc_hit_info_vec;
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<IntegerIndex> integer_index,
        IntegerIndex::Create(filesystem_, working_path_));

    // Insert some data.
    Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/0,
          /*section_id=*/20, /*keys=*/{0, 100, -100});
    Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/1,
          /*section_id=*/2, /*keys=*/{3, -1000, 500});
    Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/2,
          /*section_id=*/15, /*keys=*/{-6, 321, 98});
    integer_index->set_last_added_document_id(2);
    ICING_ASSERT_OK_AND_ASSIGN(
        doc_hit_info_vec,
        Query(integer_index.get(), kDefaultTestPropertyPath,
              /*key_lower=*/std::numeric_limits<int64_t>::min(),
              /*key_upper=*/std::numeric_limits<int64_t>::max()));
  }

  {
    // The previous instance went out of scope and was destructed. Although we
    // didn't call PersistToDisk explicitly, the destructor should invoke it and
    // thus initializing another instance on the same files should succeed, and
    // we should be able to get the same contents.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<IntegerIndex> integer_index,
        IntegerIndex::Create(filesystem_, working_path_));
    EXPECT_THAT(integer_index->last_added_document_id(), Eq(2));
    EXPECT_THAT(Query(integer_index.get(), kDefaultTestPropertyPath,
                      /*key_lower=*/std::numeric_limits<int64_t>::min(),
                      /*key_upper=*/std::numeric_limits<int64_t>::max()),
                IsOkAndHolds(ElementsAreArray(doc_hit_info_vec.begin(),
                                              doc_hit_info_vec.end())));
  }
}

TEST_F(IntegerIndexTest, InitializeExistingFilesWithWrongAllCrcShouldFail) {
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<IntegerIndex> integer_index,
        IntegerIndex::Create(filesystem_, working_path_));
    // Insert some data.
    Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/0,
          /*section_id=*/20, /*keys=*/{0, 100, -100});
    Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/1,
          /*section_id=*/2, /*keys=*/{3, -1000, 500});
    Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/2,
          /*section_id=*/15, /*keys=*/{-6, 321, 98});

    ICING_ASSERT_OK(integer_index->PersistToDisk());
  }

  const std::string metadata_file_path =
      absl_ports::StrCat(working_path_, "/", IntegerIndex::kFilePrefix, ".m");
  ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
  ASSERT_TRUE(metadata_sfd.is_valid());

  Crcs crcs;
  ASSERT_TRUE(filesystem_.PRead(metadata_sfd.get(), &crcs, sizeof(Crcs),
                                IntegerIndex::kCrcsMetadataFileOffset));

  // Manually corrupt all_crc
  crcs.all_crc += kCorruptedValueOffset;
  ASSERT_TRUE(filesystem_.PWrite(metadata_sfd.get(),
                                 IntegerIndexStorage::kCrcsMetadataFileOffset,
                                 &crcs, sizeof(Crcs)));
  metadata_sfd.reset();

  {
    // Attempt to create the integer index with metadata containing corrupted
    // all_crc. This should fail.
    libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndex>>
        integer_index_or = IntegerIndex::Create(filesystem_, working_path_);
    EXPECT_THAT(integer_index_or,
                StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
    EXPECT_THAT(integer_index_or.status().error_message(),
                HasSubstr("Invalid all crc"));
  }
}

TEST_F(IntegerIndexTest, InitializeExistingFilesWithCorruptedInfoShouldFail) {
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<IntegerIndex> integer_index,
        IntegerIndex::Create(filesystem_, working_path_));
    // Insert some data.
    Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/0,
          /*section_id=*/20, /*keys=*/{0, 100, -100});
    Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/1,
          /*section_id=*/2, /*keys=*/{3, -1000, 500});
    Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/2,
          /*section_id=*/15, /*keys=*/{-6, 321, 98});

    ICING_ASSERT_OK(integer_index->PersistToDisk());
  }

  const std::string metadata_file_path =
      absl_ports::StrCat(working_path_, "/", IntegerIndex::kFilePrefix, ".m");
  ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
  ASSERT_TRUE(metadata_sfd.is_valid());

  Info info;
  ASSERT_TRUE(filesystem_.PRead(metadata_sfd.get(), &info, sizeof(Info),
                                IntegerIndex::kInfoMetadataFileOffset));

  // Modify info, but don't update the checksum. This would be similar to
  // corruption of info.
  info.last_added_document_id += kCorruptedValueOffset;
  ASSERT_TRUE(filesystem_.PWrite(metadata_sfd.get(),
                                 IntegerIndex::kInfoMetadataFileOffset, &info,
                                 sizeof(Info)));
  metadata_sfd.reset();

  {
    // Attempt to create the integer index with info that doesn't match its
    // checksum and confirm that it fails.
    libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndex>>
        integer_index_or = IntegerIndex::Create(filesystem_, working_path_);
    EXPECT_THAT(integer_index_or,
                StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
    EXPECT_THAT(integer_index_or.status().error_message(),
                HasSubstr("Invalid info crc"));
  }
}

TEST_F(IntegerIndexTest,
       InitializeExistingFilesWithCorruptedStoragesShouldFail) {
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<IntegerIndex> integer_index,
        IntegerIndex::Create(filesystem_, working_path_));
    // Insert some data.
    Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/0,
          /*section_id=*/20, /*keys=*/{0, 100, -100});
    Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/1,
          /*section_id=*/2, /*keys=*/{3, -1000, 500});
    Index(integer_index.get(), kDefaultTestPropertyPath, /*document_id=*/2,
          /*section_id=*/15, /*keys=*/{-6, 321, 98});

    ICING_ASSERT_OK(integer_index->PersistToDisk());
  }

  {
    // Corrupt integer index storage for kDefaultTestPropertyPath manually.
    PostingListIntegerIndexSerializer posting_list_integer_index_serializer;
    std::string storage_working_path =
        absl_ports::StrCat(working_path_, "/", kDefaultTestPropertyPath);
    ASSERT_TRUE(filesystem_.DirectoryExists(storage_working_path.c_str()));

    ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<IntegerIndexStorage> storage,
                               IntegerIndexStorage::Create(
                                   filesystem_, std::move(storage_working_path),
                                   IntegerIndexStorage::Options(),
                                   &posting_list_integer_index_serializer));
    ICING_ASSERT_OK(storage->AddKeys(/*document_id=*/3, /*section_id=*/4,
                                     /*new_keys=*/{3, 4, 5}));

    ICING_ASSERT_OK(storage->PersistToDisk());
  }

  {
    // Attempt to create the integer index with corrupted storages. This should
    // fail.
    libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndex>>
        integer_index_or = IntegerIndex::Create(filesystem_, working_path_);
    EXPECT_THAT(integer_index_or,
                StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
    EXPECT_THAT(integer_index_or.status().error_message(),
                HasSubstr("Invalid storages crc"));
  }
}
TEST_F(IntegerIndexTest,
       IntegerIndexShouldWorkAfterOptimizeAndReinitialization) {
  constexpr std::string_view kPropertyPath1 = "prop1";
  constexpr SectionId kSectionId1 = 0;
  constexpr std::string_view kPropertyPath2 = "prop2";
  constexpr SectionId kSectionId2 = 1;

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<IntegerIndex> integer_index,
        IntegerIndex::Create(filesystem_, working_path_));

    // Doc id = 1: insert 2 data for "prop1", "prop2"
    Index(integer_index.get(), kPropertyPath2, /*document_id=*/1, kSectionId2,
          /*keys=*/{1});
    Index(integer_index.get(), kPropertyPath1, /*document_id=*/1, kSectionId1,
          /*keys=*/{2});

    // Doc id = 2: insert 1 data for "prop1".
    Index(integer_index.get(), kPropertyPath1, /*document_id=*/2, kSectionId1,
          /*keys=*/{3});

    // Doc id = 3: insert 2 data for "prop2"
    Index(integer_index.get(), kPropertyPath2, /*document_id=*/3, kSectionId2,
          /*keys=*/{4});

    // Doc id = 5: insert 3 data for "prop1", "prop2"
    Index(integer_index.get(), kPropertyPath2, /*document_id=*/5, kSectionId2,
          /*keys=*/{1});
    Index(integer_index.get(), kPropertyPath1, /*document_id=*/5, kSectionId1,
          /*keys=*/{2});

    // Doc id = 8: insert 1 data for "prop2".
    Index(integer_index.get(), kPropertyPath2, /*document_id=*/8, kSectionId2,
          /*keys=*/{3});

    // Doc id = 13: insert 1 data for "prop1".
    Index(integer_index.get(), kPropertyPath1, /*document_id=*/13, kSectionId1,
          /*keys=*/{4});

    // Delete doc id = 3, 5, compress and keep the rest.
    std::vector<DocumentId> document_id_old_to_new(14, kInvalidDocumentId);
    document_id_old_to_new[1] = 0;
    document_id_old_to_new[2] = 1;
    document_id_old_to_new[8] = 2;
    document_id_old_to_new[13] = 3;

    DocumentId new_last_added_document_id = 3;
    EXPECT_THAT(integer_index->Optimize(document_id_old_to_new,
                                        new_last_added_document_id),
                IsOk());
    EXPECT_THAT(integer_index->last_added_document_id(),
                Eq(new_last_added_document_id));
  }

  {
    // Reinitialize IntegerIndex and verify index and query API still work
    // normally.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<IntegerIndex> integer_index,
        IntegerIndex::Create(filesystem_, working_path_));

    // Key = 1
    EXPECT_THAT(Query(integer_index.get(), kPropertyPath1, /*key_lower=*/1,
                      /*key_upper=*/1),
                IsOkAndHolds(IsEmpty()));
    EXPECT_THAT(Query(integer_index.get(), kPropertyPath2, /*key_lower=*/1,
                      /*key_upper=*/1),
                IsOkAndHolds(ElementsAre(EqualsDocHitInfo(
                    /*document_id=*/0, std::vector<SectionId>{kSectionId2}))));

    // key = 2
    EXPECT_THAT(Query(integer_index.get(), kPropertyPath1, /*key_lower=*/2,
                      /*key_upper=*/2),
                IsOkAndHolds(ElementsAre(EqualsDocHitInfo(
                    /*document_id=*/0, std::vector<SectionId>{kSectionId1}))));
    EXPECT_THAT(Query(integer_index.get(), kPropertyPath2, /*key_lower=*/2,
                      /*key_upper=*/2),
                IsOkAndHolds(IsEmpty()));

    // key = 3
    EXPECT_THAT(Query(integer_index.get(), kPropertyPath1, /*key_lower=*/3,
                      /*key_upper=*/3),
                IsOkAndHolds(ElementsAre(EqualsDocHitInfo(
                    /*document_id=*/1, std::vector<SectionId>{kSectionId1}))));
    EXPECT_THAT(Query(integer_index.get(), kPropertyPath2, /*key_lower=*/3,
                      /*key_upper=*/3),
                IsOkAndHolds(ElementsAre(EqualsDocHitInfo(
                    /*document_id=*/2, std::vector<SectionId>{kSectionId2}))));

    // key = 4
    EXPECT_THAT(Query(integer_index.get(), kPropertyPath1, /*key_lower=*/4,
                      /*key_upper=*/4),
                IsOkAndHolds(ElementsAre(EqualsDocHitInfo(
                    /*document_id=*/3, std::vector<SectionId>{kSectionId1}))));
    EXPECT_THAT(Query(integer_index.get(), kPropertyPath2, /*key_lower=*/4,
                      /*key_upper=*/4),
                IsOkAndHolds(IsEmpty()));

    // Index new data.
    Index(integer_index.get(), kPropertyPath2, /*document_id=*/100, kSectionId2,
          /*keys=*/{123});
    Index(integer_index.get(), kPropertyPath1, /*document_id=*/100, kSectionId1,
          /*keys=*/{456});
    EXPECT_THAT(
        Query(integer_index.get(), kPropertyPath2, /*key_lower=*/123,
              /*key_upper=*/456),
        IsOkAndHolds(ElementsAre(EqualsDocHitInfo(
            /*document_id=*/100, std::vector<SectionId>{kSectionId2}))));
    EXPECT_THAT(
        Query(integer_index.get(), kPropertyPath1, /*key_lower=*/123,
              /*key_upper=*/456),
        IsOkAndHolds(ElementsAre(EqualsDocHitInfo(
            /*document_id=*/100, std::vector<SectionId>{kSectionId1}))));
  }
}

}  // namespace

}  // namespace lib
}  // namespace icing
