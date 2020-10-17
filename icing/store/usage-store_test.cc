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

#include "icing/store/usage-store.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {
using ::testing::Eq;
using ::testing::Not;

class UsageStoreTest : public testing::Test {
 protected:
  UsageStoreTest() : test_dir_(GetTestTempDir() + "/usage-store-test") {}

  void SetUp() override {
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  const Filesystem filesystem_;
  const std::string test_dir_;
};

UsageReport CreateUsageReport(std::string name_space, std::string uri,
                              int64 timestamp_ms,
                              UsageReport::UsageType usage_type) {
  UsageReport usage_report;
  usage_report.set_document_namespace(name_space);
  usage_report.set_document_uri(uri);
  usage_report.set_usage_timestamp_ms(timestamp_ms);
  usage_report.set_usage_type(usage_type);
  return usage_report;
}

TEST_F(UsageStoreTest, CreationShouldSucceed) {
  EXPECT_THAT(UsageStore::Create(&filesystem_, test_dir_), IsOk());
}

TEST_F(UsageStoreTest, CreationShouldFailOnNullPointer) {
  EXPECT_THAT(UsageStore::Create(nullptr, test_dir_),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(UsageStoreTest, UsageScoresShouldBeComparable) {
  UsageStore::UsageScores scores1;
  UsageStore::UsageScores scores2;
  EXPECT_THAT(scores1, Eq(scores2));

  // operator== should compare usage_type1_last_used_timestamp_s.
  ++scores1.usage_type1_last_used_timestamp_s;
  EXPECT_THAT(scores1, Not(Eq(scores2)));
  ++scores2.usage_type1_last_used_timestamp_s;
  EXPECT_THAT(scores1, Eq(scores2));

  // operator== should compare usage_type2_last_used_timestamp_s.
  ++scores1.usage_type2_last_used_timestamp_s;
  EXPECT_THAT(scores1, Not(Eq(scores2)));
  ++scores2.usage_type2_last_used_timestamp_s;
  EXPECT_THAT(scores1, Eq(scores2));

  // operator== should compare usage_type3_last_used_timestamp_s.
  ++scores1.usage_type3_last_used_timestamp_s;
  EXPECT_THAT(scores1, Not(Eq(scores2)));
  ++scores2.usage_type3_last_used_timestamp_s;
  EXPECT_THAT(scores1, Eq(scores2));

  // operator== should compare usage_type1_count.
  ++scores1.usage_type1_count;
  EXPECT_THAT(scores1, Not(Eq(scores2)));
  ++scores2.usage_type1_count;
  EXPECT_THAT(scores1, Eq(scores2));

  // operator== should compare usage_type2_count.
  ++scores1.usage_type2_count;
  EXPECT_THAT(scores1, Not(Eq(scores2)));
  ++scores2.usage_type2_count;
  EXPECT_THAT(scores1, Eq(scores2));

  // operator== should compare usage_type3_count.
  ++scores1.usage_type3_count;
  EXPECT_THAT(scores1, Not(Eq(scores2)));
  ++scores2.usage_type3_count;
  EXPECT_THAT(scores1, Eq(scores2));
}

TEST_F(UsageStoreTest, InvalidDocumentIdShouldReturnError) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<UsageStore> usage_store,
                             UsageStore::Create(&filesystem_, test_dir_));

  DocumentId invalid_document_id = -1;

  EXPECT_THAT(usage_store->AddUsageReport(UsageReport(), invalid_document_id),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(usage_store->DeleteUsageScores(invalid_document_id),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(usage_store->GetUsageScores(invalid_document_id),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(usage_store->SetUsageScores(invalid_document_id,
                                          UsageStore::UsageScores()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(UsageStoreTest, AddUsageReportShouldUpdateLastUsedTimestamp) {
  // Create 3 reports with different timestamps.
  UsageReport usage_report_time1 = CreateUsageReport(
      "namespace", "uri", /*timestamp_ms=*/1000, UsageReport::USAGE_TYPE1);
  UsageReport usage_report_time5 = CreateUsageReport(
      "namespace", "uri", /*timestamp_ms=*/5000, UsageReport::USAGE_TYPE1);
  UsageReport usage_report_time10 = CreateUsageReport(
      "namespace", "uri", /*timestamp_ms=*/10000, UsageReport::USAGE_TYPE1);

  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<UsageStore> usage_store,
                             UsageStore::Create(&filesystem_, test_dir_));

  // Report a usage with timestamp 5.
  usage_store->AddUsageReport(usage_report_time5, /*document_id=*/1);
  UsageStore::UsageScores expected_scores;
  expected_scores.usage_type1_last_used_timestamp_s = 5;
  expected_scores.usage_type1_count = 1;
  expected_scores.usage_type2_count = 0;
  expected_scores.usage_type3_count = 0;
  EXPECT_THAT(usage_store->GetUsageScores(/*document_id=*/1),
              IsOkAndHolds(expected_scores));

  // Report a usage with timestamp 1. The timestamp won't be updated.
  usage_store->AddUsageReport(usage_report_time1, /*document_id=*/1);
  ++expected_scores.usage_type1_count;
  EXPECT_THAT(usage_store->GetUsageScores(/*document_id=*/1),
              IsOkAndHolds(expected_scores));

  // Report a usage with timestamp 10. The timestamp should be updated.
  usage_store->AddUsageReport(usage_report_time10, /*document_id=*/1);
  expected_scores.usage_type1_last_used_timestamp_s = 10;
  ++expected_scores.usage_type1_count;
  EXPECT_THAT(usage_store->GetUsageScores(/*document_id=*/1),
              IsOkAndHolds(expected_scores));
}

TEST_F(UsageStoreTest, AddUsageReportShouldUpdateCounts) {
  // Create 3 reports with different usage types.
  UsageReport usage_report_type1 = CreateUsageReport(
      "namespace", "uri", /*timestamp_ms=*/0, UsageReport::USAGE_TYPE1);
  UsageReport usage_report_type2 = CreateUsageReport(
      "namespace", "uri", /*timestamp_ms=*/0, UsageReport::USAGE_TYPE2);
  UsageReport usage_report_type3 = CreateUsageReport(
      "namespace", "uri", /*timestamp_ms=*/0, UsageReport::USAGE_TYPE3);

  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<UsageStore> usage_store,
                             UsageStore::Create(&filesystem_, test_dir_));

  // Report a usage with type 1.
  usage_store->AddUsageReport(usage_report_type1, /*document_id=*/1);
  UsageStore::UsageScores expected_scores;
  expected_scores.usage_type1_count = 1;
  expected_scores.usage_type2_count = 0;
  expected_scores.usage_type3_count = 0;
  EXPECT_THAT(usage_store->GetUsageScores(/*document_id=*/1),
              IsOkAndHolds(expected_scores));
  // Report another usage with type 1.
  usage_store->AddUsageReport(usage_report_type1, /*document_id=*/1);
  ++expected_scores.usage_type1_count;
  EXPECT_THAT(usage_store->GetUsageScores(/*document_id=*/1),
              IsOkAndHolds(expected_scores));

  // Report a usage with type 2.
  usage_store->AddUsageReport(usage_report_type2, /*document_id=*/1);
  ++expected_scores.usage_type2_count;
  EXPECT_THAT(usage_store->GetUsageScores(/*document_id=*/1),
              IsOkAndHolds(expected_scores));
  // Report another usage with type 2.
  usage_store->AddUsageReport(usage_report_type2, /*document_id=*/1);
  ++expected_scores.usage_type2_count;
  EXPECT_THAT(usage_store->GetUsageScores(/*document_id=*/1),
              IsOkAndHolds(expected_scores));

  // Report a usage with type 3.
  usage_store->AddUsageReport(usage_report_type3, /*document_id=*/1);
  ++expected_scores.usage_type3_count;
  EXPECT_THAT(usage_store->GetUsageScores(/*document_id=*/1),
              IsOkAndHolds(expected_scores));
  // Report another usage with type 3.
  usage_store->AddUsageReport(usage_report_type3, /*document_id=*/1);
  ++expected_scores.usage_type3_count;
  EXPECT_THAT(usage_store->GetUsageScores(/*document_id=*/1),
              IsOkAndHolds(expected_scores));
}

TEST_F(UsageStoreTest, GetNonExistingDocumentShouldReturnDefaultScores) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<UsageStore> usage_store,
                             UsageStore::Create(&filesystem_, test_dir_));

  EXPECT_THAT(usage_store->GetUsageScores(/*document_id=*/1),
              IsOkAndHolds(UsageStore::UsageScores()));
}

TEST_F(UsageStoreTest, SetAndGetUsageScores) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<UsageStore> usage_store,
                             UsageStore::Create(&filesystem_, test_dir_));

  // Create usage scores with some random numbers.
  UsageStore::UsageScores scores;
  scores.usage_type1_last_used_timestamp_s = 7;
  scores.usage_type2_last_used_timestamp_s = 9;
  scores.usage_type3_last_used_timestamp_s = 11;
  scores.usage_type1_count = 3;
  scores.usage_type2_count = 4;
  scores.usage_type3_count = 9;

  // Verify that set and get results are consistent.
  ICING_EXPECT_OK(usage_store->SetUsageScores(/*document_id=*/1, scores));
  EXPECT_THAT(usage_store->GetUsageScores(/*document_id=*/1),
              IsOkAndHolds(scores));
}

TEST_F(UsageStoreTest, ImplicitlyInitializedScoresShouldBeZero) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<UsageStore> usage_store,
                             UsageStore::Create(&filesystem_, test_dir_));

  // Explicitly set scores for document 2.
  ICING_ASSERT_OK(usage_store->SetUsageScores(/*document_id=*/2,
                                              UsageStore::UsageScores()));

  // Now the scores of document 1 have been implicitly initialized. The scores
  // should all be 0.
  UsageStore::UsageScores expected_scores;
  expected_scores.usage_type1_last_used_timestamp_s = 0;
  expected_scores.usage_type2_last_used_timestamp_s = 0;
  expected_scores.usage_type3_last_used_timestamp_s = 0;
  expected_scores.usage_type1_count = 0;
  expected_scores.usage_type2_count = 0;
  expected_scores.usage_type3_count = 0;
  EXPECT_THAT(usage_store->GetUsageScores(/*document_id=*/1),
              IsOkAndHolds(expected_scores));
}

TEST_F(UsageStoreTest, DeleteUsageScores) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<UsageStore> usage_store,
                             UsageStore::Create(&filesystem_, test_dir_));

  // Create usage scores with some random numbers.
  UsageStore::UsageScores scores;
  scores.usage_type1_last_used_timestamp_s = 7;
  scores.usage_type2_last_used_timestamp_s = 9;
  scores.usage_type3_last_used_timestamp_s = 11;
  scores.usage_type1_count = 3;
  scores.usage_type2_count = 4;
  scores.usage_type3_count = 9;
  ICING_EXPECT_OK(usage_store->SetUsageScores(/*document_id=*/1, scores));

  // Delete the usage scores of document 1, all the scores of document 1 should
  // be 0.
  UsageStore::UsageScores expected_scores;
  expected_scores.usage_type1_last_used_timestamp_s = 0;
  expected_scores.usage_type2_last_used_timestamp_s = 0;
  expected_scores.usage_type3_last_used_timestamp_s = 0;
  expected_scores.usage_type1_count = 0;
  expected_scores.usage_type2_count = 0;
  expected_scores.usage_type3_count = 0;
  ICING_EXPECT_OK(usage_store->DeleteUsageScores(/*document_id=*/1));
  EXPECT_THAT(usage_store->GetUsageScores(/*document_id=*/1),
              IsOkAndHolds(expected_scores));
}

TEST_F(UsageStoreTest, PersistToDisk) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<UsageStore> usage_store,
                             UsageStore::Create(&filesystem_, test_dir_));

  // Create usage scores with some random numbers.
  UsageStore::UsageScores scores;
  scores.usage_type1_last_used_timestamp_s = 7;
  scores.usage_type2_last_used_timestamp_s = 9;
  scores.usage_type3_last_used_timestamp_s = 11;
  scores.usage_type1_count = 3;
  scores.usage_type2_count = 4;
  scores.usage_type3_count = 9;
  ICING_EXPECT_OK(usage_store->SetUsageScores(/*document_id=*/1, scores));

  EXPECT_THAT(usage_store->PersistToDisk(), IsOk());
}

TEST_F(UsageStoreTest, Reset) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<UsageStore> usage_store,
                             UsageStore::Create(&filesystem_, test_dir_));

  // Create usage scores with some random numbers.
  UsageStore::UsageScores scores;
  scores.usage_type1_last_used_timestamp_s = 7;
  scores.usage_type2_last_used_timestamp_s = 9;
  scores.usage_type3_last_used_timestamp_s = 11;
  scores.usage_type1_count = 3;
  scores.usage_type2_count = 4;
  scores.usage_type3_count = 9;

  // Set scores for document 1 and document 2.
  ICING_EXPECT_OK(usage_store->SetUsageScores(/*document_id=*/1, scores));
  ICING_EXPECT_OK(usage_store->SetUsageScores(/*document_id=*/2, scores));

  EXPECT_THAT(usage_store->Reset(), IsOk());

  // After resetting, all the scores are cleared.
  UsageStore::UsageScores expected_scores;
  expected_scores.usage_type1_last_used_timestamp_s = 0;
  expected_scores.usage_type2_last_used_timestamp_s = 0;
  expected_scores.usage_type3_last_used_timestamp_s = 0;
  expected_scores.usage_type1_count = 0;
  expected_scores.usage_type2_count = 0;
  expected_scores.usage_type3_count = 0;
  EXPECT_THAT(usage_store->GetUsageScores(/*document_id=*/1),
              IsOkAndHolds(expected_scores));
  EXPECT_THAT(usage_store->GetUsageScores(/*document_id=*/2),
              IsOkAndHolds(expected_scores));
}

TEST_F(UsageStoreTest, TimestampInSecondsShouldNotOverflow) {
  // Create a report with the max value of timestamps.
  UsageReport usage_report = CreateUsageReport(
      "namespace", "uri", /*timestamp_ms=*/std::numeric_limits<int64>::max(),
      UsageReport::USAGE_TYPE1);

  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<UsageStore> usage_store,
                             UsageStore::Create(&filesystem_, test_dir_));

  // The stored timestamp in seconds should be the max value of uint32.
  usage_store->AddUsageReport(usage_report, /*document_id=*/1);
  UsageStore::UsageScores expected_scores;
  expected_scores.usage_type1_last_used_timestamp_s =
      std::numeric_limits<uint32_t>::max();
  expected_scores.usage_type1_count = 1;
  expected_scores.usage_type2_count = 0;
  expected_scores.usage_type3_count = 0;
  EXPECT_THAT(usage_store->GetUsageScores(/*document_id=*/1),
              IsOkAndHolds(expected_scores));
}

TEST_F(UsageStoreTest, CountsShouldNotOverflow) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<UsageStore> usage_store,
                             UsageStore::Create(&filesystem_, test_dir_));

  // Create usage scores with the max value of int.
  UsageStore::UsageScores scores;
  scores.usage_type1_last_used_timestamp_s = 0;
  scores.usage_type2_last_used_timestamp_s = 0;
  scores.usage_type3_last_used_timestamp_s = 0;
  scores.usage_type1_count = std::numeric_limits<int>::max();
  scores.usage_type2_count = 0;
  scores.usage_type3_count = 0;

  ICING_ASSERT_OK(usage_store->SetUsageScores(/*document_id=*/1, scores));
  ASSERT_THAT(usage_store->GetUsageScores(/*document_id=*/1),
              IsOkAndHolds(scores));

  // Report another usage with type 1.
  UsageReport usage_report = CreateUsageReport(
      "namespace", "uri", /*timestamp_ms=*/0, UsageReport::USAGE_TYPE1);
  usage_store->AddUsageReport(usage_report, /*document_id=*/1);

  // usage_type1_count should not change because it's already the max value.
  EXPECT_THAT(usage_store->GetUsageScores(/*document_id=*/1),
              IsOkAndHolds(scores));
}

}  // namespace

}  // namespace lib
}  // namespace icing
