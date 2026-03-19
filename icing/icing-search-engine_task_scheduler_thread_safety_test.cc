// Copyright (C) 2025 Google LLC
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

#include <chrono>  // NOLINT
#include <cstdint>
#include <memory>
#include <string>
#include <thread>  // NOLINT

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/icing-search-engine.h"
#include "icing/portable/equals-proto.h"
#include "icing/portable/platform.h"
#include "icing/proto/debug.pb.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/document_wrapper.pb.h"
#include "icing/proto/initialize.pb.h"
#include "icing/proto/logging.pb.h"
#include "icing/proto/optimize.pb.h"
#include "icing/proto/persist.pb.h"
#include "icing/proto/reset.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/status.pb.h"
#include "icing/proto/storage.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/proto/usage.pb.h"
#include "icing/schema-builder.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/jni-test-helpers.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/clock.h"
#include "icing/util/icu-data-file-helper.h"

namespace icing {
namespace lib {

namespace {

using ::testing::Eq;

static constexpr int kNumIterations = 10;

std::string GetTestBaseDir() { return GetTestTempDir() + "/icing"; }

// Thread safety test for IcingSearchEngine's task scheduler.
//
// WARNING: if this test timeouts, then it's likely there is a deadlock between
//   IcingSearchEngine and the scheduled background task. Please revisit all
//   logics of task scheduling and task implementation to fix the issue.
class IcingSearchEngineTaskSchedulerThreadSafetyTest : public testing::Test {
 protected:
  void SetUp() override {
    if (!IsCfStringTokenization() && !IsReverseJniTokenization()) {
      // If we've specified using the reverse-JNI method for segmentation (i.e.
      // not ICU), then we won't have the ICU data file included to set up.
      // Technically, we could choose to use reverse-JNI for segmentation AND
      // include an ICU data file, but that seems unlikely and our current BUILD
      // setup doesn't do this.
      // File generated via icu_data_file rule in //icing/BUILD.
      std::string icu_data_file_path =
          GetTestFilePath("icing/icu.dat");
      ICING_ASSERT_OK(
          icu_data_file_helper::SetUpIcuDataFile(icu_data_file_path));
    }
    filesystem_.CreateDirectoryRecursively(GetTestBaseDir().c_str());
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(GetTestBaseDir().c_str());
  }

  Filesystem filesystem_;
  Clock clock_;
};

IcingSearchEngineOptions GetDefaultIcingOptions() {
  IcingSearchEngineOptions icing_options;
  icing_options.set_base_dir(GetTestBaseDir());
  icing_options.set_enable_repeated_field_joins(true);
  icing_options.set_enable_soft_index_restoration(true);
  icing_options.set_enable_qualified_id_join_index_v3(true);
  icing_options.set_enable_delete_propagation_from(true);
  icing_options.set_enable_background_task_scheduler(true);
  icing_options.set_expired_document_purge_threshold_ms(0);
  return icing_options;
}

TEST_F(IcingSearchEngineTaskSchedulerThreadSafetyTest,
       Destructor_TaskSchedulerShouldNotCauseDeadlockOrCrash) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  for (int i = 0; i < kNumIterations; ++i) {
    // Initialize Icing and set schema.
    auto icing = std::make_unique<IcingSearchEngine>(GetDefaultIcingOptions(),
                                                     GetTestJniCache());
    InitializeResultProto initialize_result_proto = icing->Initialize();

    // There should be no recovery.
    ASSERT_THAT(initialize_result_proto.status(), ProtoIsOk());
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().document_store_data_status(),
        Eq(InitializeStatsProto::NO_DATA_LOSS));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .schema_store_recovery_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .document_store_recovery_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .integer_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .embedding_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));

    ASSERT_THAT(icing->SetSchema(schema).status(), ProtoIsOk());

    // Generate 5 person documents that expire in 2 seconds.
    int64_t current_time_ms = clock_.GetSystemTimeMilliseconds();
    for (int j = 0; j < 5; ++j) {
      DocumentProto person =
          DocumentBuilder()
              .SetKey("namespace", "person/" + std::to_string(j))
              .SetSchema("Person")
              .SetCreationTimestampMs(current_time_ms)
              .SetTtlMs(2000)
              .AddStringProperty("name", "person name")
              .Build();
      ASSERT_THAT(icing->Put(person).status(), ProtoIsOk());
    }

    int64_t expected_first_expiration_time_ms = current_time_ms + 2000;
    // Sleep until the expiration time.
    std::this_thread::sleep_for(
        std::chrono::milliseconds(expected_first_expiration_time_ms -
                                  clock_.GetSystemTimeMilliseconds()));

    // Destroy Icing at the same time when HandleExpiredDocuments task is about
    // to fire. Destructor should join the task scheduler thread and destruct
    // the task scheduler.
    // - There should be no crash or deadlock.
    // - PersistToDisk should be called after terminating/wating the last task
    //   if present. The next initialization should not have data recovery.
    icing.reset();
  }
}

TEST_F(IcingSearchEngineTaskSchedulerThreadSafetyTest,
       ClearAndDestroy_TaskSchedulerShouldNotCauseDeadlockOrCrash) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  for (int i = 0; i < kNumIterations; ++i) {
    // Initialize Icing and set schema.
    auto icing = std::make_unique<IcingSearchEngine>(GetDefaultIcingOptions(),
                                                     GetTestJniCache());
    ASSERT_THAT(icing->Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing->SetSchema(schema).status(), ProtoIsOk());

    // Generate 5 person documents that expire in 2 seconds.
    int64_t current_time_ms = clock_.GetSystemTimeMilliseconds();
    for (int j = 0; j < 5; ++j) {
      DocumentProto person =
          DocumentBuilder()
              .SetKey("namespace", "person/" + std::to_string(j))
              .SetSchema("Person")
              .SetCreationTimestampMs(current_time_ms)
              .SetTtlMs(2000)
              .AddStringProperty("name", "person name")
              .Build();
      ASSERT_THAT(icing->Put(person).status(), ProtoIsOk());
    }

    int64_t expected_first_expiration_time_ms = current_time_ms + 2000;
    // Sleep until the expiration time.
    std::this_thread::sleep_for(
        std::chrono::milliseconds(expected_first_expiration_time_ms -
                                  clock_.GetSystemTimeMilliseconds()));

    // Call ClearAndDestroy() at the same time when HandleExpiredDocuments task.
    // is about to fire.
    // - It should either cancel or wait for the ongoing task to finish; and
    //   destruct the task scheduler before erasing the database.
    // - There should be no crash or deadlock.
    ResetResultProto clear_and_destroy_result = icing->ClearAndDestroy();
    EXPECT_THAT(clear_and_destroy_result.status(), ProtoIsOk());
  }
}

TEST_F(IcingSearchEngineTaskSchedulerThreadSafetyTest,
       Reset_TaskSchedulerShouldNotCauseDeadlockOrCrash) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  for (int i = 0; i < kNumIterations; ++i) {
    // Initialize Icing and set schema.
    auto icing = std::make_unique<IcingSearchEngine>(GetDefaultIcingOptions(),
                                                     GetTestJniCache());
    ASSERT_THAT(icing->Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing->SetSchema(schema).status(), ProtoIsOk());

    // Generate 5 person documents that expire in 2 seconds.
    int64_t current_time_ms = clock_.GetSystemTimeMilliseconds();
    for (int j = 0; j < 5; ++j) {
      DocumentProto person =
          DocumentBuilder()
              .SetKey("namespace", "person/" + std::to_string(j))
              .SetSchema("Person")
              .SetCreationTimestampMs(current_time_ms)
              .SetTtlMs(2000)
              .AddStringProperty("name", "person name")
              .Build();
      ASSERT_THAT(icing->Put(person).status(), ProtoIsOk());
    }

    int64_t expected_first_expiration_time_ms = current_time_ms + 2000;
    // Sleep until the expiration time.
    std::this_thread::sleep_for(
        std::chrono::milliseconds(expected_first_expiration_time_ms -
                                  clock_.GetSystemTimeMilliseconds()));

    // Call Reset() at the same time when HandleExpiredDocuments task is about
    // to fire. There should be no crash or deadlock.
    ResetResultProto reset_result = icing->Reset();
    EXPECT_THAT(reset_result.status(), ProtoIsOk());
  }
}

}  // namespace

}  // namespace lib
}  // namespace icing
