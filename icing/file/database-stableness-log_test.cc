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

#include "icing/file/database-stableness-log.h"

#include <cstdint>
#include <memory>
#include <string>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/file/filesystem.h"
#include "icing/portable/equals-proto.h"
#include "icing/proto/logging.pb.h"
#include "icing/proto/persist.pb.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::HasSubstr;
using ::testing::IsFalse;
using ::testing::IsTrue;

class DatabaseStablenessLogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = GetTestTempDir() + "/icing_database_stableness_log_test";
    file_path_ = test_dir_ + "/database_stableness_log";

    ASSERT_THAT(filesystem_.CreateDirectoryRecursively(test_dir_.c_str()),
                IsTrue());
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  libtextclassifier3::StatusOr<IcingDatabaseStablenessProto>
  ReadProtoFromFile() {
    ScopedFd sfd(filesystem_.OpenForRead(file_path_.c_str()));
    if (!sfd.is_valid()) {
      return absl_ports::InternalError("Failed to open file for read.");
    }

    int64_t file_size = filesystem_.GetFileSize(sfd.get());
    if (file_size == Filesystem::kBadFileSize || file_size <= 0) {
      return absl_ports::InternalError("Failed to get file size.");
    }

    auto buffer = std::make_unique<uint8_t[]>(file_size);
    if (filesystem_.PRead(sfd.get(), buffer.get(), file_size, /*offset=*/0) !=
        file_size) {
      return absl_ports::InternalError("Failed to read proto from file.");
    }

    IcingDatabaseStablenessProto proto;
    if (!proto.ParseFromArray(buffer.get(), static_cast<int>(file_size))) {
      return absl_ports::InternalError("Failed to parse proto");
    }
    return proto;
  }

  Filesystem filesystem_;
  std::string test_dir_;
  std::string file_path_;
};

TEST_F(DatabaseStablenessLogTest, CreateNew) {
  ASSERT_THAT(filesystem_.FileExists(file_path_.c_str()), IsFalse());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DatabaseStablenessLog> database_stableness_log,
      DatabaseStablenessLog::Create(&filesystem_, file_path_));
  EXPECT_THAT(filesystem_.FileExists(file_path_.c_str()), IsTrue());
}

TEST_F(DatabaseStablenessLogTest, UpdateApiHistory) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DatabaseStablenessLog> database_stableness_log,
      DatabaseStablenessLog::Create(&filesystem_, file_path_));

  // Update history for some APIs.
  EXPECT_THAT(database_stableness_log->UpdateApiHistory(
                  IcingApiCallType::INITIALIZE, /*timestamp_ms=*/100),
              IsOk());
  EXPECT_THAT(database_stableness_log->UpdateApiHistory(
                  IcingApiCallType::SET_SCHEMA, /*timestamp_ms=*/200),
              IsOk());
  EXPECT_THAT(database_stableness_log->UpdateApiHistory(
                  IcingApiCallType::BATCH_PUT, /*timestamp_ms=*/300),
              IsOk());

  IcingDatabaseStablenessProto expected_proto;
  ApiHistoryProto* api_history0 = expected_proto.add_api_history();
  api_history0->set_call_type(IcingApiCallType::INITIALIZE);
  api_history0->set_last_call_timestamp_ms(100);
  ApiHistoryProto* api_history1 = expected_proto.add_api_history();
  api_history1->set_call_type(IcingApiCallType::SET_SCHEMA);
  api_history1->set_last_call_timestamp_ms(200);
  ApiHistoryProto* api_history2 = expected_proto.add_api_history();
  api_history2->set_call_type(IcingApiCallType::BATCH_PUT);
  api_history2->set_last_call_timestamp_ms(300);

  EXPECT_THAT(database_stableness_log->GetCachedProto(),
              EqualsProto(expected_proto));
  // Manually read and deserialize the proto from disk. This ensures
  // UpdateApiHistory also writes the proto into the disk.
  EXPECT_THAT(ReadProtoFromFile(), IsOkAndHolds(EqualsProto(expected_proto)));
}

TEST_F(DatabaseStablenessLogTest,
       UpdateApiHistory_shouldOverwriteExistingCallType) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DatabaseStablenessLog> database_stableness_log,
      DatabaseStablenessLog::Create(&filesystem_, file_path_));

  // Update history for some APIs.
  EXPECT_THAT(database_stableness_log->UpdateApiHistory(
                  IcingApiCallType::INITIALIZE, /*timestamp_ms=*/100),
              IsOk());
  EXPECT_THAT(database_stableness_log->UpdateApiHistory(
                  IcingApiCallType::SET_SCHEMA, /*timestamp_ms=*/200),
              IsOk());
  EXPECT_THAT(database_stableness_log->UpdateApiHistory(
                  IcingApiCallType::BATCH_PUT, /*timestamp_ms=*/300),
              IsOk());
  EXPECT_THAT(database_stableness_log->UpdateApiHistory(
                  IcingApiCallType::BATCH_PUT, /*timestamp_ms=*/400),
              IsOk());
  EXPECT_THAT(database_stableness_log->UpdateApiHistory(
                  IcingApiCallType::SET_SCHEMA, /*timestamp_ms=*/500),
              IsOk());
  EXPECT_THAT(database_stableness_log->UpdateApiHistory(
                  IcingApiCallType::BATCH_PUT, /*timestamp_ms=*/600),
              IsOk());

  IcingDatabaseStablenessProto expected_proto;
  ApiHistoryProto* api_history0 = expected_proto.add_api_history();
  api_history0->set_call_type(IcingApiCallType::INITIALIZE);
  api_history0->set_last_call_timestamp_ms(100);
  ApiHistoryProto* api_history1 = expected_proto.add_api_history();
  api_history1->set_call_type(IcingApiCallType::SET_SCHEMA);
  api_history1->set_last_call_timestamp_ms(500);
  ApiHistoryProto* api_history2 = expected_proto.add_api_history();
  api_history2->set_call_type(IcingApiCallType::BATCH_PUT);
  api_history2->set_last_call_timestamp_ms(600);

  EXPECT_THAT(database_stableness_log->GetCachedProto(),
              EqualsProto(expected_proto));
  // Manually read and deserialize the proto from disk. This ensures
  // UpdateApiHistory also writes the proto into the disk.
  EXPECT_THAT(ReadProtoFromFile(), IsOkAndHolds(EqualsProto(expected_proto)));
}

TEST_F(DatabaseStablenessLogTest, UpdateApiHistory_invalidCallType) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DatabaseStablenessLog> database_stableness_log,
      DatabaseStablenessLog::Create(&filesystem_, file_path_));

  EXPECT_THAT(database_stableness_log->UpdateApiHistory(
                  IcingApiCallType::UNKNOWN, /*timestamp_ms=*/100),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("Invalid API call type")));
  EXPECT_THAT(
      database_stableness_log->UpdateApiHistory(
          IcingApiCallType::PERSIST_TO_DISK, /*timestamp_ms=*/200),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
               HasSubstr("Should call UpdatePersistToDiskHistory instead")));
}

TEST_F(DatabaseStablenessLogTest, UpdatePersistToDiskHistory) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DatabaseStablenessLog> database_stableness_log,
      DatabaseStablenessLog::Create(&filesystem_, file_path_));

  // FULL
  EXPECT_THAT(database_stableness_log->UpdatePersistToDiskHistory(
                  PersistType::FULL, /*timestamp_ms=*/100),
              IsOk());
  IcingDatabaseStablenessProto expected_proto;
  expected_proto.set_last_flush_full_timestamp_ms(100);
  EXPECT_THAT(database_stableness_log->GetCachedProto(),
              EqualsProto(expected_proto));
  EXPECT_THAT(ReadProtoFromFile(), IsOkAndHolds(EqualsProto(expected_proto)));

  // LITE
  EXPECT_THAT(database_stableness_log->UpdatePersistToDiskHistory(
                  PersistType::LITE, /*timestamp_ms=*/200),
              IsOk());
  expected_proto.set_last_flush_lite_timestamp_ms(200);
  EXPECT_THAT(database_stableness_log->GetCachedProto(),
              EqualsProto(expected_proto));
  EXPECT_THAT(ReadProtoFromFile(), IsOkAndHolds(EqualsProto(expected_proto)));

  // RECOVERY_PROOF
  EXPECT_THAT(database_stableness_log->UpdatePersistToDiskHistory(
                  PersistType::RECOVERY_PROOF, /*timestamp_ms=*/300),
              IsOk());
  expected_proto.set_last_flush_recovery_proof_timestamp_ms(300);
  EXPECT_THAT(database_stableness_log->GetCachedProto(),
              EqualsProto(expected_proto));
  EXPECT_THAT(ReadProtoFromFile(), IsOkAndHolds(EqualsProto(expected_proto)));

  // SHUTDOWN
  EXPECT_THAT(database_stableness_log->UpdatePersistToDiskHistory(
                  PersistType::SHUTDOWN, /*timestamp_ms=*/400),
              IsOk());
  expected_proto.set_last_flush_shutdown_timestamp_ms(400);
  EXPECT_THAT(database_stableness_log->GetCachedProto(),
              EqualsProto(expected_proto));
  EXPECT_THAT(ReadProtoFromFile(), IsOkAndHolds(EqualsProto(expected_proto)));

  // DESTRUCTOR
  EXPECT_THAT(database_stableness_log->UpdatePersistToDiskHistory(
                  PersistType::DESTRUCTOR, /*timestamp_ms=*/500),
              IsOk());
  expected_proto.set_last_flush_destructor_timestamp_ms(500);
  EXPECT_THAT(database_stableness_log->GetCachedProto(),
              EqualsProto(expected_proto));
  EXPECT_THAT(ReadProtoFromFile(), IsOkAndHolds(EqualsProto(expected_proto)));

  // Update RECOVERY_PROOF again. It should overwrite the existing timestamp of
  // RECOVERY_PROOF.
  EXPECT_THAT(database_stableness_log->UpdatePersistToDiskHistory(
                  PersistType::RECOVERY_PROOF, /*timestamp_ms=*/1000),
              IsOk());
  expected_proto.set_last_flush_recovery_proof_timestamp_ms(1000);
  EXPECT_THAT(database_stableness_log->GetCachedProto(),
              EqualsProto(expected_proto));
  EXPECT_THAT(ReadProtoFromFile(), IsOkAndHolds(EqualsProto(expected_proto)));
}

TEST_F(DatabaseStablenessLogTest,
       UpdatePersistToDiskHistory_unknownPersistTypeShouldReturnError) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DatabaseStablenessLog> database_stableness_log,
      DatabaseStablenessLog::Create(&filesystem_, file_path_));

  EXPECT_THAT(database_stableness_log->UpdatePersistToDiskHistory(
                  PersistType::UNKNOWN, /*timestamp_ms=*/100),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("Invalid persist type")));
}

TEST_F(DatabaseStablenessLogTest, UpdateAll) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DatabaseStablenessLog> database_stableness_log,
      DatabaseStablenessLog::Create(&filesystem_, file_path_));

  // Update history for some APIs.
  EXPECT_THAT(database_stableness_log->UpdateApiHistory(
                  IcingApiCallType::INITIALIZE, /*timestamp_ms=*/100),
              IsOk());
  EXPECT_THAT(database_stableness_log->UpdateApiHistory(
                  IcingApiCallType::SET_SCHEMA, /*timestamp_ms=*/200),
              IsOk());
  EXPECT_THAT(database_stableness_log->UpdateApiHistory(
                  IcingApiCallType::BATCH_PUT, /*timestamp_ms=*/300),
              IsOk());
  EXPECT_THAT(database_stableness_log->UpdatePersistToDiskHistory(
                  PersistType::RECOVERY_PROOF, /*timestamp_ms=*/400),
              IsOk());
  EXPECT_THAT(database_stableness_log->UpdateApiHistory(
                  IcingApiCallType::BATCH_PUT, /*timestamp_ms=*/500),
              IsOk());
  EXPECT_THAT(database_stableness_log->UpdateApiHistory(
                  IcingApiCallType::SET_SCHEMA, /*timestamp_ms=*/600),
              IsOk());
  EXPECT_THAT(database_stableness_log->UpdateApiHistory(
                  IcingApiCallType::BATCH_PUT, /*timestamp_ms=*/700),
              IsOk());
  EXPECT_THAT(database_stableness_log->UpdatePersistToDiskHistory(
                  PersistType::FULL, /*timestamp_ms=*/800),
              IsOk());

  IcingDatabaseStablenessProto expected_proto;
  expected_proto.set_last_flush_recovery_proof_timestamp_ms(400);
  expected_proto.set_last_flush_full_timestamp_ms(800);
  ApiHistoryProto* api_history0 = expected_proto.add_api_history();
  api_history0->set_call_type(IcingApiCallType::INITIALIZE);
  api_history0->set_last_call_timestamp_ms(100);
  ApiHistoryProto* api_history1 = expected_proto.add_api_history();
  api_history1->set_call_type(IcingApiCallType::SET_SCHEMA);
  api_history1->set_last_call_timestamp_ms(600);
  ApiHistoryProto* api_history2 = expected_proto.add_api_history();
  api_history2->set_call_type(IcingApiCallType::BATCH_PUT);
  api_history2->set_last_call_timestamp_ms(700);
  EXPECT_THAT(database_stableness_log->GetCachedProto(),
              EqualsProto(expected_proto));
  EXPECT_THAT(ReadProtoFromFile(), IsOkAndHolds(EqualsProto(expected_proto)));
}

}  // namespace

}  // namespace lib
}  // namespace icing
