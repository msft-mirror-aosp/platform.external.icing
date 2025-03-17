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

#include "icing/file/marker-file.h"

#include <memory>
#include <string>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/file/file-backed-proto.h"
#include "icing/file/filesystem.h"
#include "icing/file/mock-filesystem.h"
#include "icing/portable/equals-proto.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsFalse;
using ::testing::IsNull;
using ::testing::IsTrue;
using ::testing::Matcher;
using ::testing::Pointee;
using ::testing::Return;

class MarkerFileTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = GetTestTempDir() + "/icing_marker_file_test";
    ASSERT_THAT(filesystem_.CreateDirectoryRecursively(test_dir_.c_str()),
                IsTrue());
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  Filesystem filesystem_;
  std::string test_dir_;
};

TEST_F(MarkerFileTest, CreateAndDestruct) {
  std::string file_path = test_dir_ + "/marker_file";
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<MarkerFile> marker_file,
        MarkerFile::Create(
            &filesystem_, file_path,
            IcingSearchEngineMarkerProto::OperationType::SET_SCHEMA));

    EXPECT_THAT(filesystem_.FileExists(file_path.c_str()), IsTrue());

    {
      // Use another FileBackedProto object to examine the content.
      FileBackedProto<IcingSearchEngineMarkerProto> file_backed_proto(
          filesystem_, file_path);
      ICING_ASSERT_OK_AND_ASSIGN(
          const IcingSearchEngineMarkerProto* marker_proto,
          file_backed_proto.Read());
      EXPECT_THAT(marker_proto->operation_type(),
                  IcingSearchEngineMarkerProto::OperationType::SET_SCHEMA);
    }

    EXPECT_THAT(filesystem_.FileExists(file_path.c_str()), IsTrue());
  }

  // After leaving the scope, the marker file object is destructed, and the
  // underlying file should be deleted.
  EXPECT_THAT(filesystem_.FileExists(file_path.c_str()), IsFalse());
}

TEST_F(MarkerFileTest, CreateWithUnknownOperationTypeShouldFail) {
  std::string file_path = test_dir_ + "/marker_file";

  EXPECT_THAT(
      MarkerFile::Create(&filesystem_, file_path,
                         IcingSearchEngineMarkerProto::OperationType::UNKNOWN),
      StatusIs(
          libtextclassifier3::StatusCode::INVALID_ARGUMENT,
          HasSubstr("Cannot create marker file with UNKNOWN operation type")));
  // The marker file should not be created.
  EXPECT_THAT(filesystem_.FileExists(file_path.c_str()), IsFalse());
}

TEST_F(MarkerFileTest, CreateWithExistingFileShouldFail) {
  std::string file_path = test_dir_ + "/marker_file";

  {
    // Create the file.
    ScopedFd sfd(filesystem_.OpenForWrite(file_path.c_str()));
    ASSERT_THAT(sfd.is_valid(), IsTrue());
    ASSERT_THAT(filesystem_.Write(sfd.get(), "test", /*data_size=*/4),
                IsTrue());
  }
  ASSERT_THAT(filesystem_.FileExists(file_path.c_str()), IsTrue());
  ASSERT_THAT(filesystem_.GetFileSize(file_path.c_str()), Eq(4));

  EXPECT_THAT(MarkerFile::Create(
                  &filesystem_, file_path,
                  IcingSearchEngineMarkerProto::OperationType::SET_SCHEMA),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION,
                       HasSubstr("Marker file already exists")));
  // The file should still exist and remain unchanged.
  EXPECT_THAT(filesystem_.FileExists(file_path.c_str()), IsTrue());
  EXPECT_THAT(filesystem_.GetFileSize(file_path.c_str()), Eq(4));
  char buf[4];
  EXPECT_THAT(filesystem_.Read(file_path.c_str(), buf, /*buf_size=*/4),
              IsTrue());
  EXPECT_THAT(std::string(buf, 4), Eq("test"));
}

TEST_F(MarkerFileTest, WriteFailureShouldDeleteTheFile) {
  auto mock_filesystem = std::make_unique<MockFilesystem>();
  std::string file_path = test_dir_ + "/marker_file";

  // Mock Write to fail.
  ON_CALL(*mock_filesystem, Write(Matcher<int>(_), _, _))
      .WillByDefault(Return(false));

  EXPECT_THAT(MarkerFile::Create(
                  mock_filesystem.get(), file_path,
                  IcingSearchEngineMarkerProto::OperationType::SET_SCHEMA),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL,
                       HasSubstr("Failed to write")));
  // The marker file should be deleted after the write failure.
  EXPECT_THAT(filesystem_.FileExists(file_path.c_str()), IsFalse());
}

TEST_F(MarkerFileTest, PostmortemExistingMarkerFile) {
  std::string file_path = test_dir_ + "/marker_file";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<MarkerFile> marker_file,
      MarkerFile::Create(
          &filesystem_, file_path,
          IcingSearchEngineMarkerProto::OperationType::SET_SCHEMA));

  EXPECT_THAT(filesystem_.FileExists(file_path.c_str()), IsTrue());

  IcingSearchEngineMarkerProto expected_proto;
  expected_proto.set_operation_type(
      IcingSearchEngineMarkerProto::OperationType::SET_SCHEMA);
  EXPECT_THAT(MarkerFile::Postmortem(filesystem_, file_path),
              Pointee(EqualsProto(expected_proto)));
}

TEST_F(MarkerFileTest, PostmortemReadErrorShouldReturnDefaultMarkerProto) {
  std::string file_path = test_dir_ + "/marker_file";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<MarkerFile> marker_file,
      MarkerFile::Create(
          &filesystem_, file_path,
          IcingSearchEngineMarkerProto::OperationType::SET_SCHEMA));

  EXPECT_THAT(filesystem_.FileExists(file_path.c_str()), IsTrue());

  // Mock Read to fail.
  auto mock_filesystem = std::make_unique<MockFilesystem>();
  ON_CALL(*mock_filesystem, OpenForRead(Eq(file_path)))
      .WillByDefault(Return(-1));
  EXPECT_THAT(
      MarkerFile::Postmortem(*mock_filesystem, file_path),
      Pointee(EqualsProto(IcingSearchEngineMarkerProto::default_instance())));
}

TEST_F(MarkerFileTest, PostmortemNonExistingMarkerFileShouldReturnNullptr) {
  std::string file_path = test_dir_ + "/marker_file";
  ASSERT_THAT(filesystem_.FileExists(file_path.c_str()), IsFalse());

  EXPECT_THAT(MarkerFile::Postmortem(filesystem_, file_path), IsNull());
}

}  // namespace

}  // namespace lib
}  // namespace icing
