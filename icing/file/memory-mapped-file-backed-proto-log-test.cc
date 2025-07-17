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
// See the License for the specific language governing permissions and
// limitations under the License.

#include "icing/file/memory-mapped-file-backed-proto-log.h"

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/portable/equals-proto.h"
#include "icing/proto/document.pb.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/crc32.h"

namespace icing {
namespace lib {
namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::HasSubstr;
using ::testing::StrEq;

static DocumentProto document1 =
    DocumentBuilder().SetKey("namespace", "uri").Build();
static DocumentProto document2 =
    DocumentBuilder().SetKey("namespace2", "uri2").SetScore(10).Build();
static DocumentProto document3 =
    DocumentBuilder()
        .SetKey("namespace3", "uri3")
        .AddStringProperty("str_property", "a_random_string")
        .Build();

class MemoryMappedFileBackedProtoLogTest : public ::testing::Test {
 protected:
  MemoryMappedFileBackedProtoLogTest() {}

  void SetUp() override {
    base_dir_ = GetTestTempDir() + "/tmp_dir";
    ASSERT_TRUE(filesystem_.CreateDirectoryRecursively(base_dir_.c_str()));
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(base_dir_.c_str());
  }

  Filesystem filesystem_;
  std::string base_dir_;
};

TEST_F(MemoryMappedFileBackedProtoLogTest, WriteAndRead) {
  ICING_ASSERT_OK_AND_ASSIGN(
      auto mmaped_proto_log,
      MemoryMappedFileBackedProtoLog<DocumentProto>::Create(
          filesystem_, base_dir_ + "/proto_log_file"));

  ICING_ASSERT_OK_AND_ASSIGN(int32_t document1_index,
                             mmaped_proto_log->Write(document1));
  ICING_ASSERT_OK_AND_ASSIGN(int32_t document2_index,
                             mmaped_proto_log->Write(document2));
  ICING_ASSERT_OK_AND_ASSIGN(int32_t document3_index,
                             mmaped_proto_log->Write(document3));

  ASSERT_THAT(mmaped_proto_log->Read(document1_index),
              IsOkAndHolds(EqualsProto(document1)));
  ASSERT_THAT(mmaped_proto_log->Read(document2_index),
              IsOkAndHolds(EqualsProto(document2)));
  ASSERT_THAT(mmaped_proto_log->Read(document3_index),
              IsOkAndHolds(EqualsProto(document3)));
  mmaped_proto_log->PersistToDisk();

  // Creates a new instance that loads the existing file.
  ICING_ASSERT_OK_AND_ASSIGN(
      auto new_mmaped_proto_log,
      MemoryMappedFileBackedProtoLog<DocumentProto>::Create(
          filesystem_, base_dir_ + "/proto_log_file"));

  ASSERT_THAT(new_mmaped_proto_log->Read(document1_index),
              IsOkAndHolds(EqualsProto(document1)));
  ASSERT_THAT(new_mmaped_proto_log->Read(document2_index),
              IsOkAndHolds(EqualsProto(document2)));
  ASSERT_THAT(new_mmaped_proto_log->Read(document3_index),
              IsOkAndHolds(EqualsProto(document3)));
}

TEST_F(MemoryMappedFileBackedProtoLogTest, CheckSumTest) {
  ICING_ASSERT_OK_AND_ASSIGN(
      auto mmaped_proto_log,
      MemoryMappedFileBackedProtoLog<DocumentProto>::Create(
          filesystem_, base_dir_ + "/proto_log_file"));
  Crc32 checksum_before_write = mmaped_proto_log->GetChecksum();
  ICING_ASSERT_OK(mmaped_proto_log->Write(document1));
  Crc32 checksum_after_write = mmaped_proto_log->GetChecksum();
  EXPECT_NE(checksum_before_write, checksum_after_write);
  EXPECT_THAT(mmaped_proto_log->UpdateChecksum(),
              IsOkAndHolds(checksum_after_write));
}

TEST_F(MemoryMappedFileBackedProtoLogTest, ProtoSizeTooLarge) {
  ICING_ASSERT_OK_AND_ASSIGN(
      auto mmaped_proto_log,
      MemoryMappedFileBackedProtoLog<DocumentProto>::Create(
          filesystem_, base_dir_ + "/proto_log_file"));

  // Creates a proto that exceeds the max proto size limit, 16Mb.
  std::string long_string(17 * 1024 * 1024, 'a');
  DocumentProto document =
      DocumentBuilder()
          .SetKey("namespace", "uri")
          .AddStringProperty("long_str", std::move(long_string))
          .Build();

  ASSERT_THAT(mmaped_proto_log->Write(document),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("Proto data size must be under 16MiB")));
}

TEST_F(MemoryMappedFileBackedProtoLogTest, Delete) {
  ICING_ASSERT_OK_AND_ASSIGN(
      auto mmaped_proto_log,
      MemoryMappedFileBackedProtoLog<DocumentProto>::Create(
          filesystem_, base_dir_ + "/proto_log_file"));

  std::string file_path = base_dir_ + "/proto_log_file";
  ICING_ASSERT_OK(mmaped_proto_log->Write(document1));
  ICING_ASSERT_OK(mmaped_proto_log->Delete(filesystem_, file_path));
  EXPECT_FALSE(filesystem_.FileExists(file_path.data()));
}

TEST_F(MemoryMappedFileBackedProtoLogTest, ReadInvalidIndex) {
  ICING_ASSERT_OK_AND_ASSIGN(
      auto mmaped_proto_log,
      MemoryMappedFileBackedProtoLog<DocumentProto>::Create(
          filesystem_, base_dir_ + "/proto_log_file"));

  ICING_ASSERT_OK(mmaped_proto_log->Write(document1));
  ASSERT_THAT(mmaped_proto_log->Read(/*index=*/-1),
              StatusIs(libtextclassifier3::StatusCode::OUT_OF_RANGE,
                       StrEq("Index, -1, is less than 0")));
  ASSERT_THAT(
      mmaped_proto_log->Read(/*index=*/16),
      StatusIs(libtextclassifier3::StatusCode::OUT_OF_RANGE,
               StrEq("Index, 16, is greater/equal than the upper bound, 16")));

  ASSERT_THAT(
      mmaped_proto_log->Read(/*index=*/20),
      StatusIs(libtextclassifier3::StatusCode::OUT_OF_RANGE,
               StrEq("Index, 20, is greater/equal than the upper bound, 16")));

  ASSERT_THAT(mmaped_proto_log->Read(/*index=*/15),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       StrEq("Proto metadata has invalid magic number")));
}

TEST_F(MemoryMappedFileBackedProtoLogTest, WriteAndReadWithEmptyProto) {
  ICING_ASSERT_OK_AND_ASSIGN(
      auto mmaped_proto_log,
      MemoryMappedFileBackedProtoLog<DocumentProto>::Create(
          filesystem_, base_dir_ + "/proto_log_file"));

  DocumentProto empty_document;
  ICING_ASSERT_OK_AND_ASSIGN(int32_t index1,
                             mmaped_proto_log->Write(empty_document));
  EXPECT_EQ(index1, 0);

  ICING_ASSERT_OK_AND_ASSIGN(int32_t index2,
                             mmaped_proto_log->Write(document1));
  EXPECT_EQ(index2, 4);

  ICING_ASSERT_OK_AND_ASSIGN(int32_t index3,
                             mmaped_proto_log->Write(empty_document));
  EXPECT_EQ(index3, 24);

  ICING_ASSERT_OK_AND_ASSIGN(int32_t index4,
                             mmaped_proto_log->Write(document2));
  EXPECT_EQ(index4, 28);

  ASSERT_THAT(mmaped_proto_log->Read(index1),
              IsOkAndHolds(EqualsProto(empty_document)));
  ASSERT_THAT(mmaped_proto_log->Read(index2),
              IsOkAndHolds(EqualsProto(document1)));
  ASSERT_THAT(mmaped_proto_log->Read(index3),
              IsOkAndHolds(EqualsProto(empty_document)));
  ASSERT_THAT(mmaped_proto_log->Read(index4),
              IsOkAndHolds(EqualsProto(document2)));
}

}  // namespace
}  // namespace lib
}  // namespace icing
