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

#include "icing/legacy/index/icing-filesystem.h"

#include <cstddef>
#include <cstdio>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/testing/tmp-directory.h"

using ::testing::Eq;
using ::testing::Ne;

namespace icing {
namespace lib {

namespace {

class IcingFilesystemTest : public testing::Test {
 protected:
  void SetUp() override {
    temp_dir_ = GetTestTempDir() + "/icing_filesystem";
    IcingFilesystem filesystem;
    ASSERT_TRUE(filesystem.CreateDirectoryRecursively(temp_dir_.c_str()));
  }

  void TearDown() override {
    IcingFilesystem filesystem;
    EXPECT_TRUE(filesystem.DeleteDirectoryRecursively(temp_dir_.c_str()));
  }

  // Write junk data of given size to the given file descriptor
  void WriteJunk(int fd, size_t size) {
    const int kBufLen = 1024;
    int buf[kBufLen];
    for (int i = 0; i < kBufLen; ++i) {
      buf[i] = i;
    }
    const int kBufSize = kBufLen * sizeof(int);

    IcingFilesystem filesystem;
    for (size_t i = 0; i < size / kBufSize; ++i) {
      EXPECT_TRUE(filesystem.Write(fd, buf, kBufSize));
    }
    if (size % kBufSize) {
      EXPECT_TRUE(filesystem.Write(fd, buf, size % kBufSize));
    }
  }

  std::string temp_dir_;
};

TEST_F(IcingFilesystemTest, FSync) {
  IcingFilesystem filesystem;
  const std::string foo_file = temp_dir_ + "/foo_file";
  int fd = filesystem.OpenForWrite(foo_file.c_str());
  ASSERT_THAT(fd, Ne(-1));
  EXPECT_TRUE(filesystem.DataSync(fd));
  close(fd);
}

TEST_F(IcingFilesystemTest, Truncate) {
  IcingFilesystem filesystem;
  const std::string foo_file = temp_dir_ + "/foo_file";
  const char* filename = foo_file.c_str();
  int fd = filesystem.OpenForWrite(filename);
  ASSERT_THAT(fd, Ne(-1));
  char data[10000] = {0};  // Zero-init to satisfy msan.
  EXPECT_TRUE(filesystem.Write(fd, data, sizeof(data)));
  close(fd);
  EXPECT_THAT(filesystem.GetFileSize(filename), Eq(sizeof(data)));
  EXPECT_TRUE(filesystem.Truncate(filename, sizeof(data) / 2));
  EXPECT_THAT(filesystem.GetFileSize(filename), Eq(sizeof(data) / 2));
  EXPECT_TRUE(filesystem.Truncate(filename, 0));
  EXPECT_THAT(filesystem.GetFileSize(filename), Eq(0u));
}

TEST_F(IcingFilesystemTest, Grow) {
  IcingFilesystem filesystem;
  const std::string foo_file = temp_dir_ + "/foo_file";
  const char* filename = foo_file.c_str();
  int fd = filesystem.OpenForWrite(filename);
  ASSERT_THAT(fd, Ne(-1));
  char data[10000] = {0};  // Zero-init to satisfy msan.
  EXPECT_TRUE(filesystem.Write(fd, data, sizeof(data)));

  EXPECT_THAT(filesystem.GetFileSize(filename), Eq(sizeof(data)));
  EXPECT_TRUE(filesystem.Grow(fd, sizeof(data) * 2));
  EXPECT_THAT(filesystem.GetFileSize(filename), Eq(sizeof(data) * 2));
  close(fd);
}

TEST_F(IcingFilesystemTest, GrowUsingWrite) {
  IcingFilesystem filesystem;
  const std::string foo_file_grow = temp_dir_ + "/foo_file_grow";
  const std::string foo_file_grow_with_write =
      temp_dir_ + "/foo_file_grow_with_write";

  const char* filename = foo_file_grow_with_write.c_str();

  int fd = filesystem.OpenForWrite(filename);
  ASSERT_THAT(fd, Ne(-1));

  char data[10000] = {0};  // Zero-init to satisfy msan.
  EXPECT_TRUE(filesystem.Write(fd, data, sizeof(data)));
  // lseek file pointer to a random place in the file
  off_t position = lseek(fd, 10, SEEK_SET);

  EXPECT_THAT(filesystem.GetFileSize(fd), Eq(sizeof(data)));
  EXPECT_THAT(lseek(fd, 0, SEEK_CUR), Eq(position));

  EXPECT_TRUE(filesystem.GrowUsingPWrite(fd, sizeof(data) * 2));
  EXPECT_THAT(filesystem.GetFileSize(fd), Eq(sizeof(data) * 2));
  // Verify that the file pointer position is unchanged after the grow.
  EXPECT_THAT(lseek(fd, 0, SEEK_CUR), Eq(position));
  close(fd);
}

}  // namespace

}  // namespace lib
}  // namespace icing
