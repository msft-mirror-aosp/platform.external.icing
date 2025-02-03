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

#include "icing/legacy/index/icing-flash-bitmap.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/crc32.h"

namespace icing {
namespace lib {

namespace {

using testing::Eq;

class IcingFlashBitmapTest : public ::testing::Test {
 protected:
  void SetUp() override {
    bitmap_files_dir_ = GetTestTempDir() + "/array_files";
    filesystem_.CreateDirectoryRecursively(bitmap_files_dir_.c_str());
    bitmap_file_ = bitmap_files_dir_ + "/array_file";
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(bitmap_files_dir_.c_str());
  }

  IcingFilesystem filesystem_;
  std::string bitmap_files_dir_;
  std::string bitmap_file_;
};

TEST_F(IcingFlashBitmapTest, UpdateCrc) {
  IcingFlashBitmap bitmap(bitmap_file_, &filesystem_);
  ASSERT_TRUE(bitmap.Init());

  // Initial Crc should be 0.
  EXPECT_THAT(bitmap.GetCrc(), Eq(Crc32()));
  EXPECT_THAT(bitmap.UpdateCrc(), Eq(Crc32()));
  EXPECT_THAT(bitmap.GetCrc(), Eq(Crc32()));

  EXPECT_TRUE(bitmap.SetBit(0, true));

  EXPECT_THAT(bitmap.GetCrc(), Eq(Crc32(1180633950)));
  EXPECT_THAT(bitmap.UpdateCrc(), Eq(Crc32(1180633950)));
  EXPECT_THAT(bitmap.GetCrc(), Eq(Crc32(1180633950)));
}

TEST_F(IcingFlashBitmapTest, GetCrcDoesNotUpdateHeader) {
  IcingFlashBitmap bitmap_one(bitmap_file_, &filesystem_);
  ASSERT_TRUE(bitmap_one.Init());

  // Initial Crc should be 0.
  EXPECT_THAT(bitmap_one.GetCrc(), Eq(Crc32()));
  EXPECT_THAT(bitmap_one.UpdateCrc(), Eq(Crc32()));
  EXPECT_THAT(bitmap_one.GetCrc(), Eq(Crc32()));

  EXPECT_TRUE(bitmap_one.SetBit(0, true));

  EXPECT_THAT(bitmap_one.GetCrc(), Eq(Crc32(1180633950)));

  // Create a second bitmap for the same file. This should see the changes made
  // above, but they won't match the checksum (which should still be 0). This
  // should cause init to fail.
  IcingFlashBitmap bitmap_two(bitmap_file_, &filesystem_);
  EXPECT_TRUE(bitmap_two.Init());
  EXPECT_FALSE(bitmap_two.Verify());
}

TEST_F(IcingFlashBitmapTest, UpdateCrcDoesUpdateHeader) {
  IcingFlashBitmap bitmap_one(bitmap_file_, &filesystem_);
  ASSERT_TRUE(bitmap_one.Init());

  // Initial Crc should be 0.
  EXPECT_THAT(bitmap_one.GetCrc(), Eq(Crc32()));
  EXPECT_THAT(bitmap_one.UpdateCrc(), Eq(Crc32()));
  EXPECT_THAT(bitmap_one.GetCrc(), Eq(Crc32()));

  EXPECT_TRUE(bitmap_one.SetBit(0, true));

  EXPECT_THAT(bitmap_one.GetCrc(), Eq(Crc32(1180633950)));
  EXPECT_THAT(bitmap_one.UpdateCrc(), Eq(Crc32(1180633950)));
  EXPECT_THAT(bitmap_one.GetCrc(), Eq(Crc32(1180633950)));

  // Create a second bitmap for the same file. This should see the changes made
  // above, but they won't match the checksum (which should still be 0). This
  // should cause init to fail.
  IcingFlashBitmap bitmap_two(bitmap_file_, &filesystem_);
  EXPECT_TRUE(bitmap_two.Init());
  EXPECT_TRUE(bitmap_two.Verify());
  EXPECT_THAT(bitmap_one.GetCrc(), Eq(Crc32(1180633950)));
}

}  // namespace

}  // namespace lib
}  // namespace icing