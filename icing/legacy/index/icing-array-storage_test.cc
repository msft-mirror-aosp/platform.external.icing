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

#include "icing/legacy/index/icing-array-storage.h"

#include <cstdint>
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

class IcingArrayStorageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    array_files_dir_ = GetTestTempDir() + "/array_files";
    filesystem_.CreateDirectoryRecursively(array_files_dir_.c_str());
    array_file_ = array_files_dir_ + "/array_file";
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(array_files_dir_.c_str());
  }

  IcingFilesystem filesystem_;
  std::string array_files_dir_;
  std::string array_file_;
};

TEST_F(IcingArrayStorageTest, UpdateCrcNoCrcPtr) {
  IcingArrayStorage storage(filesystem_);
  IcingScopedFd sfd(filesystem_.OpenForWrite(array_file_.c_str()));
  ASSERT_TRUE(sfd.is_valid());
  ASSERT_TRUE(storage.Init(sfd.get(), /*fd_offset=*/0, /*map_shared=*/true,
                           /*elt_size=*/sizeof(uint32_t), /*num_elts=*/0,
                           /*max_num_elts=*/1000, /*crc_ptr=*/nullptr,
                           /*init_crc=*/false));

  // Initial Crc should be 0.
  EXPECT_THAT(storage.GetCrc(), Eq(Crc32()));
  EXPECT_THAT(storage.UpdateCrc(), Eq(Crc32()));
  EXPECT_THAT(storage.GetCrc(), Eq(Crc32()));

  uint32_t* val = storage.GetMutableMem<uint32_t>(0, 1);
  *val = 5;

  // Because there is no crc_ptr, the crc should remain 0 even though we have
  // added content.
  EXPECT_THAT(storage.GetCrc(), Eq(Crc32()));
  EXPECT_THAT(storage.UpdateCrc(), Eq(Crc32()));
  EXPECT_THAT(storage.GetCrc(), Eq(Crc32()));
}

TEST_F(IcingArrayStorageTest, UpdateCrc) {
  IcingArrayStorage storage(filesystem_);
  uint32_t crc = 0;
  IcingScopedFd sfd(filesystem_.OpenForWrite(array_file_.c_str()));
  ASSERT_TRUE(sfd.is_valid());
  ASSERT_TRUE(storage.Init(sfd.get(), /*fd_offset=*/0, /*map_shared=*/true,
                           /*elt_size=*/sizeof(uint32_t), /*num_elts=*/0,
                           /*max_num_elts=*/1000, &crc, /*init_crc=*/false));

  // Initial Crc should be 0.
  EXPECT_THAT(storage.GetCrc(), Eq(Crc32()));
  EXPECT_THAT(storage.UpdateCrc(), Eq(Crc32()));
  EXPECT_THAT(storage.GetCrc(), Eq(Crc32()));

  uint32_t* val = storage.GetMutableMem<uint32_t>(0, 1);
  *val = 5;

  EXPECT_THAT(storage.GetCrc(), Eq(Crc32(937357362)));
  EXPECT_THAT(storage.UpdateCrc(), Eq(Crc32(937357362)));
  EXPECT_THAT(storage.GetCrc(), Eq(Crc32(937357362)));
}

TEST_F(IcingArrayStorageTest, GetCrcDoesNotUpdateHeader) {
  uint32_t crc = 0;
  IcingScopedFd sfd(filesystem_.OpenForWrite(array_file_.c_str()));
  ASSERT_TRUE(sfd.is_valid());

  {
    IcingArrayStorage storage_one(filesystem_);
    ASSERT_TRUE(
        storage_one.Init(sfd.get(), /*fd_offset=*/0, /*map_shared=*/true,
                         /*elt_size=*/sizeof(uint32_t), /*num_elts=*/0,
                         /*max_num_elts=*/1000, &crc, /*init_crc=*/false));

    // Initial Crc should be 0.
    EXPECT_THAT(storage_one.GetCrc(), Eq(Crc32()));
    EXPECT_THAT(storage_one.UpdateCrc(), Eq(Crc32()));
    EXPECT_THAT(storage_one.GetCrc(), Eq(Crc32()));

    uint32_t* val = storage_one.GetMutableMem<uint32_t>(0, 1);
    *val = 5;

    EXPECT_THAT(storage_one.GetCrc(), Eq(Crc32(937357362)));
    EXPECT_THAT(crc, Eq(0));
  }

  {
    IcingArrayStorage storage_two(filesystem_);
    // Init should fail because the updated checksum was never written to crc.
    ASSERT_FALSE(
        storage_two.Init(sfd.get(), /*fd_offset=*/0, /*map_shared=*/true,
                         /*elt_size=*/sizeof(uint32_t), /*num_elts=*/1,
                         /*max_num_elts=*/1000, &crc, /*init_crc=*/false));
  }
}

TEST_F(IcingArrayStorageTest, UpdateCrcDoesUpdateHeader) {
  uint32_t crc = 0;
  IcingScopedFd sfd(filesystem_.OpenForWrite(array_file_.c_str()));
  ASSERT_TRUE(sfd.is_valid());

  {
    IcingArrayStorage storage_one(filesystem_);
    ASSERT_TRUE(
        storage_one.Init(sfd.get(), /*fd_offset=*/0, /*map_shared=*/true,
                         /*elt_size=*/sizeof(uint32_t), /*num_elts=*/0,
                         /*max_num_elts=*/1000, &crc, /*init_crc=*/false));

    // Initial Crc should be 0.
    EXPECT_THAT(storage_one.GetCrc(), Eq(Crc32()));
    EXPECT_THAT(storage_one.UpdateCrc(), Eq(Crc32()));
    EXPECT_THAT(storage_one.GetCrc(), Eq(Crc32()));

    uint32_t* val = storage_one.GetMutableMem<uint32_t>(0, 1);
    *val = 5;

    EXPECT_THAT(storage_one.GetCrc(), Eq(Crc32(937357362)));
    EXPECT_THAT(storage_one.UpdateCrc(), Eq(Crc32(937357362)));
    EXPECT_THAT(storage_one.GetCrc(), Eq(Crc32(937357362)));
    EXPECT_THAT(crc, Eq(937357362));
  }

  {
    IcingArrayStorage storage_two(filesystem_);
    // Init should fail because the updated checksum was never written to crc.
    ASSERT_TRUE(
        storage_two.Init(sfd.get(), /*fd_offset=*/0, /*map_shared=*/true,
                         /*elt_size=*/sizeof(uint32_t), /*num_elts=*/1,
                         /*max_num_elts=*/1000, &crc, /*init_crc=*/false));
    EXPECT_THAT(storage_two.GetCrc().Get(), Eq(937357362));
  }
}

}  // namespace

}  // namespace lib
}  // namespace icing