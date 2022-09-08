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

#include "icing/file/persistent-hash-map.h"

#include <cstring>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/crc32.h"

namespace icing {
namespace lib {

namespace {

static constexpr int32_t kCorruptedValueOffset = 3;

using ::testing::Contains;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Key;
using ::testing::Not;
using ::testing::Pair;
using ::testing::Pointee;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;

using Bucket = PersistentHashMap::Bucket;
using Crcs = PersistentHashMap::Crcs;
using Entry = PersistentHashMap::Entry;
using Info = PersistentHashMap::Info;

class PersistentHashMapTest : public ::testing::Test {
 protected:
  void SetUp() override {
    base_dir_ = GetTestTempDir() + "/persistent_hash_map_test";
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(base_dir_.c_str());
  }

  std::vector<char> Serialize(int val) {
    std::vector<char> ret(sizeof(val));
    memcpy(ret.data(), &val, sizeof(val));
    return ret;
  }

  libtextclassifier3::StatusOr<int> GetValueByKey(
      PersistentHashMap* persistent_hash_map, std::string_view key) {
    int val;
    ICING_RETURN_IF_ERROR(persistent_hash_map->Get(key, &val));
    return val;
  }

  std::unordered_map<std::string, int> GetAllKeyValuePairs(
      PersistentHashMap::Iterator&& iter) {
    std::unordered_map<std::string, int> kvps;

    while (iter.Advance()) {
      int val;
      memcpy(&val, iter.GetValue(), sizeof(val));
      kvps.emplace(iter.GetKey(), val);
    }
    return kvps;
  }

  Filesystem filesystem_;
  std::string base_dir_;
};

TEST_F(PersistentHashMapTest, InvalidBaseDir) {
  EXPECT_THAT(PersistentHashMap::Create(filesystem_, "/dev/null",
                                        /*value_type_size=*/sizeof(int)),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL));
}

TEST_F(PersistentHashMapTest, InitializeNewFiles) {
  {
    ASSERT_FALSE(filesystem_.DirectoryExists(base_dir_.c_str()));
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<PersistentHashMap> persistent_hash_map,
        PersistentHashMap::Create(filesystem_, base_dir_,
                                  /*value_type_size=*/sizeof(int)));
    EXPECT_THAT(persistent_hash_map, Pointee(IsEmpty()));

    ICING_ASSERT_OK(persistent_hash_map->PersistToDisk());
  }

  // Metadata file should be initialized correctly for both info and crcs
  // sections.
  const std::string metadata_file_path =
      absl_ports::StrCat(base_dir_, "/", PersistentHashMap::kSubDirectory, "/",
                         PersistentHashMap::kFilePrefix, ".m");
  ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
  ASSERT_TRUE(metadata_sfd.is_valid());

  // Check info section
  Info info;
  ASSERT_TRUE(filesystem_.PRead(metadata_sfd.get(), &info, sizeof(Info),
                                Info::kFileOffset));
  EXPECT_THAT(info.version, Eq(PersistentHashMap::kVersion));
  EXPECT_THAT(info.value_type_size, Eq(sizeof(int)));
  EXPECT_THAT(info.max_load_factor_percent,
              Eq(PersistentHashMap::kDefaultMaxLoadFactorPercent));
  EXPECT_THAT(info.num_deleted_entries, Eq(0));
  EXPECT_THAT(info.num_deleted_key_value_bytes, Eq(0));

  // Check crcs section
  Crcs crcs;
  ASSERT_TRUE(filesystem_.PRead(metadata_sfd.get(), &crcs, sizeof(Crcs),
                                Crcs::kFileOffset));
  // # of elements in bucket_storage should be 1, so it should have non-zero
  // crc value.
  EXPECT_THAT(crcs.component_crcs.bucket_storage_crc, Not(Eq(0)));
  // Other empty file backed vectors should have 0 crc value.
  EXPECT_THAT(crcs.component_crcs.entry_storage_crc, Eq(0));
  EXPECT_THAT(crcs.component_crcs.kv_storage_crc, Eq(0));
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

TEST_F(PersistentHashMapTest,
       TestInitializationFailsWithoutPersistToDiskOrDestruction) {
  // Create new persistent hash map
  // Set max_load_factor_percent as 1000. Load factor percent is calculated as
  // 100 * num_keys / num_buckets. Therefore, with 1 bucket (the initial # of
  // buckets in an empty PersistentHashMap) and a max_load_factor_percent of
  // 1000, we would allow the insertion of up to 10 keys before rehashing, to
  // avoid PersistToDisk being called implicitly by rehashing.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int),
                                /*max_load_factor_percent=*/1000));

  // Put some key value pairs.
  ICING_ASSERT_OK(persistent_hash_map->Put("a", Serialize(1).data()));
  ICING_ASSERT_OK(persistent_hash_map->Put("b", Serialize(2).data()));
  ICING_ASSERT_OK(persistent_hash_map->Put("c", Serialize(3).data()));
  // Call Delete() to change PersistentHashMap metadata info
  // (num_deleted_entries)
  ICING_ASSERT_OK(persistent_hash_map->Delete("c"));

  ASSERT_THAT(persistent_hash_map, Pointee(SizeIs(2)));
  ASSERT_THAT(GetValueByKey(persistent_hash_map.get(), "a"), IsOkAndHolds(1));
  ASSERT_THAT(GetValueByKey(persistent_hash_map.get(), "b"), IsOkAndHolds(2));

  // Without calling PersistToDisk, checksums will not be recomputed or synced
  // to disk, so initializing another instance on the same files should fail.
  EXPECT_THAT(PersistentHashMap::Create(filesystem_, base_dir_,
                                        /*value_type_size=*/sizeof(int),
                                        /*max_load_factor_percent=*/1000),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(PersistentHashMapTest, TestInitializationSucceedsWithPersistToDisk) {
  // Create new persistent hash map
  // Set max_load_factor_percent as 1000. Load factor percent is calculated as
  // 100 * num_keys / num_buckets. Therefore, with 1 bucket (the initial # of
  // buckets in an empty PersistentHashMap) and a max_load_factor_percent of
  // 1000, we would allow the insertion of up to 10 keys before rehashing, to
  // avoid PersistToDisk being called implicitly by rehashing.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map1,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int),
                                /*max_load_factor_percent=*/1000));

  // Put some key value pairs.
  ICING_ASSERT_OK(persistent_hash_map1->Put("a", Serialize(1).data()));
  ICING_ASSERT_OK(persistent_hash_map1->Put("b", Serialize(2).data()));
  ICING_ASSERT_OK(persistent_hash_map1->Put("c", Serialize(3).data()));
  // Call Delete() to change PersistentHashMap metadata info
  // (num_deleted_entries)
  ICING_ASSERT_OK(persistent_hash_map1->Delete("c"));

  ASSERT_THAT(persistent_hash_map1, Pointee(SizeIs(2)));
  ASSERT_THAT(GetValueByKey(persistent_hash_map1.get(), "a"), IsOkAndHolds(1));
  ASSERT_THAT(GetValueByKey(persistent_hash_map1.get(), "b"), IsOkAndHolds(2));

  // After calling PersistToDisk, all checksums should be recomputed and synced
  // correctly to disk, so initializing another instance on the same files
  // should succeed, and we should be able to get the same contents.
  ICING_EXPECT_OK(persistent_hash_map1->PersistToDisk());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map2,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int),
                                /*max_load_factor_percent=*/1000));
  EXPECT_THAT(persistent_hash_map2, Pointee(SizeIs(2)));
  EXPECT_THAT(GetValueByKey(persistent_hash_map2.get(), "a"), IsOkAndHolds(1));
  EXPECT_THAT(GetValueByKey(persistent_hash_map2.get(), "b"), IsOkAndHolds(2));
}

TEST_F(PersistentHashMapTest, TestInitializationSucceedsAfterDestruction) {
  {
    // Create new persistent hash map
    // Set max_load_factor_percent as 1000. Load factor percent is calculated as
    // 100 * num_keys / num_buckets. Therefore, with 1 bucket (the initial # of
    // buckets in an empty PersistentHashMap) and a max_load_factor_percent of
    // 1000, we would allow the insertion of up to 10 keys before rehashing, to
    // avoid PersistToDisk being called implicitly by rehashing.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<PersistentHashMap> persistent_hash_map,
        PersistentHashMap::Create(filesystem_, base_dir_,
                                  /*value_type_size=*/sizeof(int),
                                  /*max_load_factor_percent=*/1000));
    ICING_ASSERT_OK(persistent_hash_map->Put("a", Serialize(1).data()));
    ICING_ASSERT_OK(persistent_hash_map->Put("b", Serialize(2).data()));
    ICING_ASSERT_OK(persistent_hash_map->Put("c", Serialize(3).data()));
    // Call Delete() to change PersistentHashMap metadata info
    // (num_deleted_entries)
    ICING_ASSERT_OK(persistent_hash_map->Delete("c"));

    ASSERT_THAT(persistent_hash_map, Pointee(SizeIs(2)));
    ASSERT_THAT(GetValueByKey(persistent_hash_map.get(), "a"), IsOkAndHolds(1));
    ASSERT_THAT(GetValueByKey(persistent_hash_map.get(), "b"), IsOkAndHolds(2));
  }

  {
    // The previous instance went out of scope and was destructed. Although we
    // didn't call PersistToDisk explicitly, the destructor should invoke it and
    // thus initializing another instance on the same files should succeed, and
    // we should be able to get the same contents.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<PersistentHashMap> persistent_hash_map,
        PersistentHashMap::Create(filesystem_, base_dir_,
                                  /*value_type_size=*/sizeof(int),
                                  /*max_load_factor_percent=*/1000));
    EXPECT_THAT(persistent_hash_map, Pointee(SizeIs(2)));
    EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "a"), IsOkAndHolds(1));
    EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "b"), IsOkAndHolds(2));
  }
}

TEST_F(PersistentHashMapTest,
       InitializeExistingFilesWithDifferentValueTypeSizeShouldFail) {
  {
    // Create new persistent hash map
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<PersistentHashMap> persistent_hash_map,
        PersistentHashMap::Create(filesystem_, base_dir_,
                                  /*value_type_size=*/sizeof(int)));
    ICING_ASSERT_OK(persistent_hash_map->Put("a", Serialize(1).data()));

    ICING_ASSERT_OK(persistent_hash_map->PersistToDisk());
  }

  {
    // Attempt to create the persistent hash map with different value type size.
    // This should fail.
    ASSERT_THAT(sizeof(char), Not(Eq(sizeof(int))));
    libtextclassifier3::StatusOr<std::unique_ptr<PersistentHashMap>>
        persistent_hash_map_or = PersistentHashMap::Create(
            filesystem_, base_dir_, /*value_type_size=*/sizeof(char));
    EXPECT_THAT(persistent_hash_map_or,
                StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
    EXPECT_THAT(persistent_hash_map_or.status().error_message(),
                HasSubstr("Incorrect value type size"));
  }
}

TEST_F(PersistentHashMapTest, InitializeExistingFilesWithWrongAllCrc) {
  {
    // Create new persistent hash map
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<PersistentHashMap> persistent_hash_map,
        PersistentHashMap::Create(filesystem_, base_dir_,
                                  /*value_type_size=*/sizeof(int)));
    ICING_ASSERT_OK(persistent_hash_map->Put("a", Serialize(1).data()));

    ICING_ASSERT_OK(persistent_hash_map->PersistToDisk());
  }

  const std::string metadata_file_path =
      absl_ports::StrCat(base_dir_, "/", PersistentHashMap::kSubDirectory, "/",
                         PersistentHashMap::kFilePrefix, ".m");
  ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
  ASSERT_TRUE(metadata_sfd.is_valid());

  Crcs crcs;
  ASSERT_TRUE(filesystem_.PRead(metadata_sfd.get(), &crcs, sizeof(Crcs),
                                Crcs::kFileOffset));

  // Manually corrupt all_crc
  crcs.all_crc += kCorruptedValueOffset;
  ASSERT_TRUE(filesystem_.PWrite(metadata_sfd.get(), Crcs::kFileOffset, &crcs,
                                 sizeof(Crcs)));
  metadata_sfd.reset();

  {
    // Attempt to create the persistent hash map with metadata containing
    // corrupted all_crc. This should fail.
    libtextclassifier3::StatusOr<std::unique_ptr<PersistentHashMap>>
        persistent_hash_map_or = PersistentHashMap::Create(
            filesystem_, base_dir_, /*value_type_size=*/sizeof(int));
    EXPECT_THAT(persistent_hash_map_or,
                StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
    EXPECT_THAT(persistent_hash_map_or.status().error_message(),
                HasSubstr("Invalid all crc for PersistentHashMap"));
  }
}

TEST_F(PersistentHashMapTest,
       InitializeExistingFilesWithCorruptedInfoShouldFail) {
  {
    // Create new persistent hash map
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<PersistentHashMap> persistent_hash_map,
        PersistentHashMap::Create(filesystem_, base_dir_,
                                  /*value_type_size=*/sizeof(int)));
    ICING_ASSERT_OK(persistent_hash_map->Put("a", Serialize(1).data()));

    ICING_ASSERT_OK(persistent_hash_map->PersistToDisk());
  }

  const std::string metadata_file_path =
      absl_ports::StrCat(base_dir_, "/", PersistentHashMap::kSubDirectory, "/",
                         PersistentHashMap::kFilePrefix, ".m");
  ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
  ASSERT_TRUE(metadata_sfd.is_valid());

  Info info;
  ASSERT_TRUE(filesystem_.PRead(metadata_sfd.get(), &info, sizeof(Info),
                                Info::kFileOffset));

  // Modify info, but don't update the checksum. This would be similar to
  // corruption of info.
  info.num_deleted_entries += kCorruptedValueOffset;
  ASSERT_TRUE(filesystem_.PWrite(metadata_sfd.get(), Info::kFileOffset, &info,
                                 sizeof(Info)));
  {
    // Attempt to create the persistent hash map with info that doesn't match
    // its checksum and confirm that it fails.
    libtextclassifier3::StatusOr<std::unique_ptr<PersistentHashMap>>
        persistent_hash_map_or = PersistentHashMap::Create(
            filesystem_, base_dir_, /*value_type_size=*/sizeof(int));
    EXPECT_THAT(persistent_hash_map_or,
                StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
    EXPECT_THAT(persistent_hash_map_or.status().error_message(),
                HasSubstr("Invalid info crc for PersistentHashMap"));
  }
}

TEST_F(PersistentHashMapTest,
       InitializeExistingFilesWithWrongBucketStorageCrc) {
  {
    // Create new persistent hash map
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<PersistentHashMap> persistent_hash_map,
        PersistentHashMap::Create(filesystem_, base_dir_,
                                  /*value_type_size=*/sizeof(int)));
    ICING_ASSERT_OK(persistent_hash_map->Put("a", Serialize(1).data()));

    ICING_ASSERT_OK(persistent_hash_map->PersistToDisk());
  }

  const std::string metadata_file_path =
      absl_ports::StrCat(base_dir_, "/", PersistentHashMap::kSubDirectory, "/",
                         PersistentHashMap::kFilePrefix, ".m");
  ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
  ASSERT_TRUE(metadata_sfd.is_valid());

  Crcs crcs;
  ASSERT_TRUE(filesystem_.PRead(metadata_sfd.get(), &crcs, sizeof(Crcs),
                                Crcs::kFileOffset));

  // Manually corrupt bucket_storage_crc
  crcs.component_crcs.bucket_storage_crc += kCorruptedValueOffset;
  crcs.all_crc = Crc32(std::string_view(
                           reinterpret_cast<const char*>(&crcs.component_crcs),
                           sizeof(Crcs::ComponentCrcs)))
                     .Get();
  ASSERT_TRUE(filesystem_.PWrite(metadata_sfd.get(), Crcs::kFileOffset, &crcs,
                                 sizeof(Crcs)));
  {
    // Attempt to create the persistent hash map with metadata containing
    // corrupted bucket_storage_crc. This should fail.
    libtextclassifier3::StatusOr<std::unique_ptr<PersistentHashMap>>
        persistent_hash_map_or = PersistentHashMap::Create(
            filesystem_, base_dir_, /*value_type_size=*/sizeof(int));
    EXPECT_THAT(persistent_hash_map_or,
                StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
    EXPECT_THAT(
        persistent_hash_map_or.status().error_message(),
        HasSubstr("Mismatch crc with PersistentHashMap bucket storage"));
  }
}

TEST_F(PersistentHashMapTest, InitializeExistingFilesWithWrongEntryStorageCrc) {
  {
    // Create new persistent hash map
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<PersistentHashMap> persistent_hash_map,
        PersistentHashMap::Create(filesystem_, base_dir_,
                                  /*value_type_size=*/sizeof(int)));
    ICING_ASSERT_OK(persistent_hash_map->Put("a", Serialize(1).data()));

    ICING_ASSERT_OK(persistent_hash_map->PersistToDisk());
  }

  const std::string metadata_file_path =
      absl_ports::StrCat(base_dir_, "/", PersistentHashMap::kSubDirectory, "/",
                         PersistentHashMap::kFilePrefix, ".m");
  ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
  ASSERT_TRUE(metadata_sfd.is_valid());

  Crcs crcs;
  ASSERT_TRUE(filesystem_.PRead(metadata_sfd.get(), &crcs, sizeof(Crcs),
                                Crcs::kFileOffset));

  // Manually corrupt entry_storage_crc
  crcs.component_crcs.entry_storage_crc += kCorruptedValueOffset;
  crcs.all_crc = Crc32(std::string_view(
                           reinterpret_cast<const char*>(&crcs.component_crcs),
                           sizeof(Crcs::ComponentCrcs)))
                     .Get();
  ASSERT_TRUE(filesystem_.PWrite(metadata_sfd.get(), Crcs::kFileOffset, &crcs,
                                 sizeof(Crcs)));
  {
    // Attempt to create the persistent hash map with metadata containing
    // corrupted entry_storage_crc. This should fail.
    libtextclassifier3::StatusOr<std::unique_ptr<PersistentHashMap>>
        persistent_hash_map_or = PersistentHashMap::Create(
            filesystem_, base_dir_, /*value_type_size=*/sizeof(int));
    EXPECT_THAT(persistent_hash_map_or,
                StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
    EXPECT_THAT(persistent_hash_map_or.status().error_message(),
                HasSubstr("Mismatch crc with PersistentHashMap entry storage"));
  }
}

TEST_F(PersistentHashMapTest,
       InitializeExistingFilesWithWrongKeyValueStorageCrc) {
  {
    // Create new persistent hash map
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<PersistentHashMap> persistent_hash_map,
        PersistentHashMap::Create(filesystem_, base_dir_,
                                  /*value_type_size=*/sizeof(int)));
    ICING_ASSERT_OK(persistent_hash_map->Put("a", Serialize(1).data()));

    ICING_ASSERT_OK(persistent_hash_map->PersistToDisk());
  }

  const std::string metadata_file_path =
      absl_ports::StrCat(base_dir_, "/", PersistentHashMap::kSubDirectory, "/",
                         PersistentHashMap::kFilePrefix, ".m");
  ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
  ASSERT_TRUE(metadata_sfd.is_valid());

  Crcs crcs;
  ASSERT_TRUE(filesystem_.PRead(metadata_sfd.get(), &crcs, sizeof(Crcs),
                                Crcs::kFileOffset));

  // Manually corrupt kv_storage_crc
  crcs.component_crcs.kv_storage_crc += kCorruptedValueOffset;
  crcs.all_crc = Crc32(std::string_view(
                           reinterpret_cast<const char*>(&crcs.component_crcs),
                           sizeof(Crcs::ComponentCrcs)))
                     .Get();
  ASSERT_TRUE(filesystem_.PWrite(metadata_sfd.get(), Crcs::kFileOffset, &crcs,
                                 sizeof(Crcs)));
  {
    // Attempt to create the persistent hash map with metadata containing
    // corrupted kv_storage_crc. This should fail.
    libtextclassifier3::StatusOr<std::unique_ptr<PersistentHashMap>>
        persistent_hash_map_or = PersistentHashMap::Create(
            filesystem_, base_dir_, /*value_type_size=*/sizeof(int));
    EXPECT_THAT(persistent_hash_map_or,
                StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
    EXPECT_THAT(
        persistent_hash_map_or.status().error_message(),
        HasSubstr("Mismatch crc with PersistentHashMap key value storage"));
  }
}

TEST_F(PersistentHashMapTest,
       InitializeExistingFilesAllowDifferentMaxLoadFactorPercent) {
  {
    // Create new persistent hash map
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<PersistentHashMap> persistent_hash_map,
        PersistentHashMap::Create(filesystem_, base_dir_,
                                  /*value_type_size=*/sizeof(int)));
    ICING_ASSERT_OK(persistent_hash_map->Put("a", Serialize(1).data()));
    ICING_ASSERT_OK(persistent_hash_map->Put("b", Serialize(2).data()));

    ASSERT_THAT(persistent_hash_map, Pointee(SizeIs(2)));
    ASSERT_THAT(GetValueByKey(persistent_hash_map.get(), "a"), IsOkAndHolds(1));
    ASSERT_THAT(GetValueByKey(persistent_hash_map.get(), "b"), IsOkAndHolds(2));

    ICING_ASSERT_OK(persistent_hash_map->PersistToDisk());
  }

  int32_t new_max_load_factor_percent = 100;
  {
    ASSERT_THAT(new_max_load_factor_percent,
                Not(Eq(PersistentHashMap::kDefaultMaxLoadFactorPercent)));
    // Attempt to create the persistent hash map with different max load factor
    // percent. This should succeed and metadata should be modified correctly.
    // Also verify all entries should remain unchanged.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<PersistentHashMap> persistent_hash_map,
        PersistentHashMap::Create(filesystem_, base_dir_,
                                  /*value_type_size=*/sizeof(int),
                                  new_max_load_factor_percent));

    EXPECT_THAT(persistent_hash_map, Pointee(SizeIs(2)));
    EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "a"), IsOkAndHolds(1));
    EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "b"), IsOkAndHolds(2));

    ICING_ASSERT_OK(persistent_hash_map->PersistToDisk());
  }

  const std::string metadata_file_path =
      absl_ports::StrCat(base_dir_, "/", PersistentHashMap::kSubDirectory, "/",
                         PersistentHashMap::kFilePrefix, ".m");
  ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
  ASSERT_TRUE(metadata_sfd.is_valid());

  Info info;
  ASSERT_TRUE(filesystem_.PRead(metadata_sfd.get(), &info, sizeof(Info),
                                Info::kFileOffset));
  EXPECT_THAT(info.max_load_factor_percent, Eq(new_max_load_factor_percent));

  // Also should update crcs correctly. We test it by creating instance again
  // and make sure it won't get corrupted crcs/info errors.
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<PersistentHashMap> persistent_hash_map,
        PersistentHashMap::Create(filesystem_, base_dir_,
                                  /*value_type_size=*/sizeof(int),
                                  new_max_load_factor_percent));

    ICING_ASSERT_OK(persistent_hash_map->PersistToDisk());
  }
}

TEST_F(PersistentHashMapTest, PutAndGet) {
  // Create new persistent hash map
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int)));

  EXPECT_THAT(persistent_hash_map, Pointee(IsEmpty()));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-youtube.com"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  ICING_EXPECT_OK(
      persistent_hash_map->Put("default-google.com", Serialize(100).data()));
  ICING_EXPECT_OK(
      persistent_hash_map->Put("default-youtube.com", Serialize(50).data()));

  EXPECT_THAT(persistent_hash_map, Pointee(SizeIs(2)));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com"),
              IsOkAndHolds(100));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-youtube.com"),
              IsOkAndHolds(50));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "key-not-exist"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  ICING_ASSERT_OK(persistent_hash_map->PersistToDisk());
}

TEST_F(PersistentHashMapTest, PutShouldOverwriteValueIfKeyExists) {
  // Create new persistent hash map
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int)));

  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com", Serialize(100).data()));
  ASSERT_THAT(persistent_hash_map, Pointee(SizeIs(1)));
  ASSERT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com"),
              IsOkAndHolds(100));

  ICING_EXPECT_OK(
      persistent_hash_map->Put("default-google.com", Serialize(200).data()));
  EXPECT_THAT(persistent_hash_map, Pointee(SizeIs(1)));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com"),
              IsOkAndHolds(200));

  ICING_EXPECT_OK(
      persistent_hash_map->Put("default-google.com", Serialize(300).data()));
  EXPECT_THAT(persistent_hash_map, Pointee(SizeIs(1)));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com"),
              IsOkAndHolds(300));
}

TEST_F(PersistentHashMapTest, GetOrPutShouldPutIfKeyDoesNotExist) {
  // Create new persistent hash map
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int)));

  ASSERT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  int val = 1;
  EXPECT_THAT(persistent_hash_map->GetOrPut("default-google.com", &val),
              IsOk());
  EXPECT_THAT(val, Eq(1));
  EXPECT_THAT(persistent_hash_map, Pointee(SizeIs(1)));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com"),
              IsOkAndHolds(1));
}

TEST_F(PersistentHashMapTest, GetOrPutShouldGetIfKeyExists) {
  // Create new persistent hash map
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int)));

  ASSERT_THAT(
      persistent_hash_map->Put("default-google.com", Serialize(1).data()),
      IsOk());
  ASSERT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com"),
              IsOkAndHolds(1));

  int val = 2;
  EXPECT_THAT(persistent_hash_map->GetOrPut("default-google.com", &val),
              IsOk());
  EXPECT_THAT(val, Eq(1));
  EXPECT_THAT(persistent_hash_map, Pointee(SizeIs(1)));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com"),
              IsOkAndHolds(1));
}

TEST_F(PersistentHashMapTest, Delete) {
  // Create new persistent hash map
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int)));

  // Delete a non-existing key should get NOT_FOUND error
  EXPECT_THAT(persistent_hash_map->Delete("default-google.com"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com", Serialize(100).data()));
  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-youtube.com", Serialize(50).data()));
  ASSERT_THAT(persistent_hash_map, Pointee(SizeIs(2)));

  // Delete an existing key should succeed
  ICING_EXPECT_OK(persistent_hash_map->Delete("default-google.com"));
  EXPECT_THAT(persistent_hash_map, Pointee(SizeIs(1)));
  // The deleted key should not be found.
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  // Other key should remain unchanged and available
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-youtube.com"),
              IsOkAndHolds(50));

  // Insert back the deleted key. Should get new value
  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com", Serialize(200).data()));
  ASSERT_THAT(persistent_hash_map, Pointee(SizeIs(2)));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com"),
              IsOkAndHolds(200));

  // Delete again
  ICING_EXPECT_OK(persistent_hash_map->Delete("default-google.com"));
  EXPECT_THAT(persistent_hash_map, Pointee(SizeIs(1)));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  // Other keys should remain unchanged and available
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-youtube.com"),
              IsOkAndHolds(50));
}

TEST_F(PersistentHashMapTest, DeleteMultiple) {
  // Create new persistent hash map
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int)));

  std::unordered_map<std::string, int> existing_keys;
  std::unordered_set<std::string> deleted_keys;
  // Insert 100 key value pairs
  for (int i = 0; i < 100; ++i) {
    std::string key = "default-google.com-" + std::to_string(i);
    ICING_ASSERT_OK(persistent_hash_map->Put(key, &i));
    existing_keys[key] = i;
  }
  ASSERT_THAT(persistent_hash_map, Pointee(SizeIs(existing_keys.size())));

  // Delete several keys.
  // Simulate with std::unordered_map and verify.
  std::vector<int> delete_target_ids{3, 4, 6, 9, 13, 18, 24, 31, 39, 48, 58};
  for (const int delete_target_id : delete_target_ids) {
    std::string key = "default-google.com-" + std::to_string(delete_target_id);
    ASSERT_THAT(existing_keys, Contains(Key(key)));
    ASSERT_THAT(GetValueByKey(persistent_hash_map.get(), key),
                IsOkAndHolds(existing_keys[key]));
    ICING_EXPECT_OK(persistent_hash_map->Delete(key));

    existing_keys.erase(key);
    deleted_keys.insert(key);
  }

  // Deleted keys should not be found.
  for (const std::string& deleted_key : deleted_keys) {
    EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), deleted_key),
                StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  }
  // Other keys should remain unchanged and available
  for (const auto& [existing_key, existing_value] : existing_keys) {
    EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), existing_key),
                IsOkAndHolds(existing_value));
  }
  // Verify by iterator as well
  EXPECT_THAT(GetAllKeyValuePairs(persistent_hash_map->GetIterator()),
              Eq(existing_keys));
}

TEST_F(PersistentHashMapTest, DeleteBucketHeadElement) {
  // Create new persistent hash map
  // Set max_load_factor_percent as 1000. Load factor percent is calculated as
  // 100 * num_keys / num_buckets. Therefore, with 1 bucket (the initial # of
  // buckets in an empty PersistentHashMap) and a max_load_factor_percent of
  // 1000, we would allow the insertion of up to 10 keys before rehashing.
  // Preventing rehashing makes it much easier to test collisions.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int),
                                /*max_load_factor_percent=*/1000));

  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-0", Serialize(0).data()));
  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-1", Serialize(1).data()));
  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-2", Serialize(2).data()));
  ASSERT_THAT(persistent_hash_map, Pointee(SizeIs(3)));
  ASSERT_THAT(persistent_hash_map->num_buckets(), Eq(1));

  // Delete the head element of the bucket. Note that in our implementation, the
  // last added element will become the head element of the bucket.
  ICING_ASSERT_OK(persistent_hash_map->Delete("default-google.com-2"));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com-0"),
              IsOkAndHolds(0));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com-1"),
              IsOkAndHolds(1));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com-2"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(PersistentHashMapTest, DeleteBucketIntermediateElement) {
  // Create new persistent hash map
  // Set max_load_factor_percent as 1000. Load factor percent is calculated as
  // 100 * num_keys / num_buckets. Therefore, with 1 bucket (the initial # of
  // buckets in an empty PersistentHashMap) and a max_load_factor_percent of
  // 1000, we would allow the insertion of up to 10 keys before rehashing.
  // Preventing rehashing makes it much easier to test collisions.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int),
                                /*max_load_factor_percent=*/1000));

  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-0", Serialize(0).data()));
  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-1", Serialize(1).data()));
  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-2", Serialize(2).data()));
  ASSERT_THAT(persistent_hash_map, Pointee(SizeIs(3)));
  ASSERT_THAT(persistent_hash_map->num_buckets(), Eq(1));

  // Delete any intermediate element of the bucket.
  ICING_ASSERT_OK(persistent_hash_map->Delete("default-google.com-1"));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com-0"),
              IsOkAndHolds(0));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com-1"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com-2"),
              IsOkAndHolds(2));
}

TEST_F(PersistentHashMapTest, DeleteBucketTailElement) {
  // Create new persistent hash map
  // Set max_load_factor_percent as 1000. Load factor percent is calculated as
  // 100 * num_keys / num_buckets. Therefore, with 1 bucket (the initial # of
  // buckets in an empty PersistentHashMap) and a max_load_factor_percent of
  // 1000, we would allow the insertion of up to 10 keys before rehashing.
  // Preventing rehashing makes it much easier to test collisions.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int),
                                /*max_load_factor_percent=*/1000));

  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-0", Serialize(0).data()));
  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-1", Serialize(1).data()));
  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-2", Serialize(2).data()));
  ASSERT_THAT(persistent_hash_map, Pointee(SizeIs(3)));
  ASSERT_THAT(persistent_hash_map->num_buckets(), Eq(1));

  // Delete the last element of the bucket. Note that in our implementation, the
  // first added element will become the tail element of the bucket.
  ICING_ASSERT_OK(persistent_hash_map->Delete("default-google.com-0"));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com-0"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com-1"),
              IsOkAndHolds(1));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com-2"),
              IsOkAndHolds(2));
}

TEST_F(PersistentHashMapTest, DeleteBucketOnlySingleElement) {
  // Create new persistent hash map
  // Set max_load_factor_percent as 1000. Load factor percent is calculated as
  // 100 * num_keys / num_buckets. Therefore, with 1 bucket (the initial # of
  // buckets in an empty PersistentHashMap) and a max_load_factor_percent of
  // 1000, we would allow the insertion of up to 10 keys before rehashing.
  // Preventing rehashing makes it much easier to test collisions.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int),
                                /*max_load_factor_percent=*/1000));

  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com", Serialize(100).data()));
  ASSERT_THAT(persistent_hash_map, Pointee(SizeIs(1)));

  // Delete the only single element of the bucket.
  ICING_ASSERT_OK(persistent_hash_map->Delete("default-google.com"));
  ASSERT_THAT(persistent_hash_map, Pointee(IsEmpty()));
  EXPECT_THAT(GetValueByKey(persistent_hash_map.get(), "default-google.com"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(PersistentHashMapTest, ShouldFailIfKeyContainsTerminationCharacter) {
  // Create new persistent hash map
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int)));

  const char invalid_key[] = "a\0bc";
  std::string_view invalid_key_view(invalid_key, 4);

  int val = 1;
  EXPECT_THAT(persistent_hash_map->Put(invalid_key_view, &val),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(persistent_hash_map->GetOrPut(invalid_key_view, &val),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(persistent_hash_map->Get(invalid_key_view, &val),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(persistent_hash_map->Delete(invalid_key_view),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(PersistentHashMapTest, EmptyHashMapIterator) {
  // Create new persistent hash map
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int)));

  EXPECT_FALSE(persistent_hash_map->GetIterator().Advance());
}

TEST_F(PersistentHashMapTest, Iterator) {
  // Create new persistent hash map
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int)));

  std::unordered_map<std::string, int> kvps;
  // Insert 100 key value pairs
  for (int i = 0; i < 100; ++i) {
    std::string key = "default-google.com-" + std::to_string(i);
    ICING_ASSERT_OK(persistent_hash_map->Put(key, &i));
    kvps.emplace(key, i);
  }
  ASSERT_THAT(persistent_hash_map, Pointee(SizeIs(kvps.size())));

  EXPECT_THAT(GetAllKeyValuePairs(persistent_hash_map->GetIterator()),
              Eq(kvps));
}

TEST_F(PersistentHashMapTest, IteratorAfterDeletingFirstKeyValuePair) {
  // Create new persistent hash map
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int)));

  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-0", Serialize(0).data()));
  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-1", Serialize(1).data()));
  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-2", Serialize(2).data()));
  ASSERT_THAT(persistent_hash_map, Pointee(SizeIs(3)));

  // Delete the first key value pair.
  ICING_ASSERT_OK(persistent_hash_map->Delete("default-google.com-0"));
  EXPECT_THAT(GetAllKeyValuePairs(persistent_hash_map->GetIterator()),
              UnorderedElementsAre(Pair("default-google.com-1", 1),
                                   Pair("default-google.com-2", 2)));
}

TEST_F(PersistentHashMapTest, IteratorAfterDeletingIntermediateKeyValuePair) {
  // Create new persistent hash map
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int)));

  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-0", Serialize(0).data()));
  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-1", Serialize(1).data()));
  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-2", Serialize(2).data()));
  ASSERT_THAT(persistent_hash_map, Pointee(SizeIs(3)));

  // Delete any intermediate key value pair.
  ICING_ASSERT_OK(persistent_hash_map->Delete("default-google.com-1"));
  EXPECT_THAT(GetAllKeyValuePairs(persistent_hash_map->GetIterator()),
              UnorderedElementsAre(Pair("default-google.com-0", 0),
                                   Pair("default-google.com-2", 2)));
}

TEST_F(PersistentHashMapTest, IteratorAfterDeletingLastKeyValuePair) {
  // Create new persistent hash map
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int)));

  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-0", Serialize(0).data()));
  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-1", Serialize(1).data()));
  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-2", Serialize(2).data()));
  ASSERT_THAT(persistent_hash_map, Pointee(SizeIs(3)));

  // Delete the last key value pair.
  ICING_ASSERT_OK(persistent_hash_map->Delete("default-google.com-2"));
  EXPECT_THAT(GetAllKeyValuePairs(persistent_hash_map->GetIterator()),
              UnorderedElementsAre(Pair("default-google.com-0", 0),
                                   Pair("default-google.com-1", 1)));
}

TEST_F(PersistentHashMapTest, IteratorAfterDeletingAllKeyValuePairs) {
  // Create new persistent hash map
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<PersistentHashMap> persistent_hash_map,
      PersistentHashMap::Create(filesystem_, base_dir_,
                                /*value_type_size=*/sizeof(int)));

  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-0", Serialize(0).data()));
  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-1", Serialize(1).data()));
  ICING_ASSERT_OK(
      persistent_hash_map->Put("default-google.com-2", Serialize(2).data()));
  ASSERT_THAT(persistent_hash_map, Pointee(SizeIs(3)));

  // Delete all key value pairs.
  ICING_ASSERT_OK(persistent_hash_map->Delete("default-google.com-0"));
  ICING_ASSERT_OK(persistent_hash_map->Delete("default-google.com-1"));
  ICING_ASSERT_OK(persistent_hash_map->Delete("default-google.com-2"));
  ASSERT_THAT(persistent_hash_map, Pointee(IsEmpty()));
  EXPECT_FALSE(persistent_hash_map->GetIterator().Advance());
}

}  // namespace

}  // namespace lib
}  // namespace icing
