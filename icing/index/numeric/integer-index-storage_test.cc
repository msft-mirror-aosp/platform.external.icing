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

#include "icing/index/numeric/integer-index-storage.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/file/posting_list/posting-list-identifier.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/index/numeric/posting-list-integer-index-serializer.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/crc32.h"

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
using ::testing::Ne;

using Bucket = IntegerIndexStorage::Bucket;
using Crcs = IntegerIndexStorage::Crcs;
using Info = IntegerIndexStorage::Info;
using Options = IntegerIndexStorage::Options;

static constexpr int32_t kCorruptedValueOffset = 3;
static constexpr DocumentId kDefaultDocumentId = 123;
static constexpr SectionId kDefaultSectionId = 31;

class IntegerIndexStorageTest : public ::testing::Test {
 protected:
  void SetUp() override {
    base_dir_ = GetTestTempDir() + "/integer_index_storage_test";

    serializer_ = std::make_unique<PostingListIntegerIndexSerializer>();
  }

  void TearDown() override {
    serializer_.reset();
    filesystem_.DeleteDirectoryRecursively(base_dir_.c_str());
  }

  Filesystem filesystem_;
  std::string base_dir_;
  std::unique_ptr<PostingListIntegerIndexSerializer> serializer_;
};

libtextclassifier3::StatusOr<std::vector<DocHitInfo>> Query(
    const IntegerIndexStorage* storage, int64_t key_lower, int64_t key_upper) {
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<DocHitInfoIterator> iter,
                         storage->GetIterator(key_lower, key_upper));
  std::vector<DocHitInfo> hits;
  while (iter->Advance().ok()) {
    hits.push_back(iter->doc_hit_info());
  }
  return hits;
}

TEST_F(IntegerIndexStorageTest, OptionsEmptyCustomInitBucketsShouldBeValid) {
  EXPECT_THAT(Options().IsValid(), IsTrue());
}

TEST_F(IntegerIndexStorageTest, OptionsInvalidCustomInitBucketsRange) {
  // Invalid custom init sorted bucket
  EXPECT_THAT(
      Options(/*custom_init_sorted_buckets_in=*/
              {Bucket(std::numeric_limits<int64_t>::min(), 5), Bucket(9, 6)},
              /*custom_init_unsorted_buckets_in=*/
              {Bucket(10, std::numeric_limits<int64_t>::max())})
          .IsValid(),
      IsFalse());

  // Invalid custom init unsorted bucket
  EXPECT_THAT(
      Options(/*custom_init_sorted_buckets_in=*/
              {Bucket(10, std::numeric_limits<int64_t>::max())},
              /*custom_init_unsorted_buckets_in=*/
              {Bucket(std::numeric_limits<int64_t>::min(), 5), Bucket(9, 6)})
          .IsValid(),
      IsFalse());
}

TEST_F(IntegerIndexStorageTest,
       OptionsInvalidCustomInitBucketsPostingListIdentifier) {
  // Custom init buckets should contain invalid posting list identifier.
  PostingListIdentifier valid_posting_list_identifier(0, 0, 0);
  ASSERT_THAT(valid_posting_list_identifier.is_valid(), IsTrue());

  // Invalid custom init sorted bucket
  EXPECT_THAT(Options(/*custom_init_sorted_buckets_in=*/
                      {Bucket(std::numeric_limits<int64_t>::min(),
                              std::numeric_limits<int64_t>::max(),
                              valid_posting_list_identifier)},
                      /*custom_init_unsorted_buckets_in=*/{})
                  .IsValid(),
              IsFalse());

  // Invalid custom init unsorted bucket
  EXPECT_THAT(Options(/*custom_init_sorted_buckets_in=*/{},
                      /*custom_init_unsorted_buckets_in=*/
                      {Bucket(std::numeric_limits<int64_t>::min(),
                              std::numeric_limits<int64_t>::max(),
                              valid_posting_list_identifier)})
                  .IsValid(),
              IsFalse());
}

TEST_F(IntegerIndexStorageTest, OptionsInvalidCustomInitBucketsOverlapping) {
  // sorted buckets overlap
  EXPECT_THAT(Options(/*custom_init_sorted_buckets_in=*/
                      {Bucket(std::numeric_limits<int64_t>::min(), -100),
                       Bucket(-100, std::numeric_limits<int64_t>::max())},
                      /*custom_init_unsorted_buckets_in=*/{})
                  .IsValid(),
              IsFalse());

  // unsorted buckets overlap
  EXPECT_THAT(Options(/*custom_init_sorted_buckets_in=*/{},
                      /*custom_init_unsorted_buckets_in=*/
                      {Bucket(-100, std::numeric_limits<int64_t>::max()),
                       Bucket(std::numeric_limits<int64_t>::min(), -100)})
                  .IsValid(),
              IsFalse());

  // Cross buckets overlap
  EXPECT_THAT(Options(/*custom_init_sorted_buckets_in=*/
                      {Bucket(std::numeric_limits<int64_t>::min(), -100),
                       Bucket(-99, 0)},
                      /*custom_init_unsorted_buckets_in=*/
                      {Bucket(200, std::numeric_limits<int64_t>::max()),
                       Bucket(0, 50), Bucket(51, 199)})
                  .IsValid(),
              IsFalse());
}

TEST_F(IntegerIndexStorageTest, OptionsInvalidCustomInitBucketsUnion) {
  // Missing INT64_MAX
  EXPECT_THAT(Options(/*custom_init_sorted_buckets_in=*/
                      {Bucket(std::numeric_limits<int64_t>::min(), -100),
                       Bucket(-99, 0)},
                      /*custom_init_unsorted_buckets_in=*/
                      {Bucket(1, 1000)})
                  .IsValid(),
              IsFalse());

  // Missing INT64_MIN
  EXPECT_THAT(Options(/*custom_init_sorted_buckets_in=*/
                      {Bucket(-200, -100), Bucket(-99, 0)},
                      /*custom_init_unsorted_buckets_in=*/
                      {Bucket(1, std::numeric_limits<int64_t>::max())})
                  .IsValid(),
              IsFalse());

  // Missing some intermediate ranges
  EXPECT_THAT(Options(/*custom_init_sorted_buckets_in=*/
                      {Bucket(std::numeric_limits<int64_t>::min(), -100)},
                      /*custom_init_unsorted_buckets_in=*/
                      {Bucket(1, std::numeric_limits<int64_t>::max())})
                  .IsValid(),
              IsFalse());
}

TEST_F(IntegerIndexStorageTest, InvalidBaseDir) {
  EXPECT_THAT(IntegerIndexStorage::Create(filesystem_, "/dev/null", Options(),
                                          serializer_.get()),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL));
}

TEST_F(IntegerIndexStorageTest, CreateWithInvalidOptionsShouldFail) {
  Options invalid_options(
      /*custom_init_sorted_buckets_in=*/{},
      /*custom_init_unsorted_buckets_in=*/{
          Bucket(-100, std::numeric_limits<int64_t>::max()),
          Bucket(std::numeric_limits<int64_t>::min(), -100)});
  ASSERT_THAT(invalid_options.IsValid(), IsFalse());

  EXPECT_THAT(IntegerIndexStorage::Create(filesystem_, base_dir_,
                                          invalid_options, serializer_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(IntegerIndexStorageTest, InitializeNewFiles) {
  {
    // Create new integer index storage
    ASSERT_FALSE(filesystem_.DirectoryExists(base_dir_.c_str()));
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<IntegerIndexStorage> storage,
        IntegerIndexStorage::Create(filesystem_, base_dir_, Options(),
                                    serializer_.get()));

    ICING_ASSERT_OK(storage->PersistToDisk());
  }

  // Metadata file should be initialized correctly for both info and crcs
  // sections.
  const std::string metadata_file_path =
      absl_ports::StrCat(base_dir_, "/", IntegerIndexStorage::kSubDirectory,
                         "/", IntegerIndexStorage::kFilePrefix, ".m");
  ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
  ASSERT_TRUE(metadata_sfd.is_valid());

  // Check info section
  Info info;
  ASSERT_TRUE(filesystem_.PRead(metadata_sfd.get(), &info, sizeof(Info),
                                Info::kFileOffset));
  EXPECT_THAT(info.magic, Eq(Info::kMagic));
  EXPECT_THAT(info.num_keys, Eq(0));

  // Check crcs section
  Crcs crcs;
  ASSERT_TRUE(filesystem_.PRead(metadata_sfd.get(), &crcs, sizeof(Crcs),
                                Crcs::kFileOffset));
  // # of elements in sorted_buckets should be 1, so it should have non-zero
  // crc value.
  EXPECT_THAT(crcs.component_crcs.sorted_buckets_crc, Ne(0));
  // Other empty file backed vectors should have 0 crc value.
  EXPECT_THAT(crcs.component_crcs.unsorted_buckets_crc, Eq(0));
  EXPECT_THAT(crcs.component_crcs.flash_index_storage_crc, Eq(0));
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

TEST_F(IntegerIndexStorageTest,
       InitializationShouldFailWithoutPersistToDiskOrDestruction) {
  // Create new integer index storage
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<IntegerIndexStorage> storage,
      IntegerIndexStorage::Create(filesystem_, base_dir_, Options(),
                                  serializer_.get()));

  // Insert some data.
  ICING_ASSERT_OK(storage->AddKeys(/*document_id=*/0, /*section_id=*/20,
                                   /*new_keys=*/{0, 100, -100}));
  ICING_ASSERT_OK(storage->AddKeys(/*document_id=*/1, /*section_id=*/2,
                                   /*new_keys=*/{3, -1000, 500}));
  ICING_ASSERT_OK(storage->AddKeys(/*document_id=*/2, /*section_id=*/15,
                                   /*new_keys=*/{-6, 321, 98}));

  // Without calling PersistToDisk, checksums will not be recomputed or synced
  // to disk, so initializing another instance on the same files should fail.
  EXPECT_THAT(IntegerIndexStorage::Create(filesystem_, base_dir_, Options(),
                                          serializer_.get()),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(IntegerIndexStorageTest, InitializationShouldSucceedWithPersistToDisk) {
  // Create new integer index storage
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<IntegerIndexStorage> storage1,
      IntegerIndexStorage::Create(filesystem_, base_dir_, Options(),
                                  serializer_.get()));

  // Insert some data.
  ICING_ASSERT_OK(storage1->AddKeys(/*document_id=*/0, /*section_id=*/20,
                                    /*new_keys=*/{0, 100, -100}));
  ICING_ASSERT_OK(storage1->AddKeys(/*document_id=*/1, /*section_id=*/2,
                                    /*new_keys=*/{3, -1000, 500}));
  ICING_ASSERT_OK(storage1->AddKeys(/*document_id=*/2, /*section_id=*/15,
                                    /*new_keys=*/{-6, 321, 98}));
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<DocHitInfo> doc_hit_info_vec,
      Query(storage1.get(),
            /*key_lower=*/std::numeric_limits<int64_t>::min(),
            /*key_upper=*/std::numeric_limits<int64_t>::max()));

  // After calling PersistToDisk, all checksums should be recomputed and synced
  // correctly to disk, so initializing another instance on the same files
  // should succeed, and we should be able to get the same contents.
  ICING_EXPECT_OK(storage1->PersistToDisk());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<IntegerIndexStorage> storage2,
      IntegerIndexStorage::Create(filesystem_, base_dir_, Options(),
                                  serializer_.get()));
  EXPECT_THAT(
      Query(storage2.get(), /*key_lower=*/std::numeric_limits<int64_t>::min(),
            /*key_upper=*/std::numeric_limits<int64_t>::max()),
      IsOkAndHolds(
          ElementsAreArray(doc_hit_info_vec.begin(), doc_hit_info_vec.end())));
}

TEST_F(IntegerIndexStorageTest, InitializationShouldSucceedAfterDestruction) {
  std::vector<DocHitInfo> doc_hit_info_vec;
  {
    // Create new integer index storage
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<IntegerIndexStorage> storage,
        IntegerIndexStorage::Create(filesystem_, base_dir_, Options(),
                                    serializer_.get()));

    ICING_ASSERT_OK_AND_ASSIGN(
        doc_hit_info_vec,
        Query(storage.get(),
              /*key_lower=*/std::numeric_limits<int64_t>::min(),
              /*key_upper=*/std::numeric_limits<int64_t>::max()));
  }

  {
    // The previous instance went out of scope and was destructed. Although we
    // didn't call PersistToDisk explicitly, the destructor should invoke it and
    // thus initializing another instance on the same files should succeed, and
    // we should be able to get the same contents.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<IntegerIndexStorage> storage,
        IntegerIndexStorage::Create(filesystem_, base_dir_, Options(),
                                    serializer_.get()));
    EXPECT_THAT(
        Query(storage.get(), /*key_lower=*/std::numeric_limits<int64_t>::min(),
              /*key_upper=*/std::numeric_limits<int64_t>::max()),
        IsOkAndHolds(ElementsAreArray(doc_hit_info_vec.begin(),
                                      doc_hit_info_vec.end())));
  }
}

TEST_F(IntegerIndexStorageTest,
       InitializeExistingFilesWithWrongAllCrcShouldFail) {
  {
    // Create new integer index storage
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<IntegerIndexStorage> storage,
        IntegerIndexStorage::Create(filesystem_, base_dir_, Options(),
                                    serializer_.get()));
    ICING_ASSERT_OK(storage->AddKeys(kDefaultDocumentId, kDefaultSectionId,
                                     /*new_keys=*/{0, 100, -100}));

    ICING_ASSERT_OK(storage->PersistToDisk());
  }

  const std::string metadata_file_path =
      absl_ports::StrCat(base_dir_, "/", IntegerIndexStorage::kSubDirectory,
                         "/", IntegerIndexStorage::kFilePrefix, ".m");
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
    // Attempt to create the integer index storage with metadata containing
    // corrupted all_crc. This should fail.
    libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndexStorage>>
        storage_or = IntegerIndexStorage::Create(filesystem_, base_dir_,
                                                 Options(), serializer_.get());
    EXPECT_THAT(storage_or,
                StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
    EXPECT_THAT(storage_or.status().error_message(),
                HasSubstr("Invalid all crc for IntegerIndexStorage"));
  }
}

TEST_F(IntegerIndexStorageTest,
       InitializeExistingFilesWithCorruptedInfoShouldFail) {
  {
    // Create new integer index storage
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<IntegerIndexStorage> storage,
        IntegerIndexStorage::Create(filesystem_, base_dir_, Options(),
                                    serializer_.get()));
    ICING_ASSERT_OK(storage->AddKeys(kDefaultDocumentId, kDefaultSectionId,
                                     /*new_keys=*/{0, 100, -100}));

    ICING_ASSERT_OK(storage->PersistToDisk());
  }

  const std::string metadata_file_path =
      absl_ports::StrCat(base_dir_, "/", IntegerIndexStorage::kSubDirectory,
                         "/", IntegerIndexStorage::kFilePrefix, ".m");
  ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
  ASSERT_TRUE(metadata_sfd.is_valid());

  Info info;
  ASSERT_TRUE(filesystem_.PRead(metadata_sfd.get(), &info, sizeof(Info),
                                Info::kFileOffset));

  // Modify info, but don't update the checksum. This would be similar to
  // corruption of info.
  info.num_keys += kCorruptedValueOffset;
  ASSERT_TRUE(filesystem_.PWrite(metadata_sfd.get(), Info::kFileOffset, &info,
                                 sizeof(Info)));
  {
    // Attempt to create the integer index storage with info that doesn't match
    // its checksum and confirm that it fails.
    libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndexStorage>>
        storage_or = IntegerIndexStorage::Create(filesystem_, base_dir_,
                                                 Options(), serializer_.get());
    EXPECT_THAT(storage_or,
                StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
    EXPECT_THAT(storage_or.status().error_message(),
                HasSubstr("Invalid info crc for IntegerIndexStorage"));
  }
}

TEST_F(IntegerIndexStorageTest,
       InitializeExistingFilesWithWrongSortedBucketsCrcShouldFail) {
  {
    // Create new integer index storage
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<IntegerIndexStorage> storage,
        IntegerIndexStorage::Create(filesystem_, base_dir_, Options(),
                                    serializer_.get()));
    ICING_ASSERT_OK(storage->AddKeys(kDefaultDocumentId, kDefaultSectionId,
                                     /*new_keys=*/{0, 100, -100}));

    ICING_ASSERT_OK(storage->PersistToDisk());
  }

  const std::string metadata_file_path =
      absl_ports::StrCat(base_dir_, "/", IntegerIndexStorage::kSubDirectory,
                         "/", IntegerIndexStorage::kFilePrefix, ".m");
  ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
  ASSERT_TRUE(metadata_sfd.is_valid());

  Crcs crcs;
  ASSERT_TRUE(filesystem_.PRead(metadata_sfd.get(), &crcs, sizeof(Crcs),
                                Crcs::kFileOffset));

  // Manually corrupt sorted_buckets_crc
  crcs.component_crcs.sorted_buckets_crc += kCorruptedValueOffset;
  crcs.all_crc = crcs.component_crcs.ComputeChecksum().Get();
  ASSERT_TRUE(filesystem_.PWrite(metadata_sfd.get(), Crcs::kFileOffset, &crcs,
                                 sizeof(Crcs)));
  {
    // Attempt to create the integer index storage with metadata containing
    // corrupted sorted_buckets_crc. This should fail.
    libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndexStorage>>
        storage_or = IntegerIndexStorage::Create(filesystem_, base_dir_,
                                                 Options(), serializer_.get());
    EXPECT_THAT(storage_or,
                StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
    EXPECT_THAT(
        storage_or.status().error_message(),
        HasSubstr("Mismatch crc with IntegerIndexStorage sorted buckets"));
  }
}

TEST_F(IntegerIndexStorageTest,
       InitializeExistingFilesWithWrongUnsortedBucketsCrcShouldFail) {
  {
    // Create new integer index storage
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<IntegerIndexStorage> storage,
        IntegerIndexStorage::Create(filesystem_, base_dir_, Options(),
                                    serializer_.get()));
    ICING_ASSERT_OK(storage->AddKeys(kDefaultDocumentId, kDefaultSectionId,
                                     /*new_keys=*/{0, 100, -100}));

    ICING_ASSERT_OK(storage->PersistToDisk());
  }

  const std::string metadata_file_path =
      absl_ports::StrCat(base_dir_, "/", IntegerIndexStorage::kSubDirectory,
                         "/", IntegerIndexStorage::kFilePrefix, ".m");
  ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
  ASSERT_TRUE(metadata_sfd.is_valid());

  Crcs crcs;
  ASSERT_TRUE(filesystem_.PRead(metadata_sfd.get(), &crcs, sizeof(Crcs),
                                Crcs::kFileOffset));

  // Manually corrupt unsorted_buckets_crc
  crcs.component_crcs.unsorted_buckets_crc += kCorruptedValueOffset;
  crcs.all_crc = crcs.component_crcs.ComputeChecksum().Get();
  ASSERT_TRUE(filesystem_.PWrite(metadata_sfd.get(), Crcs::kFileOffset, &crcs,
                                 sizeof(Crcs)));
  {
    // Attempt to create the integer index storage with metadata containing
    // corrupted unsorted_buckets_crc. This should fail.
    libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndexStorage>>
        storage_or = IntegerIndexStorage::Create(filesystem_, base_dir_,
                                                 Options(), serializer_.get());
    EXPECT_THAT(storage_or,
                StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
    EXPECT_THAT(
        storage_or.status().error_message(),
        HasSubstr("Mismatch crc with IntegerIndexStorage unsorted buckets"));
  }
}

// TODO(b/259744228): add test for corrupted flash_index_storage_crc

TEST_F(IntegerIndexStorageTest, InvalidQuery) {
  // Create new integer index storage
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<IntegerIndexStorage> storage,
      IntegerIndexStorage::Create(filesystem_, base_dir_, Options(),
                                  serializer_.get()));
  EXPECT_THAT(
      storage->GetIterator(/*query_key_lower=*/0, /*query_key_upper=*/-1),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(IntegerIndexStorageTest, ExactQuerySortedBuckets) {
  // We use predefined custom buckets to initialize new integer index storage
  // and create some test keys accordingly.
  std::vector<Bucket> custom_init_sorted_buckets = {
      Bucket(-1000, -100), Bucket(0, 100), Bucket(150, 199), Bucket(200, 300),
      Bucket(301, 999)};
  std::vector<Bucket> custom_init_unsorted_buckets = {
      Bucket(1000, std::numeric_limits<int64_t>::max()), Bucket(-99, -1),
      Bucket(101, 149), Bucket(std::numeric_limits<int64_t>::min(), -1001)};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<IntegerIndexStorage> storage,
      IntegerIndexStorage::Create(
          filesystem_, base_dir_,
          Options(std::move(custom_init_sorted_buckets),
                  std::move(custom_init_unsorted_buckets)),
          serializer_.get()));

  // Add some keys into sorted buckets [(-1000,-100), (200,300)].
  EXPECT_THAT(storage->AddKeys(/*document_id=*/0, kDefaultSectionId,
                               /*new_keys=*/{-500}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/1, kDefaultSectionId,
                               /*new_keys=*/{208}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/2, kDefaultSectionId,
                               /*new_keys=*/{-200}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/3, kDefaultSectionId,
                               /*new_keys=*/{-1000}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/4, kDefaultSectionId,
                               /*new_keys=*/{300}),
              IsOk());

  std::vector<SectionId> expected_sections = {kDefaultSectionId};
  // Exact query on key in each sorted bucket should get the correct result.
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/-500, /*key_upper=*/-500),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/208, /*key_upper=*/208),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/-200, /*key_upper=*/-200),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/2, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/-1000, /*key_upper=*/-1000),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/3, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/300, /*key_upper=*/300),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/4, expected_sections))));
}

TEST_F(IntegerIndexStorageTest, ExactQueryUnsortedBuckets) {
  // We use predefined custom buckets to initialize new integer index storage
  // and create some test keys accordingly.
  std::vector<Bucket> custom_init_sorted_buckets = {
      Bucket(-1000, -100), Bucket(0, 100), Bucket(150, 199), Bucket(200, 300),
      Bucket(301, 999)};
  std::vector<Bucket> custom_init_unsorted_buckets = {
      Bucket(1000, std::numeric_limits<int64_t>::max()), Bucket(-99, -1),
      Bucket(101, 149), Bucket(std::numeric_limits<int64_t>::min(), -1001)};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<IntegerIndexStorage> storage,
      IntegerIndexStorage::Create(
          filesystem_, base_dir_,
          Options(std::move(custom_init_sorted_buckets),
                  std::move(custom_init_unsorted_buckets)),
          serializer_.get()));

  // Add some keys into unsorted buckets [(1000,INT64_MAX), (INT64_MIN,-1001)].
  EXPECT_THAT(storage->AddKeys(/*document_id=*/0, kDefaultSectionId,
                               /*new_keys=*/{1024}),
              IsOk());
  EXPECT_THAT(
      storage->AddKeys(/*document_id=*/1, kDefaultSectionId,
                       /*new_keys=*/{std::numeric_limits<int64_t>::max()}),
      IsOk());
  EXPECT_THAT(
      storage->AddKeys(/*document_id=*/2, kDefaultSectionId,
                       /*new_keys=*/{std::numeric_limits<int64_t>::min()}),
      IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/3, kDefaultSectionId,
                               /*new_keys=*/{-1500}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/4, kDefaultSectionId,
                               /*new_keys=*/{2000}),
              IsOk());

  std::vector<SectionId> expected_sections = {kDefaultSectionId};
  // Exact query on key in each unsorted bucket should get the correct result.
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/1024, /*key_upper=*/1024),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
  EXPECT_THAT(
      Query(storage.get(), /*key_lower=*/std::numeric_limits<int64_t>::max(),
            /*key_upper=*/std::numeric_limits<int64_t>::max()),
      IsOkAndHolds(
          ElementsAre(EqualsDocHitInfo(/*document_id=*/1, expected_sections))));
  EXPECT_THAT(
      Query(storage.get(), /*key_lower=*/std::numeric_limits<int64_t>::min(),
            /*key_upper=*/std::numeric_limits<int64_t>::min()),
      IsOkAndHolds(
          ElementsAre(EqualsDocHitInfo(/*document_id=*/2, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/-1500, /*key_upper=*/-1500),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/3, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/2000, /*key_upper=*/2000),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/4, expected_sections))));
}

TEST_F(IntegerIndexStorageTest, ExactQueryIdenticalKeys) {
  // We use predefined custom buckets to initialize new integer index storage
  // and create some test keys accordingly.
  std::vector<Bucket> custom_init_sorted_buckets = {
      Bucket(-1000, -100), Bucket(0, 100), Bucket(150, 199), Bucket(200, 300),
      Bucket(301, 999)};
  std::vector<Bucket> custom_init_unsorted_buckets = {
      Bucket(1000, std::numeric_limits<int64_t>::max()), Bucket(-99, -1),
      Bucket(101, 149), Bucket(std::numeric_limits<int64_t>::min(), -1001)};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<IntegerIndexStorage> storage,
      IntegerIndexStorage::Create(
          filesystem_, base_dir_,
          Options(std::move(custom_init_sorted_buckets),
                  std::move(custom_init_unsorted_buckets)),
          serializer_.get()));

  // Add some keys into buckets [(0,100), (1000,INT64_MAX)].
  EXPECT_THAT(storage->AddKeys(/*document_id=*/0, kDefaultSectionId,
                               /*new_keys=*/{1024}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/1, kDefaultSectionId,
                               /*new_keys=*/{1024}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/2, kDefaultSectionId,
                               /*new_keys=*/{20}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/3, kDefaultSectionId,
                               /*new_keys=*/{20}),
              IsOk());

  std::vector<SectionId> expected_sections = {kDefaultSectionId};
  // Exact query on key with multiple hits should get the correct result.
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/1024, /*key_upper=*/1024),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/20, /*key_upper=*/20),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/3, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/2, expected_sections))));
}

TEST_F(IntegerIndexStorageTest, RangeQueryEmptyIntegerIndexStorage) {
  std::vector<Bucket> custom_init_sorted_buckets = {
      Bucket(-1000, -100), Bucket(0, 100), Bucket(150, 199), Bucket(200, 300),
      Bucket(301, 999)};
  std::vector<Bucket> custom_init_unsorted_buckets = {
      Bucket(1000, std::numeric_limits<int64_t>::max()), Bucket(-99, -1),
      Bucket(101, 149), Bucket(std::numeric_limits<int64_t>::min(), -1001)};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<IntegerIndexStorage> storage,
      IntegerIndexStorage::Create(
          filesystem_, base_dir_,
          Options(std::move(custom_init_sorted_buckets),
                  std::move(custom_init_unsorted_buckets)),
          serializer_.get()));

  EXPECT_THAT(
      Query(storage.get(), /*key_lower=*/std::numeric_limits<int64_t>::min(),
            /*key_upper=*/std::numeric_limits<int64_t>::max()),
      IsOkAndHolds(IsEmpty()));
}

TEST_F(IntegerIndexStorageTest, RangeQuerySingleEntireSortedBucket) {
  // We use predefined custom buckets to initialize new integer index storage
  // and create some test keys accordingly.
  std::vector<Bucket> custom_init_sorted_buckets = {
      Bucket(-1000, -100), Bucket(0, 100), Bucket(150, 199), Bucket(200, 300),
      Bucket(301, 999)};
  std::vector<Bucket> custom_init_unsorted_buckets = {
      Bucket(1000, std::numeric_limits<int64_t>::max()), Bucket(-99, -1),
      Bucket(101, 149), Bucket(std::numeric_limits<int64_t>::min(), -1001)};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<IntegerIndexStorage> storage,
      IntegerIndexStorage::Create(
          filesystem_, base_dir_,
          Options(std::move(custom_init_sorted_buckets),
                  std::move(custom_init_unsorted_buckets)),
          serializer_.get()));

  // Add some keys into sorted buckets [(-1000,-100), (200,300)].
  EXPECT_THAT(storage->AddKeys(/*document_id=*/0, kDefaultSectionId,
                               /*new_keys=*/{-500}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/1, kDefaultSectionId,
                               /*new_keys=*/{208}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/2, kDefaultSectionId,
                               /*new_keys=*/{-200}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/3, kDefaultSectionId,
                               /*new_keys=*/{-1000}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/4, kDefaultSectionId,
                               /*new_keys=*/{300}),
              IsOk());

  std::vector<SectionId> expected_sections = {kDefaultSectionId};
  // Range query on each sorted bucket boundary should get the correct result.
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/-1000, /*key_upper=*/-100),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/3, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/2, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/0, /*key_upper=*/100),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/150, /*key_upper=*/199),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/200, /*key_upper=*/300),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/4, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/301, /*key_upper=*/999),
              IsOkAndHolds(IsEmpty()));
}

TEST_F(IntegerIndexStorageTest, RangeQuerySingleEntireUnsortedBucket) {
  // We use predefined custom buckets to initialize new integer index storage
  // and create some test keys accordingly.
  std::vector<Bucket> custom_init_sorted_buckets = {
      Bucket(-1000, -100), Bucket(0, 100), Bucket(150, 199), Bucket(200, 300),
      Bucket(301, 999)};
  std::vector<Bucket> custom_init_unsorted_buckets = {
      Bucket(1000, std::numeric_limits<int64_t>::max()), Bucket(-99, -1),
      Bucket(101, 149), Bucket(std::numeric_limits<int64_t>::min(), -1001)};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<IntegerIndexStorage> storage,
      IntegerIndexStorage::Create(
          filesystem_, base_dir_,
          Options(std::move(custom_init_sorted_buckets),
                  std::move(custom_init_unsorted_buckets)),
          serializer_.get()));

  // Add some keys into unsorted buckets [(1000,INT64_MAX), (INT64_MIN,-1001)].
  EXPECT_THAT(storage->AddKeys(/*document_id=*/0, kDefaultSectionId,
                               /*new_keys=*/{1024}),
              IsOk());
  EXPECT_THAT(
      storage->AddKeys(/*document_id=*/1, kDefaultSectionId,
                       /*new_keys=*/{std::numeric_limits<int64_t>::max()}),
      IsOk());
  EXPECT_THAT(
      storage->AddKeys(/*document_id=*/2, kDefaultSectionId,
                       /*new_keys=*/{std::numeric_limits<int64_t>::min()}),
      IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/3, kDefaultSectionId,
                               /*new_keys=*/{-1500}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/4, kDefaultSectionId,
                               /*new_keys=*/{2000}),
              IsOk());

  std::vector<SectionId> expected_sections = {kDefaultSectionId};
  // Range query on each unsorted bucket boundary should get the correct result.
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/1000,
                    /*key_upper=*/std::numeric_limits<int64_t>::max()),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/4, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/-99, /*key_upper=*/-1),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/101, /*key_upper=*/149),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(
      Query(storage.get(), /*key_lower=*/std::numeric_limits<int64_t>::min(),
            /*key_upper=*/-1001),
      IsOkAndHolds(
          ElementsAre(EqualsDocHitInfo(/*document_id=*/3, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/2, expected_sections))));
}

TEST_F(IntegerIndexStorageTest, RangeQuerySinglePartialSortedBucket) {
  // We use predefined custom buckets to initialize new integer index storage
  // and create some test keys accordingly.
  std::vector<Bucket> custom_init_sorted_buckets = {
      Bucket(-1000, -100), Bucket(0, 100), Bucket(150, 199), Bucket(200, 300),
      Bucket(301, 999)};
  std::vector<Bucket> custom_init_unsorted_buckets = {
      Bucket(1000, std::numeric_limits<int64_t>::max()), Bucket(-99, -1),
      Bucket(101, 149), Bucket(std::numeric_limits<int64_t>::min(), -1001)};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<IntegerIndexStorage> storage,
      IntegerIndexStorage::Create(
          filesystem_, base_dir_,
          Options(std::move(custom_init_sorted_buckets),
                  std::move(custom_init_unsorted_buckets)),
          serializer_.get()));

  // Add some keys into sorted bucket (0,100).
  EXPECT_THAT(storage->AddKeys(/*document_id=*/0, kDefaultSectionId,
                               /*new_keys=*/{43}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/1, kDefaultSectionId,
                               /*new_keys=*/{30}),
              IsOk());

  std::vector<SectionId> expected_sections = {kDefaultSectionId};
  // Range query on partial range of each sorted bucket should get the correct
  // result.
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/25, /*key_upper=*/200),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/-1000, /*key_upper=*/49),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/25, /*key_upper=*/49),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/31, /*key_upper=*/49),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/25, /*key_upper=*/31),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/3, /*key_upper=*/5),
              IsOkAndHolds(IsEmpty()));
}

TEST_F(IntegerIndexStorageTest, RangeQuerySinglePartialUnsortedBucket) {
  // We use predefined custom buckets to initialize new integer index storage
  // and create some test keys accordingly.
  std::vector<Bucket> custom_init_sorted_buckets = {
      Bucket(-1000, -100), Bucket(0, 100), Bucket(150, 199), Bucket(200, 300),
      Bucket(301, 999)};
  std::vector<Bucket> custom_init_unsorted_buckets = {
      Bucket(1000, std::numeric_limits<int64_t>::max()), Bucket(-99, -1),
      Bucket(101, 149), Bucket(std::numeric_limits<int64_t>::min(), -1001)};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<IntegerIndexStorage> storage,
      IntegerIndexStorage::Create(
          filesystem_, base_dir_,
          Options(std::move(custom_init_sorted_buckets),
                  std::move(custom_init_unsorted_buckets)),
          serializer_.get()));

  // Add some keys into unsorted buckets (-99,-1).
  EXPECT_THAT(storage->AddKeys(/*document_id=*/0, kDefaultSectionId,
                               /*new_keys=*/{-19}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/1, kDefaultSectionId,
                               /*new_keys=*/{-72}),
              IsOk());

  std::vector<SectionId> expected_sections = {kDefaultSectionId};
  // Range query on partial range of each unsorted bucket should get the correct
  // result.
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/-1000, /*key_upper=*/-15),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/-80, /*key_upper=*/149),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/-80, /*key_upper=*/-15),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/-38, /*key_upper=*/-15),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/-80, /*key_upper=*/-38),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/-95, /*key_upper=*/-92),
              IsOkAndHolds(IsEmpty()));
}

TEST_F(IntegerIndexStorageTest, RangeQueryMultipleBuckets) {
  // We use predefined custom buckets to initialize new integer index storage
  // and create some test keys accordingly.
  std::vector<Bucket> custom_init_sorted_buckets = {
      Bucket(-1000, -100), Bucket(0, 100), Bucket(150, 199), Bucket(200, 300),
      Bucket(301, 999)};
  std::vector<Bucket> custom_init_unsorted_buckets = {
      Bucket(1000, std::numeric_limits<int64_t>::max()), Bucket(-99, -1),
      Bucket(101, 149), Bucket(std::numeric_limits<int64_t>::min(), -1001)};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<IntegerIndexStorage> storage,
      IntegerIndexStorage::Create(
          filesystem_, base_dir_,
          Options(std::move(custom_init_sorted_buckets),
                  std::move(custom_init_unsorted_buckets)),
          serializer_.get()));

  // Add some keys into buckets [(-1000,-100), (200,300), (1000,INT64_MAX),
  // (INT64_MIN,-1001)]
  EXPECT_THAT(storage->AddKeys(/*document_id=*/0, kDefaultSectionId,
                               /*new_keys=*/{-500}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/1, kDefaultSectionId,
                               /*new_keys=*/{1024}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/2, kDefaultSectionId,
                               /*new_keys=*/{-200}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/3, kDefaultSectionId,
                               /*new_keys=*/{208}),
              IsOk());
  EXPECT_THAT(
      storage->AddKeys(/*document_id=*/4, kDefaultSectionId,
                       /*new_keys=*/{std::numeric_limits<int64_t>::max()}),
      IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/5, kDefaultSectionId,
                               /*new_keys=*/{-1000}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/6, kDefaultSectionId,
                               /*new_keys=*/{300}),
              IsOk());
  EXPECT_THAT(
      storage->AddKeys(/*document_id=*/7, kDefaultSectionId,
                       /*new_keys=*/{std::numeric_limits<int64_t>::min()}),
      IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/8, kDefaultSectionId,
                               /*new_keys=*/{-1500}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(/*document_id=*/9, kDefaultSectionId,
                               /*new_keys=*/{2000}),
              IsOk());

  std::vector<SectionId> expected_sections = {kDefaultSectionId};
  // Range query should get the correct result.
  EXPECT_THAT(
      Query(storage.get(), /*key_lower=*/std::numeric_limits<int64_t>::min(),
            /*key_upper=*/std::numeric_limits<int64_t>::max()),
      IsOkAndHolds(
          ElementsAre(EqualsDocHitInfo(/*document_id=*/9, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/8, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/7, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/6, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/5, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/4, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/3, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/2, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/1, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
  EXPECT_THAT(Query(storage.get(), /*key_lower=*/-199,
                    /*key_upper=*/std::numeric_limits<int64_t>::max()),
              IsOkAndHolds(ElementsAre(
                  EqualsDocHitInfo(/*document_id=*/9, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/6, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/4, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/3, expected_sections),
                  EqualsDocHitInfo(/*document_id=*/1, expected_sections))));
  EXPECT_THAT(
      Query(storage.get(), /*key_lower=*/std::numeric_limits<int64_t>::min(),
            /*key_upper=*/-200),
      IsOkAndHolds(
          ElementsAre(EqualsDocHitInfo(/*document_id=*/8, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/7, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/5, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/2, expected_sections),
                      EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
}

TEST_F(IntegerIndexStorageTest, BatchAdd) {
  // We use predefined custom buckets to initialize new integer index storage
  // and create some test keys accordingly.
  std::vector<Bucket> custom_init_sorted_buckets = {
      Bucket(-1000, -100), Bucket(0, 100), Bucket(150, 199), Bucket(200, 300),
      Bucket(301, 999)};
  std::vector<Bucket> custom_init_unsorted_buckets = {
      Bucket(1000, std::numeric_limits<int64_t>::max()), Bucket(-99, -1),
      Bucket(101, 149), Bucket(std::numeric_limits<int64_t>::min(), -1001)};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<IntegerIndexStorage> storage,
      IntegerIndexStorage::Create(
          filesystem_, base_dir_,
          Options(std::move(custom_init_sorted_buckets),
                  std::move(custom_init_unsorted_buckets)),
          serializer_.get()));

  // Sorted buckets: [(-1000,-100), (0,100), (150,199), (200,300), (301,999)]
  // Unsorted buckets: [(1000,INT64_MAX), (-99,-1), (101,149),
  //                    (INT64_MIN,-1001)]
  // Batch add the following keys (including some edge cases) to test the
  // correctness of the sort and binary search logic in AddKeys().
  // clang-format off
  std::vector<int64_t> keys = {4000, 3000, 2000,  300,   201,   200,  106, 104,
                               100,  3,    2,     1,     0,     -97,  -98, -99,
                               -100, -200, -1000, -1001, -1500, -2000,
                               std::numeric_limits<int64_t>::max(),
                               std::numeric_limits<int64_t>::min()};
  // clang-format on
  EXPECT_THAT(storage->AddKeys(kDefaultDocumentId, kDefaultSectionId,
                               std::vector<int64_t>(keys)),
              IsOk());

  std::vector<SectionId> expected_sections = {kDefaultSectionId};
  for (int64_t key : keys) {
    EXPECT_THAT(Query(storage.get(), /*key_lower=*/key, /*key_upper=*/key),
                IsOkAndHolds(ElementsAre(
                    EqualsDocHitInfo(kDefaultDocumentId, expected_sections))));
  }
}

TEST_F(IntegerIndexStorageTest, MultipleKeysShouldMergeAndDedupeDocHitInfo) {
  // We use predefined custom buckets to initialize new integer index storage
  // and create some test keys accordingly.
  std::vector<Bucket> custom_init_sorted_buckets = {
      Bucket(-1000, -100), Bucket(0, 100), Bucket(150, 199), Bucket(200, 300),
      Bucket(301, 999)};
  std::vector<Bucket> custom_init_unsorted_buckets = {
      Bucket(1000, std::numeric_limits<int64_t>::max()), Bucket(-99, -1),
      Bucket(101, 149), Bucket(std::numeric_limits<int64_t>::min(), -1001)};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<IntegerIndexStorage> storage,
      IntegerIndexStorage::Create(
          filesystem_, base_dir_,
          Options(std::move(custom_init_sorted_buckets),
                  std::move(custom_init_unsorted_buckets)),
          serializer_.get()));

  // Add some keys with same document id and section id.
  EXPECT_THAT(
      storage->AddKeys(
          /*document_id=*/0, kDefaultSectionId, /*new_keys=*/
          {-500, 1024, -200, 208, std::numeric_limits<int64_t>::max(), -1000,
           300, std::numeric_limits<int64_t>::min(), -1500, 2000}),
      IsOk());

  std::vector<SectionId> expected_sections = {kDefaultSectionId};
  EXPECT_THAT(
      Query(storage.get(), /*key_lower=*/std::numeric_limits<int64_t>::min(),
            /*key_upper=*/std::numeric_limits<int64_t>::max()),
      IsOkAndHolds(
          ElementsAre(EqualsDocHitInfo(/*document_id=*/0, expected_sections))));
}

TEST_F(IntegerIndexStorageTest,
       MultipleSectionsShouldMergeSectionsAndDedupeDocHitInfo) {
  // We use predefined custom buckets to initialize new integer index storage
  // and create some test keys accordingly.
  std::vector<Bucket> custom_init_sorted_buckets = {
      Bucket(-1000, -100), Bucket(0, 100), Bucket(150, 199), Bucket(200, 300),
      Bucket(301, 999)};
  std::vector<Bucket> custom_init_unsorted_buckets = {
      Bucket(1000, std::numeric_limits<int64_t>::max()), Bucket(-99, -1),
      Bucket(101, 149), Bucket(std::numeric_limits<int64_t>::min(), -1001)};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<IntegerIndexStorage> storage,
      IntegerIndexStorage::Create(
          filesystem_, base_dir_,
          Options(std::move(custom_init_sorted_buckets),
                  std::move(custom_init_unsorted_buckets)),
          serializer_.get()));

  // Add some keys with same document id but different section ids.
  EXPECT_THAT(storage->AddKeys(kDefaultDocumentId, /*section_id=*/63,
                               /*new_keys=*/{-500}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(kDefaultDocumentId, /*section_id=*/62,
                               /*new_keys=*/{1024}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(kDefaultDocumentId, /*section_id=*/61,
                               /*new_keys=*/{-200}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(kDefaultDocumentId, /*section_id=*/60,
                               /*new_keys=*/{208}),
              IsOk());
  EXPECT_THAT(
      storage->AddKeys(kDefaultDocumentId, /*section_id=*/59,
                       /*new_keys=*/{std::numeric_limits<int64_t>::max()}),
      IsOk());
  EXPECT_THAT(storage->AddKeys(kDefaultDocumentId, /*section_id=*/58,
                               /*new_keys=*/{-1000}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(kDefaultDocumentId, /*section_id=*/57,
                               /*new_keys=*/{300}),
              IsOk());
  EXPECT_THAT(
      storage->AddKeys(kDefaultDocumentId, /*section_id=*/56,
                       /*new_keys=*/{std::numeric_limits<int64_t>::min()}),
      IsOk());
  EXPECT_THAT(storage->AddKeys(kDefaultDocumentId, /*section_id=*/55,
                               /*new_keys=*/{-1500}),
              IsOk());
  EXPECT_THAT(storage->AddKeys(kDefaultDocumentId, /*section_id=*/54,
                               /*new_keys=*/{2000}),
              IsOk());

  std::vector<SectionId> expected_sections = {63, 62, 61, 60, 59,
                                              58, 57, 56, 55, 54};
  EXPECT_THAT(
      Query(storage.get(), /*key_lower=*/std::numeric_limits<int64_t>::min(),
            /*key_upper=*/std::numeric_limits<int64_t>::max()),
      IsOkAndHolds(ElementsAre(
          EqualsDocHitInfo(kDefaultDocumentId, expected_sections))));
}

}  // namespace

}  // namespace lib
}  // namespace icing
