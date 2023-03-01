// Copyright (C) 2023 Google LLC
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

#include "icing/join/qualified-id-type-joinable-cache.h"

#include <memory>
#include <string>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/file/filesystem.h"
#include "icing/file/persistent-storage.h"
#include "icing/join/doc-join-info.h"
#include "icing/store/document-id.h"
#include "icing/store/persistent-hash-map-key-mapper.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/crc32.h"

namespace icing {
namespace lib {

namespace {

using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsTrue;
using ::testing::Ne;
using ::testing::Not;

using Crcs = PersistentStorage::Crcs;
using Info = QualifiedIdTypeJoinableCache::Info;

static constexpr int32_t kCorruptedValueOffset = 3;

class QualifiedIdTypeJoinableCacheTest : public ::testing::Test {
 protected:
  void SetUp() override {
    base_dir_ = GetTestTempDir() + "/icing";
    ASSERT_THAT(filesystem_.CreateDirectoryRecursively(base_dir_.c_str()),
                IsTrue());

    working_path_ = base_dir_ + "/qualified_id_type_joinable_cache_test";
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(base_dir_.c_str());
  }

  Filesystem filesystem_;
  std::string base_dir_;
  std::string working_path_;
};

TEST_F(QualifiedIdTypeJoinableCacheTest, InvalidWorkingPath) {
  EXPECT_THAT(
      QualifiedIdTypeJoinableCache::Create(
          filesystem_, "/dev/null/qualified_id_type_joinable_cache_test"),
      StatusIs(libtextclassifier3::StatusCode::INTERNAL));
}

TEST_F(QualifiedIdTypeJoinableCacheTest, InitializeNewFiles) {
  {
    // Create new qualified id type joinable cache
    ASSERT_FALSE(filesystem_.DirectoryExists(working_path_.c_str()));
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdTypeJoinableCache> cache,
        QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_));

    ICING_ASSERT_OK(cache->PersistToDisk());
  }

  // Metadata file should be initialized correctly for both info and crcs
  // sections.
  const std::string metadata_file_path = absl_ports::StrCat(
      working_path_, "/", QualifiedIdTypeJoinableCache::kFilePrefix, ".m");
  auto metadata_buffer = std::make_unique<uint8_t[]>(
      QualifiedIdTypeJoinableCache::kMetadataFileSize);
  ASSERT_THAT(
      filesystem_.PRead(metadata_file_path.c_str(), metadata_buffer.get(),
                        QualifiedIdTypeJoinableCache::kMetadataFileSize,
                        /*offset=*/0),
      IsTrue());

  // Check info section
  const Info* info = reinterpret_cast<const Info*>(
      metadata_buffer.get() +
      QualifiedIdTypeJoinableCache::kInfoMetadataBufferOffset);
  EXPECT_THAT(info->magic, Eq(Info::kMagic));
  EXPECT_THAT(info->last_added_document_id, Eq(kInvalidDocumentId));

  // Check crcs section
  const Crcs* crcs = reinterpret_cast<const Crcs*>(
      metadata_buffer.get() +
      QualifiedIdTypeJoinableCache::kCrcsMetadataBufferOffset);
  // There are some initial info in KeyMapper, so storages_crc should be
  // non-zero.
  EXPECT_THAT(crcs->component_crcs.storages_crc, Ne(0));
  EXPECT_THAT(crcs->component_crcs.info_crc,
              Eq(Crc32(std::string_view(reinterpret_cast<const char*>(info),
                                        sizeof(Info)))
                     .Get()));
  EXPECT_THAT(crcs->all_crc,
              Eq(Crc32(std::string_view(
                           reinterpret_cast<const char*>(&crcs->component_crcs),
                           sizeof(Crcs::ComponentCrcs)))
                     .Get()));
}

TEST_F(QualifiedIdTypeJoinableCacheTest,
       InitializationShouldFailWithoutPersistToDiskOrDestruction) {
  // Create new qualified id type joinable cache
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QualifiedIdTypeJoinableCache> cache,
      QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_));

  // Insert some data.
  ICING_ASSERT_OK(
      cache->Put(DocJoinInfo(/*document_id=*/1, /*joinable_property_id=*/20),
                 /*ref_document_id=*/0));
  ICING_ASSERT_OK(
      cache->Put(DocJoinInfo(/*document_id=*/3, /*joinable_property_id=*/20),
                 /*ref_document_id=*/2));
  ICING_ASSERT_OK(
      cache->Put(DocJoinInfo(/*document_id=*/5, /*joinable_property_id=*/20),
                 /*ref_document_id=*/4));

  // Without calling PersistToDisk, checksums will not be recomputed or synced
  // to disk, so initializing another instance on the same files should fail.
  EXPECT_THAT(QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(QualifiedIdTypeJoinableCacheTest,
       InitializationShouldSucceedWithPersistToDisk) {
  // Create new qualified id type joinable cache
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QualifiedIdTypeJoinableCache> cache1,
      QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_));

  // Insert some data.
  ICING_ASSERT_OK(
      cache1->Put(DocJoinInfo(/*document_id=*/1, /*joinable_property_id=*/20),
                  /*ref_document_id=*/0));
  ICING_ASSERT_OK(
      cache1->Put(DocJoinInfo(/*document_id=*/3, /*joinable_property_id=*/20),
                  /*ref_document_id=*/2));
  ICING_ASSERT_OK(
      cache1->Put(DocJoinInfo(/*document_id=*/5, /*joinable_property_id=*/20),
                  /*ref_document_id=*/4));

  // After calling PersistToDisk, all checksums should be recomputed and synced
  // correctly to disk, so initializing another instance on the same files
  // should succeed, and we should be able to get the same contents.
  ICING_EXPECT_OK(cache1->PersistToDisk());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QualifiedIdTypeJoinableCache> cache2,
      QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_));
  EXPECT_THAT(
      cache2->Get(DocJoinInfo(/*document_id=*/1, /*joinable_property_id=*/20)),
      IsOkAndHolds(0));
  EXPECT_THAT(
      cache2->Get(DocJoinInfo(/*document_id=*/3, /*joinable_property_id=*/20)),
      IsOkAndHolds(2));
  EXPECT_THAT(
      cache2->Get(DocJoinInfo(/*document_id=*/5, /*joinable_property_id=*/20)),
      IsOkAndHolds(4));
}

TEST_F(QualifiedIdTypeJoinableCacheTest,
       InitializationShouldSucceedAfterDestruction) {
  {
    // Create new qualified id type joinable cache
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdTypeJoinableCache> cache,
        QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_));

    // Insert some data.
    ICING_ASSERT_OK(
        cache->Put(DocJoinInfo(/*document_id=*/1, /*joinable_property_id=*/20),
                   /*ref_document_id=*/0));
    ICING_ASSERT_OK(
        cache->Put(DocJoinInfo(/*document_id=*/3, /*joinable_property_id=*/20),
                   /*ref_document_id=*/2));
    ICING_ASSERT_OK(
        cache->Put(DocJoinInfo(/*document_id=*/5, /*joinable_property_id=*/20),
                   /*ref_document_id=*/4));
  }

  {
    // The previous instance went out of scope and was destructed. Although we
    // didn't call PersistToDisk explicitly, the destructor should invoke it and
    // thus initializing another instance on the same files should succeed, and
    // we should be able to get the same contents.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdTypeJoinableCache> cache,
        QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_));
    EXPECT_THAT(cache->Get(DocJoinInfo(/*document_id=*/1,
                                       /*joinable_property_id=*/20)),
                IsOkAndHolds(0));
    EXPECT_THAT(cache->Get(DocJoinInfo(/*document_id=*/3,
                                       /*joinable_property_id=*/20)),
                IsOkAndHolds(2));
    EXPECT_THAT(cache->Get(DocJoinInfo(/*document_id=*/5,
                                       /*joinable_property_id=*/20)),
                IsOkAndHolds(4));
  }
}

TEST_F(QualifiedIdTypeJoinableCacheTest,
       InitializeExistingFilesWithDifferentMagicShouldFail) {
  {
    // Create new qualified id type joinable cache
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdTypeJoinableCache> cache,
        QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_));
    ICING_ASSERT_OK(
        cache->Put(DocJoinInfo(/*document_id=*/1, /*joinable_property_id=*/20),
                   /*ref_document_id=*/0));

    ICING_ASSERT_OK(cache->PersistToDisk());
  }

  {
    // Manually change magic and update checksum
    const std::string metadata_file_path = absl_ports::StrCat(
        working_path_, "/", QualifiedIdTypeJoinableCache::kFilePrefix, ".m");
    ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
    ASSERT_THAT(metadata_sfd.is_valid(), IsTrue());

    auto metadata_buffer = std::make_unique<uint8_t[]>(
        QualifiedIdTypeJoinableCache::kMetadataFileSize);
    ASSERT_THAT(
        filesystem_.PRead(metadata_sfd.get(), metadata_buffer.get(),
                          QualifiedIdTypeJoinableCache::kMetadataFileSize,
                          /*offset=*/0),
        IsTrue());

    // Manually change magic and update checksums.
    Crcs* crcs = reinterpret_cast<Crcs*>(
        metadata_buffer.get() +
        QualifiedIdTypeJoinableCache::kCrcsMetadataBufferOffset);
    Info* info = reinterpret_cast<Info*>(
        metadata_buffer.get() +
        QualifiedIdTypeJoinableCache::kInfoMetadataBufferOffset);
    info->magic += kCorruptedValueOffset;
    crcs->component_crcs.info_crc = info->ComputeChecksum().Get();
    crcs->all_crc = crcs->component_crcs.ComputeChecksum().Get();
    ASSERT_THAT(filesystem_.PWrite(
                    metadata_sfd.get(), /*offset=*/0, metadata_buffer.get(),
                    QualifiedIdTypeJoinableCache::kMetadataFileSize),
                IsTrue());
  }

  // Attempt to create the qualified id type joinable cache with different
  // magic. This should fail.
  EXPECT_THAT(QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION,
                       HasSubstr("Incorrect magic value")));
}

TEST_F(QualifiedIdTypeJoinableCacheTest,
       InitializeExistingFilesWithWrongAllCrcShouldFail) {
  {
    // Create new qualified id type joinable cache
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdTypeJoinableCache> cache,
        QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_));
    ICING_ASSERT_OK(
        cache->Put(DocJoinInfo(/*document_id=*/1, /*joinable_property_id=*/20),
                   /*ref_document_id=*/0));

    ICING_ASSERT_OK(cache->PersistToDisk());
  }

  {
    const std::string metadata_file_path = absl_ports::StrCat(
        working_path_, "/", QualifiedIdTypeJoinableCache::kFilePrefix, ".m");
    ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
    ASSERT_THAT(metadata_sfd.is_valid(), IsTrue());

    auto metadata_buffer = std::make_unique<uint8_t[]>(
        QualifiedIdTypeJoinableCache::kMetadataFileSize);
    ASSERT_THAT(
        filesystem_.PRead(metadata_sfd.get(), metadata_buffer.get(),
                          QualifiedIdTypeJoinableCache::kMetadataFileSize,
                          /*offset=*/0),
        IsTrue());

    // Manually corrupt all_crc
    Crcs* crcs = reinterpret_cast<Crcs*>(
        metadata_buffer.get() +
        QualifiedIdTypeJoinableCache::kCrcsMetadataBufferOffset);
    crcs->all_crc += kCorruptedValueOffset;

    ASSERT_THAT(filesystem_.PWrite(
                    metadata_sfd.get(), /*offset=*/0, metadata_buffer.get(),
                    QualifiedIdTypeJoinableCache::kMetadataFileSize),
                IsTrue());
  }

  // Attempt to create the qualified id type joinable cache with metadata
  // containing corrupted all_crc. This should fail.
  EXPECT_THAT(QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION,
                       HasSubstr("Invalid all crc")));
}

TEST_F(QualifiedIdTypeJoinableCacheTest,
       InitializeExistingFilesWithCorruptedInfoShouldFail) {
  {
    // Create new qualified id type joinable cache
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdTypeJoinableCache> cache,
        QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_));
    ICING_ASSERT_OK(
        cache->Put(DocJoinInfo(/*document_id=*/1, /*joinable_property_id=*/20),
                   /*ref_document_id=*/0));

    ICING_ASSERT_OK(cache->PersistToDisk());
  }

  {
    const std::string metadata_file_path = absl_ports::StrCat(
        working_path_, "/", QualifiedIdTypeJoinableCache::kFilePrefix, ".m");
    ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
    ASSERT_THAT(metadata_sfd.is_valid(), IsTrue());

    auto metadata_buffer = std::make_unique<uint8_t[]>(
        QualifiedIdTypeJoinableCache::kMetadataFileSize);
    ASSERT_THAT(
        filesystem_.PRead(metadata_sfd.get(), metadata_buffer.get(),
                          QualifiedIdTypeJoinableCache::kMetadataFileSize,
                          /*offset=*/0),
        IsTrue());

    // Modify info, but don't update the checksum. This would be similar to
    // corruption of info.
    Info* info = reinterpret_cast<Info*>(
        metadata_buffer.get() +
        QualifiedIdTypeJoinableCache::kInfoMetadataBufferOffset);
    info->last_added_document_id += kCorruptedValueOffset;

    ASSERT_THAT(filesystem_.PWrite(
                    metadata_sfd.get(), /*offset=*/0, metadata_buffer.get(),
                    QualifiedIdTypeJoinableCache::kMetadataFileSize),
                IsTrue());
  }

  // Attempt to create the qualified id type joinable cache with info that
  // doesn't match its checksum. This should fail.
  EXPECT_THAT(QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION,
                       HasSubstr("Invalid info crc")));
}

TEST_F(
    QualifiedIdTypeJoinableCacheTest,
    InitializeExistingFilesWithCorruptedDocumentToQualifiedIdMapperShouldFail) {
  {
    // Create new qualified id type joinable cache
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdTypeJoinableCache> cache,
        QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_));
    ICING_ASSERT_OK(
        cache->Put(DocJoinInfo(/*document_id=*/1, /*joinable_property_id=*/20),
                   /*ref_document_id=*/0));

    ICING_ASSERT_OK(cache->PersistToDisk());
  }

  {
    // Corrupt document_to_qualified_id_mapper manually.
    std::string mapper_working_path = absl_ports::StrCat(
        working_path_, "/", QualifiedIdTypeJoinableCache::kFilePrefix,
        "_mapper");
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<PersistentHashMapKeyMapper<DocumentId>> mapper,
        PersistentHashMapKeyMapper<DocumentId>::Create(
            filesystem_, std::move(mapper_working_path)));
    ICING_ASSERT_OK_AND_ASSIGN(Crc32 old_crc, mapper->ComputeChecksum());
    ICING_ASSERT_OK(mapper->Put("foo", 12345));
    ICING_ASSERT_OK(mapper->PersistToDisk());
    ICING_ASSERT_OK_AND_ASSIGN(Crc32 new_crc, mapper->ComputeChecksum());
    ASSERT_THAT(old_crc, Not(Eq(new_crc)));
  }

  // Attempt to create the qualified id type joinable cache with corrupted
  // document_to_qualified_id_mapper. This should fail.
  EXPECT_THAT(QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION,
                       HasSubstr("Invalid storages crc")));
}

TEST_F(QualifiedIdTypeJoinableCacheTest, InvalidPut) {
  // Create new qualified id type joinable cache
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QualifiedIdTypeJoinableCache> cache,
      QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_));

  DocJoinInfo default_invalid;
  EXPECT_THAT(cache->Put(default_invalid, /*ref_document_id=*/0),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(QualifiedIdTypeJoinableCacheTest, InvalidGet) {
  // Create new qualified id type joinable cache
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QualifiedIdTypeJoinableCache> cache,
      QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_));

  DocJoinInfo default_invalid;
  EXPECT_THAT(cache->Get(default_invalid),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(QualifiedIdTypeJoinableCacheTest, PutAndGet) {
  DocJoinInfo target_info1(/*document_id=*/1, /*joinable_property_id=*/20);
  DocumentId ref_document1 = 0;

  DocJoinInfo target_info2(/*document_id=*/3, /*joinable_property_id=*/13);
  DocumentId ref_document2 = 2;

  DocJoinInfo target_info3(/*document_id=*/4, /*joinable_property_id=*/4);
  DocumentId ref_document3 = ref_document1;

  {
    // Create new qualified id type joinable cache
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdTypeJoinableCache> cache,
        QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_));

    EXPECT_THAT(cache->Put(target_info1, /*ref_document_id=*/ref_document1),
                IsOk());
    EXPECT_THAT(cache->Put(target_info2, /*ref_document_id=*/ref_document2),
                IsOk());
    EXPECT_THAT(cache->Put(target_info3, /*ref_document_id=*/ref_document3),
                IsOk());

    EXPECT_THAT(cache->Get(target_info1), IsOkAndHolds(ref_document1));
    EXPECT_THAT(cache->Get(target_info2), IsOkAndHolds(ref_document2));
    EXPECT_THAT(cache->Get(target_info3), IsOkAndHolds(ref_document3));

    ICING_ASSERT_OK(cache->PersistToDisk());
  }

  // Verify we can get all of them after destructing and re-initializing.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QualifiedIdTypeJoinableCache> cache,
      QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_));
  EXPECT_THAT(cache->Get(target_info1), IsOkAndHolds(ref_document1));
  EXPECT_THAT(cache->Get(target_info2), IsOkAndHolds(ref_document2));
  EXPECT_THAT(cache->Get(target_info3), IsOkAndHolds(ref_document3));
}

TEST_F(QualifiedIdTypeJoinableCacheTest,
       GetShouldReturnNotFoundErrorIfNotExist) {
  DocJoinInfo target_info(/*document_id=*/1, /*joinable_property_id=*/20);
  DocumentId ref_document = 0;

  // Create new qualified id type joinable cache
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QualifiedIdTypeJoinableCache> cache,
      QualifiedIdTypeJoinableCache::Create(filesystem_, working_path_));

  // Verify entry is not found in the beginning.
  EXPECT_THAT(cache->Get(target_info),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  ICING_ASSERT_OK(cache->Put(target_info, /*ref_document_id=*/ref_document));
  ASSERT_THAT(cache->Get(target_info), IsOkAndHolds(ref_document));

  // Get another non-existing entry. This should get NOT_FOUND_ERROR.
  DocJoinInfo another_target_info(/*document_id=*/2,
                                  /*joinable_property_id=*/20);
  EXPECT_THAT(cache->Get(another_target_info),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

}  // namespace

}  // namespace lib
}  // namespace icing
