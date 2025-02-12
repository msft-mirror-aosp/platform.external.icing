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

#include "icing/join/qualified-id-join-index-impl-v3.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/feature-flags.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/file/persistent-storage.h"
#include "icing/join/document-join-id-pair.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/crc32.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::IsFalse;
using ::testing::IsTrue;
using ::testing::Lt;
using ::testing::Not;
using ::testing::Pointee;
using ::testing::SizeIs;

using Crcs = PersistentStorage::Crcs;
using Info = QualifiedIdJoinIndexImplV3::Info;
using ArrayInfo = QualifiedIdJoinIndexImplV3::ArrayInfo;

class QualifiedIdJoinIndexImplV3Test : public ::testing::Test {
 protected:
  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());

    base_dir_ = GetTestTempDir() + "/icing";
    ASSERT_THAT(filesystem_.CreateDirectoryRecursively(base_dir_.c_str()),
                IsTrue());

    working_path_ = base_dir_ + "/qualified_id_join_index_impl_v3";
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(base_dir_.c_str());
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  Filesystem filesystem_;
  std::string base_dir_;
  std::string working_path_;
};

TEST_F(QualifiedIdJoinIndexImplV3Test, InvalidWorkingPath) {
  EXPECT_THAT(QualifiedIdJoinIndexImplV3::Create(
                  filesystem_, "/dev/null/qualified_id_join_index_impl_v3",
                  *feature_flags_),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL));
}

TEST_F(QualifiedIdJoinIndexImplV3Test, InitializeNewFiles) {
  {
    // Create new qualified id join index
    ASSERT_FALSE(filesystem_.DirectoryExists(working_path_.c_str()));
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
        QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                           *feature_flags_));
    EXPECT_THAT(index, Pointee(IsEmpty()));

    ICING_ASSERT_OK(index->PersistToDisk());
  }

  // Metadata file should be initialized correctly for both info and crcs
  // sections.
  const std::string metadata_file_path =
      absl_ports::StrCat(working_path_, "/metadata");
  auto metadata_buffer = std::make_unique<uint8_t[]>(
      QualifiedIdJoinIndexImplV3::kMetadataFileSize);
  ASSERT_THAT(
      filesystem_.PRead(metadata_file_path.c_str(), metadata_buffer.get(),
                        QualifiedIdJoinIndexImplV3::kMetadataFileSize,
                        /*offset=*/0),
      IsTrue());

  // Check info section
  const Info* info = reinterpret_cast<const Info*>(
      metadata_buffer.get() +
      QualifiedIdJoinIndexImplV3::kInfoMetadataFileOffset);
  EXPECT_THAT(info->magic, Eq(Info::kMagic));
  EXPECT_THAT(info->num_data, Eq(0));
  EXPECT_THAT(info->last_added_document_id, Eq(kInvalidDocumentId));

  // Check crcs section
  const Crcs* crcs = reinterpret_cast<const Crcs*>(
      metadata_buffer.get() +
      QualifiedIdJoinIndexImplV3::kCrcsMetadataFileOffset);
  // There are no data in FileBackedVectors, so storages_crc should be zero.
  EXPECT_THAT(crcs->component_crcs.storages_crc, Eq(0));
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

TEST_F(QualifiedIdJoinIndexImplV3Test,
       InitializationShouldFailIfMissingMetadataFile) {
  {
    DocumentJoinIdPair child_join_id_pair1(/*document_id=*/100,
                                           /*joinable_property_id=*/20);
    DocumentJoinIdPair child_join_id_pair2(/*document_id=*/101,
                                           /*joinable_property_id=*/2);

    // Create new qualified id join index
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
        QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                           *feature_flags_));

    // Insert some data.
    ICING_ASSERT_OK(
        index->Put(child_join_id_pair1,
                   /*parent_document_ids=*/std::vector<DocumentId>{0}));
    ICING_ASSERT_OK(
        index->Put(child_join_id_pair2,
                   /*parent_document_ids=*/std::vector<DocumentId>{1}));
    ASSERT_THAT(index, Pointee(SizeIs(2)));

    ICING_ASSERT_OK(index->PersistToDisk());
  }

  // Manually delete the metadata file.
  const std::string metadata_file_path =
      absl_ports::StrCat(working_path_, "/metadata");
  ASSERT_THAT(filesystem_.DeleteFile(metadata_file_path.c_str()), IsTrue());

  // Attempt to create the qualified id join index with missing metadata file.
  // This should fail.
  EXPECT_THAT(
      QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                         *feature_flags_),
      StatusIs(
          libtextclassifier3::StatusCode::FAILED_PRECONDITION,
          HasSubstr("Inconsistent state of qualified id join index (v3)")));
}

TEST_F(QualifiedIdJoinIndexImplV3Test,
       InitializationShouldFailIfMissingParentDocumentIdToChildArrayInfoFile) {
  {
    DocumentJoinIdPair child_join_id_pair1(/*document_id=*/100,
                                           /*joinable_property_id=*/20);
    DocumentJoinIdPair child_join_id_pair2(/*document_id=*/101,
                                           /*joinable_property_id=*/2);

    // Create new qualified id join index
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
        QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                           *feature_flags_));

    // Insert some data.
    ICING_ASSERT_OK(
        index->Put(child_join_id_pair1,
                   /*parent_document_ids=*/std::vector<DocumentId>{0}));
    ICING_ASSERT_OK(
        index->Put(child_join_id_pair2,
                   /*parent_document_ids=*/std::vector<DocumentId>{1}));
    ASSERT_THAT(index, Pointee(SizeIs(2)));

    ICING_ASSERT_OK(index->PersistToDisk());
  }

  // Manually delete parent_document_id_to_child_array_info file.
  const std::string array_working_path = absl_ports::StrCat(
      working_path_, "/parent_document_id_to_child_array_info");
  ASSERT_THAT(filesystem_.DeleteFile(array_working_path.c_str()), IsTrue());

  // Attempt to create the qualified id join index with missing
  // parent_document_id_to_child_array_info file. This should fail.
  EXPECT_THAT(
      QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                         *feature_flags_),
      StatusIs(
          libtextclassifier3::StatusCode::FAILED_PRECONDITION,
          HasSubstr("Inconsistent state of qualified id join index (v3)")));
}

TEST_F(QualifiedIdJoinIndexImplV3Test,
       InitializationShouldFailIfMissingChildDocumentJoinIdPairArrayFile) {
  {
    DocumentJoinIdPair child_join_id_pair1(/*document_id=*/100,
                                           /*joinable_property_id=*/20);
    DocumentJoinIdPair child_join_id_pair2(/*document_id=*/101,
                                           /*joinable_property_id=*/2);

    // Create new qualified id join index
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
        QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                           *feature_flags_));

    // Insert some data.
    ICING_ASSERT_OK(
        index->Put(child_join_id_pair1,
                   /*parent_document_ids=*/std::vector<DocumentId>{0}));
    ICING_ASSERT_OK(
        index->Put(child_join_id_pair2,
                   /*parent_document_ids=*/std::vector<DocumentId>{1}));
    ASSERT_THAT(index, Pointee(SizeIs(2)));

    ICING_ASSERT_OK(index->PersistToDisk());
  }

  // Manually delete child_document_join_id_pair_array file.
  const std::string array_working_path =
      absl_ports::StrCat(working_path_, "/child_document_join_id_pair_array");
  ASSERT_THAT(filesystem_.DeleteFile(array_working_path.c_str()), IsTrue());

  // Attempt to create the qualified id join index with missing
  // child_document_join_id_pair_array file. This should fail.
  EXPECT_THAT(
      QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                         *feature_flags_),
      StatusIs(
          libtextclassifier3::StatusCode::FAILED_PRECONDITION,
          HasSubstr("Inconsistent state of qualified id join index (v3)")));
}

TEST_F(QualifiedIdJoinIndexImplV3Test,
       InitializationShouldFailWithoutPersistToDiskOrDestruction) {
  DocumentJoinIdPair child_join_id_pair1(/*document_id=*/100,
                                         /*joinable_property_id=*/20);
  DocumentJoinIdPair child_join_id_pair2(/*document_id=*/101,
                                         /*joinable_property_id=*/2);

  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  // Insert some data.
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair1,
                 /*parent_document_ids=*/std::vector<DocumentId>{0}));
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair2,
                 /*parent_document_ids=*/std::vector<DocumentId>{1}));
  ASSERT_THAT(index, Pointee(SizeIs(2)));

  // GetChecksum should succeed without updating the checksum.
  EXPECT_THAT(index->GetChecksum(), IsOk());

  // Without calling PersistToDisk, checksums will not be recomputed or synced
  // to disk, so initializing another instance on the same files should fail.
  EXPECT_THAT(QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                                 *feature_flags_),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(QualifiedIdJoinIndexImplV3Test,
       InitializationShouldSucceedWithUpdateChecksums) {
  DocumentJoinIdPair child_join_id_pair1(/*document_id=*/100,
                                         /*joinable_property_id=*/20);
  DocumentJoinIdPair child_join_id_pair2(/*document_id=*/101,
                                         /*joinable_property_id=*/2);

  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index1,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  // Insert some data.
  ICING_ASSERT_OK(
      index1->Put(child_join_id_pair1,
                  /*parent_document_ids=*/std::vector<DocumentId>{0}));
  ICING_ASSERT_OK(
      index1->Put(child_join_id_pair2,
                  /*parent_document_ids=*/std::vector<DocumentId>{1}));
  ASSERT_THAT(index1, Pointee(SizeIs(2)));

  // After calling UpdateChecksums, all checksums should be recomputed and
  // synced correctly to disk, so initializing another instance on the same
  // files should succeed, and we should be able to get the same contents.
  ICING_ASSERT_OK_AND_ASSIGN(Crc32 crc, index1->GetChecksum());
  EXPECT_THAT(index1->UpdateChecksums(), IsOkAndHolds(Eq(crc)));
  EXPECT_THAT(index1->GetChecksum(), IsOkAndHolds(Eq(crc)));

  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index2,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));
  EXPECT_THAT(index2, Pointee(SizeIs(2)));
  EXPECT_THAT(index2->Get(/*parent_document_id=*/0),
              IsOkAndHolds(ElementsAre(child_join_id_pair1)));
  EXPECT_THAT(index2->Get(/*parent_document_id=*/1),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
}

TEST_F(QualifiedIdJoinIndexImplV3Test,
       InitializationShouldSucceedWithPersistToDisk) {
  DocumentJoinIdPair child_join_id_pair1(/*document_id=*/100,
                                         /*joinable_property_id=*/20);
  DocumentJoinIdPair child_join_id_pair2(/*document_id=*/101,
                                         /*joinable_property_id=*/2);

  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index1,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  // Insert some data.
  ICING_ASSERT_OK(
      index1->Put(child_join_id_pair1,
                  /*parent_document_ids=*/std::vector<DocumentId>{0}));
  ICING_ASSERT_OK(
      index1->Put(child_join_id_pair2,
                  /*parent_document_ids=*/std::vector<DocumentId>{1}));
  ASSERT_THAT(index1, Pointee(SizeIs(2)));

  // After calling PersistToDisk, all checksums should be recomputed and synced
  // correctly to disk, so initializing another instance on the same files
  // should succeed, and we should be able to get the same contents.
  ICING_EXPECT_OK(index1->PersistToDisk());

  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index2,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));
  EXPECT_THAT(index2, Pointee(SizeIs(2)));
  EXPECT_THAT(index2->Get(/*parent_document_id=*/0),
              IsOkAndHolds(ElementsAre(child_join_id_pair1)));
  EXPECT_THAT(index2->Get(/*parent_document_id=*/1),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
}

TEST_F(QualifiedIdJoinIndexImplV3Test,
       InitializationShouldSucceedAfterDestruction) {
  DocumentJoinIdPair child_join_id_pair1(/*document_id=*/100,
                                         /*joinable_property_id=*/20);
  DocumentJoinIdPair child_join_id_pair2(/*document_id=*/101,
                                         /*joinable_property_id=*/2);

  {
    // Create new qualified id join index
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
        QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                           *feature_flags_));

    // Insert some data.
    ICING_ASSERT_OK(
        index->Put(child_join_id_pair1,
                   /*parent_document_ids=*/std::vector<DocumentId>{0}));
    ICING_ASSERT_OK(
        index->Put(child_join_id_pair2,
                   /*parent_document_ids=*/std::vector<DocumentId>{1}));
    ASSERT_THAT(index, Pointee(SizeIs(2)));
  }

  {
    // The previous instance went out of scope and was destructed. Although we
    // didn't call PersistToDisk explicitly, the destructor should invoke it and
    // thus initializing another instance on the same files should succeed, and
    // we should be able to get the same contents.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
        QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                           *feature_flags_));
    EXPECT_THAT(index, Pointee(SizeIs(2)));
    EXPECT_THAT(index->Get(/*parent_document_id=*/0),
                IsOkAndHolds(ElementsAre(child_join_id_pair1)));
    EXPECT_THAT(index->Get(/*parent_document_id=*/1),
                IsOkAndHolds(ElementsAre(child_join_id_pair2)));
  }
}

TEST_F(QualifiedIdJoinIndexImplV3Test,
       InitializeExistingFilesWithDifferentMagicShouldFail) {
  {
    // Create new qualified id join index
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
        QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                           *feature_flags_));
    ICING_ASSERT_OK(index->Put(
        DocumentJoinIdPair(/*document_id=*/100, /*joinable_property_id=*/20),
        /*parent_document_ids=*/std::vector<DocumentId>{0}));

    ICING_ASSERT_OK(index->PersistToDisk());
  }

  {
    const std::string metadata_file_path =
        absl_ports::StrCat(working_path_, "/metadata");
    ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
    ASSERT_THAT(metadata_sfd.is_valid(), IsTrue());

    auto metadata_buffer = std::make_unique<uint8_t[]>(
        QualifiedIdJoinIndexImplV3::kMetadataFileSize);
    ASSERT_THAT(filesystem_.PRead(metadata_sfd.get(), metadata_buffer.get(),
                                  QualifiedIdJoinIndexImplV3::kMetadataFileSize,
                                  /*offset=*/0),
                IsTrue());

    // Manually change magic and update checksum
    Crcs* crcs = reinterpret_cast<Crcs*>(
        metadata_buffer.get() +
        QualifiedIdJoinIndexImplV3::kCrcsMetadataFileOffset);
    Info* info = reinterpret_cast<Info*>(
        metadata_buffer.get() +
        QualifiedIdJoinIndexImplV3::kInfoMetadataFileOffset);
    info->magic += 1;
    crcs->component_crcs.info_crc = info->GetChecksum().Get();
    crcs->all_crc = crcs->component_crcs.GetChecksum().Get();
    ASSERT_THAT(filesystem_.PWrite(
                    metadata_sfd.get(), /*offset=*/0, metadata_buffer.get(),
                    QualifiedIdJoinIndexImplV3::kMetadataFileSize),
                IsTrue());
  }

  // Attempt to create the qualified id join index with different magic. This
  // should fail.
  EXPECT_THAT(QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                                 *feature_flags_),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION,
                       HasSubstr("Incorrect magic value")));
}

TEST_F(QualifiedIdJoinIndexImplV3Test,
       InitializeExistingFilesWithWrongAllCrcShouldFail) {
  {
    // Create new qualified id join index
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
        QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                           *feature_flags_));
    ICING_ASSERT_OK(index->Put(
        DocumentJoinIdPair(/*document_id=*/100, /*joinable_property_id=*/20),
        /*parent_document_ids=*/std::vector<DocumentId>{0}));

    ICING_ASSERT_OK(index->PersistToDisk());
  }

  {
    const std::string metadata_file_path =
        absl_ports::StrCat(working_path_, "/metadata");
    ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
    ASSERT_THAT(metadata_sfd.is_valid(), IsTrue());

    auto metadata_buffer = std::make_unique<uint8_t[]>(
        QualifiedIdJoinIndexImplV3::kMetadataFileSize);
    ASSERT_THAT(filesystem_.PRead(metadata_sfd.get(), metadata_buffer.get(),
                                  QualifiedIdJoinIndexImplV3::kMetadataFileSize,
                                  /*offset=*/0),
                IsTrue());

    // Manually corrupt all_crc
    Crcs* crcs = reinterpret_cast<Crcs*>(
        metadata_buffer.get() +
        QualifiedIdJoinIndexImplV3::kCrcsMetadataFileOffset);
    crcs->all_crc += 1;

    ASSERT_THAT(filesystem_.PWrite(
                    metadata_sfd.get(), /*offset=*/0, metadata_buffer.get(),
                    QualifiedIdJoinIndexImplV3::kMetadataFileSize),
                IsTrue());
  }

  // Attempt to create the qualified id join index with metadata containing
  // corrupted all_crc. This should fail.
  EXPECT_THAT(QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                                 *feature_flags_),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION,
                       HasSubstr("Invalid all crc")));
}

TEST_F(QualifiedIdJoinIndexImplV3Test,
       InitializeExistingFilesWithCorruptedInfoShouldFail) {
  {
    // Create new qualified id join index
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
        QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                           *feature_flags_));
    ICING_ASSERT_OK(index->Put(
        DocumentJoinIdPair(/*document_id=*/100, /*joinable_property_id=*/20),
        /*parent_document_ids=*/std::vector<DocumentId>{0}));

    ICING_ASSERT_OK(index->PersistToDisk());
  }

  {
    const std::string metadata_file_path =
        absl_ports::StrCat(working_path_, "/metadata");
    ScopedFd metadata_sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
    ASSERT_THAT(metadata_sfd.is_valid(), IsTrue());

    auto metadata_buffer = std::make_unique<uint8_t[]>(
        QualifiedIdJoinIndexImplV3::kMetadataFileSize);
    ASSERT_THAT(filesystem_.PRead(metadata_sfd.get(), metadata_buffer.get(),
                                  QualifiedIdJoinIndexImplV3::kMetadataFileSize,
                                  /*offset=*/0),
                IsTrue());

    // Modify info, but don't update the checksum. This would be similar to
    // corruption of info.
    Info* info = reinterpret_cast<Info*>(
        metadata_buffer.get() +
        QualifiedIdJoinIndexImplV3::kInfoMetadataFileOffset);
    info->last_added_document_id += 1;

    ASSERT_THAT(filesystem_.PWrite(
                    metadata_sfd.get(), /*offset=*/0, metadata_buffer.get(),
                    QualifiedIdJoinIndexImplV3::kMetadataFileSize),
                IsTrue());
  }

  // Attempt to create the qualified id join index with info that doesn't match
  // its checksum. This should fail.
  EXPECT_THAT(QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                                 *feature_flags_),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION,
                       HasSubstr("Invalid info crc")));
}

TEST_F(
    QualifiedIdJoinIndexImplV3Test,
    InitializeExistingFilesWithCorruptedParentDocumentIdToChildArrayInfoShouldFail) {
  {
    // Create new qualified id join index
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
        QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                           *feature_flags_));
    ICING_ASSERT_OK(index->Put(
        DocumentJoinIdPair(/*document_id=*/100, /*joinable_property_id=*/20),
        /*parent_document_ids=*/std::vector<DocumentId>{0}));

    ICING_ASSERT_OK(index->PersistToDisk());
  }

  // Corrupt parent_document_id_to_child_array_info manually.
  {
    const std::string array_working_path = absl_ports::StrCat(
        working_path_, "/parent_document_id_to_child_array_info");
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FileBackedVector<ArrayInfo>> fbv,
        FileBackedVector<ArrayInfo>::Create(
            filesystem_, std::move(array_working_path),
            MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
            FileBackedVector<ArrayInfo>::kMaxFileSize,
            /*pre_mapping_mmap_size=*/0));
    ICING_ASSERT_OK_AND_ASSIGN(Crc32 old_crc, fbv->UpdateChecksum());
    ICING_ASSERT_OK(fbv->Append(
        ArrayInfo(/*index_in=*/100, /*length_in=*/10, /*used_length_in=*/0)));
    ICING_ASSERT_OK(fbv->PersistToDisk());
    ICING_ASSERT_OK_AND_ASSIGN(Crc32 new_crc, fbv->UpdateChecksum());
    ASSERT_THAT(old_crc, Not(Eq(new_crc)));
  }

  // Attempt to create the qualified id join index with corrupted
  // parent_document_id_to_child_array_info. This should fail.
  EXPECT_THAT(QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                                 *feature_flags_),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION,
                       HasSubstr("Invalid storages crc")));
}

TEST_F(
    QualifiedIdJoinIndexImplV3Test,
    InitializeExistingFilesWithCorruptedChildDocumentJoinIdPairArrayShouldFail) {
  {
    // Create new qualified id join index
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
        QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                           *feature_flags_));
    ICING_ASSERT_OK(index->Put(
        DocumentJoinIdPair(/*document_id=*/100, /*joinable_property_id=*/20),
        /*parent_document_ids=*/std::vector<DocumentId>{0}));

    ICING_ASSERT_OK(index->PersistToDisk());
  }

  // Corrupt child_document_join_id_pair_array manually.
  {
    const std::string array_working_path =
        absl_ports::StrCat(working_path_, "/child_document_join_id_pair_array");
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FileBackedVector<DocumentJoinIdPair>> fbv,
        FileBackedVector<DocumentJoinIdPair>::Create(
            filesystem_, std::move(array_working_path),
            MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
            FileBackedVector<DocumentJoinIdPair>::kMaxFileSize,
            /*pre_mapping_mmap_size=*/0));
    ICING_ASSERT_OK_AND_ASSIGN(Crc32 old_crc, fbv->UpdateChecksum());
    ICING_ASSERT_OK(fbv->Append(DocumentJoinIdPair(/*value=*/12345)));
    ICING_ASSERT_OK(fbv->PersistToDisk());
    ICING_ASSERT_OK_AND_ASSIGN(Crc32 new_crc, fbv->UpdateChecksum());
    ASSERT_THAT(old_crc, Not(Eq(new_crc)));
  }

  // Attempt to create the qualified id join index with corrupted
  // child_document_join_id_pair_array. This should fail.
  EXPECT_THAT(QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                                 *feature_flags_),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION,
                       HasSubstr("Invalid storages crc")));
}

TEST_F(QualifiedIdJoinIndexImplV3Test, Put) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  // Add 6 children with their parents to the index.
  DocumentJoinIdPair child_join_id_pair1(/*document_id=*/100,
                                         /*joinable_property_id=*/20);
  DocumentJoinIdPair child_join_id_pair2(/*document_id=*/101,
                                         /*joinable_property_id=*/2);
  DocumentJoinIdPair child_join_id_pair3(/*document_id=*/104,
                                         /*joinable_property_id=*/4);
  DocumentJoinIdPair child_join_id_pair4(/*document_id=*/105,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair5(/*document_id=*/109,
                                         /*joinable_property_id=*/20);
  DocumentJoinIdPair child_join_id_pair6(/*document_id=*/121,
                                         /*joinable_property_id=*/3);
  EXPECT_THAT(index->Put(child_join_id_pair1,
                         /*parent_document_ids=*/std::vector<DocumentId>{1}),
              IsOk());
  EXPECT_THAT(index->Put(child_join_id_pair2,
                         /*parent_document_ids=*/std::vector<DocumentId>{1}),
              IsOk());
  EXPECT_THAT(index->Put(child_join_id_pair3,
                         /*parent_document_ids=*/std::vector<DocumentId>{2}),
              IsOk());
  EXPECT_THAT(index->Put(child_join_id_pair4,
                         /*parent_document_ids=*/std::vector<DocumentId>{0}),
              IsOk());
  EXPECT_THAT(index->Put(child_join_id_pair5,
                         /*parent_document_ids=*/std::vector<DocumentId>{1}),
              IsOk());
  EXPECT_THAT(index->Put(child_join_id_pair6,
                         /*parent_document_ids=*/std::vector<DocumentId>{5}),
              IsOk());

  EXPECT_THAT(index, Pointee(SizeIs(6)));

  // Verify Get API.
  EXPECT_THAT(index->Get(/*parent_document_id=*/0),
              IsOkAndHolds(ElementsAre(child_join_id_pair4)));
  EXPECT_THAT(index->Get(/*parent_document_id=*/1),
              IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair2,
                                       child_join_id_pair5)));
  EXPECT_THAT(index->Get(/*parent_document_id=*/2),
              IsOkAndHolds(ElementsAre(child_join_id_pair3)));
  EXPECT_THAT(index->Get(/*parent_document_id=*/3), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->Get(/*parent_document_id=*/4), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->Get(/*parent_document_id=*/5),
              IsOkAndHolds(ElementsAre(child_join_id_pair6)));
}

TEST_F(QualifiedIdJoinIndexImplV3Test,
       Put_multipleParentsInASingleJoinableProperty) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  // Add 3 children with multiple parents to the index.
  DocumentJoinIdPair child_join_id_pair1(/*document_id=*/100,
                                         /*joinable_property_id=*/20);
  DocumentJoinIdPair child_join_id_pair2(/*document_id=*/101,
                                         /*joinable_property_id=*/2);
  DocumentJoinIdPair child_join_id_pair3(/*document_id=*/104,
                                         /*joinable_property_id=*/4);
  EXPECT_THAT(
      index->Put(child_join_id_pair1,
                 /*parent_document_ids=*/std::vector<DocumentId>{1, 4, 7, 10}),
      IsOk());
  EXPECT_THAT(
      index->Put(
          child_join_id_pair2,
          /*parent_document_ids=*/std::vector<DocumentId>{0, 1, 2, 3, 5, 8}),
      IsOk());
  EXPECT_THAT(
      index->Put(child_join_id_pair3,
                 /*parent_document_ids=*/std::vector<DocumentId>{2, 5, 7}),
      IsOk());

  EXPECT_THAT(index, Pointee(SizeIs(13)));

  // Verify Get API.
  EXPECT_THAT(index->Get(/*parent_document_id=*/0),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
  EXPECT_THAT(
      index->Get(/*parent_document_id=*/1),
      IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair2)));
  EXPECT_THAT(
      index->Get(/*parent_document_id=*/2),
      IsOkAndHolds(ElementsAre(child_join_id_pair2, child_join_id_pair3)));
  EXPECT_THAT(index->Get(/*parent_document_id=*/3),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
  EXPECT_THAT(index->Get(/*parent_document_id=*/4),
              IsOkAndHolds(ElementsAre(child_join_id_pair1)));
  EXPECT_THAT(
      index->Get(/*parent_document_id=*/5),
      IsOkAndHolds(ElementsAre(child_join_id_pair2, child_join_id_pair3)));
  EXPECT_THAT(index->Get(/*parent_document_id=*/6), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(
      index->Get(/*parent_document_id=*/7),
      IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair3)));
  EXPECT_THAT(index->Get(/*parent_document_id=*/8),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
  EXPECT_THAT(index->Get(/*parent_document_id=*/9), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->Get(/*parent_document_id=*/10),
              IsOkAndHolds(ElementsAre(child_join_id_pair1)));
}

TEST_F(QualifiedIdJoinIndexImplV3Test,
       Put_multipleParentsInMultipleJoinableProperties) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  // Add 1 child document with multiple parents in multiple joinable properties
  // to the index.
  DocumentJoinIdPair child_join_id_pair1(/*document_id=*/100,
                                         /*joinable_property_id=*/20);
  DocumentJoinIdPair child_join_id_pair2(/*document_id=*/100,
                                         /*joinable_property_id=*/18);
  DocumentJoinIdPair child_join_id_pair3(/*document_id=*/100,
                                         /*joinable_property_id=*/5);
  EXPECT_THAT(
      index->Put(child_join_id_pair1,
                 /*parent_document_ids=*/std::vector<DocumentId>{1, 4, 7, 10}),
      IsOk());
  EXPECT_THAT(
      index->Put(
          child_join_id_pair2,
          /*parent_document_ids=*/std::vector<DocumentId>{0, 1, 2, 3, 5, 8}),
      IsOk());
  EXPECT_THAT(
      index->Put(child_join_id_pair3,
                 /*parent_document_ids=*/std::vector<DocumentId>{2, 5, 7}),
      IsOk());

  EXPECT_THAT(index, Pointee(SizeIs(13)));

  // Verify Get API.
  EXPECT_THAT(index->Get(/*parent_document_id=*/0),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
  EXPECT_THAT(
      index->Get(/*parent_document_id=*/1),
      IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair2)));
  EXPECT_THAT(
      index->Get(/*parent_document_id=*/2),
      IsOkAndHolds(ElementsAre(child_join_id_pair2, child_join_id_pair3)));
  EXPECT_THAT(index->Get(/*parent_document_id=*/3),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
  EXPECT_THAT(index->Get(/*parent_document_id=*/4),
              IsOkAndHolds(ElementsAre(child_join_id_pair1)));
  EXPECT_THAT(
      index->Get(/*parent_document_id=*/5),
      IsOkAndHolds(ElementsAre(child_join_id_pair2, child_join_id_pair3)));
  EXPECT_THAT(index->Get(/*parent_document_id=*/6), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(
      index->Get(/*parent_document_id=*/7),
      IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair3)));
  EXPECT_THAT(index->Get(/*parent_document_id=*/8),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
  EXPECT_THAT(index->Get(/*parent_document_id=*/9), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->Get(/*parent_document_id=*/10),
              IsOkAndHolds(ElementsAre(child_join_id_pair1)));
}

TEST_F(QualifiedIdJoinIndexImplV3Test,
       PutShouldResizeParentDocumentIdToChildArrayInfo) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  constexpr DocumentId kParentDocumentId = 10;
  DocumentJoinIdPair child_join_id_pair(/*document_id=*/100,
                                        /*joinable_property_id=*/20);

  // Even though document 0 to 9 are missing in the index, adding parent
  // document id 10 should resize the FileBackedVector and succeed without
  // error.
  EXPECT_THAT(
      index->Put(
          child_join_id_pair,
          /*parent_document_ids=*/std::vector<DocumentId>{kParentDocumentId}),
      IsOk());
  EXPECT_THAT(index, Pointee(SizeIs(1)));

  // Get API should return empty result for document 0 to 9.
  for (DocumentId parent_doc_id = 0; parent_doc_id < kParentDocumentId;
       ++parent_doc_id) {
    EXPECT_THAT(index->Get(parent_doc_id), IsOkAndHolds(IsEmpty()));
  }
  // Get API should return the child document for document 10.
  EXPECT_THAT(index->Get(kParentDocumentId),
              IsOkAndHolds(ElementsAre(child_join_id_pair)));
}

TEST_F(QualifiedIdJoinIndexImplV3Test,
       PutShouldExtendChildDocumentJoinIdPairArray) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  // Put 1 child for parent1.
  DocumentId parent1 = 1;
  DocumentJoinIdPair child_join_id_pair1(/*document_id=*/100,
                                         /*joinable_property_id=*/20);
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair1,
                 /*parent_document_ids=*/std::vector<DocumentId>{parent1}));
  EXPECT_THAT(index, Pointee(SizeIs(1)));

  // Put 1 child for parent2. This makes parent2's array locate right after
  // parent1's array.
  DocumentId parent2 = 2;
  DocumentJoinIdPair child_join_id_pair2(/*document_id=*/101,
                                         /*joinable_property_id=*/20);
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair2,
                 /*parent_document_ids=*/std::vector<DocumentId>{parent2}));
  EXPECT_THAT(index, Pointee(SizeIs(2)));

  constexpr int kNumAdditionalChildren = 100;
  // Put 100 more children for parent1. The array storing the child document
  // join id pairs should be extended correctly to fit all the new elements
  // without affecting (overwriting) parent2's array.
  std::vector<DocumentJoinIdPair> child_join_id_pairs;
  child_join_id_pairs.reserve(kNumAdditionalChildren + 1);
  child_join_id_pairs.push_back(child_join_id_pair1);
  for (int i = 0; i < kNumAdditionalChildren; ++i) {
    DocumentJoinIdPair child_join_id_pair(/*document_id=*/200 + i,
                                          /*joinable_property_id=*/5);
    EXPECT_THAT(
        index->Put(child_join_id_pair,
                   /*parent_document_ids=*/std::vector<DocumentId>{parent1}),
        IsOk());
    child_join_id_pairs.push_back(std::move(child_join_id_pair));
  }
  EXPECT_THAT(index, Pointee(SizeIs(102)));

  EXPECT_THAT(index->Get(parent1), IsOkAndHolds(child_join_id_pairs));
  EXPECT_THAT(index->Get(parent2),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
}

TEST_F(QualifiedIdJoinIndexImplV3Test, PutShouldSkipInvalidParentDocumentId) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  DocumentJoinIdPair child_join_id_pair(/*document_id=*/100,
                                        /*joinable_property_id=*/20);
  EXPECT_THAT(
      index->Put(
          child_join_id_pair,
          /*parent_document_ids=*/std::vector<DocumentId>{-1,
                                                          kInvalidDocumentId, 1,
                                                          3, 2}),
      IsOk());

  // -1, kInvalidDocumentId should be skipped, so only 3 valid join relations
  // should be added to the index.
  EXPECT_THAT(index, Pointee(SizeIs(3)));

  // Verify Get API.
  EXPECT_THAT(index->Get(/*parent_document_id=*/1),
              IsOkAndHolds(ElementsAre(child_join_id_pair)));
  EXPECT_THAT(index->Get(/*parent_document_id=*/2),
              IsOkAndHolds(ElementsAre(child_join_id_pair)));
  EXPECT_THAT(index->Get(/*parent_document_id=*/3),
              IsOkAndHolds(ElementsAre(child_join_id_pair)));
}

TEST_F(QualifiedIdJoinIndexImplV3Test,
       PutShouldReturnInvalidArgumentErrorForInvalidChild) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  DocumentJoinIdPair invalid_child_join_id;  // Default constructor creates an
                                             // invalid DocumentJoinIdPair.
  ASSERT_THAT(invalid_child_join_id.is_valid(), IsFalse());

  EXPECT_THAT(index->Put(invalid_child_join_id,
                         /*parent_document_ids=*/std::vector<DocumentId>{0}),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(index, Pointee(IsEmpty()));
}

TEST_F(QualifiedIdJoinIndexImplV3Test, GetEmptyIndex) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));
  EXPECT_THAT(index, Pointee(IsEmpty()));

  EXPECT_THAT(index->Get(/*parent_document_id=*/0), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->Get(/*parent_document_id=*/1), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->Get(/*parent_document_id=*/2), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->Get(/*parent_document_id=*/3), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->Get(/*parent_document_id=*/4), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->Get(/*parent_document_id=*/5), IsOkAndHolds(IsEmpty()));
}

TEST_F(
    QualifiedIdJoinIndexImplV3Test,
    GetShouldReturnEmptyResultWithoutAccessingArrayForNonExistingLargeParent) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  DocumentJoinIdPair child_join_id_pair(/*document_id=*/100,
                                        /*joinable_property_id=*/20);
  ICING_ASSERT_OK(index->Put(
      child_join_id_pair, /*parent_document_ids=*/std::vector<DocumentId>{1}));
  EXPECT_THAT(index, Pointee(SizeIs(1)));

  EXPECT_THAT(index->Get(/*parent_document_id=*/0), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->Get(/*parent_document_id=*/1),
              IsOkAndHolds(ElementsAre(child_join_id_pair)));

  // Now, only parent document id 1 is in the index, so the FileBackedVector has
  // been resized to fit parent document id 1.
  // Get API for parent document id greater than 1 should return empty result
  // without accessing the FileBackedVector.
  EXPECT_THAT(index->Get(/*parent_document_id=*/2), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->Get(/*parent_document_id=*/3), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->Get(kMaxDocumentId), IsOkAndHolds(IsEmpty()));
}

TEST_F(QualifiedIdJoinIndexImplV3Test,
       GetShouldReturnInvalidArgumentErrorForInvalidParentDocumentId) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  EXPECT_THAT(index->Get(/*parent_document_id=*/-1),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(index->Get(kInvalidDocumentId),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(QualifiedIdJoinIndexImplV3Test, MigrateParent) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  DocumentId parent_doc_id1 = 1;
  DocumentId parent_doc_id2 = 1024;

  // Add 6 children with their parents to the index.
  DocumentJoinIdPair child_join_id_pair1(/*document_id=*/100,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair2(/*document_id=*/101,
                                         /*joinable_property_id=*/0);
  ICING_ASSERT_OK(index->Put(
      child_join_id_pair1,
      /*parent_document_ids=*/std::vector<DocumentId>{parent_doc_id1}));
  ICING_ASSERT_OK(index->Put(
      child_join_id_pair2,
      /*parent_document_ids=*/std::vector<DocumentId>{parent_doc_id1}));

  // Sanity check.
  ASSERT_THAT(index, Pointee(SizeIs(2)));
  ASSERT_THAT(
      index->Get(parent_doc_id1),
      IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair2)));
  ASSERT_THAT(index->Get(parent_doc_id2), IsOkAndHolds(IsEmpty()));

  // Migrate parent document id 1 to 1024.
  EXPECT_THAT(index->MigrateParent(parent_doc_id1, parent_doc_id2), IsOk());
  EXPECT_THAT(index->Get(parent_doc_id1), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(
      index->Get(parent_doc_id2),
      IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair2)));
}

TEST_F(QualifiedIdJoinIndexImplV3Test, SetLastAddedDocumentId) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  EXPECT_THAT(index->last_added_document_id(), Eq(kInvalidDocumentId));

  constexpr DocumentId kDocumentId = 100;
  index->set_last_added_document_id(kDocumentId);
  EXPECT_THAT(index->last_added_document_id(), Eq(kDocumentId));

  constexpr DocumentId kNextDocumentId = 123;
  index->set_last_added_document_id(kNextDocumentId);
  EXPECT_THAT(index->last_added_document_id(), Eq(kNextDocumentId));
}

TEST_F(
    QualifiedIdJoinIndexImplV3Test,
    SetLastAddedDocumentIdShouldIgnoreNewDocumentIdNotGreaterThanTheCurrent) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  constexpr DocumentId kDocumentId = 123;
  index->set_last_added_document_id(kDocumentId);
  ASSERT_THAT(index->last_added_document_id(), Eq(kDocumentId));

  constexpr DocumentId kNextDocumentId = 100;
  ASSERT_THAT(kNextDocumentId, Lt(kDocumentId));
  index->set_last_added_document_id(kNextDocumentId);
  // last_added_document_id() should remain unchanged.
  EXPECT_THAT(index->last_added_document_id(), Eq(kDocumentId));
}

TEST_F(QualifiedIdJoinIndexImplV3Test, Optimize) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  // Create 4 parent and 7 child documents (with N to N joins):
  // - Document 1: 101, 103, 104, 105, 107
  // - Document 2: 102, 103, 105
  // - Document 3: 101, 106
  // - Document 4: 103
  // Add 6 children with their parents to the index.
  DocumentJoinIdPair child_join_id_pair1(/*document_id=*/101,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair2(/*document_id=*/102,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair3(/*document_id=*/103,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair4(/*document_id=*/104,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair5(/*document_id=*/105,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair6(/*document_id=*/106,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair7(/*document_id=*/107,
                                         /*joinable_property_id=*/0);
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair1,
                 /*parent_document_ids=*/std::vector<DocumentId>{1, 3}));
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair2,
                 /*parent_document_ids=*/std::vector<DocumentId>{2}));
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair3,
                 /*parent_document_ids=*/std::vector<DocumentId>{1, 2, 4}));
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair4,
                 /*parent_document_ids=*/std::vector<DocumentId>{1}));
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair5,
                 /*parent_document_ids=*/std::vector<DocumentId>{1, 2}));
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair6,
                 /*parent_document_ids=*/std::vector<DocumentId>{3}));
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair7,
                 /*parent_document_ids=*/std::vector<DocumentId>{1}));

  ASSERT_THAT(index, Pointee(SizeIs(11)));
  index->set_last_added_document_id(107);
  ASSERT_THAT(index->last_added_document_id(), Eq(107));

  // Delete parent 3, child 103, 107. Create a new mapping from old document id
  // to new document id.
  std::vector<DocumentId> document_id_old_to_new(108, kInvalidDocumentId);
  document_id_old_to_new[1] = 0;
  document_id_old_to_new[2] = 1;
  document_id_old_to_new[4] = 2;
  document_id_old_to_new[101] = 11;
  document_id_old_to_new[102] = 12;
  document_id_old_to_new[104] = 13;
  document_id_old_to_new[105] = 14;
  document_id_old_to_new[106] = 15;

  // Note: namespace_id_old_to_new is not used in
  // QualifiedIdJoinIndexImplV3::Optimize.
  DocumentId new_last_added_document_id = 15;
  EXPECT_THAT(
      index->Optimize(document_id_old_to_new, /*namespace_id_old_to_new=*/{},
                      new_last_added_document_id),
      IsOk());
  EXPECT_THAT(index, Pointee(SizeIs(5)));
  EXPECT_THAT(index->last_added_document_id(), Eq(new_last_added_document_id));

  // Verify document 0 (originally document 1)
  // - Child docs 101, 104, 105 become 11, 13, 14.
  // - Child docs 103, 107 are deleted.
  EXPECT_THAT(
      index->Get(/*parent_document_id=*/0),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(/*document_id=*/11, /*joinable_property_id=*/0),
          DocumentJoinIdPair(/*document_id=*/13, /*joinable_property_id=*/0),
          DocumentJoinIdPair(/*document_id=*/14, /*joinable_property_id=*/0))));

  // Verify document 1 (originally document 2)
  // - Child docs 102, 105 become 12, 14.
  // - Child doc 103 is deleted.
  EXPECT_THAT(
      index->Get(/*parent_document_id=*/1),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(/*document_id=*/12, /*joinable_property_id=*/0),
          DocumentJoinIdPair(/*document_id=*/14, /*joinable_property_id=*/0))));

  // Verify document 2 (originally document 4)
  // - Child doc 103 is deleted.
  EXPECT_THAT(index->Get(/*parent_document_id=*/2), IsOkAndHolds(IsEmpty()));

  // Verify document 3 and 4:
  // - These 2 doc ids don't exist after optimize.
  // - The relations for the original document 3 and 4 should be deleted.
  EXPECT_THAT(index->Get(/*parent_document_id=*/3), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->Get(/*parent_document_id=*/4), IsOkAndHolds(IsEmpty()));

  // Verify Put API should work normally after Optimize().
  DocumentJoinIdPair another_child_join_id_pair(/*document_id=*/16,
                                                /*joinable_property_id=*/0);
  EXPECT_THAT(
      index->Put(another_child_join_id_pair,
                 /*parent_document_ids=*/std::vector<DocumentId>{0, 2, 3}),
      IsOk());
  index->set_last_added_document_id(16);

  EXPECT_THAT(index, Pointee(SizeIs(8)));
  EXPECT_THAT(index->last_added_document_id(), Eq(16));
  EXPECT_THAT(
      index->Get(/*parent_document_id=*/0),
      IsOkAndHolds(ElementsAre(DocumentJoinIdPair(/*document_id=*/11,
                                                  /*joinable_property_id=*/0),
                               DocumentJoinIdPair(/*document_id=*/13,
                                                  /*joinable_property_id=*/0),
                               DocumentJoinIdPair(/*document_id=*/14,
                                                  /*joinable_property_id=*/0),
                               another_child_join_id_pair)));
  EXPECT_THAT(index->Get(/*parent_document_id=*/2),
              IsOkAndHolds(ElementsAre(another_child_join_id_pair)));
  EXPECT_THAT(index->Get(/*parent_document_id=*/3),
              IsOkAndHolds(ElementsAre(another_child_join_id_pair)));
}

TEST_F(QualifiedIdJoinIndexImplV3Test, OptimizeOutOfRangeDocumentId) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  // Create 2 parent and 3 child documents (with N to N joins):
  // - Document 1: 101, 106, 108
  // - Document 120: 101
  // Add 3 children with their parents to the index.
  DocumentJoinIdPair child_join_id_pair1(/*document_id=*/101,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair2(/*document_id=*/106,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair3(/*document_id=*/108,
                                         /*joinable_property_id=*/0);
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair1,
                 /*parent_document_ids=*/std::vector<DocumentId>{1, 120}));
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair2,
                 /*parent_document_ids=*/std::vector<DocumentId>{1}));
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair3,
                 /*parent_document_ids=*/std::vector<DocumentId>{1}));

  ASSERT_THAT(index, Pointee(SizeIs(4)));
  index->set_last_added_document_id(120);
  ASSERT_THAT(index->last_added_document_id(), Eq(120));

  // Create document_id_old_to_new with size = 107 (from index 0 to 106), which
  // makes parent document 120 and child document 108 out of range.
  //
  // Optimize should handle out of range DocumentId properly without crashing.
  std::vector<DocumentId> document_id_old_to_new(107, kInvalidDocumentId);
  document_id_old_to_new[1] = 0;
  document_id_old_to_new[101] = 11;
  document_id_old_to_new[106] = 12;

  // Note: namespace_id_old_to_new is not used in
  // QualifiedIdJoinIndexImplV3::Optimize.
  DocumentId new_last_added_document_id = 12;
  EXPECT_THAT(
      index->Optimize(document_id_old_to_new, /*namespace_id_old_to_new=*/{},
                      new_last_added_document_id),
      IsOk());
  EXPECT_THAT(index, Pointee(SizeIs(2)));
  EXPECT_THAT(index->last_added_document_id(), Eq(new_last_added_document_id));

  // Verify document 0 (originally document 1)
  // - Child doc 101, 106 become 11, 12.
  // - Child doc 108 is out of range, so it should be deleted.
  EXPECT_THAT(
      index->Get(/*parent_document_id=*/0),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(/*document_id=*/11, /*joinable_property_id=*/0),
          DocumentJoinIdPair(/*document_id=*/12, /*joinable_property_id=*/0))));
}

TEST_F(QualifiedIdJoinIndexImplV3Test, OptimizeDeleteAllDocuments) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  // Create 4 parent and 7 child documents (with N to N joins):
  // - Document 1: 101, 103, 104, 105, 107
  // - Document 2: 102, 103, 105
  // - Document 3: 101, 106
  // - Document 4: 103
  // Add 6 children with their parents to the index.
  DocumentJoinIdPair child_join_id_pair1(/*document_id=*/101,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair2(/*document_id=*/102,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair3(/*document_id=*/103,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair4(/*document_id=*/104,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair5(/*document_id=*/105,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair6(/*document_id=*/106,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair7(/*document_id=*/107,
                                         /*joinable_property_id=*/0);
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair1,
                 /*parent_document_ids=*/std::vector<DocumentId>{1, 3}));
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair2,
                 /*parent_document_ids=*/std::vector<DocumentId>{2}));
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair3,
                 /*parent_document_ids=*/std::vector<DocumentId>{1, 2, 4}));
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair4,
                 /*parent_document_ids=*/std::vector<DocumentId>{1}));
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair5,
                 /*parent_document_ids=*/std::vector<DocumentId>{1, 2}));
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair6,
                 /*parent_document_ids=*/std::vector<DocumentId>{3}));
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair7,
                 /*parent_document_ids=*/std::vector<DocumentId>{1}));

  ASSERT_THAT(index, Pointee(SizeIs(11)));
  index->set_last_added_document_id(107);
  ASSERT_THAT(index->last_added_document_id(), Eq(107));

  // Delete all documents.
  std::vector<DocumentId> document_id_old_to_new(108, kInvalidDocumentId);

  // Note: namespace_id_old_to_new is not used in
  // QualifiedIdJoinIndexImplV3::Optimize.
  DocumentId new_last_added_document_id = kInvalidDocumentId;
  EXPECT_THAT(
      index->Optimize(document_id_old_to_new, /*namespace_id_old_to_new=*/{},
                      new_last_added_document_id),
      IsOk());
  EXPECT_THAT(index, Pointee(IsEmpty()));
  EXPECT_THAT(index->last_added_document_id(), Eq(new_last_added_document_id));
  EXPECT_THAT(index->Get(/*parent_document_id=*/0), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->Get(/*parent_document_id=*/1), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->Get(/*parent_document_id=*/2), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->Get(/*parent_document_id=*/3), IsOkAndHolds(IsEmpty()));
}

TEST_F(QualifiedIdJoinIndexImplV3Test, Clear) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  // Add 6 children with their parents to the index.
  DocumentJoinIdPair child_join_id_pair1(/*document_id=*/100,
                                         /*joinable_property_id=*/20);
  DocumentJoinIdPair child_join_id_pair2(/*document_id=*/101,
                                         /*joinable_property_id=*/2);
  DocumentJoinIdPair child_join_id_pair3(/*document_id=*/104,
                                         /*joinable_property_id=*/4);
  DocumentJoinIdPair child_join_id_pair4(/*document_id=*/105,
                                         /*joinable_property_id=*/0);
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair1,
                 /*parent_document_ids=*/std::vector<DocumentId>{1}));
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair2,
                 /*parent_document_ids=*/std::vector<DocumentId>{1}));
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair3,
                 /*parent_document_ids=*/std::vector<DocumentId>{2}));
  ICING_ASSERT_OK(
      index->Put(child_join_id_pair4,
                 /*parent_document_ids=*/std::vector<DocumentId>{0}));

  ASSERT_THAT(index, Pointee(SizeIs(4)));
  index->set_last_added_document_id(105);
  ASSERT_THAT(index->last_added_document_id(), Eq(105));

  // After Clear(), last_added_document_id should be set to kInvalidDocumentId,
  // and the previous added data should be deleted.
  EXPECT_THAT(index->Clear(), IsOk());
  EXPECT_THAT(index, Pointee(IsEmpty()));
  EXPECT_THAT(index->last_added_document_id(), Eq(kInvalidDocumentId));
  EXPECT_THAT(index->Get(/*parent_document_id=*/0), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->Get(/*parent_document_id=*/1), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->Get(/*parent_document_id=*/2), IsOkAndHolds(IsEmpty()));

  // Join index should be able to work normally after Clear().
  EXPECT_THAT(index->Put(child_join_id_pair4,
                         /*parent_document_ids=*/std::vector<DocumentId>{5}),
              IsOk());
  index->set_last_added_document_id(105);

  EXPECT_THAT(index, Pointee(SizeIs(1)));
  EXPECT_THAT(index->last_added_document_id(), Eq(105));
  EXPECT_THAT(index->Get(/*parent_document_id=*/5),
              IsOkAndHolds(ElementsAre(child_join_id_pair4)));

  ICING_ASSERT_OK(index->PersistToDisk());
  index.reset();

  // Verify index after reconstructing.
  ICING_ASSERT_OK_AND_ASSIGN(
      index, QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                                *feature_flags_));
  EXPECT_THAT(index, Pointee(SizeIs(1)));
  EXPECT_THAT(index->last_added_document_id(), Eq(105));
  EXPECT_THAT(index->Get(/*parent_document_id=*/5),
              IsOkAndHolds(ElementsAre(child_join_id_pair4)));
}

}  // namespace

}  // namespace lib
}  // namespace icing
