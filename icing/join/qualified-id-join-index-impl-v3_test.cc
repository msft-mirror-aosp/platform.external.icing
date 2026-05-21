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
#include "icing/document-builder.h"
#include "icing/feature-flags.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/file/persistent-storage.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/join/document-join-id-pair.h"
#include "icing/join/qualified-id-join-index.h"
#include "icing/join/qualified-id.h"
#include "icing/portable/gzip_stream.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/clock.h"
#include "icing/util/crc32.h"
#include "icing/util/document-util.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::Eq;
using ::testing::Gt;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::IsFalse;
using ::testing::IsNull;
using ::testing::IsTrue;
using ::testing::Lt;
using ::testing::Ne;
using ::testing::Not;
using ::testing::NotNull;
using ::testing::Pointee;
using ::testing::SizeIs;

using Crcs = PersistentStorage::Crcs;
using Info = QualifiedIdJoinIndexImplV3::Info;
using ArrayInfo = QualifiedIdJoinIndexImplV3::ArrayInfo;

class QualifiedIdJoinIndexImplV3Test : public ::testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(
        /*allow_circular_schema_definitions=*/true,
        /*enable_repeated_field_joins=*/true,
        /*enable_embedding_backup_generation=*/true,
        /*enable_optimize_improvements=*/true,
        /*expired_document_purge_threshold_ms=*/0,
        /*enable_non_existent_qualified_id_join=*/GetParam(),
        /*enable_skip_set_schema_type_equality_check=*/true,
        /*enable_schema_definition_deduping=*/true,
        /*enable_delete_propagation_from=*/true);

    base_dir_ = GetTestTempDir() + "/icing";
    working_path_ = base_dir_ + "/qualified_id_join_index_impl_v3";
    document_store_dir_ = base_dir_ + "/document_store";
    schema_store_dir_ = base_dir_ + "/schema_store";
    ASSERT_THAT(filesystem_.CreateDirectoryRecursively(base_dir_.c_str()),
                IsTrue());
    filesystem_.CreateDirectoryRecursively(document_store_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());
    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, schema_store_dir_,
                                           &clock_, feature_flags_.get()));
    ICING_ASSERT_OK(schema_store_->SetSchema(
        SchemaBuilder()
            .AddType(SchemaTypeConfigBuilder().SetType("type"))
            .Build(),
        /*ignore_errors_and_delete_documents=*/false));
    ASSERT_NO_FATAL_FAILURE(CreateDocumentStore());
  }

  void CreateDocumentStore() {
    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(
            &filesystem_, document_store_dir_, &clock_, schema_store_.get(),
            feature_flags_.get(),
            /*force_recovery_and_revalidate_documents=*/false,
            /*pre_mapping_fbv=*/false,
            /*use_persistent_hash_map=*/true,
            PortableFileBackedProtoLog<
                DocumentWrapper>::kDefaultCompressionLevel,
            PortableFileBackedProtoLog<
                DocumentWrapper>::kDefaultCompressionThresholdBytes,
            protobuf_ports::kDefaultMemLevel,
            /*initialize_stats=*/nullptr));
    document_store_ = std::move(create_result.document_store);
  }

  void OptimizeDocumentStore() {
    std::string optimized_document_store_dir =
        base_dir_ + "/document_store_optimized";
    ASSERT_THAT(filesystem_.CreateDirectoryRecursively(
                    optimized_document_store_dir.c_str()),
                IsTrue());
    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::OptimizeResult optimize_result,
        document_store_->OptimizeInto(
            optimized_document_store_dir, /*lang_segmenter=*/nullptr,
            /*potentially_optimizable_blob_handles=*/{}));
    document_store_.reset();
    ASSERT_THAT(filesystem_.SwapFiles(document_store_dir_.c_str(),
                                      optimized_document_store_dir.c_str()),
                IsTrue());
    ASSERT_NO_FATAL_FAILURE(CreateDocumentStore());
  }

  void FillDocumentStore(int num_documents) {
    for (int i = 0; i < num_documents; ++i) {
      ICING_ASSERT_OK(document_store_->Put(document_util::CreateDocumentWrapper(
          DocumentBuilder()
              .SetKey("ns", absl_ports::StrCat("uri", std::to_string(i)))
              .SetSchema("type")
              .Build())));
    }
  }

  void TearDown() override {
    document_store_.reset();
    schema_store_.reset();
    filesystem_.DeleteDirectoryRecursively(base_dir_.c_str());
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  Clock clock_;
  Filesystem filesystem_;
  std::string base_dir_;
  std::string working_path_;
  std::string document_store_dir_;
  std::string schema_store_dir_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
};

TEST_P(QualifiedIdJoinIndexImplV3Test, InvalidWorkingPath) {
  EXPECT_THAT(QualifiedIdJoinIndexImplV3::Create(
                  filesystem_, "/dev/null/qualified_id_join_index_impl_v3",
                  *feature_flags_),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL));
}

TEST_P(QualifiedIdJoinIndexImplV3Test, InitializeNewFiles) {
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
      Eq(QualifiedIdJoinIndexImplV3::kMetadataFileSize));

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
  if (!feature_flags_->enable_non_existent_qualified_id_join()) {
    // There are no data in FileBackedVectors, so storages_crc should be zero.
    EXPECT_THAT(crcs->component_crcs.storages_crc, Eq(0));
  }
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

TEST_P(QualifiedIdJoinIndexImplV3Test,
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

TEST_P(QualifiedIdJoinIndexImplV3Test,
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

TEST_P(QualifiedIdJoinIndexImplV3Test,
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

TEST_P(QualifiedIdJoinIndexImplV3Test,
       InitializationShouldFailIfMissingQualifiedIdMapperFile) {
  if (!feature_flags_->enable_non_existent_qualified_id_join()) {
    return;  // This test is only relevant when the feature is enabled.
  }

  {
    // Create new qualified id join index
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
        QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                           *feature_flags_));

    // Insert some data with a qualified id.
    ICING_ASSERT_OK(index->Put(
        document_store_.get(),
        DocumentJoinIdPair(/*document_id=*/100, /*joinable_property_id=*/20),
        /*parent_qualified_ids=*/
        std::vector<QualifiedId>{QualifiedId("namespace", "uri")}));
    ASSERT_THAT(index, Pointee(SizeIs(1)));

    ICING_ASSERT_OK(index->PersistToDisk());
  }

  // Manually delete the parent_qualified_id_to_child_array_info directory.
  const std::string dir_path = absl_ports::StrCat(
      working_path_, "/parent_qualified_id_to_child_array_info");
  ASSERT_THAT(filesystem_.DeleteDirectoryRecursively(dir_path.c_str()),
              IsTrue());

  // Attempt to create the qualified id join index. This should fail because of
  // checksum mismatch.
  EXPECT_THAT(QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                                 *feature_flags_),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION,
                       HasSubstr("Invalid storages crc")));
}

TEST_P(QualifiedIdJoinIndexImplV3Test,
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

TEST_P(QualifiedIdJoinIndexImplV3Test,
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
  EXPECT_THAT(index2->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
              IsOkAndHolds(ElementsAre(child_join_id_pair1)));
  EXPECT_THAT(index2->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/1),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
}

TEST_P(QualifiedIdJoinIndexImplV3Test,
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
  EXPECT_THAT(index2->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
              IsOkAndHolds(ElementsAre(child_join_id_pair1)));
  EXPECT_THAT(index2->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/1),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
}

TEST_P(QualifiedIdJoinIndexImplV3Test,
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
    EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
                IsOkAndHolds(ElementsAre(child_join_id_pair1)));
    EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/1),
                IsOkAndHolds(ElementsAre(child_join_id_pair2)));
  }
}

TEST_P(QualifiedIdJoinIndexImplV3Test,
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
                Eq(QualifiedIdJoinIndexImplV3::kMetadataFileSize));

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
  EXPECT_THAT(
      QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                         *feature_flags_),
      StatusIs(
          libtextclassifier3::StatusCode::FAILED_PRECONDITION,
          HasSubstr("Invalid header magic for QualifiedIdJoinIndexImplV3")));
}

TEST_P(QualifiedIdJoinIndexImplV3Test,
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
                Eq(QualifiedIdJoinIndexImplV3::kMetadataFileSize));

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

TEST_P(QualifiedIdJoinIndexImplV3Test,
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
                Eq(QualifiedIdJoinIndexImplV3::kMetadataFileSize));

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

TEST_P(
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

TEST_P(
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

TEST_P(QualifiedIdJoinIndexImplV3Test, Put) {
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
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
              IsOkAndHolds(ElementsAre(child_join_id_pair4)));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/1),
              IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair2,
                                       child_join_id_pair5)));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/2),
              IsOkAndHolds(ElementsAre(child_join_id_pair3)));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/3),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/4),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/5),
              IsOkAndHolds(ElementsAre(child_join_id_pair6)));
}

TEST_P(QualifiedIdJoinIndexImplV3Test,
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
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
  EXPECT_THAT(
      index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/1),
      IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair2)));
  EXPECT_THAT(
      index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/2),
      IsOkAndHolds(ElementsAre(child_join_id_pair2, child_join_id_pair3)));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/3),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/4),
              IsOkAndHolds(ElementsAre(child_join_id_pair1)));
  EXPECT_THAT(
      index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/5),
      IsOkAndHolds(ElementsAre(child_join_id_pair2, child_join_id_pair3)));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/6),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(
      index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/7),
      IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair3)));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/8),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/9),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/10),
              IsOkAndHolds(ElementsAre(child_join_id_pair1)));
}

TEST_P(QualifiedIdJoinIndexImplV3Test,
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
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
  EXPECT_THAT(
      index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/1),
      IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair2)));
  EXPECT_THAT(
      index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/2),
      IsOkAndHolds(ElementsAre(child_join_id_pair2, child_join_id_pair3)));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/3),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/4),
              IsOkAndHolds(ElementsAre(child_join_id_pair1)));
  EXPECT_THAT(
      index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/5),
      IsOkAndHolds(ElementsAre(child_join_id_pair2, child_join_id_pair3)));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/6),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(
      index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/7),
      IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair3)));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/8),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/9),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/10),
              IsOkAndHolds(ElementsAre(child_join_id_pair1)));
}

TEST_P(QualifiedIdJoinIndexImplV3Test,
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
    EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(parent_doc_id),
                IsOkAndHolds(IsEmpty()));
  }
  // Get API should return the child document for document 10.
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(kParentDocumentId),
              IsOkAndHolds(ElementsAre(child_join_id_pair)));
}

TEST_P(QualifiedIdJoinIndexImplV3Test,
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

  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(parent1),
              IsOkAndHolds(ElementsAreArray(child_join_id_pairs)));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(parent2),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
}

TEST_P(QualifiedIdJoinIndexImplV3Test,
       PutLargeParentShouldHandleAddressCorrectlyForRemap) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  const std::string array_working_path = absl_ports::StrCat(
      working_path_, "/parent_document_id_to_child_array_info");

  // Add a child for parent doc id 1 to the index.
  DocumentId parent_doc_id1 = 1;
  DocumentJoinIdPair child_join_id_pair1(/*document_id=*/100,
                                         /*joinable_property_id=*/0);
  EXPECT_THAT(
      index->Put(
          child_join_id_pair1,
          /*parent_document_ids=*/std::vector<DocumentId>{parent_doc_id1}),
      IsOk());
  int64_t file_size_before =
      filesystem_.GetFileSize(array_working_path.c_str());
  ASSERT_THAT(file_size_before, Ne(Filesystem::kBadFileSize));

  // Add another child with large parent document id to the index. This will
  // cause parent_document_id_to_child_array_info being extended and remap. The
  // test verifies that addresses after remap are handled correctly without
  // crashing.
  DocumentId parent_doc_id2 = 30000;
  DocumentJoinIdPair child_join_id_pair2(/*document_id=*/101,
                                         /*joinable_property_id=*/0);
  EXPECT_THAT(
      index->Put(
          child_join_id_pair2,
          /*parent_document_ids=*/std::vector<DocumentId>{parent_doc_id2}),
      IsOk());
  int64_t file_size_after = filesystem_.GetFileSize(array_working_path.c_str());
  ASSERT_THAT(file_size_after, Ne(Filesystem::kBadFileSize));

  // Sanity check that the file size is extended and remap happens.
  EXPECT_THAT(file_size_after, Gt(file_size_before));
}

TEST_P(QualifiedIdJoinIndexImplV3Test,
       PutLargeNumberOfDataShouldHandleRemapAddressCorrectly) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  DocumentId parent_doc_id = 0;
  DocumentId child_doc_id = 30000;

  // For the first grow of FBV, the file size is 65536. 12 bytes will be used
  // for the header, so we can fit (65536 - 12) / 4 = 16378 children.
  //
  // Add 16378 unique parent and child pairs, so we allocate 16378
  // DocumentJoinIdPair (extensible) arrays with size 1 for all parents, and
  // FBV is full now.
  constexpr int kNumChildrenToFillFbv = 16378;
  for (int i = 0; i < kNumChildrenToFillFbv; ++i) {
    DocumentJoinIdPair child_join_id_pair(child_doc_id++,
                                          /*joinable_property_id=*/20);
    EXPECT_THAT(
        index->Put(
            child_join_id_pair,
            /*parent_document_ids=*/std::vector<DocumentId>{parent_doc_id++}),
        IsOk());
  }

  // Put a child for parent doc_id=0 again. This will cause:
  // - FBV file size is extended to 131072, and remap happens.
  // - Parent 0's array is reallocated and extended to size 2.
  //
  // The test verifies that object related to mmap address is refreshed
  // correctly after remap.
  DocumentJoinIdPair additional_child_join_id_pair(child_doc_id++,
                                                   /*joinable_property_id=*/20);
  EXPECT_THAT(index->Put(additional_child_join_id_pair,
                         /*parent_document_ids=*/std::vector<DocumentId>{0}),
              IsOk());

  EXPECT_THAT(index, Pointee(SizeIs(kNumChildrenToFillFbv + 1)));
}

TEST_P(QualifiedIdJoinIndexImplV3Test, PutShouldSkipInvalidParentDocumentId) {
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
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/1),
              IsOkAndHolds(ElementsAre(child_join_id_pair)));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/2),
              IsOkAndHolds(ElementsAre(child_join_id_pair)));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/3),
              IsOkAndHolds(ElementsAre(child_join_id_pair)));
}

TEST_P(QualifiedIdJoinIndexImplV3Test,
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

TEST_P(QualifiedIdJoinIndexImplV3Test, DocumentJoinIdPairArrayView) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  // Add 2 children for parent document 0.
  DocumentJoinIdPair child_join_id_pair1(/*document_id=*/100,
                                         /*joinable_property_id=*/20);
  DocumentJoinIdPair child_join_id_pair2(/*document_id=*/101,
                                         /*joinable_property_id=*/2);
  EXPECT_THAT(index->Put(child_join_id_pair1,
                         /*parent_document_ids=*/std::vector<DocumentId>{0}),
              IsOk());
  EXPECT_THAT(index->Put(child_join_id_pair2,
                         /*parent_document_ids=*/std::vector<DocumentId>{0}),
              IsOk());

  EXPECT_THAT(index, Pointee(SizeIs(2)));

  // Get array view. Test each STL style method.
  ICING_ASSERT_OK_AND_ASSIGN(
      QualifiedIdJoinIndex::DocumentJoinIdPairArrayView array_view1,
      index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0));
  EXPECT_THAT(array_view1, Not(IsEmpty()));
  EXPECT_THAT(array_view1.data(), NotNull());
  EXPECT_THAT(array_view1, SizeIs(2));
  EXPECT_THAT(array_view1.begin(), NotNull());
  EXPECT_THAT(array_view1.end(), NotNull());
  EXPECT_THAT(array_view1,
              ElementsAre(child_join_id_pair1, child_join_id_pair2));

  // Add 1 more child for parent document 0.
  DocumentJoinIdPair child_join_id_pair3(/*document_id=*/102,
                                         /*joinable_property_id=*/2);
  EXPECT_THAT(index->Put(child_join_id_pair3,
                         /*parent_document_ids=*/std::vector<DocumentId>{0}),
              IsOk());

  // Get array view again. Test each STL style method.
  ICING_ASSERT_OK_AND_ASSIGN(
      QualifiedIdJoinIndex::DocumentJoinIdPairArrayView array_view2,
      index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0));
  EXPECT_THAT(array_view2, Not(IsEmpty()));
  EXPECT_THAT(array_view2.data(), NotNull());
  EXPECT_THAT(array_view2, SizeIs(3));
  EXPECT_THAT(array_view2.begin(), NotNull());
  EXPECT_THAT(array_view2.end(), NotNull());
  EXPECT_THAT(array_view2, ElementsAre(child_join_id_pair1, child_join_id_pair2,
                                       child_join_id_pair3));
}

TEST_P(QualifiedIdJoinIndexImplV3Test, EmptyDocumentJoinIdPairArrayView) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));
  EXPECT_THAT(index, Pointee(IsEmpty()));

  ICING_ASSERT_OK_AND_ASSIGN(
      QualifiedIdJoinIndex::DocumentJoinIdPairArrayView array_view,
      index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0));
  EXPECT_THAT(array_view, IsEmpty());
  EXPECT_THAT(array_view.data(), IsNull());
  EXPECT_THAT(array_view.size(), Eq(0));
  EXPECT_THAT(array_view.begin(), IsNull());
  EXPECT_THAT(array_view.end(), IsNull());

  // Use colon to iterate the array_view. There should be no crash and no-op.
  for (const DocumentJoinIdPair& _ : array_view) {
    ADD_FAILURE() << "Unexpectedly iterated the empty array_view.";
  }
}

TEST_P(QualifiedIdJoinIndexImplV3Test,
       GetDocumentJoinIdPairArrayView_emptyIndex) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));
  EXPECT_THAT(index, Pointee(IsEmpty()));

  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/1),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/2),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/3),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/4),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/5),
              IsOkAndHolds(IsEmpty()));
}

TEST_P(
    QualifiedIdJoinIndexImplV3Test,
    GetDocumentJoinIdPairArrayView_shouldReturnEmptyArrayViewForNonExistingLargeParent) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  DocumentJoinIdPair child_join_id_pair(/*document_id=*/100,
                                        /*joinable_property_id=*/20);
  ICING_ASSERT_OK(index->Put(
      child_join_id_pair, /*parent_document_ids=*/std::vector<DocumentId>{1}));
  EXPECT_THAT(index, Pointee(SizeIs(1)));

  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/1),
              IsOkAndHolds(ElementsAre(child_join_id_pair)));

  // Now, only parent document id 1 is in the index, so the FileBackedVector has
  // been resized to fit parent document id 1.
  // Get API for parent document id greater than 1 should return empty result
  // without accessing the FileBackedVector.
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/2),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/3),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(kMaxDocumentId),
              IsOkAndHolds(IsEmpty()));
}

TEST_P(
    QualifiedIdJoinIndexImplV3Test,
    GetDocumentJoinIdPairArrayView_shouldReturnEmptyArrayViewForParentWithNoChildren) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  // Add a child for parent document id 2.
  DocumentJoinIdPair child_join_id_pair(/*document_id=*/100,
                                        /*joinable_property_id=*/20);
  ICING_ASSERT_OK(index->Put(
      child_join_id_pair, /*parent_document_ids=*/std::vector<DocumentId>{2}));
  EXPECT_THAT(index, Pointee(SizeIs(1)));

  // Since parent array info FBV is resized to fit parent document id 2, parent
  // document 0 and 1 should also have array info element with invalid data
  // index for the 2nd FBV.
  //
  // Getting array view for parent document 0 and 1 should return empty result
  // when seeing invalid data index.
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/1),
              IsOkAndHolds(IsEmpty()));
}

TEST_P(
    QualifiedIdJoinIndexImplV3Test,
    GetDocumentJoinIdPairArrayView_shouldReturnInvalidArgumentErrorForInvalidParentDocumentId) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/-1),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(kInvalidDocumentId),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_P(QualifiedIdJoinIndexImplV3Test, MigrateParent) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  DocumentId parent_doc_id1 = 1;
  DocumentId parent_doc_id2 = 1024;

  // Add 2 children with their parents to the index.
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
      index->GetDocumentJoinIdPairArrayView(parent_doc_id1),
      IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair2)));
  ASSERT_THAT(index->GetDocumentJoinIdPairArrayView(parent_doc_id2),
              IsOkAndHolds(IsEmpty()));

  // Migrate parent document id 1 to 1024.
  EXPECT_THAT(index->MigrateParent(parent_doc_id1, parent_doc_id2), IsOk());
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(parent_doc_id1),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(
      index->GetDocumentJoinIdPairArrayView(parent_doc_id2),
      IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair2)));
}

TEST_P(QualifiedIdJoinIndexImplV3Test,
       MigrateParentToLargeIdShouldHandleAddressCorrectlyForRemap) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  const std::string array_working_path = absl_ports::StrCat(
      working_path_, "/parent_document_id_to_child_array_info");

  DocumentId parent_doc_id1 = 1;
  DocumentId parent_doc_id2 = 30000;

  // Add 2 children for parent doc id 1 to the index.
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
  int64_t file_size_before =
      filesystem_.GetFileSize(array_working_path.c_str());
  ASSERT_THAT(file_size_before, Ne(Filesystem::kBadFileSize));

  // Sanity check.
  ASSERT_THAT(index, Pointee(SizeIs(2)));
  ASSERT_THAT(
      index->GetDocumentJoinIdPairArrayView(parent_doc_id1),
      IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair2)));
  ASSERT_THAT(index->GetDocumentJoinIdPairArrayView(parent_doc_id2),
              IsOkAndHolds(IsEmpty()));

  // Migrate parent document id 1 to 30000. This will
  // cause parent_document_id_to_child_array_info being extended and remap. The
  // test verifies that addresses after remap are handled correctly without
  // crashing.
  EXPECT_THAT(index->MigrateParent(parent_doc_id1, parent_doc_id2), IsOk());
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(parent_doc_id1),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(
      index->GetDocumentJoinIdPairArrayView(parent_doc_id2),
      IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair2)));
  int64_t file_size_after = filesystem_.GetFileSize(array_working_path.c_str());
  ASSERT_THAT(file_size_after, Ne(Filesystem::kBadFileSize));

  // Sanity check that the file size is extended and remap happens.
  EXPECT_THAT(file_size_after, Gt(file_size_before));
}

TEST_P(QualifiedIdJoinIndexImplV3Test, MigrateParentShouldSetDirty) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  DocumentId parent_doc_id1 = 1;
  DocumentId parent_doc_id2 = 1024;

  // Add 2 children with their parents to the index.
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
      index->GetDocumentJoinIdPairArrayView(parent_doc_id1),
      IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair2)));
  ASSERT_THAT(index->GetDocumentJoinIdPairArrayView(parent_doc_id2),
              IsOkAndHolds(IsEmpty()));
  // PersistToDisk after putting data and get the checksum. This will reset the
  // dirty flag.
  ICING_ASSERT_OK(index->PersistToDisk());
  ICING_ASSERT_OK_AND_ASSIGN(Crc32 crc1, index->GetChecksum());

  // Migrate parent document id 1 to 1024.
  ICING_ASSERT_OK(index->MigrateParent(parent_doc_id1, parent_doc_id2));

  // Call UpdateChecksums(). The checksum should be recomputed and be different
  // from the previous one. This validates that MigrateParent() should set the
  // dirty flag.
  ICING_ASSERT_OK_AND_ASSIGN(Crc32 crc2, index->UpdateChecksums());
  EXPECT_THAT(crc2, Ne(crc1));

  // Create another qualified id join index instance with the same file. It
  // should succeed and GetChecksum() should return the same checksum as the
  // previous one.
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index2,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));
  ICING_ASSERT_OK_AND_ASSIGN(Crc32 crc3, index2->GetChecksum());
  EXPECT_THAT(crc3, Eq(crc2));
}

TEST_P(QualifiedIdJoinIndexImplV3Test, PutAndMigrateQualifiedId) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  DocumentJoinIdPair child_join_id_pair1(/*document_id=*/100,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair2(/*document_id=*/101,
                                         /*joinable_property_id=*/0);
  QualifiedId parent_qualified_id1("namespace", "uri1");
  QualifiedId parent_qualified_id2("namespace", "uri2");
  DocumentId parent_doc_id1 = 1;
  DocumentId parent_doc_id2 = 2;
  DocumentId parent_doc_id3 = 3;

  auto put_status1 = index->Put(
      document_store_.get(), child_join_id_pair1,
      /*parent_qualified_ids=*/std::vector<QualifiedId>{parent_qualified_id1});
  auto put_status2 = index->Put(
      document_store_.get(), child_join_id_pair2,
      /*parent_qualified_ids=*/
      std::vector<QualifiedId>{parent_qualified_id1, parent_qualified_id2});

  if (!feature_flags_->enable_non_existent_qualified_id_join()) {
    // If the flag is not enabled, Put should fail.
    EXPECT_THAT(put_status1,
                StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
    EXPECT_THAT(put_status2,
                StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
    EXPECT_THAT(index, Pointee(IsEmpty()));
    return;
  }

  ICING_ASSERT_OK(put_status1);
  ICING_ASSERT_OK(put_status2);

  // Sanity check after Put.
  EXPECT_THAT(index, Pointee(SizeIs(3)));

  // Migrate parent_qualified_id1 to parent_doc_id1.
  ICING_ASSERT_OK(index->MigrateParent(parent_qualified_id1, parent_doc_id1));

  // Verify that children are migrated to parent_doc_id1.
  EXPECT_THAT(
      index->GetDocumentJoinIdPairArrayView(parent_doc_id1),
      IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair2)));

  // Migrate parent_qualified_id2 to parent_doc_id2.
  ICING_ASSERT_OK(index->MigrateParent(parent_qualified_id2, parent_doc_id2));

  // Verify that children are migrated to parent_doc_id2.
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(parent_doc_id2),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));

  // parent_qualified_id1 and parent_qualified_id2 should still exist in the
  // qualified id mapper. Migrating them again to another parent_doc_id should
  // succeed.
  ICING_ASSERT_OK(index->MigrateParent(parent_qualified_id1, parent_doc_id3));
  EXPECT_THAT(
      index->GetDocumentJoinIdPairArrayView(parent_doc_id3),
      IsOkAndHolds(ElementsAre(child_join_id_pair1, child_join_id_pair2)));
  ICING_ASSERT_OK(index->MigrateParent(parent_qualified_id2, parent_doc_id3));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(parent_doc_id3),
              IsOkAndHolds(ElementsAre(child_join_id_pair2)));
}

TEST_P(QualifiedIdJoinIndexImplV3Test, SetLastAddedDocumentId) {
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

TEST_P(
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

TEST_P(QualifiedIdJoinIndexImplV3Test, Optimize) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  // Add documents for parent doc ids to the document store.
  ASSERT_NO_FATAL_FAILURE(FillDocumentStore(/*num_documents=*/5));

  // Create 4 parent and 7 child documents (with N to N joins):
  // - Document 1: 101, 103, 104, 105, 107
  // - Document 2: 102, 103, 105
  // - Document 3: 101, 106
  // - Document 4: 103
  // Add 7 children with their parents to the index.
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

  if (!feature_flags_->enable_non_existent_qualified_id_join()) {
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
  } else {
    // With the dual-tracking implementation, we need to use the Put overload
    // that takes qualified ids to populate the qualified id mapper, which is
    // the source of truth during Optimize().
    QualifiedId parent_qid1("ns", "uri1");
    QualifiedId parent_qid2("ns", "uri2");
    QualifiedId parent_qid3("ns", "uri3");
    QualifiedId parent_qid4("ns", "uri4");
    ICING_ASSERT_OK(
        index->Put(document_store_.get(), child_join_id_pair1,
                   /*parent_qualified_ids=*/
                   std::vector<QualifiedId>{parent_qid1, parent_qid3}));
    ICING_ASSERT_OK(index->Put(
        document_store_.get(), child_join_id_pair2,
        /*parent_qualified_ids=*/std::vector<QualifiedId>{parent_qid2}));
    ICING_ASSERT_OK(index->Put(
        document_store_.get(), child_join_id_pair3,
        /*parent_qualified_ids=*/
        std::vector<QualifiedId>{parent_qid1, parent_qid2, parent_qid4}));
    ICING_ASSERT_OK(index->Put(
        document_store_.get(), child_join_id_pair4,
        /*parent_qualified_ids=*/std::vector<QualifiedId>{parent_qid1}));
    ICING_ASSERT_OK(
        index->Put(document_store_.get(), child_join_id_pair5,
                   /*parent_qualified_ids=*/
                   std::vector<QualifiedId>{parent_qid1, parent_qid2}));
    ICING_ASSERT_OK(index->Put(
        document_store_.get(), child_join_id_pair6,
        /*parent_qualified_ids=*/std::vector<QualifiedId>{parent_qid3}));
    ICING_ASSERT_OK(index->Put(
        document_store_.get(), child_join_id_pair7,
        /*parent_qualified_ids=*/std::vector<QualifiedId>{parent_qid1}));
  }

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

  // Update the document store so that Join index can get the correct document
  // ids for parents.
  ICING_ASSERT_OK(document_store_->Delete("ns", "uri0", /*current_time_ms=*/0));
  ICING_ASSERT_OK(document_store_->Delete("ns", "uri3", /*current_time_ms=*/0));
  ASSERT_NO_FATAL_FAILURE(OptimizeDocumentStore());

  // Note: namespace_id_old_to_new is not used in
  // QualifiedIdJoinIndexImplV3::Optimize.
  DocumentId new_last_added_document_id = 15;
  EXPECT_THAT(index->Optimize(document_store_.get(), document_id_old_to_new,
                              /*namespace_id_old_to_new=*/{},
                              new_last_added_document_id),
              IsOk());
  if (!feature_flags_->enable_non_existent_qualified_id_join()) {
    EXPECT_THAT(index, Pointee(SizeIs(5)));
  } else {
    // Join index will no longer drop join relations for non-existent parents.
    EXPECT_THAT(index, Pointee(SizeIs(7)));
  }
  EXPECT_THAT(index->last_added_document_id(), Eq(new_last_added_document_id));

  // Verify document 0 (originally document 1)
  // - Child docs 101, 104, 105 become 11, 13, 14.
  // - Child docs 103, 107 are deleted.
  EXPECT_THAT(
      index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(/*document_id=*/11, /*joinable_property_id=*/0),
          DocumentJoinIdPair(/*document_id=*/13, /*joinable_property_id=*/0),
          DocumentJoinIdPair(/*document_id=*/14, /*joinable_property_id=*/0))));

  // Verify document 1 (originally document 2)
  // - Child docs 102, 105 become 12, 14.
  // - Child doc 103 is deleted.
  EXPECT_THAT(
      index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/1),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(/*document_id=*/12, /*joinable_property_id=*/0),
          DocumentJoinIdPair(/*document_id=*/14, /*joinable_property_id=*/0))));

  // Verify document 2 (originally document 4)
  // - Child doc 103 is deleted.
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/2),
              IsOkAndHolds(IsEmpty()));

  // Verify document 3 and 4:
  // - These 2 doc ids don't exist after optimize.
  // - The relations for the original document 3 and 4 should be deleted.
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/3),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/4),
              IsOkAndHolds(IsEmpty()));

  // Verify Put API should work normally after Optimize().
  DocumentJoinIdPair another_child_join_id_pair(/*document_id=*/16,
                                                /*joinable_property_id=*/0);
  EXPECT_THAT(
      index->Put(another_child_join_id_pair,
                 /*parent_document_ids=*/std::vector<DocumentId>{0, 2, 3}),
      IsOk());
  index->set_last_added_document_id(16);

  if (!feature_flags_->enable_non_existent_qualified_id_join()) {
    EXPECT_THAT(index, Pointee(SizeIs(8)));
  } else {
    // Join index will no longer drop join relations for non-existent parents.
    EXPECT_THAT(index, Pointee(SizeIs(10)));
  }
  EXPECT_THAT(index->last_added_document_id(), Eq(16));
  EXPECT_THAT(
      index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
      IsOkAndHolds(ElementsAre(DocumentJoinIdPair(/*document_id=*/11,
                                                  /*joinable_property_id=*/0),
                               DocumentJoinIdPair(/*document_id=*/13,
                                                  /*joinable_property_id=*/0),
                               DocumentJoinIdPair(/*document_id=*/14,
                                                  /*joinable_property_id=*/0),
                               another_child_join_id_pair)));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/2),
              IsOkAndHolds(ElementsAre(another_child_join_id_pair)));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/3),
              IsOkAndHolds(ElementsAre(another_child_join_id_pair)));
}

TEST_P(QualifiedIdJoinIndexImplV3Test, OptimizeOutOfRangeParentDocumentId) {
  if (feature_flags_->enable_non_existent_qualified_id_join()) {
    // Not applicable for the dual-tracking implementation. Optimize() will use
    // the qualified id mapper as the source of truth, and parent document ids
    // will be read from the document store directly using qualified ids,
    // instead of document_id_old_to_new.
    return;
  }

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

  // Create document_id_old_to_new with size = 109 (from index 0 to 108), which
  // makes parent document 120 out of range.
  //
  // Optimize should return internal error for out of range parent document id
  // without crashing.
  std::vector<DocumentId> document_id_old_to_new(109, kInvalidDocumentId);
  document_id_old_to_new[1] = 0;
  document_id_old_to_new[101] = 11;
  document_id_old_to_new[106] = 12;

  // Note: namespace_id_old_to_new is not used in
  // QualifiedIdJoinIndexImplV3::Optimize.
  DocumentId new_last_added_document_id = 12;
  EXPECT_THAT(
      index->Optimize(document_store_.get(), document_id_old_to_new,
                      /*namespace_id_old_to_new=*/{},
                      new_last_added_document_id),
      StatusIs(libtextclassifier3::StatusCode::INTERNAL,
               HasSubstr("Qualified id join index data parent document id is "
                         "out of range. The index may have been corrupted.")));
}

TEST_P(QualifiedIdJoinIndexImplV3Test, OptimizeOutOfRangeChildDocumentId) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  // Add documents for parent doc ids to the document store.
  ASSERT_NO_FATAL_FAILURE(FillDocumentStore(/*num_documents=*/3));

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
  if (!feature_flags_->enable_non_existent_qualified_id_join()) {
    ICING_ASSERT_OK(
        index->Put(child_join_id_pair1,
                   /*parent_document_ids=*/std::vector<DocumentId>{1, 2}));
    ICING_ASSERT_OK(
        index->Put(child_join_id_pair2,
                   /*parent_document_ids=*/std::vector<DocumentId>{1}));
    ICING_ASSERT_OK(
        index->Put(child_join_id_pair3,
                   /*parent_document_ids=*/std::vector<DocumentId>{1}));
  } else {
    QualifiedId parent_qid1("ns", "uri1");
    QualifiedId parent_qid2("ns", "uri2");
    ICING_ASSERT_OK(
        index->Put(document_store_.get(), child_join_id_pair1,
                   /*parent_qualified_ids=*/
                   std::vector<QualifiedId>{parent_qid1, parent_qid2}));
    ICING_ASSERT_OK(index->Put(
        document_store_.get(), child_join_id_pair2,
        /*parent_qualified_ids=*/std::vector<QualifiedId>{parent_qid1}));
    ICING_ASSERT_OK(index->Put(
        document_store_.get(), child_join_id_pair3,
        /*parent_qualified_ids=*/std::vector<QualifiedId>{parent_qid1}));
  }

  ASSERT_THAT(index, Pointee(SizeIs(4)));
  index->set_last_added_document_id(120);
  ASSERT_THAT(index->last_added_document_id(), Eq(120));

  // Create document_id_old_to_new with size = 107 (from index 0 to 106), which
  // makes child document 108 out of range.
  //
  // Optimize should return internal error for out of range child document id
  // without crashing.
  std::vector<DocumentId> document_id_old_to_new(107, kInvalidDocumentId);
  document_id_old_to_new[1] = 0;
  document_id_old_to_new[101] = 11;
  document_id_old_to_new[106] = 12;

  // Update the document store so that Join index can get the correct document
  // ids for parents.
  ICING_ASSERT_OK(document_store_->Delete("ns", "uri0", /*current_time_ms=*/0));
  ASSERT_NO_FATAL_FAILURE(OptimizeDocumentStore());

  // Note: namespace_id_old_to_new is not used in
  // QualifiedIdJoinIndexImplV3::Optimize.
  DocumentId new_last_added_document_id = 12;
  EXPECT_THAT(
      index->Optimize(document_store_.get(), document_id_old_to_new,
                      /*namespace_id_old_to_new=*/{},
                      new_last_added_document_id),
      StatusIs(libtextclassifier3::StatusCode::INTERNAL,
               HasSubstr("Qualified id join index data child document id is "
                         "out of range. The index may have been corrupted.")));
}

TEST_P(QualifiedIdJoinIndexImplV3Test, OptimizeDeleteAllDocuments) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  // Add documents for parent doc ids to the document store.
  ASSERT_NO_FATAL_FAILURE(FillDocumentStore(/*num_documents=*/5));

  // Create 4 parent and 7 child documents (with N to N joins):
  // - Document 1: 101, 103, 104, 105, 107
  // - Document 2: 102, 103, 105
  // - Document 3: 101, 106
  // - Document 4: 103
  // Add 7 children with their parents to the index.
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

  if (!feature_flags_->enable_non_existent_qualified_id_join()) {
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
  } else {
    QualifiedId parent_qid1("ns", "uri1");
    QualifiedId parent_qid2("ns", "uri2");
    QualifiedId parent_qid3("ns", "uri3");
    QualifiedId parent_qid4("ns", "uri4");
    ICING_ASSERT_OK(
        index->Put(document_store_.get(), child_join_id_pair1,
                   /*parent_qualified_ids=*/
                   std::vector<QualifiedId>{parent_qid1, parent_qid3}));
    ICING_ASSERT_OK(index->Put(
        document_store_.get(), child_join_id_pair2,
        /*parent_qualified_ids=*/std::vector<QualifiedId>{parent_qid2}));
    ICING_ASSERT_OK(index->Put(
        document_store_.get(), child_join_id_pair3,
        /*parent_qualified_ids=*/
        std::vector<QualifiedId>{parent_qid1, parent_qid2, parent_qid4}));
    ICING_ASSERT_OK(index->Put(
        document_store_.get(), child_join_id_pair4,
        /*parent_qualified_ids=*/std::vector<QualifiedId>{parent_qid1}));
    ICING_ASSERT_OK(
        index->Put(document_store_.get(), child_join_id_pair5,
                   /*parent_qualified_ids=*/
                   std::vector<QualifiedId>{parent_qid1, parent_qid2}));
    ICING_ASSERT_OK(index->Put(
        document_store_.get(), child_join_id_pair6,
        /*parent_qualified_ids=*/std::vector<QualifiedId>{parent_qid3}));
    ICING_ASSERT_OK(index->Put(
        document_store_.get(), child_join_id_pair7,
        /*parent_qualified_ids=*/std::vector<QualifiedId>{parent_qid1}));
  }

  ASSERT_THAT(index, Pointee(SizeIs(11)));
  index->set_last_added_document_id(107);
  ASSERT_THAT(index->last_added_document_id(), Eq(107));

  // Delete all documents.
  std::vector<DocumentId> document_id_old_to_new(108, kInvalidDocumentId);

  // Update the document store so that Join index can get the correct document
  // ids for parents.
  ICING_ASSERT_OK(document_store_->Delete("ns", "uri0", /*current_time_ms=*/0));
  ICING_ASSERT_OK(document_store_->Delete("ns", "uri1", /*current_time_ms=*/0));
  ICING_ASSERT_OK(document_store_->Delete("ns", "uri2", /*current_time_ms=*/0));
  ICING_ASSERT_OK(document_store_->Delete("ns", "uri3", /*current_time_ms=*/0));
  ICING_ASSERT_OK(document_store_->Delete("ns", "uri4", /*current_time_ms=*/0));
  ASSERT_NO_FATAL_FAILURE(OptimizeDocumentStore());

  // Note: namespace_id_old_to_new is not used in
  // QualifiedIdJoinIndexImplV3::Optimize.
  DocumentId new_last_added_document_id = kInvalidDocumentId;
  EXPECT_THAT(index->Optimize(document_store_.get(), document_id_old_to_new,
                              /*namespace_id_old_to_new=*/{},
                              new_last_added_document_id),
              IsOk());
  EXPECT_THAT(index, Pointee(IsEmpty()));
  EXPECT_THAT(index->last_added_document_id(), Eq(new_last_added_document_id));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/1),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/2),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/3),
              IsOkAndHolds(IsEmpty()));
}

TEST_P(QualifiedIdJoinIndexImplV3Test, OptimizeWithMissingParents) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  // Add a document for the existing parent.
  // Doc id 0: dummy
  ICING_ASSERT_OK(document_store_->Put(document_util::CreateDocumentWrapper(
      DocumentBuilder().SetKey("ns", "uri0").SetSchema("type").Build())));
  // Doc id 1
  ICING_ASSERT_OK(document_store_->Put(
      document_util::CreateDocumentWrapper(DocumentBuilder()
                                               .SetKey("ns", "uri1_existing")
                                               .SetSchema("type")
                                               .Build())));

  // Add join data with both regular and missing parents.
  DocumentId parent_doc_id1 = 1;
  QualifiedId parent_qid1("ns", "uri1_existing");
  QualifiedId missing_parent_qualified_id1("namespace", "uri1_missing");
  QualifiedId missing_parent_qualified_id2("namespace", "uri2_missing");

  DocumentJoinIdPair child_join_id_pair1(/*document_id=*/101,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair2(/*document_id=*/102,
                                         /*joinable_property_id=*/0);
  DocumentJoinIdPair child_join_id_pair3(/*document_id=*/103,
                                         /*joinable_property_id=*/0);

  if (!feature_flags_->enable_non_existent_qualified_id_join()) {
    ICING_ASSERT_OK(index->Put(
        child_join_id_pair1,
        /*parent_document_ids=*/std::vector<DocumentId>{parent_doc_id1}));
  } else {
    ICING_ASSERT_OK(index->Put(
        document_store_.get(), child_join_id_pair1,
        /*parent_qualified_ids=*/std::vector<QualifiedId>{parent_qid1}));
  }
  auto missing_parent1_put_status =
      index->Put(document_store_.get(), child_join_id_pair2,
                 /*parent_qualified_ids=*/
                 std::vector<QualifiedId>{missing_parent_qualified_id1});
  auto missing_parent2_put_status =
      index->Put(document_store_.get(), child_join_id_pair3,
                 /*parent_qualified_ids=*/
                 std::vector<QualifiedId>{missing_parent_qualified_id1,
                                          missing_parent_qualified_id2});

  if (!feature_flags_->enable_non_existent_qualified_id_join()) {
    // If the flag is not enabled, put should fail.
    EXPECT_THAT(missing_parent1_put_status,
                StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
    EXPECT_THAT(missing_parent2_put_status,
                StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
    ASSERT_THAT(index, Pointee(SizeIs(1)));
  } else {
    ICING_ASSERT_OK(missing_parent1_put_status);
    ICING_ASSERT_OK(missing_parent2_put_status);
    ASSERT_THAT(index, Pointee(SizeIs(4)));
  }
  index->set_last_added_document_id(103);
  ASSERT_THAT(index->last_added_document_id(), Eq(103));

  // Remap doc ids. Delete child 102.
  std::vector<DocumentId> document_id_old_to_new(104, kInvalidDocumentId);
  document_id_old_to_new[1] = 0;     // parent_doc_id1 -> 0
  document_id_old_to_new[101] = 11;  // child_join_id_pair1 -> 11
  document_id_old_to_new[103] = 13;  // child_join_id_pair3 -> 13

  // Update the document store so that Join index can get the correct document
  // ids for parents.
  ICING_ASSERT_OK(document_store_->Delete("ns", "uri0", /*current_time_ms=*/0));
  ASSERT_NO_FATAL_FAILURE(OptimizeDocumentStore());

  DocumentId new_last_added_document_id = 13;
  ICING_ASSERT_OK(index->Optimize(document_store_.get(), document_id_old_to_new,
                                  /*namespace_id_old_to_new=*/{},
                                  new_last_added_document_id));

  // Verify parent_doc_id1 (now 0).
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
              IsOkAndHolds(ElementsAre(DocumentJoinIdPair(
                  /*document_id=*/11, /*joinable_property_id=*/0))));

  if (!feature_flags_->enable_non_existent_qualified_id_join()) {
    // Nothing more to check if the flag is disabled.
    EXPECT_THAT(index, Pointee(SizeIs(1)));
    return;
  }

  EXPECT_THAT(index, Pointee(SizeIs(3)));  // 1 from regular, 2 from missing

  // Migrate missing parents and verify.
  DocumentId new_parent_doc_id1 = 1;
  DocumentId new_parent_doc_id2 = 2;
  ICING_ASSERT_OK(
      index->MigrateParent(missing_parent_qualified_id1, new_parent_doc_id1));
  ICING_ASSERT_OK(
      index->MigrateParent(missing_parent_qualified_id2, new_parent_doc_id2));

  // child 102 was deleted, so missing_parent1 should only have child 103 (now
  // 13).
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(new_parent_doc_id1),
              IsOkAndHolds(ElementsAre(DocumentJoinIdPair(
                  /*document_id=*/13, /*joinable_property_id=*/0))));
  // missing_parent2 had child 103 (now 13).
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(new_parent_doc_id2),
              IsOkAndHolds(ElementsAre(DocumentJoinIdPair(
                  /*document_id=*/13, /*joinable_property_id=*/0))));
}

TEST_P(QualifiedIdJoinIndexImplV3Test,
       OptimizeShouldKeepJoinDataOfDeletedParents) {
  if (!feature_flags_->enable_non_existent_qualified_id_join()) {
    return;
  }

  // Index a parent document.
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  QualifiedId parent_qualified_id("ns", "uri0");
  // Doc id 0
  ICING_ASSERT_OK(document_store_->Put(document_util::CreateDocumentWrapper(
      DocumentBuilder().SetKey("ns", "uri0").SetSchema("type").Build())));
  // Doc id 1
  ICING_ASSERT_OK(document_store_->Put(document_util::CreateDocumentWrapper(
      DocumentBuilder().SetKey("ns", "uri1").SetSchema("type").Build())));

  auto doc_id_or = document_store_->GetDocumentId("ns", "uri0");
  ASSERT_THAT(doc_id_or, IsOk());
  DocumentId parent_doc_id = doc_id_or.ValueOrDie();
  ASSERT_THAT(parent_doc_id, Eq(0));

  // Index a join relation.
  DocumentJoinIdPair child_join_id_pair(/*document_id=*/100,
                                        /*joinable_property_id=*/0);
  ICING_ASSERT_OK(index->Put(
      document_store_.get(), child_join_id_pair,
      /*parent_qualified_ids=*/std::vector<QualifiedId>{parent_qualified_id}));

  // Verify the join relation.
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(parent_doc_id),
              IsOkAndHolds(ElementsAre(child_join_id_pair)));

  // Delete the parent document.
  ICING_ASSERT_OK(document_store_->Delete("ns", "uri0", /*current_time_ms=*/0));

  // Run Optimize.
  // After optimize, doc "ns", "uri1" will be doc 0.
  std::vector<DocumentId> document_id_old_to_new(101, kInvalidDocumentId);
  document_id_old_to_new[1] = 0;     // "ns", "uri1" moved to docid 0.
  document_id_old_to_new[100] = 99;  // child doc 100 becomes 99.
  ASSERT_NO_FATAL_FAILURE(OptimizeDocumentStore());

  DocumentId new_last_added_document_id = 99;
  ICING_ASSERT_OK(index->Optimize(document_store_.get(), document_id_old_to_new,
                                  /*namespace_id_old_to_new=*/{},
                                  new_last_added_document_id));

  // The new document with docid 0 is the old document 1, which was not a
  // parent.
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(0),
              IsOkAndHolds(IsEmpty()));

  // Migrating the parent to a new doc id and check the child is still there.
  DocumentId new_parent_doc_id = 1;
  ICING_ASSERT_OK(index->MigrateParent(parent_qualified_id, new_parent_doc_id));
  DocumentJoinIdPair new_child_join_id_pair(/*document_id=*/99,
                                            /*joinable_property_id=*/0);
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(new_parent_doc_id),
              IsOkAndHolds(ElementsAre(new_child_join_id_pair)));
}

TEST_P(QualifiedIdJoinIndexImplV3Test,
       OptimizeShouldAllowAddingChildrenToDeletedParents) {
  if (!feature_flags_->enable_non_existent_qualified_id_join()) {
    return;
  }

  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  QualifiedId parent_qualified_id("ns", "uri0");
  // Doc id 0
  ICING_ASSERT_OK(document_store_->Put(document_util::CreateDocumentWrapper(
      DocumentBuilder().SetKey("ns", "uri0").SetSchema("type").Build())));
  ASSERT_THAT(document_store_->GetDocumentId("ns", "uri0"), IsOkAndHolds(0));

  // Index a join relation.
  DocumentJoinIdPair child1(/*document_id=*/100, /*joinable_property_id=*/0);
  ICING_ASSERT_OK(index->Put(
      document_store_.get(), child1,
      /*parent_qualified_ids=*/std::vector<QualifiedId>{parent_qualified_id}));

  // Delete the parent document.
  ICING_ASSERT_OK(document_store_->Delete("ns", "uri0", /*current_time_ms=*/0));

  // Run Optimize.
  std::vector<DocumentId> document_id_old_to_new(101, kInvalidDocumentId);
  document_id_old_to_new[100] = 99;  // child doc 100 becomes 99.
  ASSERT_NO_FATAL_FAILURE(OptimizeDocumentStore());

  DocumentId new_last_added_document_id = 99;
  ICING_ASSERT_OK(index->Optimize(document_store_.get(), document_id_old_to_new,
                                  /*namespace_id_old_to_new=*/{},
                                  new_last_added_document_id));

  // Index a new child for the deleted parent.
  DocumentJoinIdPair child2(/*document_id=*/101, /*joinable_property_id=*/0);
  ICING_ASSERT_OK(index->Put(
      document_store_.get(), child2,
      /*parent_qualified_ids=*/std::vector<QualifiedId>{parent_qualified_id}));

  // Migrating the parent to a new doc id and check both children are there.
  DocumentId new_parent_doc_id = 1;
  ICING_ASSERT_OK(index->MigrateParent(parent_qualified_id, new_parent_doc_id));
  DocumentJoinIdPair remapped_child1(/*document_id=*/99,
                                     /*joinable_property_id=*/0);
  ICING_ASSERT_OK_AND_ASSIGN(
      auto view, index->GetDocumentJoinIdPairArrayView(new_parent_doc_id));
  EXPECT_THAT(view, ElementsAre(remapped_child1, child2));
}

TEST_P(QualifiedIdJoinIndexImplV3Test, Clear) {
  // Create new qualified id join index
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  // Add 4 children with their parents to the index.
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
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/0),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/1),
              IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/2),
              IsOkAndHolds(IsEmpty()));

  // Join index should be able to work normally after Clear().
  EXPECT_THAT(index->Put(child_join_id_pair4,
                         /*parent_document_ids=*/std::vector<DocumentId>{5}),
              IsOk());
  index->set_last_added_document_id(105);

  EXPECT_THAT(index, Pointee(SizeIs(1)));
  EXPECT_THAT(index->last_added_document_id(), Eq(105));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/5),
              IsOkAndHolds(ElementsAre(child_join_id_pair4)));

  ICING_ASSERT_OK(index->PersistToDisk());
  index.reset();

  // Verify index after reconstructing.
  ICING_ASSERT_OK_AND_ASSIGN(
      index, QualifiedIdJoinIndexImplV3::Create(filesystem_, working_path_,
                                                *feature_flags_));
  EXPECT_THAT(index, Pointee(SizeIs(1)));
  EXPECT_THAT(index->last_added_document_id(), Eq(105));
  EXPECT_THAT(index->GetDocumentJoinIdPairArrayView(/*parent_document_id=*/5),
              IsOkAndHolds(ElementsAre(child_join_id_pair4)));
}

TEST_P(QualifiedIdJoinIndexImplV3Test, V2ApiShouldBeUnimplemented) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<QualifiedIdJoinIndexImplV3> index,
                             QualifiedIdJoinIndexImplV3::Create(
                                 filesystem_, working_path_, *feature_flags_));

  EXPECT_THAT(
      index->Put(/*schema_type_id=*/0, /*joinable_property_id=*/0,
                 /*document_id=*/0, /*ref_namespace_id_uri_fingerprints=*/{}),
      StatusIs(libtextclassifier3::StatusCode::UNIMPLEMENTED));

  EXPECT_THAT(index->GetIterator(/*schema_type_id=*/0,
                                 /*joinable_property_id=*/0),
              StatusIs(libtextclassifier3::StatusCode::UNIMPLEMENTED));
}

INSTANTIATE_TEST_SUITE_P(QualifiedIdJoinIndexImplV3Test,
                         QualifiedIdJoinIndexImplV3Test,
                         // Parameter: enable_non_existent_qualified_id_join
                         testing::Values(true, false));

}  // namespace

}  // namespace lib
}  // namespace icing
