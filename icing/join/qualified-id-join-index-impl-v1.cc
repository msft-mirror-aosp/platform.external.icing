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

#include "icing/join/qualified-id-join-index-impl-v1.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/file/destructible-directory.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/join/document-join-id-pair.h"
#include "icing/join/qualified-id-join-index.h"
#include "icing/store/document-id.h"
#include "icing/store/dynamic-trie-key-mapper.h"
#include "icing/store/key-mapper.h"
#include "icing/store/namespace-id.h"
#include "icing/store/persistent-hash-map-key-mapper.h"
#include "icing/util/crc32.h"
#include "icing/util/encode-util.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

// Set 1M for max # of qualified id entries and 10 bytes for key-value bytes.
// This will take at most 23 MiB disk space and mmap for persistent hash map.
static constexpr int32_t kDocumentJoinIdPairMapperMaxNumEntries = 1 << 20;
static constexpr int32_t kDocumentJoinIdPairMapperAverageKVByteSize = 10;

static constexpr int32_t kDocumentJoinIdPairMapperDynamicTrieMaxSize =
    128 * 1024 * 1024;  // 128 MiB

DocumentId GetNewDocumentId(
    const std::vector<DocumentId>& document_id_old_to_new,
    DocumentId old_document_id) {
  if (old_document_id >= document_id_old_to_new.size()) {
    return kInvalidDocumentId;
  }
  return document_id_old_to_new[old_document_id];
}

std::string GetMetadataFilePath(std::string_view working_path) {
  return absl_ports::StrCat(working_path, "/metadata");
}

std::string GetDocumentJoinIdPairMapperPath(std::string_view working_path) {
  // Use the old directory name "doc_join_info_mapper" to avoid rebuild. We just
  // changed the class, variable and function name, so there is no need to
  // change the directory name or rebuild the index.
  return absl_ports::StrCat(working_path, "/doc_join_info_mapper");
}

std::string GetQualifiedIdStoragePath(std::string_view working_path) {
  return absl_ports::StrCat(working_path, "/qualified_id_storage");
}

}  // namespace

/* static */ libtextclassifier3::StatusOr<
    std::unique_ptr<QualifiedIdJoinIndexImplV1>>
QualifiedIdJoinIndexImplV1::Create(const Filesystem& filesystem,
                                   std::string working_path,
                                   bool pre_mapping_fbv,
                                   bool use_persistent_hash_map) {
  if (!filesystem.FileExists(GetMetadataFilePath(working_path).c_str()) ||
      !filesystem.DirectoryExists(
          GetDocumentJoinIdPairMapperPath(working_path).c_str()) ||
      !filesystem.FileExists(GetQualifiedIdStoragePath(working_path).c_str())) {
    // Discard working_path if any file/directory is missing, and reinitialize.
    if (filesystem.DirectoryExists(working_path.c_str())) {
      ICING_RETURN_IF_ERROR(
          QualifiedIdJoinIndex::Discard(filesystem, working_path));
    }
    return InitializeNewFiles(filesystem, std::move(working_path),
                              pre_mapping_fbv, use_persistent_hash_map);
  }
  return InitializeExistingFiles(filesystem, std::move(working_path),
                                 pre_mapping_fbv, use_persistent_hash_map);
}

QualifiedIdJoinIndexImplV1::~QualifiedIdJoinIndexImplV1() {
  if (!PersistToDisk().ok()) {
    ICING_LOG(WARNING) << "Failed to persist qualified id type joinable index "
                          "to disk while destructing "
                       << working_path_;
  }
}

libtextclassifier3::Status QualifiedIdJoinIndexImplV1::Put(
    const DocumentJoinIdPair& document_join_id_pair,
    std::string_view ref_qualified_id_str) {
  SetDirty();

  if (!document_join_id_pair.is_valid()) {
    return absl_ports::InvalidArgumentError(
        "Cannot put data for an invalid DocumentJoinIdPair");
  }

  int32_t qualified_id_index = qualified_id_storage_->num_elements();
  ICING_ASSIGN_OR_RETURN(
      FileBackedVector<char>::MutableArrayView mutable_arr,
      qualified_id_storage_->Allocate(ref_qualified_id_str.size() + 1));
  mutable_arr.SetArray(/*idx=*/0, ref_qualified_id_str.data(),
                       ref_qualified_id_str.size());
  mutable_arr.SetArray(/*idx=*/ref_qualified_id_str.size(), /*arr=*/"\0",
                       /*arr_len=*/1);

  ICING_RETURN_IF_ERROR(document_join_id_pair_mapper_->Put(
      encode_util::EncodeIntToCString(document_join_id_pair.value()),
      qualified_id_index));

  // TODO(b/268521214): add data into delete propagation storage

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<std::string_view> QualifiedIdJoinIndexImplV1::Get(
    const DocumentJoinIdPair& document_join_id_pair) const {
  if (!document_join_id_pair.is_valid()) {
    return absl_ports::InvalidArgumentError(
        "Cannot get data for an invalid DocumentJoinIdPair");
  }

  ICING_ASSIGN_OR_RETURN(
      int32_t qualified_id_index,
      document_join_id_pair_mapper_->Get(
          encode_util::EncodeIntToCString(document_join_id_pair.value())));

  const char* data = qualified_id_storage_->array() + qualified_id_index;
  return std::string_view(data, strlen(data));
}

libtextclassifier3::Status QualifiedIdJoinIndexImplV1::Optimize(
    const std::vector<DocumentId>& document_id_old_to_new,
    const std::vector<NamespaceId>& namespace_id_old_to_new,
    DocumentId new_last_added_document_id) {
  std::string temp_working_path = working_path_ + "_temp";
  ICING_RETURN_IF_ERROR(
      QualifiedIdJoinIndex::Discard(filesystem_, temp_working_path));

  DestructibleDirectory temp_working_path_ddir(&filesystem_,
                                               std::move(temp_working_path));
  if (!temp_working_path_ddir.is_valid()) {
    return absl_ports::InternalError(
        "Unable to create temp directory to build new qualified id type "
        "joinable index");
  }

  {
    // Transfer all data from the current to new qualified id type joinable
    // index. Also PersistToDisk and destruct the instance after finishing, so
    // we can safely swap directories later.
    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<QualifiedIdJoinIndexImplV1> new_index,
        Create(filesystem_, temp_working_path_ddir.dir(), pre_mapping_fbv_,
               use_persistent_hash_map_));
    ICING_RETURN_IF_ERROR(
        TransferIndex(document_id_old_to_new, new_index.get()));
    new_index->set_last_added_document_id(new_last_added_document_id);
    ICING_RETURN_IF_ERROR(new_index->PersistToDisk());
  }

  // Destruct current index's storage instances to safely swap directories.
  // TODO(b/268521214): handle delete propagation storage
  document_join_id_pair_mapper_.reset();
  qualified_id_storage_.reset();

  if (!filesystem_.SwapFiles(temp_working_path_ddir.dir().c_str(),
                             working_path_.c_str())) {
    return absl_ports::InternalError(
        "Unable to apply new qualified id type joinable index due to failed "
        "swap");
  }

  // Reinitialize qualified id type joinable index.
  if (!filesystem_.PRead(GetMetadataFilePath(working_path_).c_str(),
                         metadata_buffer_.get(), kMetadataFileSize,
                         /*offset=*/0)) {
    return absl_ports::InternalError("Fail to read metadata file");
  }
  if (use_persistent_hash_map_) {
    ICING_ASSIGN_OR_RETURN(
        document_join_id_pair_mapper_,
        PersistentHashMapKeyMapper<int32_t>::Create(
            filesystem_, GetDocumentJoinIdPairMapperPath(working_path_),
            pre_mapping_fbv_,
            /*max_num_entries=*/kDocumentJoinIdPairMapperMaxNumEntries,
            /*average_kv_byte_size=*/
            kDocumentJoinIdPairMapperAverageKVByteSize));
  } else {
    ICING_ASSIGN_OR_RETURN(
        document_join_id_pair_mapper_,
        DynamicTrieKeyMapper<int32_t>::Create(
            filesystem_, GetDocumentJoinIdPairMapperPath(working_path_),
            kDocumentJoinIdPairMapperDynamicTrieMaxSize));
  }

  ICING_ASSIGN_OR_RETURN(
      qualified_id_storage_,
      FileBackedVector<char>::Create(
          filesystem_, GetQualifiedIdStoragePath(working_path_),
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
          FileBackedVector<char>::kMaxFileSize,
          /*pre_mapping_mmap_size=*/pre_mapping_fbv_ ? 1024 * 1024 : 0));

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status QualifiedIdJoinIndexImplV1::Clear() {
  SetDirty();

  document_join_id_pair_mapper_.reset();
  // Discard and reinitialize doc join info mapper.
  std::string document_join_id_pair_mapper_path =
      GetDocumentJoinIdPairMapperPath(working_path_);
  if (use_persistent_hash_map_) {
    ICING_RETURN_IF_ERROR(PersistentHashMapKeyMapper<int32_t>::Delete(
        filesystem_, document_join_id_pair_mapper_path));
    ICING_ASSIGN_OR_RETURN(
        document_join_id_pair_mapper_,
        PersistentHashMapKeyMapper<int32_t>::Create(
            filesystem_, std::move(document_join_id_pair_mapper_path),
            pre_mapping_fbv_,
            /*max_num_entries=*/kDocumentJoinIdPairMapperMaxNumEntries,
            /*average_kv_byte_size=*/
            kDocumentJoinIdPairMapperAverageKVByteSize));
  } else {
    ICING_RETURN_IF_ERROR(DynamicTrieKeyMapper<int32_t>::Delete(
        filesystem_, document_join_id_pair_mapper_path));
    ICING_ASSIGN_OR_RETURN(document_join_id_pair_mapper_,
                           DynamicTrieKeyMapper<int32_t>::Create(
                               filesystem_, document_join_id_pair_mapper_path,
                               kDocumentJoinIdPairMapperDynamicTrieMaxSize));
  }

  // Clear qualified_id_storage_.
  if (qualified_id_storage_->num_elements() > 0) {
    ICING_RETURN_IF_ERROR(qualified_id_storage_->TruncateTo(0));
  }

  // TODO(b/268521214): clear delete propagation storage

  info().last_added_document_id = kInvalidDocumentId;
  return libtextclassifier3::Status::OK;
}

/* static */ libtextclassifier3::StatusOr<
    std::unique_ptr<QualifiedIdJoinIndexImplV1>>
QualifiedIdJoinIndexImplV1::InitializeNewFiles(const Filesystem& filesystem,
                                               std::string&& working_path,
                                               bool pre_mapping_fbv,
                                               bool use_persistent_hash_map) {
  // Create working directory.
  if (!filesystem.CreateDirectoryRecursively(working_path.c_str())) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Failed to create directory: ", working_path));
  }

  // Initialize document_join_id_pair_mapper
  std::unique_ptr<KeyMapper<int32_t>> document_join_id_pair_mapper;
  if (use_persistent_hash_map) {
    // TODO(b/263890397): decide PersistentHashMapKeyMapper size
    ICING_ASSIGN_OR_RETURN(
        document_join_id_pair_mapper,
        PersistentHashMapKeyMapper<int32_t>::Create(
            filesystem, GetDocumentJoinIdPairMapperPath(working_path),
            pre_mapping_fbv,
            /*max_num_entries=*/kDocumentJoinIdPairMapperMaxNumEntries,
            /*average_kv_byte_size=*/
            kDocumentJoinIdPairMapperAverageKVByteSize));
  } else {
    ICING_ASSIGN_OR_RETURN(
        document_join_id_pair_mapper,
        DynamicTrieKeyMapper<int32_t>::Create(
            filesystem, GetDocumentJoinIdPairMapperPath(working_path),
            kDocumentJoinIdPairMapperDynamicTrieMaxSize));
  }

  // Initialize qualified_id_storage
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<FileBackedVector<char>> qualified_id_storage,
      FileBackedVector<char>::Create(
          filesystem, GetQualifiedIdStoragePath(working_path),
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
          FileBackedVector<char>::kMaxFileSize,
          /*pre_mapping_mmap_size=*/pre_mapping_fbv ? 1024 * 1024 : 0));

  // Create instance.
  auto new_index = std::unique_ptr<QualifiedIdJoinIndexImplV1>(
      new QualifiedIdJoinIndexImplV1(
          filesystem, std::move(working_path),
          /*metadata_buffer=*/std::make_unique<uint8_t[]>(kMetadataFileSize),
          std::move(document_join_id_pair_mapper),
          std::move(qualified_id_storage), pre_mapping_fbv,
          use_persistent_hash_map));
  // Initialize info content.
  new_index->info().magic = Info::kMagic;
  new_index->info().last_added_document_id = kInvalidDocumentId;

  // Initialize new PersistentStorage. The initial checksums will be computed
  // and set via InitializeNewStorage.
  ICING_RETURN_IF_ERROR(new_index->InitializeNewStorage());

  return new_index;
}

/* static */ libtextclassifier3::StatusOr<
    std::unique_ptr<QualifiedIdJoinIndexImplV1>>
QualifiedIdJoinIndexImplV1::InitializeExistingFiles(
    const Filesystem& filesystem, std::string&& working_path,
    bool pre_mapping_fbv, bool use_persistent_hash_map) {
  // PRead metadata file.
  auto metadata_buffer = std::make_unique<uint8_t[]>(kMetadataFileSize);
  if (!filesystem.PRead(GetMetadataFilePath(working_path).c_str(),
                        metadata_buffer.get(), kMetadataFileSize,
                        /*offset=*/0)) {
    return absl_ports::InternalError("Fail to read metadata file");
  }

  // Initialize document_join_id_pair_mapper
  bool dynamic_trie_key_mapper_dir_exists = filesystem.DirectoryExists(
      absl_ports::StrCat(GetDocumentJoinIdPairMapperPath(working_path),
                         "/key_mapper_dir")
          .c_str());
  if ((use_persistent_hash_map && dynamic_trie_key_mapper_dir_exists) ||
      (!use_persistent_hash_map && !dynamic_trie_key_mapper_dir_exists)) {
    // Return a failure here so that the caller can properly delete and rebuild
    // this component.
    return absl_ports::FailedPreconditionError("Key mapper type mismatch");
  }

  std::unique_ptr<KeyMapper<int32_t>> document_join_id_pair_mapper;
  if (use_persistent_hash_map) {
    ICING_ASSIGN_OR_RETURN(
        document_join_id_pair_mapper,
        PersistentHashMapKeyMapper<int32_t>::Create(
            filesystem, GetDocumentJoinIdPairMapperPath(working_path),
            pre_mapping_fbv,
            /*max_num_entries=*/kDocumentJoinIdPairMapperMaxNumEntries,
            /*average_kv_byte_size=*/
            kDocumentJoinIdPairMapperAverageKVByteSize));
  } else {
    ICING_ASSIGN_OR_RETURN(
        document_join_id_pair_mapper,
        DynamicTrieKeyMapper<int32_t>::Create(
            filesystem, GetDocumentJoinIdPairMapperPath(working_path),
            kDocumentJoinIdPairMapperDynamicTrieMaxSize));
  }

  // Initialize qualified_id_storage
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<FileBackedVector<char>> qualified_id_storage,
      FileBackedVector<char>::Create(
          filesystem, GetQualifiedIdStoragePath(working_path),
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
          FileBackedVector<char>::kMaxFileSize,
          /*pre_mapping_mmap_size=*/pre_mapping_fbv ? 1024 * 1024 : 0));

  // Create instance.
  auto type_joinable_index = std::unique_ptr<QualifiedIdJoinIndexImplV1>(
      new QualifiedIdJoinIndexImplV1(filesystem, std::move(working_path),
                                     std::move(metadata_buffer),
                                     std::move(document_join_id_pair_mapper),
                                     std::move(qualified_id_storage),
                                     pre_mapping_fbv, use_persistent_hash_map));

  // Initialize existing PersistentStorage. Checksums will be validated.
  ICING_RETURN_IF_ERROR(type_joinable_index->InitializeExistingStorage());

  // Validate magic.
  if (type_joinable_index->info().magic != Info::kMagic) {
    return absl_ports::FailedPreconditionError("Incorrect magic value");
  }

  return type_joinable_index;
}

libtextclassifier3::Status QualifiedIdJoinIndexImplV1::TransferIndex(
    const std::vector<DocumentId>& document_id_old_to_new,
    QualifiedIdJoinIndexImplV1* new_index) const {
  std::unique_ptr<KeyMapper<int32_t>::Iterator> iter =
      document_join_id_pair_mapper_->GetIterator();
  while (iter->Advance()) {
    DocumentJoinIdPair old_document_join_id_pair(
        encode_util::DecodeIntFromCString(iter->GetKey()));
    int32_t qualified_id_index = iter->GetValue();

    const char* data = qualified_id_storage_->array() + qualified_id_index;
    std::string_view ref_qualified_id_str(data, strlen(data));

    // Translate to new doc id.
    DocumentId new_document_id = GetNewDocumentId(
        document_id_old_to_new, old_document_join_id_pair.document_id());

    if (new_document_id != kInvalidDocumentId) {
      ICING_RETURN_IF_ERROR(new_index->Put(
          DocumentJoinIdPair(new_document_id,
                             old_document_join_id_pair.joinable_property_id()),
          ref_qualified_id_str));
    }
  }

  // TODO(b/268521214): transfer delete propagation storage
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status QualifiedIdJoinIndexImplV1::PersistMetadataToDisk() {
  if (is_initialized_ && !is_info_dirty() && !is_storage_dirty()) {
    return libtextclassifier3::Status::OK;
  }

  std::string metadata_file_path = GetMetadataFilePath(working_path_);
  ScopedFd sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
  ICING_RETURN_IF_ERROR(InternalWriteMetadata(sfd));
  if (!filesystem_.DataSync(sfd.get())) {
    return absl_ports::InternalError("Fail to sync metadata to disk");
  }
  is_info_dirty_ = false;
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status QualifiedIdJoinIndexImplV1::PersistStoragesToDisk() {
  if (is_initialized_ && !is_storage_dirty()) {
    return libtextclassifier3::Status::OK;
  }

  ICING_RETURN_IF_ERROR(document_join_id_pair_mapper_->PersistToDisk());
  ICING_RETURN_IF_ERROR(qualified_id_storage_->PersistToDisk());
  is_storage_dirty_ = false;
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status QualifiedIdJoinIndexImplV1::WriteMetadata() {
  if (is_initialized_ && !is_info_dirty() && !is_storage_dirty()) {
    return libtextclassifier3::Status::OK;
  }

  std::string metadata_file_path = GetMetadataFilePath(working_path_);
  ScopedFd sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
  return InternalWriteMetadata(std::move(sfd));
}

libtextclassifier3::Status QualifiedIdJoinIndexImplV1::InternalWriteMetadata(
    const ScopedFd& sfd) {
  if (!sfd.is_valid()) {
    return absl_ports::InternalError("Fail to open metadata file for write");
  }
  if (!filesystem_.PWrite(sfd.get(), /*offset=*/0, metadata_buffer_.get(),
                          kMetadataFileSize)) {
    return absl_ports::InternalError("Fail to write metadata file");
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<Crc32>
QualifiedIdJoinIndexImplV1::UpdateStoragesChecksum() {
  if (is_initialized_ && !is_storage_dirty()) {
    return Crc32(crcs().component_crcs.storages_crc);
  }

  ICING_ASSIGN_OR_RETURN(Crc32 document_join_id_pair_mapper_crc,
                         document_join_id_pair_mapper_->UpdateChecksum());
  ICING_ASSIGN_OR_RETURN(Crc32 qualified_id_storage_crc,
                         qualified_id_storage_->UpdateChecksum());
  return Crc32(document_join_id_pair_mapper_crc.Get() ^
               qualified_id_storage_crc.Get());
}

libtextclassifier3::StatusOr<Crc32>
QualifiedIdJoinIndexImplV1::GetInfoChecksum() const {
  // Info checksum is not cached and is calculated on the fly. Just call Get.
  if (is_initialized_ && !is_info_dirty()) {
    return Crc32(crcs().component_crcs.info_crc);
  }
  return info().GetChecksum();
}

libtextclassifier3::StatusOr<Crc32>
QualifiedIdJoinIndexImplV1::GetStoragesChecksum() const {
  if (is_initialized_ && !is_storage_dirty()) {
    return Crc32(crcs().component_crcs.storages_crc);
  }

  ICING_ASSIGN_OR_RETURN(Crc32 document_join_id_pair_mapper_crc,
                         document_join_id_pair_mapper_->GetChecksum());
  Crc32 qualified_id_storage_crc = qualified_id_storage_->GetChecksum();
  return Crc32(document_join_id_pair_mapper_crc.Get() ^
               qualified_id_storage_crc.Get());
}

}  // namespace lib
}  // namespace icing
