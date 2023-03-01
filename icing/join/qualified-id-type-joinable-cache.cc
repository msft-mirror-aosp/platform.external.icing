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
#include <string_view>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/file/filesystem.h"
#include "icing/join/doc-join-info.h"
#include "icing/store/document-id.h"
#include "icing/store/key-mapper.h"
#include "icing/store/persistent-hash-map-key-mapper.h"
#include "icing/util/crc32.h"
#include "icing/util/encode-util.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

std::string GetMetadataFilePath(std::string_view working_path) {
  return absl_ports::StrCat(working_path, "/",
                            QualifiedIdTypeJoinableCache::kFilePrefix, ".m");
}

std::string GetDocumentToQualifiedIdMapperPath(std::string_view working_path) {
  return absl_ports::StrCat(
      working_path, "/", QualifiedIdTypeJoinableCache::kFilePrefix, "_mapper");
}

}  // namespace

/* static */ libtextclassifier3::StatusOr<
    std::unique_ptr<QualifiedIdTypeJoinableCache>>
QualifiedIdTypeJoinableCache::Create(const Filesystem& filesystem,
                                     std::string working_path) {
  if (!filesystem.FileExists(GetMetadataFilePath(working_path).c_str()) ||
      !filesystem.DirectoryExists(
          GetDocumentToQualifiedIdMapperPath(working_path).c_str())) {
    // Discard working_path if any file/directory is missing, and reinitialize.
    ICING_RETURN_IF_ERROR(
        PersistentStorage::Discard(filesystem, working_path, kWorkingPathType));
    return InitializeNewFiles(filesystem, std::move(working_path));
  }
  return InitializeExistingFiles(filesystem, std::move(working_path));
}

QualifiedIdTypeJoinableCache::~QualifiedIdTypeJoinableCache() {
  if (!PersistToDisk().ok()) {
    ICING_LOG(WARNING) << "Failed to persist qualified id type joinable cache "
                          "to disk while destructing "
                       << working_path_;
  }
}

libtextclassifier3::Status QualifiedIdTypeJoinableCache::Put(
    const DocJoinInfo& doc_join_info, DocumentId ref_document_id) {
  if (!doc_join_info.is_valid()) {
    return absl_ports::InvalidArgumentError(
        "Cannot put data for an invalid DocJoinInfo");
  }

  ICING_RETURN_IF_ERROR(document_to_qualified_id_mapper_->Put(
      encode_util::EncodeIntToCString(doc_join_info.value()), ref_document_id));

  // TODO(b/263890397): add delete propagation

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<DocumentId> QualifiedIdTypeJoinableCache::Get(
    const DocJoinInfo& doc_join_info) const {
  if (!doc_join_info.is_valid()) {
    return absl_ports::InvalidArgumentError(
        "Cannot get data for an invalid DocJoinInfo");
  }

  return document_to_qualified_id_mapper_->Get(
      encode_util::EncodeIntToCString(doc_join_info.value()));
}

/* static */ libtextclassifier3::StatusOr<
    std::unique_ptr<QualifiedIdTypeJoinableCache>>
QualifiedIdTypeJoinableCache::InitializeNewFiles(const Filesystem& filesystem,
                                                 std::string&& working_path) {
  // Create working directory.
  if (!filesystem.CreateDirectoryRecursively(working_path.c_str())) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Failed to create directory: ", working_path));
  }

  // Initialize document_to_qualified_id_mapper
  // TODO(b/263890397): decide PersistentHashMapKeyMapper size
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<KeyMapper<DocumentId>> document_to_qualified_id_mapper,
      PersistentHashMapKeyMapper<DocumentId>::Create(
          filesystem, GetDocumentToQualifiedIdMapperPath(working_path)));

  // Create instance.
  auto new_type_joinable_cache = std::unique_ptr<QualifiedIdTypeJoinableCache>(
      new QualifiedIdTypeJoinableCache(
          filesystem, std::move(working_path),
          /*metadata_buffer=*/std::make_unique<uint8_t[]>(kMetadataFileSize),
          std::move(document_to_qualified_id_mapper)));
  // Initialize info content.
  new_type_joinable_cache->info().magic = Info::kMagic;
  new_type_joinable_cache->info().last_added_document_id = kInvalidDocumentId;
  // Initialize new PersistentStorage. The initial checksums will be computed
  // and set via InitializeNewStorage. Also write them into disk as well.
  ICING_RETURN_IF_ERROR(new_type_joinable_cache->InitializeNewStorage());
  ICING_RETURN_IF_ERROR(new_type_joinable_cache->PersistMetadataToDisk());

  return new_type_joinable_cache;
}

/* static */ libtextclassifier3::StatusOr<
    std::unique_ptr<QualifiedIdTypeJoinableCache>>
QualifiedIdTypeJoinableCache::InitializeExistingFiles(
    const Filesystem& filesystem, std::string&& working_path) {
  // PRead metadata file.
  auto metadata_buffer = std::make_unique<uint8_t[]>(kMetadataFileSize);
  if (!filesystem.PRead(GetMetadataFilePath(working_path).c_str(),
                        metadata_buffer.get(), kMetadataFileSize,
                        /*offset=*/0)) {
    return absl_ports::InternalError("Fail to read metadata file");
  }

  // Initialize document_to_qualified_id_mapper
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<KeyMapper<DocumentId>> document_to_qualified_id_mapper,
      PersistentHashMapKeyMapper<DocumentId>::Create(
          filesystem, GetDocumentToQualifiedIdMapperPath(working_path)));

  // Create instance.
  auto type_joinable_cache = std::unique_ptr<QualifiedIdTypeJoinableCache>(
      new QualifiedIdTypeJoinableCache(
          filesystem, std::move(working_path), std::move(metadata_buffer),
          std::move(document_to_qualified_id_mapper)));
  // Initialize existing PersistentStorage. Checksums will be validated.
  ICING_RETURN_IF_ERROR(type_joinable_cache->InitializeExistingStorage());

  // Validate magic.
  if (type_joinable_cache->info().magic != Info::kMagic) {
    return absl_ports::FailedPreconditionError("Incorrect magic value");
  }

  return type_joinable_cache;
}

libtextclassifier3::Status
QualifiedIdTypeJoinableCache::PersistMetadataToDisk() {
  std::string metadata_file_path = GetMetadataFilePath(working_path_);

  ScopedFd sfd(filesystem_.OpenForWrite(metadata_file_path.c_str()));
  if (!sfd.is_valid()) {
    return absl_ports::InternalError("Fail to open metadata file for write");
  }

  if (!filesystem_.PWrite(sfd.get(), /*offset=*/0, metadata_buffer_.get(),
                          kMetadataFileSize)) {
    return absl_ports::InternalError("Fail to write metadata file");
  }

  if (!filesystem_.DataSync(sfd.get())) {
    return absl_ports::InternalError("Fail to sync metadata to disk");
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status
QualifiedIdTypeJoinableCache::PersistStoragesToDisk() {
  return document_to_qualified_id_mapper_->PersistToDisk();
}

libtextclassifier3::StatusOr<Crc32>
QualifiedIdTypeJoinableCache::ComputeInfoChecksum() {
  return info().ComputeChecksum();
}

libtextclassifier3::StatusOr<Crc32>
QualifiedIdTypeJoinableCache::ComputeStoragesChecksum() {
  return document_to_qualified_id_mapper_->ComputeChecksum();
}

}  // namespace lib
}  // namespace icing
