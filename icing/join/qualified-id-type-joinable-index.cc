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

#include "icing/join/qualified-id-type-joinable-index.h"

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/file/destructible-directory.h"
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

DocumentId GetNewDocumentId(
    const std::vector<DocumentId>& document_id_old_to_new,
    DocumentId old_document_id) {
  if (old_document_id >= document_id_old_to_new.size()) {
    return kInvalidDocumentId;
  }
  return document_id_old_to_new[old_document_id];
}

std::string GetMetadataFilePath(std::string_view working_path) {
  return absl_ports::StrCat(working_path, "/",
                            QualifiedIdTypeJoinableIndex::kFilePrefix, ".m");
}

std::string GetDocumentToQualifiedIdMapperPath(std::string_view working_path) {
  return absl_ports::StrCat(
      working_path, "/", QualifiedIdTypeJoinableIndex::kFilePrefix, "_mapper");
}

}  // namespace

/* static */ libtextclassifier3::StatusOr<
    std::unique_ptr<QualifiedIdTypeJoinableIndex>>
QualifiedIdTypeJoinableIndex::Create(const Filesystem& filesystem,
                                     std::string working_path) {
  if (!filesystem.FileExists(GetMetadataFilePath(working_path).c_str()) ||
      !filesystem.DirectoryExists(
          GetDocumentToQualifiedIdMapperPath(working_path).c_str())) {
    // Discard working_path if any file/directory is missing, and reinitialize.
    ICING_RETURN_IF_ERROR(Discard(filesystem, working_path));
    return InitializeNewFiles(filesystem, std::move(working_path));
  }
  return InitializeExistingFiles(filesystem, std::move(working_path));
}

QualifiedIdTypeJoinableIndex::~QualifiedIdTypeJoinableIndex() {
  if (!PersistToDisk().ok()) {
    ICING_LOG(WARNING) << "Failed to persist qualified id type joinable index "
                          "to disk while destructing "
                       << working_path_;
  }
}

libtextclassifier3::Status QualifiedIdTypeJoinableIndex::Put(
    const DocJoinInfo& doc_join_info, DocumentId ref_document_id) {
  if (!doc_join_info.is_valid()) {
    return absl_ports::InvalidArgumentError(
        "Cannot put data for an invalid DocJoinInfo");
  }

  ICING_RETURN_IF_ERROR(document_to_qualified_id_mapper_->Put(
      encode_util::EncodeIntToCString(doc_join_info.value()), ref_document_id));

  // TODO(b/268521214): add data into delete propagation storage

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<DocumentId> QualifiedIdTypeJoinableIndex::Get(
    const DocJoinInfo& doc_join_info) const {
  if (!doc_join_info.is_valid()) {
    return absl_ports::InvalidArgumentError(
        "Cannot get data for an invalid DocJoinInfo");
  }

  return document_to_qualified_id_mapper_->Get(
      encode_util::EncodeIntToCString(doc_join_info.value()));
}

libtextclassifier3::Status QualifiedIdTypeJoinableIndex::Optimize(
    const std::vector<DocumentId>& document_id_old_to_new,
    DocumentId new_last_added_document_id) {
  std::string temp_working_path = working_path_ + "_temp";
  ICING_RETURN_IF_ERROR(Discard(filesystem_, temp_working_path));

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
        std::unique_ptr<QualifiedIdTypeJoinableIndex> new_index,
        Create(filesystem_, temp_working_path_ddir.dir()));
    ICING_RETURN_IF_ERROR(
        TransferIndex(document_id_old_to_new, new_index.get()));
    new_index->set_last_added_document_id(new_last_added_document_id);
    ICING_RETURN_IF_ERROR(new_index->PersistToDisk());
  }

  // Destruct current index's storage instances to safely swap directories.
  // TODO(b/268521214): handle delete propagation storage
  document_to_qualified_id_mapper_.reset();

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
  ICING_ASSIGN_OR_RETURN(
      document_to_qualified_id_mapper_,
      PersistentHashMapKeyMapper<DocumentId>::Create(
          filesystem_, GetDocumentToQualifiedIdMapperPath(working_path_)));

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status QualifiedIdTypeJoinableIndex::Clear() {
  document_to_qualified_id_mapper_.reset();
  // Discard and reinitialize document to qualified id mapper.
  std::string document_to_qualified_id_mapper_path =
      GetDocumentToQualifiedIdMapperPath(working_path_);
  ICING_RETURN_IF_ERROR(PersistentHashMapKeyMapper<DocumentId>::Delete(
      filesystem_, document_to_qualified_id_mapper_path));
  ICING_ASSIGN_OR_RETURN(
      document_to_qualified_id_mapper_,
      PersistentHashMapKeyMapper<DocumentId>::Create(
          filesystem_, std::move(document_to_qualified_id_mapper_path)));

  // TODO(b/268521214): clear delete propagation storage

  info().last_added_document_id = kInvalidDocumentId;
  return libtextclassifier3::Status::OK;
}

/* static */ libtextclassifier3::StatusOr<
    std::unique_ptr<QualifiedIdTypeJoinableIndex>>
QualifiedIdTypeJoinableIndex::InitializeNewFiles(const Filesystem& filesystem,
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
  auto new_index = std::unique_ptr<QualifiedIdTypeJoinableIndex>(
      new QualifiedIdTypeJoinableIndex(
          filesystem, std::move(working_path),
          /*metadata_buffer=*/std::make_unique<uint8_t[]>(kMetadataFileSize),
          std::move(document_to_qualified_id_mapper)));
  // Initialize info content.
  new_index->info().magic = Info::kMagic;
  new_index->info().last_added_document_id = kInvalidDocumentId;
  // Initialize new PersistentStorage. The initial checksums will be computed
  // and set via InitializeNewStorage. Also write them into disk as well.
  ICING_RETURN_IF_ERROR(new_index->InitializeNewStorage());
  ICING_RETURN_IF_ERROR(new_index->PersistMetadataToDisk());

  return new_index;
}

/* static */ libtextclassifier3::StatusOr<
    std::unique_ptr<QualifiedIdTypeJoinableIndex>>
QualifiedIdTypeJoinableIndex::InitializeExistingFiles(
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
  auto type_joinable_index = std::unique_ptr<QualifiedIdTypeJoinableIndex>(
      new QualifiedIdTypeJoinableIndex(
          filesystem, std::move(working_path), std::move(metadata_buffer),
          std::move(document_to_qualified_id_mapper)));
  // Initialize existing PersistentStorage. Checksums will be validated.
  ICING_RETURN_IF_ERROR(type_joinable_index->InitializeExistingStorage());

  // Validate magic.
  if (type_joinable_index->info().magic != Info::kMagic) {
    return absl_ports::FailedPreconditionError("Incorrect magic value");
  }

  return type_joinable_index;
}

libtextclassifier3::Status QualifiedIdTypeJoinableIndex::TransferIndex(
    const std::vector<DocumentId>& document_id_old_to_new,
    QualifiedIdTypeJoinableIndex* new_index) const {
  std::unique_ptr<KeyMapper<DocumentId>::Iterator> iter =
      document_to_qualified_id_mapper_->GetIterator();
  while (iter->Advance()) {
    DocJoinInfo old_doc_join_info(
        encode_util::DecodeIntFromCString(iter->GetKey()));
    DocumentId old_ref_document_id = iter->GetValue();

    // Translate to new doc ids.
    DocumentId new_document_id = GetNewDocumentId(
        document_id_old_to_new, old_doc_join_info.document_id());
    DocumentId new_ref_document_id =
        GetNewDocumentId(document_id_old_to_new, old_ref_document_id);

    if (new_document_id != kInvalidDocumentId &&
        new_ref_document_id != kInvalidDocumentId) {
      ICING_RETURN_IF_ERROR(
          new_index->Put(DocJoinInfo(new_document_id,
                                     old_doc_join_info.joinable_property_id()),
                         new_ref_document_id));
    }
  }

  // TODO(b/268521214): transfer delete propagation storage

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status
QualifiedIdTypeJoinableIndex::PersistMetadataToDisk() {
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
QualifiedIdTypeJoinableIndex::PersistStoragesToDisk() {
  return document_to_qualified_id_mapper_->PersistToDisk();
}

libtextclassifier3::StatusOr<Crc32>
QualifiedIdTypeJoinableIndex::ComputeInfoChecksum() {
  return info().ComputeChecksum();
}

libtextclassifier3::StatusOr<Crc32>
QualifiedIdTypeJoinableIndex::ComputeStoragesChecksum() {
  return document_to_qualified_id_mapper_->ComputeChecksum();
}

}  // namespace lib
}  // namespace icing
