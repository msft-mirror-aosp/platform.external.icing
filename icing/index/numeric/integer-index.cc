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

#include "icing/index/numeric/integer-index.h"

#include <cstdint>
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
#include "icing/file/memory-mapped-file.h"
#include "icing/index/numeric/doc-hit-info-iterator-numeric.h"
#include "icing/index/numeric/integer-index-storage.h"
#include "icing/index/numeric/posting-list-integer-index-serializer.h"
#include "icing/store/document-id.h"
#include "icing/util/crc32.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

// Helper function to get the file name of metadata.
std::string GetMetadataFileName() {
  return absl_ports::StrCat(IntegerIndex::kFilePrefix, ".m");
}

// Helper function to get the file path of metadata according to the given
// working directory.
std::string GetMetadataFilePath(std::string_view working_path) {
  return absl_ports::StrCat(working_path, "/", GetMetadataFileName());
}

// Helper function to get the sub working (directory) path of
// IntegerIndexStorage according to the given working directory and property
// path.
std::string GetPropertyIndexStoragePath(std::string_view working_path,
                                        std::string_view property_path) {
  return absl_ports::StrCat(working_path, "/", property_path);
}

// Helper function to get all existing property paths by listing all
// directories.
libtextclassifier3::StatusOr<std::vector<std::string>>
GetAllExistingPropertyPaths(const Filesystem& filesystem,
                            const std::string& working_path) {
  std::vector<std::string> property_paths;
  if (!filesystem.ListDirectory(working_path.c_str(),
                                /*exclude=*/{GetMetadataFileName()},
                                /*recursive=*/false, &property_paths)) {
    return absl_ports::InternalError("Failed to list directory");
  }
  return property_paths;
}

libtextclassifier3::StatusOr<IntegerIndex::PropertyToStorageMapType>
GetPropertyIntegerIndexStorageMap(
    const Filesystem& filesystem, const std::string& working_path,
    PostingListIntegerIndexSerializer* posting_list_serializer) {
  ICING_ASSIGN_OR_RETURN(std::vector<std::string> property_paths,
                         GetAllExistingPropertyPaths(filesystem, working_path));

  IntegerIndex::PropertyToStorageMapType property_to_storage_map;
  for (const std::string& property_path : property_paths) {
    std::string storage_working_path =
        GetPropertyIndexStoragePath(working_path, property_path);
    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<IntegerIndexStorage> storage,
        IntegerIndexStorage::Create(filesystem, storage_working_path,
                                    IntegerIndexStorage::Options(),
                                    posting_list_serializer));
    property_to_storage_map.insert(
        std::make_pair(property_path, std::move(storage)));
  }

  return property_to_storage_map;
}

}  // namespace

libtextclassifier3::Status IntegerIndex::Editor::IndexAllBufferedKeys() && {
  auto iter = integer_index_.property_to_storage_map_.find(property_path_);
  IntegerIndexStorage* target_storage = nullptr;
  if (iter != integer_index_.property_to_storage_map_.end()) {
    target_storage = iter->second.get();
  } else {
    // A new property path. Create a new storage instance and insert into the
    // map.
    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<IntegerIndexStorage> new_storage,
        IntegerIndexStorage::Create(
            integer_index_.filesystem_,
            GetPropertyIndexStoragePath(integer_index_.working_path_,
                                        property_path_),
            IntegerIndexStorage::Options(),
            integer_index_.posting_list_serializer_.get()));
    target_storage = new_storage.get();
    integer_index_.property_to_storage_map_.insert(
        std::make_pair(property_path_, std::move(new_storage)));
  }

  return target_storage->AddKeys(document_id_, section_id_,
                                 std::move(seen_keys_));
}

/* static */ libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndex>>
IntegerIndex::Create(const Filesystem& filesystem, std::string working_path) {
  if (!filesystem.FileExists(GetMetadataFilePath(working_path).c_str())) {
    // Discard working_path if metadata file is missing, and reinitialize.
    if (filesystem.DirectoryExists(working_path.c_str())) {
      ICING_RETURN_IF_ERROR(Discard(filesystem, working_path));
    }
    return InitializeNewFiles(filesystem, std::move(working_path));
  }
  return InitializeExistingFiles(filesystem, std::move(working_path));
}

IntegerIndex::~IntegerIndex() {
  if (!PersistToDisk().ok()) {
    ICING_LOG(WARNING)
        << "Failed to persist integer index to disk while destructing "
        << working_path_;
  }
}

libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>>
IntegerIndex::GetIterator(std::string_view property_path, int64_t key_lower,
                          int64_t key_upper) const {
  auto iter = property_to_storage_map_.find(std::string(property_path));
  if (iter == property_to_storage_map_.end()) {
    // Return an empty iterator.
    return std::make_unique<DocHitInfoIteratorNumeric<int64_t>>(
        /*numeric_index_iter=*/nullptr);
  }

  return iter->second->GetIterator(key_lower, key_upper);
}

libtextclassifier3::Status IntegerIndex::Optimize(
    const std::vector<DocumentId>& document_id_old_to_new,
    DocumentId new_last_added_document_id) {
  std::string temp_working_path = working_path_ + "_temp";
  ICING_RETURN_IF_ERROR(Discard(filesystem_, temp_working_path));

  DestructibleDirectory temp_working_path_ddir(&filesystem_,
                                               std::move(temp_working_path));
  if (!temp_working_path_ddir.is_valid()) {
    return absl_ports::InternalError(
        "Unable to create temp directory to build new integer index");
  }

  {
    // Transfer all indexed data from current integer index to new integer
    // index. Also PersistToDisk and destruct the instance after finishing, so
    // we can safely swap directories later.
    ICING_ASSIGN_OR_RETURN(std::unique_ptr<IntegerIndex> new_integer_index,
                           Create(filesystem_, temp_working_path_ddir.dir()));
    ICING_RETURN_IF_ERROR(
        TransferIndex(document_id_old_to_new, new_integer_index.get()));
    new_integer_index->set_last_added_document_id(new_last_added_document_id);
    ICING_RETURN_IF_ERROR(new_integer_index->PersistToDisk());
  }

  // Destruct current storage instances to safely swap directories.
  metadata_mmapped_file_.reset();
  property_to_storage_map_.clear();
  if (!filesystem_.SwapFiles(temp_working_path_ddir.dir().c_str(),
                             working_path_.c_str())) {
    return absl_ports::InternalError(
        "Unable to apply new integer index due to failed swap");
  }

  // Reinitialize the integer index.
  ICING_ASSIGN_OR_RETURN(
      MemoryMappedFile metadata_mmapped_file,
      MemoryMappedFile::Create(filesystem_, GetMetadataFilePath(working_path_),
                               MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
                               /*max_file_size=*/kMetadataFileSize,
                               /*pre_mapping_file_offset=*/0,
                               /*pre_mapping_mmap_size=*/kMetadataFileSize));
  metadata_mmapped_file_ =
      std::make_unique<MemoryMappedFile>(std::move(metadata_mmapped_file));

  // Initialize all existing integer index storages.
  ICING_ASSIGN_OR_RETURN(
      property_to_storage_map_,
      GetPropertyIntegerIndexStorageMap(filesystem_, working_path_,
                                        posting_list_serializer_.get()));

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status IntegerIndex::Clear() {
  // Step 1: clear property_to_storage_map_.
  property_to_storage_map_.clear();

  // Step 2: delete all IntegerIndexStorages. It is safe because there is no
  //         active IntegerIndexStorage after clearing the map.
  ICING_ASSIGN_OR_RETURN(
      std::vector<std::string> property_paths,
      GetAllExistingPropertyPaths(filesystem_, working_path_));
  for (const std::string& property_path : property_paths) {
    ICING_RETURN_IF_ERROR(IntegerIndexStorage::Discard(
        filesystem_,
        GetPropertyIndexStoragePath(working_path_, property_path)));
  }

  info().last_added_document_id = kInvalidDocumentId;
  return libtextclassifier3::Status::OK;
}

/* static */ libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndex>>
IntegerIndex::InitializeNewFiles(const Filesystem& filesystem,
                                 std::string&& working_path) {
  // Create working directory.
  if (!filesystem.CreateDirectoryRecursively(working_path.c_str())) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Failed to create directory: ", working_path));
  }

  // Initialize metadata file. Create MemoryMappedFile with pre-mapping, and
  // call GrowAndRemapIfNecessary to grow the underlying file.
  ICING_ASSIGN_OR_RETURN(
      MemoryMappedFile metadata_mmapped_file,
      MemoryMappedFile::Create(filesystem, GetMetadataFilePath(working_path),
                               MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
                               /*max_file_size=*/kMetadataFileSize,
                               /*pre_mapping_file_offset=*/0,
                               /*pre_mapping_mmap_size=*/kMetadataFileSize));
  ICING_RETURN_IF_ERROR(metadata_mmapped_file.GrowAndRemapIfNecessary(
      /*file_offset=*/0, /*mmap_size=*/kMetadataFileSize));

  // Create instance.
  auto new_integer_index = std::unique_ptr<IntegerIndex>(new IntegerIndex(
      filesystem, std::move(working_path),
      std::make_unique<PostingListIntegerIndexSerializer>(),
      std::make_unique<MemoryMappedFile>(std::move(metadata_mmapped_file)),
      /*property_to_storage_map=*/{}));
  // Initialize info content by writing mapped memory directly.
  Info& info_ref = new_integer_index->info();
  info_ref.magic = Info::kMagic;
  info_ref.last_added_document_id = kInvalidDocumentId;
  // Initialize new PersistentStorage. The initial checksums will be computed
  // and set via InitializeNewStorage.
  ICING_RETURN_IF_ERROR(new_integer_index->InitializeNewStorage());

  return new_integer_index;
}

/* static */ libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndex>>
IntegerIndex::InitializeExistingFiles(const Filesystem& filesystem,
                                      std::string&& working_path) {
  // Mmap the content of the crcs and info.
  ICING_ASSIGN_OR_RETURN(
      MemoryMappedFile metadata_mmapped_file,
      MemoryMappedFile::Create(filesystem, GetMetadataFilePath(working_path),
                               MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
                               /*max_file_size=*/kMetadataFileSize,
                               /*pre_mapping_file_offset=*/0,
                               /*pre_mapping_mmap_size=*/kMetadataFileSize));

  auto posting_list_serializer =
      std::make_unique<PostingListIntegerIndexSerializer>();

  // Initialize all existing integer index storages.
  ICING_ASSIGN_OR_RETURN(
      PropertyToStorageMapType property_to_storage_map,
      GetPropertyIntegerIndexStorageMap(filesystem, working_path,
                                        posting_list_serializer.get()));

  // Create instance.
  auto integer_index = std::unique_ptr<IntegerIndex>(new IntegerIndex(
      filesystem, std::move(working_path), std::move(posting_list_serializer),
      std::make_unique<MemoryMappedFile>(std::move(metadata_mmapped_file)),
      std::move(property_to_storage_map)));
  // Initialize existing PersistentStorage. Checksums will be validated.
  ICING_RETURN_IF_ERROR(integer_index->InitializeExistingStorage());

  // Validate magic.
  if (integer_index->info().magic != Info::kMagic) {
    return absl_ports::FailedPreconditionError("Incorrect magic value");
  }

  return integer_index;
}

libtextclassifier3::Status IntegerIndex::TransferIndex(
    const std::vector<DocumentId>& document_id_old_to_new,
    IntegerIndex* new_integer_index) const {
  for (const auto& [property_path, old_storage] : property_to_storage_map_) {
    std::string new_storage_working_path = GetPropertyIndexStoragePath(
        new_integer_index->working_path_, property_path);
    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<IntegerIndexStorage> new_storage,
        IntegerIndexStorage::Create(
            new_integer_index->filesystem_, new_storage_working_path,
            IntegerIndexStorage::Options(),
            new_integer_index->posting_list_serializer_.get()));

    ICING_RETURN_IF_ERROR(
        old_storage->TransferIndex(document_id_old_to_new, new_storage.get()));

    if (new_storage->num_data() == 0) {
      new_storage.reset();
      ICING_RETURN_IF_ERROR(
          IntegerIndexStorage::Discard(filesystem_, new_storage_working_path));
    } else {
      new_integer_index->property_to_storage_map_.insert(
          std::make_pair(property_path, std::move(new_storage)));
    }
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status IntegerIndex::PersistStoragesToDisk() {
  for (auto& [_, storage] : property_to_storage_map_) {
    ICING_RETURN_IF_ERROR(storage->PersistToDisk());
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status IntegerIndex::PersistMetadataToDisk() {
  // Changes should have been applied to the underlying file when using
  // MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC, but call msync() as an
  // extra safety step to ensure they are written out.
  return metadata_mmapped_file_->PersistToDisk();
}

libtextclassifier3::StatusOr<Crc32> IntegerIndex::ComputeInfoChecksum() {
  return info().ComputeChecksum();
}

libtextclassifier3::StatusOr<Crc32> IntegerIndex::ComputeStoragesChecksum() {
  // XOR all crcs of all storages. Since XOR is commutative and associative, the
  // order doesn't matter.
  uint32_t storages_checksum = 0;
  for (auto& [property_path, storage] : property_to_storage_map_) {
    ICING_ASSIGN_OR_RETURN(Crc32 storage_crc, storage->UpdateChecksums());
    storage_crc.Append(property_path);

    storages_checksum ^= storage_crc.Get();
  }
  return Crc32(storages_checksum);
}

}  // namespace lib
}  // namespace icing
