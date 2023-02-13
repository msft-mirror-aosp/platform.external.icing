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
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
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

/* static */ libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndex>>
IntegerIndex::Create(const Filesystem& filesystem, std::string working_path) {
  if (!filesystem.FileExists(GetMetadataFilePath(working_path).c_str())) {
    // Discard working_path if metadata file is missing, and reinitialize.
    ICING_RETURN_IF_ERROR(Discard(filesystem, working_path, kWorkingPathType));
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

libtextclassifier3::Status IntegerIndex::Reset() {
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

  info()->last_added_document_id = kInvalidDocumentId;
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
  Info* info_ptr = new_integer_index->info();
  info_ptr->magic = Info::kMagic;
  info_ptr->last_added_document_id = kInvalidDocumentId;
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
  if (integer_index->info()->magic != Info::kMagic) {
    return absl_ports::FailedPreconditionError("Incorrect magic value");
  }

  return integer_index;
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
  return info()->ComputeChecksum();
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
