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

#include <algorithm>
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
#include "icing/feature-flags.h"
#include "icing/file/destructible-directory.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/join/document-join-id-pair.h"
#include "icing/join/qualified-id-join-index.h"
#include "icing/store/document-id.h"
#include "icing/store/namespace-id.h"
#include "icing/util/crc32.h"
#include "icing/util/logging.h"
#include "icing/util/math-util.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

static constexpr DocumentJoinIdPair kInvalidDocumentJoinIdPair;
static constexpr QualifiedIdJoinIndexImplV3::ArrayInfo kInvalidArrayInfo;

namespace {

std::string MakeMetadataFilePath(std::string_view working_path) {
  return absl_ports::StrCat(working_path, "/metadata");
}

std::string MakeParentDocumentIdToChildArrayInfoFilePath(
    std::string_view working_path) {
  return absl_ports::StrCat(working_path,
                            "/parent_document_id_to_child_array_info");
}

std::string MakeChildDocumentJoinIdPairArrayFilePath(
    std::string_view working_path) {
  return absl_ports::StrCat(working_path, "/child_document_join_id_pair_array");
}

}  // namespace

/* static */ libtextclassifier3::StatusOr<
    std::unique_ptr<QualifiedIdJoinIndexImplV3>>
QualifiedIdJoinIndexImplV3::Create(const Filesystem& filesystem,
                                   std::string working_path,
                                   const FeatureFlags& feature_flags) {
  bool metadata_file_exists =
      filesystem.FileExists(MakeMetadataFilePath(working_path).c_str());
  bool parent_document_id_to_child_array_info_file_exists =
      filesystem.FileExists(
          MakeParentDocumentIdToChildArrayInfoFilePath(working_path).c_str());
  bool child_document_join_id_pair_array_file_exists = filesystem.FileExists(
      MakeChildDocumentJoinIdPairArrayFilePath(working_path).c_str());

  // If all files exist, initialize from existing files.
  if (metadata_file_exists &&
      parent_document_id_to_child_array_info_file_exists &&
      child_document_join_id_pair_array_file_exists) {
    return InitializeExistingFiles(filesystem, std::move(working_path),
                                   feature_flags);
  }

  // If all files don't exist, initialize new files.
  if (!metadata_file_exists &&
      !parent_document_id_to_child_array_info_file_exists &&
      !child_document_join_id_pair_array_file_exists) {
    return InitializeNewFiles(filesystem, std::move(working_path),
                              feature_flags);
  }

  // Otherwise, some files are missing, and the join index is somehow corrupted.
  // Return error to let the caller discard and rebuild.
  return absl_ports::FailedPreconditionError(
      "Inconsistent state of qualified id join index (v3)");
}

QualifiedIdJoinIndexImplV3::~QualifiedIdJoinIndexImplV3() {
  if (!PersistToDisk().ok()) {
    ICING_LOG(WARNING) << "Failed to persist qualified id join index (v3) to "
                          "disk while destructing "
                       << working_path_;
  }
}

libtextclassifier3::Status QualifiedIdJoinIndexImplV3::Put(
    const DocumentJoinIdPair& child_document_join_id_pair,
    std::vector<DocumentId>&& parent_document_ids) {
  if (!child_document_join_id_pair.is_valid()) {
    return absl_ports::InvalidArgumentError("Invalid child DocumentJoinIdPair");
  }

  if (parent_document_ids.empty()) {
    return libtextclassifier3::Status::OK;
  }

  SetDirty();

  // Sort and dedupe parent document ids. It will be more efficient to access
  // parent_document_id_to_child_array_info_ in the order of ids.
  std::sort(parent_document_ids.begin(), parent_document_ids.end());
  auto last =
      std::unique(parent_document_ids.begin(), parent_document_ids.end());
  parent_document_ids.erase(last, parent_document_ids.end());

  // Append child_document_join_id_pair to each parent's DocumentJoinIdPair
  // array.
  for (DocumentId parent_document_id : parent_document_ids) {
    if (parent_document_id < 0 || parent_document_id == kInvalidDocumentId) {
      // Skip invalid parent document id.
      continue;
    }

    // Insert child_document_join_id_pair into the parent's DocumentJoinIdPair
    // array.
    ICING_RETURN_IF_ERROR(AppendChildDocumentJoinIdPairsForParent(
        parent_document_id, {child_document_join_id_pair}));
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<std::vector<DocumentJoinIdPair>>
QualifiedIdJoinIndexImplV3::Get(DocumentId parent_document_id) const {
  if (parent_document_id < 0 || parent_document_id == kInvalidDocumentId) {
    return absl_ports::InvalidArgumentError("Invalid parent document id");
  }

  if (parent_document_id >=
      parent_document_id_to_child_array_info_->num_elements()) {
    return std::vector<DocumentJoinIdPair>();
  }

  // Get the child array info for the parent.
  ICING_ASSIGN_OR_RETURN(
      const ArrayInfo* array_info,
      parent_document_id_to_child_array_info_->Get(parent_document_id));
  if (!array_info->IsValid()) {
    return std::vector<DocumentJoinIdPair>();
  }

  // Safe check to avoid out-of-bound access. This should never happen unless
  // the internal data structure is corrupted.
  if (array_info->index + array_info->length >
      child_document_join_id_pair_array_->num_elements()) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Invalid array info: ", std::to_string(array_info->index), " + ",
        std::to_string(array_info->length), " > ",
        std::to_string(child_document_join_id_pair_array_->num_elements())));
  }

  // Get the DocumentJoinIdPair array and return the child DocumentJoinIdPairs.
  ICING_ASSIGN_OR_RETURN(
      const DocumentJoinIdPair* ptr,
      child_document_join_id_pair_array_->Get(array_info->index));
  return std::vector<DocumentJoinIdPair>(ptr, ptr + array_info->used_length);
}

libtextclassifier3::Status QualifiedIdJoinIndexImplV3::MigrateParent(
    DocumentId old_document_id, DocumentId new_document_id) {
  if (!IsDocumentIdValid(old_document_id) ||
      !IsDocumentIdValid(new_document_id)) {
    return absl_ports::InvalidArgumentError(
        "Invalid parent document id to migrate");
  }

  if (old_document_id >=
      parent_document_id_to_child_array_info_->num_elements()) {
    // It means the parent document doesn't have any children at this moment. No
    // need to migrate.
    return libtextclassifier3::Status::OK;
  }

  ICING_ASSIGN_OR_RETURN(
      FileBackedVector<ArrayInfo>::MutableView mutable_old_array_info,
      parent_document_id_to_child_array_info_->GetMutable(old_document_id));
  if (!mutable_old_array_info.Get().IsValid()) {
    // It means the parent document doesn't have any children at this moment. No
    // need to migrate.
    return libtextclassifier3::Status::OK;
  }

  ICING_RETURN_IF_ERROR(
      ExtendParentDocumentIdToChildArrayInfoIfNecessary(new_document_id));
  ICING_RETURN_IF_ERROR(parent_document_id_to_child_array_info_->Set(
      new_document_id, mutable_old_array_info.Get()));
  mutable_old_array_info.Get() = kInvalidArrayInfo;

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status QualifiedIdJoinIndexImplV3::Optimize(
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
        "Unable to create temp directory to build new qualified id join index "
        "(v3)");
  }

  {
    // Transfer all data from the current to new qualified id join index. Also
    // PersistToDisk and destruct the instance after finishing, so we can safely
    // swap directories later.
    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<QualifiedIdJoinIndexImplV3> new_index,
        Create(filesystem_, temp_working_path_ddir.dir(), feature_flags_));
    ICING_RETURN_IF_ERROR(
        TransferIndex(document_id_old_to_new, new_index.get()));
    new_index->set_last_added_document_id(new_last_added_document_id);
    new_index->SetDirty();

    ICING_RETURN_IF_ERROR(new_index->PersistToDisk());
  }

  // Destruct current index's storage instances to safely swap directories.
  child_document_join_id_pair_array_.reset();
  parent_document_id_to_child_array_info_.reset();
  metadata_mmapped_file_.reset();
  if (!filesystem_.SwapFiles(temp_working_path_ddir.dir().c_str(),
                             working_path_.c_str())) {
    return absl_ports::InternalError(
        "Unable to apply new qualified id join index (v3) due to failed swap");
  }

  // Reinitialize qualified id join index.
  ICING_ASSIGN_OR_RETURN(
      MemoryMappedFile metadata_mmapped_file,
      MemoryMappedFile::Create(filesystem_, MakeMetadataFilePath(working_path_),
                               MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
                               /*max_file_size=*/kMetadataFileSize,
                               /*pre_mapping_file_offset=*/0,
                               /*pre_mapping_mmap_size=*/kMetadataFileSize));
  metadata_mmapped_file_ =
      std::make_unique<MemoryMappedFile>(std::move(metadata_mmapped_file));

  ICING_ASSIGN_OR_RETURN(
      parent_document_id_to_child_array_info_,
      FileBackedVector<ArrayInfo>::Create(
          filesystem_,
          MakeParentDocumentIdToChildArrayInfoFilePath(working_path_),
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
          FileBackedVector<ArrayInfo>::kMaxFileSize,
          /*pre_mapping_mmap_size=*/0));

  ICING_ASSIGN_OR_RETURN(
      child_document_join_id_pair_array_,
      FileBackedVector<DocumentJoinIdPair>::Create(
          filesystem_, MakeChildDocumentJoinIdPairArrayFilePath(working_path_),
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
          FileBackedVector<DocumentJoinIdPair>::kMaxFileSize,
          /*pre_mapping_mmap_size=*/0));

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status QualifiedIdJoinIndexImplV3::Clear() {
  SetDirty();

  parent_document_id_to_child_array_info_.reset();
  // Discard and reinitialize parent_document_id_to_child_array_info.
  std::string parent_document_id_to_child_array_info_file_path =
      MakeParentDocumentIdToChildArrayInfoFilePath(working_path_);
  if (!filesystem_.DeleteFile(
          parent_document_id_to_child_array_info_file_path.c_str())) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Failed to clear parent document id to child array info: ",
        parent_document_id_to_child_array_info_file_path));
  }
  ICING_ASSIGN_OR_RETURN(
      parent_document_id_to_child_array_info_,
      FileBackedVector<ArrayInfo>::Create(
          filesystem_, parent_document_id_to_child_array_info_file_path,
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
          FileBackedVector<ArrayInfo>::kMaxFileSize,
          /*pre_mapping_mmap_size=*/0));

  child_document_join_id_pair_array_.reset();
  // Discard and reinitialize child_document_join_id_pair_array.
  std::string child_document_join_id_pair_array_file_path =
      MakeChildDocumentJoinIdPairArrayFilePath(working_path_);
  if (!filesystem_.DeleteFile(
          child_document_join_id_pair_array_file_path.c_str())) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Failed to clear child document join id pair array: ",
        child_document_join_id_pair_array_file_path));
  }
  ICING_ASSIGN_OR_RETURN(
      child_document_join_id_pair_array_,
      FileBackedVector<DocumentJoinIdPair>::Create(
          filesystem_, child_document_join_id_pair_array_file_path,
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
          FileBackedVector<DocumentJoinIdPair>::kMaxFileSize,
          /*pre_mapping_mmap_size=*/0));

  info().num_data = 0;
  info().last_added_document_id = kInvalidDocumentId;
  return libtextclassifier3::Status::OK;
}

/* static */ libtextclassifier3::StatusOr<
    std::unique_ptr<QualifiedIdJoinIndexImplV3>>
QualifiedIdJoinIndexImplV3::InitializeNewFiles(
    const Filesystem& filesystem, std::string working_path,
    const FeatureFlags& feature_flags) {
  // Create working directory.
  if (!filesystem.CreateDirectoryRecursively(working_path.c_str())) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Failed to create directory: ", working_path));
  }

  // Initialize metadata file.
  // - Create MemoryMappedFile with pre-mapping and call GrowAndRemapIfNecessary
  //   to grow the underlying file.
  // - Initialize metadata content by writing 0 bytes to the memory region
  //   directly.
  ICING_ASSIGN_OR_RETURN(
      MemoryMappedFile metadata_mmapped_file,
      MemoryMappedFile::Create(filesystem, MakeMetadataFilePath(working_path),
                               MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
                               /*max_file_size=*/kMetadataFileSize,
                               /*pre_mapping_file_offset=*/0,
                               /*pre_mapping_mmap_size=*/kMetadataFileSize));
  ICING_RETURN_IF_ERROR(metadata_mmapped_file.GrowAndRemapIfNecessary(
      /*file_offset=*/0, /*mmap_size=*/kMetadataFileSize));
  memset(metadata_mmapped_file.mutable_region(), 0, kMetadataFileSize);

  // Initialize parent_document_id_to_child_array_info.
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<FileBackedVector<ArrayInfo>>
          parent_document_id_to_child_array_info,
      FileBackedVector<ArrayInfo>::Create(
          filesystem,
          MakeParentDocumentIdToChildArrayInfoFilePath(working_path),
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
          FileBackedVector<ArrayInfo>::kMaxFileSize,
          /*pre_mapping_mmap_size=*/0));

  // Initialize child_document_join_id_pair_array.
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<FileBackedVector<DocumentJoinIdPair>>
          child_document_join_id_pair_array,
      FileBackedVector<DocumentJoinIdPair>::Create(
          filesystem, MakeChildDocumentJoinIdPairArrayFilePath(working_path),
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
          FileBackedVector<DocumentJoinIdPair>::kMaxFileSize,
          /*pre_mapping_mmap_size=*/0));

  // Create instance.
  auto new_join_index = std::unique_ptr<QualifiedIdJoinIndexImplV3>(
      new QualifiedIdJoinIndexImplV3(
          filesystem, std::move(working_path),
          std::make_unique<MemoryMappedFile>(std::move(metadata_mmapped_file)),
          std::move(parent_document_id_to_child_array_info),
          std::move(child_document_join_id_pair_array), feature_flags));
  // Initialize info content.
  new_join_index->info().magic = Info::kMagic;
  new_join_index->info().num_data = 0;
  new_join_index->info().last_added_document_id = kInvalidDocumentId;

  // Initialize new PersistentStorage. The initial checksums will be computed
  // and set via InitializeNewStorage.
  ICING_RETURN_IF_ERROR(new_join_index->InitializeNewStorage());

  return new_join_index;
}

/* static */ libtextclassifier3::StatusOr<
    std::unique_ptr<QualifiedIdJoinIndexImplV3>>
QualifiedIdJoinIndexImplV3::InitializeExistingFiles(
    const Filesystem& filesystem, std::string working_path,
    const FeatureFlags& feature_flags) {
  // Initialize metadata file.
  ICING_ASSIGN_OR_RETURN(
      MemoryMappedFile metadata_mmapped_file,
      MemoryMappedFile::Create(filesystem, MakeMetadataFilePath(working_path),
                               MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
                               /*max_file_size=*/kMetadataFileSize,
                               /*pre_mapping_file_offset=*/0,
                               /*pre_mapping_mmap_size=*/kMetadataFileSize));
  if (metadata_mmapped_file.available_size() != kMetadataFileSize) {
    return absl_ports::FailedPreconditionError("Incorrect metadata file size");
  }

  // Initialize parent_document_id_to_child_array_info. Set mmap pre-mapping
  // size to 0, but MemoryMappedFile will still mmap to the file size.
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<FileBackedVector<ArrayInfo>>
          parent_document_id_to_child_array_info,
      FileBackedVector<ArrayInfo>::Create(
          filesystem,
          MakeParentDocumentIdToChildArrayInfoFilePath(working_path),
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
          FileBackedVector<ArrayInfo>::kMaxFileSize,
          /*pre_mapping_mmap_size=*/0));

  // Initialize child_document_join_id_pair_array. Set mmap pre-mapping size to
  // 0, but MemoryMappedFile will still mmap to the file size.
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<FileBackedVector<DocumentJoinIdPair>>
          child_document_join_id_pair_array,
      FileBackedVector<DocumentJoinIdPair>::Create(
          filesystem, MakeChildDocumentJoinIdPairArrayFilePath(working_path),
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
          FileBackedVector<DocumentJoinIdPair>::kMaxFileSize,
          /*pre_mapping_mmap_size=*/0));

  // Create instance.
  auto join_index = std::unique_ptr<QualifiedIdJoinIndexImplV3>(
      new QualifiedIdJoinIndexImplV3(
          filesystem, std::move(working_path),
          std::make_unique<MemoryMappedFile>(std::move(metadata_mmapped_file)),
          std::move(parent_document_id_to_child_array_info),
          std::move(child_document_join_id_pair_array), feature_flags));

  // Initialize existing PersistentStorage. Checksums will be validated.
  ICING_RETURN_IF_ERROR(join_index->InitializeExistingStorage());

  // Validate magic.
  if (join_index->info().magic != Info::kMagic) {
    return absl_ports::FailedPreconditionError("Incorrect magic value");
  }

  return join_index;
}

libtextclassifier3::Status
QualifiedIdJoinIndexImplV3::AppendChildDocumentJoinIdPairsForParent(
    DocumentId parent_document_id,
    std::vector<DocumentJoinIdPair>&& child_document_join_id_pairs) {
  if (child_document_join_id_pairs.empty()) {
    return libtextclassifier3::Status::OK;
  }

  // Step 1: extend parent_document_id_to_child_array_info_ if necessary.
  ICING_RETURN_IF_ERROR(
      ExtendParentDocumentIdToChildArrayInfoIfNecessary(parent_document_id));

  // Step 2: get the parent's child DocumentJoinIdPair mutable array (extend if
  //         necessary), append new child_document_join_id_pairs to the array,
  //         and assign the new ArrayInfo to
  //         parent_document_id_to_child_array_info_.
  ICING_ASSIGN_OR_RETURN(
      const ArrayInfo* array_info,
      parent_document_id_to_child_array_info_->Get(parent_document_id));
  ICING_ASSIGN_OR_RETURN(
      GetMutableAndExtendResult result,
      GetMutableAndExtendChildDocumentJoinIdPairArrayIfNecessary(
          *array_info, /*num_to_add=*/child_document_join_id_pairs.size()));
  // - [0, result.array_info.used_length) contain valid elements.
  // - We will write new elements starting from index
  //   result.array_info.used_length, and update the used_length.
  result.mutable_arr.SetArray(/*idx=*/result.array_info.used_length,
                              /*arr=*/child_document_join_id_pairs.data(),
                              /*len=*/child_document_join_id_pairs.size());
  result.array_info.used_length += child_document_join_id_pairs.size();

  // Set ArrayInfo back to parent_document_id_to_child_array_info_.
  ICING_RETURN_IF_ERROR(parent_document_id_to_child_array_info_->Set(
      parent_document_id, result.array_info));

  // Update header.
  info().num_data += child_document_join_id_pairs.size();

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status
QualifiedIdJoinIndexImplV3::ExtendParentDocumentIdToChildArrayInfoIfNecessary(
    DocumentId parent_document_id) {
  if (parent_document_id >=
      parent_document_id_to_child_array_info_->num_elements()) {
    int32_t num_to_extend =
        (parent_document_id + 1) -
        parent_document_id_to_child_array_info_->num_elements();
    ICING_ASSIGN_OR_RETURN(
        FileBackedVector<ArrayInfo>::MutableArrayView mutable_arr,
        parent_document_id_to_child_array_info_->Allocate(num_to_extend));
    mutable_arr.Fill(/*idx=*/0, /*len=*/num_to_extend, kInvalidArrayInfo);
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<
    QualifiedIdJoinIndexImplV3::GetMutableAndExtendResult>
QualifiedIdJoinIndexImplV3::
    GetMutableAndExtendChildDocumentJoinIdPairArrayIfNecessary(
        const ArrayInfo& array_info, int32_t num_to_add) {
  if (num_to_add > kMaxNumChildrenPerParent) {
    return absl_ports::ResourceExhaustedError(
        absl_ports::StrCat("Too many children to add for a single parent: ",
                           std::to_string(num_to_add)));
  }

  if (!array_info.IsValid()) {
    // If the array info is invalid, then it means the array is not allocated
    // yet. Allocate a new array and fill it with invalid values.
    ArrayInfo new_array_info(
        /*index_in=*/child_document_join_id_pair_array_->num_elements(),
        /*length_in=*/math_util::NextPowerOf2(num_to_add),
        /*used_length_in=*/0);
    ICING_ASSIGN_OR_RETURN(
        FileBackedVector<DocumentJoinIdPair>::MutableArrayView new_mutable_arr,
        child_document_join_id_pair_array_->Allocate(new_array_info.length));
    new_mutable_arr.Fill(/*idx=*/0, /*len=*/new_array_info.length,
                         kInvalidDocumentJoinIdPair);

    return GetMutableAndExtendResult{.array_info = new_array_info,
                                     .mutable_arr = std::move(new_mutable_arr)};
  }

  // The original array is valid. Get it and check if it needs to be extended.
  ICING_ASSIGN_OR_RETURN(FileBackedVector<DocumentJoinIdPair>::MutableArrayView
                             original_mutable_arr,
                         child_document_join_id_pair_array_->GetMutable(
                             array_info.index, array_info.length));
  // - [0, array_info.used_length) contain valid elements.
  // - [array_info.used_length, array_info.length) are invalid elements and
  //   available for new elements.
  if (array_info.length - array_info.used_length >= num_to_add) {
    // Available space is enough to fit num_to_add. Return the original array.
    return GetMutableAndExtendResult{
        .array_info = array_info,
        .mutable_arr = std::move(original_mutable_arr)};
  }

  // Need to extend. The new array length should be able to fit all existing
  // elements and the newly added elements.
  //
  // Check the new # of elements should not exceed the max limit. Use
  // substraction to avoid overflow.
  if (num_to_add > kMaxNumChildrenPerParent - array_info.used_length) {
    return absl_ports::ResourceExhaustedError(absl_ports::StrCat(
        "Too many children to add for an existing parent: ",
        std::to_string(num_to_add),
        "; existing: ", std::to_string(array_info.used_length)));
  }

  if (array_info.index + array_info.length ==
      child_document_join_id_pair_array_->num_elements()) {
    // The original array is at the end of the FBV. We can extend and reshape it
    // directly without copying elements.
    //
    // (Note: # means data out of the array_info we're dealing with.)
    // Original FBV and array_info layout:
    // +-------------------------+---------------.------+
    // |#########################|     used      |unused|
    // +-------------------------+---------------.------+
    //                           ^                      ^
    //                         index                 FBV_END
    //                           |<-used_length->|
    //                           |<-      length      ->|
    //
    // New FBV and new_array_info layout after extension:
    // +-------------------------+---------------.------.------------------+
    // |#########################|     used      |unused|  extended slice  |
    // +-------------------------+---------------.------.------------------+
    //                           ^                                         ^
    //                         index                                    FBV_END
    //                           |<-used_length->|
    //                           |<-              new length             ->|
    ArrayInfo new_array_info = array_info;
    new_array_info.length =
        math_util::NextPowerOf2(array_info.used_length + num_to_add);

    // Extend the original array by allocating a new slice.
    ICING_ASSIGN_OR_RETURN(
        FileBackedVector<DocumentJoinIdPair>::MutableArrayView extended_slice,
        child_document_join_id_pair_array_->Allocate(new_array_info.length -
                                                     array_info.length));
    // Fill the extended slice with invalid values.
    extended_slice.Fill(/*idx=*/0,
                        /*len=*/new_array_info.length - array_info.length,
                        kInvalidDocumentJoinIdPair);

    // Construct the mutable array view for new_array_info and return.
    ICING_ASSIGN_OR_RETURN(
        FileBackedVector<DocumentJoinIdPair>::MutableArrayView mutable_arr,
        child_document_join_id_pair_array_->GetMutable(new_array_info.index,
                                                       new_array_info.length));

    return GetMutableAndExtendResult{.array_info = std::move(new_array_info),
                                     .mutable_arr = std::move(mutable_arr)};
  }

  // The original array is not at the end of the file. We allocate a new array
  // starting at the end of the FBV, copy all existing elements to the new
  // array, and fill the remaining with invalid values.
  //
  // Original FBV and array_info layout:
  // +---+---------------.------+------+
  // |###|     used      |unused|######|
  // +---+---------------.------+------+
  //     ^                             ^
  //   index                        FBV_END
  //     |<-used_length->|
  //     |<-      length      ->|
  //
  // New FBV and new_array_info layout after extension and moving:
  // +---+----------------------+------+---------------.-----------------------+
  // |###|      <INVALID>       |######|     used      |                       |
  // +---+----------------------+------+---------------.-----------------------+
  //                                   ^
  //                               new_index
  //                                   |<-used_length->|
  //                                   |<-            new_length             ->|
  ArrayInfo new_array_info(
      /*index_in=*/child_document_join_id_pair_array_->num_elements(),
      /*length_in=*/
      math_util::NextPowerOf2(array_info.used_length + num_to_add),
      /*used_length_in=*/array_info.used_length);
  ICING_ASSIGN_OR_RETURN(
      FileBackedVector<DocumentJoinIdPair>::MutableArrayView new_mutable_arr,
      child_document_join_id_pair_array_->Allocate(new_array_info.length));

  // Move all existing elements to the new array.
  new_mutable_arr.SetArray(/*idx=*/0, original_mutable_arr.data(),
                           array_info.used_length);
  // Fill the remaining with invalid values.
  new_mutable_arr.Fill(/*idx=*/array_info.used_length,
                       /*len=*/new_array_info.length - array_info.used_length,
                       kInvalidDocumentJoinIdPair);
  // Invalidate the original array.
  original_mutable_arr.Fill(
      /*idx=*/0, /*len=*/array_info.length, kInvalidDocumentJoinIdPair);
  return GetMutableAndExtendResult{.array_info = new_array_info,
                                   .mutable_arr = std::move(new_mutable_arr)};
}

libtextclassifier3::Status QualifiedIdJoinIndexImplV3::TransferIndex(
    const std::vector<DocumentId>& document_id_old_to_new,
    QualifiedIdJoinIndexImplV3* new_index) const {
  for (DocumentId old_parent_doc_id = 0;
       old_parent_doc_id <
       parent_document_id_to_child_array_info_->num_elements();
       ++old_parent_doc_id) {
    if (old_parent_doc_id >= document_id_old_to_new.size() ||
        document_id_old_to_new[old_parent_doc_id] == kInvalidDocumentId) {
      // Skip if the old parent document id is invalid after optimization.
      continue;
    }

    ICING_ASSIGN_OR_RETURN(
        const ArrayInfo* array_info,
        parent_document_id_to_child_array_info_->Get(old_parent_doc_id));
    if (!array_info->IsValid()) {
      continue;
    }
    ICING_ASSIGN_OR_RETURN(
        const DocumentJoinIdPair* ptr,
        child_document_join_id_pair_array_->Get(array_info->index));

    // Get all child DocumentJoinIdPairs and assign new child document ids.
    std::vector<DocumentJoinIdPair> new_child_doc_join_id_pairs;
    new_child_doc_join_id_pairs.reserve(array_info->length);
    for (int i = 0; i < array_info->used_length; ++i) {
      DocumentId old_child_doc_id = ptr[i].document_id();
      DocumentId new_child_doc_id =
          old_child_doc_id >= 0 &&
                  old_child_doc_id < document_id_old_to_new.size()
              ? document_id_old_to_new[old_child_doc_id]
              : kInvalidDocumentId;
      if (new_child_doc_id == kInvalidDocumentId) {
        continue;
      }

      new_child_doc_join_id_pairs.push_back(
          DocumentJoinIdPair(new_child_doc_id, ptr[i].joinable_property_id()));
    }

    ICING_RETURN_IF_ERROR(new_index->AppendChildDocumentJoinIdPairsForParent(
        document_id_old_to_new[old_parent_doc_id],
        std::move(new_child_doc_join_id_pairs)));
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status QualifiedIdJoinIndexImplV3::PersistMetadataToDisk() {
  // We can skip persisting metadata to disk only if both info and storage are
  // clean.
  if (is_initialized_ && !is_info_dirty() && !is_storage_dirty()) {
    return libtextclassifier3::Status::OK;
  }

  // Changes should have been applied to the underlying file when using
  // MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC, but call msync() as an
  // extra safety step to ensure they are written out.
  ICING_RETURN_IF_ERROR(metadata_mmapped_file_->PersistToDisk());
  is_info_dirty_ = false;
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status QualifiedIdJoinIndexImplV3::PersistStoragesToDisk() {
  if (is_initialized_ && !is_storage_dirty()) {
    return libtextclassifier3::Status::OK;
  }

  ICING_RETURN_IF_ERROR(
      parent_document_id_to_child_array_info_->PersistToDisk());
  ICING_RETURN_IF_ERROR(child_document_join_id_pair_array_->PersistToDisk());

  is_storage_dirty_ = false;
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<Crc32>
QualifiedIdJoinIndexImplV3::UpdateStoragesChecksum() {
  if (is_initialized_ && !is_storage_dirty()) {
    return Crc32(crcs().component_crcs.storages_crc);
  }

  // Compute crcs
  ICING_ASSIGN_OR_RETURN(
      Crc32 parent_document_id_to_child_array_info_crc,
      parent_document_id_to_child_array_info_->UpdateChecksum());
  ICING_ASSIGN_OR_RETURN(Crc32 child_document_join_id_pair_array_crc,
                         child_document_join_id_pair_array_->UpdateChecksum());

  return Crc32(parent_document_id_to_child_array_info_crc.Get() ^
               child_document_join_id_pair_array_crc.Get());
}

libtextclassifier3::StatusOr<Crc32>
QualifiedIdJoinIndexImplV3::GetInfoChecksum() const {
  if (is_initialized_ && !is_info_dirty()) {
    return Crc32(crcs().component_crcs.info_crc);
  }

  return info().GetChecksum();
}

libtextclassifier3::StatusOr<Crc32>
QualifiedIdJoinIndexImplV3::GetStoragesChecksum() const {
  if (is_initialized_ && !is_storage_dirty()) {
    return Crc32(crcs().component_crcs.storages_crc);
  }

  // Get checksums for all components.
  Crc32 parent_document_id_to_child_array_info_crc =
      parent_document_id_to_child_array_info_->GetChecksum();
  Crc32 child_document_join_id_pair_array_crc =
      child_document_join_id_pair_array_->GetChecksum();

  return Crc32(parent_document_id_to_child_array_info_crc.Get() ^
               child_document_join_id_pair_array_crc.Get());
}

}  // namespace lib
}  // namespace icing
