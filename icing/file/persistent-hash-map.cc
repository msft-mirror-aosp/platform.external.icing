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

#include "icing/file/persistent-hash-map.h"

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/util/crc32.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

// Helper function to check if there is no termination character '\0' in the
// key.
libtextclassifier3::Status ValidateKey(std::string_view key) {
  if (key.find('\0') != std::string_view::npos) {  // NOLINT
    return absl_ports::InvalidArgumentError(
        "Key cannot contain termination character '\\0'");
  }
  return libtextclassifier3::Status::OK;
}

// Helper function to convert the key to bucket index by hash.
//
// Returns:
//   int32_t: A valid bucket index with range [0, num_buckets - 1].
//   INTERNAL_ERROR if num_buckets == 0
libtextclassifier3::StatusOr<int32_t> HashKeyToBucketIndex(
    std::string_view key, int32_t num_buckets) {
  if (num_buckets == 0) {
    return absl_ports::InternalError("Should not have empty bucket");
  }
  return static_cast<int32_t>(std::hash<std::string_view>()(key) % num_buckets);
}

// Helper function to PWrite crcs and info to metadata_file_path. Note that
// metadata_file_path will be the normal or temporary (for branching use when
// rehashing) metadata file path.
libtextclassifier3::Status WriteMetadata(const Filesystem& filesystem,
                                         const char* metadata_file_path,
                                         const PersistentHashMap::Crcs* crcs,
                                         const PersistentHashMap::Info* info) {
  ScopedFd sfd(filesystem.OpenForWrite(metadata_file_path));
  if (!sfd.is_valid()) {
    return absl_ports::InternalError("Failed to create metadata file");
  }

  // Write crcs and info. File layout: <Crcs><Info>
  if (!filesystem.PWrite(sfd.get(), PersistentHashMap::Crcs::kFileOffset, crcs,
                         sizeof(PersistentHashMap::Crcs))) {
    return absl_ports::InternalError("Failed to write crcs into metadata file");
  }
  // Note that PWrite won't change the file offset, so we need to specify
  // the correct offset when writing Info.
  if (!filesystem.PWrite(sfd.get(), PersistentHashMap::Info::kFileOffset, info,
                         sizeof(PersistentHashMap::Info))) {
    return absl_ports::InternalError("Failed to write info into metadata file");
  }

  return libtextclassifier3::Status::OK;
}

// Helper function to update checksums from info and storages to a Crcs
// instance. Note that storages will be the normal instances used by
// PersistentHashMap, or the temporary instances (for branching use when
// rehashing).
libtextclassifier3::Status UpdateChecksums(
    PersistentHashMap::Crcs* crcs, PersistentHashMap::Info* info,
    FileBackedVector<PersistentHashMap::Bucket>* bucket_storage,
    FileBackedVector<PersistentHashMap::Entry>* entry_storage,
    FileBackedVector<char>* kv_storage) {
  // Compute crcs
  ICING_ASSIGN_OR_RETURN(Crc32 bucket_storage_crc,
                         bucket_storage->ComputeChecksum());
  ICING_ASSIGN_OR_RETURN(Crc32 entry_storage_crc,
                         entry_storage->ComputeChecksum());
  ICING_ASSIGN_OR_RETURN(Crc32 kv_storage_crc, kv_storage->ComputeChecksum());

  crcs->component_crcs.info_crc = info->ComputeChecksum().Get();
  crcs->component_crcs.bucket_storage_crc = bucket_storage_crc.Get();
  crcs->component_crcs.entry_storage_crc = entry_storage_crc.Get();
  crcs->component_crcs.kv_storage_crc = kv_storage_crc.Get();
  crcs->all_crc = crcs->component_crcs.ComputeChecksum().Get();

  return libtextclassifier3::Status::OK;
}

// Helper function to validate checksums.
libtextclassifier3::Status ValidateChecksums(
    const PersistentHashMap::Crcs* crcs, const PersistentHashMap::Info* info,
    FileBackedVector<PersistentHashMap::Bucket>* bucket_storage,
    FileBackedVector<PersistentHashMap::Entry>* entry_storage,
    FileBackedVector<char>* kv_storage) {
  if (crcs->all_crc != crcs->component_crcs.ComputeChecksum().Get()) {
    return absl_ports::FailedPreconditionError(
        "Invalid all crc for PersistentHashMap");
  }

  if (crcs->component_crcs.info_crc != info->ComputeChecksum().Get()) {
    return absl_ports::FailedPreconditionError(
        "Invalid info crc for PersistentHashMap");
  }

  ICING_ASSIGN_OR_RETURN(Crc32 bucket_storage_crc,
                         bucket_storage->ComputeChecksum());
  if (crcs->component_crcs.bucket_storage_crc != bucket_storage_crc.Get()) {
    return absl_ports::FailedPreconditionError(
        "Mismatch crc with PersistentHashMap bucket storage");
  }

  ICING_ASSIGN_OR_RETURN(Crc32 entry_storage_crc,
                         entry_storage->ComputeChecksum());
  if (crcs->component_crcs.entry_storage_crc != entry_storage_crc.Get()) {
    return absl_ports::FailedPreconditionError(
        "Mismatch crc with PersistentHashMap entry storage");
  }

  ICING_ASSIGN_OR_RETURN(Crc32 kv_storage_crc, kv_storage->ComputeChecksum());
  if (crcs->component_crcs.kv_storage_crc != kv_storage_crc.Get()) {
    return absl_ports::FailedPreconditionError(
        "Mismatch crc with PersistentHashMap key value storage");
  }

  return libtextclassifier3::Status::OK;
}

// Since metadata/bucket/entry storages should be branched when rehashing, we
// have to store them together under the same sub directory
// ("<base_dir>/<sub_dir>"). On the other hand, key-value storage won't be
// branched and it will be stored under <base_dir>.
//
// The following 4 methods are helper functions to get the correct path of
// metadata/bucket/entry/key-value storages, according to the given base
// directory and sub directory.
std::string GetMetadataFilePath(std::string_view base_dir,
                                std::string_view sub_dir) {
  return absl_ports::StrCat(base_dir, "/", sub_dir, "/",
                            PersistentHashMap::kFilePrefix, ".m");
}

std::string GetBucketStorageFilePath(std::string_view base_dir,
                                     std::string_view sub_dir) {
  return absl_ports::StrCat(base_dir, "/", sub_dir, "/",
                            PersistentHashMap::kFilePrefix, ".b");
}

std::string GetEntryStorageFilePath(std::string_view base_dir,
                                    std::string_view sub_dir) {
  return absl_ports::StrCat(base_dir, "/", sub_dir, "/",
                            PersistentHashMap::kFilePrefix, ".e");
}

std::string GetKeyValueStorageFilePath(std::string_view base_dir) {
  return absl_ports::StrCat(base_dir, "/", PersistentHashMap::kFilePrefix,
                            ".k");
}

}  // namespace

/* static */ libtextclassifier3::StatusOr<std::unique_ptr<PersistentHashMap>>
PersistentHashMap::Create(const Filesystem& filesystem,
                          std::string_view base_dir, int32_t value_type_size,
                          int32_t max_load_factor_percent) {
  if (!filesystem.FileExists(
          GetMetadataFilePath(base_dir, kSubDirectory).c_str()) ||
      !filesystem.FileExists(
          GetBucketStorageFilePath(base_dir, kSubDirectory).c_str()) ||
      !filesystem.FileExists(
          GetEntryStorageFilePath(base_dir, kSubDirectory).c_str()) ||
      !filesystem.FileExists(GetKeyValueStorageFilePath(base_dir).c_str())) {
    // TODO: erase all files if missing any.
    return InitializeNewFiles(filesystem, base_dir, value_type_size,
                              max_load_factor_percent);
  }
  return InitializeExistingFiles(filesystem, base_dir, value_type_size,
                                 max_load_factor_percent);
}

PersistentHashMap::~PersistentHashMap() {
  if (!PersistToDisk().ok()) {
    ICING_LOG(WARNING)
        << "Failed to persist hash map to disk while destructing " << base_dir_;
  }
}

libtextclassifier3::Status PersistentHashMap::Put(std::string_view key,
                                                  const void* value) {
  ICING_RETURN_IF_ERROR(ValidateKey(key));
  ICING_ASSIGN_OR_RETURN(
      int32_t bucket_idx,
      HashKeyToBucketIndex(key, bucket_storage_->num_elements()));

  ICING_ASSIGN_OR_RETURN(EntryIndexPair idx_pair,
                         FindEntryIndexByKey(bucket_idx, key));
  if (idx_pair.target_entry_index == Entry::kInvalidIndex) {
    // If not found, then insert new key value pair.
    return Insert(bucket_idx, key, value);
  }

  // Otherwise, overwrite the value.
  ICING_ASSIGN_OR_RETURN(const Entry* entry,
                         entry_storage_->Get(idx_pair.target_entry_index));

  int32_t kv_len = key.length() + 1 + info()->value_type_size;
  int32_t value_offset = key.length() + 1;
  ICING_ASSIGN_OR_RETURN(
      typename FileBackedVector<char>::MutableArrayView mutable_kv_arr,
      kv_storage_->GetMutable(entry->key_value_index(), kv_len));
  // It is the same key and value_size is fixed, so we can directly overwrite
  // serialized value.
  mutable_kv_arr.SetArray(value_offset, reinterpret_cast<const char*>(value),
                          info()->value_type_size);

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status PersistentHashMap::GetOrPut(std::string_view key,
                                                       void* next_value) {
  ICING_RETURN_IF_ERROR(ValidateKey(key));
  ICING_ASSIGN_OR_RETURN(
      int32_t bucket_idx,
      HashKeyToBucketIndex(key, bucket_storage_->num_elements()));

  ICING_ASSIGN_OR_RETURN(EntryIndexPair idx_pair,
                         FindEntryIndexByKey(bucket_idx, key));
  if (idx_pair.target_entry_index == Entry::kInvalidIndex) {
    // If not found, then insert new key value pair.
    return Insert(bucket_idx, key, next_value);
  }

  // Otherwise, copy the hash map value into next_value.
  return CopyEntryValue(idx_pair.target_entry_index, next_value);
}

libtextclassifier3::Status PersistentHashMap::Get(std::string_view key,
                                                  void* value) const {
  ICING_RETURN_IF_ERROR(ValidateKey(key));
  ICING_ASSIGN_OR_RETURN(
      int32_t bucket_idx,
      HashKeyToBucketIndex(key, bucket_storage_->num_elements()));

  ICING_ASSIGN_OR_RETURN(EntryIndexPair idx_pair,
                         FindEntryIndexByKey(bucket_idx, key));
  if (idx_pair.target_entry_index == Entry::kInvalidIndex) {
    return absl_ports::NotFoundError(
        absl_ports::StrCat("Key not found in PersistentHashMap ", base_dir_));
  }

  return CopyEntryValue(idx_pair.target_entry_index, value);
}

libtextclassifier3::Status PersistentHashMap::Delete(std::string_view key) {
  ICING_RETURN_IF_ERROR(ValidateKey(key));
  ICING_ASSIGN_OR_RETURN(
      int32_t bucket_idx,
      HashKeyToBucketIndex(key, bucket_storage_->num_elements()));

  ICING_ASSIGN_OR_RETURN(EntryIndexPair idx_pair,
                         FindEntryIndexByKey(bucket_idx, key));
  if (idx_pair.target_entry_index == Entry::kInvalidIndex) {
    return absl_ports::NotFoundError(
        absl_ports::StrCat("Key not found in PersistentHashMap ", base_dir_));
  }

  ICING_ASSIGN_OR_RETURN(
      typename FileBackedVector<Entry>::MutableView mutable_target_entry,
      entry_storage_->GetMutable(idx_pair.target_entry_index));
  if (idx_pair.prev_entry_index == Entry::kInvalidIndex) {
    // If prev_entry_idx is Entry::kInvalidIndex, then target_entry must be the
    // head element of the entry linked list, and we have to update
    // bucket->head_entry_index_.
    //
    // Before: target_entry (head) -> next_entry -> ...
    // After: next_entry (head) -> ...
    ICING_ASSIGN_OR_RETURN(
        typename FileBackedVector<Bucket>::MutableView mutable_bucket,
        bucket_storage_->GetMutable(bucket_idx));
    if (mutable_bucket.Get().head_entry_index() !=
        idx_pair.target_entry_index) {
      return absl_ports::InternalError(
          "Bucket head entry index is inconsistent with the actual entry linked"
          "list head. This shouldn't happen");
    }
    mutable_bucket.Get().set_head_entry_index(
        mutable_target_entry.Get().next_entry_index());
  } else {
    // Otherwise, connect prev_entry and next_entry, to remove target_entry from
    // the entry linked list.
    //
    // Before: ... -> prev_entry -> target_entry -> next_entry -> ...
    // After: ... -> prev_entry -> next_entry -> ...
    ICING_ASSIGN_OR_RETURN(
        typename FileBackedVector<Entry>::MutableView mutable_prev_entry,
        entry_storage_->GetMutable(idx_pair.prev_entry_index));
    mutable_prev_entry.Get().set_next_entry_index(
        mutable_target_entry.Get().next_entry_index());
  }

  // Zero out the key value bytes. It is necessary for iterator to iterate
  // through kv_storage and handle deleted keys properly.
  int32_t kv_len = key.length() + 1 + info()->value_type_size;
  ICING_RETURN_IF_ERROR(kv_storage_->Set(
      mutable_target_entry.Get().key_value_index(), kv_len, '\0'));

  // Invalidate target_entry
  mutable_target_entry.Get().set_key_value_index(kInvalidKVIndex);
  mutable_target_entry.Get().set_next_entry_index(Entry::kInvalidIndex);

  ++(info()->num_deleted_entries);

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status PersistentHashMap::PersistToDisk() {
  ICING_RETURN_IF_ERROR(bucket_storage_->PersistToDisk());
  ICING_RETURN_IF_ERROR(entry_storage_->PersistToDisk());
  ICING_RETURN_IF_ERROR(kv_storage_->PersistToDisk());

  ICING_RETURN_IF_ERROR(UpdateChecksums(crcs(), info(), bucket_storage_.get(),
                                        entry_storage_.get(),
                                        kv_storage_.get()));
  // Changes should have been applied to the underlying file when using
  // MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC, but call msync() as an
  // extra safety step to ensure they are written out.
  ICING_RETURN_IF_ERROR(metadata_mmapped_file_->PersistToDisk());

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<int64_t> PersistentHashMap::GetDiskUsage() const {
  ICING_ASSIGN_OR_RETURN(int64_t bucket_storage_disk_usage,
                         bucket_storage_->GetDiskUsage());
  ICING_ASSIGN_OR_RETURN(int64_t entry_storage_disk_usage,
                         entry_storage_->GetDiskUsage());
  ICING_ASSIGN_OR_RETURN(int64_t kv_storage_disk_usage,
                         kv_storage_->GetDiskUsage());

  int64_t total = bucket_storage_disk_usage + entry_storage_disk_usage +
                  kv_storage_disk_usage;
  Filesystem::IncrementByOrSetInvalid(
      filesystem_->GetDiskUsage(
          GetMetadataFilePath(base_dir_, kSubDirectory).c_str()),
      &total);

  if (total < 0 || total == Filesystem::kBadFileSize) {
    return absl_ports::InternalError(
        "Failed to get disk usage of PersistentHashMap");
  }
  return total;
}

libtextclassifier3::StatusOr<int64_t> PersistentHashMap::GetElementsSize()
    const {
  ICING_ASSIGN_OR_RETURN(int64_t bucket_storage_elements_size,
                         bucket_storage_->GetElementsFileSize());
  ICING_ASSIGN_OR_RETURN(int64_t entry_storage_elements_size,
                         entry_storage_->GetElementsFileSize());
  ICING_ASSIGN_OR_RETURN(int64_t kv_storage_elements_size,
                         kv_storage_->GetElementsFileSize());
  return bucket_storage_elements_size + entry_storage_elements_size +
         kv_storage_elements_size;
}

libtextclassifier3::StatusOr<Crc32> PersistentHashMap::ComputeChecksum() {
  Crcs* crcs_ptr = crcs();
  ICING_RETURN_IF_ERROR(UpdateChecksums(crcs_ptr, info(), bucket_storage_.get(),
                                        entry_storage_.get(),
                                        kv_storage_.get()));
  return Crc32(crcs_ptr->all_crc);
}

/* static */ libtextclassifier3::StatusOr<std::unique_ptr<PersistentHashMap>>
PersistentHashMap::InitializeNewFiles(const Filesystem& filesystem,
                                      std::string_view base_dir,
                                      int32_t value_type_size,
                                      int32_t max_load_factor_percent) {
  // Create directory.
  const std::string dir_path = absl_ports::StrCat(base_dir, "/", kSubDirectory);
  if (!filesystem.CreateDirectoryRecursively(dir_path.c_str())) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Failed to create directory: ", dir_path));
  }

  // Initialize 3 storages
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<FileBackedVector<Bucket>> bucket_storage,
      FileBackedVector<Bucket>::Create(
          filesystem, GetBucketStorageFilePath(base_dir, kSubDirectory),
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC));
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<FileBackedVector<Entry>> entry_storage,
      FileBackedVector<Entry>::Create(
          filesystem, GetEntryStorageFilePath(base_dir, kSubDirectory),
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC));
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<FileBackedVector<char>> kv_storage,
                         FileBackedVector<char>::Create(
                             filesystem, GetKeyValueStorageFilePath(base_dir),
                             MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC));

  // Initialize one bucket.
  ICING_RETURN_IF_ERROR(bucket_storage->Append(Bucket()));
  ICING_RETURN_IF_ERROR(bucket_storage->PersistToDisk());

  // Create and initialize new info
  Info new_info;
  new_info.version = kVersion;
  new_info.value_type_size = value_type_size;
  new_info.max_load_factor_percent = max_load_factor_percent;
  new_info.num_deleted_entries = 0;
  new_info.num_deleted_key_value_bytes = 0;

  // Compute checksums
  Crcs new_crcs;
  ICING_RETURN_IF_ERROR(UpdateChecksums(&new_crcs, &new_info,
                                        bucket_storage.get(),
                                        entry_storage.get(), kv_storage.get()));

  const std::string metadata_file_path =
      GetMetadataFilePath(base_dir, kSubDirectory);
  // Write new metadata file
  ICING_RETURN_IF_ERROR(WriteMetadata(filesystem, metadata_file_path.c_str(),
                                      &new_crcs, &new_info));

  // Mmap the content of the crcs and info.
  auto metadata_mmapped_file = std::make_unique<MemoryMappedFile>(
      filesystem, metadata_file_path,
      MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC);
  ICING_RETURN_IF_ERROR(metadata_mmapped_file->Remap(
      /*file_offset=*/0, /*mmap_size=*/sizeof(Crcs) + sizeof(Info)));

  return std::unique_ptr<PersistentHashMap>(new PersistentHashMap(
      filesystem, base_dir, std::move(metadata_mmapped_file),
      std::move(bucket_storage), std::move(entry_storage),
      std::move(kv_storage)));
}

/* static */ libtextclassifier3::StatusOr<std::unique_ptr<PersistentHashMap>>
PersistentHashMap::InitializeExistingFiles(const Filesystem& filesystem,
                                           std::string_view base_dir,
                                           int32_t value_type_size,
                                           int32_t max_load_factor_percent) {
  // Mmap the content of the crcs and info.
  auto metadata_mmapped_file = std::make_unique<MemoryMappedFile>(
      filesystem, GetMetadataFilePath(base_dir, kSubDirectory),
      MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC);
  ICING_RETURN_IF_ERROR(metadata_mmapped_file->Remap(
      /*file_offset=*/0, /*mmap_size=*/sizeof(Crcs) + sizeof(Info)));

  // Initialize 3 storages
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<FileBackedVector<Bucket>> bucket_storage,
      FileBackedVector<Bucket>::Create(
          filesystem, GetBucketStorageFilePath(base_dir, kSubDirectory),
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC));
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<FileBackedVector<Entry>> entry_storage,
      FileBackedVector<Entry>::Create(
          filesystem, GetEntryStorageFilePath(base_dir, kSubDirectory),
          MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC));
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<FileBackedVector<char>> kv_storage,
                         FileBackedVector<char>::Create(
                             filesystem, GetKeyValueStorageFilePath(base_dir),
                             MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC));

  Crcs* crcs_ptr = reinterpret_cast<Crcs*>(
      metadata_mmapped_file->mutable_region() + Crcs::kFileOffset);
  Info* info_ptr = reinterpret_cast<Info*>(
      metadata_mmapped_file->mutable_region() + Info::kFileOffset);

  // Value type size should be consistent.
  if (value_type_size != info_ptr->value_type_size) {
    return absl_ports::FailedPreconditionError("Incorrect value type size");
  }

  // Validate checksums of info and 3 storages.
  ICING_RETURN_IF_ERROR(
      ValidateChecksums(crcs_ptr, info_ptr, bucket_storage.get(),
                        entry_storage.get(), kv_storage.get()));

  // Allow max_load_factor_percent_ change.
  if (max_load_factor_percent != info_ptr->max_load_factor_percent) {
    ICING_VLOG(2) << "Changing max_load_factor_percent from "
                  << info_ptr->max_load_factor_percent << " to "
                  << max_load_factor_percent;

    info_ptr->max_load_factor_percent = max_load_factor_percent;
    crcs_ptr->component_crcs.info_crc = info_ptr->ComputeChecksum().Get();
    crcs_ptr->all_crc = crcs_ptr->component_crcs.ComputeChecksum().Get();
    ICING_RETURN_IF_ERROR(metadata_mmapped_file->PersistToDisk());
    // TODO(b/193919210): rehash if needed
  }

  return std::unique_ptr<PersistentHashMap>(new PersistentHashMap(
      filesystem, base_dir, std::move(metadata_mmapped_file),
      std::move(bucket_storage), std::move(entry_storage),
      std::move(kv_storage)));
}

libtextclassifier3::StatusOr<PersistentHashMap::EntryIndexPair>
PersistentHashMap::FindEntryIndexByKey(int32_t bucket_idx,
                                       std::string_view key) const {
  // Iterate all entries in the bucket, compare with key, and return the entry
  // index if exists.
  ICING_ASSIGN_OR_RETURN(const Bucket* bucket,
                         bucket_storage_->Get(bucket_idx));

  int32_t prev_entry_idx = Entry::kInvalidIndex;
  int32_t curr_entry_idx = bucket->head_entry_index();
  while (curr_entry_idx != Entry::kInvalidIndex) {
    ICING_ASSIGN_OR_RETURN(const Entry* entry,
                           entry_storage_->Get(curr_entry_idx));
    if (entry->key_value_index() == kInvalidKVIndex) {
      ICING_LOG(ERROR) << "Got an invalid key value index in the persistent "
                          "hash map bucket. This shouldn't happen";
      return absl_ports::InternalError("Unexpected invalid key value index");
    }
    ICING_ASSIGN_OR_RETURN(const char* kv_arr,
                           kv_storage_->Get(entry->key_value_index()));
    if (key.compare(kv_arr) == 0) {
      return EntryIndexPair(curr_entry_idx, prev_entry_idx);
    }

    prev_entry_idx = curr_entry_idx;
    curr_entry_idx = entry->next_entry_index();
  }

  return EntryIndexPair(curr_entry_idx, prev_entry_idx);
}

libtextclassifier3::Status PersistentHashMap::CopyEntryValue(
    int32_t entry_idx, void* value) const {
  ICING_ASSIGN_OR_RETURN(const Entry* entry, entry_storage_->Get(entry_idx));

  ICING_ASSIGN_OR_RETURN(const char* kv_arr,
                         kv_storage_->Get(entry->key_value_index()));
  int32_t value_offset = strlen(kv_arr) + 1;
  memcpy(value, kv_arr + value_offset, info()->value_type_size);

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status PersistentHashMap::Insert(int32_t bucket_idx,
                                                     std::string_view key,
                                                     const void* value) {
  // If size() + 1 exceeds Entry::kMaxNumEntries, then return error.
  if (size() > Entry::kMaxNumEntries - 1) {
    return absl_ports::ResourceExhaustedError("Cannot insert new entry");
  }

  ICING_ASSIGN_OR_RETURN(
      typename FileBackedVector<Bucket>::MutableView mutable_bucket,
      bucket_storage_->GetMutable(bucket_idx));

  // Append new key value.
  int32_t new_kv_idx = kv_storage_->num_elements();
  int32_t kv_len = key.size() + 1 + info()->value_type_size;
  int32_t value_offset = key.size() + 1;
  ICING_ASSIGN_OR_RETURN(
      typename FileBackedVector<char>::MutableArrayView mutable_new_kv_arr,
      kv_storage_->Allocate(kv_len));
  mutable_new_kv_arr.SetArray(/*idx=*/0, key.data(), key.size());
  mutable_new_kv_arr.SetArray(/*idx=*/key.size(), "\0", 1);
  mutable_new_kv_arr.SetArray(/*idx=*/value_offset,
                              reinterpret_cast<const char*>(value),
                              info()->value_type_size);

  // Append new entry.
  int32_t new_entry_idx = entry_storage_->num_elements();
  ICING_RETURN_IF_ERROR(entry_storage_->Append(
      Entry(new_kv_idx, mutable_bucket.Get().head_entry_index())));
  mutable_bucket.Get().set_head_entry_index(new_entry_idx);

  // TODO: rehash if needed

  return libtextclassifier3::Status::OK;
}

bool PersistentHashMap::Iterator::Advance() {
  // Jump over the current key value pair before advancing to the next valid
  // key value pair. In the first round (after construction), curr_key_len_
  // is 0, so don't jump over anything.
  if (curr_key_len_ != 0) {
    curr_kv_idx_ += curr_key_len_ + 1 + map_->info()->value_type_size;
    curr_key_len_ = 0;
  }

  // By skipping null chars, we will be automatically handling deleted entries
  // (which are zeroed out during deletion).
  for (const char* curr_kv_ptr = map_->kv_storage_->array() + curr_kv_idx_;
       curr_kv_idx_ < map_->kv_storage_->num_elements();
       ++curr_kv_ptr, ++curr_kv_idx_) {
    if (*curr_kv_ptr != '\0') {
      curr_key_len_ = strlen(curr_kv_ptr);
      return true;
    }
  }
  return false;
}

}  // namespace lib
}  // namespace icing
