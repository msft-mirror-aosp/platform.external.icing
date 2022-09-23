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

#ifndef ICING_FILE_PERSISTENT_HASH_MAP_H_
#define ICING_FILE_PERSISTENT_HASH_MAP_H_

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/util/crc32.h"

namespace icing {
namespace lib {

// Low level persistent hash map.
// It supports variant length serialized key + fixed length serialized value.
// Key and value can be any type, but callers should serialize key/value by
// themselves and pass raw bytes into the hash map, and the serialized key
// should not contain termination character '\0'.
class PersistentHashMap {
 public:
  // Crcs and Info will be written into the metadata file.
  // File layout: <Crcs><Info>
  // Crcs
  struct Crcs {
    static constexpr int32_t kFileOffset = 0;

    struct ComponentCrcs {
      uint32_t info_crc;
      uint32_t bucket_storage_crc;
      uint32_t entry_storage_crc;
      uint32_t kv_storage_crc;

      bool operator==(const ComponentCrcs& other) const {
        return info_crc == other.info_crc &&
               bucket_storage_crc == other.bucket_storage_crc &&
               entry_storage_crc == other.entry_storage_crc &&
               kv_storage_crc == other.kv_storage_crc;
      }

      Crc32 ComputeChecksum() const {
        return Crc32(std::string_view(reinterpret_cast<const char*>(this),
                                      sizeof(ComponentCrcs)));
      }
    } __attribute__((packed));

    bool operator==(const Crcs& other) const {
      return all_crc == other.all_crc && component_crcs == other.component_crcs;
    }

    uint32_t all_crc;
    ComponentCrcs component_crcs;
  } __attribute__((packed));
  static_assert(sizeof(Crcs) == 20, "");

  // Info
  struct Info {
    static constexpr int32_t kFileOffset = static_cast<int32_t>(sizeof(Crcs));

    int32_t version;
    int32_t value_type_size;
    int32_t max_load_factor_percent;
    int32_t num_deleted_entries;
    int32_t num_deleted_key_value_bytes;

    Crc32 ComputeChecksum() const {
      return Crc32(
          std::string_view(reinterpret_cast<const char*>(this), sizeof(Info)));
    }
  } __attribute__((packed));
  static_assert(sizeof(Info) == 20, "");

  // Bucket
  class Bucket {
   public:
    // Absolute max # of buckets allowed. Since max file size on Android is
    // 2^31-1, we can at most have ~2^29 buckets. To make it power of 2, round
    // it down to 2^28. Also since we're using FileBackedVector to store
    // buckets, add some static_asserts to ensure numbers here are compatible
    // with FileBackedVector.
    static constexpr int32_t kMaxNumBuckets = 1 << 28;

    explicit Bucket(int32_t head_entry_index = Entry::kInvalidIndex)
        : head_entry_index_(head_entry_index) {}

    // For FileBackedVector
    bool operator==(const Bucket& other) const {
      return head_entry_index_ == other.head_entry_index_;
    }

    int32_t head_entry_index() const { return head_entry_index_; }
    void set_head_entry_index(int32_t head_entry_index) {
      head_entry_index_ = head_entry_index;
    }

   private:
    int32_t head_entry_index_;
  } __attribute__((packed));
  static_assert(sizeof(Bucket) == 4, "");
  static_assert(sizeof(Bucket) == FileBackedVector<Bucket>::kElementTypeSize,
                "Bucket type size is inconsistent with FileBackedVector "
                "element type size");
  static_assert(Bucket::kMaxNumBuckets <=
                    (FileBackedVector<Bucket>::kMaxFileSize -
                     FileBackedVector<Bucket>::Header::kHeaderSize) /
                        FileBackedVector<Bucket>::kElementTypeSize,
                "Max # of buckets cannot fit into FileBackedVector");

  // Entry
  class Entry {
   public:
    // Absolute max # of entries allowed. Since max file size on Android is
    // 2^31-1, we can at most have ~2^28 entries. To make it power of 2, round
    // it down to 2^27. Also since we're using FileBackedVector to store
    // entries, add some static_asserts to ensure numbers here are compatible
    // with FileBackedVector.
    //
    // Still the actual max # of entries are determined by key-value storage,
    // since length of the key varies and affects # of actual key-value pairs
    // that can be stored.
    static constexpr int32_t kMaxNumEntries = 1 << 27;
    static constexpr int32_t kMaxIndex = kMaxNumEntries - 1;
    static constexpr int32_t kInvalidIndex = -1;

    explicit Entry(int32_t key_value_index, int32_t next_entry_index)
        : key_value_index_(key_value_index),
          next_entry_index_(next_entry_index) {}

    bool operator==(const Entry& other) const {
      return key_value_index_ == other.key_value_index_ &&
             next_entry_index_ == other.next_entry_index_;
    }

    int32_t key_value_index() const { return key_value_index_; }
    void set_key_value_index(int32_t key_value_index) {
      key_value_index_ = key_value_index;
    }

    int32_t next_entry_index() const { return next_entry_index_; }
    void set_next_entry_index(int32_t next_entry_index) {
      next_entry_index_ = next_entry_index;
    }

   private:
    int32_t key_value_index_;
    int32_t next_entry_index_;
  } __attribute__((packed));
  static_assert(sizeof(Entry) == 8, "");
  static_assert(sizeof(Entry) == FileBackedVector<Entry>::kElementTypeSize,
                "Entry type size is inconsistent with FileBackedVector "
                "element type size");
  static_assert(Entry::kMaxNumEntries <=
                    (FileBackedVector<Entry>::kMaxFileSize -
                     FileBackedVector<Entry>::Header::kHeaderSize) /
                        FileBackedVector<Entry>::kElementTypeSize,
                "Max # of entries cannot fit into FileBackedVector");

  // Key-value serialized type
  static constexpr int32_t kMaxKVTotalByteSize =
      (FileBackedVector<char>::kMaxFileSize -
       FileBackedVector<char>::Header::kHeaderSize) /
      FileBackedVector<char>::kElementTypeSize;
  static constexpr int32_t kMaxKVIndex = kMaxKVTotalByteSize - 1;
  static constexpr int32_t kInvalidKVIndex = -1;
  static_assert(sizeof(char) == FileBackedVector<char>::kElementTypeSize,
                "Char type size is inconsistent with FileBackedVector element "
                "type size");

  static constexpr int32_t kVersion = 1;
  static constexpr int32_t kDefaultMaxLoadFactorPercent = 75;

  static constexpr std::string_view kFilePrefix = "persistent_hash_map";
  // Only metadata, bucket, entry files are stored under this sub-directory, for
  // rehashing branching use.
  static constexpr std::string_view kSubDirectory = "dynamic";

  // Creates a new PersistentHashMap to read/write/delete key value pairs.
  //
  // filesystem: Object to make system level calls
  // base_dir: Specifies the directory for all persistent hash map related
  //           sub-directory and files to be stored. If base_dir doesn't exist,
  //           then PersistentHashMap will automatically create it. If files
  //           exist, then it will initialize the hash map from existing files.
  // value_type_size: (fixed) size of the serialized value type for hash map.
  // max_load_factor_percent: percentage of the max loading for the hash map.
  //                          load_factor_percent = 100 * num_keys / num_buckets
  //                          If load_factor_percent exceeds
  //                          max_load_factor_percent, then rehash will be
  //                          invoked (and # of buckets will be doubled).
  //                          Note that load_factor_percent exceeding 100 is
  //                          considered valid.
  //
  // Returns:
  //   FAILED_PRECONDITION_ERROR if the file checksum doesn't match the stored
  //                             checksum.
  //   INTERNAL_ERROR on I/O errors.
  //   Any FileBackedVector errors.
  static libtextclassifier3::StatusOr<std::unique_ptr<PersistentHashMap>>
  Create(const Filesystem& filesystem, std::string_view base_dir,
         int32_t value_type_size,
         int32_t max_load_factor_percent = kDefaultMaxLoadFactorPercent);

  ~PersistentHashMap();

  // Update a key value pair. If key does not exist, then insert (key, value)
  // into the storage. Otherwise overwrite the value into the storage.
  //
  // REQUIRES: the buffer pointed to by value must be of value_size()
  //
  // Returns:
  //   OK on success
  //   RESOURCE_EXHAUSTED_ERROR if # of entries reach kMaxNumEntries
  //   INVALID_ARGUMENT_ERROR if the key is invalid (i.e. contains '\0')
  //   INTERNAL_ERROR on I/O error or any data inconsistency
  //   Any FileBackedVector errors
  libtextclassifier3::Status Put(std::string_view key, const void* value);

  // If key does not exist, then insert (key, next_value) into the storage.
  // Otherwise, copy the hash map value into next_value.
  //
  // REQUIRES: the buffer pointed to by next_value must be of value_size()
  //
  // Returns:
  //   OK on success
  //   INVALID_ARGUMENT_ERROR if the key is invalid (i.e. contains '\0')
  //   INTERNAL_ERROR on I/O error or any data inconsistency
  //   Any FileBackedVector errors
  libtextclassifier3::Status GetOrPut(std::string_view key, void* next_value);

  // Get the value by key from the storage. If key exists, then copy the hash
  // map value into into value buffer. Otherwise, return NOT_FOUND_ERROR.
  //
  // REQUIRES: the buffer pointed to by value must be of value_size()
  //
  // Returns:
  //   OK on success
  //   NOT_FOUND_ERROR if the key doesn't exist
  //   INVALID_ARGUMENT_ERROR if the key is invalid (i.e. contains '\0')
  //   INTERNAL_ERROR on I/O error or any data inconsistency
  //   Any FileBackedVector errors
  libtextclassifier3::Status Get(std::string_view key, void* value) const;

  // Flushes content to underlying files.
  //
  // Returns:
  //   OK on success
  //   INTERNAL_ERROR on I/O error
  libtextclassifier3::Status PersistToDisk();

  // Calculates and returns the disk usage (metadata + 3 storages total file
  // size) in bytes.
  //
  // Returns:
  //   Disk usage on success
  //   INTERNAL_ERROR on I/O error
  libtextclassifier3::StatusOr<int64_t> GetDiskUsage() const;

  // Returns the total file size of the all the elements held in the persistent
  // hash map. File size is in bytes. This excludes the size of any internal
  // metadata, i.e. crcs/info of persistent hash map, file backed vector's
  // header.
  //
  // Returns:
  //   File size on success
  //   INTERNAL_ERROR on I/O error
  libtextclassifier3::StatusOr<int64_t> GetElementsSize() const;

  // Updates all checksums of the persistent hash map components and returns
  // all_crc.
  //
  // Returns:
  //   Crc of all components (all_crc) on success
  //   INTERNAL_ERROR if any data inconsistency
  libtextclassifier3::StatusOr<Crc32> ComputeChecksum();

  int32_t size() const {
    return entry_storage_->num_elements() - info()->num_deleted_entries;
  }

  bool empty() const { return size() == 0; }

 private:
  explicit PersistentHashMap(
      const Filesystem& filesystem, std::string_view base_dir,
      std::unique_ptr<MemoryMappedFile> metadata_mmapped_file,
      std::unique_ptr<FileBackedVector<Bucket>> bucket_storage,
      std::unique_ptr<FileBackedVector<Entry>> entry_storage,
      std::unique_ptr<FileBackedVector<char>> kv_storage)
      : filesystem_(&filesystem),
        base_dir_(base_dir),
        metadata_mmapped_file_(std::move(metadata_mmapped_file)),
        bucket_storage_(std::move(bucket_storage)),
        entry_storage_(std::move(entry_storage)),
        kv_storage_(std::move(kv_storage)) {}

  static libtextclassifier3::StatusOr<std::unique_ptr<PersistentHashMap>>
  InitializeNewFiles(const Filesystem& filesystem, std::string_view base_dir,
                     int32_t value_type_size, int32_t max_load_factor_percent);

  static libtextclassifier3::StatusOr<std::unique_ptr<PersistentHashMap>>
  InitializeExistingFiles(const Filesystem& filesystem,
                          std::string_view base_dir, int32_t value_type_size,
                          int32_t max_load_factor_percent);

  // Find the index of the key entry from a bucket (specified by bucket index).
  // The caller should specify the desired bucket index.
  //
  // Returns:
  //   int32_t: on success, the index of the entry, or Entry::kInvalidIndex if
  //            not found
  //   INTERNAL_ERROR if any content inconsistency
  //   Any FileBackedVector errors
  libtextclassifier3::StatusOr<int32_t> FindEntryIndexByKey(
      int32_t bucket_idx, std::string_view key) const;

  // Copy the hash map value of the entry into value buffer.
  //
  // REQUIRES: entry_idx should be valid.
  // REQUIRES: the buffer pointed to by value must be of value_size()
  //
  // Returns:
  //   OK on success
  //   Any FileBackedVector errors
  libtextclassifier3::Status CopyEntryValue(int32_t entry_idx,
                                            void* value) const;

  // Insert a new key value pair into a bucket (specified by the bucket index).
  // The caller should specify the desired bucket index and make sure that the
  // key is not present in the hash map before calling.
  //
  // Returns:
  //   OK on success
  //   Any FileBackedVector errors
  libtextclassifier3::Status Insert(int32_t bucket_idx, std::string_view key,
                                    const void* value);

  Crcs* crcs() {
    return reinterpret_cast<Crcs*>(metadata_mmapped_file_->mutable_region() +
                                   Crcs::kFileOffset);
  }

  Info* info() {
    return reinterpret_cast<Info*>(metadata_mmapped_file_->mutable_region() +
                                   Info::kFileOffset);
  }

  const Info* info() const {
    return reinterpret_cast<const Info*>(metadata_mmapped_file_->region() +
                                         Info::kFileOffset);
  }

  const Filesystem* filesystem_;
  std::string base_dir_;

  std::unique_ptr<MemoryMappedFile> metadata_mmapped_file_;

  // Storages
  std::unique_ptr<FileBackedVector<Bucket>> bucket_storage_;
  std::unique_ptr<FileBackedVector<Entry>> entry_storage_;
  std::unique_ptr<FileBackedVector<char>> kv_storage_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_FILE_PERSISTENT_HASH_MAP_H_
