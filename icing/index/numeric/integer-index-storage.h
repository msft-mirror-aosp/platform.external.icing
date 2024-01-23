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

#ifndef ICING_INDEX_NUMERIC_INTEGER_INDEX_STORAGE_H_
#define ICING_INDEX_NUMERIC_INTEGER_INDEX_STORAGE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/file/posting_list/flash-index-storage.h"
#include "icing/file/posting_list/posting-list-identifier.h"
#include "icing/index/numeric/posting-list-used-integer-index-data-serializer.h"
#include "icing/util/crc32.h"

namespace icing {
namespace lib {

// IntegerIndexStorage: a class for indexing (persistent storage) and searching
// contents of integer type sections in documents.
// - Accepts new integer contents (a.k.a keys) and adds records (BasicHit, key)
//   into the integer index.
// - Stores records (BasicHit, key) in posting lists and compresses them.
// - Bucketizes these records by key to make range query more efficient and
//   manages them with the corresponding posting lists.
//   - When a posting list reaches the max size and is full, the mechanism of
//     PostingListAccessor is to create another new (max-size) posting list and
//     chain them together.
//   - It will be inefficient if we store all records in the same PL chain. E.g.
//     small range query needs to iterate through the whole PL chain but skips a
//     lot of non-relevant records (whose keys don't belong to the query range).
//   - Therefore, we implement the splitting mechanism to split a full max-size
//     posting list. Also adjust range of the original bucket and add new
//     buckets.
//   - Ranges of all buckets are disjoint and the union of them is [INT64_MIN,
//     INT64_MAX].
//   - Buckets should be sorted, so we can do binary search to find the desired
//     bucket(s). However, we may split a bucket into several buckets, and the
//     cost to insert newly created buckets is high.
//   - Thus, we introduce an unsorted bucket array for newly created buckets,
//     and merge unsorted buckets into the sorted bucket array only if length of
//     the unsorted bucket array exceeds the threshold. This mechanism will
//     reduce # of merging events and amortize the overall cost for bucket order
//     maintenance.
//     Note: some tree data structures (e.g. segment tree, B+ tree) maintain the
//     bucket order more efficiently than the sorted/unsorted bucket array
//     mechanism, but the implementation is more complicated and doesn't improve
//     the performance too much according to our analysis, so currently we
//     choose sorted/unsorted bucket array.
//   - Then we do binary search on the sorted bucket array and sequential search
//     on the unsorted bucket array.
class IntegerIndexStorage {
 public:
  // Crcs and Info will be written into the metadata file.
  // File layout: <Crcs><Info>
  // Crcs
  struct Crcs {
    static constexpr int32_t kFileOffset = 0;

    struct ComponentCrcs {
      uint32_t info_crc;
      uint32_t sorted_buckets_crc;
      uint32_t unsorted_buckets_crc;
      uint32_t flash_index_storage_crc;

      bool operator==(const ComponentCrcs& other) const {
        return info_crc == other.info_crc &&
               sorted_buckets_crc == other.sorted_buckets_crc &&
               unsorted_buckets_crc == other.unsorted_buckets_crc &&
               flash_index_storage_crc == other.flash_index_storage_crc;
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
    static constexpr int32_t kMagic = 0xc4bf0ccc;

    int32_t magic;
    int32_t num_keys;

    Crc32 ComputeChecksum() const {
      return Crc32(
          std::string_view(reinterpret_cast<const char*>(this), sizeof(Info)));
    }
  } __attribute__((packed));
  static_assert(sizeof(Info) == 8, "");

  // Bucket
  class Bucket {
   public:
    // Absolute max # of buckets allowed. Since the absolute max file size of
    // FileBackedVector on 32-bit platform is ~2^28, we can at most have ~13.4M
    // buckets. To make it power of 2, round it down to 2^23. Also since we're
    // using FileBackedVector to store buckets, add some static_asserts to
    // ensure numbers here are compatible with FileBackedVector.
    static constexpr int32_t kMaxNumBuckets = 1 << 23;

    explicit Bucket(int64_t key_lower, int64_t key_upper,
                    PostingListIdentifier posting_list_identifier)
        : key_lower_(key_lower),
          key_upper_(key_upper),
          posting_list_identifier_(posting_list_identifier) {}

    // For FileBackedVector
    bool operator==(const Bucket& other) const {
      return key_lower_ == other.key_lower_ && key_upper_ == other.key_upper_ &&
             posting_list_identifier_ == other.posting_list_identifier_;
    }

    PostingListIdentifier posting_list_identifier() const {
      return posting_list_identifier_;
    }
    void set_posting_list_identifier(
        PostingListIdentifier posting_list_identifier) {
      posting_list_identifier_ = posting_list_identifier;
    }

   private:
    int64_t key_lower_;
    int64_t key_upper_;
    PostingListIdentifier posting_list_identifier_;
  } __attribute__((packed));
  static_assert(sizeof(Bucket) == 20, "");
  static_assert(sizeof(Bucket) == FileBackedVector<Bucket>::kElementTypeSize,
                "Bucket type size is inconsistent with FileBackedVector "
                "element type size");
  static_assert(Bucket::kMaxNumBuckets <=
                    (FileBackedVector<Bucket>::kMaxFileSize -
                     FileBackedVector<Bucket>::Header::kHeaderSize) /
                        FileBackedVector<Bucket>::kElementTypeSize,
                "Max # of buckets cannot fit into FileBackedVector");

 private:
  explicit IntegerIndexStorage(
      const Filesystem& filesystem, std::string_view base_dir,
      PostingListUsedIntegerIndexDataSerializer* serializer,
      std::unique_ptr<MemoryMappedFile> metadata_mmapped_file,
      std::unique_ptr<FileBackedVector<Bucket>> sorted_buckets,
      std::unique_ptr<FileBackedVector<Bucket>> unsorted_buckets,
      std::unique_ptr<FlashIndexStorage> flash_index_storage);

  const Filesystem& filesystem_;
  std::string base_dir_;

  PostingListUsedIntegerIndexDataSerializer* serializer_;  // Does not own.

  std::unique_ptr<MemoryMappedFile> metadata_mmapped_file_;
  std::unique_ptr<FileBackedVector<Bucket>> sorted_buckets_;
  std::unique_ptr<FileBackedVector<Bucket>> unsorted_buckets_;
  std::unique_ptr<FlashIndexStorage> flash_index_storage_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_NUMERIC_INTEGER_INDEX_STORAGE_H_
