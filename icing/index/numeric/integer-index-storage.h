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
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/file/posting_list/flash-index-storage.h"
#include "icing/file/posting_list/posting-list-identifier.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/index/numeric/posting-list-integer-index-serializer.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
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

    libtextclassifier3::Status Serialize(const Filesystem& filesystem,
                                         int fd) const {
      if (!filesystem.PWrite(fd, kFileOffset, this, sizeof(*this))) {
        return absl_ports::InternalError("Failed to write crcs into file");
      }
      return libtextclassifier3::Status::OK;
    }

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

    libtextclassifier3::Status Serialize(const Filesystem& filesystem,
                                         int fd) const {
      if (!filesystem.PWrite(fd, kFileOffset, this, sizeof(*this))) {
        return absl_ports::InternalError("Failed to write info into file");
      }
      return libtextclassifier3::Status::OK;
    }

    Crc32 ComputeChecksum() const {
      return Crc32(
          std::string_view(reinterpret_cast<const char*>(this), sizeof(Info)));
    }
  } __attribute__((packed));
  static_assert(sizeof(Info) == 8, "");

  static constexpr int32_t kMetadataFileSize = sizeof(Crcs) + sizeof(Info);
  static_assert(kMetadataFileSize == 28);

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
                    PostingListIdentifier posting_list_identifier =
                        PostingListIdentifier::kInvalid)
        : key_lower_(key_lower),
          key_upper_(key_upper),
          posting_list_identifier_(posting_list_identifier) {}

    bool operator<(const Bucket& other) const {
      return key_lower_ < other.key_lower_;
    }

    // For FileBackedVector
    bool operator==(const Bucket& other) const {
      return key_lower_ == other.key_lower_ && key_upper_ == other.key_upper_ &&
             posting_list_identifier_ == other.posting_list_identifier_;
    }

    int64_t key_lower() const { return key_lower_; }

    int64_t key_upper() const { return key_upper_; }

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

  struct Options {
    explicit Options() {}

    explicit Options(std::vector<Bucket> custom_init_sorted_buckets_in,
                     std::vector<Bucket> custom_init_unsorted_buckets_in)
        : custom_init_sorted_buckets(std::move(custom_init_sorted_buckets_in)),
          custom_init_unsorted_buckets(
              std::move(custom_init_unsorted_buckets_in)) {}

    bool IsValid() const;

    bool HasCustomInitBuckets() const {
      return !custom_init_sorted_buckets.empty() ||
             !custom_init_unsorted_buckets.empty();
    }

    // Custom buckets when initializing new files. If both are empty, then the
    // initial bucket is (INT64_MIN, INT64_MAX). Usually we only set them in the
    // unit test. Note that all buckets in custom_init_sorted_buckets and
    // custom_init_unsorted_buckets should be disjoint and the range union
    // should be [INT64_MIN, INT64_MAX].
    std::vector<Bucket> custom_init_sorted_buckets;
    std::vector<Bucket> custom_init_unsorted_buckets;
  };

  static constexpr std::string_view kSubDirectory = "storage_dir";
  static constexpr std::string_view kFilePrefix = "integer_index_storage";

  // Creates a new IntegerIndexStorage instance to index integers. For directory
  // management purpose, we define working_dir as "<base_dir>/storage_dir", and
  // all underlying files will be stored under it. If any of the underlying file
  // is missing, then delete the whole working_dir and (re)initialize with new
  // ones. Otherwise initialize and create the instance by existing files.
  //
  // filesystem: Object to make system level calls
  // base_dir: Specifies the base directory for all integer index data related
  //           files to be stored. As mentioned above, all files will be stored
  //           under working_dir (which is "<base_dir>/storage_dir").
  // options: Options instance.
  // posting_list_serializer: a PostingListIntegerIndexSerializer instance to
  //                          serialize/deserialize integer index data to/from
  //                          posting lists.
  //
  // Returns:
  //   - INVALID_ARGUMENT_ERROR if any value in options is invalid.
  //   - FAILED_PRECONDITION_ERROR if the file checksum doesn't match the stored
  //                               checksum.
  //   - INTERNAL_ERROR on I/O errors.
  //   - Any FileBackedVector/FlashIndexStorage errors.
  static libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndexStorage>>
  Create(const Filesystem& filesystem, std::string_view base_dir,
         Options options,
         PostingListIntegerIndexSerializer* posting_list_serializer);

  // Delete copy and move constructor/assignment operator.
  IntegerIndexStorage(const IntegerIndexStorage&) = delete;
  IntegerIndexStorage& operator=(const IntegerIndexStorage&) = delete;

  IntegerIndexStorage(IntegerIndexStorage&&) = delete;
  IntegerIndexStorage& operator=(IntegerIndexStorage&&) = delete;

  ~IntegerIndexStorage();

  // Batch adds new keys (of the same DocumentId and SectionId) into the integer
  // index storage.
  // Note that since we separate different property names into different integer
  // index storages, it is impossible to have keys in a single document across
  // multiple sections to add into the same integer index storage.
  //
  // Returns:
  //   - OK on success
  //   - Any FileBackedVector or PostingList errors
  libtextclassifier3::Status AddKeys(DocumentId document_id,
                                     SectionId section_id,
                                     std::vector<int64_t>&& new_keys);

  // Returns a DocHitInfoIteratorNumeric<int64_t> (in DocHitInfoIterator
  // interface type format) for iterating through all docs which have the
  // specified (integer) property contents in range [query_key_lower,
  // query_key_upper].
  // When iterating through all relevant doc hits, it:
  // - Merges multiple SectionIds of doc hits with same DocumentId into a single
  //   SectionIdMask and constructs DocHitInfo.
  // - Returns DocHitInfo in descending DocumentId order.
  //
  // Returns:
  //   - On success: a DocHitInfoIterator(Numeric)
  //   - INVALID_ARGUMENT_ERROR if query_key_lower > query_key_upper
  //   - Any FileBackedVector or PostingList errors
  libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>> GetIterator(
      int64_t query_key_lower, int64_t query_key_upper) const;

  // Flushes content to underlying files.
  //
  // Returns:
  //   - OK on success
  //   - INTERNAL_ERROR on I/O error
  libtextclassifier3::Status PersistToDisk();

 private:
  static libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndexStorage>>
  InitializeNewFiles(
      const Filesystem& filesystem, std::string&& working_dir,
      Options&& options,
      PostingListIntegerIndexSerializer* posting_list_serializer);

  static libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndexStorage>>
  InitializeExistingFiles(
      const Filesystem& filesystem, std::string&& working_dir,
      Options&& options,
      PostingListIntegerIndexSerializer* posting_list_serializer);

  explicit IntegerIndexStorage(
      const Filesystem& filesystem, std::string&& working_dir,
      Options&& options,
      PostingListIntegerIndexSerializer* posting_list_serializer,
      std::unique_ptr<MemoryMappedFile> metadata_mmapped_file,
      std::unique_ptr<FileBackedVector<Bucket>> sorted_buckets,
      std::unique_ptr<FileBackedVector<Bucket>> unsorted_buckets,
      std::unique_ptr<FlashIndexStorage> flash_index_storage)
      : filesystem_(filesystem),
        working_dir_(std::move(working_dir)),
        options_(std::move(options)),
        posting_list_serializer_(posting_list_serializer),
        metadata_mmapped_file_(std::move(metadata_mmapped_file)),
        sorted_buckets_(std::move(sorted_buckets)),
        unsorted_buckets_(std::move(unsorted_buckets)),
        flash_index_storage_(std::move(flash_index_storage)) {}

  // Helper function to add keys in range [it_start, it_end) into the given
  // bucket. It handles the bucket and its corresponding posting list(s) to make
  // searching and indexing efficient.
  //
  // When the (single) posting list of the bucket is full:
  // - If the size of posting list hasn't reached the max size, then just simply
  //   add a new key into it, and PostingListAccessor mechanism will
  //   automatically double the size of the posting list.
  // - Else:
  //   - If the bucket is splittable (i.e. key_lower < key_upper), then split it
  //     into several new buckets with new ranges, and split the data (according
  //     to their keys and the range of new buckets) of the original posting
  //     list into several new posting lists.
  //     TODO(b/259743562): [Optimization 1] implement split
  //   - Otherwise, just simply add a new key into it, and PostingListAccessor
  //     mechanism will automatically create a new max size posting list and
  //     chain them.
  //
  // Returns:
  //   - On success: a vector of new Buckets (to add into the unsorted bucket
  //     array later)
  //   - Any FileBackedVector or PostingList errors
  libtextclassifier3::StatusOr<std::vector<Bucket>>
  AddKeysIntoBucketAndSplitIfNecessary(
      DocumentId document_id, SectionId section_id,
      const std::vector<int64_t>::const_iterator& it_start,
      const std::vector<int64_t>::const_iterator& it_end,
      FileBackedVector<Bucket>::MutableView& mutable_bucket);

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

  const Filesystem& filesystem_;
  std::string working_dir_;

  Options options_;

  PostingListIntegerIndexSerializer* posting_list_serializer_;  // Does not own.

  std::unique_ptr<MemoryMappedFile> metadata_mmapped_file_;
  std::unique_ptr<FileBackedVector<Bucket>> sorted_buckets_;
  std::unique_ptr<FileBackedVector<Bucket>> unsorted_buckets_;
  std::unique_ptr<FlashIndexStorage> flash_index_storage_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_NUMERIC_INTEGER_INDEX_STORAGE_H_
