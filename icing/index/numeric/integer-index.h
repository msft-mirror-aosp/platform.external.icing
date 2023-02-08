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

#ifndef ICING_INDEX_NUMERIC_INTEGER_INDEX_H_
#define ICING_INDEX_NUMERIC_INTEGER_INDEX_H_

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/index/numeric/integer-index-storage.h"
#include "icing/index/numeric/numeric-index.h"
#include "icing/index/numeric/posting-list-integer-index-serializer.h"
#include "icing/store/document-id.h"
#include "icing/util/crc32.h"

namespace icing {
namespace lib {

// IntegerIndex: a wrapper class for managing IntegerIndexStorage (a lower level
// persistent storage class for indexing and searching contents of integer type
// sections in documents) instances for different property paths.
// We separate indexable integer data from different properties into different
// storages, and IntegerIndex manages and handles indexable integer data
// appropriately to their corresponding IntegerIndexStorage instance according
// to the given property path.
class IntegerIndex : public NumericIndex<int64_t> {
 public:
  using PropertyToStorageMapType =
      std::unordered_map<std::string, std::unique_ptr<IntegerIndexStorage>>;

  struct Info {
    static constexpr int32_t kMagic = 0x238a3dcb;

    int32_t magic;
    DocumentId last_added_document_id;

    Crc32 ComputeChecksum() const {
      return Crc32(
          std::string_view(reinterpret_cast<const char*>(this), sizeof(Info)));
    }
  } __attribute__((packed));
  static_assert(sizeof(Info) == 8, "");

  // Metadata file layout: <Crcs><Info>
  static constexpr int32_t kCrcsMetadataFileOffset = 0;
  static constexpr int32_t kInfoMetadataFileOffset =
      static_cast<int32_t>(sizeof(Crcs));
  static constexpr int32_t kMetadataFileSize = sizeof(Crcs) + sizeof(Info);
  static_assert(kMetadataFileSize == 20, "");

  static constexpr WorkingPathType kWorkingPathType =
      WorkingPathType::kDirectory;
  static constexpr std::string_view kFilePrefix = "integer_index";

  // Creates a new IntegerIndex instance to index integers. If any of the
  // underlying file is missing, then delete the whole working_path and
  // (re)initialize with new ones. Otherwise initialize and create the instance
  // by existing files.
  //
  // filesystem: Object to make system level calls
  // working_path: Specifies the working path for PersistentStorage.
  //               IntegerIndex uses working path as working directory and all
  //               related files will be stored under this directory. See
  //               PersistentStorage for more details about the concept of
  //               working_path.
  //
  // Returns:
  //   - FAILED_PRECONDITION_ERROR if the file checksum doesn't match the stored
  //                               checksum.
  //   - INTERNAL_ERROR on I/O errors.
  //   - Any FileBackedVector/MemoryMappedFile errors.
  static libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndex>> Create(
      const Filesystem& filesystem, std::string working_path);

  ~IntegerIndex() override;

  // TODO(b/249829533): implement these functions and add comments.
  std::unique_ptr<typename NumericIndex<int64_t>::Editor> Edit(
      std::string_view property_path, DocumentId document_id,
      SectionId section_id) override;

  libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIterator>> GetIterator(
      std::string_view property_path, int64_t key_lower,
      int64_t key_upper) const override;

  // Clears all integer index data.
  //
  // Returns:
  //   - OK on success
  //   - INTERNAL_ERROR on I/O error
  libtextclassifier3::Status Reset() override;

 private:
  explicit IntegerIndex(const Filesystem& filesystem,
                        std::string&& working_path,
                        std::unique_ptr<PostingListIntegerIndexSerializer>
                            posting_list_serializer,
                        std::unique_ptr<MemoryMappedFile> metadata_mmapped_file,
                        PropertyToStorageMapType&& property_to_storage_map)
      : NumericIndex<int64_t>(filesystem, std::move(working_path),
                              kWorkingPathType),
        posting_list_serializer_(std::move(posting_list_serializer)),
        metadata_mmapped_file_(std::move(metadata_mmapped_file)),
        property_to_storage_map_(std::move(property_to_storage_map)) {}

  static libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndex>>
  InitializeNewFiles(const Filesystem& filesystem, std::string&& working_path);

  static libtextclassifier3::StatusOr<std::unique_ptr<IntegerIndex>>
  InitializeExistingFiles(const Filesystem& filesystem,
                          std::string&& working_path);

  // Flushes contents of all storages to underlying files.
  //
  // Returns:
  //   - OK on success
  //   - INTERNAL_ERROR on I/O error
  libtextclassifier3::Status PersistStoragesToDisk() override;

  // Flushes contents of metadata file.
  //
  // Returns:
  //   - OK on success
  //   - INTERNAL_ERROR on I/O error
  libtextclassifier3::Status PersistMetadataToDisk() override;

  // Computes and returns Info checksum.
  //
  // Returns:
  //   - Crc of the Info on success
  libtextclassifier3::StatusOr<Crc32> ComputeInfoChecksum() override;

  // Computes and returns all storages checksum. Checksums of bucket_storage_,
  // entry_storage_ and kv_storage_ will be combined together by XOR.
  //
  // Returns:
  //   - Crc of all storages on success
  //   - INTERNAL_ERROR if any data inconsistency
  libtextclassifier3::StatusOr<Crc32> ComputeStoragesChecksum() override;

  Crcs& crcs() override {
    return *reinterpret_cast<Crcs*>(metadata_mmapped_file_->mutable_region() +
                                    kCrcsMetadataFileOffset);
  }

  const Crcs& crcs() const override {
    return *reinterpret_cast<const Crcs*>(metadata_mmapped_file_->region() +
                                          kCrcsMetadataFileOffset);
  }

  Info* info() {
    return reinterpret_cast<Info*>(metadata_mmapped_file_->mutable_region() +
                                   kInfoMetadataFileOffset);
  }

  const Info* info() const {
    return reinterpret_cast<const Info*>(metadata_mmapped_file_->region() +
                                         kInfoMetadataFileOffset);
  }

  std::unique_ptr<PostingListIntegerIndexSerializer> posting_list_serializer_;

  std::unique_ptr<MemoryMappedFile> metadata_mmapped_file_;

  // Property path to integer index storage map.
  PropertyToStorageMapType property_to_storage_map_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_NUMERIC_INTEGER_INDEX_H_
