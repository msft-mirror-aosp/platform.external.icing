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

#ifndef ICING_STORE_BLOB_STORE_H_
#define ICING_STORE_BLOB_STORE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/file/filesystem.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/storage.pb.h"
#include "icing/store/key-mapper.h"
#include "icing/util/clock.h"

namespace icing {
namespace lib {

// Provides storage interfaces for Blobs.
//
// The BlobStore is responsible for storing blobs in a directory and for
// ensuring that the directory is in a consistent state.
//
// A blob is a file that is stored in the BlobStore. A blob is identified by
// a blob handle, which is a unique identifier for the blob.
//
// Any blob that is written to the BlobStore must be committed before it can be
// read. A blob can be committed only once. After a blob is committed, it is
// not allowed to be updated.
//
// The BlobStore is not thread-safe.
class BlobStore {
 public:
  // BlobInfo holds information about a blob. This struct will be stored as the
  // value in the dynamic trie key mapper, so it must be packed to avoid
  // padding (which potentially causes use-of-uninitialized-value errors).
  struct BlobInfo {
    // The creation time of the blob. This is used to determine when to delete
    // the orphaned blobs.
    // We are using creation_time_ms to be file name of the blob, so this field
    // is unique for each blob.
    int64_t creation_time_ms;

    // The offset of the package name stored in the package name file.
    int32_t package_offset;

    // The param needed for dynamic trie, we shouldn't call this constructor
    // directly.
    BlobInfo() : BlobInfo(/*creation_time_ms=*/-1, /*package_offset=*/-1) {}

    BlobInfo(int64_t creation_time_ms, int32_t package_offset)
        : creation_time_ms(creation_time_ms), package_offset(package_offset) {}
  } __attribute__((packed));
  static_assert(sizeof(BlobInfo) == 12, "Invalid BlobInfo size");

  // Factory function to create a BlobStore instance. The base directory is
  // used to persist blobs. If a blob store was previously created with
  // this directory, it will reload the files saved by the last instance.
  //
  // The callers must create the base directory before calling this function.
  //
  // Returns:
  //   A BlobStore on success
  //   FAILED_PRECONDITION on any null pointer input
  //   INTERNAL_ERROR on I/O error
  static libtextclassifier3::StatusOr<BlobStore> Create(
      const Filesystem* filesystem, std::string base_dir, const Clock* clock,
      int64_t orphan_blob_time_to_live_ms);

  // Gets or creates a file for write only purpose for the given blob handle.
  // To mark the blob is completed written, CommitBlob must be called. Once
  // CommitBlob is called, the blob is sealed and rewrite is not allowed.
  //
  // Returns:
  //   File descriptor (writable) on success
  //   INVALID_ARGUMENT on invalid blob handle
  //   ALREADY_EXISTS if the blob has already been committed
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::StatusOr<int> OpenWrite(
      std::string_view package_name,
      const PropertyProto::BlobHandleProto& blob_handle);

  // Gets a file for read only purpose for the given blob handle.
  // Will only succeed for blobs that were committed by calling CommitBlob.
  //
  // Returns:
  //   File descriptor (read only) on success
  //   INVALID_ARGUMENT on invalid blob handle
  //   NOT_FOUND on blob is not found or is not committed
  libtextclassifier3::StatusOr<int> OpenRead(
      const PropertyProto::BlobHandleProto& blob_handle);

  // Commits the given blob, if the blob is finished wrote via OpenWrite.
  // Before the blob is committed, it is not visible to any reader via OpenRead.
  // After the blob is committed, it is not allowed to rewrite or update the
  // content.
  //
  // Returns:
  //   OK on the blob is successfully committed.
  //   ALREADY_EXISTS on the blob is already committed, this is no op.
  //   INVALID_ARGUMENT on invalid blob handle or digest is mismatch with
  //                        file content.
  //   NOT_FOUND on blob is not found.
  libtextclassifier3::Status CommitBlob(
      std::string_view package_name,
      const PropertyProto::BlobHandleProto& blob_handle);

  // Persists the blobs to disk.
  libtextclassifier3::Status PersistToDisk();

  // Gets the potentially optimizable blob handles.
  //
  // A blob will be consider as a potentially optimizable blob if it created
  // before the orphan_blob_time_to_live_ms. And the blob should be removed if
  // it has no reference document links to it.
  std::unordered_set<std::string> GetPotentiallyOptimizableBlobHandles();

  // Optimize the blob store and remove dead blob files.
  //
  // A blob will be consider as a dead blob and removed if it meets BOTH of
  // following conditions
  //  1: has no reference document links to it
  //  2: It's mature.
  //
  // Returns:
  //   OK on success
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::Status Optimize(
      const std::unordered_set<std::string>& dead_blob_handles);

  // Calculates the StorageInfo for the Blob Store.
  //
  // Returns:
  //   Vector of PackageBlobStorageInfoProto contains size of each package.
  //   INTERNAL on I/O error
  libtextclassifier3::StatusOr<std::vector<PackageBlobStorageInfoProto>>
  GetStorageInfo() const;

 private:
  explicit BlobStore(
      const Filesystem* filesystem, std::string base_dir, const Clock* clock,
      int64_t orphan_blob_time_to_live_ms, ScopedFd package_name_fd,
      std::unique_ptr<KeyMapper<BlobInfo>> blob_info_mapper,
      std::unordered_map<std::string, int32_t> package_name_to_offset,
      std::unordered_set<std::string> known_file_names)
      : filesystem_(*filesystem),
        base_dir_(std::move(base_dir)),
        clock_(*clock),
        orphan_blob_time_to_live_ms_(orphan_blob_time_to_live_ms),
        package_name_fd_(std::move(package_name_fd)),
        blob_info_mapper_(std::move(blob_info_mapper)),
        package_name_to_offset_(std::move(package_name_to_offset)),
        known_file_names_(std::move(known_file_names)) {}

  libtextclassifier3::StatusOr<BlobStore::BlobInfo> GetOrCreateBlobInfo(
      const std::string& blob_handle_str, std::string_view package_name);

  libtextclassifier3::StatusOr<int32_t> GetOrCreatePackageOffset(
      const std::string& package_name);

  // Optimize the package name file. Package names that have no blob will be
  // removed. The old package name file will be replaced by a new file with new
  // offsets.
  //
  // Return a map from old offset to new offset.
  libtextclassifier3::StatusOr<std::unordered_map<int32_t, int32_t>>
  OptimizePackageNameFile(const std::unordered_set<int32_t>& existing_offsets);

  const Filesystem& filesystem_;
  std::string base_dir_;
  const Clock& clock_;
  int64_t orphan_blob_time_to_live_ms_;
  ScopedFd package_name_fd_;

  // The key mapper for BlobHandle string to BlobInfo.
  // The keys are the Encoded CString from 2 states of blobs.
  // 1. pending blob: The key is constructed of digest, package name, and label.
  //    Represents a blob that is being written, and the owner is the caller
  //    package.
  // 2. committed blob: The key is constructed of digest and label.
  //    Represents a blob that is already written and committed, and the owner
  //    is the Icing.
  // TODO(b/273591938) Separate the key mapper for pending blob and committed
  // blob.
  std::unique_ptr<KeyMapper<BlobInfo>> blob_info_mapper_;
  std::unordered_map<std::string, int32_t> package_name_to_offset_;
  // The map to tracking sent file descriptors for write.
  // The key is the pending blob handle CString which is constructed of
  // digest, package name, and label.
  // The value is the set of file descriptors for write.
  std::unordered_map<std::string, int> file_descriptors_for_write_;
  std::unordered_set<std::string> known_file_names_;
  bool has_mutated_ = false;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_STORE_BLOB_STORE_H_
