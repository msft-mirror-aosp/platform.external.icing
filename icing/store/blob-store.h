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
#include <unordered_set>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/file/filesystem.h"
#include "icing/proto/document.pb.h"
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
    bool is_committed;

    // The param needed for dynamic trie, we shouldn't call this constructor
    // directly.
    BlobInfo() : BlobInfo(/*creation_time_ms=*/-1, /*is_committed=*/false) {}

    BlobInfo(int64_t creation_time_ms, bool is_committed)
        : creation_time_ms(creation_time_ms), is_committed(is_committed) {}
  } __attribute__((packed));
  static_assert(sizeof(BlobInfo) == 9, "Invalid BlobInfo size");

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
      const Filesystem* filesystem, std::string base_dir, const Clock* clock);

  // Gets or creates a file for write only purpose for the given blob handle.
  // To mark the blob is completed written, CommitBlob must be called. Once
  // CommitBlob is called, the blob is sealed and rewrite is not allowed.
  //
  // Returns:
  //   File descriptor (writable) on success
  //   INVALID_ARGUMENT on invalid blob handle
  //   FAILED_PRECONDITION if the blob has already been committed
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::StatusOr<int> OpenWrite(
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
      const PropertyProto::BlobHandleProto& blob_handle);

  // Persists the blobs to disk.
  libtextclassifier3::Status PersistToDisk();

 private:
  explicit BlobStore(const Filesystem* filesystem, std::string base_dir,
                     const Clock* clock,
                     std::unique_ptr<KeyMapper<BlobInfo>> blob_info_mapper,
                     std::unordered_set<std::string> known_file_names)
      : filesystem_(*filesystem),
        base_dir_(std::move(base_dir)),
        clock_(*clock),
        blob_info_mapper_(std::move(blob_info_mapper)),
        known_file_names_(std::move(known_file_names)) {}

  libtextclassifier3::StatusOr<BlobStore::BlobInfo> GetOrCreateBlobInfo(
      const std::string& blob_handle_str);

  const Filesystem& filesystem_;
  std::string base_dir_;
  const Clock& clock_;

  std::unique_ptr<KeyMapper<BlobInfo>> blob_info_mapper_;
  std::unordered_set<std::string> known_file_names_;
  bool has_mutated_ = false;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_STORE_BLOB_STORE_H_
