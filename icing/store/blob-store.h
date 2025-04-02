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
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/proto/blob.pb.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/storage.pb.h"
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
  // Builds a string representation of a blob handle.
  // The string is used as the key in the key mapper.
  static std::string BuildBlobHandleStr(
      const PropertyProto::BlobHandleProto& blob_handle);

  // Factory function to create a BlobStore instance. The base directory is
  // used to persist blobs. If a blob store was previously created with
  // this directory, it will reload the files saved by the last instance.
  //
  // The callers must create the base directory before calling this function.
  //
  // Returns:
  //   A BlobStore on success
  //   FAILED_PRECONDITION_ERROR on any null pointer input
  //   INTERNAL_ERROR on I/O error
  static libtextclassifier3::StatusOr<BlobStore> Create(
      const Filesystem* filesystem, std::string base_dir, const Clock* clock,
      int64_t orphan_blob_time_to_live_ms, int32_t compression_level,
      int32_t compression_mem_level, bool manage_blob_files);

  // Gets or creates a file for write only purpose for the given blob handle.
  // To mark the blob is completed written, CommitBlob must be called. Once
  // CommitBlob is called, the blob is sealed and rewrite is not allowed.
  //
  // If Icing does not manage blob files, this method only creates necessary
  // metadata for the blob but does not open or manage the file descriptor. The
  // caller is responsible for opening, writing to, and closing the file using
  // the returned file name.
  //
  // Otherwise, a file descriptor is returned, and it is the user's
  // responsibility to close the file descriptor after writing is done and
  // should not operate on the file descriptor after commit or remove it.
  //
  // Returns:
  //   OK with results on success
  //   InvalidArgumentError on invalid blob handle
  //   FailedPreconditionError if the blob is already opened for write
  //   AlreadyExistsError if the blob is already committed
  //   InternalError on IO error
  BlobProto OpenWrite(const PropertyProto::BlobHandleProto& blob_handle);

  // Removes a blob file and blob handle from the blob store.
  //
  // This will remove the blob on any state. No matter it's committed or not or
  // it has reference document links or not.
  //
  // If Icing does not manage blob files, this method only removes the metadata
  // entry from the blob store, but does not delete the actual blob file. The
  // caller is responsible for deleting the blob file.
  //
  // Returns:
  //   OK with results on success
  //   InvalidArgumentError on invalid blob handle
  //   NotFoundError if the blob is not found
  //   InternalError on IO error
  BlobProto RemoveBlob(const PropertyProto::BlobHandleProto& blob_handle);

  // Gets a file for read only purpose for the given blob handle.
  // The blob must be committed by calling CommitBlob otherwise it is not
  // accessible.
  //
  // If Icing does not manage blob files, this method only returns the file name
  // associated with the blob but does not open or manage the file descriptor.
  // The caller is responsible for opening, reading from, and closing the file
  // using the returned file name.
  //
  // Otherwise, a file descriptor is returned, and it is the user's
  // responsibility to close the file descriptor after reading.
  //
  // Returns:
  //   OK with results on success
  //   InvalidArgumentError on invalid blob handle
  //   NotFoundError if the blob is not found or is not committed
  BlobProto OpenRead(const PropertyProto::BlobHandleProto& blob_handle) const;

  // Commits the given blob when writing of the blob via OpenWrite is complete.
  // Before the blob is committed, it is not visible to any reader
  // via OpenRead. After the blob is committed, it is not allowed to rewrite or
  // update the content.
  //
  // If Icing does not manage blob files, this method marks the blob as
  // committed in the metadata store. The caller is responsible for verifying
  // the digest of the blob file.
  //
  // Returns:
  //   OK on success
  //   AlreadyExistsError if the blob is already committed
  //   InvalidArgumentError on invalid blob handle or if the digest is mismatch
  //     with file content
  //   NotFoundError if the blob is not found
  BlobProto CommitBlob(const PropertyProto::BlobHandleProto& blob_handle);

  // Persists the blobs to disk.
  libtextclassifier3::Status PersistToDisk();

  // Gets the potentially optimizable blob handles.
  //
  // A blob will be consider as a potentially optimizable blob if it created
  // before the orphan_blob_time_to_live_ms. And the blob should be removed if
  // it has no reference document links to it.
  std::unordered_set<std::string> GetPotentiallyOptimizableBlobHandles() const;

  // Optimize the blob store and remove dead blob files.
  //
  // A blob will be consider as a dead blob and removed if it meets BOTH of
  // following conditions
  //  1: has no reference document links to it
  //  2: It's mature.
  //
  // Returns:
  //   The list of expired blob file names to be removed on success. If Icing
  //   manages blob files, this list will be empty.
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::StatusOr<std::vector<std::string>> Optimize(
      const std::unordered_set<std::string>& dead_blob_handles);

  // Calculates the StorageInfo for the Blob Store.
  //
  // Returns:
  //   Vector of NamespaceBlobStorageInfoProto contains size of each namespace.
  //   INTERNAL_ERROR on I/O error
  libtextclassifier3::StatusOr<std::vector<NamespaceBlobStorageInfoProto>>
  GetStorageInfo() const;

 private:
  explicit BlobStore(
      const Filesystem* filesystem, std::string base_dir, const Clock* clock,
      int64_t orphan_blob_time_to_live_ms, int32_t compression_level,
      int32_t compression_mem_level, bool manage_blob_files,
      std::unique_ptr<PortableFileBackedProtoLog<BlobInfoProto>> blob_info_log,
      std::unordered_map<std::string, int32_t> blob_handle_to_offset,
      std::unordered_set<std::string> known_file_names)
      : filesystem_(*filesystem),
        base_dir_(std::move(base_dir)),
        clock_(*clock),
        orphan_blob_time_to_live_ms_(orphan_blob_time_to_live_ms),
        compression_level_(compression_level),
        compression_mem_level_(compression_mem_level),
        manage_blob_files_(manage_blob_files),
        blob_info_log_(std::move(blob_info_log)),
        blob_handle_to_offset_(std::move(blob_handle_to_offset)),
        known_file_names_(std::move(known_file_names)) {}

  libtextclassifier3::StatusOr<BlobInfoProto> GetBlobInfo(
      const PropertyProto::BlobHandleProto& blob_handle) const;

  libtextclassifier3::StatusOr<BlobInfoProto> GetOrCreateBlobInfo(
      const std::string& blob_handle_str,
      const PropertyProto::BlobHandleProto& blob_handle);

  libtextclassifier3::Status CommitBlobMetadata(
      const PropertyProto::BlobHandleProto& blob_handle);

  const Filesystem& filesystem_;
  std::string base_dir_;
  const Clock& clock_;
  int64_t orphan_blob_time_to_live_ms_;
  int32_t compression_level_;
  int32_t compression_mem_level_;
  bool manage_blob_files_;

  // The ground truth blob info log file, which is used to read/write/erase
  // BlobInfoProto.
  std::unique_ptr<PortableFileBackedProtoLog<BlobInfoProto>> blob_info_log_;

  // The map for BlobHandle string to the offset of BlobInfoProto in the
  // BlobInfoProto log file.
  // The keys are the Encoded CString from BlobHandleProto.
  std::unordered_map<std::string, int32_t> blob_handle_to_offset_;

  // The set of used file names to store blobs in the blob store.
  std::unordered_set<std::string> known_file_names_;

  bool has_mutated_ = false;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_STORE_BLOB_STORE_H_
