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

#include "icing/store/blob-store.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/file/filesystem.h"
#include "icing/proto/document.pb.h"
#include "icing/store/dynamic-trie-key-mapper.h"
#include "icing/store/key-mapper.h"
#include "icing/util/clock.h"
#include "icing/util/encode-util.h"
#include "icing/util/sha256.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

// - Key: sha 256 digest (32 bytes)
// - Value: BlobInfo (24 bytes)
// Allow max 1M of blob info entries.
static constexpr int32_t kBlobInfoMapperMaxSize = 56 * 1024 * 1024;  // 56 MiB
static constexpr int32_t kSha256LengthBytes = 32;
static constexpr int32_t kReadBufferSize = 8192;

// TODO(b/273591938): Remove orphan files in optimize().
BlobStore::BlobStore(const Filesystem* filesystem, std::string_view base_dir,
                     const Clock* clock,
                     std::unique_ptr<KeyMapper<BlobInfo>> blob_info_mapper,
                     std::unordered_set<std::string> known_file_names)
    : filesystem_(filesystem),
      base_dir_(std::string(base_dir)),
      clock_(*clock),
      blob_info_mapper_(std::move(blob_info_mapper)),
      known_file_names_(std::move(known_file_names)) {}

libtextclassifier3::Status ValidateBlobHandle(
    PropertyProto::BlobHandleProto blob_handle) {
  if (blob_handle.digest().size() != kSha256LengthBytes) {
    return absl_ports::InvalidArgumentError(
        "Invalid blob handle. The digest is not sha 256 digest.");
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<BlobStore> BlobStore::Create(
    const Filesystem* filesystem, const std::string& base_dir,
    const Clock* clock) {
  ICING_RETURN_ERROR_IF_NULL(filesystem);
  ICING_RETURN_ERROR_IF_NULL(clock);
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<KeyMapper<BlobInfo>> blob_info_mapper,
                         DynamicTrieKeyMapper<BlobInfo>::Create(
                             *filesystem, base_dir, kBlobInfoMapperMaxSize));

  // Load existing file names.
  std::vector<std::string> file_names;
  if (!filesystem->ListDirectory(base_dir.c_str(), &file_names)) {
    return absl_ports::InternalError("Failed to list directory.");
  }
  std::unordered_set<std::string> known_file_names(
      std::make_move_iterator(file_names.begin()),
      std::make_move_iterator(file_names.end()));
  return BlobStore(filesystem, base_dir, clock, std::move(blob_info_mapper),
                   std::move(known_file_names));
}

libtextclassifier3::StatusOr<int32_t> BlobStore::OpenWrite(
    PropertyProto::BlobHandleProto blob_handle) {
  ICING_RETURN_IF_ERROR(ValidateBlobHandle(blob_handle));
  std::string blob_handle_str =
      encode_util::EncodeStringToCString(blob_handle.digest()) +
      blob_handle.label();
  ICING_ASSIGN_OR_RETURN(BlobInfo blob_info,
                         GetOrCreateBlobInfo(blob_handle_str));
  if (blob_info.is_committed) {
    return absl_ports::FailedPreconditionError(
        "Rewriting the committed blob is not allowed.");
  }
  std::string file_name = absl_ports::StrCat(
      base_dir_, "/", std::to_string(blob_info.creation_time_ms));
  int32_t file_descriptor = filesystem_->OpenForWrite(file_name.c_str());
  if (file_descriptor < 0) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Failed to open blob file for handle: ", blob_handle.label()));
  }
  return file_descriptor;
}

libtextclassifier3::StatusOr<int32_t> BlobStore::OpenRead(
    PropertyProto::BlobHandleProto blob_handle) {
  ICING_RETURN_IF_ERROR(ValidateBlobHandle(blob_handle));
  std::string blob_handle_str =
      encode_util::EncodeStringToCString(blob_handle.digest()) +
      blob_handle.label();
  ICING_ASSIGN_OR_RETURN(BlobInfo blob_info,
                         blob_info_mapper_->Get(blob_handle_str));
  if (!blob_info.is_committed) {
    return absl_ports::NotFoundError("Cannot find blob file to read.");
  }

  std::string file_name = absl_ports::StrCat(
      base_dir_, "/", std::to_string(blob_info.creation_time_ms));
  int32_t file_descriptor = filesystem_->OpenForRead(file_name.c_str());
  if (file_descriptor < 0) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Failed to open blob file for handle: ", blob_handle.label()));
  }
  return file_descriptor;
}

libtextclassifier3::Status BlobStore::CommitBlob(
    PropertyProto::BlobHandleProto blob_handle) {
  ICING_RETURN_IF_ERROR(ValidateBlobHandle(blob_handle));
  std::string blob_handle_str =
      encode_util::EncodeStringToCString(blob_handle.digest()) +
      blob_handle.label();
  ICING_ASSIGN_OR_RETURN(BlobInfo blob_info,
                         blob_info_mapper_->Get(blob_handle_str));
  if (blob_info.is_committed) {
    return absl_ports::AlreadyExistsError(absl_ports::StrCat(
        "The blob is already committed for handle: ", blob_handle.label()));
  }
  // Read the file and verify the digest.
  std::string file_name = absl_ports::StrCat(
      base_dir_, "/", std::to_string(blob_info.creation_time_ms));
  ScopedFd sfd(filesystem_->OpenForRead(file_name.c_str()));
  if (!sfd.is_valid()) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Failed to open blob file for handle: ", blob_handle.label()));
  }
  int64_t file_size = filesystem_->GetFileSize(sfd.get());
  int32_t offset = 0;
  // Read 8 KiB per iteration
  std::unique_ptr<uint8_t[]> buffer =
      std::make_unique<uint8_t[]>(kReadBufferSize);

  Sha256 sha256;
  while (offset < file_size &&
         filesystem_->PRead(sfd.get(), buffer.get(), kReadBufferSize, offset)) {
    sha256.Update(buffer.get(),
                  std::min<int32_t>(kReadBufferSize, file_size - offset));
    offset += kReadBufferSize;
  }
  std::array<uint8_t, 32> hash = std::move(sha256).Finalize();

  const std::string& digest = blob_handle.digest();
  if (digest.length() != hash.size() ||
      digest.compare(0, digest.length(),
                     reinterpret_cast<const char*>(hash.data()),
                     hash.size()) != 0) {
    return absl_ports::InvalidArgumentError(
        "The blob content doesn't match to the digest.");
  }

  // Mark the blob is committed
  blob_info.is_committed = true;
  has_mutated_ = true;
  ICING_RETURN_IF_ERROR(blob_info_mapper_->Put(blob_handle_str, blob_info));
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status BlobStore::PersistToDisk() {
  if (has_mutated_) {
    ICING_RETURN_IF_ERROR(blob_info_mapper_->PersistToDisk());
    has_mutated_ = false;
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<BlobStore::BlobInfo>
BlobStore::GetOrCreateBlobInfo(std::string blob_handle_str) {
  libtextclassifier3::StatusOr<BlobInfo> blob_info_or =
      blob_info_mapper_->Get(blob_handle_str);

  if (absl_ports::IsNotFound(blob_info_or.status())) {
    // Create a new blob info, we are using creation time as the unique file
    // name.
    int64_t timestamp = clock_.GetSystemTimeMilliseconds();
    std::string file_name = std::to_string(timestamp);
    while (known_file_names_.find(file_name) != known_file_names_.end()) {
      timestamp++;
      file_name = std::to_string(timestamp);
    }
    BlobInfo blob_info = {timestamp, /*is_committed=*/false};
    ICING_RETURN_IF_ERROR(blob_info_mapper_->Put(blob_handle_str, blob_info));
    has_mutated_ = true;
    return blob_info;
  }

  return blob_info_or;
}

}  // namespace lib
}  // namespace icing
