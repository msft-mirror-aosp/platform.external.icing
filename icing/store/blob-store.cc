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
#include <limits>
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
#include "icing/file/destructible-directory.h"
#include "icing/file/filesystem.h"
#include "icing/proto/document.pb.h"
#include "icing/store/dynamic-trie-key-mapper.h"
#include "icing/store/key-mapper.h"
#include "icing/util/clock.h"
#include "icing/util/encode-util.h"
#include "icing/util/logging.h"
#include "icing/util/sha256.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

static constexpr std::string_view kKeyMapperDir = "key_mapper";
// - Key: sha 256 digest (32 bytes)
// - Value: BlobInfo (24 bytes)
// Allow max 1M of blob info entries.
static constexpr int32_t kBlobInfoMapperMaxSize = 56 * 1024 * 1024;  // 56 MiB
static constexpr int32_t kSha256LengthBytes = 32;
static constexpr int32_t kReadBufferSize = 8192;

std::string MakeKeyMapperDir(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kKeyMapperDir);
}

namespace {

libtextclassifier3::Status ValidateBlobHandle(
    const PropertyProto::BlobHandleProto& blob_handle) {
  if (blob_handle.digest().size() != kSha256LengthBytes) {
    return absl_ports::InvalidArgumentError(
        "Invalid blob handle. The digest is not sha 256 digest.");
  }
  return libtextclassifier3::Status::OK;
}

}  // namespace

libtextclassifier3::StatusOr<BlobStore> BlobStore::Create(
    const Filesystem* filesystem, std::string base_dir, const Clock* clock,
    int64_t orphan_blob_time_to_live_ms) {
  ICING_RETURN_ERROR_IF_NULL(filesystem);
  ICING_RETURN_ERROR_IF_NULL(clock);
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<KeyMapper<BlobInfo>> blob_info_mapper,
      DynamicTrieKeyMapper<BlobInfo>::Create(
          *filesystem, MakeKeyMapperDir(base_dir), kBlobInfoMapperMaxSize));

  // Load existing file names (excluding the directory of key mapper).
  std::vector<std::string> file_names;
  std::unordered_set<std::string> excludes = {kKeyMapperDir.data()};
  if (!filesystem->ListDirectory(base_dir.c_str(), excludes,
                                 /*recursive=*/false, &file_names)) {
    return absl_ports::InternalError("Failed to list directory.");
  }
  std::unordered_set<std::string> known_file_names(
      std::make_move_iterator(file_names.begin()),
      std::make_move_iterator(file_names.end()));
  if (orphan_blob_time_to_live_ms <= 0) {
    orphan_blob_time_to_live_ms = std::numeric_limits<int64_t>::max();
  }
  return BlobStore(filesystem, std::move(base_dir), clock,
                   orphan_blob_time_to_live_ms, std::move(blob_info_mapper),
                   std::move(known_file_names));
}

libtextclassifier3::StatusOr<int> BlobStore::OpenWrite(
    const PropertyProto::BlobHandleProto& blob_handle) {
  ICING_RETURN_IF_ERROR(ValidateBlobHandle(blob_handle));
  std::string blob_handle_str =
      encode_util::EncodeStringToCString(blob_handle.digest()) +
      blob_handle.label();
  ICING_ASSIGN_OR_RETURN(BlobInfo blob_info,
                         GetOrCreateBlobInfo(blob_handle_str));
  if (blob_info.is_committed) {
    return absl_ports::AlreadyExistsError(
        "Rewriting the committed blob is not allowed.");
  }
  std::string file_name = absl_ports::StrCat(
      base_dir_, "/", std::to_string(blob_info.creation_time_ms));
  int file_descriptor = filesystem_.OpenForWrite(file_name.c_str());
  if (file_descriptor < 0) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Failed to open blob file for handle: ", blob_handle.label()));
  }
  return file_descriptor;
}

libtextclassifier3::StatusOr<int> BlobStore::OpenRead(
    const PropertyProto::BlobHandleProto& blob_handle) {
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
  int file_descriptor = filesystem_.OpenForRead(file_name.c_str());
  if (file_descriptor < 0) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Failed to open blob file for handle: ", blob_handle.label()));
  }
  return file_descriptor;
}

libtextclassifier3::Status BlobStore::CommitBlob(
    const PropertyProto::BlobHandleProto& blob_handle) {
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
  ScopedFd sfd(filesystem_.OpenForRead(file_name.c_str()));
  if (!sfd.is_valid()) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Failed to open blob file for handle: ", blob_handle.label()));
  }

  int64_t file_size = filesystem_.GetFileSize(sfd.get());
  if (file_size == Filesystem::kBadFileSize) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Failed to get file size for handle: ", blob_handle.label()));
  }

  Sha256 sha256;
  // Read 8 KiB per iteration
  int64_t prev_total_read_size = 0;
  auto buffer = std::make_unique<uint8_t[]>(kReadBufferSize);
  while (prev_total_read_size < file_size) {
    int32_t size_to_read =
        std::min<int32_t>(kReadBufferSize, file_size - prev_total_read_size);
    if (!filesystem_.Read(sfd.get(), buffer.get(), size_to_read)) {
      return absl_ports::InternalError(absl_ports::StrCat(
          "Failed to read blob file for handle: ", blob_handle.label()));
    }

    sha256.Update(buffer.get(), size_to_read);
    prev_total_read_size += size_to_read;
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
BlobStore::GetOrCreateBlobInfo(const std::string& blob_handle_str) {
  libtextclassifier3::StatusOr<BlobInfo> blob_info_or =
      blob_info_mapper_->Get(blob_handle_str);

  if (absl_ports::IsNotFound(blob_info_or.status())) {
    // Create a new blob info, we are using creation time as the unique file
    // name.
    int64_t timestamp = clock_.GetSystemTimeMilliseconds();
    std::string file_name = std::to_string(timestamp);
    while (known_file_names_.find(file_name) != known_file_names_.end()) {
      ++timestamp;
      file_name = std::to_string(timestamp);
    }
    known_file_names_.insert(file_name);

    BlobInfo blob_info = {timestamp, /*is_committed=*/false};
    ICING_RETURN_IF_ERROR(blob_info_mapper_->Put(blob_handle_str, blob_info));
    has_mutated_ = true;
    return blob_info;
  }

  return blob_info_or;
}

std::unordered_set<std::string>
BlobStore::GetPotentiallyOptimizableBlobHandles() {
  int64_t current_time_ms = clock_.GetSystemTimeMilliseconds();
  if (orphan_blob_time_to_live_ms_ > current_time_ms) {
    // Nothing to optimize, return empty set.
    return std::unordered_set<std::string>();
  }
  int64_t expired_threshold =
      clock_.GetSystemTimeMilliseconds() - orphan_blob_time_to_live_ms_;
  std::unique_ptr<typename KeyMapper<BlobInfo>::Iterator> itr =
      blob_info_mapper_->GetIterator();

  std::unordered_set<std::string> expired_blob_handles;
  while (itr->Advance()) {
    if (itr->GetValue().creation_time_ms < expired_threshold) {
      expired_blob_handles.insert(std::string(itr->GetKey()));
    }
  }
  return expired_blob_handles;
}

libtextclassifier3::Status BlobStore::Optimize(
    const std::unordered_set<std::string>& dead_blob_handles) {
  if (dead_blob_handles.empty()) {
    // nothing to optimize, return early.
    return libtextclassifier3::Status::OK;
  }

  // Create the temp blob store directory.
  std::string temp_blob_store_dir_path = base_dir_ + "_temp";
  if (!filesystem_.DeleteDirectoryRecursively(
          temp_blob_store_dir_path.c_str())) {
    ICING_LOG(ERROR) << "Recursively deleting "
                     << temp_blob_store_dir_path.c_str();
    return absl_ports::InternalError(
        "Unable to delete temp directory to prepare to build new blob store.");
  }
  DestructibleDirectory temp_blob_store_dir(&filesystem_,
                                            temp_blob_store_dir_path);
  if (!temp_blob_store_dir.is_valid()) {
    return absl_ports::InternalError(
        "Unable to create temp directory to build new blob store.");
  }

  // Destroy the old blob info mapper and replace it with the new one.
  std::string new_key_mapper_dir = MakeKeyMapperDir(temp_blob_store_dir_path);
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<KeyMapper<BlobInfo>> new_blob_info_mapper,
      DynamicTrieKeyMapper<BlobInfo>::Create(filesystem_, new_key_mapper_dir,
                                             kBlobInfoMapperMaxSize));
  std::unique_ptr<typename KeyMapper<BlobInfo>::Iterator> itr =
      blob_info_mapper_->GetIterator();
  while (itr->Advance()) {
    if (dead_blob_handles.find(std::string(itr->GetKey())) ==
        dead_blob_handles.end()) {
      ICING_RETURN_IF_ERROR(
          new_blob_info_mapper->Put(itr->GetKey(), itr->GetValue()));
    } else {
      // Delete the file of dead blobs.
      std::string file_name = absl_ports::StrCat(
          base_dir_, "/", std::to_string(itr->GetValue().creation_time_ms));
      if (!filesystem_.DeleteFile(file_name.c_str())) {
        return absl_ports::InternalError(
            absl_ports::StrCat("Failed to delete blob file: ", file_name));
      }
    }
  }
  new_blob_info_mapper.reset();
  // Then we swap the new key mapper directory with the old one.
  if (!filesystem_.SwapFiles(MakeKeyMapperDir(base_dir_).c_str(),
                             new_key_mapper_dir.c_str())) {
    return absl_ports::InternalError(
        "Unable to apply new blob store due to failed swap!");
  }
  ICING_ASSIGN_OR_RETURN(
      blob_info_mapper_,
      DynamicTrieKeyMapper<BlobInfo>::Create(
          filesystem_, MakeKeyMapperDir(base_dir_), kBlobInfoMapperMaxSize));

  return libtextclassifier3::Status::OK;
}

}  // namespace lib
}  // namespace icing
