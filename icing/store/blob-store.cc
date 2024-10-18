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

#include <fcntl.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
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
static constexpr std::string_view kPackageNameFileName = "package_name_file";
static constexpr char kPackageNameDelimiter = '\0';

// - Key: sha 256 digest (32 bytes)
// - Value: BlobInfo (24 bytes)
// Allow max 1M of blob info entries.
static constexpr int32_t kBlobInfoMapperMaxSize = 56 * 1024 * 1024;  // 56 MiB
static constexpr int32_t kSha256LengthBytes = 32;
static constexpr int32_t kReadBufferSize = 8192;
static constexpr int32_t kPackageFileReadBufferSize = 128;

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

libtextclassifier3::StatusOr<std::unordered_map<std::string, int32_t>>
LoadPackageNameToOffsetMapper(const Filesystem& filesystem,
                              const std::string& base_dir,
                              const ScopedFd& package_name_fd) {
  // Open the package name file for reading.
  int64_t file_size = filesystem.GetFileSize(package_name_fd.get());
  if (file_size == Filesystem::kBadFileSize) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Bad file size for package name file"));
  }

  // Create a map to store the package names and their corresponding offsets.
  std::unordered_map<std::string, int32_t> package_name_to_offset;
  uint8_t buffer[kPackageFileReadBufferSize];

  int64_t bytes_fetched = 0;
  int32_t offset = 0;
  std::string package_name;
  while (bytes_fetched < file_size) {
    // Determine the size of the next chunk to read.
    int32_t size_to_read = std::min<int32_t>(kPackageFileReadBufferSize,
                                             file_size - bytes_fetched);

    if (!filesystem.Read(package_name_fd.get(), buffer, size_to_read)) {
      return absl_ports::InternalError(
          absl_ports::StrCat("Failed to read package name file"));
    }

    std::string_view buffer_view(reinterpret_cast<const char*>(buffer),
                                 size_to_read);
    size_t start_pos = 0;
    size_t end_pos = buffer_view.find('\0', start_pos);
    while (end_pos != std::string_view::npos) {
      package_name.append(buffer_view.substr(start_pos, end_pos - start_pos));
      int next_offset = offset + package_name.length() + 1;
      package_name_to_offset[std::move(package_name)] = offset;
      package_name = std::string();
      offset = next_offset;
      start_pos = end_pos + 1;
      end_pos = buffer_view.find('\0', start_pos);
    }

    bytes_fetched += size_to_read;

    // If there are remaining unprocessed bytes in the buffer, move them to the
    // start of the buffer.
    if (start_pos < size_to_read) {
      package_name.append(
          buffer_view.substr(start_pos, buffer_view.length() - start_pos));
    }
  }
  // We should have read all the bytes in the file since we always add '\0' at
  // the end of the package name.
  return package_name_to_offset;
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
  std::unordered_set<std::string> excludes = {
      std::string(kKeyMapperDir), std::string(kPackageNameFileName)};
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

  std::string file_name =
      absl_ports::StrCat(base_dir, "/", kPackageNameFileName);
  ScopedFd package_name_fd(filesystem->OpenForWrite(file_name.c_str()));
  if (!package_name_fd.is_valid()) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Failed to open package name file"));
  }

  auto package_name_to_offset_or =
      LoadPackageNameToOffsetMapper(*filesystem, base_dir, package_name_fd);
  if (!package_name_to_offset_or.ok()) {
    return package_name_to_offset_or.status();
  }

  return BlobStore(filesystem, std::move(base_dir), clock,
                   orphan_blob_time_to_live_ms, std::move(package_name_fd),
                   std::move(blob_info_mapper),
                   std::move(package_name_to_offset_or).ValueOrDie(),
                   std::move(known_file_names));
}

libtextclassifier3::StatusOr<int> BlobStore::OpenWrite(
    std::string_view package_name,
    const PropertyProto::BlobHandleProto& blob_handle) {
  ICING_RETURN_IF_ERROR(ValidateBlobHandle(blob_handle));
  std::string committed_blob_handle_str = encode_util::EncodeStringToCString(
      blob_handle.digest() + blob_handle.label());

  if (blob_info_mapper_->Get(committed_blob_handle_str).ok()) {
    return absl_ports::AlreadyExistsError(absl_ports::StrCat(
        "Rewriting the committed blob is not allowed for blob handle: ",
        blob_handle.label()));
  }

  // Create the pending blob handle string and get the blob info.
  std::string pending_blob_handle_str = encode_util::EncodeStringToCString(
      blob_handle.digest() + std::string(package_name) + kPackageNameDelimiter +
      blob_handle.label());
  auto itr = file_descriptors_for_write_.find(pending_blob_handle_str);
  if (itr != file_descriptors_for_write_.end()) {
    if (fcntl(itr->second, F_GETFD) != -1 || errno != EBADF) {
      // The file descriptor is still valid, return it.
      return itr->second;
    }
  }

  ICING_ASSIGN_OR_RETURN(
      BlobInfo blob_info,
      GetOrCreateBlobInfo(pending_blob_handle_str, package_name));

  std::string file_name = absl_ports::StrCat(
      base_dir_, "/", std::to_string(blob_info.creation_time_ms));
  int file_descriptor = filesystem_.OpenForWrite(file_name.c_str());
  if (file_descriptor < 0) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Failed to open blob file for handle: ", blob_handle.label()));
  }
  // Add the file descriptor for write to the file descriptors set under
  // pending_blob_handle_str.
  file_descriptors_for_write_[pending_blob_handle_str] = file_descriptor;
  return file_descriptor;
}

libtextclassifier3::StatusOr<int> BlobStore::OpenRead(
    const PropertyProto::BlobHandleProto& blob_handle) {
  ICING_RETURN_IF_ERROR(ValidateBlobHandle(blob_handle));
  std::string committed_blob_handle_str = encode_util::EncodeStringToCString(
      blob_handle.digest() + blob_handle.label());
  ICING_ASSIGN_OR_RETURN(BlobInfo blob_info,
                         blob_info_mapper_->Get(committed_blob_handle_str));

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
    std::string_view package_name,
    const PropertyProto::BlobHandleProto& blob_handle) {
  ICING_RETURN_IF_ERROR(ValidateBlobHandle(blob_handle));

  std::string pending_blob_handle_str = encode_util::EncodeStringToCString(
      blob_handle.digest() + std::string(package_name) + kPackageNameDelimiter +
      blob_handle.label());
  auto pending_blob_info_or = blob_info_mapper_->Get(pending_blob_handle_str);

  // Check if the blob is already committed.
  std::string committed_blob_handle_str = encode_util::EncodeStringToCString(
      blob_handle.digest() + blob_handle.label());
  if (blob_info_mapper_->Get(committed_blob_handle_str).ok()) {
    // The blob is already committed, delete the pending blob file and info.
    if (pending_blob_info_or.ok()) {
      blob_info_mapper_->Delete(pending_blob_handle_str);
      std::string file_name = absl_ports::StrCat(
          base_dir_, "/",
          std::to_string(pending_blob_info_or.ValueOrDie().creation_time_ms));
      if (!filesystem_.DeleteFile(file_name.c_str())) {
        ICING_LOG(ERROR) << "Failed to delete blob file: " << file_name;
      }
    }
    return absl_ports::AlreadyExistsError(absl_ports::StrCat(
        "The blob is already committed for handle: ", blob_handle.label()));
  }

  if (!pending_blob_info_or.ok()) {
    // Cannot find the pending blob to commit.
    return std::move(pending_blob_info_or).status();
  }

  // Read the file and verify the digest.
  BlobInfo blob_info = std::move(pending_blob_info_or).ValueOrDie();
  std::string file_name = absl_ports::StrCat(
      base_dir_, "/", std::to_string(blob_info.creation_time_ms));
  Sha256 sha256;
  {
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

    // Read 8 KiB per iteration
    int64_t prev_total_read_size = 0;
    uint8_t buffer[kReadBufferSize];
    while (prev_total_read_size < file_size) {
      int32_t size_to_read =
          std::min<int32_t>(kReadBufferSize, file_size - prev_total_read_size);
      if (!filesystem_.Read(sfd.get(), buffer, size_to_read)) {
        return absl_ports::InternalError(absl_ports::StrCat(
            "Failed to read blob file for handle: ", blob_handle.label()));
      }

      sha256.Update(buffer, size_to_read);
      prev_total_read_size += size_to_read;
    }
  }

  std::array<uint8_t, 32> hash = std::move(sha256).Finalize();

  const std::string& digest = blob_handle.digest();
  if (digest.length() != hash.size() ||
      digest.compare(0, digest.length(),
                     reinterpret_cast<const char*>(hash.data()),
                     hash.size()) != 0) {
    // The blob content doesn't match to the digest. Delete this corrupted blob.
    if (!filesystem_.DeleteFile(file_name.c_str())) {
      return absl_ports::InternalError(absl_ports::StrCat(
          "Failed to delete corrupted blob file for handle: ",
          blob_handle.label()));
    }
    if (!blob_info_mapper_->Delete(pending_blob_handle_str)) {
      return absl_ports::InternalError(absl_ports::StrCat(
          "Failed to delete corrupted blob info for handle: ",
          blob_handle.label()));
    }
    return absl_ports::InvalidArgumentError(
        "The blob content doesn't match to the digest.");
  }

  // Mark the blob is committed by removing package name from the blob handle
  // str.
  // Close sent file descriptor for write.
  close(file_descriptors_for_write_[pending_blob_handle_str]);
  file_descriptors_for_write_.erase(pending_blob_handle_str);
  has_mutated_ = true;
  ICING_RETURN_IF_ERROR(
      blob_info_mapper_->Put(committed_blob_handle_str, blob_info));
  blob_info_mapper_->Delete(pending_blob_handle_str);

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
BlobStore::GetOrCreateBlobInfo(const std::string& blob_handle_str,
                               std::string_view package_name) {
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

    ICING_ASSIGN_OR_RETURN(int32_t offset,
                           GetOrCreatePackageOffset(std::string(package_name)));
    BlobInfo blob_info(timestamp, offset);
    ICING_RETURN_IF_ERROR(blob_info_mapper_->Put(blob_handle_str, blob_info));
    has_mutated_ = true;
    return blob_info;
  }

  return blob_info_or;
}

libtextclassifier3::StatusOr<int32_t> BlobStore::GetOrCreatePackageOffset(
    const std::string& package_name) {
  auto itr = package_name_to_offset_.find(package_name);
  if (itr != package_name_to_offset_.end()) {
    return itr->second;
  }

  // This is the first time we see this package name, we need to write it to the
  // package name file.
  int64_t file_size = filesystem_.GetFileSize(package_name_fd_.get());
  if (file_size == Filesystem::kBadFileSize) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Failed to get file size for handle: ", package_name));
  }
  // we need to write package_name size +1 for the null terminator
  if (!filesystem_.PWrite(package_name_fd_.get(),
                          /*offset=*/file_size, package_name.c_str(),
                          package_name.size() + 1)) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Failed to write package name."));
  }
  package_name_to_offset_[package_name] = file_size;
  has_mutated_ = true;
  return file_size;
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

  // Get all existing package offsets.
  std::unordered_set<int32_t> existing_package_offsets;
  std::unique_ptr<typename KeyMapper<BlobInfo>::Iterator> itr =
      blob_info_mapper_->GetIterator();
  while (itr->Advance()) {
    if (dead_blob_handles.find(std::string(itr->GetKey())) ==
        dead_blob_handles.end()) {
      existing_package_offsets.insert(itr->GetValue().package_offset);
    } else {
      // Delete all dead blob files.
      std::string file_name = absl_ports::StrCat(
          base_dir_, "/", std::to_string(itr->GetValue().creation_time_ms));
      if (!filesystem_.DeleteFile(file_name.c_str())) {
        return absl_ports::InternalError(
            absl_ports::StrCat("Failed to delete blob file: ", file_name));
      }
    }
  }

  // Clean the package name file and get the new offset to old offset map.
  auto old_offset_to_new_offset_or =
      OptimizePackageNameFile(existing_package_offsets);
  if (!old_offset_to_new_offset_or.ok()) {
    return old_offset_to_new_offset_or.status();
  }
  std::unordered_map<int32_t, int32_t> old_offset_to_new_offset =
      std::move(old_offset_to_new_offset_or).ValueOrDie();

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
  itr = blob_info_mapper_->GetIterator();
  while (itr->Advance()) {
    if (dead_blob_handles.find(std::string(itr->GetKey())) ==
        dead_blob_handles.end()) {
      BlobInfo new_blob_info = itr->GetValue();
      new_blob_info.package_offset =
          old_offset_to_new_offset[new_blob_info.package_offset];
      ICING_RETURN_IF_ERROR(
          new_blob_info_mapper->Put(itr->GetKey(), new_blob_info));
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

// Clean the package name file by creating a new package name file with new
// offsets.
// Return a map from old offset to new offset.
libtextclassifier3::StatusOr<std::unordered_map<int32_t, int32_t>>
BlobStore::OptimizePackageNameFile(
    const std::unordered_set<int32_t>& existing_offsets) {
  // Delete the temp package name file if it exists.
  std::string package_name_temp_file =
      absl_ports::StrCat(base_dir_, "/", kPackageNameFileName, ".temp");
  if (!filesystem_.DeleteFile(package_name_temp_file.c_str())) {
    return absl_ports::InternalError(
        "Unable to delete temp package name file to prepare to build new blob "
        "store.");
  }

  // Write the package names to the temp file and build a map from old offset to
  // new offset.
  std::unordered_map<int32_t, int32_t> old_offset_to_new_offset;
  std::unordered_map<std::string, int32_t> new_package_name_to_offset;
  int32_t new_offset = 0;
  {
    ScopedFd sfd(filesystem_.OpenForWrite(package_name_temp_file.c_str()));
    if (!sfd.is_valid()) {
      return absl_ports::InternalError(
          absl_ports::StrCat("Failed to open package name temp file"));
    }
    for (const auto& [package_name, offset] : package_name_to_offset_) {
      if (existing_offsets.find(offset) == existing_offsets.end()) {
        continue;
      }
      // +1 for the null terminator
      size_t package_size = package_name.size() + 1;
      if (!filesystem_.Write(sfd.get(), package_name.c_str(), package_size)) {
        return absl_ports::InternalError(
            absl_ports::StrCat("Failed to write package name."));
      }
      old_offset_to_new_offset[offset] = new_offset;
      new_package_name_to_offset[package_name] = new_offset;
      new_offset += package_size;
    }
  }

  // Swap the temp file with the original file.
  std::string package_name_file =
      absl_ports::StrCat(base_dir_, "/", kPackageNameFileName);
  if (!filesystem_.SwapFiles(package_name_file.c_str(),
                             package_name_temp_file.c_str())) {
    return absl_ports::InternalError(
        "Unable to apply new package name file due to failed swap!");
  }
  // Update the package name to offset map.
  package_name_to_offset_ = std::move(new_package_name_to_offset);
  package_name_fd_.reset(filesystem_.OpenForWrite(package_name_file.c_str()));
  if (!package_name_fd_.is_valid()) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Failed to open package name file"));
  }

  return old_offset_to_new_offset;
}

libtextclassifier3::StatusOr<std::vector<PackageBlobStorageInfoProto>>
BlobStore::GetStorageInfo() const {
  // Get the file size of each package offset.
  std::unordered_map<int32_t, PackageBlobStorageInfoProto>
      package_offset_to_storage_info;
  std::unique_ptr<typename KeyMapper<BlobInfo>::Iterator> itr =
      blob_info_mapper_->GetIterator();
  while (itr->Advance()) {
    BlobInfo blob_info = itr->GetValue();
    std::string file_name = absl_ports::StrCat(
        base_dir_, "/", std::to_string(blob_info.creation_time_ms));
    int64_t file_size = filesystem_.GetFileSize(file_name.c_str());
    if (file_size == Filesystem::kBadFileSize) {
      ICING_LOG(WARNING) << "Bad file size for blob file: " << file_name;
      continue;
    }
    PackageBlobStorageInfoProto package_blob_storage_info =
        package_offset_to_storage_info[blob_info.package_offset];
    package_blob_storage_info.set_blob_size(
        package_blob_storage_info.blob_size() + file_size);
    package_blob_storage_info.set_num_blobs(
        package_blob_storage_info.num_blobs() + 1);
    package_offset_to_storage_info[blob_info.package_offset] =
        package_blob_storage_info;
  }

  // Create the package blob storage info for each package.
  std::vector<PackageBlobStorageInfoProto> package_blob_storage_infos;
  for (const auto& [package_name, offset] : package_name_to_offset_) {
    auto itr = package_offset_to_storage_info.find(offset);
    if (itr == package_offset_to_storage_info.end()) {
      // Existing package name, but there is no blob file under it.
      // So simply skip it.
      continue;
    }
    itr->second.set_package_name(package_name);
    package_blob_storage_infos.push_back(std::move(itr->second));
  }
  return package_blob_storage_infos;
}

}  // namespace lib
}  // namespace icing
