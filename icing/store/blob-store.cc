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
#include <cstdint>
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
#include "icing/file/constants.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/proto/blob.pb.h"
#include "icing/proto/document.pb.h"
#include "icing/util/clock.h"
#include "icing/util/encode-util.h"
#include "icing/util/logging.h"
#include "icing/util/sha256.h"
#include "icing/util/status-macros.h"
#include "icing/util/status-util.h"

namespace icing {
namespace lib {

static constexpr std::string_view kBlobFileDir = "blob_files";
static constexpr std::string_view kBlobInfoProtoLogFileName =
    "blob_info_proto_file";
static constexpr int32_t kSha256LengthBytes = 32;
static constexpr int32_t kReadBufferSize = 8192;

namespace {

using ::icing::lib::status_util::TransformStatus;

std::string MakeBlobInfoProtoLogFileName(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kBlobInfoProtoLogFileName);
}

std::string MakeBlobFileDir(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kBlobFileDir);
}

std::string MakeBlobFilePath(const std::string& base_dir,
                             int64_t creation_time_ms) {
  return absl_ports::StrCat(MakeBlobFileDir(base_dir), "/",
                            std::to_string(creation_time_ms));
}

libtextclassifier3::Status ValidateBlobHandle(
    const PropertyProto::BlobHandleProto& blob_handle) {
  if (blob_handle.digest().size() != kSha256LengthBytes) {
    return absl_ports::InvalidArgumentError(
        "Invalid blob handle. The digest is not sha 256 digest.");
  }
  if (blob_handle.namespace_().empty()) {
    return absl_ports::InvalidArgumentError(
        "Invalid blob handle. The namespace is empty.");
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<std::unordered_map<std::string, int32_t>>
LoadBlobHandleToOffsetMapper(
    PortableFileBackedProtoLog<BlobInfoProto>* blob_info_log) {
  std::unordered_map<std::string, int32_t> blob_handle_to_offset;
  auto itr = blob_info_log->GetIterator();
  while (itr.Advance().ok()) {
    auto blob_info_proto_or = blob_info_log->ReadProto(itr.GetOffset());
    if (!blob_info_proto_or.ok()) {
      if (absl_ports::IsNotFound(blob_info_proto_or.status())) {
        // Skip erased proto.
        continue;
      }

      // Return real error.
      return std::move(blob_info_proto_or).status();
    }
    BlobInfoProto blob_info_proto = std::move(blob_info_proto_or).ValueOrDie();

    std::string blob_handle_str =
        BlobStore::BuildBlobHandleStr(blob_info_proto.blob_handle());
    blob_handle_to_offset.insert({std::move(blob_handle_str), itr.GetOffset()});
  }
  return blob_handle_to_offset;
}

BlobProto CreateBlobProtoFromError(const libtextclassifier3::Status& status) {
  BlobProto blob_proto;
  TransformStatus(status, blob_proto.mutable_status());
  return blob_proto;
}

BlobProto CreateBlobProtoFromFilename(std::string filename) {
  BlobProto blob_proto;
  blob_proto.mutable_status()->set_code(StatusProto::OK);
  blob_proto.set_file_name(std::move(filename));
  return blob_proto;
}

BlobProto CreateBlobProtoFromFileDescriptor(int file_descriptor) {
  BlobProto blob_proto;
  blob_proto.mutable_status()->set_code(StatusProto::OK);
  blob_proto.set_file_descriptor(file_descriptor);
  return blob_proto;
}

}  // namespace

/* static */ std::string BlobStore::BuildBlobHandleStr(
    const PropertyProto::BlobHandleProto& blob_handle) {
  return encode_util::EncodeStringToCString(blob_handle.digest() +
                                            blob_handle.namespace_());
}

libtextclassifier3::StatusOr<BlobStore> BlobStore::Create(
    const Filesystem* filesystem, std::string base_dir, const Clock* clock,
    int64_t orphan_blob_time_to_live_ms, int32_t compression_level,
    bool manage_blob_files) {
  ICING_RETURN_ERROR_IF_NULL(filesystem);
  ICING_RETURN_ERROR_IF_NULL(clock);

  // Make sure the blob file directory exists.
  if (!filesystem->CreateDirectoryRecursively(
          MakeBlobFileDir(base_dir).c_str())) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Could not create blob file directory."));
  }

  // Load existing file names (excluding the directory of key mapper).
  std::vector<std::string> file_names;
  if (!filesystem->ListDirectory(MakeBlobFileDir(base_dir).c_str(),
                                 &file_names)) {
    return absl_ports::InternalError("Failed to list directory.");
  }
  std::unordered_set<std::string> known_file_names(
      std::make_move_iterator(file_names.begin()),
      std::make_move_iterator(file_names.end()));
  if (orphan_blob_time_to_live_ms <= 0) {
    orphan_blob_time_to_live_ms = std::numeric_limits<int64_t>::max();
  }

  std::string blob_info_proto_file_name =
      MakeBlobInfoProtoLogFileName(base_dir);

  ICING_ASSIGN_OR_RETURN(
      PortableFileBackedProtoLog<BlobInfoProto>::CreateResult log_create_result,
      PortableFileBackedProtoLog<BlobInfoProto>::Create(
          filesystem, blob_info_proto_file_name,
          PortableFileBackedProtoLog<BlobInfoProto>::Options(
              /*compress_in=*/true, constants::kMaxProtoSize, compression_level,
              /*compression_threshold_bytes=*/0)));

  std::unordered_map<std::string, int> blob_handle_to_offset;
  ICING_ASSIGN_OR_RETURN(
      blob_handle_to_offset,
      LoadBlobHandleToOffsetMapper(log_create_result.proto_log.get()));

  return BlobStore(filesystem, std::move(base_dir), clock,
                   orphan_blob_time_to_live_ms, compression_level,
                   manage_blob_files, std::move(log_create_result.proto_log),
                   std::move(blob_handle_to_offset),
                   std::move(known_file_names));
}

BlobProto BlobStore::OpenWrite(
    const PropertyProto::BlobHandleProto& blob_handle) {
  ICING_RETURN_EXPRESSION_IF_ERROR(ValidateBlobHandle(blob_handle),
                                   CreateBlobProtoFromError(_));
  std::string blob_handle_str = BuildBlobHandleStr(blob_handle);

  auto blob_info_itr = blob_handle_to_offset_.find(blob_handle_str);
  if (blob_info_itr != blob_handle_to_offset_.end()) {
    ICING_ASSIGN_OR_RETURN(BlobInfoProto blob_info,
                           blob_info_log_->ReadProto(blob_info_itr->second),
                           CreateBlobProtoFromError(_));
    if (blob_info.is_committed()) {
      // The blob is already committed, return error.
      return CreateBlobProtoFromError(
          absl_ports::AlreadyExistsError(absl_ports::StrCat(
              "Rewriting the committed blob is not allowed for blob handle: ",
              blob_handle.digest())));
    }
  }

  // Create a new blob info and blob file.
  ICING_ASSIGN_OR_RETURN(BlobInfoProto blob_info,
                         GetOrCreateBlobInfo(blob_handle_str, blob_handle),
                         CreateBlobProtoFromError(_));

  if (!manage_blob_files_) {
    return CreateBlobProtoFromFilename(
        std::to_string(blob_info.creation_time_ms()));
  }

  std::string file_path =
      MakeBlobFilePath(base_dir_, blob_info.creation_time_ms());
  int file_descriptor = filesystem_.OpenForWrite(file_path.c_str());
  if (file_descriptor < 0) {
    return CreateBlobProtoFromError(
        absl_ports::InternalError(absl_ports::StrCat(
            "Failed to open blob file for handle: ", blob_handle.digest())));
  }
  return CreateBlobProtoFromFileDescriptor(file_descriptor);
}

BlobProto BlobStore::RemoveBlob(
    const PropertyProto::BlobHandleProto& blob_handle) {
  ICING_RETURN_EXPRESSION_IF_ERROR(ValidateBlobHandle(blob_handle),
                                   CreateBlobProtoFromError(_));
  std::string blob_handle_str = BuildBlobHandleStr(blob_handle);

  auto blob_info_itr = blob_handle_to_offset_.find(blob_handle_str);
  if (blob_info_itr == blob_handle_to_offset_.end()) {
    return CreateBlobProtoFromError(
        absl_ports::NotFoundError(absl_ports::StrCat(
            "Cannot find the blob for handle: ", blob_handle.digest())));
  }

  int64_t blob_info_offset = blob_info_itr->second;
  ICING_ASSIGN_OR_RETURN(BlobInfoProto blob_info,
                         blob_info_log_->ReadProto(blob_info_offset),
                         CreateBlobProtoFromError(_));

  ICING_RETURN_EXPRESSION_IF_ERROR(blob_info_log_->EraseProto(blob_info_offset),
                                   CreateBlobProtoFromError(_));
  blob_handle_to_offset_.erase(blob_info_itr);
  has_mutated_ = true;

  if (!manage_blob_files_) {
    return CreateBlobProtoFromFilename(
        std::to_string(blob_info.creation_time_ms()));
  }

  std::string file_path =
      MakeBlobFilePath(base_dir_, blob_info.creation_time_ms());
  if (!filesystem_.DeleteFile(file_path.c_str())) {
    return CreateBlobProtoFromError(
        absl_ports::InternalError(absl_ports::StrCat(
            "Failed to abandon blob file for handle: ", blob_handle.digest())));
  }

  BlobProto blob_proto;
  blob_proto.mutable_status()->set_code(StatusProto::OK);
  return blob_proto;
}

libtextclassifier3::StatusOr<BlobInfoProto> BlobStore::GetBlobInfo(
    const PropertyProto::BlobHandleProto& blob_handle) const {
  ICING_RETURN_IF_ERROR(ValidateBlobHandle(blob_handle));
  std::string blob_handle_str = BuildBlobHandleStr(blob_handle);
  auto itr = blob_handle_to_offset_.find(blob_handle_str);
  if (itr == blob_handle_to_offset_.end()) {
    return absl_ports::NotFoundError(absl_ports::StrCat(
        "Cannot find the blob for handle: ", blob_handle.digest()));
  }
  return blob_info_log_->ReadProto(itr->second);
}

BlobProto BlobStore::OpenRead(
    const PropertyProto::BlobHandleProto& blob_handle) const {
  ICING_ASSIGN_OR_RETURN(BlobInfoProto blob_info, GetBlobInfo(blob_handle),
                         CreateBlobProtoFromError(_));
  if (!blob_info.is_committed()) {
    // The blob is not committed, return error.
    return CreateBlobProtoFromError(
        absl_ports::NotFoundError(absl_ports::StrCat(
            "Cannot find the blob for handle: ", blob_handle.digest())));
  }

  if (!manage_blob_files_) {
    return CreateBlobProtoFromFilename(
        std::to_string(blob_info.creation_time_ms()));
  }

  std::string file_path =
      MakeBlobFilePath(base_dir_, blob_info.creation_time_ms());
  int file_descriptor = filesystem_.OpenForRead(file_path.c_str());
  if (file_descriptor < 0) {
    return CreateBlobProtoFromError(
        absl_ports::InternalError(absl_ports::StrCat(
            "Failed to open blob file for handle: ", blob_handle.digest())));
  }
  return CreateBlobProtoFromFileDescriptor(file_descriptor);
}

libtextclassifier3::Status BlobStore::CommitBlobMetadata(
    const PropertyProto::BlobHandleProto& blob_handle) {
  ICING_RETURN_IF_ERROR(ValidateBlobHandle(blob_handle));

  std::string blob_handle_str = BuildBlobHandleStr(blob_handle);

  auto pending_blob_info_itr = blob_handle_to_offset_.find(blob_handle_str);
  if (pending_blob_info_itr == blob_handle_to_offset_.end()) {
    return absl_ports::NotFoundError(absl_ports::StrCat(
        "Cannot find the blob for handle: ", blob_handle.digest()));
  }
  int64_t pending_blob_info_offset = pending_blob_info_itr->second;

  ICING_ASSIGN_OR_RETURN(BlobInfoProto blob_info_proto,
                         blob_info_log_->ReadProto(pending_blob_info_offset));

  // Check if the blob is already committed.
  if (blob_info_proto.is_committed()) {
    return absl_ports::AlreadyExistsError(absl_ports::StrCat(
        "The blob is already committed for handle: ", blob_handle.digest()));
  }

  // Update the blob info proto to committed.
  ICING_RETURN_IF_ERROR(blob_info_log_->EraseProto(pending_blob_info_offset));
  has_mutated_ = true;
  blob_info_proto.set_is_committed(true);
  auto blob_info_offset_or = blob_info_log_->WriteProto(blob_info_proto);
  if (!blob_info_offset_or.ok()) {
    ICING_LOG(ERROR) << blob_info_offset_or.status().error_message()
                     << "Failed to write blob info";
    return blob_info_offset_or.status();
  }
  blob_handle_to_offset_[blob_handle_str] = blob_info_offset_or.ValueOrDie();
  return libtextclassifier3::Status::OK;
}

BlobProto BlobStore::CommitBlob(
    const PropertyProto::BlobHandleProto& blob_handle) {
  BlobProto blob_proto;
  blob_proto.mutable_status()->set_code(StatusProto::OK);

  if (!manage_blob_files_) {
    ICING_RETURN_EXPRESSION_IF_ERROR(CommitBlobMetadata(blob_handle),
                                     CreateBlobProtoFromError(_))
    return blob_proto;
  }

  ICING_ASSIGN_OR_RETURN(BlobInfoProto blob_info, GetBlobInfo(blob_handle),
                         CreateBlobProtoFromError(_));
  std::string file_path =
      MakeBlobFilePath(base_dir_, blob_info.creation_time_ms());
  // Read the file and verify the digest.
  Sha256 sha256;
  {
    ScopedFd sfd(filesystem_.OpenForRead(file_path.c_str()));
    if (!sfd.is_valid()) {
      return CreateBlobProtoFromError(
          absl_ports::InternalError(absl_ports::StrCat(
              "Failed to open blob file for handle: ", blob_handle.digest())));
    }

    int64_t file_size = filesystem_.GetFileSize(sfd.get());
    if (file_size == Filesystem::kBadFileSize) {
      return CreateBlobProtoFromError(
          absl_ports::InternalError(absl_ports::StrCat(
              "Failed to get file size for handle: ", blob_handle.digest())));
    }

    // Read 8 KiB per iteration
    int64_t prev_total_read_size = 0;
    uint8_t buffer[kReadBufferSize];
    while (prev_total_read_size < file_size) {
      int32_t size_to_read =
          std::min<int32_t>(kReadBufferSize, file_size - prev_total_read_size);
      if (!filesystem_.Read(sfd.get(), buffer, size_to_read)) {
        return CreateBlobProtoFromError(absl_ports::InternalError(
            absl_ports::StrCat("Failed to read blob file for handle: ",
                               blob_handle.digest())));
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
    BlobProto remove_blob_result = RemoveBlob(blob_handle);
    if (remove_blob_result.status().code() != StatusProto::OK) {
      return remove_blob_result;
    }
    return CreateBlobProtoFromError(absl_ports::InvalidArgumentError(
        "The blob content doesn't match to the digest."));
  }
  // Mark the blob as committed.
  ICING_RETURN_EXPRESSION_IF_ERROR(CommitBlobMetadata(blob_handle),
                                   CreateBlobProtoFromError(_));
  return blob_proto;
}

libtextclassifier3::Status BlobStore::PersistToDisk() {
  if (has_mutated_) {
    ICING_RETURN_IF_ERROR(blob_info_log_->PersistToDisk());
    has_mutated_ = false;
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<BlobInfoProto> BlobStore::GetOrCreateBlobInfo(
    const std::string& blob_handle_str,
    const PropertyProto::BlobHandleProto& blob_handle) {
  auto itr = blob_handle_to_offset_.find(blob_handle_str);
  if (itr != blob_handle_to_offset_.end()) {
    return blob_info_log_->ReadProto(itr->second);
  }

  // Create a new blob info, we are using creation time as the unique file
  // name.
  int64_t timestamp = clock_.GetSystemTimeMilliseconds();
  std::string file_name = std::to_string(timestamp);
  while (known_file_names_.find(file_name) != known_file_names_.end()) {
    ++timestamp;
    file_name = std::to_string(timestamp);
  }
  known_file_names_.insert(file_name);

  BlobInfoProto blob_info_proto;
  *blob_info_proto.mutable_blob_handle() = blob_handle;
  blob_info_proto.set_creation_time_ms(timestamp);
  blob_info_proto.set_is_committed(false);

  auto blob_info_offset_or = blob_info_log_->WriteProto(blob_info_proto);
  if (!blob_info_offset_or.ok()) {
    ICING_LOG(ERROR) << blob_info_offset_or.status().error_message()
                     << "Failed to write blob info";
    return blob_info_offset_or.status();
  }

  has_mutated_ = true;
  blob_handle_to_offset_[blob_handle_str] = blob_info_offset_or.ValueOrDie();

  return blob_info_proto;
}

std::unordered_set<std::string>
BlobStore::GetPotentiallyOptimizableBlobHandles() const {
  int64_t current_time_ms = clock_.GetSystemTimeMilliseconds();
  if (orphan_blob_time_to_live_ms_ > current_time_ms) {
    // Nothing to optimize, return empty set.
    return std::unordered_set<std::string>();
  }
  int64_t expired_threshold = current_time_ms - orphan_blob_time_to_live_ms_;
  std::unordered_set<std::string> expired_blob_handles;
  auto itr = blob_info_log_->GetIterator();
  while (itr.Advance().ok()) {
    auto blob_info_proto_or = blob_info_log_->ReadProto(itr.GetOffset());
    if (!blob_info_proto_or.ok()) {
      continue;
    }
    BlobInfoProto blob_info_proto = std::move(blob_info_proto_or).ValueOrDie();
    if (blob_info_proto.creation_time_ms() < expired_threshold) {
      expired_blob_handles.insert(
          BuildBlobHandleStr(blob_info_proto.blob_handle()));
    }
  }
  return expired_blob_handles;
}

libtextclassifier3::StatusOr<std::vector<std::string>> BlobStore::Optimize(
    const std::unordered_set<std::string>& dead_blob_handles) {
  std::vector<std::string> blob_file_names_to_remove;
  blob_file_names_to_remove.reserve(dead_blob_handles.size());

  // Create the temp blob info log file.
  std::string temp_blob_info_proto_file_name =
      absl_ports::StrCat(MakeBlobInfoProtoLogFileName(base_dir_), "_temp");
  if (!filesystem_.DeleteFile(temp_blob_info_proto_file_name.c_str())) {
    return absl_ports::InternalError(
        "Unable to delete temp file to prepare to build new blob proto file.");
  }

  ICING_ASSIGN_OR_RETURN(
      PortableFileBackedProtoLog<BlobInfoProto>::CreateResult
          temp_log_create_result,
      PortableFileBackedProtoLog<BlobInfoProto>::Create(
          &filesystem_, temp_blob_info_proto_file_name,
          PortableFileBackedProtoLog<BlobInfoProto>::Options(
              /*compress_in=*/true, constants::kMaxProtoSize,
              compression_level_, /*compression_threshold_bytes=*/0)));
  std::unique_ptr<PortableFileBackedProtoLog<BlobInfoProto>> new_blob_info_log =
      std::move(temp_log_create_result.proto_log);

  auto itr = blob_info_log_->GetIterator();
  std::unordered_map<std::string, int32_t> new_blob_handle_to_offset;
  while (itr.Advance().ok()) {
    auto blob_info_proto_or = blob_info_log_->ReadProto(itr.GetOffset());
    if (!blob_info_proto_or.ok()) {
      if (absl_ports::IsNotFound(blob_info_proto_or.status())) {
        // Skip erased proto.
        continue;
      }

      // Return real error.
      return std::move(blob_info_proto_or).status();
    }
    BlobInfoProto blob_info_proto = std::move(blob_info_proto_or).ValueOrDie();
    std::string blob_handle_str =
        BuildBlobHandleStr(blob_info_proto.blob_handle());
    if (dead_blob_handles.find(blob_handle_str) != dead_blob_handles.end()) {
      // Delete all dead blob files.

      if (manage_blob_files_) {
        std::string file_path =
            MakeBlobFilePath(base_dir_, blob_info_proto.creation_time_ms());
        if (!filesystem_.DeleteFile(file_path.c_str())) {
          return absl_ports::InternalError(
              absl_ports::StrCat("Failed to delete blob file: ", file_path));
        }
      } else {
        blob_file_names_to_remove.push_back(
            std::to_string(blob_info_proto.creation_time_ms()));
      }
    } else {
      // Write the alive blob info to the new blob info log file.
      ICING_ASSIGN_OR_RETURN(int32_t new_offset,
                             new_blob_info_log->WriteProto(blob_info_proto));
      new_blob_handle_to_offset[blob_handle_str] = new_offset;
    }
  }
  new_blob_info_log->PersistToDisk();
  new_blob_info_log.reset();
  blob_info_log_.reset();
  std::string old_blob_info_proto_file_name =
      MakeBlobInfoProtoLogFileName(base_dir_);
  // Then we swap the new key mapper directory with the old one.
  if (!filesystem_.SwapFiles(old_blob_info_proto_file_name.c_str(),
                             temp_blob_info_proto_file_name.c_str())) {
    return absl_ports::InternalError(
        "Unable to apply new blob store due to failed swap!");
  }

  // Delete the temp file, don't need to throw error if it fails, it will be
  // deleted in the next run.
  filesystem_.DeleteFile(temp_blob_info_proto_file_name.c_str());

  ICING_ASSIGN_OR_RETURN(
      PortableFileBackedProtoLog<BlobInfoProto>::CreateResult log_create_result,
      PortableFileBackedProtoLog<BlobInfoProto>::Create(
          &filesystem_, old_blob_info_proto_file_name,
          PortableFileBackedProtoLog<BlobInfoProto>::Options(
              /*compress_in=*/true, constants::kMaxProtoSize,
              compression_level_, /*compression_threshold_bytes=*/0)));
  blob_info_log_ = std::move(log_create_result.proto_log);
  blob_handle_to_offset_ = std::move(new_blob_handle_to_offset);
  return blob_file_names_to_remove;
}

libtextclassifier3::StatusOr<std::vector<NamespaceBlobStorageInfoProto>>
BlobStore::GetStorageInfo() const {
  // Get the file size of each namespace offset.
  std::unordered_map<std::string, NamespaceBlobStorageInfoProto>
      namespace_to_storage_info;
  auto itr = blob_info_log_->GetIterator();
  while (itr.Advance().ok()) {
    auto blob_info_proto_or = blob_info_log_->ReadProto(itr.GetOffset());
    if (!blob_info_proto_or.ok()) {
      if (absl_ports::IsNotFound(blob_info_proto_or.status())) {
        // Skip erased proto.
        continue;
      }

      // Return real error.
      return std::move(blob_info_proto_or).status();
    }
    BlobInfoProto blob_info_proto = std::move(blob_info_proto_or).ValueOrDie();

    std::string file_path =
        MakeBlobFilePath(base_dir_, blob_info_proto.creation_time_ms());
    std::string name_space = blob_info_proto.blob_handle().namespace_();
    NamespaceBlobStorageInfoProto& namespace_blob_storage_info =
        namespace_to_storage_info[name_space];
    namespace_blob_storage_info.set_namespace_(name_space);

    if (manage_blob_files_) {
      int64_t file_size = filesystem_.GetFileSize(file_path.c_str());
      if (file_size == Filesystem::kBadFileSize) {
        ICING_LOG(WARNING) << "Bad file size for blob file: " << file_path;
        continue;
      }
      namespace_blob_storage_info.set_blob_size(
          namespace_blob_storage_info.blob_size() + file_size);
      namespace_blob_storage_info.set_num_blobs(
          namespace_blob_storage_info.num_blobs() + 1);
    } else {
      namespace_blob_storage_info.add_blob_file_names(
          std::to_string(blob_info_proto.creation_time_ms()));
    }
  }

  // Create the namespace blob storage info for each namespace.
  std::vector<NamespaceBlobStorageInfoProto> namespace_blob_storage_infos;
  namespace_blob_storage_infos.reserve(namespace_to_storage_info.size());
  for (const auto& [_, namespace_blob_storage_info] :
       namespace_to_storage_info) {
    namespace_blob_storage_infos.push_back(
        std::move(namespace_blob_storage_info));
  }

  return namespace_blob_storage_infos;
}

}  // namespace lib
}  // namespace icing
