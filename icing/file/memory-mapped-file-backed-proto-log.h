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

#ifndef ICING_FILE_MEMORY_MAPPED_FILE_BACKED_PROTO_LOG_H_
#define ICING_FILE_MEMORY_MAPPED_FILE_BACKED_PROTO_LOG_H_

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/file/constants.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/util/crc32.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

// Memory-mapped-file backed proto log for append-only writes and position based
// reads.
//
// This class is built on top of the FileBackedVector class, which handles the
// underlying files related operations, such as checksums, flushing to disk.
//
// This class is NOT thread-safe.
template <typename ProtoT>
class MemoryMappedFileBackedProtoLog {
 public:
  // Creates a new MemoryMappedFileBackedProtoLog to read/write content to.
  //
  // filesystem: Object to make system level calls
  // file_path : Specifies the file to persist the log to; must be a path
  //             within a directory that already exists.
  //
  // Return:
  //   FAILED_PRECONDITION_ERROR if the file checksum doesn't match the stored
  //                             checksum.
  //   INTERNAL_ERROR on I/O errors.
  static libtextclassifier3::StatusOr<
      std::unique_ptr<MemoryMappedFileBackedProtoLog<ProtoT>>>
  Create(const Filesystem& filesystem, const std::string& file_path);

  // Deletes the underlying file.
  static libtextclassifier3::Status Delete(const Filesystem& filesystem,
                                           const std::string& file_path);

  // Delete copy constructor and assignment operator.
  MemoryMappedFileBackedProtoLog(const MemoryMappedFileBackedProtoLog&) =
      delete;
  MemoryMappedFileBackedProtoLog& operator=(
      const MemoryMappedFileBackedProtoLog&) = delete;

  // Calculates the checksum of the log contents and returns it. Does NOT
  // update the header.
  //
  // Returns:
  //   Checksum of the log contents.
  Crc32 GetChecksum() const;

  // Calculates the checksum of the log contents and updates the header to
  // hold this updated value.
  //
  // Returns:
  //   Checksum on success
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::StatusOr<Crc32> UpdateChecksum();

  // Reads the proto at the given index.
  //
  // Returns:
  //   proto on success
  //   INTERNAL_ERROR if the index points to an invalid position.
  //   OUT_OF_RANGE_ERROR if:
  //     - index < 0 or index >= num_elements - sizeof(ProtoMetadata)
  libtextclassifier3::StatusOr<ProtoT> Read(int32_t index) const;

  // Appends the proto to the end of the log.
  //
  // Returns:
  //   Index of the newly appended proto, on success.
  //   INVALID_ARGUMENT if the proto size exceeds the max size limit, 16MiB.
  libtextclassifier3::StatusOr<int32_t> Write(const ProtoT& proto);

  // Flushes content to underlying file.
  //
  // Returns:
  //   OK on success
  //   INTERNAL_ERROR on I/O errors
  libtextclassifier3::Status PersistToDisk();

 private:
  // The metadata of the proto, it contains 4 bytes, with the most significant
  // byte being the magic number, and remaining three bytes being the proto
  // size.
  // It is stored in front of every proto.
  using ProtoMetadata = int32_t;

  // Magic number encoded in the most significant byte of the proto metadata.
  static constexpr uint8_t kProtoMagic = 0x55;

  // Validates the proto metadata and extracts the proto size from it.
  //
  // Returns:
  //       INTERNAL_ERROR if the magic number stored in the metadata is
  //       invalid.
  static libtextclassifier3::StatusOr<int32_t> ValidateAndGetProtoSize(
      ProtoMetadata proto_metadata);

  explicit MemoryMappedFileBackedProtoLog(
      std::unique_ptr<FileBackedVector<uint8_t>> proto_fbv);

  std::unique_ptr<FileBackedVector<uint8_t>> proto_fbv_;
};

template <typename ProtoT>
MemoryMappedFileBackedProtoLog<ProtoT>::MemoryMappedFileBackedProtoLog(
    std::unique_ptr<FileBackedVector<uint8_t>> proto_fbv)
    : proto_fbv_(std::move(proto_fbv)) {}

template <typename ProtoT>
libtextclassifier3::StatusOr<int32_t>
MemoryMappedFileBackedProtoLog<ProtoT>::ValidateAndGetProtoSize(
    ProtoMetadata proto_metadata) {
  uint8_t magic_number = proto_metadata >> 24;
  if (magic_number != kProtoMagic) {
    return absl_ports::InvalidArgumentError(
        "Proto metadata has invalid magic number");
  }
  return proto_metadata & 0x00FFFFFF;
}

template <typename ProtoT>
Crc32 MemoryMappedFileBackedProtoLog<ProtoT>::GetChecksum() const {
  return proto_fbv_->GetChecksum();
}

template <typename ProtoT>
libtextclassifier3::StatusOr<Crc32>
MemoryMappedFileBackedProtoLog<ProtoT>::UpdateChecksum() {
  return proto_fbv_->UpdateChecksum();
}

template <typename ProtoT>
libtextclassifier3::StatusOr<
    std::unique_ptr<MemoryMappedFileBackedProtoLog<ProtoT>>>
MemoryMappedFileBackedProtoLog<ProtoT>::Create(const Filesystem& filesystem,
                                               const std::string& file_path) {
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<FileBackedVector<uint8_t>> proto_fbv,
                         FileBackedVector<uint8_t>::Create(
                             filesystem, file_path,
                             MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC));

  return std::unique_ptr<MemoryMappedFileBackedProtoLog<ProtoT>>(
      new MemoryMappedFileBackedProtoLog<ProtoT>(std::move(proto_fbv)));
}

template <typename ProtoT>
libtextclassifier3::Status MemoryMappedFileBackedProtoLog<ProtoT>::Delete(
    const Filesystem& filesystem, const std::string& file_path) {
  return FileBackedVector<uint8_t>::Delete(filesystem, file_path);
}

template <typename ProtoT>
libtextclassifier3::StatusOr<ProtoT>
MemoryMappedFileBackedProtoLog<ProtoT>::Read(int32_t index) const {
  if (index < 0) {
    return absl_ports::OutOfRangeError(
        IcingStringUtil::StringPrintf("Index, %d, is less than 0", index));
  }
  if (index + sizeof(ProtoMetadata) >= proto_fbv_->num_elements()) {
    return absl_ports::OutOfRangeError(IcingStringUtil::StringPrintf(
        "Index, %d, is greater/equal than the upper bound, %d", index,
        proto_fbv_->num_elements() - sizeof(ProtoMetadata)));
  }

  ProtoMetadata proto_metadata;
  std::memcpy(&proto_metadata, proto_fbv_->array() + index,
              sizeof(ProtoMetadata));

  ICING_ASSIGN_OR_RETURN(int32_t proto_size,
                         ValidateAndGetProtoSize(proto_metadata));
  ProtoT proto_data;
  if (!proto_data.ParseFromArray(
          proto_fbv_->array() + index + sizeof(ProtoMetadata), proto_size)) {
    return absl_ports::InternalError(
        "Failed to parse proto from MemoryMappedFileBackedProtoLog");
  }
  return proto_data;
}

template <typename ProtoT>
libtextclassifier3::StatusOr<int32_t>
MemoryMappedFileBackedProtoLog<ProtoT>::Write(const ProtoT& proto) {
  int32_t proto_byte_size = proto.ByteSizeLong();
  if (proto_byte_size > constants::kMaxProtoSize) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "Proto data size must be under 16MiB, was %d", proto_byte_size));
  }

  int32_t index_of_new_proto = proto_fbv_->num_elements();
  ICING_ASSIGN_OR_RETURN(
      FileBackedVector<uint8_t>::MutableArrayView mutable_array_view,
      proto_fbv_->Allocate(sizeof(ProtoMetadata) + proto_byte_size));

  ProtoMetadata proto_metadata = (kProtoMagic << 24) | proto_byte_size;
  uint8_t* byte_ptr = reinterpret_cast<uint8_t*>(&proto_metadata);
  mutable_array_view.SetArray(/*idx=*/0, byte_ptr, sizeof(ProtoMetadata));
  proto.SerializeWithCachedSizesToArray(
      &mutable_array_view[sizeof(ProtoMetadata)]);

  return index_of_new_proto;
}

template <typename ProtoT>
libtextclassifier3::Status
MemoryMappedFileBackedProtoLog<ProtoT>::PersistToDisk() {
  return proto_fbv_->PersistToDisk();
}

}  // namespace lib
}  // namespace icing

#endif  // ICING_FILE_MEMORY_MAPPED_FILE_BACKED_PROTO_LOG_H_
