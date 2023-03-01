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

#ifndef ICING_JOIN_QUALIFIED_ID_TYPE_JOINABLE_CACHE_H_
#define ICING_JOIN_QUALIFIED_ID_TYPE_JOINABLE_CACHE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/file/filesystem.h"
#include "icing/file/persistent-storage.h"
#include "icing/join/doc-join-info.h"
#include "icing/store/document-id.h"
#include "icing/store/key-mapper.h"
#include "icing/util/crc32.h"

namespace icing {
namespace lib {

// QualifiedIdTypeJoinableCache: a class to maintain cache data mapping
// DocJoinInfo to joinable qualified ids and delete propagation info.
class QualifiedIdTypeJoinableCache : public PersistentStorage {
 public:
  struct Info {
    static constexpr int32_t kMagic = 0x48cabdc6;

    int32_t magic;
    DocumentId last_added_document_id;

    Crc32 ComputeChecksum() const {
      return Crc32(
          std::string_view(reinterpret_cast<const char*>(this), sizeof(Info)));
    }
  } __attribute__((packed));
  static_assert(sizeof(Info) == 8, "");

  // Metadata file layout: <Crcs><Info>
  static constexpr int32_t kCrcsMetadataBufferOffset = 0;
  static constexpr int32_t kInfoMetadataBufferOffset =
      static_cast<int32_t>(sizeof(Crcs));
  static constexpr int32_t kMetadataFileSize = sizeof(Crcs) + sizeof(Info);
  static_assert(kMetadataFileSize == 20, "");

  static constexpr WorkingPathType kWorkingPathType =
      WorkingPathType::kDirectory;
  static constexpr std::string_view kFilePrefix = "qualified_id_joinable_cache";

  // Creates a QualifiedIdTypeJoinableCache instance to store qualified ids for
  // future joining search. If any of the underlying file is missing, then
  // delete the whole working_path and (re)initialize with new ones. Otherwise
  // initialize and create the instance by existing files.
  //
  // filesystem: Object to make system level calls
  // working_path: Specifies the working path for PersistentStorage.
  //               QualifiedIdTypeJoinableCache uses working path as working
  //               directory and all related files will be stored under this
  //               directory. It takes full ownership and of working_path_,
  //               including creation/deletion. It is the caller's
  //               responsibility to specify correct working path and avoid
  //               mixing different persistent storages together under the same
  //               path. Also the caller has the ownership for the parent
  //               directory of working_path_, and it is responsible for parent
  //               directory creation/deletion. See PersistentStorage for more
  //               details about the concept of working_path.
  //
  // Returns:
  //   - FAILED_PRECONDITION_ERROR if the file checksum doesn't match the stored
  //                               checksum
  //   - INTERNAL_ERROR on I/O errors
  //   - Any KeyMapper errors
  static libtextclassifier3::StatusOr<
      std::unique_ptr<QualifiedIdTypeJoinableCache>>
  Create(const Filesystem& filesystem, std::string working_path);

  // Delete copy and move constructor/assignment operator.
  QualifiedIdTypeJoinableCache(const QualifiedIdTypeJoinableCache&) = delete;
  QualifiedIdTypeJoinableCache& operator=(const QualifiedIdTypeJoinableCache&) =
      delete;

  QualifiedIdTypeJoinableCache(QualifiedIdTypeJoinableCache&&) = delete;
  QualifiedIdTypeJoinableCache& operator=(QualifiedIdTypeJoinableCache&&) =
      delete;

  ~QualifiedIdTypeJoinableCache() override;

  // Puts a new data into cache: DocJoinInfo (DocumentId, JoinablePropertyId)
  // references to ref_document_id.
  //
  // Returns:
  //   - OK on success
  //   - INVALID_ARGUMENT_ERROR if doc_join_info is invalid
  //   - Any KeyMapper errors
  libtextclassifier3::Status Put(const DocJoinInfo& doc_join_info,
                                 DocumentId ref_document_id);

  // Gets the referenced DocumentId by DocJoinInfo.
  //
  // Returns:
  //   - DocumentId referenced by the given DocJoinInfo (DocumentId,
  //     JoinablePropertyId) on success
  //   - INVALID_ARGUMENT_ERROR if doc_join_info is invalid
  //   - NOT_FOUND_ERROR if doc_join_info doesn't exist
  //   - Any KeyMapper errors
  libtextclassifier3::StatusOr<DocumentId> Get(
      const DocJoinInfo& doc_join_info) const;

 private:
  explicit QualifiedIdTypeJoinableCache(
      const Filesystem& filesystem, std::string&& working_path,
      std::unique_ptr<uint8_t[]> metadata_buffer,
      std::unique_ptr<KeyMapper<DocumentId>> key_mapper)
      : PersistentStorage(filesystem, std::move(working_path),
                          kWorkingPathType),
        metadata_buffer_(std::move(metadata_buffer)),
        document_to_qualified_id_mapper_(std::move(key_mapper)) {}

  static libtextclassifier3::StatusOr<
      std::unique_ptr<QualifiedIdTypeJoinableCache>>
  InitializeNewFiles(const Filesystem& filesystem, std::string&& working_path);

  static libtextclassifier3::StatusOr<
      std::unique_ptr<QualifiedIdTypeJoinableCache>>
  InitializeExistingFiles(const Filesystem& filesystem,
                          std::string&& working_path);

  // Flushes contents of metadata file.
  //
  // Returns:
  //   - OK on success
  //   - INTERNAL_ERROR on I/O error
  libtextclassifier3::Status PersistMetadataToDisk() override;

  // Flushes contents of all storages to underlying files.
  //
  // Returns:
  //   - OK on success
  //   - INTERNAL_ERROR on I/O error
  libtextclassifier3::Status PersistStoragesToDisk() override;

  // Computes and returns Info checksum.
  //
  // Returns:
  //   - Crc of the Info on success
  libtextclassifier3::StatusOr<Crc32> ComputeInfoChecksum() override;

  // Computes and returns all storages checksum.
  //
  // Returns:
  //   - Crc of all storages on success
  //   - INTERNAL_ERROR if any data inconsistency
  libtextclassifier3::StatusOr<Crc32> ComputeStoragesChecksum() override;

  Crcs& crcs() override {
    return *reinterpret_cast<Crcs*>(metadata_buffer_.get() +
                                    kCrcsMetadataBufferOffset);
  }

  const Crcs& crcs() const override {
    return *reinterpret_cast<const Crcs*>(metadata_buffer_.get() +
                                          kCrcsMetadataBufferOffset);
  }

  Info& info() {
    return *reinterpret_cast<Info*>(metadata_buffer_.get() +
                                    kInfoMetadataBufferOffset);
  }

  const Info& info() const {
    return *reinterpret_cast<const Info*>(metadata_buffer_.get() +
                                          kInfoMetadataBufferOffset);
  }

  // Metadata buffer
  std::unique_ptr<uint8_t[]> metadata_buffer_;

  // Persistent KeyMapper for mapping (encoded) DocJoinInfo (DocumentId,
  // JoinablePropertyId) to another referenced DocumentId (converted from
  // qualified id string).
  std::unique_ptr<KeyMapper<DocumentId>> document_to_qualified_id_mapper_;

  // TODO(b/263890397): add delete propagation storage
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_JOIN_QUALIFIED_ID_TYPE_JOINABLE_CACHE_H_
