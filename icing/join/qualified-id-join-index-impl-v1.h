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

#ifndef ICING_JOIN_QUALIFIED_ID_JOIN_INDEX_IMPL_V1_H_
#define ICING_JOIN_QUALIFIED_ID_JOIN_INDEX_IMPL_V1_H_

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/file/persistent-storage.h"
#include "icing/join/document-join-id-pair.h"
#include "icing/join/qualified-id-join-index.h"
#include "icing/schema/joinable-property.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-id.h"
#include "icing/store/key-mapper.h"
#include "icing/store/namespace-id-fingerprint.h"
#include "icing/store/namespace-id.h"
#include "icing/util/crc32.h"

namespace icing {
namespace lib {

// QualifiedIdJoinIndexImplV1: a class to maintain data mapping
// DocumentJoinIdPair to joinable qualified ids and delete propagation info.
class QualifiedIdJoinIndexImplV1 : public QualifiedIdJoinIndex {
 public:
  struct Info {
    static constexpr int32_t kMagic = 0x48cabdc6;

    int32_t magic;
    DocumentId last_added_document_id;

    Crc32 GetChecksum() const {
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

  // Creates a QualifiedIdJoinIndexImplV1 instance to store qualified ids for
  // future joining search. If any of the underlying file is missing, then
  // delete the whole working_path and (re)initialize with new ones. Otherwise
  // initialize and create the instance by existing files.
  //
  // filesystem: Object to make system level calls
  // working_path: Specifies the working path for PersistentStorage.
  //               QualifiedIdJoinIndexImplV1 uses working path as working
  //               directory and all related files will be stored under this
  //               directory. It takes full ownership and of working_path_,
  //               including creation/deletion. It is the caller's
  //               responsibility to specify correct working path and avoid
  //               mixing different persistent storages together under the same
  //               path. Also the caller has the ownership for the parent
  //               directory of working_path_, and it is responsible for parent
  //               directory creation/deletion. See PersistentStorage for more
  //               details about the concept of working_path.
  // pre_mapping_fbv: flag indicating whether memory map max possible file size
  //                  for underlying FileBackedVector before growing the actual
  //                  file size.
  // use_persistent_hash_map: flag indicating whether use persistent hash map as
  //                          the key mapper (if false, then fall back to
  //                          dynamic trie key mapper).
  //
  // Returns:
  //   - FAILED_PRECONDITION_ERROR if the file checksum doesn't match the stored
  //                               checksum
  //   - INTERNAL_ERROR on I/O errors
  //   - Any KeyMapper errors
  static libtextclassifier3::StatusOr<
      std::unique_ptr<QualifiedIdJoinIndexImplV1>>
  Create(const Filesystem& filesystem, std::string working_path,
         bool pre_mapping_fbv, bool use_persistent_hash_map);

  // Delete copy and move constructor/assignment operator.
  QualifiedIdJoinIndexImplV1(const QualifiedIdJoinIndexImplV1&) = delete;
  QualifiedIdJoinIndexImplV1& operator=(const QualifiedIdJoinIndexImplV1&) =
      delete;

  QualifiedIdJoinIndexImplV1(QualifiedIdJoinIndexImplV1&&) = delete;
  QualifiedIdJoinIndexImplV1& operator=(QualifiedIdJoinIndexImplV1&&) = delete;

  ~QualifiedIdJoinIndexImplV1() override;

  // v2 only API. Returns UNIMPLEMENTED_ERROR.
  libtextclassifier3::Status Put(
      SchemaTypeId schema_type_id, JoinablePropertyId joinable_property_id,
      DocumentId document_id,
      std::vector<NamespaceIdFingerprint>&& ref_namespace_id_uri_fingerprints)
      override {
    return absl_ports::UnimplementedError("This API is not supported in V1");
  }

  // v2 only API. Returns UNIMPLEMENTED_ERROR.
  libtextclassifier3::StatusOr<std::unique_ptr<JoinDataIteratorBase>>
  GetIterator(SchemaTypeId schema_type_id,
              JoinablePropertyId joinable_property_id) const override {
    return absl_ports::UnimplementedError("This API is not supported in V1");
  }

  // v3 only API. Returns UNIMPLEMENTED_ERROR.
  libtextclassifier3::Status Put(
      const DocumentJoinIdPair& child_document_join_id_pair,
      std::vector<DocumentId>&& parent_document_ids) override {
    return absl_ports::UnimplementedError("This API is not supported in V1");
  }

  // v3 only API. Returns UNIMPLEMENTED_ERROR.
  libtextclassifier3::StatusOr<std::vector<DocumentJoinIdPair>> Get(
      DocumentId parent_document_id) const override {
    return absl_ports::UnimplementedError("This API is not supported in V1");
  }

  libtextclassifier3::Status Put(
      const DocumentJoinIdPair& document_join_id_pair,
      std::string_view ref_qualified_id_str) override;

  libtextclassifier3::StatusOr<std::string_view> Get(
      const DocumentJoinIdPair& document_join_id_pair) const override;

  libtextclassifier3::Status Optimize(
      const std::vector<DocumentId>& document_id_old_to_new,
      const std::vector<NamespaceId>& namespace_id_old_to_new,
      DocumentId new_last_added_document_id) override;

  libtextclassifier3::Status Clear() override;

  QualifiedIdJoinIndex::Version version() const override {
    return QualifiedIdJoinIndex::Version::kV1;
  }

  int32_t size() const override {
    return document_join_id_pair_mapper_->num_keys();
  }

  bool empty() const override { return size() == 0; }

  DocumentId last_added_document_id() const override {
    return info().last_added_document_id;
  }

  void set_last_added_document_id(DocumentId document_id) override {
    SetInfoDirty();

    Info& info_ref = info();
    if (info_ref.last_added_document_id == kInvalidDocumentId ||
        document_id > info_ref.last_added_document_id) {
      info_ref.last_added_document_id = document_id;
    }
  }

 private:
  explicit QualifiedIdJoinIndexImplV1(
      const Filesystem& filesystem, std::string&& working_path,
      std::unique_ptr<uint8_t[]> metadata_buffer,
      std::unique_ptr<KeyMapper<int32_t>> document_join_id_pair_mapper,
      std::unique_ptr<FileBackedVector<char>> qualified_id_storage,
      bool pre_mapping_fbv, bool use_persistent_hash_map)
      : QualifiedIdJoinIndex(filesystem, std::move(working_path)),
        metadata_buffer_(std::move(metadata_buffer)),
        document_join_id_pair_mapper_(std::move(document_join_id_pair_mapper)),
        qualified_id_storage_(std::move(qualified_id_storage)),
        pre_mapping_fbv_(pre_mapping_fbv),
        use_persistent_hash_map_(use_persistent_hash_map),
        is_info_dirty_(false),
        is_storage_dirty_(false) {}

  static libtextclassifier3::StatusOr<
      std::unique_ptr<QualifiedIdJoinIndexImplV1>>
  InitializeNewFiles(const Filesystem& filesystem, std::string&& working_path,
                     bool pre_mapping_fbv, bool use_persistent_hash_map);

  static libtextclassifier3::StatusOr<
      std::unique_ptr<QualifiedIdJoinIndexImplV1>>
  InitializeExistingFiles(const Filesystem& filesystem,
                          std::string&& working_path, bool pre_mapping_fbv,
                          bool use_persistent_hash_map);

  // Transfers qualified id join index data from the current to new_index and
  // convert to new document id according to document_id_old_to_new. It is a
  // helper function for Optimize.
  //
  // Returns:
  //   - OK on success
  //   - INTERNAL_ERROR on I/O error
  libtextclassifier3::Status TransferIndex(
      const std::vector<DocumentId>& document_id_old_to_new,
      QualifiedIdJoinIndexImplV1* new_index) const;

  libtextclassifier3::Status PersistMetadataToDisk() override;

  libtextclassifier3::Status PersistStoragesToDisk() override;

  libtextclassifier3::Status WriteMetadata() override;

  libtextclassifier3::Status InternalWriteMetadata(const ScopedFd& sfd);

  libtextclassifier3::StatusOr<Crc32> UpdateStoragesChecksum() override;

  libtextclassifier3::StatusOr<Crc32> GetInfoChecksum() const override;

  libtextclassifier3::StatusOr<Crc32> GetStoragesChecksum() const override;

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

  void SetInfoDirty() { is_info_dirty_ = true; }

  // When storage is dirty, we have to set info dirty as well. So just expose
  // SetDirty to set both.
  void SetDirty() {
    SetInfoDirty();
    is_storage_dirty_ = true;
  }

  bool is_info_dirty() const { return is_info_dirty_; }
  bool is_storage_dirty() const { return is_storage_dirty_; }

  // Metadata buffer
  std::unique_ptr<uint8_t[]> metadata_buffer_;

  // Persistent KeyMapper for mapping (encoded) DocumentJoinIdPair (DocumentId,
  // JoinablePropertyId) to another referenced document's qualified id string
  // index in qualified_id_storage_.
  std::unique_ptr<KeyMapper<int32_t>> document_join_id_pair_mapper_;

  // Storage for qualified id strings.
  std::unique_ptr<FileBackedVector<char>> qualified_id_storage_;

  // TODO(b/268521214): add delete propagation storage

  // Flag indicating whether memory map max possible file size for underlying
  // FileBackedVector before growing the actual file size.
  bool pre_mapping_fbv_;

  // Flag indicating whether use persistent hash map as the key mapper (if
  // false, then fall back to dynamic trie key mapper).
  bool use_persistent_hash_map_;

  bool is_info_dirty_;
  bool is_storage_dirty_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_JOIN_QUALIFIED_ID_JOIN_INDEX_IMPL_V1_H_
