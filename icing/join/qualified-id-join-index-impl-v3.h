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

#ifndef ICING_JOIN_QUALIFIED_ID_JOIN_INDEX_IMPL_V3_H_
#define ICING_JOIN_QUALIFIED_ID_JOIN_INDEX_IMPL_V3_H_

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/feature-flags.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/file/persistent-storage.h"
#include "icing/join/document-join-id-pair.h"
#include "icing/join/qualified-id-join-index.h"
#include "icing/schema/joinable-property.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-id.h"
#include "icing/store/namespace-id-fingerprint.h"
#include "icing/store/namespace-id.h"
#include "icing/util/crc32.h"

namespace icing {
namespace lib {

// QualifiedIdJoinIndexImplV3: a class to maintain join data. It uses 2
// FileBackedVectors to maintain the mapping between parent document id and a
// list of its joinable child infos (document id, joinable property id).
class QualifiedIdJoinIndexImplV3 : public QualifiedIdJoinIndex {
 public:
  struct Info {
    static constexpr int32_t kMagic = 0x1529a926;

    int32_t magic;
    int32_t num_data;
    DocumentId last_added_document_id;

    // Padding exists just to reserve space for additional values and make the
    // size of the metadata file 1024 bytes.
    static constexpr int kPaddingSize = 1000;
    uint8_t padding[kPaddingSize];

    Crc32 GetChecksum() const {
      return Crc32(
          std::string_view(reinterpret_cast<const char*>(this), sizeof(Info)));
    }
  } __attribute__((packed));
  static_assert(sizeof(Info) == 1012, "");

  // Metadata file layout: <Crcs><Info<data><padding>>
  static constexpr int32_t kCrcsMetadataFileOffset = 0;
  static constexpr int32_t kInfoMetadataFileOffset =
      static_cast<int32_t>(sizeof(Crcs));
  static constexpr int32_t kMetadataFileSize = sizeof(Crcs) + sizeof(Info);
  static_assert(kMetadataFileSize == 1024, "");

  static constexpr WorkingPathType kWorkingPathType =
      WorkingPathType::kDirectory;

  // An internal struct to store the start index and length of a
  // DocumentJoinIdPair array in child_document_join_id_pair_array_.
  struct ArrayInfo {
    int32_t index;
    int32_t length;
    int32_t used_length;

    constexpr ArrayInfo() : index(-1), length(-1), used_length(-1) {}

    explicit ArrayInfo(int32_t index_in, int32_t length_in,
                       int32_t used_length_in)
        : index(index_in), length(length_in), used_length(used_length_in) {}

    bool operator==(const ArrayInfo& other) const {
      return index == other.index && length == other.length &&
             used_length == other.used_length;
    }

    bool IsValid() const {
      return index >= 0 && length > 0 && used_length >= 0 &&
             used_length <= length;
    }
  } __attribute__((packed));
  static_assert(sizeof(ArrayInfo) == 12, "Invalid ArrayInfo size");

  // Creates a QualifiedIdJoinIndexImplV3 instance to store parent document to a
  // list of its joinable children for future joining search. If any of the
  // underlying file is missing, then delete the whole working_path and
  // (re)initialize with new ones. Otherwise initialize and create the instance
  // by existing files.
  //
  // filesystem: Object to make system level calls
  // working_path: Specifies the working path for PersistentStorage.
  //               QualifiedIdJoinIndexImplV3 uses working path as working
  //               directory and all related files will be stored under this
  //               directory. It takes full ownership and of working_path_,
  //               including creation/deletion. It is the caller's
  //               responsibility to specify correct working path and avoid
  //               mixing different persistent storages together under the same
  //               path. Also the caller has the ownership for the parent
  //               directory of working_path_, and it is responsible for parent
  //               directory creation/deletion. See PersistentStorage for more
  //               details about the concept of working_path.
  // feature_flags: a reference to (global) icing search engine feature flags.
  //
  // Returns:
  //   - FAILED_PRECONDITION_ERROR if the state of the join index is
  //                               inconsistent (e.g. checksum or the magic
  //                               doesn't match the stored ones, missing some
  //                               files, etc).
  //   - INTERNAL_ERROR on I/O errors
  //   - Any MemoryMappedFile, FileBackedVector errors
  static libtextclassifier3::StatusOr<
      std::unique_ptr<QualifiedIdJoinIndexImplV3>>
  Create(const Filesystem& filesystem, std::string working_path,
         const FeatureFlags& feature_flags);

  // Delete copy and move constructor/assignment operator.
  QualifiedIdJoinIndexImplV3(const QualifiedIdJoinIndexImplV3&) = delete;
  QualifiedIdJoinIndexImplV3& operator=(const QualifiedIdJoinIndexImplV3&) =
      delete;

  QualifiedIdJoinIndexImplV3(QualifiedIdJoinIndexImplV3&&) = delete;
  QualifiedIdJoinIndexImplV3& operator=(QualifiedIdJoinIndexImplV3&&) = delete;

  ~QualifiedIdJoinIndexImplV3() override;

  // Puts new join data into the index: adds a new child document and its
  // referenced parent documents into the join index.
  //
  // Returns:
  //   - OK on success
  //   - INVALID_ARGUMENT_ERROR if child_document_join_id_pair is invalid
  //   - Any FileBackedVector errors
  libtextclassifier3::Status Put(
      const DocumentJoinIdPair& child_document_join_id_pair,
      std::vector<DocumentId>&& parent_document_ids) override;

  // Gets the list of joinable children for the given parent document id.
  //
  // Returns:
  //   - A list of children's DocumentJoinIdPair on success
  //   - Any FileBackedVector errors
  libtextclassifier3::StatusOr<std::vector<DocumentJoinIdPair>> Get(
      DocumentId parent_document_id) const override;

  // Migrates existing join data for a parent document from old_document_id to
  // new_document_id.
  //
  // Note: when updating a document, we have to migrate the document's join data
  // if it is used as a parent document. For its child join data, it will be
  // tokenized and indexed separately, so no migration is needed.
  //
  // Returns:
  //   - OK on success
  //   - INVALID_ARGUMENT_ERROR if any document id is invalid
  //   - Any FileBackedVector errors
  libtextclassifier3::Status MigrateParent(DocumentId old_document_id,
                                           DocumentId new_document_id) override;

  // v2 only API. Returns UNIMPLEMENTED_ERROR.
  libtextclassifier3::Status Put(
      SchemaTypeId schema_type_id, JoinablePropertyId joinable_property_id,
      DocumentId document_id,
      std::vector<NamespaceIdFingerprint>&& ref_namespace_id_uri_fingerprints)
      override {
    return absl_ports::UnimplementedError("This API is not supported in V3");
  }

  // v2 only API. Returns UNIMPLEMENTED_ERROR.
  libtextclassifier3::StatusOr<std::unique_ptr<JoinDataIteratorBase>>
  GetIterator(SchemaTypeId schema_type_id,
              JoinablePropertyId joinable_property_id) const override {
    return absl_ports::UnimplementedError("This API is not supported in V3");
  }

  libtextclassifier3::Status Optimize(
      const std::vector<DocumentId>& document_id_old_to_new,
      const std::vector<NamespaceId>& namespace_id_old_to_new,
      DocumentId new_last_added_document_id) override;

  libtextclassifier3::Status Clear() override;

  QualifiedIdJoinIndex::Version version() const override {
    return QualifiedIdJoinIndex::Version::kV3;
  }

  int32_t size() const override { return info().num_data; }

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
  // Set max # of child DocumentJoinIdPair per parent to 2^16.
  // - It is unlikely that a single parent document will have extremely large
  //   # of children.
  // - Also prevent over extending the array.
  static constexpr int32_t kMaxNumChildrenPerParent = INT32_C(1) << 16;
  static_assert(kMaxNumChildrenPerParent <= (1u << 31),
                "Required for math_util::NextPowerOf2");

  explicit QualifiedIdJoinIndexImplV3(
      const Filesystem& filesystem, std::string working_path,
      std::unique_ptr<MemoryMappedFile> metadata_mmapped_file,
      std::unique_ptr<FileBackedVector<ArrayInfo>>
          parent_document_id_to_child_array_info,
      std::unique_ptr<FileBackedVector<DocumentJoinIdPair>>
          child_document_join_id_pair_array,
      const FeatureFlags& feature_flags)
      : QualifiedIdJoinIndex(filesystem, std::move(working_path)),
        metadata_mmapped_file_(std::move(metadata_mmapped_file)),
        parent_document_id_to_child_array_info_(
            std::move(parent_document_id_to_child_array_info)),
        child_document_join_id_pair_array_(
            std::move(child_document_join_id_pair_array)),
        feature_flags_(feature_flags),
        is_info_dirty_(false),
        is_storage_dirty_(false) {}

  static libtextclassifier3::StatusOr<
      std::unique_ptr<QualifiedIdJoinIndexImplV3>>
  InitializeNewFiles(const Filesystem& filesystem, std::string working_path,
                     const FeatureFlags& feature_flags);

  static libtextclassifier3::StatusOr<
      std::unique_ptr<QualifiedIdJoinIndexImplV3>>
  InitializeExistingFiles(const Filesystem& filesystem,
                          std::string working_path,
                          const FeatureFlags& feature_flags);

  // Appends a list of new child DocumentJoinIdPair to the parent's
  // DocumentJoinIdPair (extensible) array. If the array is invalid or doesn't
  // have enough space for new elements, then extend it and set the new array
  // info.
  //
  // Returns:
  //   - OK on success
  //   - RESOURCE_EXHAUSTED_ERROR if the new # of elements exceed
  //     kMaxNumChildrenPerParent
  //   - Any FileBackedVector errors
  libtextclassifier3::Status AppendChildDocumentJoinIdPairsForParent(
      DocumentId parent_document_id,
      std::vector<DocumentJoinIdPair>&& child_document_join_id_pairs);

  // Extends the parent document id to child array info if necessary, according
  // to the new parent document id.
  //
  // Returns:
  //   - On success, true if extended, and the caller should invalidate or
  //     refresh existing objects using related mmap addresses due to potential
  //     remapping. False otherwise
  //   - Any FileBackedVector errors
  libtextclassifier3::StatusOr<bool>
  ExtendParentDocumentIdToChildArrayInfoIfNecessary(
      DocumentId parent_document_id);

  // Gets the DocumentJoinIdPair mutable array and extends it if necessary to
  // fit num_to_add new elements in the future for the caller. If extended:
  // - Allocate a new array with size rounded up to the next 2's power to fit
  //   all existing elements and num_to_add new elements.
  // - Old elements will be migrated to the new array.
  // - The old array will be invalidated.
  //
  // Note: this function only prepares the array to fit "future" num_to_add
  // elements for the caller. It potentially does the extension but without
  // adding any new elements into the array, so the returned ArrayInfo's
  // used_length will remain unchanged.
  //
  // Returns:
  //   - GetOrExtendResult on success
  //   - RESOURCE_EXHAUSTED_ERROR if the new # of elements exceed
  //     kMaxNumChildrenPerParent
  //   - Any FileBackedVector errors
  struct GetMutableAndExtendResult {
    // ArrayInfo of the DocumentJoinIdPair array.
    // - If extended (allocated a new array), then the new array's ArrayInfo
    //   will be returned.
    // - Otherwise, the input (original) ArrayInfo will be returned.
    ArrayInfo array_info;

    // MutableArrayView object of the DocumentJoinIdPair array corresponding to
    // array_info. It will be either the new array after extension or the
    // original array, depending on whether it is extended or not.
    FileBackedVector<DocumentJoinIdPair>::MutableArrayView mutable_arr;
  };
  libtextclassifier3::StatusOr<GetMutableAndExtendResult>
  GetMutableAndExtendChildDocumentJoinIdPairArrayIfNecessary(
      const ArrayInfo& array_info, int32_t num_to_add);

  // Transfers qualified id join index data from the current to new_index and
  // convert to new document id according to document_id_old_to_new. It is a
  // helper function for Optimize.
  //
  // Returns:
  //   - OK on success
  //   - INTERNAL_ERROR on I/O error
  libtextclassifier3::Status TransferIndex(
      const std::vector<DocumentId>& document_id_old_to_new,
      QualifiedIdJoinIndexImplV3* new_index) const;

  libtextclassifier3::Status PersistMetadataToDisk() override;

  libtextclassifier3::Status PersistStoragesToDisk() override;

  libtextclassifier3::Status WriteMetadata() override {
    // QualifiedIdJoinIndexImplV3::Header is mmapped. Therefore, writes occur
    // when the metadata is modified. So just return OK.
    return libtextclassifier3::Status::OK;
  }

  libtextclassifier3::StatusOr<Crc32> UpdateStoragesChecksum() override;

  libtextclassifier3::StatusOr<Crc32> GetInfoChecksum() const override;

  libtextclassifier3::StatusOr<Crc32> GetStoragesChecksum() const override;

  Crcs& crcs() override {
    return *reinterpret_cast<Crcs*>(metadata_mmapped_file_->mutable_region() +
                                    kCrcsMetadataFileOffset);
  }

  const Crcs& crcs() const override {
    return *reinterpret_cast<const Crcs*>(metadata_mmapped_file_->region() +
                                          kCrcsMetadataFileOffset);
  }

  Info& info() {
    return *reinterpret_cast<Info*>(metadata_mmapped_file_->mutable_region() +
                                    kInfoMetadataFileOffset);
  }

  const Info& info() const {
    return *reinterpret_cast<const Info*>(metadata_mmapped_file_->region() +
                                          kInfoMetadataFileOffset);
  }

  void SetInfoDirty() { is_info_dirty_ = true; }
  // When storage is dirty, we have to set info dirty as well. So just expose
  // SetDirty to set both.
  void SetDirty() {
    is_info_dirty_ = true;
    is_storage_dirty_ = true;
  }

  bool is_info_dirty() const { return is_info_dirty_; }
  bool is_storage_dirty() const { return is_storage_dirty_; }

  std::unique_ptr<MemoryMappedFile> metadata_mmapped_file_;

  // Storage for mapping parent document id to the ArrayInfo, which points to an
  // extensible array stored in child_document_join_id_pair_array_, and the
  // extensible array contains the parent's joinable children information.
  std::unique_ptr<FileBackedVector<ArrayInfo>>
      parent_document_id_to_child_array_info_;

  // Storage for DocumentJoinIdPair.
  // - It is a collection of multiple extensible arrays for parents.
  // - Each extensible array contains a list of child DocumentJoinIdPair.
  // - The extensible array information (starting index of the FileBackedVector,
  //   length) is stored in parent_document_id_to_child_array_info_.
  std::unique_ptr<FileBackedVector<DocumentJoinIdPair>>
      child_document_join_id_pair_array_;

  const FeatureFlags& feature_flags_;  // Does not own.

  bool is_info_dirty_;
  bool is_storage_dirty_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_JOIN_QUALIFIED_ID_JOIN_INDEX_IMPL_V3_H_
