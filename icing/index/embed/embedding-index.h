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

#ifndef ICING_INDEX_EMBED_EMBEDDING_INDEX_H_
#define ICING_INDEX_EMBED_EMBEDDING_INDEX_H_

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
#include "icing/file/memory-mapped-file.h"
#include "icing/file/persistent-storage.h"
#include "icing/file/posting_list/flash-index-storage.h"
#include "icing/file/posting_list/posting-list-identifier.h"
#include "icing/index/embed/embedding-hit.h"
#include "icing/index/embed/posting-list-embedding-hit-accessor.h"
#include "icing/index/embed/posting-list-embedding-hit-serializer.h"
#include "icing/index/hit/hit.h"
#include "icing/store/document-id.h"
#include "icing/store/key-mapper.h"
#include "icing/util/crc32.h"

namespace icing {
namespace lib {

class EmbeddingIndex : public PersistentStorage {
 public:
  struct Info {
    static constexpr int32_t kMagic = 0x61e7cbf1;

    int32_t magic;
    DocumentId last_added_document_id;
    bool is_empty;

    static constexpr int kPaddingSize = 1000;
    // Padding exists just to reserve space for additional values.
    uint8_t padding_[kPaddingSize];

    Crc32 ComputeChecksum() const {
      return Crc32(
          std::string_view(reinterpret_cast<const char*>(this), sizeof(Info)));
    }
  };
  static_assert(sizeof(Info) == 1012, "");

  // Metadata file layout: <Crcs><Info>
  static constexpr int32_t kCrcsMetadataBufferOffset = 0;
  static constexpr int32_t kInfoMetadataBufferOffset =
      static_cast<int32_t>(sizeof(Crcs));
  static constexpr int32_t kMetadataFileSize = sizeof(Crcs) + sizeof(Info);
  static_assert(kMetadataFileSize == 1024, "");

  static constexpr WorkingPathType kWorkingPathType =
      WorkingPathType::kDirectory;

  EmbeddingIndex(const EmbeddingIndex&) = delete;
  EmbeddingIndex& operator=(const EmbeddingIndex&) = delete;

  // Creates a new EmbeddingIndex instance to index embeddings.
  //
  // Returns:
  //   - FAILED_PRECONDITION_ERROR if the file checksum doesn't match the stored
  //                               checksum.
  //   - INTERNAL_ERROR on I/O errors.
  //   - Any error from MemoryMappedFile, FlashIndexStorage,
  //     DynamicTrieKeyMapper, or FileBackedVector.
  static libtextclassifier3::StatusOr<std::unique_ptr<EmbeddingIndex>> Create(
      const Filesystem* filesystem, std::string working_path);

  static libtextclassifier3::Status Discard(const Filesystem& filesystem,
                                            const std::string& working_path) {
    return PersistentStorage::Discard(filesystem, working_path,
                                      kWorkingPathType);
  }

  libtextclassifier3::Status Clear();

  // Buffer an embedding pending to be added to the index. This is required
  // since EmbeddingHits added in posting lists must be decreasing, which means
  // that section ids and location indexes for a single document must be added
  // decreasingly.
  //
  // Returns:
  //   - OK on success
  //   - INVALID_ARGUMENT error if the dimension is 0.
  //   - INTERNAL_ERROR on I/O error
  libtextclassifier3::Status BufferEmbedding(
      const BasicHit& basic_hit, const PropertyProto::VectorProto& vector);

  // Commit the embedding hits in the buffer to the index.
  //
  // Returns:
  //   - OK on success
  //   - INTERNAL_ERROR on I/O error
  //   - Any error from posting lists
  libtextclassifier3::Status CommitBufferToIndex();

  // Returns a PostingListEmbeddingHitAccessor for all embedding hits that match
  // with the provided dimension and signature.
  //
  // Returns:
  //   - a PostingListEmbeddingHitAccessor instance on success.
  //   - INVALID_ARGUMENT error if the dimension is 0.
  //   - NOT_FOUND error if there is no matching embedding hit.
  //   - Any error from posting lists.
  libtextclassifier3::StatusOr<std::unique_ptr<PostingListEmbeddingHitAccessor>>
  GetAccessor(uint32_t dimension, std::string_view model_signature) const;

  // Returns a PostingListEmbeddingHitAccessor for all embedding hits that match
  // with the provided vector's dimension and signature.
  //
  // Returns:
  //   - a PostingListEmbeddingHitAccessor instance on success.
  //   - INVALID_ARGUMENT error if the dimension is 0.
  //   - NOT_FOUND error if there is no matching embedding hit.
  //   - Any error from posting lists.
  libtextclassifier3::StatusOr<std::unique_ptr<PostingListEmbeddingHitAccessor>>
  GetAccessorForVector(const PropertyProto::VectorProto& vector) const {
    return GetAccessor(vector.values_size(), vector.model_signature());
  }

  // Reduces internal file sizes by reclaiming space of deleted documents.
  // new_last_added_document_id will be used to update the last added document
  // id in the lite index.
  //
  // Returns:
  //   - OK on success
  //   - INTERNAL_ERROR on IO error, this indicates that the index may be in an
  //     invalid state and should be cleared.
  libtextclassifier3::Status Optimize(
      const std::vector<DocumentId>& document_id_old_to_new,
      DocumentId new_last_added_document_id);

  // Returns a pointer to the embedding vector for the given hit.
  //
  // Returns:
  //   - a pointer to the embedding vector on success.
  //   - OUT_OF_RANGE error if the referred vector is out of range based on the
  //     location and dimension.
  libtextclassifier3::StatusOr<const float*> GetEmbeddingVector(
      const EmbeddingHit& hit, uint32_t dimension) const {
    if (static_cast<int64_t>(hit.location()) + dimension >
        GetTotalVectorSize()) {
      return absl_ports::OutOfRangeError(
          "Got an embedding hit that refers to a vector out of range.");
    }
    return embedding_vectors_->array() + hit.location();
  }

  libtextclassifier3::StatusOr<const float*> GetRawEmbeddingData() const {
    if (is_empty()) {
      return absl_ports::NotFoundError("EmbeddingIndex is empty");
    }
    return embedding_vectors_->array();
  }

  int32_t GetTotalVectorSize() const {
    if (is_empty()) {
      return 0;
    }
    return embedding_vectors_->num_elements();
  }

  DocumentId last_added_document_id() const {
    return info().last_added_document_id;
  }

  void set_last_added_document_id(DocumentId document_id) {
    Info& info_ref = info();
    if (info_ref.last_added_document_id == kInvalidDocumentId ||
        document_id > info_ref.last_added_document_id) {
      info_ref.last_added_document_id = document_id;
    }
  }

  bool is_empty() const { return info().is_empty; }

 private:
  explicit EmbeddingIndex(const Filesystem& filesystem,
                          std::string working_path)
      : PersistentStorage(filesystem, std::move(working_path),
                          kWorkingPathType) {}

  // Creates the storage data if the index is not empty. This will initialize
  // flash_index_storage_, embedding_posting_list_mapper_, embedding_vectors_.
  //
  // Returns:
  //   - OK on success
  //   - Any error from FlashIndexStorage, DynamicTrieKeyMapper, or
  //     FileBackedVector.
  libtextclassifier3::Status CreateStorageDataIfNonEmpty();

  // Marks the index's header to indicate that the index is non-empty.
  //
  // If the index is already marked as non-empty, this is a no-op. Otherwise,
  // CreateStorageDataIfNonEmpty will be called to create the storage data.
  //
  // Returns:
  //   - OK on success
  //   - Any error when calling CreateStorageDataIfNonEmpty.
  libtextclassifier3::Status MarkIndexNonEmpty();

  libtextclassifier3::Status Initialize();

  // Transfers embedding data and hits from the current index to new_index.
  //
  // Returns:
  //   - OK on success
  //   - FAILED_PRECONDITION_ERROR if the current index is empty.
  //   - INTERNAL_ERROR on I/O error. This could potentially leave the storages
  //     in an invalid state and the caller should handle it properly (e.g.
  //     discard and rebuild)
  libtextclassifier3::Status TransferIndex(
      const std::vector<DocumentId>& document_id_old_to_new,
      EmbeddingIndex* new_index) const;

  // Flushes contents of metadata file.
  //
  // Returns:
  //   - OK on success
  //   - INTERNAL_ERROR on I/O error
  libtextclassifier3::Status PersistMetadataToDisk(bool force) override;

  // Flushes contents of all storages to underlying files.
  //
  // Returns:
  //   - OK on success
  //   - INTERNAL_ERROR on I/O error
  libtextclassifier3::Status PersistStoragesToDisk(bool force) override;

  // Computes and returns Info checksum.
  //
  // Returns:
  //   - Crc of the Info on success
  libtextclassifier3::StatusOr<Crc32> ComputeInfoChecksum(bool force) override;

  // Computes and returns all storages checksum.
  //
  // Returns:
  //   - Crc of all storages on success
  //   - INTERNAL_ERROR if any data inconsistency
  libtextclassifier3::StatusOr<Crc32> ComputeStoragesChecksum(
      bool force) override;

  Crcs& crcs() override {
    return *reinterpret_cast<Crcs*>(metadata_mmapped_file_->mutable_region() +
                                    kCrcsMetadataBufferOffset);
  }

  const Crcs& crcs() const override {
    return *reinterpret_cast<const Crcs*>(metadata_mmapped_file_->region() +
                                          kCrcsMetadataBufferOffset);
  }

  Info& info() {
    return *reinterpret_cast<Info*>(metadata_mmapped_file_->mutable_region() +
                                    kInfoMetadataBufferOffset);
  }

  const Info& info() const {
    return *reinterpret_cast<const Info*>(metadata_mmapped_file_->region() +
                                          kInfoMetadataBufferOffset);
  }

  // In memory data:
  // Pending embedding hits with their embedding keys used for
  // embedding_posting_list_mapper_.
  std::vector<std::pair<std::string, EmbeddingHit>> pending_embedding_hits_;

  // Metadata
  std::unique_ptr<MemoryMappedFile> metadata_mmapped_file_;

  // Posting list storage
  std::unique_ptr<PostingListEmbeddingHitSerializer>
      posting_list_hit_serializer_ =
          std::make_unique<PostingListEmbeddingHitSerializer>();

  // null if the index is empty.
  std::unique_ptr<FlashIndexStorage> flash_index_storage_;

  // The mapper from embedding keys to the corresponding posting list identifier
  // that stores all embedding hits with the same key.
  //
  // The key for an embedding hit is a one-to-one encoded string of the ordered
  // pair (dimension, model_signature) corresponding to the embedding.
  //
  // null if the index is empty.
  std::unique_ptr<KeyMapper<PostingListIdentifier>>
      embedding_posting_list_mapper_;

  // A single FileBackedVector that holds all embedding vectors.
  //
  // null if the index is empty.
  std::unique_ptr<FileBackedVector<float>> embedding_vectors_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_EMBED_EMBEDDING_INDEX_H_
