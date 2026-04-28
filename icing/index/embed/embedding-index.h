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
#include "icing/absl_ports/str_cat.h"
#include "icing/feature-flags.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/file/persistent-storage.h"
#include "icing/file/posting_list/flash-index-storage.h"
#include "icing/file/posting_list/posting-list-identifier.h"
#include "icing/index/embed/embedding-hit.h"
#include "icing/index/embed/embedding-reference.h"
#include "icing/index/embed/embedding-scorer.h"
#include "icing/index/embed/posting-list-embedding-hit-accessor.h"
#include "icing/index/embed/posting-list-embedding-hit-serializer.h"
#include "icing/index/embed/quantizer.h"
#include "icing/index/hit/hit.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/store/key-mapper.h"
#include "icing/util/clock.h"
#include "icing/util/crc32.h"
#include "icing/util/logging.h"

namespace icing {
namespace lib {

class EmbeddingIndex : public PersistentStorage {
 public:
  struct Info {
    static constexpr int32_t kMagic = 0x61e7cbf1;

    int32_t magic;
    DocumentId last_added_document_id;
    bool is_empty;
    uint32_t num_shards;

    static constexpr int kPaddingSize = 996;
    // Padding exists just to reserve space for additional values.
    uint8_t padding_[kPaddingSize];

    Crc32 GetChecksum() const {
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
      const Filesystem* filesystem, std::string working_path,
      const Clock* clock, const FeatureFlags* feature_flags,
      uint32_t num_shards);

  static libtextclassifier3::Status Discard(const Filesystem& filesystem,
                                            const std::string& working_path) {
    return PersistentStorage::Discard(filesystem, working_path,
                                      kWorkingPathType);
  }

  libtextclassifier3::Status Clear();

  ~EmbeddingIndex() override {
    if (!PersistToDisk().ok()) {
      ICING_LOG(WARNING)
          << "Failed to persist embedding index to disk while destructing "
          << working_path_;
    }
  }

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
      const BasicHit& basic_hit, const PropertyProto::VectorProto& vector,
      EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
      std::string_view schema_name);

  // Commit the embedding hits in the buffer to the index.
  //
  // Returns:
  //   - OK on success
  //   - INTERNAL_ERROR on I/O error
  //   - Any error from posting lists
  libtextclassifier3::Status CommitBufferToIndex();

  class EmbeddingHitAccessor {
   public:
    EmbeddingHitAccessor(
        const EmbeddingIndex* embedding_index,
        std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor,
        std::string_view posting_list_key)
        : embedding_index_(*embedding_index),
          pl_accessor_(std::move(pl_accessor)),
          posting_list_key_hash_(
              EmbeddingIndex::GetPostingListKeyHash(posting_list_key)) {}

    libtextclassifier3::StatusOr<std::vector<EmbeddingHit>> GetNextHitsBatch() {
      return pl_accessor_->GetNextHitsBatch();
    }

    // Calculates the score for the given embedding hit with the given query.
    //
    // Returns:
    //   - The score on success.
    //   - OUT_OF_RANGE_ERROR if the referred vector is out of range based on
    //     the location and dimension.
    //   - Any error from schema store when getting the quantization type.
    libtextclassifier3::StatusOr<float> ScoreEmbeddingHit(
        const EmbeddingScorer& scorer, const PropertyProto::VectorProto& query,
        const EmbeddingHit& hit,
        EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
        uint32_t schema_name_hash);

    const DocHitInfoIterator::CallStats::EmbeddingStats& GetEmbeddingStats()
        const {
      return embedding_stats_;
    }

   private:
    const EmbeddingIndex& embedding_index_;
    std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor_;
    const uint32_t posting_list_key_hash_;
    DocHitInfoIterator::CallStats::EmbeddingStats embedding_stats_;
  };

  // Returns a EmbeddingHitAccessor for all embedding hits that match
  // with the provided dimension and signature.
  //
  // Returns:
  //   - a EmbeddingHitAccessor instance on success.
  //   - INVALID_ARGUMENT error if the dimension is 0.
  //   - NOT_FOUND error if there is no matching embedding hit.
  //   - Any error from posting lists.
  libtextclassifier3::StatusOr<std::unique_ptr<EmbeddingHitAccessor>>
  GetAccessor(uint32_t dimension, std::string_view model_signature) const;

  // Returns a EmbeddingHitAccessor for all embedding hits that match
  // with the provided vector's dimension and signature.
  //
  // Returns:
  //   - a EmbeddingHitAccessor instance on success.
  //   - INVALID_ARGUMENT error if the dimension is 0.
  //   - NOT_FOUND error if there is no matching embedding hit.
  //   - Any error from posting lists.
  libtextclassifier3::StatusOr<std::unique_ptr<EmbeddingHitAccessor>>
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
      const DocumentStore* document_store, const SchemaStore* schema_store,
      const std::vector<DocumentId>& document_id_old_to_new,
      DocumentId new_last_added_document_id);

  // Returns a pointer to the embedding vector for the given hit.
  //
  // Returns:
  //   - a pointer to the embedding vector on success.
  //   - INVALID_ARGUMENT if the shard does not exist.
  //   - OUT_OF_RANGE error if the referred vector is out of range based on the
  //     location and dimension.
  libtextclassifier3::StatusOr<const float*> GetEmbeddingVector(
      const EmbeddingHit& hit, uint32_t dimension, uint32_t shard_id) const {
    if (shard_id >= num_shards_ || embedding_vectors_[shard_id] == nullptr) {
      return absl_ports::InvalidArgumentError(
          "Attempting to query a non-existent storage shard.");
    }
    const auto& fbv = embedding_vectors_[shard_id];
    if (static_cast<int64_t>(hit.location()) + dimension >
        fbv->num_elements()) {
      return absl_ports::OutOfRangeError(
          "Got an embedding hit that refers to a vector out of range.");
    }
    return fbv->array() + hit.location();
  }
  libtextclassifier3::StatusOr<const char*> GetQuantizedEmbeddingVector(
      const EmbeddingHit& hit, uint32_t dimension, uint32_t shard_id) const {
    if (shard_id >= num_shards_ ||
        quantized_embedding_vectors_[shard_id] == nullptr) {
      return absl_ports::InvalidArgumentError(
          "Attempting to query a non-existent storage shard.");
    }
    const auto& fbv = quantized_embedding_vectors_[shard_id];
    // quantized_embedding_vectors_ stores data in char format. Every quantized
    // embedding vector contains a Quantizer header followed by the actual
    // vector, and every value in the vector is stored in uint8_t.
    if (static_cast<int64_t>(hit.location()) + sizeof(Quantizer) +
            sizeof(uint8_t) * dimension >
        fbv->num_elements()) {
      return absl_ports::OutOfRangeError(
          "Got an embedding hit that refers to a vector out of range.");
    }
    return fbv->array() + hit.location();
  }

  libtextclassifier3::StatusOr<const float*> GetRawEmbeddingData(
      uint32_t shard_id) const {
    if (is_empty()) {
      return absl_ports::NotFoundError("EmbeddingIndex is empty");
    }
    if (shard_id >= num_shards_ || embedding_vectors_[shard_id] == nullptr) {
      return absl_ports::NotFoundError(absl_ports::StrCat(
          "Embedding storage shard ", std::to_string(shard_id), " not found"));
    }
    return embedding_vectors_[shard_id]->array();
  }

  int32_t GetTotalVectorSize(uint32_t shard_id) const {
    if (is_empty() || shard_id >= num_shards_ ||
        embedding_vectors_[shard_id] == nullptr) {
      return 0;
    }
    return embedding_vectors_[shard_id]->num_elements();
  }

  int32_t GetTotalQuantizedVectorSize(uint32_t shard_id) const {
    if (is_empty() || shard_id >= num_shards_ ||
        quantized_embedding_vectors_[shard_id] == nullptr) {
      return 0;
    }
    return quantized_embedding_vectors_[shard_id]->num_elements();
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

  uint32_t GetShardId(uint32_t dimension, std::string_view model_signature,
                      std::string_view schema_name) const;

  uint32_t num_shards() const { return num_shards_; }

  Info& info() {
    return *reinterpret_cast<Info*>(metadata_mmapped_file_->mutable_region() +
                                    kInfoMetadataBufferOffset);
  }

  const Info& info() const {
    return *reinterpret_cast<const Info*>(metadata_mmapped_file_->region() +
                                          kInfoMetadataBufferOffset);
  }

 private:
  explicit EmbeddingIndex(const Filesystem& filesystem,
                          std::string working_path, const Clock* clock,
                          const FeatureFlags* feature_flags,
                          uint32_t num_shards)
      : PersistentStorage(filesystem, std::move(working_path),
                          kWorkingPathType),
        clock_(*clock),
        feature_flags_(feature_flags),
        num_shards_(num_shards),
        embedding_vectors_(num_shards),
        quantized_embedding_vectors_(num_shards) {}

  friend class EmbeddingHitAccessor;
  friend class EmbeddingIndexTest;

  static uint32_t GetPostingListKeyHash(std::string_view posting_list_key) {
    return Crc32(posting_list_key).Get();
  }

  uint32_t GetShardId(uint32_t posting_list_key_hash,
                      uint32_t schema_name_hash) const {
    return (posting_list_key_hash * 31 + schema_name_hash) % num_shards_;
  }

  // Creates the storage data. This will initialize flash_index_storage_,
  // embedding_posting_list_mapper_, and scan and initialize for existing vector
  // storage files.
  //
  // Returns:
  //   - OK on success
  //   - Any error from FlashIndexStorage, DynamicTrieKeyMapper, or
  //     FileBackedVector.
  libtextclassifier3::Status CreateStorageData();

  // Marks the index's header to indicate that the index is non-empty.
  //
  // If the index is already marked as non-empty, this is a no-op. Otherwise,
  // CreateStorageData will be called to create the storage data.
  //
  // Returns:
  //   - OK on success
  //   - Any error when calling CreateStorageData.
  libtextclassifier3::Status MarkIndexNonEmpty();

  libtextclassifier3::Status Initialize();

  // Transfers the embedding vector of the given hit from the current index to
  // the new index.
  //
  // Returns:
  //   - The location of the transferred vector in the new index on success.
  //   - Any error when allocating the vector storage in the new index.
  libtextclassifier3::StatusOr<uint32_t> TransferEmbeddingVector(
      const EmbeddingHit& old_hit, uint32_t dimension,
      EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
      uint32_t shard_id, EmbeddingIndex* new_index) const;

  // Transfers embedding data and hits from the current index to new_index.
  //
  // Returns:
  //   - OK on success
  //   - FAILED_PRECONDITION_ERROR if the current index is empty.
  //   - INTERNAL_ERROR on I/O error. This could potentially leave the storages
  //     in an invalid state and the caller should handle it properly (e.g.
  //     discard and rebuild)
  libtextclassifier3::Status TransferIndex(
      const DocumentStore& document_store, const SchemaStore& schema_store,
      const std::vector<DocumentId>& document_id_old_to_new,
      EmbeddingIndex* new_index) const;

  libtextclassifier3::Status PersistMetadataToDisk() override;

  libtextclassifier3::Status PersistStoragesToDisk() override;

  libtextclassifier3::Status WriteMetadata() override {
    // EmbeddingIndex::Header is mmapped. Therefore, writes occur when the
    // metadata is modified. So just return OK.
    return libtextclassifier3::Status::OK;
  }

  libtextclassifier3::StatusOr<Crc32> UpdateStoragesChecksum() override;

  libtextclassifier3::StatusOr<Crc32> GetInfoChecksum() const override {
    return info().GetChecksum();
  }

  libtextclassifier3::StatusOr<Crc32> GetStoragesChecksum() const override;

  // Appends the given embedding vector to the appropriate vector storage
  // shard based on the quantization type and shard_id. If the storage shard
  // does not exist, it will be created.
  //
  // Returns:
  //   - The location of the appended vector (i.e., the starting index within
  //     the vector storage shard).
  //   - Any error when allocating the vector storage.
  libtextclassifier3::StatusOr<uint32_t> AppendEmbeddingVector(
      const EmbeddingReference& embedding, uint32_t dimension,
      uint32_t shard_id);

  // Appends the given embedding vector to the appropriate vector storage
  // shard based on the quantization type and shard_id. If the storage shard
  // does not exist, it will be created.
  //
  // Returns:
  //   - The location of the appended vector (i.e., the starting index within
  //     the vector storage shard).
  //   - Any error when allocating the vector storage.
  libtextclassifier3::StatusOr<uint32_t> AppendEmbeddingVector(
      const PropertyProto::VectorProto& vector,
      EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
      uint32_t shard_id);

  Crcs& crcs() override {
    return *reinterpret_cast<Crcs*>(metadata_mmapped_file_->mutable_region() +
                                    kCrcsMetadataBufferOffset);
  }

  const Crcs& crcs() const override {
    return *reinterpret_cast<const Crcs*>(metadata_mmapped_file_->region() +
                                          kCrcsMetadataBufferOffset);
  }

  libtextclassifier3::StatusOr<FileBackedVector<float>*>
  GetOrCreateEmbeddingVector(uint32_t shard_id);

  libtextclassifier3::StatusOr<FileBackedVector<char>*>
  GetOrCreateQuantizedEmbeddingVector(uint32_t shard_id);

  const Clock& clock_;
  const FeatureFlags* feature_flags_;  // Does not own.
  const uint32_t num_shards_;

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

  // An array of FileBackedVectors that hold all embedding vectors, sharded by
  // a hash.
  //
  // An element is null if its corresponding file does not exist.
  std::vector<std::unique_ptr<FileBackedVector<float>>> embedding_vectors_;
  std::vector<std::unique_ptr<FileBackedVector<char>>>
      quantized_embedding_vectors_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_EMBED_EMBEDDING_INDEX_H_
