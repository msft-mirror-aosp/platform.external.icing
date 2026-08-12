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

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/mutex.h"
#include "icing/absl_ports/thread_annotations.h"
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
#include "icing/index/embed/mini-batch-k-means.h"
#include "icing/index/embed/posting-list-embedding-hit-accessor.h"
#include "icing/index/embed/posting-list-embedding-hit-serializer.h"
#include "icing/index/embed/quantizer.h"
#include "icing/index/hit/hit.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/proto/ann.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/store/key-mapper.h"
#include "icing/util/clock.h"
#include "icing/util/crc32.h"
#include "icing/util/embedding-util.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"

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

  // Finds and returns up to k cluster IDs whose cluster centroids are closest
  // to the given query_vector.
  //
  // Note: The order of the cluster IDs in the returned list is arbitrary and
  // is not guaranteed to be sorted by distance.
  //
  // Returns:
  //   - A list of closest cluster IDs on success. The size of the list will
  //     be min(k, total number of clusters).
  //   - An empty list if the IVF index has not been built yet.
  //   - INVALID_ARGUMENT if the dimension of `query_vector` does not
  //     match the index dimension.
  //   - INTERNAL error if the IVF index is corrupted.
  libtextclassifier3::StatusOr<std::vector<uint32_t>>
  GetClosestClusterIdsByDistance(const PropertyProto::VectorProto& query_vector,
                                 uint32_t k) const
      ICING_LOCKS_EXCLUDED(mutex_) {
    absl_ports::shared_lock l(&mutex_);

    ICING_ASSIGN_OR_RETURN(IvfContextManager ivf_context,
                           IvfContextManager::Create(query_vector));
    return ivf_context.GetClosestClusterIdsByDistance(this, query_vector, k);
  }

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

  libtextclassifier3::Status Clear() ICING_LOCKS_EXCLUDED(mutex_);

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
      std::string_view schema_name) ICING_LOCKS_EXCLUDED(mutex_);

  // Buffers the given embedding vector mapped to a cluster based on the IVF
  // index. If IVF hasn't been built yet, the vector is mapped to a delta
  // store posting list.
  //
  // Returns:
  //   - OK on success
  //   - INVALID_ARGUMENT error if the dimension is 0.
  //   - RESOURCE_EXHAUSTED error if the allocated FileBackedVector has no more
  //       storage left.
  //   - INTERNAL error if no storage is found.
  libtextclassifier3::Status BufferEmbeddingIvf(
      const BasicHit& basic_hit, const PropertyProto::VectorProto& vector,
      EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
      std::string_view schema_name) ICING_LOCKS_EXCLUDED(mutex_);

  // Commit the embedding hits in the buffer to the index.
  //
  // Returns:
  //   - OK on success
  //   - INTERNAL_ERROR on I/O error
  //   - Any error from posting lists
  libtextclassifier3::Status CommitBufferToIndex() ICING_LOCKS_EXCLUDED(mutex_);

  // Accessor class to retrieve embedding hits.
  //
  // This class aggregates hits from multiple posting lists (e.g., from
  // different IVF clusters and the linear search index). It holds multiple
  // PostingListEmbeddingHitAccessor instances and merges their hits to yield
  // them in descending order of document ID.
  class ICING_SCOPED_LOCKABLE EmbeddingHitAccessor {
   public:
    explicit EmbeddingHitAccessor(const EmbeddingIndex* embedding_index)
        ICING_SHARED_LOCK_FUNCTION(embedding_index->mutex_)
        : shared_lock_(&embedding_index->mutex_),
          embedding_index_(*embedding_index) {}

    ~EmbeddingHitAccessor() ICING_UNLOCK_FUNCTION() = default;

    libtextclassifier3::Status AssertSharedLockHeld() const
        ICING_ASSERT_SHARED_LOCK(embedding_index_.mutex_) {
      if (!shared_lock_.owns_lock()) {
        return absl_ports::InternalError(
            "Shared lock is not held by EmbeddingHitAccessor.");
      }
      return libtextclassifier3::Status::OK;
    }

    struct HitInfo {
      EmbeddingHit hit;
      uint32_t posting_list_key_hash;
    };

    libtextclassifier3::StatusOr<std::vector<HitInfo>> GetNextHitsBatch()
        ICING_SHARED_LOCKS_REQUIRED(embedding_index_.mutex_);

    // Calculates the score for the given embedding hit with the given query.
    //
    // Returns:
    //   - The score on success.
    //   - OUT_OF_RANGE_ERROR if the referred vector is out of range based on
    //     the location and dimension.
    //   - Any error from schema store when getting the quantization type.
    libtextclassifier3::StatusOr<float> ScoreEmbeddingHit(
        const EmbeddingScorer& scorer, const std::vector<float>& query_floats,
        const HitInfo& hit_info,
        EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
        uint32_t schema_name_hash, bool is_ann)
        ICING_SHARED_LOCKS_REQUIRED(embedding_index_.mutex_);

    const DocHitInfoIterator::CallStats::EmbeddingStats& GetEmbeddingStats()
        const {
      return embedding_stats_;
    }

    // Adds a new posting list accessor to the priority queue for merging.
    // Fetches the first batch of hits from the accessor and adds it to the
    // priority queue if it is non-empty.
    libtextclassifier3::Status AddAccessor(
        std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor,
        uint32_t posting_list_key_hash)
        ICING_SHARED_LOCKS_REQUIRED(embedding_index_.mutex_);

   private:
    struct AccessorData {
      std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor;
      uint32_t posting_list_key_hash;
      std::vector<EmbeddingHit> current_batch;
      uint32_t current_index = 0;

      // Used in heap operations. Must make sure current_batch is not empty
      // before putting it in the heap.
      //
      // We define operator> with normal semantics and use std::greater in heap
      // operations to create a min-heap. We need a min-heap because DocumentIds
      // are inverted in Hit values; popping the smallest Hit value correctly
      // yields the largest DocumentId first.
      bool operator>(const AccessorData& other) const {
        return other.current_batch[other.current_index] <
               current_batch[current_index];
      }
    };

    libtextclassifier3::Status AddAccessor(AccessorData accessor_data)
        ICING_SHARED_LOCKS_REQUIRED(embedding_index_.mutex_);

    absl_ports::shared_lock shared_lock_;
    const EmbeddingIndex& embedding_index_;
    DocHitInfoIterator::CallStats::EmbeddingStats embedding_stats_;
    std::vector<AccessorData> accessors_;
  };

  // Returns a EmbeddingHitAccessor for all embedding hits that match
  // with the provided dimension and signature.
  //
  // The returned hit accessor aggregates hits from multiple posting lists based
  // on the provided cluster_ids. For each element in cluster_ids:
  // - If the element is kLinearSearchClusterId, the hit accessor includes hits
  //   from the base linear search index.
  // - Otherwise, the hit accessor includes hits from the IVF cluster
  //   corresponding to that provided ID.
  //
  // Returns:
  //   - a EmbeddingHitAccessor instance on success.
  //   - INVALID_ARGUMENT error if the dimension is 0.
  //   - NOT_FOUND error if there is no matching embedding hit for any of the
  //     specified clusters.
  //   - Any error from posting lists.
  libtextclassifier3::StatusOr<std::unique_ptr<EmbeddingHitAccessor>>
  GetAccessor(uint32_t dimension, std::string_view model_signature,
              const std::vector<uint32_t>& cluster_ids) const
      ICING_LOCKS_EXCLUDED(mutex_);

  // Returns a EmbeddingHitAccessor for all embedding hits that match
  // with the provided vector's dimension and signature.
  //
  // The returned hit accessor aggregates hits from multiple posting lists based
  // on the provided cluster_ids. For each element in cluster_ids:
  // - If the element is kLinearSearchClusterId, the hit accessor includes hits
  //   from the base linear search index.
  // - Otherwise, the hit accessor includes hits from the IVF cluster
  //   corresponding to that provided ID.
  //
  // Returns:
  //   - a EmbeddingHitAccessor instance on success.
  //   - INVALID_ARGUMENT error if the dimension is 0.
  //   - NOT_FOUND error if there is no matching embedding hit for any of the
  //     specified clusters.
  //   - Any error from posting lists.
  libtextclassifier3::StatusOr<std::unique_ptr<EmbeddingHitAccessor>>
  GetAccessorForVector(const PropertyProto::VectorProto& vector,
                       const std::vector<uint32_t>& cluster_ids) const
      ICING_LOCKS_EXCLUDED(mutex_) {
    ICING_ASSIGN_OR_RETURN(uint32_t dimension,
                           embedding_util::GetDimension(vector));
    return GetAccessor(dimension, vector.model_signature(), cluster_ids);
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
      DocumentId new_last_added_document_id) ICING_LOCKS_EXCLUDED(mutex_);

  // Runs or re-runs K-Means and redistributes embeddings into clusters for all
  // existing IVF metadata base keys.
  //
  // Returns:
  //   - The total number of K-Means iterations performed across all IVF
  //     maintenance operations on success.
  //   - Any error from the KeyMapper or MiniBatchKMeans.
  libtextclassifier3::StatusOr<int> MaintainAllIvf(
      const DocumentStore& document_store, const SchemaStore& schema_store,
      const MaintainAnnIndexOptions& maintain_ann_index_options)
      ICING_LOCKS_EXCLUDED(mutex_);

  DocumentId last_added_document_id() const ICING_LOCKS_EXCLUDED(mutex_) {
    absl_ports::shared_lock l(&mutex_);

    return info().last_added_document_id;
  }

  void set_last_added_document_id(DocumentId document_id)
      ICING_LOCKS_EXCLUDED(mutex_) {
    absl_ports::unique_lock l(&mutex_);

    Info& info_ref = info();
    if (info_ref.last_added_document_id == kInvalidDocumentId ||
        document_id > info_ref.last_added_document_id) {
      info_ref.last_added_document_id = document_id;
    }
  }

  libtextclassifier3::Status PersistToDisk() override
      ICING_LOCKS_EXCLUDED(mutex_);

  libtextclassifier3::StatusOr<Crc32> UpdateChecksums() override
      ICING_LOCKS_EXCLUDED(mutex_);

  libtextclassifier3::StatusOr<Crc32> GetChecksum() const override
      ICING_LOCKS_EXCLUDED(mutex_);

  uint32_t GetShardId(uint32_t posting_list_key_hash,
                      uint32_t schema_name_hash) const {
    return (posting_list_key_hash * 31 + schema_name_hash) % num_shards_;
  }

  uint32_t num_shards() const { return num_shards_; }

 private:
  struct IvfMetadata {
    // Total number of embeddings that were used for the last IVF index
    // construction (k-means) for this IVF corpus.
    // 0 means no IVF index construction has been performed for this IVF corpus,
    // and embeddings are only maintained in the delta-store.
    uint32_t last_ivf_build_size = 0;
    // Total number of embeddings currently in this IVF corpus.
    uint32_t current_size = 0;
    // Number of clusters maintained for this IVF corpus if k-means has been
    // performed. This does not include delta-store and centroids.
    uint32_t num_clusters = 0;

    bool operator==(const IvfMetadata& other) const {
      return last_ivf_build_size == other.last_ivf_build_size &&
             current_size == other.current_size &&
             num_clusters == other.num_clusters;
    }
  };

  class IvfContextManager {
   public:
    explicit IvfContextManager(std::string base_key)
        : dimension_(embedding_util::GetDimensionFromPostingListKey(base_key)),
          base_key_(std::move(base_key)) {}

    explicit IvfContextManager(uint32_t dimension,
                               std::string_view model_signature)
        : IvfContextManager(
              embedding_util::GetPostingListKey(dimension, model_signature)) {}

    static libtextclassifier3::StatusOr<IvfContextManager> Create(
        const PropertyProto::VectorProto& vector) {
      ICING_ASSIGN_OR_RETURN(std::string key,
                             embedding_util::GetPostingListKey(vector));
      return IvfContextManager(std::move(key));
    }

    std::string GetPostingListKey(uint32_t cluster_id) const;

    libtextclassifier3::StatusOr<IvfMetadata> GetMetadata(
        const EmbeddingIndex* embedding_index) const
        ICING_SHARED_LOCKS_REQUIRED(embedding_index->mutex_);

    libtextclassifier3::Status SetMetadata(EmbeddingIndex* embedding_index,
                                           IvfMetadata metadata) const
        ICING_EXCLUSIVE_LOCKS_REQUIRED(embedding_index->mutex_);

    // Finds and returns up to k cluster IDs whose cluster centroids are closest
    // to the given query_vector.
    //
    // Note: The order of the cluster IDs in the returned list is arbitrary and
    // is not guaranteed to be sorted by distance.
    //
    // Returns:
    //   - A list of closest cluster IDs on success. The size of the list will
    //     be min(k, total number of clusters).
    //   - An empty list if the IVF index has not been built yet.
    //   - INVALID_ARGUMENT if the dimension of `query_vector` does not
    //     match the index dimension.
    //   - INTERNAL error if the IVF index is corrupted.
    libtextclassifier3::StatusOr<std::vector<uint32_t>>
    GetClosestClusterIdsByDistance(
        const EmbeddingIndex* embedding_index,
        const PropertyProto::VectorProto& query_vector, uint32_t k) const
        ICING_SHARED_LOCKS_REQUIRED(embedding_index->mutex_);

    uint32_t dimension() const { return dimension_; }

    const std::string& base_key() const { return base_key_; }

   private:
    uint32_t dimension_;
    std::string base_key_;
  };

  explicit EmbeddingIndex(const Filesystem& filesystem,
                          std::string working_path, const Clock* clock,
                          const FeatureFlags* feature_flags,
                          uint32_t num_shards)
      : PersistentStorage(filesystem, std::move(working_path),
                          kWorkingPathType),
        clock_(*clock),
        feature_flags_(feature_flags),
        num_shards_(num_shards),
        mutex_(
            /*is_noop=*/!feature_flags->enable_read_during_ann_maintenance()),
        embedding_vectors_(num_shards),
        quantized_embedding_vectors_(num_shards) {}

  friend class EmbeddingHitAccessor;
  friend class EmbeddingIndexTest;
  friend class EmbeddingIndexTestPeer;

  // Returns a pointer to the embedding vector for the given hit.
  //
  // Returns:
  //   - a pointer to the embedding vector on success.
  //   - INVALID_ARGUMENT if the shard does not exist.
  //   - OUT_OF_RANGE error if the referred vector is out of range based on the
  //     location and dimension.
  libtextclassifier3::StatusOr<const float*> GetEmbeddingVector(
      const EmbeddingHit& hit, uint32_t dimension, uint32_t shard_id) const
      ICING_SHARED_LOCKS_REQUIRED(mutex_) {
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
      const EmbeddingHit& hit, uint32_t dimension, uint32_t shard_id) const
      ICING_SHARED_LOCKS_REQUIRED(mutex_) {
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

  bool is_empty() const ICING_SHARED_LOCKS_REQUIRED(mutex_) {
    return info().is_empty;
  }

  Info& info() ICING_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return *reinterpret_cast<Info*>(metadata_mmapped_file_->mutable_region() +
                                    kInfoMetadataBufferOffset);
  }

  const Info& info() const ICING_SHARED_LOCKS_REQUIRED(mutex_) {
    return *reinterpret_cast<const Info*>(metadata_mmapped_file_->region() +
                                          kInfoMetadataBufferOffset);
  }

  libtextclassifier3::Status CommitBufferToIndexLocked()
      ICING_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Creates the storage data. This will initialize flash_index_storage_,
  // embedding_posting_list_mapper_, and scan and initialize for existing vector
  // storage files.
  //
  // Returns:
  //   - OK on success
  //   - Any error from FlashIndexStorage, DynamicTrieKeyMapper, or
  //     FileBackedVector.
  libtextclassifier3::Status CreateStorageData()
      ICING_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Marks the index's header to indicate that the index is non-empty.
  //
  // If the index is already marked as non-empty, this is a no-op. Otherwise,
  // CreateStorageData will be called to create the storage data.
  //
  // Returns:
  //   - OK on success
  //   - Any error when calling CreateStorageData.
  libtextclassifier3::Status MarkIndexNonEmpty()
      ICING_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  libtextclassifier3::Status Initialize()
      ICING_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Transfers the embedding vector of the given hit from the current index to
  // the new index.
  //
  // Returns:
  //   - The location of the transferred vector in the new index on success.
  //   - Any error when allocating the vector storage in the new index.
  libtextclassifier3::StatusOr<uint32_t> TransferEmbeddingVector(
      const EmbeddingHit& old_hit, uint32_t dimension,
      EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
      uint32_t shard_id, EmbeddingIndex* new_index) const
      ICING_SHARED_LOCKS_REQUIRED(mutex_)
          ICING_EXCLUSIVE_LOCKS_REQUIRED(new_index->mutex_);

  // Returns an EmbeddingReference for the given hit, handling quantization.
  //
  // Returns:
  //   - An EmbeddingReference on success.
  //   - Any error from GetEmbeddingVector or GetQuantizedEmbeddingVector.
  libtextclassifier3::StatusOr<EmbeddingReference> GetEmbeddingReference(
      const EmbeddingHit& hit, uint32_t dimension,
      EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
      uint32_t shard_id) const ICING_SHARED_LOCKS_REQUIRED(mutex_);

  struct ExtractedEmbeddings {
    std::vector<EmbeddingHit> hits;
    std::vector<EmbeddingReference> embeddings;
    std::vector<uint32_t> schema_name_hashes;
    std::vector<uint32_t> shard_ids;
  };

  // Helper inside MaintainIvf to extract hit info out of the indexing vectors.
  libtextclassifier3::StatusOr<ExtractedEmbeddings> RetrieveAllEmbeddings(
      const DocumentStore& document_store, const SchemaStore& schema_store,
      const std::vector<std::string>& cluster_keys_to_read, uint32_t dimension,
      uint32_t reserve_size) ICING_SHARED_LOCKS_REQUIRED(mutex_);

  // Helper inside MaintainIvf connecting K-Means clustering algorithm return
  // centroids into standard centroid hits.
  libtextclassifier3::Status WriteCentroids(
      IvfContextManager& ivf_context,
      const std::vector<std::vector<float>>& centroids)
      ICING_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Transfers embeddings from all_hits to the new clusters based on the
  // partition_assignments in the result.
  libtextclassifier3::Status TransferEmbeddingsToNewClusters(
      const IvfContextManager& ivf_context,
      const MiniBatchKMeans::ClusteringResult& result,
      const ExtractedEmbeddings& extracted_embeddings)
      ICING_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Runs or re-runs K-Means and redistributes embeddings into clusters for a
  // given corpus (base_key).
  //
  // Returns:
  //   - The number of K-Means iterations performed during this maintenance
  //     operation on success.
  //   - NOT_FOUND error if the given corpus has no stored embeddings.
  //   - Any error from the KeyMapper or MiniBatchKMeans.
  libtextclassifier3::StatusOr<int> MaintainIvf(
      IvfContextManager ivf_context, const DocumentStore& document_store,
      const SchemaStore& schema_store,
      const MaintainAnnIndexOptions& maintain_ann_index_options)
      ICING_LOCKS_EXCLUDED(mutex_);

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
      EmbeddingIndex* new_index) const ICING_SHARED_LOCKS_REQUIRED(mutex_)
      ICING_EXCLUSIVE_LOCKS_REQUIRED(new_index->mutex_);

  libtextclassifier3::Status PersistMetadataToDisk() override
      ICING_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  libtextclassifier3::Status PersistStoragesToDisk() override
      ICING_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  libtextclassifier3::Status WriteMetadata() override {
    // EmbeddingIndex::Header is mmapped. Therefore, writes occur when the
    // metadata is modified. So just return OK.
    return libtextclassifier3::Status::OK;
  }

  libtextclassifier3::StatusOr<Crc32> UpdateStoragesChecksum() override
      ICING_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  libtextclassifier3::StatusOr<Crc32> GetInfoChecksum() const override
      ICING_SHARED_LOCKS_REQUIRED(mutex_) {
    return info().GetChecksum();
  }

  libtextclassifier3::StatusOr<Crc32> GetStoragesChecksum() const override
      ICING_SHARED_LOCKS_REQUIRED(mutex_);

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
      uint32_t shard_id) ICING_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

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
      uint32_t shard_id) ICING_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  Crcs& crcs() override ICING_EXCLUSIVE_LOCKS_REQUIRED(mutex_) {
    return *reinterpret_cast<Crcs*>(metadata_mmapped_file_->mutable_region() +
                                    kCrcsMetadataBufferOffset);
  }

  const Crcs& crcs() const override ICING_SHARED_LOCKS_REQUIRED(mutex_) {
    return *reinterpret_cast<const Crcs*>(metadata_mmapped_file_->region() +
                                          kCrcsMetadataBufferOffset);
  }

  libtextclassifier3::StatusOr<FileBackedVector<float>*>
  GetOrCreateEmbeddingVector(uint32_t shard_id)
      ICING_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  libtextclassifier3::StatusOr<FileBackedVector<char>*>
  GetOrCreateQuantizedEmbeddingVector(uint32_t shard_id)
      ICING_EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  const Clock& clock_;
  const FeatureFlags* feature_flags_;  // Does not own.
  const uint32_t num_shards_;

  // Mutex used to protect internal reads and writes.
  mutable absl_ports::shared_mutex mutex_;

  // Atomic boolean used to serialize ANN maintenance tasks (MaintainAllIvf).
  // If a maintenance task is already running, new requests will return early.
  std::atomic<bool> is_maintenance_running_{false};

  // In memory data:
  // Pending embedding hits with their embedding keys used for
  // embedding_posting_list_mapper_.
  std::vector<std::pair<std::string, EmbeddingHit>> pending_embedding_hits_
      ICING_GUARDED_BY(mutex_);

  // Metadata
  std::unique_ptr<MemoryMappedFile> metadata_mmapped_file_
      ICING_GUARDED_BY(mutex_);

  // Posting list storage
  std::unique_ptr<PostingListEmbeddingHitSerializer>
      posting_list_hit_serializer_ =
          std::make_unique<PostingListEmbeddingHitSerializer>();

  // null if the index is empty.
  std::unique_ptr<FlashIndexStorage> flash_index_storage_
      ICING_GUARDED_BY(mutex_);

  // The mapper from embedding keys to the corresponding posting list identifier
  // that stores all embedding hits with the same key.
  //
  // The key for an embedding hit is a one-to-one encoded string of the ordered
  // pair (dimension, model_signature) corresponding to the embedding.
  //
  // null if the index is empty.
  std::unique_ptr<KeyMapper<PostingListIdentifier>>
      embedding_posting_list_mapper_ ICING_GUARDED_BY(mutex_);

  // The mapper from the base embedding keys (dimension, model_signature) to
  // the corresponding IVF metadata.
  //
  // null if the index is empty.
  std::unique_ptr<KeyMapper<IvfMetadata>> ivf_metadata_mapper_
      ICING_GUARDED_BY(mutex_);

  // An array of FileBackedVectors that hold all embedding vectors, sharded by
  // a hash.
  //
  // An element is null if its corresponding file does not exist.
  std::vector<std::unique_ptr<FileBackedVector<float>>> embedding_vectors_
      ICING_GUARDED_BY(mutex_);
  std::vector<std::unique_ptr<FileBackedVector<char>>>
      quantized_embedding_vectors_ ICING_GUARDED_BY(mutex_);
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_EMBED_EMBEDDING_INDEX_H_
