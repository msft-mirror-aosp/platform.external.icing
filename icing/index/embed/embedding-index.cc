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

#include "icing/index/embed/embedding-index.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/mutex.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/feature-flags.h"
#include "icing/file/destructible-directory.h"
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
#include "icing/index/embed/quantizer.h"
#include "icing/index/hit/hit.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/store/dynamic-trie-key-mapper.h"
#include "icing/store/key-mapper.h"
#include "icing/store/persistent-hash-map-key-mapper.h"
#include "icing/util/clock.h"
#include "icing/util/crc32.h"
#include "icing/util/embedding-util.h"
#include "icing/util/encode-util.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

// The maximum size of the embedding hit list mmapper.
// We use 64MiB for 32-bit platforms and 128MiB for 64-bit platforms.
#ifdef ICING_ARCH_BIT_64
constexpr uint32_t kEmbeddingHitListMapperMaxSize = 128 * 1024 * 1024;
#else
constexpr uint32_t kEmbeddingHitListMapperMaxSize = 64 * 1024 * 1024;
#endif

// The maximum size of the IVF metadata mapper.
constexpr uint32_t kIvfMetadataMapperMaxSize = kEmbeddingHitListMapperMaxSize;

constexpr uint32_t kInvalidShardId = std::numeric_limits<uint32_t>::max();

// The batch size when merging from *multiple* accessors in
// EmbeddingHitAccessor::GetNextHitsBatch.
constexpr uint32_t kKWayMergeBatchSize = 2048;

// A dummy schema name hash for IVF centroids.
const uint32_t kIvfCentroidsSchemaNameHash =
    SchemaStore::GetSchemaNameHash("_IVF_CENTROIDS_");

std::string GetMetadataFilePath(std::string_view working_path) {
  return absl_ports::StrCat(working_path, "/metadata");
}

std::string GetFlashIndexStorageFilePath(std::string_view working_path) {
  return absl_ports::StrCat(working_path, "/flash_index_storage");
}

std::string GetEmbeddingHitListMapperPath(std::string_view working_path) {
  return absl_ports::StrCat(working_path, "/embedding_hit_list_mapper");
}

std::string GetIvfMetadataMapperPath(std::string_view working_path) {
  return absl_ports::StrCat(working_path, "/ivf_metadata_mapper");
}

std::string AppendShardId(std::string&& file_path, uint32_t shard_id,
                          uint32_t num_shards) {
  if (num_shards > 1) {
    return absl_ports::StrCat(std::move(file_path), "_",
                              std::to_string(shard_id));
  }
  return file_path;
}

std::string GetEmbeddingVectorsFilePath(std::string_view working_path,
                                        uint32_t shard_id,
                                        uint32_t num_shards) {
  return AppendShardId(absl_ports::StrCat(working_path, "/embedding_vectors"),
                       shard_id, num_shards);
}

std::string GetQuantizedEmbeddingVectorsFilePath(std::string_view working_path,
                                                 uint32_t shard_id,
                                                 uint32_t num_shards) {
  return AppendShardId(
      absl_ports::StrCat(working_path, "/quantized_embedding_vectors"),
      shard_id, num_shards);
}

libtextclassifier3::StatusOr<Quantizer> CreateQuantizer(
    const PropertyProto::VectorProto& vector) {
  if (!vector.quantized_values().empty()) {
    return absl_ports::InvalidArgumentError(
        "Should not quantize an already quantized vector.");
  }
  if (vector.values().empty()) {
    return absl_ports::InvalidArgumentError("Vector dimension is 0");
  }
  auto minmax_pair =
      std::minmax_element(vector.values().begin(), vector.values().end());
  return Quantizer::Create(*minmax_pair.first, *minmax_pair.second);
}

libtextclassifier3::Status TryDelete(KeyMapper<PostingListIdentifier>* mapper,
                                     std::string_view key) {
  libtextclassifier3::Status status = mapper->Delete(key);
  if (absl_ports::IsNotFound(status)) {
    return libtextclassifier3::Status::OK;
  }
  return status;
}

}  // namespace

libtextclassifier3::StatusOr<std::vector<uint32_t>>
EmbeddingIndex::IvfContextManager::GetClosestClusterIdsByDistance(
    const EmbeddingIndex* embedding_index,
    const PropertyProto::VectorProto& query_vector, uint32_t k) const {
  ICING_ASSIGN_OR_RETURN(uint32_t dimension,
                         embedding_util::GetDimension(query_vector));
  if (dimension_ != dimension) {
    return absl_ports::InvalidArgumentError("Dimension mismatch");
  }

  const float* query_floats = nullptr;
  std::vector<float> dequantized_query;
  if (!query_vector.quantized_values().empty()) {
    dequantized_query.resize(dimension_);
    embedding_util::Dequantize(query_vector.quantized_values().data(),
                               static_cast<int>(dimension_),
                               dequantized_query.data());
    query_floats = dequantized_query.data();
  } else {
    query_floats = query_vector.values().data();
  }
  if (embedding_index->is_empty()) {
    return std::vector<uint32_t>();
  }
  if (k == 0) {
    ICING_LOG(WARNING) << "k is 0, returning empty vector";
    return std::vector<uint32_t>();
  }

  ICING_ASSIGN_OR_RETURN(IvfMetadata ivf_metadata,
                         GetMetadata(embedding_index));
  if (ivf_metadata.last_ivf_build_size == 0) {
    // IVF index is not built, so there are no centroids.
    return std::vector<uint32_t>();
  }
  if (ivf_metadata.num_clusters == 0) {
    return absl_ports::InternalError(
        "IVF is enabled but no clusters found. The metadata is not "
        "consistent.");
  }

  std::string centroid_key =
      GetPostingListKey(embedding_util::kIvfCentroidsClusterId);
  ICING_ASSIGN_OR_RETURN(
      PostingListIdentifier centroid_pl_id,
      embedding_index->embedding_posting_list_mapper_->Get(centroid_key));
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<PostingListEmbeddingHitAccessor> centroid_accessor,
      PostingListEmbeddingHitAccessor::CreateFromExisting(
          embedding_index->flash_index_storage_.get(),
          embedding_index->posting_list_hit_serializer_.get(), centroid_pl_id));

  uint32_t centroid_shard_id = embedding_index->GetShardId(
      embedding_util::GetPostingListKeyHash(centroid_key),
      kIvfCentroidsSchemaNameHash);

  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<EmbeddingScorer> scorer,
      EmbeddingScorer::Create(
          SearchSpecProto::EmbeddingQueryMetricType::EUCLIDEAN));

  std::vector<uint32_t> cluster_ids;
  std::vector<float> distances;

  int current_cluster_index = 0;
  while (true) {
    ICING_ASSIGN_OR_RETURN(std::vector<EmbeddingHit> batch,
                           centroid_accessor->GetNextHitsBatch());
    if (batch.empty()) {
      break;
    }

    for (const EmbeddingHit& centroid_hit : batch) {
      ICING_ASSIGN_OR_RETURN(const float* centroid_vector,
                             embedding_index->GetEmbeddingVector(
                                 centroid_hit, dimension_, centroid_shard_id));

      float distance = scorer->EigenScore(static_cast<int>(dimension_),
                                          query_floats, centroid_vector);
      cluster_ids.push_back(embedding_util::kIvfBaseClusterId +
                            current_cluster_index);
      distances.push_back(distance);
      current_cluster_index++;
    }
  }

  if (cluster_ids.size() != ivf_metadata.num_clusters) {
    return absl_ports::InternalError(
        "The actual number of clusters doesn't match the metadata recorded. "
        "The IVF index may be corrupted.");
  }
  if (k > cluster_ids.size()) {
    k = cluster_ids.size();
  }

  std::vector<int> indices(cluster_ids.size());
  std::iota(indices.begin(), indices.end(), 0);
  // This calculates the closest k clusters in O(N) time,
  // but the order of the returned clusters is not guaranteed.
  std::nth_element(
      indices.begin(), indices.begin() + (k - 1), indices.end(),
      [&distances](int i, int j) { return distances[i] < distances[j]; });
  std::vector<uint32_t> top_k_cluster_ids;
  top_k_cluster_ids.reserve(k);
  for (uint32_t i = 0; i < k; ++i) {
    top_k_cluster_ids.push_back(cluster_ids[indices[i]]);
  }
  return top_k_cluster_ids;
}

libtextclassifier3::StatusOr<std::unique_ptr<EmbeddingIndex>>
EmbeddingIndex::Create(const Filesystem* filesystem, std::string working_path,
                       const Clock* clock, const FeatureFlags* feature_flags,
                       uint32_t num_shards) {
  ICING_RETURN_ERROR_IF_NULL(filesystem);
  ICING_RETURN_ERROR_IF_NULL(clock);

  if (num_shards == 0) {
    return absl_ports::InvalidArgumentError("Number of shards cannot be 0");
  }

  std::unique_ptr<EmbeddingIndex> index = std::unique_ptr<EmbeddingIndex>(
      new EmbeddingIndex(*filesystem, std::move(working_path), clock,
                         feature_flags, num_shards));
  {
    absl_ports::unique_lock l(&index->mutex_);

    ICING_RETURN_IF_ERROR(index->Initialize());
  }
  return index;
}

libtextclassifier3::Status EmbeddingIndex::CreateStorageData() {
  ICING_ASSIGN_OR_RETURN(FlashIndexStorage flash_index_storage,
                         FlashIndexStorage::Create(
                             GetFlashIndexStorageFilePath(working_path_),
                             &filesystem_, posting_list_hit_serializer_.get()));
  flash_index_storage_ =
      std::make_unique<FlashIndexStorage>(std::move(flash_index_storage));

  ICING_ASSIGN_OR_RETURN(
      embedding_posting_list_mapper_,
      DynamicTrieKeyMapper<PostingListIdentifier>::Create(
          filesystem_, GetEmbeddingHitListMapperPath(working_path_),
          kEmbeddingHitListMapperMaxSize));

  ICING_ASSIGN_OR_RETURN(
      ivf_metadata_mapper_,
      PersistentHashMapKeyMapper<EmbeddingIndex::IvfMetadata>::Create(
          filesystem_, GetIvfMetadataMapperPath(working_path_),
          kIvfMetadataMapperMaxSize));

  // Scan for existing sharded vector files and load them.
  for (int i = 0; i < num_shards_; ++i) {
    // Non-quantized vectors
    if (filesystem_.FileExists(
            GetEmbeddingVectorsFilePath(working_path_, i, num_shards_)
                .c_str())) {
      ICING_RETURN_IF_ERROR(GetOrCreateEmbeddingVector(i));
    }
    // Quantized vectors
    if (filesystem_.FileExists(
            GetQuantizedEmbeddingVectorsFilePath(working_path_, i, num_shards_)
                .c_str())) {
      ICING_RETURN_IF_ERROR(GetOrCreateQuantizedEmbeddingVector(i));
    }
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status EmbeddingIndex::MarkIndexNonEmpty() {
  if (!is_empty()) {
    return libtextclassifier3::Status::OK;
  }
  ICING_RETURN_IF_ERROR(CreateStorageData());
  info().is_empty = false;
  return libtextclassifier3::Status::OK;
}

std::string EmbeddingIndex::IvfContextManager::GetPostingListKey(
    uint32_t cluster_id) const {
  return absl_ports::StrCat(base_key_,
                            embedding_util::kIvfPostingListKeySeparator,
                            encode_util::EncodeIntToCString(cluster_id));
}

libtextclassifier3::StatusOr<EmbeddingIndex::IvfMetadata>
EmbeddingIndex::IvfContextManager::GetMetadata(
    const EmbeddingIndex* embedding_index) const {
  if (embedding_index->is_empty()) {
    return absl_ports::FailedPreconditionError("EmbeddingIndex is empty");
  }
  libtextclassifier3::StatusOr<IvfMetadata> ivf_metadata_or =
      embedding_index->ivf_metadata_mapper_->Get(base_key_);
  if (absl_ports::IsNotFound(ivf_metadata_or.status())) {
    return IvfMetadata();
  }
  return ivf_metadata_or;
}

libtextclassifier3::Status EmbeddingIndex::IvfContextManager::SetMetadata(
    EmbeddingIndex* embedding_index, IvfMetadata metadata) const {
  if (embedding_index->is_empty()) {
    return absl_ports::FailedPreconditionError("EmbeddingIndex is empty");
  }
  return embedding_index->ivf_metadata_mapper_->Put(base_key_,
                                                    std::move(metadata));
}

libtextclassifier3::StatusOr<FileBackedVector<float>*>
EmbeddingIndex::GetOrCreateEmbeddingVector(uint32_t shard_id) {
  if (shard_id >= num_shards_) {
    return absl_ports::InvalidArgumentError("Shard id is out of range.");
  }
  auto& fbv_ptr = embedding_vectors_[shard_id];
  if (fbv_ptr == nullptr) {
    ICING_ASSIGN_OR_RETURN(
        fbv_ptr,
        FileBackedVector<float>::Create(
            filesystem_,
            GetEmbeddingVectorsFilePath(working_path_, shard_id, num_shards_),
            MemoryMappedFile::READ_WRITE_AUTO_SYNC));
  }
  return fbv_ptr.get();
}

libtextclassifier3::StatusOr<FileBackedVector<char>*>
EmbeddingIndex::GetOrCreateQuantizedEmbeddingVector(uint32_t shard_id) {
  if (shard_id >= num_shards_) {
    return absl_ports::InvalidArgumentError("Shard id is out of range.");
  }
  auto& fbv_ptr = quantized_embedding_vectors_[shard_id];
  if (fbv_ptr == nullptr) {
    ICING_ASSIGN_OR_RETURN(fbv_ptr,
                           FileBackedVector<char>::Create(
                               filesystem_,
                               GetQuantizedEmbeddingVectorsFilePath(
                                   working_path_, shard_id, num_shards_),
                               MemoryMappedFile::READ_WRITE_AUTO_SYNC));
  }
  return fbv_ptr.get();
}

libtextclassifier3::Status EmbeddingIndex::Initialize() {
  bool is_new = false;
  if (!filesystem_.FileExists(GetMetadataFilePath(working_path_).c_str())) {
    // Create working directory.
    if (!filesystem_.CreateDirectoryRecursively(working_path_.c_str())) {
      return absl_ports::InternalError(
          absl_ports::StrCat("Failed to create directory: ", working_path_));
    }
    is_new = true;
  }

  ICING_ASSIGN_OR_RETURN(
      MemoryMappedFile metadata_mmapped_file,
      MemoryMappedFile::Create(filesystem_, GetMetadataFilePath(working_path_),
                               MemoryMappedFile::Strategy::READ_WRITE_AUTO_SYNC,
                               /*max_file_size=*/kMetadataFileSize,
                               /*pre_mapping_file_offset=*/0,
                               /*pre_mapping_mmap_size=*/kMetadataFileSize));
  metadata_mmapped_file_ =
      std::make_unique<MemoryMappedFile>(std::move(metadata_mmapped_file));

  if (is_new) {
    ICING_RETURN_IF_ERROR(metadata_mmapped_file_->GrowAndRemapIfNecessary(
        /*file_offset=*/0, /*mmap_size=*/kMetadataFileSize));
    info().magic = Info::kMagic;
    info().last_added_document_id = kInvalidDocumentId;
    info().is_empty = true;
    info().num_shards = num_shards_;
    memset(info().padding_, 0, Info::kPaddingSize);
    ICING_RETURN_IF_ERROR(InitializeNewStorage());
  } else {
    if (metadata_mmapped_file_->available_size() != kMetadataFileSize) {
      return absl_ports::FailedPreconditionError(
          "Incorrect metadata file size");
    }
    if (info().magic != Info::kMagic) {
      ICING_LOG(ERROR) << "Invalid header magic for EmbeddingIndex "
                       << working_path_ << ". Expected: " << Info::kMagic
                       << ", actual: " << info().magic;
      return absl_ports::FailedPreconditionError(absl_ports::StrCat(
          "Invalid header magic for EmbeddingIndex: ", working_path_));
    }
    uint32_t num_shards_read = info().num_shards;
    // This happens for old versions of embedding index that did not have
    // num_shards set. Just treat it as 1.
    if (num_shards_read == 0) {
      ICING_LOG(INFO)
          << "Number of shards not set in metadata. Defaulting to 1.";
      num_shards_read = 1;
    }
    // If num_shards doesn't match, the index need to be rebuilt.
    if (num_shards_read != num_shards_) {
      ICING_LOG(ERROR) << "Mismatched number of shards. Expected "
                       << num_shards_ << ", actual " << num_shards_read;
      return absl_ports::FailedPreconditionError(absl_ports::StrCat(
          "Mismatched number of shards in metadata for EmbeddingIndex: ",
          working_path_));
    }
    if (!info().is_empty) {
      ICING_RETURN_IF_ERROR(CreateStorageData());
    }
    ICING_RETURN_IF_ERROR(InitializeExistingStorage());
    if (info().num_shards == 0) {
      // This means that num_shards isn't set in the header, but we are still
      // considering it a match for num_shards_. The only possibility is that
      // num_shards_ == 1 and we have treated info().num_shards as 1.
      // Now, update the header to record the correct num_shards_.
      info().num_shards = num_shards_;
    }
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status EmbeddingIndex::Clear() {
  absl_ports::unique_lock l(&mutex_);

  pending_embedding_hits_.clear();
  metadata_mmapped_file_.reset();
  flash_index_storage_.reset();
  embedding_posting_list_mapper_.reset();
  ivf_metadata_mapper_.reset();
  for (int i = 0; i < num_shards_; ++i) {
    embedding_vectors_[i].reset();
    quantized_embedding_vectors_[i].reset();
  }
  if (filesystem_.DirectoryExists(working_path_.c_str())) {
    ICING_RETURN_IF_ERROR(Discard(filesystem_, working_path_));
  }
  is_initialized_ = false;
  return Initialize();
}

libtextclassifier3::StatusOr<
    std::unique_ptr<EmbeddingIndex::EmbeddingHitAccessor>>
EmbeddingIndex::GetAccessor(uint32_t dimension,
                            std::string_view model_signature,
                            const std::vector<uint32_t>& cluster_ids) const {
  if (dimension == 0) {
    return absl_ports::InvalidArgumentError("Dimension is 0");
  }
  if (cluster_ids.empty()) {
    return absl_ports::InvalidArgumentError("cluster_ids cannot be empty");
  }
  auto accessor = std::make_unique<EmbeddingHitAccessor>(this);
  ICING_RETURN_IF_ERROR(accessor->AssertSharedLockHeld());
  if (is_empty()) {
    return absl_ports::NotFoundError("EmbeddingIndex is empty");
  }

  IvfContextManager ivf_manager(dimension, model_signature);
  bool has_posting_lists = false;

  std::vector<uint32_t> unique_cluster_ids = cluster_ids;
  std::sort(unique_cluster_ids.begin(), unique_cluster_ids.end());
  unique_cluster_ids.erase(
      std::unique(unique_cluster_ids.begin(), unique_cluster_ids.end()),
      unique_cluster_ids.end());

  for (const uint32_t cluster_id : unique_cluster_ids) {
    std::string key;
    if (cluster_id != embedding_util::kLinearSearchClusterId) {
      key = ivf_manager.GetPostingListKey(cluster_id);
    } else {
      key = embedding_util::GetPostingListKey(dimension, model_signature);
    }

    libtextclassifier3::StatusOr<PostingListIdentifier> posting_list_id_or =
        embedding_posting_list_mapper_->Get(key);

    if (absl_ports::IsNotFound(posting_list_id_or.status())) {
      continue;
    } else if (!posting_list_id_or.ok()) {
      return posting_list_id_or.status();
    }

    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor,
        PostingListEmbeddingHitAccessor::CreateFromExisting(
            flash_index_storage_.get(), posting_list_hit_serializer_.get(),
            posting_list_id_or.ValueOrDie()));

    ICING_RETURN_IF_ERROR(accessor->AddAccessor(
        std::move(pl_accessor), embedding_util::GetPostingListKeyHash(key)));
    has_posting_lists = true;
  }

  if (!has_posting_lists) {
    return absl_ports::NotFoundError("No posting lists found");
  }

  return accessor;
}

libtextclassifier3::StatusOr<uint32_t> EmbeddingIndex::AppendEmbeddingVector(
    const EmbeddingReference& embedding, uint32_t dimension,
    uint32_t shard_id) {
  ICING_RETURN_IF_ERROR(embedding.Validate());
  if (dimension == 0) {
    return absl_ports::InvalidArgumentError("Dimension is 0");
  }
  if (dimension > std::numeric_limits<int32_t>::max() - sizeof(Quantizer)) {
    return absl_ports::InvalidArgumentError(
        "Dimension is too large and exceeds maximum supported size.");
  }

  uint32_t location;
  if (embedding.float_vector != nullptr) {
    ICING_ASSIGN_OR_RETURN(FileBackedVector<float> * fbv_ptr,
                           GetOrCreateEmbeddingVector(shard_id));
    location = fbv_ptr->num_elements();
    ICING_ASSIGN_OR_RETURN(
        FileBackedVector<float>::MutableArrayView mutable_arr,
        fbv_ptr->Allocate(static_cast<int32_t>(dimension)));
    mutable_arr.SetArray(/*idx=*/0, embedding.float_vector,
                         static_cast<int32_t>(dimension));
  } else {
    ICING_ASSIGN_OR_RETURN(FileBackedVector<char> * fbv_ptr,
                           GetOrCreateQuantizedEmbeddingVector(shard_id));
    location = fbv_ptr->num_elements();
    ICING_ASSIGN_OR_RETURN(
        FileBackedVector<char>::MutableArrayView mutable_arr,
        fbv_ptr->Allocate(static_cast<int32_t>(sizeof(Quantizer) + dimension)));
    mutable_arr.SetArray(/*idx=*/0, embedding.quantized_vector,
                         static_cast<int32_t>(sizeof(Quantizer) + dimension));
  }
  return location;
}

libtextclassifier3::StatusOr<uint32_t> EmbeddingIndex::AppendEmbeddingVector(
    const PropertyProto::VectorProto& vector,
    EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
    uint32_t shard_id) {
  ICING_ASSIGN_OR_RETURN(uint32_t dimension,
                         embedding_util::GetDimension(vector));
  EmbeddingReference reference;
  std::vector<char> quantized_data;

  if (!vector.quantized_values().empty()) {
    reference.quantized_vector = vector.quantized_values().data();
  } else if (quantization_type ==
             EmbeddingIndexingConfig::QuantizationType::NONE) {
    reference.float_vector = vector.values().data();
  } else {
    ICING_ASSIGN_OR_RETURN(Quantizer quantizer, CreateQuantizer(vector));
    quantized_data.resize(sizeof(Quantizer) + dimension);
    memcpy(quantized_data.data(), &quantizer, sizeof(Quantizer));
    // Quantize the vector
    uint8_t* quantized_values =
        reinterpret_cast<uint8_t*>(quantized_data.data() + sizeof(Quantizer));
    for (int i = 0; i < dimension; ++i) {
      quantized_values[i] = quantizer.Quantize(vector.values(i));
    }
    reference.quantized_vector = quantized_data.data();
  }
  return AppendEmbeddingVector(reference, dimension, shard_id);
}

libtextclassifier3::Status EmbeddingIndex::BufferEmbedding(
    const BasicHit& basic_hit, const PropertyProto::VectorProto& vector,
    EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
    std::string_view schema_name) {
  absl_ports::unique_lock l(&mutex_);

  // Robustness check: return an error if quantization is not enabled but the
  // vector has quantized values.
  if (!vector.quantized_values().empty() &&
      quantization_type !=
          EmbeddingIndexingConfig::QuantizationType::QUANTIZE_8_BIT) {
    return absl_ports::InvalidArgumentError(
        "Property has 'quantized_values' set but schema quantization_type is "
        "not QUANTIZE_8_BIT.");
  }
  ICING_RETURN_IF_ERROR(MarkIndexNonEmpty());

  ICING_ASSIGN_OR_RETURN(std::string key,
                         embedding_util::GetPostingListKey(vector));
  uint32_t shard_id = GetShardId(embedding_util::GetPostingListKeyHash(key),
                                 SchemaStore::GetSchemaNameHash(schema_name));
  ICING_ASSIGN_OR_RETURN(
      uint32_t location,
      AppendEmbeddingVector(vector, quantization_type, shard_id));

  // Buffer the embedding hit.
  pending_embedding_hits_.push_back(
      {std::move(key), EmbeddingHit(basic_hit, location)});
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status EmbeddingIndex::BufferEmbeddingIvf(
    const BasicHit& basic_hit, const PropertyProto::VectorProto& vector,
    EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
    std::string_view schema_name) {
  absl_ports::unique_lock l(&mutex_);

  ICING_RETURN_IF_ERROR(MarkIndexNonEmpty());

  ICING_ASSIGN_OR_RETURN(IvfContextManager ivf_context,
                         IvfContextManager::Create(vector));

  ICING_ASSIGN_OR_RETURN(
      std::vector<uint32_t> cluster_ids,
      ivf_context.GetClosestClusterIdsByDistance(this, vector, /*k=*/1));

  std::string posting_list_key;
  if (cluster_ids.empty()) {
    // IVF not built yet. Buffer to delta store.
    posting_list_key =
        ivf_context.GetPostingListKey(embedding_util::kIvfDeltaStoreClusterId);
  } else {
    posting_list_key = ivf_context.GetPostingListKey(cluster_ids.front());
  }

  uint32_t shard_id =
      GetShardId(embedding_util::GetPostingListKeyHash(posting_list_key),
                 SchemaStore::GetSchemaNameHash(schema_name));
  ICING_ASSIGN_OR_RETURN(
      uint32_t location,
      AppendEmbeddingVector(vector, quantization_type, shard_id));

  // Buffer the embedding hit.
  pending_embedding_hits_.push_back(
      {std::move(posting_list_key), EmbeddingHit(basic_hit, location)});
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status EmbeddingIndex::CommitBufferToIndex() {
  absl_ports::unique_lock l(&mutex_);

  return CommitBufferToIndexLocked();
}

libtextclassifier3::Status EmbeddingIndex::CommitBufferToIndexLocked() {
  if (pending_embedding_hits_.empty()) {
    return libtextclassifier3::Status::OK;
  }
  ICING_RETURN_IF_ERROR(MarkIndexNonEmpty());

  std::sort(pending_embedding_hits_.begin(), pending_embedding_hits_.end());
  auto iter_curr_key = pending_embedding_hits_.rbegin();
  while (iter_curr_key != pending_embedding_hits_.rend()) {
    // In order to batch putting embedding hits with the same key (dimension,
    // model_signature) to the same posting list, we find the range
    // [iter_curr_key, iter_next_key) of embedding hits with the same key and
    // put them into their corresponding posting list together.
    auto iter_next_key = iter_curr_key;
    while (iter_next_key != pending_embedding_hits_.rend() &&
           iter_next_key->first == iter_curr_key->first) {
      iter_next_key++;
    }

    const std::string& key = iter_curr_key->first;
    libtextclassifier3::StatusOr<PostingListIdentifier> posting_list_id_or =
        embedding_posting_list_mapper_->Get(key);
    std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor;
    if (posting_list_id_or.ok()) {
      // Existing posting list.
      ICING_ASSIGN_OR_RETURN(
          pl_accessor,
          PostingListEmbeddingHitAccessor::CreateFromExisting(
              flash_index_storage_.get(), posting_list_hit_serializer_.get(),
              posting_list_id_or.ValueOrDie()));
    } else if (absl_ports::IsNotFound(posting_list_id_or.status())) {
      // New posting list.
      ICING_ASSIGN_OR_RETURN(
          pl_accessor,
          PostingListEmbeddingHitAccessor::Create(
              flash_index_storage_.get(), posting_list_hit_serializer_.get()));
    } else {
      // Errors
      return std::move(posting_list_id_or).status();
    }

    // Adding the embedding hits.
    uint32_t num_new_hits = 0;
    for (auto iter = iter_curr_key; iter != iter_next_key; ++iter) {
      ICING_RETURN_IF_ERROR(pl_accessor->PrependHit(iter->second));
      num_new_hits++;
    }

    // Update IVF metadata if this is an IVF key.
    ICING_ASSIGN_OR_RETURN(embedding_util::ParsedPostingListKey parsed_key,
                           embedding_util::ParsePostingListKey(key));
    if (parsed_key.cluster_id != embedding_util::kLinearSearchClusterId &&
        num_new_hits > 0) {
      IvfContextManager ivf_context(parsed_key.base_key);
      ICING_ASSIGN_OR_RETURN(IvfMetadata ivf_metadata,
                             ivf_context.GetMetadata(this));
      ivf_metadata.current_size += num_new_hits;
      ICING_RETURN_IF_ERROR(
          ivf_context.SetMetadata(this, std::move(ivf_metadata)));
    }

    // Finalize this posting list and add the posting list id in
    // embedding_posting_list_mapper_.
    PostingListEmbeddingHitAccessor::FinalizeResult result =
        std::move(*pl_accessor).Finalize();
    if (!result.id.is_valid()) {
      return absl_ports::InternalError("Failed to finalize posting list");
    }
    ICING_RETURN_IF_ERROR(embedding_posting_list_mapper_->Put(key, result.id));

    // Advance to the next key.
    iter_curr_key = iter_next_key;
  }
  pending_embedding_hits_.clear();
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<uint32_t> EmbeddingIndex::TransferEmbeddingVector(
    const EmbeddingHit& old_hit, uint32_t dimension,
    EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
    uint32_t shard_id, EmbeddingIndex* new_index) const {
  ICING_ASSIGN_OR_RETURN(
      EmbeddingReference embedding,
      GetEmbeddingReference(old_hit, dimension, quantization_type, shard_id));
  return new_index->AppendEmbeddingVector(embedding, dimension, shard_id);
}

libtextclassifier3::StatusOr<EmbeddingReference>
EmbeddingIndex::GetEmbeddingReference(
    const EmbeddingHit& hit, uint32_t dimension,
    EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
    uint32_t shard_id) const {
  EmbeddingReference embedding;
  if (quantization_type == EmbeddingIndexingConfig::QuantizationType::NONE) {
    ICING_ASSIGN_OR_RETURN(embedding.float_vector,
                           GetEmbeddingVector(hit, dimension, shard_id));
  } else {
    ICING_ASSIGN_OR_RETURN(
        embedding.quantized_vector,
        GetQuantizedEmbeddingVector(hit, dimension, shard_id));
  }
  return embedding;
}

libtextclassifier3::Status EmbeddingIndex::TransferIndex(
    const DocumentStore& document_store, const SchemaStore& schema_store,
    const std::vector<DocumentId>& document_id_old_to_new,
    EmbeddingIndex* new_index) const {
  if (is_empty()) {
    return absl_ports::FailedPreconditionError("EmbeddingIndex is empty");
  }

  const int64_t current_time_ms = clock_.GetSystemTimeMilliseconds();
  std::unique_ptr<KeyMapper<PostingListIdentifier>::Iterator> itr =
      embedding_posting_list_mapper_->GetIterator();
  while (itr->Advance()) {
    std::string_view key = itr->GetKey();
    uint32_t key_hash = embedding_util::GetPostingListKeyHash(key);
    // This should never happen unless there is an inconsistency, or the index
    // is corrupted.
    if (key.size() < embedding_util::kEncodedDimensionLength) {
      return absl_ports::InternalError(
          "Got invalid key from embedding posting list mapper.");
    }
    ICING_ASSIGN_OR_RETURN(embedding_util::ParsedPostingListKey parsed_key,
                           embedding_util::ParsePostingListKey(key));

    // Transfer hits
    std::vector<EmbeddingHit> new_hits;
    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<PostingListEmbeddingHitAccessor> old_pl_accessor,
        PostingListEmbeddingHitAccessor::CreateFromExisting(
            flash_index_storage_.get(), posting_list_hit_serializer_.get(),
            /*existing_posting_list_id=*/itr->GetValue()));
    DocumentId last_new_document_id = kInvalidDocumentId;
    SchemaTypeId schema_type_id = kInvalidSchemaTypeId;

    bool is_centroid =
        parsed_key.cluster_id == embedding_util::kIvfCentroidsClusterId;
    uint32_t shard_id = is_centroid
                            ? GetShardId(key_hash, kIvfCentroidsSchemaNameHash)
                            : kInvalidShardId;

    while (true) {
      ICING_ASSIGN_OR_RETURN(std::vector<EmbeddingHit> batch,
                             old_pl_accessor->GetNextHitsBatch());
      if (batch.empty()) {
        break;
      }
      for (EmbeddingHit& old_hit : batch) {
        if (is_centroid) {
          // Centroids are not associated with real documents. Transfer as-is.

          // Transfer centroids to the new index.
          ICING_RETURN_IF_ERROR(new_index->MarkIndexNonEmpty());
          ICING_ASSIGN_OR_RETURN(
              uint32_t new_location,
              TransferEmbeddingVector(
                  old_hit, parsed_key.dimension,
                  // Centroids are strictly unquantized
                  EmbeddingIndexingConfig::QuantizationType::NONE, shard_id,
                  new_index));
          new_hits.push_back(EmbeddingHit(old_hit.basic_hit(), new_location));
          continue;
        }

        // Now handle normal embeddings (linear search embeddings, delta store
        // or other clusters)

        // Safety checks to add robustness to the codebase, so to make sure
        // that we never access invalid memory, in case that hit from the
        // posting list is corrupted.
        if (old_hit.basic_hit().document_id() < 0 ||
            old_hit.basic_hit().document_id() >=
                document_id_old_to_new.size()) {
          return absl_ports::InternalError(
              "Embedding hit document id is out of bound. The provided map is "
              "too small, or the index may have been corrupted.");
        }

        // Construct transferred hit and add the embedding vector to the new
        // index.
        DocumentId new_document_id =
            document_id_old_to_new[old_hit.basic_hit().document_id()];
        if (new_document_id == kInvalidDocumentId) {
          continue;
        }
        if (new_document_id != last_new_document_id) {
          schema_type_id =
              document_store.GetSchemaTypeId(new_document_id, current_time_ms);
          libtextclassifier3::StatusOr<uint32_t> schema_name_hash_or =
              schema_store.GetSchemaNameHash(schema_type_id);
          // Shard id only depends on posting list key and schema name hash.
          // Posting list key will not change in this scope, so we only update
          // the shard id here for a new document, since it can have a
          // different schema type.
          if (schema_name_hash_or.ok()) {
            shard_id = GetShardId(key_hash, schema_name_hash_or.ValueOrDie());
          } else {
            shard_id = kInvalidShardId;
          }
        }
        last_new_document_id = new_document_id;
        if (schema_type_id == kInvalidSchemaTypeId ||
            shard_id == kInvalidShardId) {
          // This should not happen, since document store is optimized first,
          // so that new_document_id here should be alive.
          continue;
        }
        ICING_ASSIGN_OR_RETURN(
            EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
            schema_store.GetQuantizationType(schema_type_id,
                                             old_hit.basic_hit().section_id()));
        ICING_RETURN_IF_ERROR(new_index->MarkIndexNonEmpty());

        ICING_ASSIGN_OR_RETURN(
            uint32_t new_location,
            TransferEmbeddingVector(old_hit, parsed_key.dimension,
                                    quantization_type, shard_id, new_index));
        new_hits.push_back(EmbeddingHit(
            BasicHit(old_hit.basic_hit().section_id(), new_document_id),
            new_location));
      }
    }
    // No hit needs to be added to the new index.
    if (new_hits.empty()) {
      continue;
    }
    // Add transferred hits to the new index.
    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<PostingListEmbeddingHitAccessor> hit_accum,
        PostingListEmbeddingHitAccessor::Create(
            new_index->flash_index_storage_.get(),
            new_index->posting_list_hit_serializer_.get()));
    for (auto new_hit_itr = new_hits.rbegin(); new_hit_itr != new_hits.rend();
         ++new_hit_itr) {
      ICING_RETURN_IF_ERROR(hit_accum->PrependHit(*new_hit_itr));
    }
    PostingListEmbeddingHitAccessor::FinalizeResult result =
        std::move(*hit_accum).Finalize();
    if (!result.id.is_valid()) {
      return absl_ports::InternalError("Failed to finalize posting list");
    }
    ICING_RETURN_IF_ERROR(
        new_index->embedding_posting_list_mapper_->Put(key, result.id));

    if (parsed_key.cluster_id != embedding_util::kLinearSearchClusterId &&
        !is_centroid) {
      IvfContextManager ivf_context(parsed_key.base_key);
      ICING_ASSIGN_OR_RETURN(IvfMetadata metadata,
                             ivf_context.GetMetadata(new_index));
      metadata.current_size += new_hits.size();
      ICING_RETURN_IF_ERROR(
          ivf_context.SetMetadata(new_index, std::move(metadata)));
    }
  }

  // Transfer the remaining IvfMetadata (last_ivf_build_size, num_clusters), if
  // the new index is not empty.
  if (!new_index->is_empty()) {
    std::unique_ptr<KeyMapper<IvfMetadata>::Iterator> metadata_itr =
        ivf_metadata_mapper_->GetIterator();
    while (metadata_itr->Advance()) {
      IvfContextManager ivf_context(
          /*base_key=*/std::string{metadata_itr->GetKey()});
      IvfMetadata old_metadata = metadata_itr->GetValue();
      ICING_ASSIGN_OR_RETURN(IvfMetadata new_metadata,
                             ivf_context.GetMetadata(new_index));
      new_metadata.last_ivf_build_size = old_metadata.last_ivf_build_size;
      new_metadata.num_clusters = old_metadata.num_clusters;
      ICING_RETURN_IF_ERROR(
          ivf_context.SetMetadata(new_index, std::move(new_metadata)));
    }
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status EmbeddingIndex::Optimize(
    const DocumentStore* document_store, const SchemaStore* schema_store,
    const std::vector<DocumentId>& document_id_old_to_new,
    DocumentId new_last_added_document_id) {
  absl_ports::unique_lock l(&mutex_);

  ICING_RETURN_ERROR_IF_NULL(document_store);
  ICING_RETURN_ERROR_IF_NULL(schema_store);
  if (is_empty()) {
    info().last_added_document_id = new_last_added_document_id;
    return libtextclassifier3::Status::OK;
  }

  // This is just for completeness, but this should never be necessary, since we
  // should never have pending hits at the time when Optimize is run.
  ICING_RETURN_IF_ERROR(CommitBufferToIndexLocked());

  std::string temporary_index_working_path = working_path_ + "_temp";
  if (!filesystem_.DeleteDirectoryRecursively(
          temporary_index_working_path.c_str())) {
    ICING_LOG(ERROR) << "Recursively deleting " << temporary_index_working_path;
    return absl_ports::InternalError(
        "Unable to delete temp directory to prepare to build new index.");
  }

  DestructibleDirectory temporary_index_dir(
      &filesystem_, std::move(temporary_index_working_path));
  if (!temporary_index_dir.is_valid()) {
    return absl_ports::InternalError(
        "Unable to create temp directory to build new index.");
  }

  {
    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<EmbeddingIndex> new_index,
        EmbeddingIndex::Create(&filesystem_, temporary_index_dir.dir(), &clock_,
                               feature_flags_, num_shards_));
    {
      absl_ports::unique_lock new_index_lock(&new_index->mutex_);

      ICING_RETURN_IF_ERROR(TransferIndex(*document_store, *schema_store,
                                          document_id_old_to_new,
                                          new_index.get()));
    }
    new_index->set_last_added_document_id(new_last_added_document_id);
    ICING_RETURN_IF_ERROR(new_index->PersistToDisk());
  }

  // Destruct current storage instances to safely swap directories.
  metadata_mmapped_file_.reset();
  flash_index_storage_.reset();
  embedding_posting_list_mapper_.reset();
  ivf_metadata_mapper_.reset();
  for (int i = 0; i < num_shards_; ++i) {
    embedding_vectors_[i].reset();
    quantized_embedding_vectors_[i].reset();
  }

  if (!filesystem_.SwapFiles(temporary_index_dir.dir().c_str(),
                             working_path_.c_str())) {
    return absl_ports::InternalError(
        "Unable to apply new index due to failed swap!");
  }

  // Reinitialize the index.
  is_initialized_ = false;
  return Initialize();
}

libtextclassifier3::StatusOr<
    std::vector<EmbeddingIndex::EmbeddingHitAccessor::HitInfo>>
EmbeddingIndex::EmbeddingHitAccessor::GetNextHitsBatch() {
  // TODO(b/448886757): Consider passing in a single update vector for this
  // method, so that we don't need to alloc a new vector for every batch to
  // avoid memory churn.
  std::vector<HitInfo> result;
  if (accessors_.empty()) {
    return result;
  }

  // Special handling for the case of a single accessor to avoid the overhead of
  // the priority queue. This is also consistent with the old behavior that just
  // relies on the posting list accessor's GetNextHitsBatch.
  if (accessors_.size() == 1) {
    auto& accessor_data = accessors_[0];
    result.reserve(accessor_data.current_batch.size() -
                   accessor_data.current_index);
    for (uint32_t i = accessor_data.current_index;
         i < accessor_data.current_batch.size(); ++i) {
      result.push_back(HitInfo{accessor_data.current_batch[i],
                               accessor_data.posting_list_key_hash});
    }
    // Fetch the next batch for the next call.
    ICING_ASSIGN_OR_RETURN(accessor_data.current_batch,
                           accessor_data.pl_accessor->GetNextHitsBatch());
    accessor_data.current_index = 0;
    if (accessor_data.current_batch.empty()) {
      accessors_.clear();
    }
    return result;
  }

  result.reserve(kKWayMergeBatchSize);

  // Retrieve hits from multiple clusters in descending DocumentId order by
  // performing a k-way merge using a priority queue on the accessors.
  while (result.size() < kKWayMergeBatchSize && !accessors_.empty()) {
    std::pop_heap(accessors_.begin(), accessors_.end(),
                  std::greater<AccessorData>());
    AccessorData best_accessor_data = std::move(accessors_.back());
    accessors_.pop_back();

    const EmbeddingHit& best_hit =
        best_accessor_data.current_batch[best_accessor_data.current_index];

    result.push_back(
        HitInfo{best_hit, best_accessor_data.posting_list_key_hash});

    best_accessor_data.current_index++;
    ICING_RETURN_IF_ERROR(AddAccessor(std::move(best_accessor_data)));
  }

  return result;
}

libtextclassifier3::Status EmbeddingIndex::EmbeddingHitAccessor::AddAccessor(
    std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor,
    uint32_t posting_list_key_hash) {
  return AddAccessor(
      AccessorData{std::move(pl_accessor), posting_list_key_hash});
}

libtextclassifier3::Status EmbeddingIndex::EmbeddingHitAccessor::AddAccessor(
    AccessorData data) {
  // If the current batch is empty, fetch the next batch.
  if (data.current_index >= data.current_batch.size()) {
    ICING_ASSIGN_OR_RETURN(data.current_batch,
                           data.pl_accessor->GetNextHitsBatch());
    data.current_index = 0;
  }
  // If the current batch is not empty, add the accessor to the priority queue.
  // Otherwise, the accessor is exhausted and we don't need to add it.
  if (data.current_index < data.current_batch.size()) {
    accessors_.push_back(std::move(data));
    std::push_heap(accessors_.begin(), accessors_.end(),
                   std::greater<AccessorData>());
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<float>
EmbeddingIndex::EmbeddingHitAccessor::ScoreEmbeddingHit(
    const EmbeddingScorer& scorer, const std::vector<float>& query_floats,
    const HitInfo& hit_info,
    EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
    uint32_t schema_name_hash, bool is_ann) {
  if (is_ann) {
    ++embedding_stats_.num_ann_embeddings_scored;
  }
  uint32_t shard_id = embedding_index_.GetShardId(
      hit_info.posting_list_key_hash, schema_name_hash);
  int dimension = static_cast<int>(query_floats.size());
  float semantic_score;
  const EmbeddingHit& hit = hit_info.hit;
  if (quantization_type == EmbeddingIndexingConfig::QuantizationType::NONE) {
    ICING_ASSIGN_OR_RETURN(
        const float* vector,
        embedding_index_.GetEmbeddingVector(hit, dimension, shard_id));
    semantic_score = scorer.EigenScore(dimension,
                                       /*v1=*/query_floats.data(),
                                       /*v2=*/vector);
    ++embedding_stats_.num_unquantized_embeddings_scored;
    embedding_stats_.unquantized_shards_read.insert(shard_id);
    embedding_stats_.num_embedding_bytes_read +=
        static_cast<int64_t>(sizeof(float)) * dimension;
  } else {
    ICING_ASSIGN_OR_RETURN(
        const char* data,
        embedding_index_.GetQuantizedEmbeddingVector(hit, dimension, shard_id));
    Quantizer quantizer(data);
    const uint8_t* quantized_vector =
        reinterpret_cast<const uint8_t*>(data + sizeof(Quantizer));
    semantic_score = scorer.EigenScore(dimension,
                                       /*v1=*/query_floats.data(),
                                       /*v2=*/quantized_vector, quantizer);
    ++embedding_stats_.num_quantized_embeddings_scored;
    embedding_stats_.quantized_shards_read.insert(shard_id);
    embedding_stats_.num_embedding_bytes_read +=
        static_cast<int64_t>(sizeof(Quantizer)) +
        static_cast<int64_t>(sizeof(uint8_t)) * dimension;
  }
  return semantic_score;
}

libtextclassifier3::Status EmbeddingIndex::PersistMetadataToDisk() {
  return metadata_mmapped_file_->PersistToDisk();
}

libtextclassifier3::Status EmbeddingIndex::PersistStoragesToDisk() {
  if (is_empty()) {
    return libtextclassifier3::Status::OK;
  }
  if (!flash_index_storage_->PersistToDisk()) {
    return absl_ports::InternalError("Fail to persist flash index to disk");
  }
  ICING_RETURN_IF_ERROR(embedding_posting_list_mapper_->PersistToDisk());
  ICING_RETURN_IF_ERROR(ivf_metadata_mapper_->PersistToDisk());
  for (int i = 0; i < num_shards_; ++i) {
    if (embedding_vectors_[i] != nullptr) {
      ICING_RETURN_IF_ERROR(embedding_vectors_[i]->PersistToDisk());
    }
    if (quantized_embedding_vectors_[i] != nullptr) {
      ICING_RETURN_IF_ERROR(quantized_embedding_vectors_[i]->PersistToDisk());
    }
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<Crc32> EmbeddingIndex::UpdateStoragesChecksum() {
  if (is_empty()) {
    return Crc32(0);
  }
  ICING_ASSIGN_OR_RETURN(Crc32 embedding_posting_list_mapper_crc,
                         embedding_posting_list_mapper_->UpdateChecksum());
  uint32_t checksum = embedding_posting_list_mapper_crc.Get();

  ICING_ASSIGN_OR_RETURN(Crc32 ivf_metadata_mapper_crc,
                         ivf_metadata_mapper_->UpdateChecksum());
  checksum ^= ivf_metadata_mapper_crc.Get();

  for (int i = 0; i < num_shards_; ++i) {
    if (embedding_vectors_[i] != nullptr) {
      ICING_ASSIGN_OR_RETURN(Crc32 crc,
                             embedding_vectors_[i]->UpdateChecksum());
      checksum ^= crc.Get();
    }
    if (quantized_embedding_vectors_[i] != nullptr) {
      ICING_ASSIGN_OR_RETURN(Crc32 crc,
                             quantized_embedding_vectors_[i]->UpdateChecksum());
      checksum ^= crc.Get();
    }
  }
  return Crc32(checksum);
}

libtextclassifier3::StatusOr<Crc32> EmbeddingIndex::GetStoragesChecksum()
    const {
  if (is_empty()) {
    return Crc32(0);
  }
  ICING_ASSIGN_OR_RETURN(Crc32 embedding_posting_list_mapper_crc,
                         embedding_posting_list_mapper_->GetChecksum());
  uint32_t checksum = embedding_posting_list_mapper_crc.Get();

  ICING_ASSIGN_OR_RETURN(Crc32 ivf_metadata_mapper_crc,
                         ivf_metadata_mapper_->GetChecksum());
  checksum ^= ivf_metadata_mapper_crc.Get();

  for (int i = 0; i < num_shards_; ++i) {
    if (embedding_vectors_[i] != nullptr) {
      checksum ^= embedding_vectors_[i]->GetChecksum().Get();
    }
    if (quantized_embedding_vectors_[i] != nullptr) {
      checksum ^= quantized_embedding_vectors_[i]->GetChecksum().Get();
    }
  }
  return Crc32(checksum);
}

libtextclassifier3::Status EmbeddingIndex::PersistToDisk() {
  absl_ports::unique_lock l(&mutex_);

  return PersistentStorage::PersistToDisk();
}

libtextclassifier3::StatusOr<Crc32> EmbeddingIndex::UpdateChecksums() {
  absl_ports::unique_lock l(&mutex_);

  return PersistentStorage::UpdateChecksums();
}

libtextclassifier3::StatusOr<Crc32> EmbeddingIndex::GetChecksum() const {
  absl_ports::shared_lock l(&mutex_);

  return PersistentStorage::GetChecksum();
}

libtextclassifier3::StatusOr<EmbeddingIndex::ExtractedEmbeddings>
EmbeddingIndex::RetrieveAllEmbeddings(
    const DocumentStore& document_store, const SchemaStore& schema_store,
    const std::vector<std::string>& cluster_keys_to_read, uint32_t dimension,
    uint32_t reserve_size) {
  ExtractedEmbeddings extracted_embeddings;
  if (reserve_size > 0) {
    extracted_embeddings.hits.reserve(reserve_size);
    extracted_embeddings.embeddings.reserve(reserve_size);
    extracted_embeddings.schema_name_hashes.reserve(reserve_size);
    extracted_embeddings.shard_ids.reserve(reserve_size);
  }
  const int64_t current_time_ms = clock_.GetSystemTimeMilliseconds();
  for (const std::string& cluster_key : cluster_keys_to_read) {
    uint32_t pl_key_hash = embedding_util::GetPostingListKeyHash(cluster_key);
    libtextclassifier3::StatusOr<PostingListIdentifier> pl_id_or =
        embedding_posting_list_mapper_->Get(cluster_key);
    if (!pl_id_or.ok()) {
      continue;
    }

    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<PostingListEmbeddingHitAccessor> accessor,
        PostingListEmbeddingHitAccessor::CreateFromExisting(
            flash_index_storage_.get(), posting_list_hit_serializer_.get(),
            pl_id_or.ValueOrDie()));
    while (true) {
      ICING_ASSIGN_OR_RETURN(std::vector<EmbeddingHit> batch,
                             accessor->GetNextHitsBatch());
      if (batch.empty()) {
        break;
      }
      DocumentId document_id = kInvalidDocumentId;
      SchemaTypeId schema_type_id = kInvalidSchemaTypeId;
      uint32_t schema_name_hash = 0;
      uint32_t shard_id = kInvalidShardId;
      for (const EmbeddingHit& hit : batch) {
        if (hit.basic_hit().document_id() != document_id) {
          document_id = hit.basic_hit().document_id();
          schema_type_id =
              document_store.GetSchemaTypeId(document_id, current_time_ms);
          libtextclassifier3::StatusOr<uint32_t> schema_name_hash_or =
              schema_store.GetSchemaNameHash(schema_type_id);
          if (schema_name_hash_or.ok()) {
            schema_name_hash = schema_name_hash_or.ValueOrDie();
            shard_id = GetShardId(pl_key_hash, schema_name_hash);
          } else {
            schema_name_hash = 0;
            shard_id = kInvalidShardId;
          }
        }
        if (schema_type_id == kInvalidSchemaTypeId ||
            shard_id == kInvalidShardId) {
          continue;
        }
        ICING_ASSIGN_OR_RETURN(
            EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
            schema_store.GetQuantizationType(schema_type_id,
                                             hit.basic_hit().section_id()));
        ICING_ASSIGN_OR_RETURN(
            EmbeddingReference embedding_ref,
            GetEmbeddingReference(hit, dimension, quantization_type, shard_id));
        extracted_embeddings.hits.push_back(hit);
        extracted_embeddings.embeddings.push_back(embedding_ref);
        extracted_embeddings.schema_name_hashes.push_back(schema_name_hash);
        extracted_embeddings.shard_ids.push_back(shard_id);
      }
    }
  }
  return extracted_embeddings;
}

libtextclassifier3::Status EmbeddingIndex::WriteCentroids(
    IvfContextManager& ivf_context,
    const std::vector<std::vector<float>>& centroids) {
  if (centroids.empty()) {
    return absl_ports::InternalError("Centroids are empty.");
  }

  // Save (or override) centroids to centroid posting list
  std::string centroid_key =
      ivf_context.GetPostingListKey(embedding_util::kIvfCentroidsClusterId);
  uint32_t centroid_shard_id =
      GetShardId(embedding_util::GetPostingListKeyHash(centroid_key),
                 kIvfCentroidsSchemaNameHash);
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<PostingListEmbeddingHitAccessor> centroid_accum,
      PostingListEmbeddingHitAccessor::Create(
          flash_index_storage_.get(), posting_list_hit_serializer_.get()));
  ICING_ASSIGN_OR_RETURN(FileBackedVector<float> * centroid_fbv_ptr,
                         GetOrCreateEmbeddingVector(centroid_shard_id));
  std::vector<EmbeddingHit> centroid_hits;
  centroid_hits.reserve(centroids.size());
  for (int i = 0; i < centroids.size(); ++i) {
    if (centroids[i].size() != ivf_context.dimension()) {
      return absl_ports::InternalError("Centroid dimension mismatch");
    }

    // Dummy section ID 0, dummy document ID 0 for centroid hits
    EmbeddingHit centroid_hit(BasicHit(/*section_id=*/0, /*document_id=*/0),
                              centroid_fbv_ptr->num_elements());
    ICING_ASSIGN_OR_RETURN(
        FileBackedVector<float>::MutableArrayView mutable_arr,
        centroid_fbv_ptr->Allocate(ivf_context.dimension()));
    mutable_arr.SetArray(/*idx=*/0, centroids[i].data(),
                         ivf_context.dimension());
    centroid_hits.push_back(centroid_hit);
  }
  for (auto it = centroid_hits.rbegin(); it != centroid_hits.rend(); ++it) {
    ICING_RETURN_IF_ERROR(centroid_accum->PrependHit(*it));
  }
  PostingListEmbeddingHitAccessor::FinalizeResult centroid_result =
      std::move(*centroid_accum).Finalize();
  if (!centroid_result.id.is_valid()) {
    return absl_ports::InternalError(
        "Failed to finalize centroid posting list");
  }
  return embedding_posting_list_mapper_->Put(centroid_key, centroid_result.id);
}

libtextclassifier3::Status EmbeddingIndex::TransferEmbeddingsToNewClusters(
    const IvfContextManager& ivf_context,
    const MiniBatchKMeans::ClusteringResult& result,
    const ExtractedEmbeddings& extracted_embeddings) {
  uint32_t dimension = ivf_context.dimension();

  // Using fixed size local buffers to buffer the embedding reference vector's
  // data. Because AppendEmbeddingVector() may trigger FileBackedVector remap
  // internally to expand the file, any raw pointers sourced before would
  // instantly become invalid Dangling Pointers causing Segfaults or data
  // corruption.
  std::vector<float> float_vector_buffer(dimension);
  std::vector<char> quantized_vector_buffer(sizeof(Quantizer) + dimension);

  // Move embeddings to the new clusters.
  uint32_t num_clusters = result.centroids.size();
  std::vector<std::vector<int>> clustered_embedding_indices(num_clusters);
  for (size_t i = 0; i < extracted_embeddings.embeddings.size(); ++i) {
    if (result.partition_assignments[i] >= num_clusters) {
      return absl_ports::InternalError("Invalid partition assignment");
    }
    clustered_embedding_indices[result.partition_assignments[i]].push_back(i);
  }
  for (size_t c = 0; c < clustered_embedding_indices.size(); ++c) {
    std::string pl_key =
        ivf_context.GetPostingListKey(embedding_util::kIvfBaseClusterId + c);
    uint32_t pl_hash = embedding_util::GetPostingListKeyHash(pl_key);

    std::vector<int>& indices = clustered_embedding_indices[c];
    std::sort(
        indices.begin(), indices.end(), [&extracted_embeddings](int i, int j) {
          return extracted_embeddings.hits[i] < extracted_embeddings.hits[j];
        });

    std::vector<EmbeddingHit> cluster_new_hits;
    cluster_new_hits.reserve(indices.size());
    for (int index : indices) {
      EmbeddingHit old_hit = extracted_embeddings.hits[index];
      uint32_t new_shard_id =
          GetShardId(pl_hash, extracted_embeddings.schema_name_hashes[index]);

      EmbeddingReference new_embedding_ref;
      if (extracted_embeddings.embeddings[index].float_vector != nullptr) {
        ICING_ASSIGN_OR_RETURN(
            const float* vector,
            GetEmbeddingVector(old_hit, dimension,
                               extracted_embeddings.shard_ids[index]));
        std::copy(vector, vector + dimension, float_vector_buffer.begin());
        new_embedding_ref.float_vector = float_vector_buffer.data();
      } else {
        ICING_ASSIGN_OR_RETURN(
            const char* vector,
            GetQuantizedEmbeddingVector(old_hit, dimension,
                                        extracted_embeddings.shard_ids[index]));
        std::copy(vector, vector + sizeof(Quantizer) + dimension,
                  quantized_vector_buffer.begin());
        new_embedding_ref.quantized_vector = quantized_vector_buffer.data();
      }

      // Copy the embedding vector of the hit to the new location.
      ICING_ASSIGN_OR_RETURN(
          uint32_t new_location,
          AppendEmbeddingVector(new_embedding_ref, dimension, new_shard_id));
      cluster_new_hits.push_back(
          EmbeddingHit(old_hit.basic_hit(), new_location));
    }

    if (cluster_new_hits.empty()) {
      continue;
    }

    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<PostingListEmbeddingHitAccessor> cluster_accum,
        PostingListEmbeddingHitAccessor::Create(
            flash_index_storage_.get(), posting_list_hit_serializer_.get()));
    for (auto it = cluster_new_hits.rbegin(); it != cluster_new_hits.rend();
         ++it) {
      ICING_RETURN_IF_ERROR(cluster_accum->PrependHit(*it));
    }
    PostingListEmbeddingHitAccessor::FinalizeResult cluster_result =
        std::move(*cluster_accum).Finalize();
    if (!cluster_result.id.is_valid()) {
      return absl_ports::InternalError(
          "Failed to finalize cluster posting list");
    }
    ICING_RETURN_IF_ERROR(
        embedding_posting_list_mapper_->Put(pl_key, cluster_result.id));
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<int> EmbeddingIndex::MaintainAllIvf(
    const DocumentStore& document_store, const SchemaStore& schema_store,
    const MaintainAnnIndexOptions& maintain_ann_index_options) {
  if (is_maintenance_running_.exchange(true)) {
    // If a maintenance task is already running, this new request can just
    // return early.
    return 0;
  }
  struct MaintenanceGuard {
    std::atomic<bool>& is_running;
    ~MaintenanceGuard() { is_running.store(false); }
  } guard{is_maintenance_running_};

  std::vector<std::string> base_keys;
  {
    absl_ports::shared_lock l(&mutex_);
    if (is_empty()) {
      return 0;
    }
    std::unique_ptr<KeyMapper<IvfMetadata>::Iterator> itr =
        ivf_metadata_mapper_->GetIterator();
    while (itr->Advance()) {
      base_keys.push_back(std::string(itr->GetKey()));
    }
  }

  int total_iterations = 0;
  for (const std::string& base_key : base_keys) {
    IvfContextManager ivf_context(base_key);
    // NOTE: Across iterations, mutex_ is repeatedly acquired and released by
    // each MaintainIvf() call. Consequently, a concurrent query could execute
    // between MaintainIvf() calls for different base_keys.
    //
    // This interleaving is completely safe:
    // 1. All write operations use a unique_lock, so a query will never observe
    //    an inconsistent state mid-update during a MaintainIvf() write-back,
    //    nor will a MaintainIvf() write-back interrupt an ongoing query.
    // 2. Each base_key represents an independent embedding corpus. Updating
    //    one corpus does not affect the correctness of another, so executing a
    //    query between the maintenance of two different corpora is safe.
    ICING_ASSIGN_OR_RETURN(
        int iterations, MaintainIvf(ivf_context, document_store, schema_store,
                                    maintain_ann_index_options));
    total_iterations += iterations;
  }
  return total_iterations;
}

libtextclassifier3::StatusOr<int> EmbeddingIndex::MaintainIvf(
    IvfContextManager ivf_context, const DocumentStore& document_store,
    const SchemaStore& schema_store,
    const MaintainAnnIndexOptions& maintain_ann_index_options) {
  uint32_t dimension = ivf_context.dimension();
  IvfMetadata ivf_metadata;
  ExtractedEmbeddings extracted_embeddings;
  std::vector<std::string> cluster_keys_to_read;

  {
    absl_ports::shared_lock l(&mutex_);
    ICING_ASSIGN_OR_RETURN(ivf_metadata, ivf_context.GetMetadata(this));

    // If k-means has never been run previously, check the delta store size,
    // which is equal to current_size for this case.
    bool delta_store_exceed_threshold =
        ivf_metadata.last_ivf_build_size == 0 &&
        ivf_metadata.current_size >=
            maintain_ann_index_options.min_size_for_ivf();
    // If ivf has been built, we check if current_size exceeds a certain
    // percentage of the last ivf build size.
    bool ivf_grow_exceed_threshold =
        ivf_metadata.last_ivf_build_size > 0 &&
        static_cast<float>(ivf_metadata.current_size) >=
            static_cast<float>(ivf_metadata.last_ivf_build_size) *
                (1.0f + maintain_ann_index_options.rebuild_threshold());
    // Do not need to build or rebuild if neither condition is met.
    if (!delta_store_exceed_threshold && !ivf_grow_exceed_threshold) {
      return 0;
    }

    // We are going to rebuild, we have to read everything.
    // Determine clusters to read from
    cluster_keys_to_read.reserve(1 + ivf_metadata.num_clusters);
    cluster_keys_to_read.push_back(
        ivf_context.GetPostingListKey(embedding_util::kIvfDeltaStoreClusterId));
    for (uint32_t c = 0; c < ivf_metadata.num_clusters; ++c) {
      cluster_keys_to_read.push_back(
          ivf_context.GetPostingListKey(embedding_util::kIvfBaseClusterId + c));
    }

    // Retrieve all embeddings with reference.
    ICING_ASSIGN_OR_RETURN(
        extracted_embeddings,
        RetrieveAllEmbeddings(document_store, schema_store,
                              cluster_keys_to_read, dimension,
                              /*reserve_size=*/ivf_metadata.current_size));
    if (extracted_embeddings.embeddings.empty()) {
      return 0;
    }
  }

  // NOTE: There is an unlocked gap between releasing the shared_lock above and
  // acquiring the unique_lock below for the write-back phase.
  //
  // This gap is thread-compatible, but NOT thread-safe on EmbeddingIndex alone:
  // if another write occurs during this gap, it would cause a race condition
  // because MiniBatchKMeans::Compute operates on EmbeddingReferences pointing
  // to the underlying storage without holding any lock. A concurrent write
  // during Compute() would lead to undefined behavior, while a write after
  // Compute() would cause the newly computed centroids to be stale.
  //
  // In practice, this is safe because the top-level IcingSearchEngine holds a
  // shared lock across MaintainAnnIndex, preventing any concurrent writes
  // from happening during this gap.

  ICING_ASSIGN_OR_RETURN(
      MiniBatchKMeans::ClusteringResult result,
      MiniBatchKMeans::Compute(
          extracted_embeddings.embeddings, dimension,
          maintain_ann_index_options.mini_batch_k_means_options(), &clock_));

  // Write-back phase: UNIQUE LOCK!
  {
    absl_ports::unique_lock l(&mutex_);
    ivf_metadata.last_ivf_build_size = extracted_embeddings.embeddings.size();
    ivf_metadata.current_size = extracted_embeddings.embeddings.size();
    ivf_metadata.num_clusters = static_cast<uint32_t>(result.centroids.size());

    ICING_RETURN_IF_ERROR(WriteCentroids(ivf_context, result.centroids));

    // Delete the entry for the old delta store and clusters.
    for (const std::string& cluster_key : cluster_keys_to_read) {
      ICING_RETURN_IF_ERROR(
          TryDelete(embedding_posting_list_mapper_.get(), cluster_key));
    }

    ICING_RETURN_IF_ERROR(TransferEmbeddingsToNewClusters(
        ivf_context, result, extracted_embeddings));

    ICING_RETURN_IF_ERROR(ivf_context.SetMetadata(this, ivf_metadata));
  }
  return result.actual_iterations;
}

}  // namespace lib
}  // namespace icing
