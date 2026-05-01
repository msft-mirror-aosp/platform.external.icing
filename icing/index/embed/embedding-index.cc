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
#include <cstdint>
#include <cstring>
#include <limits>
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
#include "icing/file/destructible-directory.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/file/posting_list/flash-index-storage.h"
#include "icing/file/posting_list/posting-list-identifier.h"
#include "icing/index/embed/embedding-hit.h"
#include "icing/index/embed/embedding-reference.h"
#include "icing/index/embed/embedding-scorer.h"
#include "icing/index/embed/posting-list-embedding-hit-accessor.h"
#include "icing/index/embed/quantizer.h"
#include "icing/index/hit/hit.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/store/dynamic-trie-key-mapper.h"
#include "icing/store/key-mapper.h"
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

constexpr uint32_t kInvalidShardId = std::numeric_limits<uint32_t>::max();

std::string GetMetadataFilePath(std::string_view working_path) {
  return absl_ports::StrCat(working_path, "/metadata");
}

std::string GetFlashIndexStorageFilePath(std::string_view working_path) {
  return absl_ports::StrCat(working_path, "/flash_index_storage");
}

std::string GetEmbeddingHitListMapperPath(std::string_view working_path) {
  return absl_ports::StrCat(working_path, "/embedding_hit_list_mapper");
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
  if (vector.values().empty()) {
    return absl_ports::InvalidArgumentError("Vector dimension is 0");
  }
  auto minmax_pair =
      std::minmax_element(vector.values().begin(), vector.values().end());
  return Quantizer::Create(*minmax_pair.first, *minmax_pair.second);
}

}  // namespace

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
  ICING_RETURN_IF_ERROR(index->Initialize());
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
  pending_embedding_hits_.clear();
  metadata_mmapped_file_.reset();
  flash_index_storage_.reset();
  embedding_posting_list_mapper_.reset();
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
                            std::string_view model_signature) const {
  if (dimension == 0) {
    return absl_ports::InvalidArgumentError("Dimension is 0");
  }
  if (is_empty()) {
    return absl_ports::NotFoundError("EmbeddingIndex is empty");
  }

  std::string key =
      embedding_util::GetPostingListKey(dimension, model_signature);
  ICING_ASSIGN_OR_RETURN(PostingListIdentifier posting_list_id,
                         embedding_posting_list_mapper_->Get(key));
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<PostingListEmbeddingHitAccessor> pl_accessor,
      PostingListEmbeddingHitAccessor::CreateFromExisting(
          flash_index_storage_.get(), posting_list_hit_serializer_.get(),
          posting_list_id));
  return std::make_unique<EmbeddingHitAccessor>(this, std::move(pl_accessor),
                                                key);
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
  uint32_t dimension = vector.values().size();
  EmbeddingReference reference;
  std::vector<char> quantized_data;
  if (quantization_type == EmbeddingIndexingConfig::QuantizationType::NONE) {
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
  if (vector.values().empty()) {
    return absl_ports::InvalidArgumentError("Vector dimension is 0");
  }
  ICING_RETURN_IF_ERROR(MarkIndexNonEmpty());

  std::string key = embedding_util::GetPostingListKey(vector);
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

libtextclassifier3::Status EmbeddingIndex::CommitBufferToIndex() {
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
    for (auto iter = iter_curr_key; iter != iter_next_key; ++iter) {
      ICING_RETURN_IF_ERROR(pl_accessor->PrependHit(iter->second));
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
  EmbeddingReference embedding;
  if (quantization_type == EmbeddingIndexingConfig::QuantizationType::NONE) {
    ICING_ASSIGN_OR_RETURN(embedding.float_vector,
                           GetEmbeddingVector(old_hit, dimension, shard_id));
  } else {
    ICING_ASSIGN_OR_RETURN(
        embedding.quantized_vector,
        GetQuantizedEmbeddingVector(old_hit, dimension, shard_id));
  }
  return new_index->AppendEmbeddingVector(embedding, dimension, shard_id);
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
    uint32_t shard_id = kInvalidShardId;
    while (true) {
      ICING_ASSIGN_OR_RETURN(std::vector<EmbeddingHit> batch,
                             old_pl_accessor->GetNextHitsBatch());
      if (batch.empty()) {
        break;
      }
      for (EmbeddingHit& old_hit : batch) {
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
            shard_id = GetShardId(embedding_util::GetPostingListKeyHash(key),
                                  schema_name_hash_or.ValueOrDie());
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
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status EmbeddingIndex::Optimize(
    const DocumentStore* document_store, const SchemaStore* schema_store,
    const std::vector<DocumentId>& document_id_old_to_new,
    DocumentId new_last_added_document_id) {
  ICING_RETURN_ERROR_IF_NULL(document_store);
  ICING_RETURN_ERROR_IF_NULL(schema_store);
  if (is_empty()) {
    info().last_added_document_id = new_last_added_document_id;
    return libtextclassifier3::Status::OK;
  }

  // This is just for completeness, but this should never be necessary, since we
  // should never have pending hits at the time when Optimize is run.
  ICING_RETURN_IF_ERROR(CommitBufferToIndex());

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
    ICING_RETURN_IF_ERROR(TransferIndex(*document_store, *schema_store,
                                        document_id_old_to_new,
                                        new_index.get()));
    new_index->set_last_added_document_id(new_last_added_document_id);
    ICING_RETURN_IF_ERROR(new_index->PersistToDisk());
  }

  // Destruct current storage instances to safely swap directories.
  metadata_mmapped_file_.reset();
  flash_index_storage_.reset();
  embedding_posting_list_mapper_.reset();
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

libtextclassifier3::StatusOr<float>
EmbeddingIndex::EmbeddingHitAccessor::ScoreEmbeddingHit(
    const EmbeddingScorer& scorer, const PropertyProto::VectorProto& query,
    const EmbeddingHit& hit,
    EmbeddingIndexingConfig::QuantizationType::Code quantization_type,
    uint32_t schema_name_hash) {
  uint32_t shard_id =
      embedding_index_.GetShardId(posting_list_key_hash_, schema_name_hash);
  int dimension = query.values().size();
  float semantic_score;
  if (quantization_type == EmbeddingIndexingConfig::QuantizationType::NONE) {
    ICING_ASSIGN_OR_RETURN(
        const float* vector,
        embedding_index_.GetEmbeddingVector(hit, dimension, shard_id));
    semantic_score = scorer.EigenScore(dimension,
                                       /*v1=*/query.values().data(),
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
                                       /*v1=*/query.values().data(),
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

}  // namespace lib
}  // namespace icing
