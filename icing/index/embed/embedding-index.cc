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
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/file/destructible-directory.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/file/posting_list/flash-index-storage.h"
#include "icing/file/posting_list/posting-list-identifier.h"
#include "icing/index/embed/embedding-hit.h"
#include "icing/index/embed/posting-list-embedding-hit-accessor.h"
#include "icing/index/hit/hit.h"
#include "icing/store/document-id.h"
#include "icing/store/dynamic-trie-key-mapper.h"
#include "icing/store/key-mapper.h"
#include "icing/util/crc32.h"
#include "icing/util/encode-util.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

constexpr uint32_t kEmbeddingHitListMapperMaxSize =
    128 * 1024 * 1024;  // 128 MiB;

// The maximum length returned by encode_util::EncodeIntToCString is 5 for
// uint32_t.
constexpr uint32_t kEncodedDimensionLength = 5;

std::string GetMetadataFilePath(std::string_view working_path) {
  return absl_ports::StrCat(working_path, "/metadata");
}

std::string GetFlashIndexStorageFilePath(std::string_view working_path) {
  return absl_ports::StrCat(working_path, "/flash_index_storage");
}

std::string GetEmbeddingHitListMapperPath(std::string_view working_path) {
  return absl_ports::StrCat(working_path, "/embedding_hit_list_mapper");
}

std::string GetEmbeddingVectorsFilePath(std::string_view working_path) {
  return absl_ports::StrCat(working_path, "/embedding_vectors");
}

// An injective function that maps the ordered pair (dimension, model_signature)
// to a string, which is used to form a key for embedding_posting_list_mapper_.
std::string GetPostingListKey(uint32_t dimension,
                              std::string_view model_signature) {
  std::string encoded_dimension_str =
      encode_util::EncodeIntToCString(dimension);
  // Make encoded_dimension_str to fixed kEncodedDimensionLength bytes.
  while (encoded_dimension_str.size() < kEncodedDimensionLength) {
    // C string cannot contain 0 bytes, so we append it using 1, just like what
    // we do in encode_util::EncodeIntToCString.
    //
    // The reason that this works is because DecodeIntToString decodes a byte
    // value of 0x01 as 0x00. When EncodeIntToCString returns an encoded
    // dimension that is less than 5 bytes, it means that the dimension contains
    // unencoded leading 0x00. So here we're explicitly encoding those bytes as
    // 0x01.
    encoded_dimension_str.push_back(1);
  }
  return absl_ports::StrCat(encoded_dimension_str, model_signature);
}

std::string GetPostingListKey(const PropertyProto::VectorProto& vector) {
  return GetPostingListKey(vector.values_size(), vector.model_signature());
}

}  // namespace

libtextclassifier3::StatusOr<std::unique_ptr<EmbeddingIndex>>
EmbeddingIndex::Create(const Filesystem* filesystem, std::string working_path) {
  ICING_RETURN_ERROR_IF_NULL(filesystem);

  std::unique_ptr<EmbeddingIndex> index = std::unique_ptr<EmbeddingIndex>(
      new EmbeddingIndex(*filesystem, std::move(working_path)));
  ICING_RETURN_IF_ERROR(index->Initialize());
  return index;
}

libtextclassifier3::Status EmbeddingIndex::CreateStorageDataIfNonEmpty() {
  if (is_empty()) {
    return libtextclassifier3::Status::OK;
  }

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
      embedding_vectors_,
      FileBackedVector<float>::Create(
          filesystem_, GetEmbeddingVectorsFilePath(working_path_),
          MemoryMappedFile::READ_WRITE_AUTO_SYNC));

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status EmbeddingIndex::MarkIndexNonEmpty() {
  if (!is_empty()) {
    return libtextclassifier3::Status::OK;
  }
  info().is_empty = false;
  return CreateStorageDataIfNonEmpty();
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
    memset(Info().padding_, 0, Info::kPaddingSize);
    ICING_RETURN_IF_ERROR(InitializeNewStorage());
  } else {
    if (metadata_mmapped_file_->available_size() != kMetadataFileSize) {
      return absl_ports::FailedPreconditionError(
          "Incorrect metadata file size");
    }
    if (info().magic != Info::kMagic) {
      return absl_ports::FailedPreconditionError("Incorrect magic value");
    }
    ICING_RETURN_IF_ERROR(CreateStorageDataIfNonEmpty());
    ICING_RETURN_IF_ERROR(InitializeExistingStorage());
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status EmbeddingIndex::Clear() {
  pending_embedding_hits_.clear();
  metadata_mmapped_file_.reset();
  flash_index_storage_.reset();
  embedding_posting_list_mapper_.reset();
  embedding_vectors_.reset();
  if (filesystem_.DirectoryExists(working_path_.c_str())) {
    ICING_RETURN_IF_ERROR(Discard(filesystem_, working_path_));
  }
  is_initialized_ = false;
  return Initialize();
}

libtextclassifier3::StatusOr<std::unique_ptr<PostingListEmbeddingHitAccessor>>
EmbeddingIndex::GetAccessor(uint32_t dimension,
                            std::string_view model_signature) const {
  if (dimension == 0) {
    return absl_ports::InvalidArgumentError("Dimension is 0");
  }
  if (is_empty()) {
    return absl_ports::NotFoundError("EmbeddingIndex is empty");
  }

  std::string key = GetPostingListKey(dimension, model_signature);
  ICING_ASSIGN_OR_RETURN(PostingListIdentifier posting_list_id,
                         embedding_posting_list_mapper_->Get(key));
  return PostingListEmbeddingHitAccessor::CreateFromExisting(
      flash_index_storage_.get(), posting_list_hit_serializer_.get(),
      posting_list_id);
}

libtextclassifier3::Status EmbeddingIndex::BufferEmbedding(
    const BasicHit& basic_hit, const PropertyProto::VectorProto& vector) {
  if (vector.values_size() == 0) {
    return absl_ports::InvalidArgumentError("Vector dimension is 0");
  }
  ICING_RETURN_IF_ERROR(MarkIndexNonEmpty());

  uint32_t location = embedding_vectors_->num_elements();
  uint32_t dimension = vector.values_size();
  std::string key = GetPostingListKey(vector);

  // Buffer the embedding hit.
  pending_embedding_hits_.push_back(
      {std::move(key), EmbeddingHit(basic_hit, location)});

  // Put vector
  ICING_ASSIGN_OR_RETURN(FileBackedVector<float>::MutableArrayView mutable_arr,
                         embedding_vectors_->Allocate(dimension));
  mutable_arr.SetArray(/*idx=*/0, vector.values().data(), dimension);

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

libtextclassifier3::Status EmbeddingIndex::TransferIndex(
    const std::vector<DocumentId>& document_id_old_to_new,
    EmbeddingIndex* new_index) const {
  if (is_empty()) {
    return absl_ports::FailedPreconditionError("EmbeddingIndex is empty");
  }

  std::unique_ptr<KeyMapper<PostingListIdentifier>::Iterator> itr =
      embedding_posting_list_mapper_->GetIterator();
  while (itr->Advance()) {
    std::string_view key = itr->GetKey();
    // This should never happen unless there is an inconsistency, or the index
    // is corrupted.
    if (key.size() < kEncodedDimensionLength) {
      return absl_ports::InternalError(
          "Got invalid key from embedding posting list mapper.");
    }
    uint32_t dimension = encode_util::DecodeIntFromCString(
        std::string_view(key.begin(), kEncodedDimensionLength));

    // Transfer hits
    std::vector<EmbeddingHit> new_hits;
    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<PostingListEmbeddingHitAccessor> old_pl_accessor,
        PostingListEmbeddingHitAccessor::CreateFromExisting(
            flash_index_storage_.get(), posting_list_hit_serializer_.get(),
            /*existing_posting_list_id=*/itr->GetValue()));
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
        ICING_ASSIGN_OR_RETURN(const float* old_vector,
                               GetEmbeddingVector(old_hit, dimension));
        if (old_hit.basic_hit().document_id() < 0 ||
            old_hit.basic_hit().document_id() >=
                document_id_old_to_new.size()) {
          return absl_ports::InternalError(
              "Embedding hit document id is out of bound. The provided map is "
              "too small, or the index may have been corrupted.");
        }

        // Construct transferred hit
        DocumentId new_document_id =
            document_id_old_to_new[old_hit.basic_hit().document_id()];
        if (new_document_id == kInvalidDocumentId) {
          continue;
        }
        ICING_RETURN_IF_ERROR(new_index->MarkIndexNonEmpty());
        uint32_t new_location = new_index->embedding_vectors_->num_elements();
        new_hits.push_back(EmbeddingHit(
            BasicHit(old_hit.basic_hit().section_id(), new_document_id),
            new_location));

        // Copy the embedding vector of the hit to the new index.
        ICING_ASSIGN_OR_RETURN(
            FileBackedVector<float>::MutableArrayView mutable_arr,
            new_index->embedding_vectors_->Allocate(dimension));
        mutable_arr.SetArray(/*idx=*/0, old_vector, dimension);
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
    const std::vector<DocumentId>& document_id_old_to_new,
    DocumentId new_last_added_document_id) {
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
        EmbeddingIndex::Create(&filesystem_, temporary_index_dir.dir()));
    ICING_RETURN_IF_ERROR(
        TransferIndex(document_id_old_to_new, new_index.get()));
    new_index->set_last_added_document_id(new_last_added_document_id);
    ICING_RETURN_IF_ERROR(new_index->PersistToDisk());
  }

  // Destruct current storage instances to safely swap directories.
  metadata_mmapped_file_.reset();
  flash_index_storage_.reset();
  embedding_posting_list_mapper_.reset();
  embedding_vectors_.reset();

  if (!filesystem_.SwapFiles(temporary_index_dir.dir().c_str(),
                             working_path_.c_str())) {
    return absl_ports::InternalError(
        "Unable to apply new index due to failed swap!");
  }

  // Reinitialize the index.
  is_initialized_ = false;
  return Initialize();
}

libtextclassifier3::Status EmbeddingIndex::PersistMetadataToDisk(bool force) {
  return metadata_mmapped_file_->PersistToDisk();
}

libtextclassifier3::Status EmbeddingIndex::PersistStoragesToDisk(bool force) {
  if (is_empty()) {
    return libtextclassifier3::Status::OK;
  }

  if (!flash_index_storage_->PersistToDisk()) {
    return absl_ports::InternalError("Fail to persist flash index to disk");
  }
  ICING_RETURN_IF_ERROR(embedding_posting_list_mapper_->PersistToDisk());
  ICING_RETURN_IF_ERROR(embedding_vectors_->PersistToDisk());
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<Crc32> EmbeddingIndex::ComputeInfoChecksum(
    bool force) {
  return info().ComputeChecksum();
}

libtextclassifier3::StatusOr<Crc32> EmbeddingIndex::ComputeStoragesChecksum(
    bool force) {
  if (is_empty()) {
    return Crc32(0);
  }
  ICING_ASSIGN_OR_RETURN(Crc32 embedding_posting_list_mapper_crc,
                         embedding_posting_list_mapper_->ComputeChecksum());
  ICING_ASSIGN_OR_RETURN(Crc32 embedding_vectors_crc,
                         embedding_vectors_->ComputeChecksum());
  return Crc32(embedding_posting_list_mapper_crc.Get() ^
               embedding_vectors_crc.Get());
}

}  // namespace lib
}  // namespace icing
