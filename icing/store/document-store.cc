// Copyright (C) 2019 Google LLC
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

#include "icing/store/document-store.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "utils/base/status.h"
#include "utils/base/statusor.h"
#include "utils/hash/farmhash.h"
#include "icing/absl_ports/annotate.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/status_macros.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/file/file-backed-proto-log.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/document_wrapper.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-associated-score-data.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-id.h"
#include "icing/store/key-mapper.h"
#include "icing/util/clock.h"
#include "icing/util/crc32.h"
#include "icing/util/logging.h"

namespace icing {
namespace lib {

namespace {

// Used in DocumentId mapper to mark a document as deleted
constexpr int64_t kDocDeletedFlag = -1;
constexpr char kDocumentLogFilename[] = "document_log";
constexpr char kDocumentIdMapperFilename[] = "document_id_mapper";
constexpr char kDocumentStoreHeaderFilename[] = "document_store_header";
constexpr char kScoreCacheFilename[] = "score_cache";
constexpr char kFilterCacheFilename[] = "filter_cache";
constexpr char kNamespaceMapperFilename[] = "namespace_mapper";

constexpr int32_t kUriMapperMaxSize = 12 * 1024 * 1024;  // 12 MiB

// 384 KiB for a KeyMapper would allow each internal array to have a max of
// 128 KiB for storage.
constexpr int32_t kNamespaceMapperMaxSize = 3 * 128 * 1024;  // 384 KiB

DocumentWrapper CreateDocumentWrapper(DocumentProto&& document) {
  DocumentWrapper document_wrapper;
  *document_wrapper.mutable_document() = std::move(document);
  return document_wrapper;
}

DocumentWrapper CreateDocumentTombstone(std::string_view document_namespace,
                                        std::string_view document_uri) {
  DocumentWrapper document_wrapper;
  document_wrapper.set_deleted(true);
  DocumentProto* document = document_wrapper.mutable_document();
  document->set_namespace_(std::string(document_namespace));
  document->set_uri(std::string(document_uri));
  return document_wrapper;
}

DocumentWrapper CreateNamespaceTombstone(std::string_view document_namespace) {
  DocumentWrapper document_wrapper;
  document_wrapper.set_deleted(true);
  DocumentProto* document = document_wrapper.mutable_document();
  document->set_namespace_(std::string(document_namespace));
  return document_wrapper;
}

DocumentWrapper CreateSchemaTypeTombstone(
    std::string_view document_schema_type) {
  DocumentWrapper document_wrapper;
  document_wrapper.set_deleted(true);
  DocumentProto* document = document_wrapper.mutable_document();
  document->set_schema(std::string(document_schema_type));
  return document_wrapper;
}

std::string MakeHeaderFilename(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kDocumentStoreHeaderFilename);
}

std::string MakeDocumentIdMapperFilename(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kDocumentIdMapperFilename);
}

std::string MakeDocumentLogFilename(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kDocumentLogFilename);
}

std::string MakeScoreCacheFilename(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kScoreCacheFilename);
}

std::string MakeFilterCacheFilename(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kFilterCacheFilename);
}

std::string MakeNamespaceMapperFilename(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kNamespaceMapperFilename);
}

// TODO(adorokhine): This class internally uses an 8-byte fingerprint of the
// Key and stores the key/value in a file-backed-trie that adds an ~80 byte
// overhead per key. As we know that these fingerprints are always 8-bytes in
// length and that they're random, we might be able to store them more
// compactly.
std::string MakeFingerprint(std::string_view name_space, std::string_view uri) {
  // Using a 64-bit fingerprint to represent the key could lead to collisions.
  // But, even with 200K unique keys, the probability of collision is about
  // one-in-a-billion (https://en.wikipedia.org/wiki/Birthday_attack).
  uint64_t fprint =
      tc3farmhash::Fingerprint64(absl_ports::StrCat(name_space, uri));

  std::string encoded_fprint;
  // DynamicTrie cannot handle keys with '0' as bytes. So, we encode it in
  // base128 and add 1 to make sure that no byte is '0'. This increases the
  // size of the encoded_fprint from 8-bytes to 10-bytes.
  while (fprint) {
    encoded_fprint.push_back((fprint & 0x7F) + 1);
    fprint >>= 7;
  }
  return encoded_fprint;
}

int64_t CalculateExpirationTimestampSecs(int64_t creation_timestamp_secs,
                                         int64_t ttl_secs) {
  if (ttl_secs == 0) {
    // Special case where a TTL of 0 indicates the document should never
    // expire. int64_t max, interpreted as seconds since epoch, represents
    // some point in the year 292,277,026,596. So we're probably ok to use
    // this as "never reaching this point".
    return std::numeric_limits<int64_t>::max();
  }

  int64_t expiration_timestamp_secs;
  if (__builtin_add_overflow(creation_timestamp_secs, ttl_secs,
                             &expiration_timestamp_secs)) {
    // Overflow detected. Treat overflow as the same behavior of just int64_t
    // max
    return std::numeric_limits<int64_t>::max();
  }

  return expiration_timestamp_secs;
}

}  // namespace

DocumentStore::DocumentStore(const Filesystem* filesystem,
                             const std::string_view base_dir,
                             const Clock* clock,
                             const SchemaStore* schema_store)
    : filesystem_(filesystem),
      base_dir_(base_dir),
      clock_(*clock),
      schema_store_(schema_store),
      document_validator_(schema_store) {}

libtextclassifier3::StatusOr<DocumentId> DocumentStore::Put(
    const DocumentProto& document) {
  return Put(DocumentProto(document));
}

DocumentStore::~DocumentStore() {
  if (initialized_) {
    if (!PersistToDisk().ok()) {
      ICING_LOG(ERROR)
          << "Error persisting to disk in DocumentStore destructor";
    }
  }
}

libtextclassifier3::StatusOr<std::unique_ptr<DocumentStore>>
DocumentStore::Create(const Filesystem* filesystem, const std::string& base_dir,
                      const Clock* clock, const SchemaStore* schema_store) {
  auto document_store = std::unique_ptr<DocumentStore>(
      new DocumentStore(filesystem, base_dir, clock, schema_store));
  ICING_RETURN_IF_ERROR(document_store->Initialize());
  return document_store;
}

libtextclassifier3::Status DocumentStore::Initialize() {
  auto create_result_or = FileBackedProtoLog<DocumentWrapper>::Create(
      filesystem_, MakeDocumentLogFilename(base_dir_),
      FileBackedProtoLog<DocumentWrapper>::Options(
          /*compress_in=*/true));
  // TODO(b/144458732): Implement a more robust version of TC_ASSIGN_OR_RETURN
  // that can support error logging.
  if (!create_result_or.ok()) {
    ICING_LOG(ERROR) << create_result_or.status().error_message()
                     << "Failed to initialize DocumentLog";
    return create_result_or.status();
  }
  FileBackedProtoLog<DocumentWrapper>::CreateResult create_result =
      std::move(create_result_or).ValueOrDie();
  document_log_ = std::move(create_result.proto_log);

  if (create_result.data_loss) {
    ICING_LOG(WARNING)
        << "Data loss in document log, regenerating derived files.";
    libtextclassifier3::Status status = RegenerateDerivedFiles();
    if (!status.ok()) {
      ICING_LOG(ERROR)
          << "Failed to regenerate derived files for DocumentStore";
      return status;
    }
  } else {
    if (!InitializeDerivedFiles().ok()) {
      ICING_VLOG(1)
          << "Couldn't find derived files or failed to initialize them, "
             "regenerating derived files for DocumentStore.";
      libtextclassifier3::Status status = RegenerateDerivedFiles();
      if (!status.ok()) {
        ICING_LOG(ERROR)
            << "Failed to regenerate derived files for DocumentStore";
        return status;
      }
    }
  }

  initialized_ = true;

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status DocumentStore::InitializeDerivedFiles() {
  if (!HeaderExists()) {
    // Without a header, we don't know if things are consistent between each
    // other so the caller should just regenerate everything from ground
    // truth.
    return absl_ports::InternalError("DocumentStore header doesn't exist");
  }

  DocumentStore::Header header;
  if (!filesystem_->Read(MakeHeaderFilename(base_dir_).c_str(), &header,
                         sizeof(header))) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Couldn't read: ", MakeHeaderFilename(base_dir_)));
  }

  if (header.magic != DocumentStore::Header::kMagic) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Invalid header kMagic for file: ", MakeHeaderFilename(base_dir_)));
  }

  // TODO(b/144458732): Implement a more robust version of TC_ASSIGN_OR_RETURN
  // that can support error logging.
  auto document_key_mapper_or =
      KeyMapper<DocumentId>::Create(*filesystem_, base_dir_, kUriMapperMaxSize);
  if (!document_key_mapper_or.ok()) {
    ICING_LOG(ERROR) << document_key_mapper_or.status().error_message()
                     << "Failed to initialize KeyMapper";
    return document_key_mapper_or.status();
  }
  document_key_mapper_ = std::move(document_key_mapper_or).ValueOrDie();

  // TODO(b/144458732): Implement a more robust version of TC_ASSIGN_OR_RETURN
  // that can support error logging.
  auto document_id_mapper_or = FileBackedVector<int64_t>::Create(
      *filesystem_, MakeDocumentIdMapperFilename(base_dir_),
      MemoryMappedFile::READ_WRITE_AUTO_SYNC);
  if (!document_id_mapper_or.ok()) {
    ICING_LOG(ERROR) << document_id_mapper_or.status().error_message()
                     << "Failed to initialize DocumentIdMapper";
    return document_id_mapper_or.status();
  }
  document_id_mapper_ = std::move(document_id_mapper_or).ValueOrDie();

  ICING_ASSIGN_OR_RETURN(score_cache_,
                         FileBackedVector<DocumentAssociatedScoreData>::Create(
                             *filesystem_, MakeScoreCacheFilename(base_dir_),
                             MemoryMappedFile::READ_WRITE_AUTO_SYNC));

  ICING_ASSIGN_OR_RETURN(filter_cache_,
                         FileBackedVector<DocumentFilterData>::Create(
                             *filesystem_, MakeFilterCacheFilename(base_dir_),
                             MemoryMappedFile::READ_WRITE_AUTO_SYNC));

  ICING_ASSIGN_OR_RETURN(
      namespace_mapper_,
      KeyMapper<NamespaceId>::Create(*filesystem_,
                                     MakeNamespaceMapperFilename(base_dir_),
                                     kNamespaceMapperMaxSize));

  ICING_ASSIGN_OR_RETURN(Crc32 checksum, ComputeChecksum());
  if (checksum.Get() != header.checksum) {
    return absl_ports::InternalError(
        "Combined checksum of DocStore was inconsistent");
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status DocumentStore::RegenerateDerivedFiles() {
  ICING_RETURN_IF_ERROR(ResetDocumentKeyMapper());
  ICING_RETURN_IF_ERROR(ResetDocumentIdMapper());
  ICING_RETURN_IF_ERROR(ResetDocumentAssociatedScoreCache());
  ICING_RETURN_IF_ERROR(ResetFilterCache());
  ICING_RETURN_IF_ERROR(ResetNamespaceMapper());

  // Iterates through document log
  auto iterator = document_log_->GetIterator();
  auto iterator_status = iterator.Advance();
  while (iterator_status.ok()) {
    ICING_ASSIGN_OR_RETURN(DocumentWrapper document_wrapper,
                           document_log_->ReadProto(iterator.GetOffset()));
    if (document_wrapper.deleted()) {
      if (!document_wrapper.document().uri().empty()) {
        // Individual document deletion.
        auto document_id_or =
            GetDocumentId(document_wrapper.document().namespace_(),
                          document_wrapper.document().uri());
        // Updates document_id mapper with deletion
        if (document_id_or.ok()) {
          ICING_RETURN_IF_ERROR(document_id_mapper_->Set(
              document_id_or.ValueOrDie(), kDocDeletedFlag));
        } else if (!absl_ports::IsNotFound(document_id_or.status())) {
          // Real error
          return absl_ports::Annotate(
              document_id_or.status(),
              absl_ports::StrCat("Failed to find document id. namespace: ",
                                 document_wrapper.document().namespace_(),
                                 ", uri: ", document_wrapper.document().uri()));
        }
      } else if (!document_wrapper.document().namespace_().empty()) {
        // Namespace deletion.
        ICING_RETURN_IF_ERROR(UpdateDerivedFilesNamespaceDeleted(
            document_wrapper.document().namespace_()));

      } else if (!document_wrapper.document().schema().empty()) {
        // SchemaType deletion.
        auto schema_type_id_or = schema_store_->GetSchemaTypeId(
            document_wrapper.document().schema());

        if (schema_type_id_or.ok()) {
          ICING_RETURN_IF_ERROR(UpdateDerivedFilesSchemaTypeDeleted(
              schema_type_id_or.ValueOrDie()));
        } else {
          // The deleted schema type doesn't have a SchemaTypeId we can refer
          // to in the FilterCache.
          //
          // TODO(cassiewang): We could avoid reading out all the documents.
          // When we see a schema type doesn't have a SchemaTypeId, assign the
          // unknown schema type a unique, temporary SchemaTypeId and store
          // that in the FilterCache. Then, when we see the schema type
          // tombstone here, we can look up its temporary SchemaTypeId and
          // just iterate through the FilterCache to mark those documents as
          // deleted.
          int size = document_id_mapper_->num_elements();
          for (DocumentId document_id = 0; document_id < size; document_id++) {
            auto document_or = Get(document_id);
            if (absl_ports::IsNotFound(document_or.status())) {
              // Skip nonexistent documents
              continue;
            } else if (!document_or.ok()) {
              // Real error, pass up
              return absl_ports::Annotate(
                  document_or.status(),
                  IcingStringUtil::StringPrintf(
                      "Failed to retrieve Document for DocumentId %d",
                      document_id));
            }

            // Guaranteed to have a document now.
            DocumentProto document = document_or.ValueOrDie();

            if (document.schema() == document_wrapper.document().schema()) {
              ICING_RETURN_IF_ERROR(
                  document_id_mapper_->Set(document_id, kDocDeletedFlag));
            }
          }
        }
      } else {
        return absl_ports::InternalError(
            "Encountered an invalid tombstone during recovery!");
      }
    } else {
      // Updates key mapper and document_id mapper with the new document
      DocumentId new_document_id = document_id_mapper_->num_elements();
      ICING_RETURN_IF_ERROR(document_key_mapper_->Put(
          MakeFingerprint(document_wrapper.document().namespace_(),
                          document_wrapper.document().uri()),
          new_document_id));
      ICING_RETURN_IF_ERROR(
          document_id_mapper_->Set(new_document_id, iterator.GetOffset()));

      ICING_RETURN_IF_ERROR(UpdateDocumentAssociatedScoreCache(
          new_document_id,
          DocumentAssociatedScoreData(
              document_wrapper.document().score(),
              document_wrapper.document().creation_timestamp_secs())));

      SchemaTypeId schema_type_id;
      auto schema_type_id_or =
          schema_store_->GetSchemaTypeId(document_wrapper.document().schema());
      if (absl_ports::IsNotFound(schema_type_id_or.status())) {
        // Didn't find a SchemaTypeId. This means that the DocumentStore and
        // the SchemaStore are out of sync. But DocumentStore can't do
        // anything about it so just ignore this for now. This should be
        // detected/handled by the owner of DocumentStore. Set it to some
        // arbitrary invalid value for now, it'll get updated to the correct
        // ID later.
        schema_type_id = -1;
      } else if (!schema_type_id_or.ok()) {
        // Real error. Pass it up
        return schema_type_id_or.status();
      } else {
        // We're guaranteed that SchemaTypeId is valid now
        schema_type_id = schema_type_id_or.ValueOrDie();
      }

      ICING_ASSIGN_OR_RETURN(
          NamespaceId namespace_id,
          namespace_mapper_->GetOrPut(document_wrapper.document().namespace_(),
                                      namespace_mapper_->num_keys()));

      int64_t expiration_timestamp_secs = CalculateExpirationTimestampSecs(
          document_wrapper.document().creation_timestamp_secs(),
          document_wrapper.document().ttl_secs());

      ICING_RETURN_IF_ERROR(UpdateFilterCache(
          new_document_id, DocumentFilterData(namespace_id, schema_type_id,
                                              expiration_timestamp_secs)));
    }
    iterator_status = iterator.Advance();
  }

  if (!absl_ports::IsOutOfRange(iterator_status)) {
    ICING_LOG(WARNING)
        << "Failed to iterate through proto log while regenerating "
           "derived files";
    return absl_ports::Annotate(iterator_status,
                                "Failed to iterate through proto log.");
  }

  // Write the header
  ICING_ASSIGN_OR_RETURN(Crc32 checksum, ComputeChecksum());
  ICING_RETURN_IF_ERROR(UpdateHeader(checksum));

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status DocumentStore::ResetDocumentKeyMapper() {
  // TODO(b/139734457): Replace ptr.reset()->Delete->Create flow with Reset().
  document_key_mapper_.reset();
  // TODO(b/144458732): Implement a more robust version of TC_RETURN_IF_ERROR
  // that can support error logging.
  libtextclassifier3::Status status =
      KeyMapper<DocumentId>::Delete(*filesystem_, base_dir_);
  if (!status.ok()) {
    ICING_LOG(ERROR) << status.error_message()
                     << "Failed to delete old key mapper";
    return status;
  }

  // TODO(b/144458732): Implement a more robust version of TC_ASSIGN_OR_RETURN
  // that can support error logging.
  auto document_key_mapper_or =
      KeyMapper<DocumentId>::Create(*filesystem_, base_dir_, kUriMapperMaxSize);
  if (!document_key_mapper_or.ok()) {
    ICING_LOG(ERROR) << document_key_mapper_or.status().error_message()
                     << "Failed to re-init key mapper";
    return document_key_mapper_or.status();
  }
  document_key_mapper_ = std::move(document_key_mapper_or).ValueOrDie();
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status DocumentStore::ResetDocumentIdMapper() {
  // TODO(b/139734457): Replace ptr.reset()->Delete->Create flow with Reset().
  document_id_mapper_.reset();
  // TODO(b/144458732): Implement a more robust version of TC_RETURN_IF_ERROR
  // that can support error logging.
  libtextclassifier3::Status status = FileBackedVector<int64_t>::Delete(
      *filesystem_, MakeDocumentIdMapperFilename(base_dir_));
  if (!status.ok()) {
    ICING_LOG(ERROR) << status.error_message()
                     << "Failed to delete old document_id mapper";
    return status;
  }
  // TODO(b/144458732): Implement a more robust version of TC_ASSIGN_OR_RETURN
  // that can support error logging.
  auto document_id_mapper_or = FileBackedVector<int64_t>::Create(
      *filesystem_, MakeDocumentIdMapperFilename(base_dir_),
      MemoryMappedFile::READ_WRITE_AUTO_SYNC);
  if (!document_id_mapper_or.ok()) {
    ICING_LOG(ERROR) << document_id_mapper_or.status().error_message()
                     << "Failed to re-init document_id mapper";
    return document_id_mapper_or.status();
  }
  document_id_mapper_ = std::move(document_id_mapper_or).ValueOrDie();
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status DocumentStore::ResetDocumentAssociatedScoreCache() {
  // TODO(b/139734457): Replace ptr.reset()->Delete->Create flow with Reset().
  score_cache_.reset();
  ICING_RETURN_IF_ERROR(FileBackedVector<DocumentAssociatedScoreData>::Delete(
      *filesystem_, MakeScoreCacheFilename(base_dir_)));
  ICING_ASSIGN_OR_RETURN(score_cache_,
                         FileBackedVector<DocumentAssociatedScoreData>::Create(
                             *filesystem_, MakeScoreCacheFilename(base_dir_),
                             MemoryMappedFile::READ_WRITE_AUTO_SYNC));
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status DocumentStore::ResetFilterCache() {
  // TODO(b/139734457): Replace ptr.reset()->Delete->Create flow with Reset().
  filter_cache_.reset();
  ICING_RETURN_IF_ERROR(FileBackedVector<DocumentFilterData>::Delete(
      *filesystem_, MakeFilterCacheFilename(base_dir_)));
  ICING_ASSIGN_OR_RETURN(filter_cache_,
                         FileBackedVector<DocumentFilterData>::Create(
                             *filesystem_, MakeFilterCacheFilename(base_dir_),
                             MemoryMappedFile::READ_WRITE_AUTO_SYNC));
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status DocumentStore::ResetNamespaceMapper() {
  // TODO(b/139734457): Replace ptr.reset()->Delete->Create flow with Reset().
  namespace_mapper_.reset();
  // TODO(b/144458732): Implement a more robust version of TC_RETURN_IF_ERROR
  // that can support error logging.
  libtextclassifier3::Status status = KeyMapper<NamespaceId>::Delete(
      *filesystem_, MakeNamespaceMapperFilename(base_dir_));
  if (!status.ok()) {
    ICING_LOG(ERROR) << status.error_message()
                     << "Failed to delete old namespace_id mapper";
    return status;
  }
  ICING_ASSIGN_OR_RETURN(
      namespace_mapper_,
      KeyMapper<NamespaceId>::Create(*filesystem_,
                                     MakeNamespaceMapperFilename(base_dir_),
                                     kNamespaceMapperMaxSize));
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<Crc32> DocumentStore::ComputeChecksum() const {
  Crc32 total_checksum;

  // TODO(b/144458732): Implement a more robust version of TC_ASSIGN_OR_RETURN
  // that can support error logging.
  auto checksum_or = document_log_->ComputeChecksum();
  if (!checksum_or.ok()) {
    ICING_LOG(ERROR) << checksum_or.status().error_message()
                     << "Failed to compute checksum of DocumentLog";
    return checksum_or.status();
  }
  Crc32 document_log_checksum = std::move(checksum_or).ValueOrDie();

  Crc32 document_key_mapper_checksum = document_key_mapper_->ComputeChecksum();

  // TODO(b/144458732): Implement a more robust version of TC_ASSIGN_OR_RETURN
  // that can support error logging.
  checksum_or = document_id_mapper_->ComputeChecksum();
  if (!checksum_or.ok()) {
    ICING_LOG(ERROR) << checksum_or.status().error_message()
                     << "Failed to compute checksum of DocumentIdMapper";
    return checksum_or.status();
  }
  Crc32 document_id_mapper_checksum = std::move(checksum_or).ValueOrDie();

  // TODO(b/144458732): Implement a more robust version of TC_ASSIGN_OR_RETURN
  // that can support error logging.
  checksum_or = score_cache_->ComputeChecksum();
  if (!checksum_or.ok()) {
    ICING_LOG(ERROR) << checksum_or.status().error_message()
                     << "Failed to compute checksum of score cache";
    return checksum_or.status();
  }
  Crc32 score_cache_checksum = std::move(checksum_or).ValueOrDie();

  // TODO(b/144458732): Implement a more robust version of TC_ASSIGN_OR_RETURN
  // that can support error logging.
  checksum_or = filter_cache_->ComputeChecksum();
  if (!checksum_or.ok()) {
    ICING_LOG(ERROR) << checksum_or.status().error_message()
                     << "Failed to compute checksum of filter cache";
    return checksum_or.status();
  }
  Crc32 filter_cache_checksum = std::move(checksum_or).ValueOrDie();

  Crc32 namespace_mapper_checksum = namespace_mapper_->ComputeChecksum();

  total_checksum.Append(std::to_string(document_log_checksum.Get()));
  total_checksum.Append(std::to_string(document_key_mapper_checksum.Get()));
  total_checksum.Append(std::to_string(document_id_mapper_checksum.Get()));
  total_checksum.Append(std::to_string(score_cache_checksum.Get()));
  total_checksum.Append(std::to_string(filter_cache_checksum.Get()));
  total_checksum.Append(std::to_string(namespace_mapper_checksum.Get()));

  return total_checksum;
}

bool DocumentStore::HeaderExists() {
  if (!filesystem_->FileExists(MakeHeaderFilename(base_dir_).c_str())) {
    return false;
  }

  int64_t file_size =
      filesystem_->GetFileSize(MakeHeaderFilename(base_dir_).c_str());

  // If it's been truncated to size 0 before, we consider it to be a new file
  return file_size != 0 && file_size != Filesystem::kBadFileSize;
}

libtextclassifier3::Status DocumentStore::UpdateHeader(const Crc32& checksum) {
  // Write the header
  DocumentStore::Header header;
  header.magic = DocumentStore::Header::kMagic;
  header.checksum = checksum.Get();

  // This should overwrite the header.
  if (!filesystem_->Write(MakeHeaderFilename(base_dir_).c_str(), &header,
                          sizeof(header))) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Failed to write DocStore header: ", MakeHeaderFilename(base_dir_)));
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<DocumentId> DocumentStore::Put(
    DocumentProto&& document) {
  ICING_RETURN_IF_ERROR(document_validator_.Validate(document));

  // Copy fields needed before they are moved
  std::string name_space = document.namespace_();
  std::string uri = document.uri();
  std::string schema = document.schema();
  int document_score = document.score();
  int64_t creation_timestamp_secs = document.creation_timestamp_secs();

  // Sets the creation timestamp if caller hasn't specified.
  if (document.creation_timestamp_secs() == 0) {
    creation_timestamp_secs = clock_.GetCurrentSeconds();
    document.set_creation_timestamp_secs(creation_timestamp_secs);
  }

  int64_t expiration_timestamp_secs = CalculateExpirationTimestampSecs(
      creation_timestamp_secs, document.ttl_secs());

  // Update ground truth first
  // TODO(b/144458732): Implement a more robust version of TC_ASSIGN_OR_RETURN
  // that can support error logging.
  auto offset_or =
      document_log_->WriteProto(CreateDocumentWrapper(std::move(document)));
  if (!offset_or.ok()) {
    ICING_LOG(ERROR) << offset_or.status().error_message()
                     << "Failed to write document";
    return offset_or.status();
  }
  int64_t file_offset = std::move(offset_or).ValueOrDie();

  // Get existing document id
  auto old_document_id_or = GetDocumentId(name_space, uri);
  if (!old_document_id_or.ok() &&
      !absl_ports::IsNotFound(old_document_id_or.status())) {
    return absl_ports::InternalError("Failed to read from key mapper");
  }

  // Creates a new document id, updates key mapper and document_id mapper
  DocumentId new_document_id = document_id_mapper_->num_elements();
  ICING_RETURN_IF_ERROR(document_key_mapper_->Put(
      MakeFingerprint(name_space, uri), new_document_id));
  ICING_RETURN_IF_ERROR(document_id_mapper_->Set(new_document_id, file_offset));

  ICING_RETURN_IF_ERROR(UpdateDocumentAssociatedScoreCache(
      new_document_id,
      DocumentAssociatedScoreData(document_score, creation_timestamp_secs)));

  // Update namespace maps
  ICING_ASSIGN_OR_RETURN(
      NamespaceId namespace_id,
      namespace_mapper_->GetOrPut(name_space, namespace_mapper_->num_keys()));

  ICING_ASSIGN_OR_RETURN(SchemaTypeId schema_type_id,
                         schema_store_->GetSchemaTypeId(schema));

  ICING_RETURN_IF_ERROR(UpdateFilterCache(
      new_document_id, DocumentFilterData(namespace_id, schema_type_id,
                                          expiration_timestamp_secs)));

  if (old_document_id_or.ok()) {
    // Mark the old document id as deleted.
    ICING_RETURN_IF_ERROR(document_id_mapper_->Set(
        old_document_id_or.ValueOrDie(), kDocDeletedFlag));
  }

  return new_document_id;
}

libtextclassifier3::StatusOr<DocumentProto> DocumentStore::Get(
    const std::string_view name_space, const std::string_view uri) const {
  ICING_ASSIGN_OR_RETURN(DocumentId document_id,
                         GetDocumentId(name_space, uri));
  return Get(document_id);
}

libtextclassifier3::StatusOr<DocumentProto> DocumentStore::Get(
    DocumentId document_id) const {
  ICING_ASSIGN_OR_RETURN(int64_t document_log_offset,
                         DoesDocumentExistAndGetFileOffset(document_id));

  // TODO(b/144458732): Implement a more robust version of TC_ASSIGN_OR_RETURN
  // that can support error logging.
  auto document_wrapper_or = document_log_->ReadProto(document_log_offset);
  if (!document_wrapper_or.ok()) {
    ICING_LOG(ERROR) << document_wrapper_or.status().error_message()
                     << "Failed to read from document log";
    return document_wrapper_or.status();
  }
  DocumentWrapper document_wrapper =
      std::move(document_wrapper_or).ValueOrDie();

  return std::move(*document_wrapper.mutable_document());
}

libtextclassifier3::StatusOr<DocumentId> DocumentStore::GetDocumentId(
    const std::string_view name_space, const std::string_view uri) const {
  auto document_id_or =
      document_key_mapper_->Get(MakeFingerprint(name_space, uri));
  if (!document_id_or.ok()) {
    return absl_ports::Annotate(
        document_id_or.status(),
        absl_ports::StrCat("Failed to find DocumentId by key: ", name_space,
                           ", ", uri));
  }

  // Guaranteed to have a DocumentId now
  return document_id_or.ValueOrDie();
}

libtextclassifier3::StatusOr<int64_t>
DocumentStore::DoesDocumentExistAndGetFileOffset(DocumentId document_id) const {
  if (!IsDocumentIdValid(document_id)) {
    return absl_ports::InvalidArgumentError(
        IcingStringUtil::StringPrintf("DocumentId %d is invalid", document_id));
  }

  auto file_offset_or = document_id_mapper_->Get(document_id);

  bool deleted =
      file_offset_or.ok() && *file_offset_or.ValueOrDie() == kDocDeletedFlag;
  if (deleted || absl_ports::IsOutOfRange(file_offset_or.status())) {
    // Document has been deleted or doesn't exist
    return absl_ports::NotFoundError(
        IcingStringUtil::StringPrintf("Document %d not found", document_id));
  }

  ICING_ASSIGN_OR_RETURN(const DocumentFilterData* filter_data,
                         filter_cache_->Get(document_id));
  if (clock_.GetCurrentSeconds() >= filter_data->expiration_timestamp_secs()) {
    // Past the expiration time, so also return NOT FOUND since it *shouldn't*
    // exist anymore.
    return absl_ports::NotFoundError(
        IcingStringUtil::StringPrintf("Document %d not found", document_id));
  }

  ICING_RETURN_IF_ERROR(file_offset_or.status());
  return *file_offset_or.ValueOrDie();
}

bool DocumentStore::DoesDocumentExist(DocumentId document_id) const {
  // If we can successfully get the document log offset, the document exists.
  return DoesDocumentExistAndGetFileOffset(document_id).ok();
}

libtextclassifier3::Status DocumentStore::Delete(
    const std::string_view name_space, const std::string_view uri) {
  // Try to get the DocumentId first
  auto document_id_or = GetDocumentId(name_space, uri);
  if (absl_ports::IsNotFound(document_id_or.status())) {
    // No need to delete nonexistent (name_space, uri)
    return libtextclassifier3::Status::OK;
  } else if (!document_id_or.ok()) {
    // Real error
    return absl_ports::Annotate(
        document_id_or.status(),
        absl_ports::StrCat("Failed to delete Document. namespace: ", name_space,
                           ", uri: ", uri));
  }

  // Check if the DocumentId's Document still exists.
  DocumentId document_id = document_id_or.ValueOrDie();
  auto file_offset_or = DoesDocumentExistAndGetFileOffset(document_id);
  if (absl_ports::IsNotFound(file_offset_or.status())) {
    // No need to delete nonexistent documents
    return libtextclassifier3::Status::OK;
  } else if (!file_offset_or.ok()) {
    // Real error, pass it up
    return absl_ports::Annotate(
        file_offset_or.status(),
        IcingStringUtil::StringPrintf(
            "Failed to retrieve file offset for DocumentId %d", document_id));
  }

  // Update ground truth first.
  // To delete a proto we don't directly remove it. Instead, we mark it as
  // deleted first by appending a tombstone of it and actually remove it from
  // file later in Optimize()
  // TODO(b/144458732): Implement a more robust version of ICING_RETURN_IF_ERROR
  // that can support error logging.
  libtextclassifier3::Status status =
      document_log_->WriteProto(CreateDocumentTombstone(name_space, uri))
          .status();
  if (!status.ok()) {
    ICING_LOG(ERROR) << status.error_message()
                     << "Failed to delete Document. namespace: " << name_space
                     << ", uri: " << uri;
    return status;
  }

  ICING_RETURN_IF_ERROR(
      document_id_mapper_->Set(document_id_or.ValueOrDie(), kDocDeletedFlag));

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<NamespaceId> DocumentStore::GetNamespaceId(
    std::string_view name_space) const {
  return namespace_mapper_->Get(name_space);
}

libtextclassifier3::StatusOr<DocumentAssociatedScoreData>
DocumentStore::GetDocumentAssociatedScoreData(DocumentId document_id) const {
  auto score_data_or = score_cache_->Get(document_id);
  if (!score_data_or.ok()) {
    ICING_LOG(ERROR) << " while trying to access DocumentId " << document_id
                     << " from score_cache_";
    return score_data_or.status();
  }
  return *std::move(score_data_or).ValueOrDie();
}

libtextclassifier3::StatusOr<DocumentFilterData>
DocumentStore::GetDocumentFilterData(DocumentId document_id) const {
  auto filter_data_or = filter_cache_->Get(document_id);
  if (!filter_data_or.ok()) {
    ICING_LOG(ERROR) << " while trying to access DocumentId " << document_id
                     << " from filter_cache_";
    return filter_data_or.status();
  }
  return *std::move(filter_data_or).ValueOrDie();
}

libtextclassifier3::Status DocumentStore::DeleteByNamespace(
    std::string_view name_space) {
  auto namespace_id_or = namespace_mapper_->Get(name_space);
  if (absl_ports::IsNotFound(namespace_id_or.status())) {
    // Namespace doesn't exist. Don't need to delete anything.
    return libtextclassifier3::Status::OK;
  } else if (!namespace_id_or.ok()) {
    // Real error, pass it up.
    return namespace_id_or.status();
  }

  // Update ground truth first.
  // To delete an entire namespace, we append a tombstone that only contains
  // the deleted bit and the name of the deleted namespace.
  // TODO(b/144458732): Implement a more robust version of
  // ICING_RETURN_IF_ERROR that can support error logging.
  libtextclassifier3::Status status =
      document_log_->WriteProto(CreateNamespaceTombstone(name_space)).status();
  if (!status.ok()) {
    ICING_LOG(ERROR) << status.error_message()
                     << "Failed to delete namespace. namespace = "
                     << name_space;
    return status;
  }

  return UpdateDerivedFilesNamespaceDeleted(name_space);
}

libtextclassifier3::Status DocumentStore::UpdateDerivedFilesNamespaceDeleted(
    std::string_view name_space) {
  auto namespace_id_or = namespace_mapper_->Get(name_space);
  if (absl_ports::IsNotFound(namespace_id_or.status())) {
    // Namespace doesn't exist. Don't need to delete anything.
    return libtextclassifier3::Status::OK;
  } else if (!namespace_id_or.ok()) {
    // Real error, pass it up.
    return namespace_id_or.status();
  }

  // Guaranteed to have a NamespaceId now.
  NamespaceId namespace_id = namespace_id_or.ValueOrDie();

  // Traverse FilterCache and delete all docs that match namespace_id
  for (DocumentId document_id = 0; document_id < filter_cache_->num_elements();
       ++document_id) {
    // filter_cache_->Get can only fail if document_id is < 0
    // or >= filter_cache_->num_elements. So, this error SHOULD NEVER HAPPEN.
    ICING_ASSIGN_OR_RETURN(const DocumentFilterData* data,
                           filter_cache_->Get(document_id));
    if (data->namespace_id() == namespace_id) {
      // docid_mapper_->Set can only fail if document_id is < 0
      // or >= docid_mapper_->num_elements. So the only possible way to get an
      // error here would be if filter_cache_->num_elements >
      // docid_mapper_->num_elements, which SHOULD NEVER HAPPEN.
      ICING_RETURN_IF_ERROR(
          document_id_mapper_->Set(document_id, kDocDeletedFlag));
    }
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status DocumentStore::DeleteBySchemaType(
    std::string_view schema_type) {
  auto schema_type_id_or = schema_store_->GetSchemaTypeId(schema_type);
  if (absl_ports::IsNotFound(schema_type_id_or.status())) {
    // SchemaType doesn't exist. Don't need to delete anything.
    return libtextclassifier3::Status::OK;
  } else if (!schema_type_id_or.ok()) {
    // Real error, pass it up.
    return schema_type_id_or.status();
  }

  // Update ground truth first.
  // To delete an entire schema type, we append a tombstone that only contains
  // the deleted bit and the name of the deleted schema type.
  // TODO(b/144458732): Implement a more robust version of
  // ICING_RETURN_IF_ERROR that can support error logging.
  libtextclassifier3::Status status =
      document_log_->WriteProto(CreateSchemaTypeTombstone(schema_type))
          .status();
  if (!status.ok()) {
    ICING_LOG(ERROR) << status.error_message()
                     << "Failed to delete schema_type. schema_type = "
                     << schema_type;
    return status;
  }

  // Guaranteed to have a SchemaTypeId now
  SchemaTypeId schema_type_id = schema_type_id_or.ValueOrDie();

  ICING_RETURN_IF_ERROR(UpdateDerivedFilesSchemaTypeDeleted(schema_type_id));

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status DocumentStore::UpdateDerivedFilesSchemaTypeDeleted(
    SchemaTypeId schema_type_id) {
  // Traverse FilterCache and delete all docs that match schema_type_id.
  for (DocumentId document_id = 0; document_id < filter_cache_->num_elements();
       ++document_id) {
    // filter_cache_->Get can only fail if document_id is < 0
    // or >= filter_cache_->num_elements. So, this error SHOULD NEVER HAPPEN.
    ICING_ASSIGN_OR_RETURN(const DocumentFilterData* data,
                           filter_cache_->Get(document_id));
    if (data->schema_type_id() == schema_type_id) {
      // docid_mapper_->Set can only fail if document_id is < 0
      // or >= docid_mapper_->num_elements. So the only possible way to get an
      // error here would be if filter_cache_->num_elements >
      // docid_mapper_->num_elements, which SHOULD NEVER HAPPEN.
      ICING_RETURN_IF_ERROR(
          document_id_mapper_->Set(document_id, kDocDeletedFlag));
    }
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status DocumentStore::PersistToDisk() {
  ICING_RETURN_IF_ERROR(document_log_->PersistToDisk());
  ICING_RETURN_IF_ERROR(document_key_mapper_->PersistToDisk());
  ICING_RETURN_IF_ERROR(document_id_mapper_->PersistToDisk());
  ICING_RETURN_IF_ERROR(score_cache_->PersistToDisk());
  ICING_RETURN_IF_ERROR(filter_cache_->PersistToDisk());
  ICING_RETURN_IF_ERROR(namespace_mapper_->PersistToDisk());

  // Update the combined checksum and write to header file.
  ICING_ASSIGN_OR_RETURN(Crc32 checksum, ComputeChecksum());
  ICING_RETURN_IF_ERROR(UpdateHeader(checksum));

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<int64_t> DocumentStore::GetDiskUsage() const {
  ICING_ASSIGN_OR_RETURN(const int64_t document_log_disk_usage,
                         document_log_->GetDiskUsage());
  ICING_ASSIGN_OR_RETURN(const int64_t document_key_mapper_disk_usage,
                         document_key_mapper_->GetDiskUsage());
  ICING_ASSIGN_OR_RETURN(const int64_t document_id_mapper_disk_usage,
                         document_id_mapper_->GetDiskUsage());
  ICING_ASSIGN_OR_RETURN(const int64_t score_cache_disk_usage,
                         score_cache_->GetDiskUsage());
  ICING_ASSIGN_OR_RETURN(const int64_t filter_cache_disk_usage,
                         filter_cache_->GetDiskUsage());
  ICING_ASSIGN_OR_RETURN(const int64_t namespace_mapper_disk_usage,
                         namespace_mapper_->GetDiskUsage());

  return document_log_disk_usage + document_key_mapper_disk_usage +
         document_id_mapper_disk_usage + score_cache_disk_usage +
         filter_cache_disk_usage + namespace_mapper_disk_usage;
}

libtextclassifier3::Status DocumentStore::UpdateSchemaStore(
    const SchemaStore* schema_store) {
  // Update all references to the SchemaStore
  schema_store_ = schema_store;
  document_validator_.UpdateSchemaStore(schema_store);

  int size = document_id_mapper_->num_elements();
  for (DocumentId document_id = 0; document_id < size; document_id++) {
    auto document_or = Get(document_id);
    if (absl_ports::IsNotFound(document_or.status())) {
      // Skip nonexistent documents
      continue;
    } else if (!document_or.ok()) {
      // Real error, pass up
      return absl_ports::Annotate(
          document_or.status(),
          IcingStringUtil::StringPrintf(
              "Failed to retrieve Document for DocumentId %d", document_id));
    }

    // Guaranteed to have a document now.
    DocumentProto document = document_or.ValueOrDie();

    // Revalidate that this document is still compatible
    if (document_validator_.Validate(document).ok()) {
      // Update the SchemaTypeId for this entry
      ICING_ASSIGN_OR_RETURN(SchemaTypeId schema_type_id,
                             schema_store_->GetSchemaTypeId(document.schema()));
      filter_cache_->mutable_array()[document_id].set_schema_type_id(
          schema_type_id);
    } else {
      // Document is no longer valid with the new SchemaStore. Mark as
      // deleted
      ICING_RETURN_IF_ERROR(Delete(document.namespace_(), document.uri()));
    }
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status DocumentStore::OptimizedUpdateSchemaStore(
    const SchemaStore* schema_store,
    const SchemaStore::SetSchemaResult& set_schema_result) {
  if (!set_schema_result.success) {
    // No new schema was set, no work to be done
    return libtextclassifier3::Status::OK;
  }

  // Update all references to the SchemaStore
  schema_store_ = schema_store;
  document_validator_.UpdateSchemaStore(schema_store);

  // Append a tombstone for each deleted schema type. This way, we don't have
  // to read out each document, check if the schema type has been deleted, and
  // append a tombstone per-document.
  for (const auto& schema_type :
       set_schema_result.schema_types_deleted_by_name) {
    // TODO(b/144458732): Implement a more robust version of
    // ICING_RETURN_IF_ERROR that can support error logging.
    libtextclassifier3::Status status =
        document_log_->WriteProto(CreateSchemaTypeTombstone(schema_type))
            .status();
    if (!status.ok()) {
      ICING_LOG(ERROR) << status.error_message()
                       << "Failed to delete schema_type. schema_type = "
                       << schema_type;
      return status;
    }
  }

  int size = document_id_mapper_->num_elements();
  for (DocumentId document_id = 0; document_id < size; document_id++) {
    auto exists_or = DoesDocumentExistAndGetFileOffset(document_id);
    if (absl_ports::IsNotFound(exists_or.status())) {
      // Skip nonexistent documents
      continue;
    } else if (!exists_or.ok()) {
      // Real error, pass up
      return absl_ports::Annotate(
          exists_or.status(),
          IcingStringUtil::StringPrintf("Failed to retrieve DocumentId %d",
                                        document_id));
    }

    // Guaranteed that the document exists now.
    ICING_ASSIGN_OR_RETURN(const DocumentFilterData* filter_data,
                           filter_cache_->Get(document_id));

    if (set_schema_result.schema_types_deleted_by_id.count(
            filter_data->schema_type_id()) != 0) {
      // We already created a tombstone for this deleted type. Just update the
      // derived files now.
      ICING_RETURN_IF_ERROR(
          document_id_mapper_->Set(document_id, kDocDeletedFlag));
      continue;
    }

    // Check if we need to update the FilterCache entry for this document. It
    // may have been assigned a different SchemaTypeId in the new SchemaStore.
    bool update_filter_cache =
        set_schema_result.old_schema_type_ids_changed.count(
            filter_data->schema_type_id()) != 0;

    // Check if we need to revalidate this document if the type is now
    // incompatible
    bool revalidate_document =
        set_schema_result.schema_types_incompatible_by_id.count(
            filter_data->schema_type_id()) != 0;

    if (update_filter_cache || revalidate_document) {
      ICING_ASSIGN_OR_RETURN(DocumentProto document, Get(document_id));

      if (update_filter_cache) {
        ICING_ASSIGN_OR_RETURN(
            SchemaTypeId schema_type_id,
            schema_store_->GetSchemaTypeId(document.schema()));
        filter_cache_->mutable_array()[document_id].set_schema_type_id(
            schema_type_id);
      }

      if (revalidate_document) {
        if (!document_validator_.Validate(document).ok()) {
          // Document is no longer valid with the new SchemaStore. Mark as
          // deleted
          ICING_RETURN_IF_ERROR(Delete(document.namespace_(), document.uri()));
        }
      }
    }
  }

  return libtextclassifier3::Status::OK;
}

// TODO(b/121227117): Implement Optimize()
libtextclassifier3::Status DocumentStore::Optimize() {
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status DocumentStore::OptimizeInto(
    const std::string& new_directory) {
  // Validates directory
  if (new_directory == base_dir_) {
    return absl_ports::InvalidArgumentError(
        "New directory is the same as the current one.");
  }

  ICING_ASSIGN_OR_RETURN(auto new_doc_store,
                         DocumentStore::Create(filesystem_, new_directory,
                                               &clock_, schema_store_));

  // Writes all valid docs into new document store (new directory)
  int size = document_id_mapper_->num_elements();
  for (DocumentId document_id = 0; document_id < size; document_id++) {
    auto document_or = Get(document_id);
    if (absl_ports::IsNotFound(document_or.status())) {
      // Skip nonexistent documents
      continue;
    } else if (!document_or.ok()) {
      // Real error, pass up
      return absl_ports::Annotate(
          document_or.status(),
          IcingStringUtil::StringPrintf(
              "Failed to retrieve Document for DocumentId %d", document_id));
    }

    // Guaranteed to have a document now.
    DocumentProto document_to_keep = document_or.ValueOrDie();
    // TODO(b/144458732): Implement a more robust version of
    // ICING_RETURN_IF_ERROR that can support error logging.
    libtextclassifier3::Status status =
        new_doc_store->Put(std::move(document_to_keep)).status();
    if (!status.ok()) {
      ICING_LOG(ERROR) << status.error_message()
                       << "Failed to write into new document store";
      return status;
    }
  }

  ICING_RETURN_IF_ERROR(new_doc_store->PersistToDisk());
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status DocumentStore::UpdateDocumentAssociatedScoreCache(
    DocumentId document_id, const DocumentAssociatedScoreData& score_data) {
  return score_cache_->Set(document_id, score_data);
}

libtextclassifier3::Status DocumentStore::UpdateFilterCache(
    DocumentId document_id, const DocumentFilterData& filter_data) {
  return filter_cache_->Set(document_id, filter_data);
}

}  // namespace lib
}  // namespace icing
