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

#include "icing/icing-search-engine.h"

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/annotate.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/mutex.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/file/filesystem.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/index-processor.h"
#include "icing/index/index.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/initialize.pb.h"
#include "icing/proto/optimize.pb.h"
#include "icing/proto/persist.pb.h"
#include "icing/proto/reset.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/status.pb.h"
#include "icing/query/query-processor.h"
#include "icing/result/result-retriever.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/schema-util.h"
#include "icing/schema/section.h"
#include "icing/scoring/ranker.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/scoring/scoring-processor.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/transform/normalizer.h"
#include "icing/util/clock.h"
#include "icing/util/crc32.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

constexpr std::string_view kDocumentSubfolderName = "document_dir";
constexpr std::string_view kIndexSubfolderName = "index_dir";
constexpr std::string_view kSchemaSubfolderName = "schema_dir";
constexpr std::string_view kIcingSearchEngineHeaderFilename =
    "icing_search_engine_header";

libtextclassifier3::Status ValidateOptions(
    const IcingSearchEngineOptions& options) {
  // These options are only used in IndexProcessor, which won't be created
  // until the first Put call. So they must be checked here, so that any
  // errors can be surfaced in Initialize.
  if (options.max_tokens_per_doc() <= 0) {
    return absl_ports::InvalidArgumentError(
        "Options::max_tokens_per_doc must be greater than zero.");
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status ValidateResultSpec(
    const ResultSpecProto& result_spec) {
  if (result_spec.num_per_page() < 0) {
    return absl_ports::InvalidArgumentError(
        "ResultSpecProto.num_per_page cannot be negative.");
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status ValidateSearchSpec(
    const SearchSpecProto& search_spec,
    const PerformanceConfiguration& configuration) {
  if (search_spec.query().size() > configuration.max_query_length) {
    return absl_ports::InvalidArgumentError(
        absl_ports::StrCat("SearchSpecProto.query is longer than the maximum "
                           "allowed query length: ",
                           std::to_string(configuration.max_query_length)));
  }
  return libtextclassifier3::Status::OK;
}

IndexProcessor::Options CreateIndexProcessorOptions(
    const IcingSearchEngineOptions& options) {
  IndexProcessor::Options index_processor_options;
  index_processor_options.max_tokens_per_document =
      options.max_tokens_per_doc();
  index_processor_options.token_limit_behavior =
      IndexProcessor::Options::TokenLimitBehavior::kSuppressError;
  return index_processor_options;
}

std::string MakeHeaderFilename(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kIcingSearchEngineHeaderFilename);
}

// Document store files are in a standalone subfolder for easier file
// management. We can delete and recreate the subfolder and not touch/affect
// anything else.
std::string MakeDocumentDirectoryPath(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kDocumentSubfolderName);
}

// Makes a temporary folder path for the document store which will be used
// during full optimization.
std::string MakeDocumentTemporaryDirectoryPath(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kDocumentSubfolderName,
                            "_optimize_tmp");
}

// Index files are in a standalone subfolder because for easier file management.
// We can delete and recreate the subfolder and not touch/affect anything
// else.
std::string MakeIndexDirectoryPath(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kIndexSubfolderName);
}

// SchemaStore files are in a standalone subfolder for easier file management.
// We can delete and recreate the subfolder and not touch/affect anything
// else.
std::string MakeSchemaDirectoryPath(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kSchemaSubfolderName);
}

void TransformStatus(const libtextclassifier3::Status& internal_status,
                     StatusProto* status_proto) {
  switch (internal_status.CanonicalCode()) {
    case libtextclassifier3::StatusCode::OK:
      status_proto->set_code(StatusProto::OK);
      break;
    case libtextclassifier3::StatusCode::DATA_LOSS:
      status_proto->set_code(StatusProto::WARNING_DATA_LOSS);
      break;
    case libtextclassifier3::StatusCode::INVALID_ARGUMENT:
      status_proto->set_code(StatusProto::INVALID_ARGUMENT);
      break;
    case libtextclassifier3::StatusCode::NOT_FOUND:
      status_proto->set_code(StatusProto::NOT_FOUND);
      break;
    case libtextclassifier3::StatusCode::FAILED_PRECONDITION:
      status_proto->set_code(StatusProto::FAILED_PRECONDITION);
      break;
    case libtextclassifier3::StatusCode::ABORTED:
      status_proto->set_code(StatusProto::ABORTED);
      break;
    case libtextclassifier3::StatusCode::INTERNAL:
      // TODO(b/147699081): Cleanup our internal use of INTERNAL since it
      // doesn't match with what it *should* indicate as described in
      // go/icing-library-apis.
      status_proto->set_code(StatusProto::INTERNAL);
      break;
    case libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED:
      // TODO(b/147699081): Note that we don't detect all cases of OUT_OF_SPACE
      // (e.g. if the document log is full). And we use RESOURCE_EXHAUSTED
      // internally to indicate other resources are exhausted (e.g.
      // DocHitInfos) - although none of these are exposed through the API.
      // Consider separating the two cases out more clearly.
      status_proto->set_code(StatusProto::OUT_OF_SPACE);
      break;
    default:
      // Other internal status codes aren't supported externally yet. If it
      // should be supported, add another switch-case above.
      ICING_LOG(FATAL)
          << "Internal status code not supported in the external API";
      break;
  }

  status_proto->set_message(internal_status.error_message());
}

}  // namespace

IcingSearchEngine::IcingSearchEngine(const IcingSearchEngineOptions& options)
    : IcingSearchEngine(options, std::make_unique<Filesystem>(),
                        std::make_unique<Clock>()) {}

IcingSearchEngine::IcingSearchEngine(
    IcingSearchEngineOptions options,
    std::unique_ptr<const Filesystem> filesystem, std::unique_ptr<Clock> clock)
    : options_(std::move(options)),
      filesystem_(std::move(filesystem)),
      icing_filesystem_(std::make_unique<IcingFilesystem>()),
      clock_(std::move(clock)),
      result_state_manager_(performance_configuration_.max_num_hits_per_query,
                            performance_configuration_.max_num_cache_results) {
  ICING_VLOG(1) << "Creating IcingSearchEngine in dir: " << options_.base_dir();
}

IcingSearchEngine::~IcingSearchEngine() {
  if (initialized_) {
    if (PersistToDisk().status().code() != StatusProto::OK) {
      ICING_LOG(ERROR)
          << "Error persisting to disk in IcingSearchEngine destructor";
    }
  }
}

InitializeResultProto IcingSearchEngine::Initialize() {
  ICING_VLOG(1) << "Initializing IcingSearchEngine in dir: "
                << options_.base_dir();

  InitializeResultProto result_proto;
  StatusProto* result_status = result_proto.mutable_status();

  if (initialized_) {
    // Already initialized.
    result_status->set_code(StatusProto::OK);
    return result_proto;
  }

  // This method does both read and write so we need a writer lock. Using two
  // locks (reader and writer) has the chance to be interrupted during
  // switching.
  absl_ports::unique_lock l(&mutex_);

  // Releases result / query cache if any
  result_state_manager_.InvalidateAllResultStates();

  libtextclassifier3::Status status = InitializeMembers();
  if (!status.ok()) {
    TransformStatus(status, result_status);
    return result_proto;
  }

  // Even if each subcomponent initialized fine independently, we need to
  // check if they're consistent with each other.
  if (!CheckConsistency().ok()) {
    ICING_VLOG(1)
        << "IcingSearchEngine in inconsistent state, regenerating all "
           "derived data";
    status = RegenerateDerivedFiles();
    if (!status.ok()) {
      TransformStatus(status, result_status);
      return result_proto;
    }
  }

  initialized_ = true;
  result_status->set_code(StatusProto::OK);
  return result_proto;
}

libtextclassifier3::Status IcingSearchEngine::InitializeMembers() {
  ICING_RETURN_IF_ERROR(InitializeOptions());
  ICING_RETURN_IF_ERROR(InitializeSchemaStore());
  ICING_RETURN_IF_ERROR(InitializeDocumentStore());

  TC3_ASSIGN_OR_RETURN(language_segmenter_, LanguageSegmenter::Create());

  TC3_ASSIGN_OR_RETURN(normalizer_,
                       Normalizer::Create(options_.max_token_length()));

  ICING_RETURN_IF_ERROR(InitializeIndex());

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status IcingSearchEngine::InitializeOptions() {
  ICING_RETURN_IF_ERROR(ValidateOptions(options_));

  // Make sure the base directory exists
  if (!filesystem_->CreateDirectoryRecursively(options_.base_dir().c_str())) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Could not create directory: ", options_.base_dir()));
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status IcingSearchEngine::InitializeSchemaStore() {
  const std::string schema_store_dir =
      MakeSchemaDirectoryPath(options_.base_dir());
  // Make sure the sub-directory exists
  if (!filesystem_->CreateDirectoryRecursively(schema_store_dir.c_str())) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Could not create directory: ", schema_store_dir));
  }
  ICING_ASSIGN_OR_RETURN(
      schema_store_, SchemaStore::Create(filesystem_.get(), schema_store_dir));

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status IcingSearchEngine::InitializeDocumentStore() {
  const std::string document_dir =
      MakeDocumentDirectoryPath(options_.base_dir());
  // Make sure the sub-directory exists
  if (!filesystem_->CreateDirectoryRecursively(document_dir.c_str())) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Could not create directory: ", document_dir));
  }
  ICING_ASSIGN_OR_RETURN(
      document_store_,
      DocumentStore::Create(filesystem_.get(), document_dir, clock_.get(),
                            schema_store_.get()));

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status IcingSearchEngine::InitializeIndex() {
  const std::string index_dir = MakeIndexDirectoryPath(options_.base_dir());
  // Make sure the sub-directory exists
  if (!filesystem_->CreateDirectoryRecursively(index_dir.c_str())) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Could not create directory: ", index_dir));
  }
  Index::Options index_options(index_dir, options_.index_merge_size());

  auto index_or = Index::Create(index_options, icing_filesystem_.get());
  if (!index_or.ok()) {
    if (!filesystem_->DeleteDirectoryRecursively(index_dir.c_str()) ||
        !filesystem_->CreateDirectoryRecursively(index_dir.c_str())) {
      return absl_ports::InternalError(
          absl_ports::StrCat("Could not recreate directory: ", index_dir));
    }

    // Try recreating it from scratch and re-indexing everything.
    ICING_ASSIGN_OR_RETURN(
        index_, Index::Create(index_options, icing_filesystem_.get()));
    ICING_RETURN_IF_ERROR(RestoreIndex());
  } else {
    // Index was created fine.
    index_ = std::move(index_or).ValueOrDie();
  }

  return libtextclassifier3::Status::OK;
}  // namespace lib

libtextclassifier3::Status IcingSearchEngine::CheckConsistency() {
  if (!HeaderExists()) {
    // Without a header file, we have no checksum and can't even detect
    // inconsistencies
    return absl_ports::NotFoundError("No header file found.");
  }

  // Header does exist, verify that the header looks fine.
  IcingSearchEngine::Header header;
  if (!filesystem_->Read(MakeHeaderFilename(options_.base_dir()).c_str(),
                         &header, sizeof(header))) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Couldn't read: ", MakeHeaderFilename(options_.base_dir())));
  }

  if (header.magic != IcingSearchEngine::Header::kMagic) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Invalid header kMagic for file: ",
                           MakeHeaderFilename(options_.base_dir())));
  }

  ICING_ASSIGN_OR_RETURN(Crc32 checksum, ComputeChecksum());
  if (checksum.Get() != header.checksum) {
    return absl_ports::InternalError(
        "IcingSearchEngine checksum doesn't match");
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status IcingSearchEngine::RegenerateDerivedFiles() {
  ICING_RETURN_IF_ERROR(
      document_store_->UpdateSchemaStore(schema_store_.get()));
  ICING_RETURN_IF_ERROR(index_->Reset());
  ICING_RETURN_IF_ERROR(RestoreIndex());

  const std::string header_file =
      MakeHeaderFilename(options_.base_dir().c_str());
  if (HeaderExists()) {
    if (!filesystem_->DeleteFile(header_file.c_str())) {
      return absl_ports::InternalError(
          absl_ports::StrCat("Unable to delete file: ", header_file));
    }
  }
  ICING_ASSIGN_OR_RETURN(Crc32 checksum, ComputeChecksum());
  ICING_RETURN_IF_ERROR(UpdateHeader(checksum));

  return libtextclassifier3::Status::OK;
}

SetSchemaResultProto IcingSearchEngine::SetSchema(
    const SchemaProto& new_schema, bool ignore_errors_and_delete_documents) {
  return SetSchema(SchemaProto(new_schema), ignore_errors_and_delete_documents);
}

SetSchemaResultProto IcingSearchEngine::SetSchema(
    SchemaProto&& new_schema, bool ignore_errors_and_delete_documents) {
  ICING_VLOG(1) << "Setting new Schema";

  SetSchemaResultProto result_proto;
  StatusProto* result_status = result_proto.mutable_status();

  libtextclassifier3::Status status = SchemaUtil::Validate(new_schema);
  if (!status.ok()) {
    TransformStatus(status, result_status);
    return result_proto;
  }

  absl_ports::unique_lock l(&mutex_);

  auto lost_previous_schema_or = LostPreviousSchema();
  if (!lost_previous_schema_or.ok()) {
    TransformStatus(lost_previous_schema_or.status(), result_status);
    return result_proto;
  }
  bool lost_previous_schema = lost_previous_schema_or.ValueOrDie();

  auto set_schema_result_or = schema_store_->SetSchema(
      std::move(new_schema), ignore_errors_and_delete_documents);
  if (!set_schema_result_or.ok()) {
    TransformStatus(set_schema_result_or.status(), result_status);
    return result_proto;
  }
  const SchemaStore::SetSchemaResult set_schema_result =
      set_schema_result_or.ValueOrDie();

  for (const std::string& deleted_type :
       set_schema_result.schema_types_deleted_by_name) {
    result_proto.add_deleted_schema_types(deleted_type);
  }

  for (const std::string& incompatible_type :
       set_schema_result.schema_types_incompatible_by_name) {
    result_proto.add_incompatible_schema_types(incompatible_type);
  }

  if (set_schema_result.success) {
    if (lost_previous_schema) {
      // No previous schema to calculate a diff against. We have to go through
      // and revalidate all the Documents in the DocumentStore
      status = document_store_->UpdateSchemaStore(schema_store_.get());
      if (!status.ok()) {
        TransformStatus(status, result_status);
        return result_proto;
      }
    } else if (!set_schema_result.old_schema_type_ids_changed.empty() ||
               !set_schema_result.schema_types_incompatible_by_id.empty() ||
               !set_schema_result.schema_types_deleted_by_id.empty()) {
      status = document_store_->OptimizedUpdateSchemaStore(schema_store_.get(),
                                                           set_schema_result);
      if (!status.ok()) {
        TransformStatus(status, result_status);
        return result_proto;
      }
    }

    if (lost_previous_schema || set_schema_result.index_incompatible) {
      // Clears all index files
      status = index_->Reset();
      if (!status.ok()) {
        TransformStatus(status, result_status);
        return result_proto;
      }

      status = RestoreIndex();
      if (!status.ok()) {
        TransformStatus(status, result_status);
        return result_proto;
      }
    }

    result_status->set_code(StatusProto::OK);
  } else {
    result_status->set_code(StatusProto::FAILED_PRECONDITION);
    result_status->set_message("Schema is incompatible.");
  }
  return result_proto;
}

GetSchemaResultProto IcingSearchEngine::GetSchema() {
  GetSchemaResultProto result_proto;
  StatusProto* result_status = result_proto.mutable_status();

  absl_ports::shared_lock l(&mutex_);

  auto schema_or = schema_store_->GetSchema();
  if (!schema_or.ok()) {
    TransformStatus(schema_or.status(), result_status);
    return result_proto;
  }

  result_status->set_code(StatusProto::OK);
  *result_proto.mutable_schema() = *std::move(schema_or).ValueOrDie();
  return result_proto;
}

GetSchemaTypeResultProto IcingSearchEngine::GetSchemaType(
    std::string_view schema_type) {
  GetSchemaTypeResultProto result_proto;
  StatusProto* result_status = result_proto.mutable_status();

  absl_ports::shared_lock l(&mutex_);

  auto type_config_or = schema_store_->GetSchemaTypeConfig(schema_type);
  if (!type_config_or.ok()) {
    TransformStatus(type_config_or.status(), result_status);
    return result_proto;
  }

  result_status->set_code(StatusProto::OK);
  *result_proto.mutable_schema_type_config() = *(type_config_or.ValueOrDie());
  return result_proto;
}

PutResultProto IcingSearchEngine::Put(const DocumentProto& document) {
  return Put(DocumentProto(document));
}

PutResultProto IcingSearchEngine::Put(DocumentProto&& document) {
  ICING_VLOG(1) << "Writing document to document store";

  PutResultProto result_proto;
  StatusProto* result_status = result_proto.mutable_status();

  // Lock must be acquired before validation because the DocumentStore uses
  // the schema file to validate, and the schema could be changed in
  // SetSchema() which is protected by the same mutex.
  absl_ports::unique_lock l(&mutex_);

  auto document_id_or = document_store_->Put(document);
  if (!document_id_or.ok()) {
    TransformStatus(document_id_or.status(), result_status);
    return result_proto;
  }
  DocumentId document_id = document_id_or.ValueOrDie();

  auto index_processor_or = IndexProcessor::Create(
      schema_store_.get(), language_segmenter_.get(), normalizer_.get(),
      index_.get(), CreateIndexProcessorOptions(options_));
  if (!index_processor_or.ok()) {
    TransformStatus(index_processor_or.status(), result_status);
    return result_proto;
  }
  std::unique_ptr<IndexProcessor> index_processor =
      std::move(index_processor_or).ValueOrDie();

  auto status = index_processor->IndexDocument(document, document_id);
  if (!status.ok()) {
    TransformStatus(status, result_status);
    return result_proto;
  }

  result_status->set_code(StatusProto::OK);
  return result_proto;
}

GetResultProto IcingSearchEngine::Get(const std::string_view name_space,
                                      const std::string_view uri) {
  GetResultProto result_proto;
  StatusProto* result_status = result_proto.mutable_status();

  absl_ports::shared_lock l(&mutex_);

  auto document_or = document_store_->Get(name_space, uri);
  if (!document_or.ok()) {
    TransformStatus(document_or.status(), result_status);
    return result_proto;
  }

  result_status->set_code(StatusProto::OK);
  *result_proto.mutable_document() = std::move(document_or).ValueOrDie();
  return result_proto;
}

DeleteResultProto IcingSearchEngine::Delete(const std::string_view name_space,
                                            const std::string_view uri) {
  ICING_VLOG(1) << "Deleting document from doc store";

  DeleteResultProto result_proto;
  StatusProto* result_status = result_proto.mutable_status();

  absl_ports::unique_lock l(&mutex_);

  // TODO(b/144458732): Implement a more robust version of TC_RETURN_IF_ERROR
  // that can support error logging.
  libtextclassifier3::Status status = document_store_->Delete(name_space, uri);
  if (!status.ok()) {
    ICING_LOG(ERROR) << status.error_message()
                     << "Failed to delete Document. namespace: " << name_space
                     << ", uri: " << uri;
    TransformStatus(status, result_status);
    return result_proto;
  }

  result_status->set_code(StatusProto::OK);
  return result_proto;
}

DeleteByNamespaceResultProto IcingSearchEngine::DeleteByNamespace(
    const std::string_view name_space) {
  ICING_VLOG(1) << "Deleting namespace from doc store";

  absl_ports::unique_lock l(&mutex_);

  // TODO(b/144458732): Implement a more robust version of TC_RETURN_IF_ERROR
  // that can support error logging.
  libtextclassifier3::Status status =
      document_store_->DeleteByNamespace(name_space);
  DeleteByNamespaceResultProto delete_result;
  TransformStatus(status, delete_result.mutable_status());
  if (!status.ok()) {
    ICING_LOG(ERROR) << status.error_message()
                     << "Failed to delete Namespace: " << name_space;
    return delete_result;
  }
  return delete_result;
}

DeleteBySchemaTypeResultProto IcingSearchEngine::DeleteBySchemaType(
    const std::string_view schema_type) {
  ICING_VLOG(1) << "Deleting type from doc store";

  absl_ports::unique_lock l(&mutex_);

  // TODO(b/144458732): Implement a more robust version of TC_RETURN_IF_ERROR
  // that can support error logging.
  libtextclassifier3::Status status =
      document_store_->DeleteBySchemaType(schema_type);
  DeleteBySchemaTypeResultProto delete_result;
  TransformStatus(status, delete_result.mutable_status());
  if (!status.ok()) {
    ICING_LOG(ERROR) << status.error_message()
                     << "Failed to delete SchemaType: " << schema_type;
    return delete_result;
  }
  return delete_result;
}

PersistToDiskResultProto IcingSearchEngine::PersistToDisk() {
  ICING_VLOG(1) << "Persisting data to disk";

  PersistToDiskResultProto result_proto;
  StatusProto* result_status = result_proto.mutable_status();

  absl_ports::unique_lock l(&mutex_);

  auto status = InternalPersistToDisk();
  TransformStatus(status, result_status);
  return result_proto;
}

// Optimizes Icing's storage
//
// Steps:
// 1. Flush data to disk.
// 2. Copy data needed to a tmp directory.
// 3. Swap current directory and tmp directory.
OptimizeResultProto IcingSearchEngine::Optimize() {
  ICING_VLOG(1) << "Optimizing icing storage";

  OptimizeResultProto result_proto;
  StatusProto* result_status = result_proto.mutable_status();

  absl_ports::unique_lock l(&mutex_);

  // Releases result / query cache if any
  result_state_manager_.InvalidateAllResultStates();

  // Flushes data to disk before doing optimization
  auto status = InternalPersistToDisk();
  if (!status.ok()) {
    TransformStatus(status, result_status);
    return result_proto;
  }

  // TODO(b/143646633): figure out if we need to optimize index and doc store
  // at the same time.
  libtextclassifier3::Status optimization_status = OptimizeDocumentStore();

  if (!optimization_status.ok() &&
      !absl_ports::IsDataLoss(optimization_status)) {
    // The status now is either ABORTED_ERROR or INTERNAL_ERROR.
    // If ABORTED_ERROR, Icing should still be working.
    // If INTERNAL_ERROR, we're having IO errors or other errors that we can't
    // recover from.
    TransformStatus(optimization_status, result_status);
    return result_proto;
  }

  // The status is either OK or DATA_LOSS. The optimized document store is
  // guaranteed to work, so we update index according to the new document store.
  libtextclassifier3::Status index_reset_status = index_->Reset();
  if (!index_reset_status.ok()) {
    status = absl_ports::Annotate(
        absl_ports::InternalError("Failed to reset index after optimization."),
        index_reset_status.error_message());
    TransformStatus(status, result_status);
    return result_proto;
  }

  libtextclassifier3::Status index_restoration_status = RestoreIndex();
  if (!index_restoration_status.ok()) {
    status = absl_ports::Annotate(
        absl_ports::InternalError(
            "Failed to reindex documents after optimization."),
        index_restoration_status.error_message());

    TransformStatus(status, result_status);
    return result_proto;
  }

  TransformStatus(optimization_status, result_status);
  return result_proto;
}

libtextclassifier3::Status IcingSearchEngine::InternalPersistToDisk() {
  ICING_RETURN_IF_ERROR(schema_store_->PersistToDisk());
  ICING_RETURN_IF_ERROR(document_store_->PersistToDisk());
  ICING_RETURN_IF_ERROR(index_->PersistToDisk());

  // Update the combined checksum and write to header file.
  ICING_ASSIGN_OR_RETURN(Crc32 checksum, ComputeChecksum());
  ICING_RETURN_IF_ERROR(UpdateHeader(checksum));

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<Crc32> IcingSearchEngine::ComputeChecksum() {
  Crc32 total_checksum;
  // TODO(b/144458732): Implement a more robust version of TC_ASSIGN_OR_RETURN
  // that can support error logging.
  auto checksum_or = schema_store_->ComputeChecksum();
  if (!checksum_or.ok()) {
    ICING_LOG(ERROR) << checksum_or.status().error_message()
                     << "Failed to compute checksum of SchemaStore";
    return checksum_or.status();
  }

  Crc32 schema_store_checksum = std::move(checksum_or).ValueOrDie();

  // TODO(b/144458732): Implement a more robust version of TC_ASSIGN_OR_RETURN
  // that can support error logging.
  checksum_or = document_store_->ComputeChecksum();
  if (!checksum_or.ok()) {
    ICING_LOG(ERROR) << checksum_or.status().error_message()
                     << "Failed to compute checksum of DocumentStore";
    return checksum_or.status();
  }
  Crc32 document_store_checksum = std::move(checksum_or).ValueOrDie();

  Crc32 index_checksum = index_->ComputeChecksum();

  total_checksum.Append(std::to_string(document_store_checksum.Get()));
  total_checksum.Append(std::to_string(schema_store_checksum.Get()));
  total_checksum.Append(std::to_string(index_checksum.Get()));

  return total_checksum;
}

bool IcingSearchEngine::HeaderExists() {
  if (!filesystem_->FileExists(
          MakeHeaderFilename(options_.base_dir()).c_str())) {
    return false;
  }

  int64_t file_size =
      filesystem_->GetFileSize(MakeHeaderFilename(options_.base_dir()).c_str());

  // If it's been truncated to size 0 before, we consider it to be a new file
  return file_size != 0 && file_size != Filesystem::kBadFileSize;
}

libtextclassifier3::Status IcingSearchEngine::UpdateHeader(
    const Crc32& checksum) {
  // Write the header
  IcingSearchEngine::Header header;
  header.magic = IcingSearchEngine::Header::kMagic;
  header.checksum = checksum.Get();

  // This should overwrite the header.
  if (!filesystem_->Write(MakeHeaderFilename(options_.base_dir()).c_str(),
                          &header, sizeof(header))) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Failed to write IcingSearchEngine header: ",
                           MakeHeaderFilename(options_.base_dir())));
  }
  return libtextclassifier3::Status::OK;
}

SearchResultProto IcingSearchEngine::Search(
    const SearchSpecProto& search_spec, const ScoringSpecProto& scoring_spec,
    const ResultSpecProto& result_spec) {
  SearchResultProto result_proto;
  StatusProto* result_status = result_proto.mutable_status();

  libtextclassifier3::Status status = ValidateResultSpec(result_spec);
  if (!status.ok()) {
    TransformStatus(status, result_status);
    return result_proto;
  }
  status = ValidateSearchSpec(search_spec, performance_configuration_);
  if (!status.ok()) {
    TransformStatus(status, result_status);
    return result_proto;
  }

  // TODO(b/146008613) Explore ideas to make this function read-only.
  absl_ports::unique_lock l(&mutex_);

  // Gets unordered results from query processor
  auto query_processor_or = QueryProcessor::Create(
      index_.get(), language_segmenter_.get(), normalizer_.get(),
      document_store_.get(), schema_store_.get(), clock_.get());
  if (!query_processor_or.ok()) {
    TransformStatus(query_processor_or.status(), result_status);
    return result_proto;
  }
  std::unique_ptr<QueryProcessor> query_processor =
      std::move(query_processor_or).ValueOrDie();

  auto query_results_or = query_processor->ParseSearch(search_spec);
  if (!query_results_or.ok()) {
    TransformStatus(query_results_or.status(), result_status);
    return result_proto;
  }
  QueryProcessor::QueryResults query_results =
      std::move(query_results_or).ValueOrDie();

  // Scores but does not rank the results.
  libtextclassifier3::StatusOr<std::unique_ptr<ScoringProcessor>>
      scoring_processor_or =
          ScoringProcessor::Create(scoring_spec, document_store_.get());
  if (!scoring_processor_or.ok()) {
    TransformStatus(scoring_processor_or.status(), result_status);
    return result_proto;
  }
  std::unique_ptr<ScoringProcessor> scoring_processor =
      std::move(scoring_processor_or).ValueOrDie();
  std::vector<ScoredDocumentHit> result_document_hits =
      scoring_processor->Score(std::move(query_results.root_iterator),
                               performance_configuration_.num_to_score);

  // Returns early for empty result
  if (result_document_hits.empty()) {
    result_status->set_code(StatusProto::OK);
    return result_proto;
  }

  // Ranks and paginates results
  libtextclassifier3::StatusOr<PageResultState> page_result_state_or =
      result_state_manager_.RankAndPaginate(ResultState(
          std::move(result_document_hits), std::move(query_results.query_terms),
          search_spec, scoring_spec, result_spec));
  if (!page_result_state_or.ok()) {
    TransformStatus(page_result_state_or.status(), result_status);
    return result_proto;
  }
  PageResultState page_result_state =
      std::move(page_result_state_or).ValueOrDie();

  // Retrieves the document protos and snippets if requested
  auto result_retriever_or =
      ResultRetriever::Create(document_store_.get(), schema_store_.get(),
                              language_segmenter_.get(), normalizer_.get());
  if (!result_retriever_or.ok()) {
    result_state_manager_.InvalidateResultState(
        page_result_state.next_page_token);
    TransformStatus(result_retriever_or.status(), result_status);
    return result_proto;
  }
  std::unique_ptr<ResultRetriever> result_retriever =
      std::move(result_retriever_or).ValueOrDie();

  libtextclassifier3::StatusOr<std::vector<SearchResultProto::ResultProto>>
      results_or = result_retriever->RetrieveResults(page_result_state);
  if (!results_or.ok()) {
    result_state_manager_.InvalidateResultState(
        page_result_state.next_page_token);
    TransformStatus(results_or.status(), result_status);
    return result_proto;
  }
  std::vector<SearchResultProto::ResultProto> results =
      std::move(results_or).ValueOrDie();

  // Assembles the final search result proto
  result_proto.mutable_results()->Reserve(results.size());
  for (SearchResultProto::ResultProto& result : results) {
    result_proto.mutable_results()->Add(std::move(result));
  }
  result_status->set_code(StatusProto::OK);
  if (page_result_state.next_page_token != kInvalidNextPageToken) {
    result_proto.set_next_page_token(page_result_state.next_page_token);
  }
  return result_proto;
}

SearchResultProto IcingSearchEngine::GetNextPage(uint64_t next_page_token) {
  SearchResultProto result_proto;
  StatusProto* result_status = result_proto.mutable_status();

  // ResultStateManager has its own writer lock, so here we only need a reader
  // lock for other components.
  absl_ports::shared_lock l(&mutex_);

  libtextclassifier3::StatusOr<PageResultState> page_result_state_or =
      result_state_manager_.GetNextPage(next_page_token);

  if (!page_result_state_or.ok()) {
    if (absl_ports::IsNotFound(page_result_state_or.status())) {
      // NOT_FOUND means an empty result.
      result_status->set_code(StatusProto::OK);
    } else {
      // Real error, pass up.
      TransformStatus(page_result_state_or.status(), result_status);
    }
    return result_proto;
  }

  PageResultState page_result_state =
      std::move(page_result_state_or).ValueOrDie();

  // Retrieves the document protos.
  auto result_retriever_or =
      ResultRetriever::Create(document_store_.get(), schema_store_.get(),
                              language_segmenter_.get(), normalizer_.get());
  if (!result_retriever_or.ok()) {
    TransformStatus(result_retriever_or.status(), result_status);
    return result_proto;
  }
  std::unique_ptr<ResultRetriever> result_retriever =
      std::move(result_retriever_or).ValueOrDie();

  libtextclassifier3::StatusOr<std::vector<SearchResultProto::ResultProto>>
      results_or = result_retriever->RetrieveResults(page_result_state);
  if (!results_or.ok()) {
    TransformStatus(results_or.status(), result_status);
    return result_proto;
  }
  std::vector<SearchResultProto::ResultProto> results =
      std::move(results_or).ValueOrDie();

  // Assembles the final search result proto
  result_proto.mutable_results()->Reserve(results.size());
  for (SearchResultProto::ResultProto& result : results) {
    result_proto.mutable_results()->Add(std::move(result));
  }

  result_status->set_code(StatusProto::OK);
  if (result_proto.results_size() > 0) {
    result_proto.set_next_page_token(next_page_token);
  }
  return result_proto;
}

void IcingSearchEngine::InvalidateNextPageToken(uint64_t next_page_token) {
  result_state_manager_.InvalidateResultState(next_page_token);
}

libtextclassifier3::Status IcingSearchEngine::OptimizeDocumentStore() {
  // Gets the current directory path and an empty tmp directory path for
  // document store optimization.
  const std::string current_document_dir =
      MakeDocumentDirectoryPath(options_.base_dir());
  const std::string temporary_document_dir =
      MakeDocumentTemporaryDirectoryPath(options_.base_dir());
  if (!filesystem_->DeleteDirectoryRecursively(
          temporary_document_dir.c_str()) ||
      !filesystem_->CreateDirectoryRecursively(
          temporary_document_dir.c_str())) {
    return absl_ports::AbortedError(absl_ports::StrCat(
        "Failed to create a tmp directory: ", temporary_document_dir));
  }

  // Copies valid document data to tmp directory
  auto optimize_status = document_store_->OptimizeInto(temporary_document_dir);

  // Handles error if any
  if (!optimize_status.ok()) {
    filesystem_->DeleteDirectoryRecursively(temporary_document_dir.c_str());
    return absl_ports::Annotate(
        absl_ports::AbortedError("Failed to optimize document store"),
        optimize_status.error_message());
  }

  // Resets before swapping
  document_store_.reset();

  // When swapping files, always put the current working directory at the
  // second place because it is renamed at the latter position so we're less
  // vulnerable to errors.
  if (!filesystem_->SwapFiles(temporary_document_dir.c_str(),
                              current_document_dir.c_str())) {
    ICING_LOG(ERROR) << "Failed to swap files";

    // Ensures that current directory is still present.
    if (!filesystem_->CreateDirectoryRecursively(
            current_document_dir.c_str())) {
      return absl_ports::InternalError(
          "Failed to create file directory for document store");
    }

    // Tries to rebuild document store if swapping fails, to avoid leaving the
    // system in the broken state for future operations.
    auto document_store_or =
        DocumentStore::Create(filesystem_.get(), current_document_dir,
                              clock_.get(), schema_store_.get());
    // TODO(b/144458732): Implement a more robust version of
    // TC_ASSIGN_OR_RETURN that can support error logging.
    if (!document_store_or.ok()) {
      ICING_LOG(ERROR) << "Failed to create document store instance";
      return absl_ports::Annotate(
          absl_ports::InternalError("Failed to create document store instance"),
          document_store_or.status().error_message());
    }
    document_store_ = std::move(document_store_or).ValueOrDie();

    // Potential data loss
    // TODO(b/147373249): Find a way to detect true data loss error
    return absl_ports::DataLossError(
        "Failed to optimize document store, there might be data loss");
  }

  // Recreates the doc store instance
  ICING_ASSIGN_OR_RETURN(
      document_store_,
      DocumentStore::Create(filesystem_.get(), current_document_dir,
                            clock_.get(), schema_store_.get()),
      absl_ports::InternalError(
          "Document store has been optimized, but a valid document store "
          "instance can't be created"));

  // Deletes tmp directory
  if (!filesystem_->DeleteDirectoryRecursively(
          temporary_document_dir.c_str())) {
    ICING_LOG(ERROR) << "Document store has been optimized, but it failed to "
                        "delete temporary file directory";
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status IcingSearchEngine::RestoreIndex() {
  DocumentId last_stored_document_id =
      document_store_->last_added_document_id();

  if (last_stored_document_id == kInvalidDocumentId) {
    // Nothing to index
    return libtextclassifier3::Status::OK;
  }

  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<IndexProcessor> index_processor,
      IndexProcessor::Create(schema_store_.get(), language_segmenter_.get(),
                             normalizer_.get(), index_.get(),
                             CreateIndexProcessorOptions(options_)));

  for (DocumentId document_id = kMinDocumentId;
       document_id <= last_stored_document_id; document_id++) {
    libtextclassifier3::StatusOr<DocumentProto> document_or =
        document_store_->Get(document_id);

    if (!document_or.ok()) {
      if (absl_ports::IsInvalidArgument(document_or.status()) ||
          absl_ports::IsNotFound(document_or.status())) {
        // Skips invalid and non-existing documents.
        continue;
      } else {
        // Returns other errors
        return document_or.status();
      }
    }

    ICING_RETURN_IF_ERROR(
        index_processor->IndexDocument(document_or.ValueOrDie(), document_id));
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<bool> IcingSearchEngine::LostPreviousSchema() {
  auto status_or = schema_store_->GetSchema();
  if (status_or.ok()) {
    // Found a schema.
    return false;
  }

  if (!absl_ports::IsNotFound(status_or.status())) {
    // Any other type of error
    return status_or.status();
  }

  // We know: We don't have a schema now.
  //
  // We know: If no documents have been added, then the last_added_document_id
  // will be invalid.
  //
  // So: If documents have been added before and we don't have a schema now,
  // then that means we must have had a schema at some point. Since we wouldn't
  // accept documents without a schema to validate them against.
  return document_store_->last_added_document_id() != kInvalidDocumentId;
}

ResetResultProto IcingSearchEngine::Reset() {
  ICING_VLOG(1) << "Resetting IcingSearchEngine";

  ResetResultProto result_proto;
  StatusProto* result_status = result_proto.mutable_status();

  int64_t before_size = filesystem_->GetDiskUsage(options_.base_dir().c_str());

  if (!filesystem_->DeleteDirectoryRecursively(options_.base_dir().c_str())) {
    int64_t after_size = filesystem_->GetDiskUsage(options_.base_dir().c_str());
    if (after_size != before_size) {
      // Our filesystem doesn't atomically delete. If we have a discrepancy in
      // size, then that means we may have deleted some files, but not others.
      // So our data is in an invalid state now.
      result_status->set_code(StatusProto::INTERNAL);
      return result_proto;
    }

    result_status->set_code(StatusProto::ABORTED);
    return result_proto;
  }

  initialized_ = false;
  if (Initialize().status().code() != StatusProto::OK) {
    // We shouldn't hit the following Initialize errors:
    //   NOT_FOUND: all data was cleared, we aren't expecting anything
    //   DATA_LOSS: all data was cleared, we aren't expecting anything
    //   RESOURCE_EXHAUSTED: just deleted files, shouldn't run out of space
    //
    // We can't tell if Initialize failed and left Icing in an inconsistent
    // state or if it was a temporary I/O error. Group everything under INTERNAL
    // to be safe.
    //
    // TODO(b/147699081): Once Initialize returns the proper ABORTED/INTERNAL
    // status code, we can just propagate it up from here.
    result_status->set_code(StatusProto::INTERNAL);
    return result_proto;
  }

  result_status->set_code(StatusProto::OK);
  return result_proto;
}

}  // namespace lib
}  // namespace icing
