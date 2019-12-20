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

#include "utils/base/status.h"
#include "utils/base/statusor.h"
#include "icing/absl_ports/annotate.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/mutex.h"
#include "icing/absl_ports/status_macros.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/file/filesystem.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/index-processor.h"
#include "icing/index/index.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/icing-search-engine-options.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/query/query-processor.h"
#include "icing/result-retriever.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/schema-util.h"
#include "icing/schema/section.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/scoring/scoring-processor.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/transform/normalizer.h"
#include "icing/util/clock.h"
#include "icing/util/crc32.h"
#include "icing/util/logging.h"

namespace icing {
namespace lib {

namespace {

constexpr std::string_view kDocumentAndIndexSubfolderName =
    "document_index_dir";
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
  if (result_spec.num_to_retrieve() < 0) {
    return absl_ports::InvalidArgumentError(
        "ResultSpec::num_to_retrieve cannot be negative.");
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

// Document store and index files are in a standalone subfolder because they
// can be re-generated at the same time during full optimization. Others like
// schema store can be optimized separately.
std::string MakeDocumentAndIndexDirectoryPath(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kDocumentAndIndexSubfolderName);
}

// Makes a temporary folder path for document and index which will be used
// during full optimization.
std::string MakeDocumentAndIndexTemporaryDirectoryPath(
    const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kDocumentAndIndexSubfolderName,
                            "_optimize_tmp");
}

// SchemaStore files are in a standalone subfolder for easier file management.
// We can delete and recreate the subfolder and not touch/affect anything
// else.
std::string MakeSchemaDirectoryPath(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kSchemaSubfolderName);
}

// Helper function to wrap results in ScoredDocumentHit without changing the
// order.
std::vector<ScoredDocumentHit> WrapResults(
    std::unique_ptr<DocHitInfoIterator> result_iterator, int num_to_return) {
  std::vector<ScoredDocumentHit> document_hits;
  while (result_iterator->Advance().ok() && num_to_return-- > 0) {
    const DocHitInfo& doc_hit_info = result_iterator->doc_hit_info();
    // Score is just a placeholder here and has no meaning.
    document_hits.emplace_back(doc_hit_info.document_id(),
                               doc_hit_info.hit_section_ids_mask(),
                               /*score=*/0);
  }
  return document_hits;
}

libtextclassifier3::StatusOr<std::vector<ScoredDocumentHit>> RunScoring(
    std::unique_ptr<DocHitInfoIterator> result_iterator,
    const ScoringSpecProto& scoring_spec, int num_to_return,
    const DocumentStore* document_store) {
  if (scoring_spec.rank_by() == ScoringSpecProto::RankingStrategy::NONE) {
    // No scoring needed, return in original order
    return WrapResults(std::move(result_iterator), num_to_return);
  }

  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<ScoringProcessor> scoring_processor,
      ScoringProcessor::Create(scoring_spec, document_store));
  return scoring_processor->ScoreAndRank(std::move(result_iterator),
                                         num_to_return);
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
      clock_(std::move(clock)) {
  ICING_VLOG(1) << "Creating IcingSearchEngine in dir: " << options_.base_dir();
}

IcingSearchEngine::~IcingSearchEngine() {
  if (initialized_) {
    if (!PersistToDisk().ok()) {
      ICING_LOG(ERROR)
          << "Error persisting to disk in IcingSearchEngine destructor";
    }
  }
}

libtextclassifier3::Status IcingSearchEngine::Initialize() {
  ICING_VLOG(1) << "Initializing IcingSearchEngine in dir: "
                << options_.base_dir();

  ICING_RETURN_IF_ERROR(ValidateOptions(options_));

  // This method does both read and write so we need a writer lock. Using two
  // locks (reader and writer) has the chance to be interrupted during
  // switching.
  absl_ports::unique_lock l(&mutex_);

  // Make sure the base directory exists
  if (!filesystem_->CreateDirectoryRecursively(options_.base_dir().c_str())) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Could not create directory: ", options_.base_dir()));
  }

  const std::string schema_store_dir =
      MakeSchemaDirectoryPath(options_.base_dir());
  // Make sure the sub-directory exists
  if (!filesystem_->CreateDirectoryRecursively(schema_store_dir.c_str())) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Could not create directory: ", schema_store_dir));
  }
  ICING_ASSIGN_OR_RETURN(
      schema_store_, SchemaStore::Create(filesystem_.get(), schema_store_dir));

  const std::string document_store_and_index_dir =
      MakeDocumentAndIndexDirectoryPath(options_.base_dir());
  // Make sure the sub-directory exists
  if (!filesystem_->CreateDirectoryRecursively(
          document_store_and_index_dir.c_str())) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Could not create directory: ", document_store_and_index_dir));
  }
  ICING_ASSIGN_OR_RETURN(
      document_store_,
      DocumentStore::Create(filesystem_.get(), document_store_and_index_dir,
                            clock_.get(), schema_store_.get()));

  ICING_ASSIGN_OR_RETURN(language_segmenter_,
                         LanguageSegmenter::Create(options_.lang_model_path()));

  ICING_ASSIGN_OR_RETURN(normalizer_,
                         Normalizer::Create(options_.max_token_length()));

  Index::Options index_options(document_store_and_index_dir,
                               options_.index_merge_size());
  ICING_ASSIGN_OR_RETURN(index_,
                         Index::Create(index_options, icing_filesystem_.get()));

  // Even if each subcomponent initialized fine independently, we need to
  // check if they're consistent with each other.
  if (!CheckConsistency().ok()) {
    ICING_VLOG(1)
        << "IcingSearchEngine in inconsistent state, regenerating all "
           "derived data";
    ICING_RETURN_IF_ERROR(RegenerateDerivedFiles());
  }

  initialized_ = true;
  return libtextclassifier3::Status::OK;
}

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

libtextclassifier3::Status IcingSearchEngine::SetSchema(
    const SchemaProto& new_schema, bool ignore_errors_and_delete_documents) {
  return SetSchema(SchemaProto(new_schema), ignore_errors_and_delete_documents);
}

libtextclassifier3::Status IcingSearchEngine::SetSchema(
    SchemaProto&& new_schema, bool ignore_errors_and_delete_documents) {
  ICING_VLOG(1) << "Setting new Schema";

  ICING_RETURN_IF_ERROR(SchemaUtil::Validate(new_schema));

  absl_ports::unique_lock l(&mutex_);

  ICING_ASSIGN_OR_RETURN(
      const SchemaStore::SetSchemaResult set_schema_result,
      schema_store_->SetSchema(std::move(new_schema),
                               ignore_errors_and_delete_documents));

  if (set_schema_result.success) {
    if (!set_schema_result.old_schema_type_ids_changed.empty() ||
        !set_schema_result.schema_types_incompatible_by_id.empty() ||
        !set_schema_result.schema_types_deleted_by_id.empty()) {
      ICING_RETURN_IF_ERROR(document_store_->OptimizedUpdateSchemaStore(
          schema_store_.get(), set_schema_result));
    }

    if (set_schema_result.index_incompatible) {
      // Clears all index files
      ICING_RETURN_IF_ERROR(index_->Reset());
      ICING_RETURN_IF_ERROR(RestoreIndex());
    }

    return libtextclassifier3::Status::OK;
  }

  // TODO(cassiewang): Instead of returning a Status, consider returning some
  // of the information we have in SetSchemaResult such as which types were
  // deleted and which types were incompatible.
  return absl_ports::FailedPreconditionError("Schema is incompatible.");
}

libtextclassifier3::StatusOr<SchemaProto> IcingSearchEngine::GetSchema() {
  absl_ports::shared_lock l(&mutex_);
  ICING_ASSIGN_OR_RETURN(const SchemaProto* schema, schema_store_->GetSchema());
  return *schema;
}

libtextclassifier3::StatusOr<SchemaTypeConfigProto>
IcingSearchEngine::GetSchemaType(std::string schema_type) {
  absl_ports::shared_lock l(&mutex_);
  ICING_ASSIGN_OR_RETURN(const SchemaTypeConfigProto* type_config,
                         schema_store_->GetSchemaTypeConfig(schema_type));
  return *type_config;
}

libtextclassifier3::Status IcingSearchEngine::Put(
    const DocumentProto& document) {
  return Put(DocumentProto(document));
}

libtextclassifier3::Status IcingSearchEngine::Put(DocumentProto&& document) {
  ICING_VLOG(1) << "Writing document to document store";

  // Lock must be acquired before validation because the DocumentStore uses
  // the schema file to validate, and the schema could be changed in
  // SetSchema() which is protected by the same mutex.
  absl_ports::unique_lock l(&mutex_);

  ICING_ASSIGN_OR_RETURN(DocumentId document_id,
                         document_store_->Put(document));

  IndexProcessor index_processor(schema_store_.get(), language_segmenter_.get(),
                                 normalizer_.get(), index_.get(),
                                 CreateIndexProcessorOptions(options_));
  ICING_RETURN_IF_ERROR(index_processor.IndexDocument(document, document_id));

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<DocumentProto> IcingSearchEngine::Get(
    const std::string_view name_space, const std::string_view uri) {
  absl_ports::shared_lock l(&mutex_);

  return document_store_->Get(name_space, uri);
}

libtextclassifier3::Status IcingSearchEngine::Delete(
    const std::string_view name_space, const std::string_view uri) {
  ICING_VLOG(1) << "Deleting document from doc store";

  absl_ports::unique_lock l(&mutex_);

  // TODO(b/144458732): Implement a more robust version of TC_RETURN_IF_ERROR
  // that can support error logging.
  libtextclassifier3::Status status = document_store_->Delete(name_space, uri);
  if (!status.ok()) {
    ICING_LOG(ERROR) << status.error_message()
                     << "Failed to delete Document. namespace: " << name_space
                     << ", uri: " << uri;
    return status;
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status IcingSearchEngine::PersistToDisk() {
  ICING_VLOG(1) << "Persisting data to disk";
  absl_ports::unique_lock l(&mutex_);
  return InternalPersistToDisk();
}

// Optimizes storage for document store and index.
//
// Steps:
// 1. Flush data to disk.
// 2. Copy data needed to a tmp directory.
// 3. Swap current directory and tmp directory.
//
// TODO(b/143724846) Optimize schema store here as well.
// TODO(b/143724541) Signal the caller if the failure is unrecoverable.
libtextclassifier3::Status IcingSearchEngine::Optimize() {
  ICING_VLOG(1) << "Optimizing icing storage";

  absl_ports::unique_lock l(&mutex_);

  // Flushes data to disk before doing optimization
  ICING_RETURN_IF_ERROR(InternalPersistToDisk());

  // Gets the current directory path and an empty tmp directory path for
  // document store and index optimization.
  std::string current_dir =
      MakeDocumentAndIndexDirectoryPath(options_.base_dir());
  std::string temporary_dir =
      MakeDocumentAndIndexTemporaryDirectoryPath(options_.base_dir());
  if (!filesystem_->DeleteDirectoryRecursively(temporary_dir.c_str()) ||
      !filesystem_->CreateDirectoryRecursively(temporary_dir.c_str())) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Failed to create a tmp directory: ", temporary_dir));
  }

  // Copies valid document data to tmp directory
  auto optimize_status = document_store_->OptimizeInto(temporary_dir);

  // Handles error if any
  if (!optimize_status.ok()) {
    filesystem_->DeleteDirectoryRecursively(temporary_dir.c_str());
    return absl_ports::Annotate(optimize_status,
                                "Failed to optimize document store.");
  }

  // Resets before swapping
  document_store_.reset();
  index_.reset();

  // When swapping files, always put the current working directory at the
  // second place because it is renamed at the latter position so we're less
  // vulnerable to errors.
  if (!filesystem_->SwapFiles(temporary_dir.c_str(), current_dir.c_str())) {
    // Try to rebuild document store and index if swapping fails, to avoid
    // leaving the system in the broken state for future operations.
    // TODO(b/144458732): Implement a more robust version of
    // TC_ASSIGN_OR_RETURN that can support error logging.
    auto document_store_or = DocumentStore::Create(
        filesystem_.get(), current_dir, clock_.get(), schema_store_.get());
    if (!document_store_or.ok()) {
      ICING_LOG(ERROR)
          << document_store_or.status().error_message()
          << "Failed to swap files, no document store instance available";
      return document_store_or.status();
    }
    document_store_ = std::move(document_store_or).ValueOrDie();

    Index::Options index_options(current_dir, options_.index_merge_size());
    // TODO(b/144458732): Implement a more robust version of
    // TC_ASSIGN_OR_RETURN that can support error logging.
    auto index_or = Index::Create(index_options, icing_filesystem_.get());
    if (!index_or.ok()) {
      ICING_LOG(ERROR) << index_or.status().error_message()
                       << "Failed to swap files, no index instance available";
      return index_or.status();
    }
    index_ = std::move(index_or).ValueOrDie();
    return absl_ports::InternalError("Failed to rename files");
  }

  // Recreates the doc store instance
  ICING_ASSIGN_OR_RETURN(
      document_store_,
      DocumentStore::Create(filesystem_.get(), current_dir, clock_.get(),
                            schema_store_.get()));

  // Deletes tmp directory
  if (!filesystem_->DeleteDirectoryRecursively(temporary_dir.c_str())) {
    return absl_ports::InternalError("Failed to delete temporary directory");
  }

  // Recreates the index instance and re-indexes all the documents.
  // TODO(b/143646633): figure out if we need to optimize index and doc store
  // at the same time.
  Index::Options index_options(current_dir, options_.index_merge_size());
  ICING_ASSIGN_OR_RETURN(index_,
                         Index::Create(index_options, icing_filesystem_.get()));
  ICING_RETURN_IF_ERROR(RestoreIndex());

  return libtextclassifier3::Status::OK;
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

libtextclassifier3::StatusOr<SearchResultProto> IcingSearchEngine::Search(
    const SearchSpecProto& search_spec, const ScoringSpecProto& scoring_spec,
    const ResultSpecProto& result_spec) {
  ICING_RETURN_IF_ERROR(ValidateResultSpec(result_spec));

  // TODO(b/146008613) Explore ideas to make this function read-only.
  absl_ports::unique_lock l(&mutex_);

  // Gets unordered results from query processor
  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), clock_.get());
  ICING_ASSIGN_OR_RETURN(QueryProcessor::QueryResults query_results,
                         query_processor.ParseSearch(search_spec));

  // Generates the final list of document hits
  ICING_ASSIGN_OR_RETURN(
      std::vector<ScoredDocumentHit> result_document_hits,
      RunScoring(std::move(query_results.root_iterator), scoring_spec,
                 result_spec.num_to_retrieve(), document_store_.get()));

  // Retrieves the document protos and snippets if requested
  ResultRetriever result_retriever(document_store_.get(), schema_store_.get(),
                                   language_segmenter_.get());
  ICING_ASSIGN_OR_RETURN(
      std::vector<SearchResultProto::ResultProto> results,
      result_retriever.RetrieveResults(result_spec, query_results.query_terms,
                                       search_spec.term_match_type(),
                                       result_document_hits));
  // Assembles the final search result proto
  SearchResultProto search_results;
  search_results.mutable_results()->Reserve(results.size());
  for (SearchResultProto::ResultProto& result : results) {
    search_results.mutable_results()->Add(std::move(result));
  }
  return search_results;
}

libtextclassifier3::Status IcingSearchEngine::RestoreIndex() {
  DocumentId last_stored_document_id =
      document_store_->last_added_document_id();

  if (last_stored_document_id == kInvalidDocumentId) {
    // Nothing to index
    return libtextclassifier3::Status::OK;
  }

  IndexProcessor index_processor(schema_store_.get(), language_segmenter_.get(),
                                 normalizer_.get(), index_.get(),
                                 CreateIndexProcessorOptions(options_));

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
        index_processor.IndexDocument(document_or.ValueOrDie(), document_id));
  }

  return libtextclassifier3::Status::OK;
}

}  // namespace lib
}  // namespace icing
