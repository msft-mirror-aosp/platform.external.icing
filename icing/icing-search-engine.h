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

#ifndef ICING_ICING_SEARCH_ENGINE_H_
#define ICING_ICING_SEARCH_ENGINE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "utils/base/status.h"
#include "utils/base/statusor.h"
#include "icing/absl_ports/mutex.h"
#include "icing/absl_ports/thread_annotations.h"
#include "icing/file/filesystem.h"
#include "icing/index/index.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/icing-search-engine-options.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-store.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/transform/normalizer.h"
#include "icing/util/clock.h"
#include "icing/util/crc32.h"

namespace icing {
namespace lib {

// TODO(cassiewang) Top-level comments and links to design-doc.
class IcingSearchEngine {
 public:
  struct Header {
    static constexpr int32_t kMagic = 0x6e650d0a;

    // Holds the magic as a quick sanity check against file corruption.
    int32_t magic;

    // Checksum of the IcingSearchEngine's sub-component's checksums.
    uint32_t checksum;
  };

  explicit IcingSearchEngine(const IcingSearchEngineOptions& options);

  // Calculates integrity checks and persists files to disk.
  ~IcingSearchEngine();

  // Loads & verifies the contents previously indexed from disk and gets ready
  // to handle read/write requests.
  //
  // WARNING: This is expected to be fast if Icing had a clean shutdown.
  // Otherwise, it can take longer as it runs integrity checks and attempts
  // to bring the index to a consistent state. If the data on disk is not
  // consistent, it restores the state when PersistToDisk() was last called.
  //
  // Returns OK on success, ie, Icing was initialized and all data verified.
  // Returns DATA_LOSS on partial success, when Icing encountered
  // data-inconsistency and had to restore its state back to the last call
  // to PersistToDisk().
  // Returns any other error encountered due to which the call couldn't be
  // completed. The instance of IcingSearchEngine is not usable if this
  // happens.
  libtextclassifier3::Status Initialize() LOCKS_EXCLUDED(mutex_);

  // Specifies the schema to be applied on all Documents that are already
  // stored as well as future documents. A schema can be 'invalid' and/or
  // 'incompatible'. These are two independent concepts.
  //
  // An 'invalid' schema is one that is not constructed properly. For example,
  // a PropertyConfigProto is missing the property name field. A schema can be
  // 'invalid' even if there is no previously existing schema.
  //
  // An 'incompatible' schema is one that is incompatible with a previously
  // existing schema. If there is no previously existing schema, then a new
  // schema cannot be incompatible. An incompatible schema is one that
  // invalidates pre-existing data. For example, a previously OPTIONAL field is
  // now REQUIRED in the new schema, and pre-existing data is considered invalid
  // against the new schema now.
  //
  // Default behavior will not allow a new schema to be set if it is invalid or
  // incompatible.
  //
  // The argument 'ignore_errors_and_delete_documents' can be set to true to
  // force set an incompatible schema. In that case, documents that are
  // invalidated by the new schema would be deleted from Icing. This cannot be
  // used to force set an invalid schema.
  //
  // This schema is persisted to disk and used across multiple instances.
  // So, callers should only have to call this if the schema changed.
  // However, calling it multiple times with the same schema is a no-op.
  //
  // On any error, Icing will keep using the older schema.
  //
  // Returns:
  //   OK on success
  //   INVALID_ARGUMENT if 'new_schema' is invalid
  //   FAILED_PRECONDITION if 'new_schema' is incompatible
  //   INTERNAL_ERROR if Icing failed to store the new schema or upgrade
  //     existing data based on the new schema.
  //
  // TODO(cassiewang) Figure out, document (and maybe even enforce) the best
  // way ordering of calls between Initialize() and SetSchema(), both when
  // the caller is creating an instance of IcingSearchEngine for the first
  // time and when the caller is reinitializing an existing index on disk.
  libtextclassifier3::Status SetSchema(
      SchemaProto&& new_schema, bool ignore_errors_and_delete_documents = false)
      LOCKS_EXCLUDED(mutex_);

  // This function makes a copy of the schema and calls SetSchema(SchemaProto&&
  // new_schema, bool ignore_errors_and_delete_documents)
  //
  // NOTE: It's recommended to call SetSchema(SchemaProto&& new_schema, bool
  // ignore_errors_and_delete_documents) directly to avoid a copy if the caller
  // can make an rvalue SchemaProto.
  libtextclassifier3::Status SetSchema(
      const SchemaProto& new_schema,
      bool ignore_errors_and_delete_documents = false) LOCKS_EXCLUDED(mutex_);

  // Get Icing's current copy of the schema.
  //
  // Returns:
  //   SchemaProto on success
  //   NOT_FOUND if a schema has not been set yet
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::StatusOr<SchemaProto> GetSchema() LOCKS_EXCLUDED(mutex_);

  // Get Icing's copy of the SchemaTypeConfigProto of name schema_type
  //
  // Returns:
  //   SchemaTypeConfigProto on success
  //   NOT_FOUND if a schema has not been set yet or if there is no
  //     SchemaTypeConfig of schema_type in the SchemaProto
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::StatusOr<SchemaTypeConfigProto> GetSchemaType(
      std::string schema_type) LOCKS_EXCLUDED(mutex_);

  // Puts the document into icing search engine so that it's stored and
  // indexed. Documents are automatically written to disk, callers can also
  // call PersistToDisk() to flush changes immediately.
  //
  // Returns:
  //   OK on success
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::Status Put(DocumentProto&& document)
      LOCKS_EXCLUDED(mutex_);

  // This function makes a copy of document and calls Put(DocumentProto&&
  // document).
  //
  // NOTE: It's recommended to call Put(DocumentProto&& document) directly to
  // avoid a copy if the caller can make an rvalue DocumentProto.
  libtextclassifier3::Status Put(const DocumentProto& document)
      LOCKS_EXCLUDED(mutex_);

  // Finds and returns the document identified by the given key (namespace +
  // uri)
  //
  // Returns:
  //   The document found on success
  //   NOT_FOUND if the key doesn't exist or doc has been deleted
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::StatusOr<DocumentProto> Get(std::string_view name_space,
                                                  std::string_view uri);

  // Deletes the Document specified by the given namespace / uri pair from the
  // search engine. Delete changes are automatically applied to disk, callers
  // can also call PersistToDisk() to flush changes immediately.
  //
  // Returns:
  //   OK on success
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::Status Delete(std::string_view name_space,
                                    std::string_view uri)
      LOCKS_EXCLUDED(mutex_);

  // Retrieves, scores, ranks, and returns the results according to the specs.
  // Please refer to each proto file for spec definitions.
  //
  // Returns:
  //   A SearchResultProto on success
  //   INVALID_ARGUMENT if any of specs is invalid
  //   INTERNAL_ERROR on any other errors
  libtextclassifier3::StatusOr<SearchResultProto> Search(
      const SearchSpecProto& search_spec, const ScoringSpecProto& scoring_spec,
      const ResultSpecProto& result_spec) LOCKS_EXCLUDED(mutex_);

  // Makes sure that every update/delete received till this point is flushed
  // to disk. If the app crashes after a call to PersistToDisk(), Icing
  // would be able to fully recover all data written up to this point.
  //
  // NOTE: It is not necessary to call PersistToDisk() to read back data
  // that was recently written. All read APIs will include the most recent
  // updates/deletes regardless of the data being flushed to disk.
  libtextclassifier3::Status PersistToDisk() LOCKS_EXCLUDED(mutex_);

  // Allows Icing to run tasks that are too expensive and/or unnecessary to be
  // executed in real-time, but are useful to keep it fast and be
  // resource-efficient. This method purely optimizes the internal files and
  // has no functional impact on what gets accepted/returned.
  //
  // NOTE: This method should be called about once every 24 hours when the
  // device is idle and charging. It can also be called when the system needs
  // to free up extra disk-space.
  //
  // WARNING: This method is CPU and IO intensive and depending on the
  // contents stored, it can take from a few seconds to a few minutes.
  // This call also blocks all read/write operations on Icing.
  libtextclassifier3::Status Optimize() LOCKS_EXCLUDED(mutex_);

  // Disallow copy and move.
  IcingSearchEngine(const IcingSearchEngine&) = delete;
  IcingSearchEngine& operator=(const IcingSearchEngine&) = delete;

 protected:
  IcingSearchEngine(IcingSearchEngineOptions options,
                    std::unique_ptr<const Filesystem> filesystem,
                    std::unique_ptr<Clock> clock);

 private:
  const IcingSearchEngineOptions options_;
  const std::unique_ptr<const Filesystem> filesystem_;
  const std::unique_ptr<const IcingFilesystem> icing_filesystem_;
  bool initialized_ = false;

  // Abstraction for accessing time values.
  std::unique_ptr<Clock> clock_;

  // Used to provide reader and writer locks
  absl_ports::shared_mutex mutex_;

  // Stores and processes the schema
  std::unique_ptr<SchemaStore> schema_store_ GUARDED_BY(mutex_);

  // Used to store all valid documents
  std::unique_ptr<DocumentStore> document_store_ GUARDED_BY(mutex_);

  std::unique_ptr<const LanguageSegmenter> language_segmenter_
      GUARDED_BY(mutex_);

  std::unique_ptr<const Normalizer> normalizer_ GUARDED_BY(mutex_);

  // Storage for all hits of content from the document store.
  std::unique_ptr<Index> index_ GUARDED_BY(mutex_);

  // Helper method to do the actual work to persist data to disk. We need this
  // separate method so that other public methods don't need to call
  // PersistToDisk(). Public methods calling each other may cause deadlock
  // issues.
  libtextclassifier3::Status InternalPersistToDisk()
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Many of the internal components rely on other components' derived data.
  // Check that everything is consistent with each other so that we're not using
  // outdated derived data in some parts of our system.
  //
  // Returns:
  //   OK on success
  //   NOT_FOUND if missing header file
  //   INTERNAL_ERROR on any IO errors or if header is inconsistent
  libtextclassifier3::Status CheckConsistency()
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Repopulates derived data off our ground truths.
  //
  // Returns:
  //   OK on success
  //   INTERNAL_ERROR on any IO errors
  libtextclassifier3::Status RegenerateDerivedFiles()
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Helper method to restore missing document data in index_. All documents
  // will be reindexed. This does not clear the index, so it is recommended to
  // call Index::Reset first.
  //
  // Returns:
  //   OK on success
  //   RESOURCE_EXHAUSTED if the index fills up before finishing indexing
  //   NOT_FOUND if some Document's schema type is not in the SchemaStore
  //   INTERNAL_ERROR on any IO errors
  libtextclassifier3::Status RestoreIndex() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Computes the combined checksum of the IcingSearchEngine - includes all its
  // subcomponents
  //
  // Returns:
  //   Combined checksum on success
  //   INTERNAL_ERROR on compute error
  libtextclassifier3::StatusOr<Crc32> ComputeChecksum()
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Checks if the header exists already. This does not create the header file
  // if it doesn't exist.
  bool HeaderExists() EXCLUSIVE_LOCKS_REQUIRED(mutex_);

  // Update and replace the header file. Creates the header file if it doesn't
  // exist.
  libtextclassifier3::Status UpdateHeader(const Crc32& checksum)
      EXCLUSIVE_LOCKS_REQUIRED(mutex_);
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_ICING_SEARCH_ENGINE_H_
