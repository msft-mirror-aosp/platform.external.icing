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

#ifndef ICING_STORE_DOCUMENT_STORE_H_
#define ICING_STORE_DOCUMENT_STORE_H_

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/file/file-backed-proto-log.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/document_wrapper.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-associated-score-data.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-id.h"
#include "icing/store/key-mapper.h"
#include "icing/util/clock.h"
#include "icing/util/crc32.h"
#include "icing/util/document-validator.h"

namespace icing {
namespace lib {

// Provides storage interfaces for documents.
class DocumentStore {
 public:
  struct Header {
    static constexpr int32_t kMagic = 0x746f7265;

    // Holds the magic as a quick sanity check against file corruption.
    int32_t magic;

    // Checksum of the DocumentStore's sub-component's checksums.
    uint32_t checksum;
  };

  // Not copyable
  DocumentStore(const DocumentStore&) = delete;
  DocumentStore& operator=(const DocumentStore&) = delete;

  // Persists and updates checksum of subcomponents.
  ~DocumentStore();

  // Factory method to create, initialize, and return a DocumentStore. The base
  // directory is used to persist document store files. If document store was
  // previously initialized with this directory, it will reload the files saved
  // by the last instance.
  //
  // Does not take any ownership, and all pointers must refer to valid objects
  // that outlive the one constructed.
  //
  // TODO(cassiewang): Consider returning a status indicating that derived files
  // were regenerated. This may be helpful in logs.
  //
  // Returns:
  //   A DocumentStore on success
  //   FAILED_PRECONDITION on any null pointer input
  //   INTERNAL_ERROR on IO error
  static libtextclassifier3::StatusOr<std::unique_ptr<DocumentStore>> Create(
      const Filesystem* filesystem, const std::string& base_dir,
      const Clock* clock, const SchemaStore* schema_store);

  // Returns the maximum DocumentId that the DocumentStore has assigned. If
  // there has not been any DocumentIds assigned, i.e. the DocumentStore is
  // empty, then kInvalidDocumentId is returned. This does not filter out
  // DocumentIds of deleted documents.
  const DocumentId last_added_document_id() const {
    if (document_id_mapper_->num_elements() == 0) {
      return kInvalidDocumentId;
    }
    return document_id_mapper_->num_elements() - 1;
  }

  // Puts the document into document store.
  //
  // Returns:
  //   A newly generated document id on success
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::StatusOr<DocumentId> Put(const DocumentProto& document);
  libtextclassifier3::StatusOr<DocumentId> Put(DocumentProto&& document);

  // Finds and returns the document identified by the given key (namespace +
  // uri)
  //
  // Returns:
  //   The document found on success
  //   NOT_FOUND if the key doesn't exist or document has been deleted
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::StatusOr<DocumentProto> Get(std::string_view name_space,
                                                  std::string_view uri) const;

  // Finds and returns the document identified by the given document id
  //
  // Returns:
  //   The document found on success
  //   INVALID_ARGUMENT if document_id is less than 0 or greater than the
  //                    maximum value
  //   NOT_FOUND if the document doesn't exist or has been deleted
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::StatusOr<DocumentProto> Get(DocumentId document_id) const;

  // Returns true if there's an existing document associated with the given
  // document id.
  bool DoesDocumentExist(DocumentId document_id) const;

  // Deletes the document identified by the given namespace and uri
  //
  // NOTE: Space is not reclaimed for deleted documents until Optimize() is
  // called.
  //
  // Returns:
  //   OK on success
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::Status Delete(std::string_view name_space,
                                    std::string_view uri);

  // Returns the NamespaceId of the string namespace
  //
  // Returns:
  //   NamespaceId on success
  //   NOT_FOUND if the namespace doesn't exist
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::StatusOr<NamespaceId> GetNamespaceId(
      std::string_view name_space) const;

  // Returns the DocumentAssociatedScoreData of the document specified by the
  // DocumentId.
  //
  // NOTE: This does not check if the document exists and will return the
  // DocumentFilterData of the document even if it has been deleted. Users
  // should check DoesDocumentExist(document_id) if they only want existing
  // documents' DocumentFilterData.
  //
  // Returns:
  //   DocumentAssociatedScoreData on success
  //   OUT_OF_RANGE if document_id is negative or exceeds previously seen
  //                DocumentIds
  libtextclassifier3::StatusOr<DocumentAssociatedScoreData>
  GetDocumentAssociatedScoreData(DocumentId document_id) const;

  // Returns the DocumentFilterData of the document specified by the DocumentId.
  //
  // NOTE: This does not check if the document exists and will return the
  // DocumentFilterData of the document even if it has been deleted. Users
  // should check DoesDocumentExist(document_id) if they only want existing
  // documents' DocumentFilterData.
  //
  // Returns:
  //   DocumentFilterData on success
  //   OUT_OF_RANGE if document_id is negative or exceeds previously seen
  //                DocumentIds
  libtextclassifier3::StatusOr<DocumentFilterData> GetDocumentFilterData(
      DocumentId document_id) const;

  // Deletes all documents belonging to the given namespace.
  //
  // NOTE: Space is not reclaimed for deleted documents until Optimize() is
  // called.
  //
  // Returns:
  //   OK on success
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::Status DeleteByNamespace(std::string_view name_space);

  // Deletes all documents belonging to the given schema type
  //
  // NOTE: Space is not reclaimed for deleted documents until Optimize() is
  // called.
  //
  // Returns:
  //   OK on success
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::Status DeleteBySchemaType(std::string_view schema_type);

  // Syncs all the data and metadata changes to disk.
  // Returns any encountered IO errors.
  libtextclassifier3::Status PersistToDisk();

  // Calculates and returns the disk usage in bytes.
  //
  // Returns:
  //   Disk usage on success
  //   INTERNAL_ERROR on IO error
  //
  // TODO(samzheng): consider returning a struct which has the breakdown of each
  // component.
  libtextclassifier3::StatusOr<int64_t> GetDiskUsage() const;

  // Update any derived data off of the SchemaStore with the new SchemaStore.
  // This may include pointers, SchemaTypeIds, etc.
  //
  // NOTE: This function may delete documents. A document may be invalidated by
  // the new SchemaStore, such as failing validation or having its schema type
  // deleted from the schema.
  //
  // This is best used if the caller is unsure about what's changed in the
  // SchemaStore, and wants to update all information no matter what. If the
  // caller does know what has changed, then it's recommended to call
  // OptimizedUpdateSchemaStore.
  //
  // Returns;
  //   OK on success
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::Status UpdateSchemaStore(const SchemaStore* schema_store);

  // Performs the same funtionality as UpdateSchemaStore, but this can be more
  // optimized in terms of less disk reads and less work if we know exactly
  // what's changed between the old and new SchemaStore.
  //
  // Returns;
  //   OK on success
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::Status OptimizedUpdateSchemaStore(
      const SchemaStore* schema_store,
      const SchemaStore::SetSchemaResult& set_schema_result);

  // Reduces internal file sizes by reclaiming space of deleted documents and
  // regenerating derived files.
  //
  // NOTE: The tasks in this method are too expensive to be executed in
  // real-time. The caller should decide how frequently and when to call this
  // method based on device usage.
  //
  // Returns:
  //   OK on success
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::Status Optimize();

  // Copy data from current base directory into a new directory. Any outdated or
  // deleted data won't be copied. During the process, document ids will be
  // reassigned so any files / classes that are based on old document ids may be
  // outdated.
  //
  // NOTE: The tasks in this method are too expensive to be executed in
  // real-time. The caller should decide how frequently and when to call this
  // method based on device usage.
  //
  // Returns:
  //   OK on success
  //   INVALID_ARGUMENT if new_directory is same as current base directory
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::Status OptimizeInto(const std::string& new_directory);

  // Computes the combined checksum of the document store - includes the ground
  // truth and all derived files.
  //
  // Returns:
  //   Combined checksum on success
  //   INTERNAL_ERROR on compute error
  libtextclassifier3::StatusOr<Crc32> ComputeChecksum() const;

 private:
  // Use DocumentStore::Create() to instantiate.
  DocumentStore(const Filesystem* filesystem, std::string_view base_dir,
                const Clock* clock, const SchemaStore* schema_store);

  const Filesystem* const filesystem_;
  const std::string base_dir_;
  const Clock& clock_;

  // Handles the ground truth schema and all of the derived data off of the
  // schema
  const SchemaStore* schema_store_;

  // Used to validate incoming documents
  DocumentValidator document_validator_;

  // A log used to store all documents, it serves as a ground truth of doc
  // store. key_mapper_ and document_id_mapper_ can be regenerated from it.
  std::unique_ptr<FileBackedProtoLog<DocumentWrapper>> document_log_;

  // Key (namespace + uri) to DocumentId mapping
  std::unique_ptr<KeyMapper<DocumentId>> document_key_mapper_;

  // DocumentId to file offset mapping
  std::unique_ptr<FileBackedVector<int64_t>> document_id_mapper_;

  // A cache of document associated scores. The ground truth of the scores is
  // DocumentProto stored in document_log_. This cache contains:
  //   - Document score
  //   - Document creation timestamp in seconds
  std::unique_ptr<FileBackedVector<DocumentAssociatedScoreData>> score_cache_;

  // A cache of data, indexed by DocumentId, used to filter documents. Currently
  // contains:
  //   - NamespaceId
  //   - SchemaTypeId
  //   - Expiration timestamp in seconds
  std::unique_ptr<FileBackedVector<DocumentFilterData>> filter_cache_;

  // Maps namespaces to a densely-assigned unique id. Namespaces are assigned an
  // id when the first document belonging to that namespace is added to the
  // DocumentStore. Namespaces may be removed from the mapper during compaction.
  std::unique_ptr<KeyMapper<NamespaceId>> namespace_mapper_;

  // Used internally to indicate whether the class has been initialized. This is
  // to guard against cases where the object has been created, but Initialize
  // fails in the constructor. If we have successfully exited the constructor,
  // then this field can be ignored. Clients of DocumentStore should not need to
  // worry about this field.
  bool initialized_ = false;

  libtextclassifier3::Status Initialize();

  // Creates sub-components and verifies the integrity of each sub-component.
  //
  // Returns an error if subcomponents failed to initialize successfully.
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::Status InitializeDerivedFiles();

  // Re-generates all files derived from the ground truth: the document log.
  //
  // NOTE: if this function fails, the only thing we can do is to retry it until
  // it succeeds or prevent the initialization of a DocumentStore. The
  // DocumentStore object wouldn't work reliably if this fails.
  //
  // Steps:
  //   1. Delete all derived files.
  //   2. Iterate through document log, put data into new key mapper and
  //   document_id
  //      mapper.
  //   3. Create header and store the updated combined checksum
  libtextclassifier3::Status RegenerateDerivedFiles();

  // Resets the unique_ptr to the document_key_mapper, deletes the underlying
  // file, and re-creates a new instance of the document_key_mapper .
  //
  // Returns OK or any IO errors.
  libtextclassifier3::Status ResetDocumentKeyMapper();

  // Resets the unique_ptr to the document_id_mapper, deletes the underlying
  // file, and re-creates a new instance of the document_id_mapper.
  //
  // Returns OK or any IO errors.
  libtextclassifier3::Status ResetDocumentIdMapper();

  // Resets the unique_ptr to the score_cache, deletes the underlying file, and
  // re-creates a new instance of the score_cache.
  //
  // Returns OK or any IO errors.
  libtextclassifier3::Status ResetDocumentAssociatedScoreCache();

  // Resets the unique_ptr to the filter_cache, deletes the underlying file, and
  // re-creates a new instance of the filter_cache.
  //
  // Returns OK or any IO errors.
  libtextclassifier3::Status ResetFilterCache();

  // Resets the unique_ptr to the namespace_mapper, deletes the underlying file,
  // and re-creates a new instance of the namespace_mapper.
  //
  // Returns OK or any IO errors.
  libtextclassifier3::Status ResetNamespaceMapper();

  // Checks if the header exists already. This does not create the header file
  // if it doesn't exist.
  bool HeaderExists();

  // Update and replace the header file. Creates the header file if it doesn't
  // exist.
  libtextclassifier3::Status UpdateHeader(const Crc32& checksum);

  // Update derived files that `name_space` has been deleted. This is primarily
  // useful if we're trying to update derived files when we've already seen a
  // namespace tombstone, and don't need to write another tombstone.
  //
  // NOTE: Space is not reclaimed in the derived files until Optimize() is
  // called.
  //
  // Returns:
  //   OK on success
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::Status UpdateDerivedFilesNamespaceDeleted(
      std::string_view name_space);

  // Update derived files that the schema type schema_type_id has been deleted.
  // This is primarily useful if we're trying to update derived files when we've
  // already seen a schema type tombstone, and don't need to write another
  // tombstone.
  //
  // NOTE: Space is not reclaimed in the derived files until Optimize() is
  // called.
  //
  // Returns:
  //   OK on success
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::Status UpdateDerivedFilesSchemaTypeDeleted(
      SchemaTypeId schema_type_id);

  // Helper method to find a DocumentId that is associated with the given
  // namespace and uri.
  //
  // NOTE: The DocumentId may refer to a invalid document (deleted
  // or expired). Callers can call DoesDocumentExist(document_id) to ensure it
  // refers to a valid Document.
  //
  // Returns:
  //   A DocumentId on success
  //   NOT_FOUND if the key doesn't exist
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::StatusOr<DocumentId> GetDocumentId(
      std::string_view name_space, std::string_view uri) const;

  // Helper method to validate the document id and return the file offset of the
  // associated document in document_log_.
  //
  // This can be a more informative call than just DoesDocumentExist because it
  // can return more status errors on whether the Document actually doesn't
  // exist or if there was an internal error while accessing files.
  //
  // Returns:
  //   The file offset on success
  //   INVALID_ARGUMENT if document_id is less than 0 or greater than the
  //                    maximum value
  //   NOT_FOUND if the document doesn't exist (i.e. deleted or expired)
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::StatusOr<int64_t> DoesDocumentExistAndGetFileOffset(
      DocumentId document_id) const;

  // Updates the entry in the score cache for document_id.
  libtextclassifier3::Status UpdateDocumentAssociatedScoreCache(
      DocumentId document_id, const DocumentAssociatedScoreData& score_data);

  // Updates the entry in the filter cache for document_id.
  libtextclassifier3::Status UpdateFilterCache(
      DocumentId document_id, const DocumentFilterData& filter_data);
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_STORE_DOCUMENT_STORE_H_
