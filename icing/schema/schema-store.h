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

#ifndef ICING_SCHEMA_SCHEMA_STORE_H_
#define ICING_SCHEMA_SCHEMA_STORE_H_

#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/feature-flags.h"
#include "icing/file/file-backed-proto.h"
#include "icing/file/filesystem.h"
#include "icing/file/version-util.h"
#include "icing/proto/debug.pb.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/logging.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/storage.pb.h"
#include "icing/schema/joinable-property.h"
#include "icing/schema/schema-type-manager.h"
#include "icing/schema/schema-util.h"
#include "icing/schema/scorable_property_manager.h"
#include "icing/schema/section.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/key-mapper.h"
#include "icing/util/clock.h"
#include "icing/util/crc32.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

// Holds the ground truth schema proto. Tracks compatible changes to the schema
// and will update any derived data based on the schema proto, such as Sections,
// SchemaTypeConfigs, PropertyConfigs, and SchemaTypeIds. To ensure they have
// the most up-to-date data, callers should not save instances themselves and
// should always call Get* from the SchemaStore.
class SchemaStore {
 public:
  struct LegacyHeader {
    // Holds the magic as a quick sanity check against file corruption.
    int32_t magic;

    // Checksum of the SchemaStore's sub-component's checksums.
    uint32_t checksum;
  };

  class Header {
   public:
    static constexpr int32_t kMagic = 0x72650d0a;

    explicit Header(const Filesystem* filesystem, std::string path)
        : path_(std::move(path)), filesystem_(filesystem) {}

    Header(Header&& other)
        : serialized_header_(std::move(other.serialized_header_)),
          path_(std::move(other.path_)),
          header_fd_(std::move(other.header_fd_)),
          filesystem_(other.filesystem_),
          dirty_(other.dirty_) {}

    Header& operator=(Header&& other) {
      serialized_header_ = std::move(other.serialized_header_);
      path_ = std::move(other.path_);
      header_fd_ = std::move(other.header_fd_);
      filesystem_ = other.filesystem_;
      dirty_ = other.dirty_;
      return *this;
    }

    struct SerializedHeader {
      explicit SerializedHeader()
          : magic(kMagic),
            checksum(0),
            overlay_created(false),
            min_overlay_version_compatibility(
                std::numeric_limits<int32_t>::max()) {
        memset(overlay_created_padding, 0, kOverlayCreatedPaddingSize);
        memset(padding, 0, kPaddingSize);
      }
      // Holds the magic as a quick sanity check against file corruption.
      int32_t magic;

      // Checksum of the SchemaStore's sub-component's checksums.
      uint32_t checksum;

      bool overlay_created;
      // Three bytes of padding due to the fact that
      // min_overlay_version_compatibility_ has an alignof() == 4 and the offset
      // of overlay_created_padding_ == 9.
      static constexpr int kOverlayCreatedPaddingSize = 3;
      uint8_t overlay_created_padding[kOverlayCreatedPaddingSize];

      int32_t min_overlay_version_compatibility;

      static constexpr int kPaddingSize = 1008;
      // Padding exists just to reserve space for additional values.
      uint8_t padding[kPaddingSize];
    };
    static_assert(sizeof(SerializedHeader) == 1024);

    // RETURNS:
    //   - On success, a valid Header instance
    //   - NOT_FOUND if header file doesn't exist
    //   - INTERNAL if unable to read header
    static libtextclassifier3::StatusOr<Header> Read(
        const Filesystem* filesystem, std::string path);

    libtextclassifier3::Status Write();

    libtextclassifier3::Status PersistToDisk();

    int32_t magic() const { return serialized_header_.magic; }

    uint32_t checksum() const { return serialized_header_.checksum; }
    void set_checksum(uint32_t checksum) {
      dirty_ = true;
      serialized_header_.checksum = checksum;
    }

    bool overlay_created() const { return serialized_header_.overlay_created; }

    int32_t min_overlay_version_compatibility() const {
      return serialized_header_.min_overlay_version_compatibility;
    }

    void SetOverlayInfo(bool overlay_created,
                        int32_t min_overlay_version_compatibility) {
      dirty_ = true;
      serialized_header_.overlay_created = overlay_created;
      serialized_header_.min_overlay_version_compatibility =
          min_overlay_version_compatibility;
    }

   private:
    explicit Header(SerializedHeader serialized_header, std::string path,
                    ScopedFd header_fd, const Filesystem* filesystem)
        : serialized_header_(std::move(serialized_header)),
          path_(std::move(path)),
          header_fd_(std::move(header_fd)),
          filesystem_(filesystem),
          dirty_(false) {}

    SerializedHeader serialized_header_;
    std::string path_;
    ScopedFd header_fd_;
    const Filesystem* filesystem_;  // Not owned.
    bool dirty_;
  };

  // Holds information on what may have been affected by the new schema. This is
  // generally data that other classes may depend on from the SchemaStore,
  // so that we can know if we should go update those classes as well.
  struct SetSchemaResult {
    // Whether we are able to write the schema as determined by SetSchema's
    // arguments. This boolean reflects SetSchema's logic, and does not reflect
    // any system level IO errors that may prevent the schema from being written
    // to file.
    bool success = false;

    // SchemaTypeIds of schema types can be reassigned new SchemaTypeIds if:
    //   1. Schema types are added in the middle of the SchemaProto
    //   2. Schema types are removed from the middle of the SchemaProto
    //   3. Schema types are reordered in the SchemaProto
    //
    // SchemaTypeIds are not changed if schema types are added/removed to the
    // end of the SchemaProto.
    std::unordered_set<SchemaTypeId> old_schema_type_ids_changed;

    // Schema types that have been removed from the new schema. Represented by
    // the `schema_type` field in the SchemaTypeConfigProto.
    std::unordered_set<std::string> schema_types_deleted_by_name;

    // Schema types that have been removed from the new schema. Represented by
    // the SchemaTypeId assigned to this SchemaTypeConfigProto in the *old*
    // schema.
    std::unordered_set<SchemaTypeId> schema_types_deleted_by_id;

    // Schema types whose SchemaTypeConfigProto has changed in an incompatible
    // manner in the new schema. Compatibility determined in
    // SchemaUtil::ComputeCompatibilityDelta. Represented by the `schema_type`
    // field in the SchemaTypeConfigProto.
    std::unordered_set<std::string> schema_types_incompatible_by_name;

    // Schema types whose SchemaTypeConfigProto has changed in an incompatible
    // manner in the new schema. Compatibility determined in
    // SchemaUtil::ComputeCompatibilityDelta. Represented by the SchemaTypeId
    // assigned to this SchemaTypeConfigProto in the *old* schema.
    std::unordered_set<SchemaTypeId> schema_types_incompatible_by_id;

    // Schema types that were added in the new schema. Represented by the
    // `schema_type` field in the SchemaTypeConfigProto.
    std::unordered_set<std::string> schema_types_new_by_name;

    // Schema types that were changed in a way that was backwards compatible and
    // didn't invalidate the index. Represented by the `schema_type` field in
    // the SchemaTypeConfigProto.
    std::unordered_set<std::string>
        schema_types_changed_fully_compatible_by_name;

    // Schema types that were changed in a way that was backwards compatible,
    // but invalidated the index. Represented by the `schema_type` field in the
    // SchemaTypeConfigProto.
    std::unordered_set<std::string> schema_types_index_incompatible_by_name;

    // Schema types that were changed in a way that was backwards compatible,
    // but invalidated the joinable cache. Represented by the `schema_type`
    // field in the SchemaTypeConfigProto.
    std::unordered_set<std::string> schema_types_join_incompatible_by_name;

    // Schema types that were changed in a way that was backwards compatible,
    // but inconsistent with the old schema so that the scorable property cache
    // needs to be re-generated.
    std::unordered_set<SchemaTypeId>
        schema_types_scorable_property_inconsistent_by_id;

    // Schema types that were changed in a way that was backwards compatible,
    // but inconsistent with the old schema so that the scorable property cache
    // needs to be re-generated.
    std::unordered_set<std::string>
        schema_types_scorable_property_inconsistent_by_name;
  };

  struct ExpandedTypePropertyMask {
    std::string schema_type;
    std::unordered_set<std::string> paths;
  };

  static constexpr std::string_view kSchemaTypeWildcard = "*";

  // Factory function to create a SchemaStore which does not take ownership
  // of any input components, and all pointers must refer to valid objects that
  // outlive the created SchemaStore instance. The base_dir must already exist.
  // There does not need to be an existing schema already.
  //
  // If initialize_stats is present, the fields related to SchemaStore will be
  // populated.
  //
  // Returns:
  //   A SchemaStore on success
  //   FAILED_PRECONDITION on any null pointer input
  //   INTERNAL_ERROR on any IO errors
  static libtextclassifier3::StatusOr<std::unique_ptr<SchemaStore>> Create(
      const Filesystem* filesystem, const std::string& base_dir,
      const Clock* clock, const FeatureFlags* feature_flags,
      bool enable_schema_database = false,
      InitializeStatsProto* initialize_stats = nullptr);

  // Migrates schema files (backup v.s. new schema) according to version state
  // change. Also performs schema database migration and populates the database
  // fields in the persisted schema file if necessary.
  //
  // Returns:
  //   OK on success or nothing to migrate
  static libtextclassifier3::Status MigrateSchema(
      const Filesystem* filesystem, const std::string& base_dir,
      version_util::StateChange version_state_change, int32_t new_version,
      bool perform_schema_database_migration);

  // Discards all derived data in the schema store.
  //
  // Returns:
  //   OK on success or nothing to discard
  //   INTERNAL_ERROR on any I/O errors
  static libtextclassifier3::Status DiscardDerivedFiles(
      const Filesystem* filesystem, const std::string& base_dir);

  SchemaStore(SchemaStore&&) = default;
  SchemaStore& operator=(SchemaStore&&) = default;

  SchemaStore(const SchemaStore&) = delete;
  SchemaStore& operator=(const SchemaStore&) = delete;

  // Persists and updates checksum of subcomponents.
  ~SchemaStore();

  // Retrieve the current schema if it exists.
  //
  // Returns:
  //   - SchemaProto* if exists
  //   - INTERNAL_ERROR on any IO errors
  //   - NOT_FOUND_ERROR if a schema hasn't been set before
  libtextclassifier3::StatusOr<const SchemaProto*> GetSchema() const;

  // Retrieve the current schema for a given database if it exists.
  //
  // This is an expensive operation. Use GetSchema() when retrieving the entire
  // schema, or if there is only a single database in the schema store.
  //
  // Returns:
  //   - SchemaProto* containing only schema types from the database, if exists
  //   - INTERNAL_ERROR on any IO errors
  //   - NOT_FOUND_ERROR if the database doesn't exist in the schema, or if a
  //     schema hasn't been set before
  libtextclassifier3::StatusOr<SchemaProto> GetSchema(
      const std::string& database) const;

  // Update our current schema if it's compatible. Does not accept incompatible
  // schema or schema with types from multiple databases. Compatibility rules
  // defined by SchemaUtil::ComputeCompatibilityDelta.
  //
  // The schema types in the new schema proto must all be from a single
  // database. Does not support setting schema types across multiple databases
  // at once.
  //
  // If ignore_errors_and_delete_documents is set to true, then incompatible
  // schema are allowed and we'll force set the schema, meaning
  // SetSchemaResult.success will always be true.
  //
  // Returns:
  //   - SetSchemaResult that encapsulates the differences between the old and
  //     new schema, as well as if the new schema can be set.
  //   - INTERNAL_ERROR on any IO errors
  //   - ALREADY_EXISTS_ERROR if type names in the new schema are already in use
  //     by a different database.
  //   - INVALID_ARGUMENT_ERROR if the schema is invalid, or if the schema types
  //     are from multiple databases (once schema database is enabled).
  libtextclassifier3::StatusOr<SetSchemaResult> SetSchema(
      const SchemaProto& new_schema, bool ignore_errors_and_delete_documents,
      bool allow_circular_schema_definitions);
  libtextclassifier3::StatusOr<SetSchemaResult> SetSchema(
      SchemaProto&& new_schema, bool ignore_errors_and_delete_documents,
      bool allow_circular_schema_definitions);

  // Get the SchemaTypeConfigProto of schema_type name.
  //
  // Returns:
  //   SchemaTypeConfigProto on success
  //   FAILED_PRECONDITION if schema hasn't been set yet
  //   NOT_FOUND if schema type name doesn't exist
  //   INTERNAL on any I/O errors
  libtextclassifier3::StatusOr<const SchemaTypeConfigProto*>
  GetSchemaTypeConfig(std::string_view schema_type) const;

  // Get a map contains all schema_type name to its blob property paths.
  //
  // Returns:
  //   A map contains all schema_type name to its blob property paths on success
  //   FAILED_PRECONDITION if schema hasn't been set yet
  //   INTERNAL on any I/O errors
  libtextclassifier3::StatusOr<
      std::unordered_map<std::string, std::vector<std::string>>>
  ConstructBlobPropertyMap() const;

  // Returns the schema type of the passed in SchemaTypeId
  //
  // Returns:
  //   schema type on success
  //   FAILED_PRECONDITION if schema hasn't been set yet
  //   INVALID_ARGUMENT if schema type id is invalid
  libtextclassifier3::StatusOr<const std::string*> GetSchemaType(
      SchemaTypeId schema_type_id) const;

  // Returns the SchemaTypeId of the passed in schema type
  //
  // Returns:
  //   SchemaTypeId on success
  //   FAILED_PRECONDITION if schema hasn't been set yet
  //   NOT_FOUND_ERROR if we don't know about the schema type
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::StatusOr<SchemaTypeId> GetSchemaTypeId(
      std::string_view schema_type) const;

  // Similar to GetSchemaTypeId but will return a set of SchemaTypeId to also
  // include child types.
  //
  // Returns:
  //   A set of SchemaTypeId on success
  //   FAILED_PRECONDITION if schema hasn't been set yet
  //   NOT_FOUND_ERROR if we don't know about the schema type
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::StatusOr<const std::unordered_set<SchemaTypeId>*>
  GetSchemaTypeIdsWithChildren(std::string_view schema_type) const;

  // Returns the SectionMetadata associated with the SectionId that's in the
  // SchemaTypeId.
  //
  // Returns:
  //   Valid pointer to SectionMetadata on success
  //   FAILED_PRECONDITION if schema hasn't been set yet
  //   INVALID_ARGUMENT if schema type id or section id is invalid
  libtextclassifier3::StatusOr<const SectionMetadata*> GetSectionMetadata(
      SchemaTypeId schema_type_id, SectionId section_id) const;

  // Returns true if a property is defined in the said schema, regardless of
  // whether it is indexed or not.
  bool IsPropertyDefinedInSchema(SchemaTypeId schema_type_id,
                                 const std::string& property) const;

  // Extracts all sections of different types from the given document and group
  // them by type.
  // - Each Section vector is sorted by section Id in ascending order. The
  //   sorted section ids may not be continuous, since not all sections are
  //   present in the document.
  // - Sections with empty content won't be returned.
  // - For example, we may extract:
  //   string_sections: [2, 7, 10]
  //   integer_sections: [3, 5, 8]
  //
  // Returns:
  //   A SectionGroup instance on success
  //   FAILED_PRECONDITION if schema hasn't been set yet
  //   NOT_FOUND if type config name of document not found
  libtextclassifier3::StatusOr<SectionGroup> ExtractSections(
      const DocumentProto& document) const;

  // Returns the JoinablePropertyMetadata associated with property_path that's
  // in the SchemaTypeId.
  //
  // Returns:
  //   Valid pointer to JoinablePropertyMetadata on success
  //   nullptr if property_path doesn't exist (or is not joinable) in the
  //     joinable metadata list of the schema
  //   FAILED_PRECONDITION if schema hasn't been set yet
  //   INVALID_ARGUMENT if schema type id is invalid
  libtextclassifier3::StatusOr<const JoinablePropertyMetadata*>
  GetJoinablePropertyMetadata(SchemaTypeId schema_type_id,
                              const std::string& property_path) const;

  // Returns the JoinablePropertyMetadata associated with joinable_property_id
  // that's in the SchemaTypeId.
  //
  // Returns:
  //   Valid pointer to JoinablePropertyMetadata on success
  //   FAILED_PRECONDITION if schema hasn't been set yet
  //   INVALID_ARGUMENT if schema type id or joinable property id is invalid
  libtextclassifier3::StatusOr<const JoinablePropertyMetadata*>
  GetJoinablePropertyMetadata(SchemaTypeId schema_type_id,
                              JoinablePropertyId joinable_property_id) const;

  // Extracts all joinable property contents of different types from the given
  // document and group them by joinable value type.
  // - Joinable properties are sorted by joinable property id in ascending
  //   order. The sorted joinable property ids may not be continuous, since not
  //   all joinable properties are present in the document.
  // - Joinable property ids start from 0.
  // - Joinable properties with empty content won't be returned.
  //
  // Returns:
  //   A JoinablePropertyGroup instance on success
  //   FAILED_PRECONDITION if schema hasn't been set yet
  //   NOT_FOUND if the type config name of document not found
  libtextclassifier3::StatusOr<JoinablePropertyGroup> ExtractJoinableProperties(
      const DocumentProto& document) const;

  // Returns the quantization type for the given schema_type_id and section_id.
  //
  // Returns:
  //   - The quantization type on success.
  //   - INVALID_ARGUMENT_ERROR if schema_type_id or section_id is invalid.
  //   - Any error from schema store.
  libtextclassifier3::StatusOr<EmbeddingIndexingConfig::QuantizationType::Code>
  GetQuantizationType(SchemaTypeId schema_type_id, SectionId section_id) const {
    ICING_ASSIGN_OR_RETURN(const SectionMetadata* section_metadata,
                           GetSectionMetadata(schema_type_id, section_id));
    return section_metadata->quantization_type;
  }

  // Syncs all the data changes to disk.
  //
  // Returns:
  //   OK on success
  //   INTERNAL on I/O errors.
  libtextclassifier3::Status PersistToDisk();

  // Recomputes the combined checksum of components of the schema store and
  // updates the header.
  //
  // Returns:
  //   - the checksum on success
  //   - INTERNAL on I/O errors.
  libtextclassifier3::StatusOr<Crc32> UpdateChecksum();

  // Recomputes the combined checksum of components of the schema store. Does
  // NOT update the header.
  //
  // Returns:
  //   - the checksum on success
  //   - INTERNAL on I/O errors.
  libtextclassifier3::StatusOr<Crc32> GetChecksum() const;

  // Returns:
  //   - On success, the section metadata list for the specified schema type
  //   - NOT_FOUND if the schema type is not present in the schema
  libtextclassifier3::StatusOr<const std::vector<SectionMetadata>*>
  GetSectionMetadata(const std::string& schema_type) const;

  // Gets the index of the given |property_path|, where the index N means that
  // it is the Nth scorable property path in the schema config of the given
  // |schema_type_id|, in lexicographical order.
  //
  // Returns:
  //   - Index on success
  //   - std::nullopt if the |property_path| doesn't point to a scorable
  //     property under the |schema_type_id|
  //   - FAILED_PRECONDITION if the schema hasn't been set yet
  //   - INVALID_ARGUMENT if |schema_type_id| is invalid
  libtextclassifier3::StatusOr<std::optional<int>> GetScorablePropertyIndex(
      SchemaTypeId schema_type_id, std::string_view property_path) const;

  // Returns the list of ScorablePropertyInfo for the given |schema_type_id|,
  // in lexicographical order of its property path.
  //
  // Returns:
  //   - Vector of scorable property info on success. The vector can be empty
  //     if no scorable property is found under the schema config of
  //     |schema_type_id|.
  //   - FAILED_PRECONDITION if the schema hasn't been set yet
  //   - INVALID_ARGUMENT if |schema_type_id| is invalid
  libtextclassifier3::StatusOr<
      const std::vector<ScorablePropertyManager::ScorablePropertyInfo>*>
  GetOrderedScorablePropertyInfo(SchemaTypeId schema_type_id) const;

  // Calculates the StorageInfo for the Schema Store.
  //
  // If an IO error occurs while trying to calculate the value for a field, then
  // that field will be set to -1.
  SchemaStoreStorageInfoProto GetStorageInfo() const;

  // Get debug information for the schema store.
  //
  // Returns:
  //   SchemaDebugInfoProto on success
  //   INTERNAL_ERROR on IO errors, crc compute error
  libtextclassifier3::StatusOr<SchemaDebugInfoProto> GetDebugInfo() const;

  // Expands the provided type_property_masks into a vector of
  // ExpandedTypePropertyMasks to account for polymorphism. If both a parent
  // type and one of its child type appears in the masks, the parent type's
  // paths will be merged into the child's.
  //
  // For example, assume that we have two schema types A and B, and we have
  // - A is the parent type of B
  // - Paths of A: {P1, P2}
  // - Paths of B: {P3}
  //
  // Then, we will have the following in the result.
  // - Expanded paths of A: {P1, P2}
  // - Expanded paths of B: {P1, P2, P3}
  std::vector<ExpandedTypePropertyMask> ExpandTypePropertyMasks(
      const google::protobuf::RepeatedPtrField<TypePropertyMask>& type_property_masks)
      const;

 private:
  // Factory function to create a SchemaStore and set its schema. The created
  // instance does not take ownership of any input components and all pointers
  // must refer to valid objects that outlive the created SchemaStore instance.
  // The base_dir must already exist. No schema must have set in base_dir prior
  // to this.
  //
  // Returns:
  //   A SchemaStore on success
  //   FAILED_PRECONDITION on any null pointer input or if there has already
  //       been a schema set for this path.
  //   INTERNAL_ERROR on any IO errors
  static libtextclassifier3::StatusOr<std::unique_ptr<SchemaStore>> Create(
      const Filesystem* filesystem, const std::string& base_dir,
      const Clock* clock, const FeatureFlags* feature_flags, SchemaProto schema,
      bool enable_schema_database);

  // Use SchemaStore::Create instead.
  explicit SchemaStore(const Filesystem* filesystem, std::string base_dir,
                       const Clock* clock, const FeatureFlags* feature_flags,
                       bool enable_schema_database);

  // Deletes the overlay schema and ensures that the Header is correctly set.
  //
  // RETURNS:
  //   OK on success
  //   INTERNAL_ERROR on any IO errors
  static libtextclassifier3::Status DiscardOverlaySchema(
      const Filesystem* filesystem, const std::string& base_dir,
      Header& header);

  // Handles the overlay schema after a version change by deleting it if it is
  // no longer compatible with the new version.
  //
  // Requires: base_dir exists.
  //
  // Returns:
  //   OK on success
  //   INTERNAL_ERROR on any IO errors
  static libtextclassifier3::Status HandleOverlaySchemaForVersionChange(
      const Filesystem* filesystem, const std::string& base_dir,
      version_util::StateChange version_state_change, int32_t new_version);

  // Populates the schema database field in the schema proto that is stored in
  // the input schema file.
  //
  // Returns:
  //   OK on success or nothing to migrate
  //   INTERNAL_ERROR on IO error
  static libtextclassifier3::Status PopulateSchemaDatabaseFieldForSchemaFile(
      const Filesystem* filesystem, const std::string& schema_filename);

  // Verifies that there is no error retrieving a previously set schema. Then
  // initializes like normal.
  //
  // Returns:
  //   OK on success
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::Status Initialize(InitializeStatsProto* initialize_stats);

  // First, blindly writes new_schema to the schema_file. Then initializes like
  // normal.
  //
  // Returns:
  //   OK on success
  //   INTERNAL_ERROR on IO error
  //   FAILED_PRECONDITION if there is already a schema set for the schema_file.
  libtextclassifier3::Status Initialize(SchemaProto new_schema);

  // Handles initializing the SchemaStore and regenerating any data if needed.
  //
  // Returns:
  //   OK on success
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::Status InitializeInternal(
      bool create_overlay_if_necessary, InitializeStatsProto* initialize_stats);

  // Creates sub-components and verifies the integrity of each sub-component.
  //
  // Returns:
  //   OK on success
  //   INTERNAL_ERROR on IO error
  libtextclassifier3::Status InitializeDerivedFiles();

  // Populates any derived data structures off of the schema.
  //
  // Returns:
  //   OK on success
  //   NOT_FOUND_ERROR if a schema proto has not been set
  //   INTERNAL_ERROR on any IO errors
  libtextclassifier3::Status RegenerateDerivedFiles(
      bool create_overlay_if_necessary);

  // Build type_config_map_, schema_subtype_id_map_, and schema_type_manager_.
  //
  // Returns:
  //   OK on success
  //   NOT_FOUND_ERROR if a schema proto has not been set
  //   INTERNAL_ERROR on any IO errors
  libtextclassifier3::Status BuildInMemoryCache();

  // Update and replace the header file. Creates the header file if it doesn't
  // exist.
  //
  // Returns:
  //   OK on success
  //   INTERNAL on I/O error
  libtextclassifier3::Status UpdateHeader(const Crc32& checksum);

  // Resets the unique_ptr to the schema_type_mapper_, deletes the underlying
  // file, and re-creates a new instance of the schema_type_mapper_. Does not
  // populate the schema_type_mapper_.
  //
  // Returns any IO errors.
  libtextclassifier3::Status ResetSchemaTypeMapper();

  // Creates a new schema store with new_schema and then swaps that new schema
  // store with the existing one. This function guarantees that either: this
  // instance will be fully updated to the new schema or no changes will take
  // effect.
  //
  // Returns:
  //   OK on success
  //   INTERNAL on I/O error.
  libtextclassifier3::Status ApplySchemaChange(SchemaProto new_schema);

  libtextclassifier3::Status CheckSchemaSet() const {
    return has_schema_successfully_set_
               ? libtextclassifier3::Status::OK
               : absl_ports::FailedPreconditionError("Schema not set yet.");
  }

  // Correctly loads the Header, schema_file_ and (if present) the
  // overlay_schema_file_.
  // RETURNS:
  //   - OK on success
  //   - INTERNAL if an IO error is encountered when reading the Header or
  //   schemas.
  //     Or an invalid schema configuration is present.
  libtextclassifier3::Status LoadSchema();

  // Sets the schema for a database for the first time.
  //
  // Note that when schema database is disabled, this function sets the entire
  // schema, with all under the default empty database.
  //
  // Requires:
  //   - All types in new_schema are from the same database.
  //   - new_schema does not contain type names that are already in use by a
  //     different database.
  //
  // Returns:
  //   - SetSchemaResult that indicates if the new schema can be set.
  //   - INTERNAL_ERROR on any IO errors.
  //   - INVALID_ARGUMENT_ERROR if the schema is invalid.
  libtextclassifier3::StatusOr<SchemaStore::SetSchemaResult>
  SetInitialSchemaForDatabase(SchemaProto new_schema,
                              bool ignore_errors_and_delete_documents,
                              bool allow_circular_schema_definitions);

  // Sets the schema for a database, overriding any existing schema for that
  // database.
  //
  // Note that when schema database is disabled, this function sets and
  // overrides the entire schema.
  //
  // Requires:
  //   - All types in new_schema are from the same database.
  //   - new_schema does not contain type names that are already in use by a
  //     different database.
  //
  // Returns:
  //   - SetSchemaResult that encapsulates the differences between the old and
  //     new schema, as well as if the new schema can be set.
  //   - INTERNAL_ERROR on any IO errors.
  //   - INVALID_ARGUMENT_ERROR if the schema is invalid.
  libtextclassifier3::StatusOr<SchemaStore::SetSchemaResult>
  SetSchemaWithDatabaseOverride(SchemaProto new_schema,
                                const SchemaProto& old_schema,
                                bool ignore_errors_and_delete_documents,
                                bool allow_circular_schema_definitions);

  // Initial validation on the SchemaProto for SetSchema. This is intended as a
  // preliminary check before any expensive operations are performed during
  // `SetSchema::Validate`. Returns the schema's database if it's valid.
  //
  // Note that when schema database is disabled, any schema input is valid and
  // an empty string is returned as the database.
  //
  // Checks that:
  // - The new schema only contains types from a single database.
  // - The schema's type names are not already in use in other databases. This
  //   is done outside of `SchemaUtil::Validate` because we need to know all
  //   existing type names, which is stored in the SchemaStore and not known to
  //   SchemaUtil.
  //
  // Returns:
  //   - new_schema's database on success
  //   - INVALID_ARGUMENT_ERROR if new_schema contains types from multiple
  //     databases
  //   - ALREADY_EXISTS_ERROR if new_schema's types names are not unique
  libtextclassifier3::StatusOr<std::string> ValidateAndGetDatabase(
      const SchemaProto& new_schema) const;

  // Returns a SchemaProto representing the full schema, which is a combination
  // of the existing schema and the input database schema.
  //
  // For the database being updated by the input database schema:
  // - If the existing schema does not contain the database, the input types
  //   are appended to the end of the SchemaProto, without changing the order
  //   of the existing schema types.
  // - Otherwise, the existing schema types are replaced with types from the
  //   input database schema in their original position in the existing
  //   SchemaProto.
  //   - Types from input_database_schema are added in the order in which they
  //     appear.
  //   - If more types are added to the database, the additional types are
  //     appended at the end of the SchemaProto, without changing the order of
  //     existing types from unaffected databases.
  //
  // Requires:
  //   - input_database_schema must not contain types from multiple databases.
  //
  // Returns:
  //   - SchemaProto on success
  //   - INTERNAL_ERROR on any IO errors, or if the schema store was not
  //     previously initialized properly.
  //   - INVALID_ARGUMENT_ERROR if the input schema contains types from multiple
  //     databases.
  libtextclassifier3::StatusOr<SchemaProto> GetFullSchemaProtoWithUpdatedDb(
      SchemaProto input_database_schema) const;

  const Filesystem* filesystem_;
  std::string base_dir_;
  const Clock* clock_;
  const FeatureFlags* feature_flags_;  // Does not own.

  // Used internally to indicate whether the class has been successfully
  // initialized with a valid schema. Will be false if Initialize failed or no
  // schema has ever been set.
  bool has_schema_successfully_set_ = false;

  // Cached schema
  std::unique_ptr<FileBackedProto<SchemaProto>> schema_file_;

  // This schema holds the definition of any schema types that are not
  // compatible with older versions of Icing code.
  std::unique_ptr<FileBackedProto<SchemaProto>> overlay_schema_file_;

  // Maps schema types to a densely-assigned unique id.
  std::unique_ptr<KeyMapper<SchemaTypeId>> schema_type_mapper_;

  // Maps schema type ids to the corresponding schema type. This is an inverse
  // map of schema_type_mapper_.
  std::unordered_map<SchemaTypeId, std::string> reverse_schema_type_mapper_;

  // A hash map of (database -> vector of type config names in the database).
  //
  // We use a vector instead of a set because we need to preserve the order of
  // the types (i.e. the order in which they appear in the input SchemaProto
  // during SetSchema), so that we can return the correct SchemaProto for
  // GetSchema.
  //
  // This keeps track of the type configs defined in each database, which allows
  // schema operations to be performed on a per-database basis.
  std::unordered_map<std::string, std::vector<std::string>> database_type_map_;

  // A hash map of (type config name -> type config), allows faster lookup of
  // type config in schema. The O(1) type config access makes schema-related and
  // section-related operations faster.
  SchemaUtil::TypeConfigMap type_config_map_;

  // Maps from each type id to all of its subtype ids.
  // T2 is a subtype of T1, if and only if one of the following conditions is
  // met:
  // - T2 is T1
  // - T2 extends T1
  // - There exists a type U, such that T2 is a subtype of U, and U is a subtype
  //   of T1
  std::unordered_map<SchemaTypeId, std::unordered_set<SchemaTypeId>>
      schema_subtype_id_map_;

  // Manager of section (indexable property) and joinable property related
  // metadata for all Schemas.
  std::unique_ptr<const SchemaTypeManager> schema_type_manager_;

  // Used to cache and manage the schema's scorable properties.
  std::unique_ptr<ScorablePropertyManager> scorable_property_manager_;

  std::unique_ptr<Header> header_;

  // Whether to use the database field for the schema.
  //
  // This is a temporary flag to control the rollout of the schema database. It
  // affects the `SetSchema` and `GetSchema(std::string database)` methods.
  // TODO - b/337913932: Remove this flag once the schema database is fully
  // rolled out.
  bool enable_schema_database_ = false;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_SCHEMA_SCHEMA_STORE_H_
