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
#include "icing/absl_ports/str_cat.h"
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
#include "icing/util/logging.h"
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

    void SetSwappedFilepath(std::string path) { path_ = std::move(path); }

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

    // Byte size of the full schema proto written by this SetSchema call.
    int64_t schema_proto_byte_size = 0;
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
      InitializeStatsProto* initialize_stats = nullptr);

  // Migrates schema files (backup vs. new schema) based on version changes and
  // feature flag updates. This includes:
  // - Handling overlay schema: If the overlay schema is incompatible with
  //   the new version or if deduping is rolled back, it's discarded.
  // - Schema database migration: If `perform_schema_database_migration` is
  //   true, rewrites the schema file to populate the `database` field for
  //   schema types.
  // - Recalculating properties digests: If `recalculate_properties_digests` is
  //   true, rewrites the schema to compute and populate `properties_digests`
  //   to support schema deduplication.
  //
  // Returns:
  //   OK on success or nothing to migrate
  static libtextclassifier3::Status MigrateSchema(
      const Filesystem* filesystem, const std::string& base_dir,
      version_util::StateChange version_state_change, int32_t new_version,
      bool perform_schema_database_migration,
      bool recalculate_properties_digests, bool schema_deduping_flag_rollback);

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

  // Retrieves the current schema stored in the file-backed schema proto.
  //
  // Note: When enable_schema_definition_deduping is enabled, this method should
  // only be used if you don't need the full schema property definitions in
  // SchemaTypeConfigProto.properties. Otherwise, use `GetFullSchemaProto()`.
  //
  // Returns:
  //   - SchemaProto* if exists
  //   - INTERNAL_ERROR on any IO errors
  //   - NOT_FOUND_ERROR if a schema hasn't been set before
  libtextclassifier3::StatusOr<const SchemaProto*> GetFileBackedSchemaProto()
      const;

  // Retrieves the full schema proto, with full schema type config definitions
  // that contains all property definitions.
  //
  // Returns:
  //   - SchemaProto if exists
  //   - INTERNAL_ERROR on any IO errors
  //   - NOT_FOUND_ERROR if a schema hasn't been set before
  libtextclassifier3::StatusOr<SchemaProto> GetFullSchemaProto() const;

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
  // NOTE: This method is deprecated. Please use
  // `SetSchema(SetSchemaRequestProto&& set_schema_request)` instead.
  //
  // TODO: b/337913932 - Remove this method once all callers (currently only
  // used in tests) are migrated to the new SetSchema method that takes a
  // SetSchemaRequestProto.
  libtextclassifier3::StatusOr<SetSchemaResult> SetSchema(
      SchemaProto new_schema, bool ignore_errors_and_delete_documents);

  // Update our current schema if it's compatible. Does not accept incompatible
  // schema or schema subsets with types from multiple databases. Compatibility
  // rules defined by SchemaUtil::ComputeCompatibilityDelta.
  //
  // This method accepts either a full schema (indicated by an empty database
  // field) or a schema subset with types from a single database.
  // - If `set_schema_request.database()` is non-empty, then all types in the
  //   new schema must have their `database` field matching
  //   `set_schema_request.database()`.
  // - If `set_schema_request.database()` is empty, then the new schema will be
  //   taken as the full schema, and will replace the entire existing schema.
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
  //   - INVALID_ARGUMENT_ERROR if the schema is invalid. This can happen if
  //     the schema is malformed, if the new schema contains types where the
  //     database field does not match the database field in the
  //     set_schema_request.
  libtextclassifier3::StatusOr<SetSchemaResult> SetSchema(
      SetSchemaRequestProto&& set_schema_request);

  // TODO - b/448166747: Remove this method once
  // enable_schema_definition_deduping is fully rolled out.
  //
  // DEPRECATED: This method should not be called, especially when
  // feature_flags_->enable_schema_definition_deduping()` is true.
  // Use GetSchemaTypeConfig(std::string_view schema_type) instead.
  //
  // Gets a pointer to the SchemaTypeConfigProto of schema_type name stored in
  // the schema store.
  // -  With schema deduplication enabled, this pointer points to the internal
  //    SchemaTypeConfigProto that schema store holds after deduplication, which
  //    could be have all property definitions removed.
  //
  // Returns:
  //   - SchemaTypeConfigProto* on success
  //   - FAILED_PRECONDITION if schema hasn't been set yet
  //   - NOT_FOUND if schema type name doesn't exist
  //   - INTERNAL on any I/O errors or if called when schema deduplication is
  //     enabled.
  libtextclassifier3::StatusOr<const SchemaTypeConfigProto*>
  GetSchemaTypeConfigPointer(std::string_view schema_type) const;

  // Fetches a TypeConfigHolder with a unified view of the base
  // SchemaTypeConfigProto and its full properties definitions.
  //
  // LIFETIME: The returned Holder contains references to data owned by this
  // cache. It must not outlive the TypeConfigInfoCache or the specific type
  // config within it.
  //
  // Returns:
  //   - A TypeConfigHolder providing a non-owning view to the full type
  //     definition on success.
  //   - FAILED_PRECONDITION if schema hasn't been set yet
  //   - NOT_FOUND if the schema_type does not exist in the cache.
  //   - INTERNAL_ERROR on any I/O or deserialization errors.
  libtextclassifier3::StatusOr<
      SchemaUtil::TypeConfigInfoCache::TypeConfigHolder>
  GetSchemaTypeConfigHolder(std::string_view schema_type) const;

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

  // Returns the hash of a schema name.
  static uint32_t GetSchemaNameHash(std::string_view schema_name) {
    return Crc32(schema_name).Get();
  }

  // Returns the hash of the schema name for the given schema type id.
  //
  // Returns:
  //   - The hash value on success.
  //   - INVALID_ARGUMENT_ERROR if schema_type_id is invalid.
  libtextclassifier3::StatusOr<uint32_t> GetSchemaNameHash(
      SchemaTypeId schema_type_id) const {
    auto it = reverse_schema_type_mapper_hash_.find(schema_type_id);
    if (it == reverse_schema_type_mapper_hash_.end()) {
      return absl_ports::InvalidArgumentError(absl_ports::StrCat(
          "Invalid SchemaTypeId ", std::to_string(schema_type_id)));
    }
    return it->second;
  }

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
      const Clock* clock, const FeatureFlags* feature_flags,
      SchemaProto schema);

  // Use SchemaStore::Create instead.
  explicit SchemaStore(const Filesystem* filesystem, std::string base_dir,
                       const Clock* clock, const FeatureFlags* feature_flags);

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
      version_util::StateChange version_state_change, int32_t new_version,
      bool schema_deduping_flag_rollback);

  // Rewrites the schema file on disk by recomputing and updating its metadata
  // fields as specified.
  //
  // Currently, the metadata fields that can be updated are:
  //  - `database` field: if `update_database_field` is true.
  //  - `properties_digest` field: if `update_properties_digest` is true.
  //
  // Returns:
  //   OK on success or nothing to migrate
  //   INTERNAL_ERROR on IO error
  static libtextclassifier3::Status RewriteSchemaFileMetadataFields(
      bool update_database_field, bool update_properties_digest_field,
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
  //
  // If feature_flags_->release_backup_schema_file_after_initialization() is
  // true, then schema_file_ will be released if the overlay_schema_file_ is
  // present.
  //
  // RETURNS:
  //   - OK on success
  //   - INTERNAL if an IO error is encountered when reading the Header or
  //   schemas.
  //     Or an invalid schema configuration is present.
  libtextclassifier3::Status LoadSchema();

  // Returns the size of the schema proto in bytes.
  int64_t GetStoredSchemaProtoByteSize() const;

  // Resets the schema_file_'s cached FileBackedProto instance if needed.
  //
  // This is the case if the overlay_schema_file_ is present.
  void ResetSchemaFileIfNeeded() {
    if (overlay_schema_file_ != nullptr) {
      ICING_VLOG(2)
          << "Freeing schema store's base schema file's "
             "FileBackedProto instance since overlay_schema_file_ is present.";
      schema_file_.ReleaseCachedSchemaFile();
    }
  }

  // Sets the schema for a database for the first time.
  //
  // Note that when schema database is disabled, this function sets the entire
  // schema, with all types under the default empty database.
  //
  // Requires:
  //   - `new_schema` is valid according to `ValidateSchemaDatabase'
  //
  // Returns:
  //   - SetSchemaResult that indicates if the new schema can be set.
  //   - INTERNAL_ERROR on any IO errors.
  //   - INVALID_ARGUMENT_ERROR if the new schema is invalid.
  libtextclassifier3::StatusOr<SchemaStore::SetSchemaResult>
  SetInitialSchemaForDatabase(SchemaProto new_schema,
                              const std::string& database);

  // Sets the schema for a database, overriding any existing schema for that
  // database.
  //
  // Note that when schema database is disabled, this function sets and
  // overrides the entire schema.
  //
  // Requires:
  //   - `new_schema` and `database` are valid according to
  //     `ValidateSchemaDatabase(new_schema, database)`
  //   - `database` is not empty.
  //   - Types in `new_schema` and `old_schema` all belong to the provided
  //     database.
  //     - The old schema is guaranteed to contain types from exactly one
  //       database when schema database is enabled, because it was obtained
  //       using `GetSchema(database)`.
  //
  // Returns:
  //   - SetSchemaResult that encapsulates the differences between the old and
  //     new schema, as well as if the new schema can be set.
  //   - INTERNAL_ERROR on any IO errors.
  //   - INVALID_ARGUMENT_ERROR if the schema is invalid, or if there are
  //     mismatches between the schema databases.
  libtextclassifier3::StatusOr<SchemaStore::SetSchemaResult>
  SetSchemaWithDatabaseOverride(SchemaProto new_schema,
                                const SchemaProto& old_schema,
                                const std::string& database,
                                bool ignore_errors_and_delete_documents);

  // Initial validation on the SchemaProto for SetSchema. This is intended as a
  // preliminary check before any expensive operations are performed during
  // `SetSchema::Validate`. Returns the schema's database if it's valid.
  //
  // Note that when schema database is disabled, any schema input is valid and
  // an empty string is returned as the database.
  //
  // Checks that:
  // - The new schema only contains types from a single database, which matches
  //   the provided database.
  // - The schema's type names are not already in use in other databases. This
  //   is done outside of `SchemaUtil::Validate` because we need to know all
  //   existing type names, which is stored in the SchemaStore and not known to
  //   SchemaUtil.
  //
  // Returns:
  //   - OK on success
  //   - INVALID_ARGUMENT_ERROR if new_schema.types's databases do not match the
  //     provided database.
  //   - ALREADY_EXISTS_ERROR if new_schema's types names are not unique
  libtextclassifier3::Status ValidateSchemaDatabase(
      const SchemaProto& new_schema, const std::string& database) const;

  // Returns a SchemaProto representing the full schema, which is a combination
  // of the existing schema and the input database schema. The returned
  // SchemaProto is optimized to preserve as many type ids as possible.
  //
  // Note that `database_to_update` could also be the empty string, which means
  // that the entire schema is being updated. In this case,
  // `input_database_schema` must contain all types in the full schema. Any
  // preexisting type not in `input_database_schema` will be deleted.
  //
  // For database_to_update, we replace the existing types with the input
  //   types. Any existing type not included in input_database_schema is
  //   deleted.
  // - When possible, existing types are added in the position in which they
  //   appear in the existing schema so as to preserve the type-ids of
  //   existing types.
  // - If there are more input types than existing types for
  //   database_to_update, added input types are appended to the end of the
  //   full_schema.
  // - If there are fewer input types than existing types for
  //   database_to_update, we use the last few input types to replace the
  //   deleted existing types, so as to preserve as many old type-ids as
  //   possible.
  // - For existing types from other databases, we preserve the existing order
  //   after adding to full_schema. Note that the type-ids of existing types
  //   might still change if some types are deleted in the database_to_update
  //   as this will cause all subsequent types ids to shift forward.
  // - This means that:
  //   - When adding types to a database, the type-ids of existing types will
  //     not change.
  //   - When types are deleted, we fill their original slots with the last
  //     valid types in the schema to preserve as many type-ids as possible.
  // - If input_database_schema is an empty proto, then all types from
  //   database_to_update are deleted.
  //
  // If `enable_schema_definition_deduping` is true, then the returned
  // SchemaProto's type configs will be deduped.
  //
  // Requires:
  //   - input_database_schema is valid according to `ValidateSchemaDatabase`.
  //   - `schema_delta` is the real schema delta between the existing schema and
  //     the input schema computed using `SchemaUtil::ComputeSchemaDelta`.
  //
  // Returns:
  //   - SchemaProto on success
  //   - INTERNAL_ERROR on any IO errors, or if the schema store was not
  //     previously initialized properly.
  //   - INVALID_ARGUMENT_ERROR if the input schema does not match
  //     database_to_update.
  libtextclassifier3::StatusOr<SchemaProto> GetFullOptimizedSchemaProto(
      SchemaProto input_database_schema, const std::string& database_to_update,
      const SchemaUtil::SchemaDelta& schema_delta) const;

  // Merges new types into the existing schema and returns a deduped
  // SchemaProto.
  //
  // Requires:
  //   - `new_types_vector` is type config vector constructed from a schema
  //     that is valid according to `ValidateSchemaDatabase` and
  //     `SchemaUtil::Validate`.
  //   - `schema_delta` is the real schema delta between the existing schema and
  //     the schema represented by `new_types_vector` computed using
  //     `SchemaUtil::ComputeSchemaDelta`.
  //
  // Returns:
  //   - SchemaProto on success
  //   - INTERNAL_ERROR on any IO errors, or if the schema store was not
  //     previously initialized properly.
  libtextclassifier3::StatusOr<SchemaProto> BuildDedupedSchemaProto(
      std::vector<SchemaTypeConfigProto>&& new_types_vector,
      const SchemaUtil::SchemaDelta& schema_delta) const;

  const Filesystem* filesystem_;
  std::string base_dir_;
  const Clock* clock_;
  const FeatureFlags* feature_flags_;  // Does not own.

  // Used internally to indicate whether the class has been successfully
  // initialized with a valid schema. Will be false if Initialize failed or no
  // schema has ever been set.
  bool has_schema_successfully_set_ = false;

  // Wrapper class to store a cached schema file FileBackedProto instance and
  // its checksum.
  class SchemaFileCache {
   public:
    explicit SchemaFileCache(const Filesystem* filesystem,
                             const std::string& schema_file_path)
        : filesystem_(filesystem), schema_file_path_(schema_file_path) {}
    // Returns a reference to the proto read from the schema FileBackedProto.
    //
    // NOTE: The caller does NOT get ownership of the object returned and
    // the returned object is only valid till a new version of the proto is
    // written to the file.
    //
    // Returns NOT_FOUND if the file was empty or never written to.
    // Returns INTERNAL_ERROR if an IO error or a corruption was encountered.
    libtextclassifier3::StatusOr<const SchemaProto*> Read() {
      return GetCachedSchemaFile().Read();
    }

    // Writes the new schema_proto to schema_file_ and updates the cached
    // checksum.
    //
    // Returns: INTERNAL_ERROR if any IO error is encountered.
    libtextclassifier3::Status Write(
        std::unique_ptr<SchemaProto> schema_proto) {
      ICING_RETURN_IF_ERROR(
          GetCachedSchemaFile().Write(std::move(schema_proto)));
      ICING_ASSIGN_OR_RETURN(Crc32 checksum,
                             GetCachedSchemaFile().GetChecksum());
      checksum_ = std::make_unique<Crc32>(checksum);
      return libtextclassifier3::Status::OK;
    }

    // Sets the swapped_to_file_path for the cached schema_file_ instance and
    // the schema_file_path_.
    void SetSwappedFilepath(std::string new_schema_file_path) {
      if (schema_file_ != nullptr) {
        schema_file_->SetSwappedFilepath(new_schema_file_path);
      }
      schema_file_path_ = std::move(new_schema_file_path);
    }

    // Releases the cached schema_file_ FileBackedProto instance.
    void ReleaseCachedSchemaFile() { schema_file_.reset(); }

    libtextclassifier3::StatusOr<Crc32> GetChecksum() {
      if (checksum_ == nullptr) {
        ICING_ASSIGN_OR_RETURN(Crc32 checksum,
                               GetCachedSchemaFile().GetChecksum());
        checksum_ = std::make_unique<Crc32>(std::move(checksum));
      }
      return *checksum_;
    }

   private:
    FileBackedProto<SchemaProto>& GetCachedSchemaFile() {
      if (schema_file_ == nullptr) {
        schema_file_ = std::make_unique<FileBackedProto<SchemaProto>>(
            *filesystem_, schema_file_path_);
      }
      return *schema_file_;
    }

    const Filesystem* filesystem_;
    std::string schema_file_path_;
    std::unique_ptr<FileBackedProto<SchemaProto>> schema_file_;
    std::unique_ptr<Crc32> checksum_;
  };

  // Caches a FileBackedProto instance and the checksum for the schema file.
  //
  // If the overlay_schema_file_ is present, then the cached schema
  // FileBackedProto instance should be released and reloaded only during
  // mutating SetSchema operations.
  mutable SchemaFileCache schema_file_;

  // This schema holds the definition of any schema types that are not
  // compatible with older versions of Icing code.
  std::unique_ptr<FileBackedProto<SchemaProto>> overlay_schema_file_;

  // Maps schema types to a densely-assigned unique id.
  std::unique_ptr<KeyMapper<SchemaTypeId>> schema_type_mapper_;

  // Maps schema type ids to the corresponding schema type. This is an inverse
  // map of schema_type_mapper_.
  std::unordered_map<SchemaTypeId, std::string> reverse_schema_type_mapper_;

  // Maps schema type ids to the hash value of the corresponding schema type
  // name.
  // TODO(b/436237337): Consider merging this with reverse_schema_type_mapper_
  // to save memory.
  std::unordered_map<SchemaTypeId, uint32_t> reverse_schema_type_mapper_hash_;

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

  // The type config info cache contains the following:
  //
  // 1. TypeConfigMap: A map of (type config name -> type config).
  //    - When schema-deduping is enabled, type configs in this map will be
  //      deduped and many may not have any property definitions.
  //    - When disabled, this map contains full type config definitions.
  //
  // 2. PropertiesDigestToTypeConfigMap
  //    - Only populated when schema-deduping is enabled.
  //    - A map of (Sha256 properties digest -> vector of type names that match
  //      that properties digest).
  //    - The first element in the vector is the name of the fully-defined type
  //      which is stored in the TypeConfigMap with full property definitions.
  //    - The remaining elements are duplicate types whose configs are stored
  //      without any property definitions.
  //
  // This cache allows faster lookup of type configs in the schema and makes
  // schema-related and section-related operations faster.
  SchemaUtil::TypeConfigInfoCache type_config_info_cache_;

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
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_SCHEMA_SCHEMA_STORE_H_
