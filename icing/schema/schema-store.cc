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

#include "icing/schema/schema-store.h"

#include <cinttypes>
#include <cstddef>
#include <cstdint>
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
#include "icing/file/destructible-directory.h"
#include "icing/file/file-backed-proto.h"
#include "icing/file/filesystem.h"
#include "icing/file/version-util.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/proto/debug.pb.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/logging.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/storage.pb.h"
#include "icing/schema/backup-schema-producer.h"
#include "icing/schema/joinable-property.h"
#include "icing/schema/schema-property-iterator.h"
#include "icing/schema/schema-type-manager.h"
#include "icing/schema/schema-util.h"
#include "icing/schema/scorable_property_manager.h"
#include "icing/schema/section.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/dynamic-trie-key-mapper.h"
#include "icing/util/clock.h"
#include "icing/util/crc32.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

constexpr char kSchemaStoreHeaderFilename[] = "schema_store_header";
constexpr char kSchemaFilename[] = "schema.pb";
constexpr char kOverlaySchemaFilename[] = "overlay_schema.pb";
constexpr char kSchemaTypeMapperFilename[] = "schema_type_mapper";

// This should be kept consistent with the delimiter used in AppSearch.
// See:
// https://cs.android.com/androidx/platform/frameworks/support/+/androidx-main:appsearch/appsearch-local-storage/src/main/java/androidx/appsearch/localstorage/util/PrefixUtil.java;l=42;drc=ffaf979c6f0cbd26caafd7a9d07a6bad12fe3a2a

constexpr char kAppSearchDatabaseDelimiter = '/';

// A DynamicTrieKeyMapper stores its data across 3 arrays internally. Giving
// each array 128KiB for storage means the entire DynamicTrieKeyMapper requires
// 384KiB.
constexpr int32_t kSchemaTypeMapperMaxSize = 3 * 128 * 1024;  // 384 KiB

std::string MakeHeaderFilename(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kSchemaStoreHeaderFilename);
}

std::string MakeSchemaFilename(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kSchemaFilename);
}

std::string MakeOverlaySchemaFilename(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kOverlaySchemaFilename);
}

std::string MakeSchemaTypeMapperFilename(const std::string& base_dir) {
  return absl_ports::StrCat(base_dir, "/", kSchemaTypeMapperFilename);
}

// Assuming that SchemaTypeIds are assigned to schema types based on their order
// in the SchemaProto. Check if the schema type->SchemaTypeId mapping would
// change with the new schema.
std::unordered_set<SchemaTypeId> SchemaTypeIdsChanged(
    const SchemaProto& old_schema, const SchemaProto& new_schema) {
  std::unordered_set<SchemaTypeId> old_schema_type_ids_changed;

  std::unordered_map<std::string, int> old_types_and_index;
  for (int i = 0; i < old_schema.types().size(); ++i) {
    old_types_and_index.emplace(old_schema.types(i).schema_type(), i);
  }

  std::unordered_map<std::string, int> new_types_and_index;
  for (int i = 0; i < new_schema.types().size(); ++i) {
    new_types_and_index.emplace(new_schema.types(i).schema_type(), i);
  }

  for (const auto& old_type_index : old_types_and_index) {
    const auto& iter = new_types_and_index.find(old_type_index.first);
    // We only care if the type exists in both the old and new schema. If the
    // type has been deleted, then it'll be captured in
    // SetSchemaResult.schema_types_deleted*. If the type has been added in the
    // new schema then we also don't care because nothing needs to be updated.
    if (iter != new_types_and_index.end()) {
      // Since the SchemaTypeId of the schema type is just the index of it in
      // the SchemaProto, compare the index and save it if it's not the same
      if (old_type_index.second != iter->second) {
        old_schema_type_ids_changed.emplace(old_type_index.second);
      }
    }
  }

  return old_schema_type_ids_changed;
}

// Returns the database from the schema type name if it exists.
//
// The schema type is expected to be in the format of
// <database><delimiter><actual_type_name>.
//
// Returns an empty string if the schema type name is not in the database
// format.
std::string GetDatabaseFromSchemaType(const std::string& schema_type,
                                      char database_delimeter) {
  size_t db_index = schema_type.find(database_delimeter);
  std::string database;
  if (db_index != std::string::npos) {
    database = schema_type.substr(0, db_index);
  }
  return database;
}

// For each schema type in the schema proto, parses out the database from the
// type name, and sets it as the database field in the input proto in
// place. The schema_type name field itself is not modified.
//
// If the schema type name does not contain an AppSearch database, then
// SchemaTypeConfigProto is not modified.
//
// Returns:
//   - True if any SchemaTypeConfigProto in the schema proto is rewritten.
//   - False otherwise.
bool ParseAndPopulateAppSearchDatabaseField(SchemaProto& schema_proto) {
  bool populated_database_field = false;
  for (auto& type : *schema_proto.mutable_types()) {
    std::string database = GetDatabaseFromSchemaType(
        type.schema_type(), kAppSearchDatabaseDelimiter);
    if (type.database() != database) {
      type.set_database(std::move(database));
      populated_database_field = true;
    }
  }
  return populated_database_field;
}

// Compares the schema types list defined in two schemas, ignoring order.
//
// Requires: old_schema.schema_database() == new_schema.schema_database()
//
// Returns: true if the types in `new_schema` are identical to the types
// in `old_schema`, otherwise returns false.
bool AreSchemaTypesEqual(const SchemaProto& old_schema,
                         const SchemaProto& new_schema) {
  if (old_schema.types().size() != new_schema.types().size()) {
    return false;
  }

  // Create a map of old schema types to and check that the new schema's types
  // are identical.
  std::unordered_map<std::string_view, const SchemaTypeConfigProto&>
      old_schema_types;
  old_schema_types.reserve(old_schema.types().size());
  for (const SchemaTypeConfigProto& old_type : old_schema.types()) {
    old_schema_types.emplace(old_type.schema_type(), old_type);
  }
  for (const SchemaTypeConfigProto& new_type : new_schema.types()) {
    auto old_type_itr = old_schema_types.find(new_type.schema_type());
    if (old_type_itr == old_schema_types.end()) {
      return false;
    }
    if (old_type_itr->second.SerializeAsString() !=
        new_type.SerializeAsString()) {
      return false;
    }
  }

  return true;
}

}  // namespace

/* static */ libtextclassifier3::StatusOr<SchemaStore::Header>
SchemaStore::Header::Read(const Filesystem* filesystem, std::string path) {
  if (!filesystem->FileExists(path.c_str())) {
    return absl_ports::NotFoundError(
        absl_ports::StrCat("Header file is empty: ", path));
  }

  SerializedHeader serialized_header;
  ScopedFd sfd(filesystem->OpenForWrite(path.c_str()));
  if (!sfd.is_valid()) {
    return absl_ports::InternalError("Unable to open or create header file.");
  }

  // If file is sizeof(LegacyHeader), then it must be LegacyHeader.
  int64_t file_size = filesystem->GetFileSize(sfd.get());
  if (file_size == sizeof(LegacyHeader)) {
    LegacyHeader legacy_header;
    if (!filesystem->Read(sfd.get(), &legacy_header, sizeof(legacy_header))) {
      return absl_ports::InternalError(
          absl_ports::StrCat("Couldn't read: ", path));
    }
    if (legacy_header.magic != Header::kMagic) {
      return absl_ports::InternalError(
          absl_ports::StrCat("Invalid header kMagic for file: ", path));
    }
    serialized_header.checksum = legacy_header.checksum;
  } else if (file_size == sizeof(SerializedHeader)) {
    if (!filesystem->Read(sfd.get(), &serialized_header,
                          sizeof(serialized_header))) {
      return absl_ports::InternalError(
          absl_ports::StrCat("Couldn't read: ", path));
    }
    if (serialized_header.magic != Header::kMagic) {
      return absl_ports::InternalError(
          absl_ports::StrCat("Invalid header kMagic for file: ", path));
    }
  } else if (file_size != 0) {
    // file is neither the legacy header, the new header nor empty. Something is
    // wrong here.
    int legacy_header_size = sizeof(LegacyHeader);
    int header_size = sizeof(SerializedHeader);
    return absl_ports::InternalError(IcingStringUtil::StringPrintf(
        "Unexpected header size %" PRId64 ". Expected %d or %d", file_size,
        legacy_header_size, header_size));
  }
  return Header(serialized_header, std::move(path), std::move(sfd), filesystem);
}

libtextclassifier3::Status SchemaStore::Header::Write() {
  if (!dirty_) {
    return libtextclassifier3::Status::OK;
  }
  if (!header_fd_.is_valid() && !filesystem_->FileExists(path_.c_str())) {
    header_fd_.reset(filesystem_->OpenForWrite(path_.c_str()));
  }
  // This should overwrite the header.
  if (!header_fd_.is_valid() ||
      !filesystem_->PWrite(header_fd_.get(), /*offset=*/0, &serialized_header_,
                           sizeof(serialized_header_))) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Failed to write SchemaStore header"));
  }
  dirty_ = false;
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status SchemaStore::Header::PersistToDisk() {
  if (dirty_) {
    ICING_RETURN_IF_ERROR(Write());
  }
  // This should overwrite the header.
  if (!header_fd_.is_valid() || !filesystem_->DataSync(header_fd_.get())) {
    return absl_ports::InternalError(
        absl_ports::StrCat("Failed to sync SchemaStore header."));
  }
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<std::unique_ptr<SchemaStore>> SchemaStore::Create(
    const Filesystem* filesystem, const std::string& base_dir,
    const Clock* clock, const FeatureFlags* feature_flags,
    InitializeStatsProto* initialize_stats) {
  ICING_RETURN_ERROR_IF_NULL(filesystem);
  ICING_RETURN_ERROR_IF_NULL(clock);
  ICING_RETURN_ERROR_IF_NULL(feature_flags);

  if (!filesystem->DirectoryExists(base_dir.c_str())) {
    return absl_ports::FailedPreconditionError(
        "Schema store base directory does not exist!");
  }
  std::unique_ptr<SchemaStore> schema_store = std::unique_ptr<SchemaStore>(
      new SchemaStore(filesystem, base_dir, clock, feature_flags));
  ICING_RETURN_IF_ERROR(schema_store->Initialize(initialize_stats));
  return schema_store;
}

libtextclassifier3::StatusOr<std::unique_ptr<SchemaStore>> SchemaStore::Create(
    const Filesystem* filesystem, const std::string& base_dir,
    const Clock* clock, const FeatureFlags* feature_flags, SchemaProto schema) {
  ICING_RETURN_ERROR_IF_NULL(filesystem);
  ICING_RETURN_ERROR_IF_NULL(clock);
  ICING_RETURN_ERROR_IF_NULL(feature_flags);

  if (!filesystem->DirectoryExists(base_dir.c_str())) {
    return absl_ports::FailedPreconditionError(
        "Schema store base directory does not exist!");
  }
  std::unique_ptr<SchemaStore> schema_store = std::unique_ptr<SchemaStore>(
      new SchemaStore(filesystem, base_dir, clock, feature_flags));
  ICING_RETURN_IF_ERROR(schema_store->Initialize(std::move(schema)));
  return schema_store;
}

/* static */ libtextclassifier3::Status
SchemaStore::PopulateSchemaDatabaseFieldForSchemaFile(
    const Filesystem* filesystem, const std::string& schema_filename) {
  FileBackedProto<SchemaProto> schema_file(*filesystem, schema_filename);
  auto schema_proto_or = schema_file.Read();
  if (absl_ports::IsNotFound(schema_proto_or.status())) {
    // Don't have an existing schema proto, that's fine
    return libtextclassifier3::Status::OK;
  } else if (!schema_proto_or.ok()) {
    // Real error when trying to read the existing schema
    return schema_proto_or.status();
  }

  SchemaProto schema_proto_copy = *schema_proto_or.ValueOrDie();
  bool schema_changed =
      ParseAndPopulateAppSearchDatabaseField(schema_proto_copy);
  if (!schema_changed) {
    // Nothing to do if the schema is not changed.
    return libtextclassifier3::Status::OK;
  }

  // Create a temporary schema file and schema proto copy to update the
  // schema.
  std::string temp_schema_filename = schema_filename + ".tmp";
  if (!filesystem->DeleteFile(temp_schema_filename.c_str())) {
    return absl_ports::InternalError(
        "Unable to delete temp schema file to prepare for schema database "
        "migration.");
  }

  {
    FileBackedProto<SchemaProto> temp_schema_file(*filesystem,
                                                  temp_schema_filename);
    ICING_RETURN_IF_ERROR(temp_schema_file.Write(
        std::make_unique<SchemaProto>(schema_proto_copy)));
  }

  // Swap the temp schema file with the original schema file.
  if (!filesystem->SwapFiles(temp_schema_filename.c_str(),
                             schema_filename.c_str())) {
    return absl_ports::InternalError(
        "Unable to apply migrated schema with database due to failed swap!");
  }
  // Clean up the temp schema file.
  if (!filesystem->DeleteFile(temp_schema_filename.c_str())) {
    return absl_ports::InternalError(
        "Unable to delete temp schema file after schema database migration.");
  }

  return libtextclassifier3::Status::OK;
}

/* static */ libtextclassifier3::Status SchemaStore::DiscardOverlaySchema(
    const Filesystem* filesystem, const std::string& base_dir, Header& header) {
  std::string header_filename = MakeHeaderFilename(base_dir);
  if (header.overlay_created()) {
    header.SetOverlayInfo(
        /*overlay_created=*/false,
        /*min_overlay_version_compatibility=*/std::numeric_limits<
            int32_t>::max());
    ICING_RETURN_IF_ERROR(header.Write());
  }
  std::string schema_overlay_filename = MakeOverlaySchemaFilename(base_dir);
  if (!filesystem->DeleteFile(schema_overlay_filename.c_str())) {
    return absl_ports::InternalError(
        "Unable to delete stale schema overlay file.");
  }
  return libtextclassifier3::Status::OK;
}

/* static */ libtextclassifier3::Status SchemaStore::MigrateSchema(
    const Filesystem* filesystem, const std::string& base_dir,
    version_util::StateChange version_state_change, int32_t new_version,
    bool perform_schema_database_migration) {
  if (!filesystem->DirectoryExists(base_dir.c_str())) {
    // Situations when schema store directory doesn't exist:
    // - Initializing new Icing instance: don't have to do anything now. The
    //   directory will be created later.
    // - Lose schema store: there is nothing we can do now. The logic will be
    //   handled later by initializing.
    //
    // Therefore, just simply return OK here.
    return libtextclassifier3::Status::OK;
  }

  ICING_RETURN_IF_ERROR(HandleOverlaySchemaForVersionChange(
      filesystem, base_dir, version_state_change, new_version));

  // Perform schema database migration if needed.
  // - This populates the the database field in the schema proto and writes it
  //   to the schema file.
  // - If the overlay schema file exists at this point, does the same for the
  //   overlay schema.
  if (perform_schema_database_migration) {
    std::string base_schema_filename = MakeSchemaFilename(base_dir);
    ICING_RETURN_IF_ERROR(PopulateSchemaDatabaseFieldForSchemaFile(
        filesystem, base_schema_filename));

    std::string overlay_schema_filename = MakeOverlaySchemaFilename(base_dir);
    if (filesystem->FileExists(overlay_schema_filename.c_str())) {
      ICING_RETURN_IF_ERROR(PopulateSchemaDatabaseFieldForSchemaFile(
          filesystem, overlay_schema_filename));
    }
  }

  return libtextclassifier3::Status::OK;
}

/* static */ libtextclassifier3::Status
SchemaStore::HandleOverlaySchemaForVersionChange(
    const Filesystem* filesystem, const std::string& base_dir,
    version_util::StateChange version_state_change, int32_t new_version) {
  std::string overlay_schema_filename = MakeOverlaySchemaFilename(base_dir);
  if (!filesystem->FileExists(overlay_schema_filename.c_str())) {
    // The overlay doesn't exist. So there should be nothing particularly
    // interesting to worry about.
    return libtextclassifier3::Status::OK;
  }

  std::string header_filename = MakeHeaderFilename(base_dir);
  libtextclassifier3::StatusOr<Header> header_or;
  switch (version_state_change) {
    // No necessary actions for normal upgrades or no version change. The data
    // that was produced by the previous version is fully compatible with this
    // version and there's no stale data for us to clean up.
    // The same is true for a normal rollforward. A normal rollforward implies
    // that the previous version was one that understood the concept of the
    // overlay schema and would have already discarded it if it was unusable.
    case version_util::StateChange::kVersionZeroUpgrade:
      // fallthrough
    case version_util::StateChange::kUpgrade:
      // fallthrough
    case version_util::StateChange::kRollForward:
      // fallthrough
    case version_util::StateChange::kCompatible:
      return libtextclassifier3::Status::OK;
    case version_util::StateChange::kVersionZeroRollForward: {
      // We've rolled forward. The schema overlay file, if it exists, is
      // possibly stale. We must throw it out.
      header_or = Header::Read(filesystem, header_filename);
      ICING_RETURN_IF_ERROR(header_or.status());
      return SchemaStore::DiscardOverlaySchema(filesystem, base_dir,
                                               header_or.ValueOrDie());
    }
    case version_util::StateChange::kRollBack: {
      header_or = Header::Read(filesystem, header_filename);
      ICING_RETURN_IF_ERROR(header_or.status());
      if (header_or.ValueOrDie().min_overlay_version_compatibility() <=
          new_version) {
        // We've been rolled back, but the overlay schema claims that it
        // supports this version. So we can safely return.
        return libtextclassifier3::Status::OK;
      }
      // We've been rolled back to a version that the overlay schema doesn't
      // support. We must throw it out.
      return SchemaStore::DiscardOverlaySchema(filesystem, base_dir,
                                               header_or.ValueOrDie());
    }
    case version_util::StateChange::kUndetermined:
      // It's not clear what version we're on, but the base schema should always
      // be safe to use. Throw out the overlay.
      header_or = Header::Read(filesystem, header_filename);
      ICING_RETURN_IF_ERROR(header_or.status());
      return SchemaStore::DiscardOverlaySchema(filesystem, base_dir,
                                               header_or.ValueOrDie());
  }
  return libtextclassifier3::Status::OK;
}

/* static */ libtextclassifier3::Status SchemaStore::DiscardDerivedFiles(
    const Filesystem* filesystem, const std::string& base_dir) {
  // Schema type mapper
  return DynamicTrieKeyMapper<SchemaTypeId>::Delete(
      *filesystem, MakeSchemaTypeMapperFilename(base_dir));
}

SchemaStore::SchemaStore(const Filesystem* filesystem, std::string base_dir,
                         const Clock* clock, const FeatureFlags* feature_flags)
    : filesystem_(filesystem),
      base_dir_(std::move(base_dir)),
      clock_(clock),
      feature_flags_(feature_flags),
      schema_file_(std::make_unique<FileBackedProto<SchemaProto>>(
          *filesystem, MakeSchemaFilename(base_dir_))) {}

SchemaStore::~SchemaStore() {
  if (has_schema_successfully_set_ && schema_file_ != nullptr &&
      schema_type_mapper_ != nullptr && schema_type_manager_ != nullptr) {
    if (!PersistToDisk().ok()) {
      ICING_LOG(ERROR) << "Error persisting to disk in SchemaStore destructor";
    }
  }
}

libtextclassifier3::Status SchemaStore::Initialize(SchemaProto new_schema) {
  ICING_RETURN_IF_ERROR(LoadSchema());
  if (!absl_ports::IsNotFound(GetSchema().status())) {
    return absl_ports::FailedPreconditionError(
        "Incorrectly tried to initialize schema store with a new schema, when "
        "one is already set!");
  }
  ICING_RETURN_IF_ERROR(schema_file_->Write(
      std::make_unique<SchemaProto>(std::move(new_schema))));
  return InitializeInternal(/*create_overlay_if_necessary=*/true,
                            /*initialize_stats=*/nullptr);
}

libtextclassifier3::Status SchemaStore::Initialize(
    InitializeStatsProto* initialize_stats) {
  ICING_RETURN_IF_ERROR(LoadSchema());
  auto schema_proto_or = GetSchema();
  if (absl_ports::IsNotFound(schema_proto_or.status())) {
    // Don't have an existing schema proto, that's fine
    return libtextclassifier3::Status::OK;
  } else if (!schema_proto_or.ok()) {
    // Real error when trying to read the existing schema
    return schema_proto_or.status();
  }
  return InitializeInternal(/*create_overlay_if_necessary=*/false,
                            initialize_stats);
}

libtextclassifier3::Status SchemaStore::LoadSchema() {
  libtextclassifier3::StatusOr<Header> header_or =
      Header::Read(filesystem_, MakeHeaderFilename(base_dir_));
  bool header_exists = false;
  if (!header_or.ok() && !absl_ports::IsNotFound(header_or.status())) {
    return header_or.status();
  } else if (!header_or.ok()) {
    header_ =
        std::make_unique<Header>(filesystem_, MakeHeaderFilename(base_dir_));
  } else {
    header_exists = true;
    header_ = std::make_unique<Header>(std::move(header_or).ValueOrDie());
  }

  std::string overlay_schema_filename = MakeOverlaySchemaFilename(base_dir_);
  bool overlay_schema_file_exists =
      filesystem_->FileExists(overlay_schema_filename.c_str());

  libtextclassifier3::Status base_schema_state = schema_file_->Read().status();
  if (!base_schema_state.ok() && !absl_ports::IsNotFound(base_schema_state)) {
    return base_schema_state;
  }

  // There are three valid cases:
  // 1. Everything is missing. This is an empty schema store.
  if (!base_schema_state.ok() && !overlay_schema_file_exists &&
      !header_exists) {
    return libtextclassifier3::Status::OK;
  }

  // 2. There never was a overlay schema. The header exists, the base schema
  //    exists and the header says the overlay schema shouldn't exist
  if (base_schema_state.ok() && !overlay_schema_file_exists && header_exists &&
      !header_->overlay_created()) {
    // Nothing else to do. Just return safely.
    return libtextclassifier3::Status::OK;
  }

  // 3. There is an overlay schema and a base schema and a header. The header
  // says that the overlay schema should exist.
  if (base_schema_state.ok() && overlay_schema_file_exists && header_exists &&
      header_->overlay_created()) {
    overlay_schema_file_ = std::make_unique<FileBackedProto<SchemaProto>>(
        *filesystem_, MakeOverlaySchemaFilename(base_dir_));
    return libtextclassifier3::Status::OK;
  }

  // Something has gone wrong. We've lost part of the schema ground truth.
  // Return an error.
  bool overlay_created = header_->overlay_created();
  bool base_schema_exists = base_schema_state.ok();
  return absl_ports::InternalError(IcingStringUtil::StringPrintf(
      "Unable to properly load schema. Header {exists:%d, overlay_created:%d}, "
      "base schema exists: %d, overlay_schema_exists: %d",
      header_exists, overlay_created, base_schema_exists,
      overlay_schema_file_exists));
}

libtextclassifier3::Status SchemaStore::InitializeInternal(
    bool create_overlay_if_necessary, InitializeStatsProto* initialize_stats) {
  if (!InitializeDerivedFiles().ok()) {
    ICING_VLOG(3)
        << "Couldn't find derived files or failed to initialize them, "
           "regenerating derived files for SchemaStore.";
    std::unique_ptr<Timer> regenerate_timer = clock_->GetNewTimer();
    if (initialize_stats != nullptr) {
      initialize_stats->set_schema_store_recovery_cause(
          InitializeStatsProto::IO_ERROR);
    }
    ICING_RETURN_IF_ERROR(RegenerateDerivedFiles(create_overlay_if_necessary));
    if (initialize_stats != nullptr) {
      initialize_stats->set_schema_store_recovery_latency_ms(
          regenerate_timer->GetElapsedMilliseconds());
    }
  }

  if (initialize_stats != nullptr) {
    initialize_stats->set_num_schema_types(type_config_map_.size());
  }
  has_schema_successfully_set_ = true;

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status SchemaStore::InitializeDerivedFiles() {
  ICING_ASSIGN_OR_RETURN(
      schema_type_mapper_,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(
          *filesystem_, MakeSchemaTypeMapperFilename(base_dir_),
          kSchemaTypeMapperMaxSize));

  Crc32 expected_checksum(header_->checksum());
  ICING_ASSIGN_OR_RETURN(Crc32 checksum, GetChecksum());
  if (checksum != expected_checksum) {
    return absl_ports::InternalError(
        "Combined checksum of SchemaStore was inconsistent");
  }

  ICING_RETURN_IF_ERROR(BuildInMemoryCache());
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status SchemaStore::RegenerateDerivedFiles(
    bool create_overlay_if_necessary) {
  ICING_ASSIGN_OR_RETURN(const SchemaProto* schema_proto, GetSchema());

  ICING_RETURN_IF_ERROR(ResetSchemaTypeMapper());

  for (const SchemaTypeConfigProto& type_config : schema_proto->types()) {
    // Assign a SchemaTypeId to the type
    ICING_RETURN_IF_ERROR(schema_type_mapper_->Put(
        type_config.schema_type(), schema_type_mapper_->num_keys()));
  }
  ICING_RETURN_IF_ERROR(BuildInMemoryCache());

  if (create_overlay_if_necessary) {
    BackupSchemaProducer producer(feature_flags_);
    ICING_ASSIGN_OR_RETURN(
        BackupSchemaProducer::BackupSchemaResult backup_result,
        producer.Produce(*schema_proto,
                         schema_type_manager_->section_manager()));

    if (backup_result.backup_schema_produced) {
      // The overlay schema should be written to the overlay file location.
      overlay_schema_file_ = std::make_unique<FileBackedProto<SchemaProto>>(
          *filesystem_, MakeOverlaySchemaFilename(base_dir_));
      auto schema_ptr = std::make_unique<SchemaProto>(std::move(*schema_proto));
      ICING_RETURN_IF_ERROR(overlay_schema_file_->Write(std::move(schema_ptr)));

      // The base schema should be written to the original file
      auto base_schema_ptr =
          std::make_unique<SchemaProto>(std::move(backup_result.backup_schema));
      ICING_RETURN_IF_ERROR(schema_file_->Write(std::move(base_schema_ptr)));

      // LINT.IfChange(min_overlay_version_compatibility)
      // Although the current version is 5, the schema is compatible with
      // version 1, so min_overlay_version_compatibility should be 1.
      int32_t min_overlay_version_compatibility = version_util::kVersionOne;
      // LINT.ThenChange(//depot/google3/icing/file/version-util.h:kVersion)
      header_->SetOverlayInfo(
          /*overlay_created=*/true, min_overlay_version_compatibility);
      // Rebuild in memory data - references to the old schema will be invalid
      // now.
      ICING_RETURN_IF_ERROR(BuildInMemoryCache());
    }
  }

  // Write the header
  ICING_RETURN_IF_ERROR(UpdateChecksum());
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status SchemaStore::BuildInMemoryCache() {
  ICING_ASSIGN_OR_RETURN(const SchemaProto* schema_proto, GetSchema());
  ICING_ASSIGN_OR_RETURN(
      SchemaUtil::InheritanceMap inheritance_map,
      SchemaUtil::BuildTransitiveInheritanceGraph(*schema_proto));

  reverse_schema_type_mapper_.clear();
  database_type_map_.clear();
  type_config_map_.clear();
  schema_subtype_id_map_.clear();
  for (const SchemaTypeConfigProto& type_config : schema_proto->types()) {
    const std::string& database = type_config.database();
    const std::string& type_name = type_config.schema_type();
    ICING_ASSIGN_OR_RETURN(SchemaTypeId type_id,
                           schema_type_mapper_->Get(type_name));

    // Build reverse_schema_type_mapper_
    reverse_schema_type_mapper_.insert({type_id, type_name});

    // Build database_type_map_
    database_type_map_[database].push_back(type_name);

    // Build type_config_map_
    type_config_map_.insert({type_name, type_config});

    // Build schema_subtype_id_map_
    std::unordered_set<SchemaTypeId>& subtype_id_set =
        schema_subtype_id_map_[type_id];
    // Find all child types
    auto child_types_names = inheritance_map.find(type_name);
    if (child_types_names != inheritance_map.end()) {
      subtype_id_set.reserve(child_types_names->second.size() + 1);
      for (const auto& [child_type_name, is_direct_child] :
           child_types_names->second) {
        ICING_ASSIGN_OR_RETURN(SchemaTypeId child_type_id,
                               schema_type_mapper_->Get(child_type_name));
        subtype_id_set.insert(child_type_id);
      }
    }
    // Every type is a subtype of itself.
    subtype_id_set.insert(type_id);
  }

  // Build schema_type_manager_
  ICING_ASSIGN_OR_RETURN(
      schema_type_manager_,
      SchemaTypeManager::Create(type_config_map_, schema_type_mapper_.get()));

  scorable_property_manager_ = std::make_unique<ScorablePropertyManager>();

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status SchemaStore::ResetSchemaTypeMapper() {
  // TODO(b/139734457): Replace ptr.reset()->Delete->Create flow with Reset().
  schema_type_mapper_.reset();
  // TODO(b/216487496): Implement a more robust version of TC_RETURN_IF_ERROR
  // that can support error logging.
  libtextclassifier3::Status status =
      DynamicTrieKeyMapper<SchemaTypeId>::Delete(
          *filesystem_, MakeSchemaTypeMapperFilename(base_dir_));
  if (!status.ok()) {
    ICING_LOG(ERROR) << status.error_message()
                     << "Failed to delete old schema_type mapper";
    return status;
  }
  ICING_ASSIGN_OR_RETURN(
      schema_type_mapper_,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(
          *filesystem_, MakeSchemaTypeMapperFilename(base_dir_),
          kSchemaTypeMapperMaxSize));

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<Crc32> SchemaStore::GetChecksum() const {
  ICING_ASSIGN_OR_RETURN(Crc32 schema_checksum, schema_file_->GetChecksum());
  // We've gotten the schema_checksum successfully. This means that
  // schema_file_->Read() will only return either a schema or NOT_FOUND.
  // Sadly, we actually need to differentiate between an existing, but empty
  // schema and a non-existent schema (both of which will have a checksum of 0).
  // For existing, but empty schemas, we need to continue with the checksum
  // calculation of the other components.
  if (schema_checksum == Crc32() &&
      absl_ports::IsNotFound(schema_file_->Read().status())) {
    return schema_checksum;
  }

  Crc32 total_checksum;
  total_checksum.Append(std::to_string(schema_checksum.Get()));
  if (overlay_schema_file_ != nullptr) {
    ICING_ASSIGN_OR_RETURN(Crc32 overlay_schema_checksum,
                           overlay_schema_file_->GetChecksum());
    total_checksum.Append(std::to_string(overlay_schema_checksum.Get()));
  }

  ICING_ASSIGN_OR_RETURN(Crc32 schema_type_mapper_checksum,
                         schema_type_mapper_->GetChecksum());
  total_checksum.Append(std::to_string(schema_type_mapper_checksum.Get()));
  return total_checksum;
}

libtextclassifier3::StatusOr<Crc32> SchemaStore::UpdateChecksum() {
  // FileBackedProto always keeps its checksum up to date. So we just need to
  // retrieve the checksum.
  ICING_ASSIGN_OR_RETURN(Crc32 schema_checksum, schema_file_->GetChecksum());
  // We've gotten the schema_checksum successfully. This means that
  // schema_file_->Read() will only return either a schema or NOT_FOUND.
  // Sadly, we actually need to differentiate between an existing, but empty
  // schema and a non-existent schema (both of which will have a checksum of 0).
  // For existing, but empty schemas, we need to continue with the checksum
  // calculation of the other components so that we will correctly write the
  // header.
  if (schema_checksum == Crc32() &&
      absl_ports::IsNotFound(schema_file_->Read().status())) {
    return schema_checksum;
  }
  Crc32 total_checksum;
  total_checksum.Append(std::to_string(schema_checksum.Get()));

  if (overlay_schema_file_ != nullptr) {
    ICING_ASSIGN_OR_RETURN(Crc32 overlay_schema_checksum,
                           overlay_schema_file_->GetChecksum());
    total_checksum.Append(std::to_string(overlay_schema_checksum.Get()));
  }

  ICING_ASSIGN_OR_RETURN(Crc32 schema_type_mapper_checksum,
                         schema_type_mapper_->UpdateChecksum());
  total_checksum.Append(std::to_string(schema_type_mapper_checksum.Get()));

  header_->set_checksum(total_checksum.Get());
  ICING_RETURN_IF_ERROR(header_->Write());
  return total_checksum;
}

libtextclassifier3::StatusOr<const SchemaProto*> SchemaStore::GetSchema()
    const {
  if (overlay_schema_file_ != nullptr) {
    return overlay_schema_file_->Read();
  }
  return schema_file_->Read();
}

libtextclassifier3::StatusOr<SchemaProto> SchemaStore::GetSchema(
    const std::string& database) const {
  if (!has_schema_successfully_set_) {
    return absl_ports::NotFoundError("No schema found.");
  }

  const auto database_type_map_itr_ = database_type_map_.find(database);
  if (database_type_map_itr_ == database_type_map_.end()) {
    return absl_ports::NotFoundError(
        absl_ports::StrCat("No schema found for database '", database, "'."));
  }

  SchemaProto schema_proto;
  for (const std::string& type_name : database_type_map_itr_->second) {
    ICING_ASSIGN_OR_RETURN(const SchemaTypeConfigProto* type_config,
                           GetSchemaTypeConfig(type_name));
    *schema_proto.add_types() = *type_config;
  }
  return schema_proto;
}

// TODO(cassiewang): Consider removing this definition of SetSchema if it's not
// needed by production code. It's currently being used by our tests, but maybe
// it's trivial to change our test code to also use the
// SetSchema(SchemaProto&& new_schema)
libtextclassifier3::StatusOr<SchemaStore::SetSchemaResult>
SchemaStore::SetSchema(const SchemaProto& new_schema,
                       bool ignore_errors_and_delete_documents) {
  return SetSchema(SchemaProto(new_schema), ignore_errors_and_delete_documents);
}

libtextclassifier3::StatusOr<SchemaStore::SetSchemaResult>
SchemaStore::SetSchema(SchemaProto&& new_schema,
                       bool ignore_errors_and_delete_documents) {
  if (feature_flags_->enable_schema_database()) {
    // Step 1: (Only required if schema database is enabled)
    // Do some preliminary checks on the new schema before formal validation and
    // delta computation. This checks that:
    // - The new schema only contains types from a single database.
    // - The new schema's type names are not already in use from other
    // databases.
    ICING_ASSIGN_OR_RETURN(std::string database,
                           ValidateAndGetDatabase(new_schema));

    // Step 2: Schema validation and delta computation -- try to get the
    // existing schema for the database to compare to the new schema.
    libtextclassifier3::StatusOr<SchemaProto> schema_proto =
        GetSchema(database);
    if (absl_ports::IsNotFound(schema_proto.status())) {
      // Case 1: No preexisting schema for this database.
      return SetInitialSchemaForDatabase(std::move(new_schema),
                                         ignore_errors_and_delete_documents);
    }

    if (!schema_proto.ok()) {
      // Case 2: Real error
      return schema_proto.status();
    }

    // Case 3: At this point, we're guaranteed that we have an existing schema
    // for this database.
    const SchemaProto& old_schema = schema_proto.ValueOrDie();
    return SetSchemaWithDatabaseOverride(std::move(new_schema), old_schema,
                                         ignore_errors_and_delete_documents);
  }

  // Get the full schema if schema database is disabled.
  libtextclassifier3::StatusOr<const SchemaProto*> schema_proto = GetSchema();
  if (absl_ports::IsNotFound(schema_proto.status())) {
    // Case 1: No preexisting schema
    return SetInitialSchemaForDatabase(std::move(new_schema),
                                       ignore_errors_and_delete_documents);
  }

  if (!schema_proto.ok()) {
    // Case 2: Real error
    return schema_proto.status();
  }

  // Case 3: At this point, we're guaranteed that we have an existing schema
  const SchemaProto& old_schema = *schema_proto.ValueOrDie();
  return SetSchemaWithDatabaseOverride(std::move(new_schema), old_schema,
                                       ignore_errors_and_delete_documents);
}

libtextclassifier3::StatusOr<SchemaStore::SetSchemaResult>
SchemaStore::SetInitialSchemaForDatabase(
    SchemaProto new_schema, bool ignore_errors_and_delete_documents) {
  SetSchemaResult result;

  ICING_RETURN_IF_ERROR(SchemaUtil::Validate(new_schema, *feature_flags_));

  result.success = true;
  for (const SchemaTypeConfigProto& type_config : new_schema.types()) {
    result.schema_types_new_by_name.insert(type_config.schema_type());
  }
  // Get the full new SchemaProto that is a combination of the existing schema
  // and new_schema. This is needed as we can only write the full proto to the
  // schema file.
  ICING_ASSIGN_OR_RETURN(
      SchemaProto full_new_schema,
      GetFullSchemaProtoWithUpdatedDb(std::move(new_schema)));
  ICING_RETURN_IF_ERROR(ApplySchemaChange(std::move(full_new_schema)));
  has_schema_successfully_set_ = true;

  return result;
}

libtextclassifier3::StatusOr<SchemaStore::SetSchemaResult>
SchemaStore::SetSchemaWithDatabaseOverride(
    SchemaProto new_schema, const SchemaProto& old_schema,
    bool ignore_errors_and_delete_documents) {
  // Assume we can set the schema unless proven otherwise.
  SetSchemaResult result;
  result.success = true;

  if (feature_flags_->enable_schema_database()) {
    // Check if the schema types are the same between the new and old schema,
    // ignoring order.
    if (AreSchemaTypesEqual(new_schema, old_schema)) {
      return result;
    }
  } else {
    // Old equality check that is sensitive to type definition order.
    if (new_schema.SerializeAsString() == old_schema.SerializeAsString()) {
      // Same schema as before. No need to update anything
      return result;
    }
  }

  // Different schema -- we need to validate the schema and track the
  // differences to see if we can still write it.
  //
  // Validate the new schema and compute the delta between the old and new
  // schema.
  ICING_ASSIGN_OR_RETURN(SchemaUtil::DependentMap new_dependent_map,
                         SchemaUtil::Validate(new_schema, *feature_flags_));
  SchemaUtil::SchemaDelta schema_delta = SchemaUtil::ComputeCompatibilityDelta(
      old_schema, new_schema, new_dependent_map, *feature_flags_);

  result.schema_types_new_by_name = std::move(schema_delta.schema_types_new);
  result.schema_types_changed_fully_compatible_by_name =
      std::move(schema_delta.schema_types_changed_fully_compatible);
  result.schema_types_index_incompatible_by_name =
      std::move(schema_delta.schema_types_index_incompatible);
  result.schema_types_join_incompatible_by_name =
      std::move(schema_delta.schema_types_join_incompatible);
  result.schema_types_scorable_property_inconsistent_by_name =
      std::move(schema_delta.schema_types_scorable_property_inconsistent);

  for (const std::string& schema_type : schema_delta.schema_types_deleted) {
    // We currently don't support deletions, so mark this as not possible.
    // This will change once we allow force-set schemas.
    result.success = false;

    result.schema_types_deleted_by_name.emplace(schema_type);

    ICING_ASSIGN_OR_RETURN(SchemaTypeId schema_type_id,
                           GetSchemaTypeId(schema_type));
    result.schema_types_deleted_by_id.emplace(schema_type_id);
  }

  for (const std::string& schema_type :
       schema_delta.schema_types_incompatible) {
    // We currently don't support incompatible schemas, so mark this as
    // not possible. This will change once we allow force-set schemas.
    result.success = false;

    result.schema_types_incompatible_by_name.emplace(schema_type);

    ICING_ASSIGN_OR_RETURN(SchemaTypeId schema_type_id,
                           GetSchemaTypeId(schema_type));
    result.schema_types_incompatible_by_id.emplace(schema_type_id);
  }

  // Get the full new SchemaProto that is a combination of the existing schema
  // and new_schema. This is needed to calculate the updated SchemaTypeIds, and
  // for writing the full proto to the schema file.
  ICING_ASSIGN_OR_RETURN(
      SchemaProto full_new_schema,
      GetFullSchemaProtoWithUpdatedDb(std::move(new_schema)));

  // We still need to update old_schema_type_ids_changed. We need to retrieve
  // the entire old schema for this, as type ids are assigned for the entire
  // schema, and not on a per-database level.
  //
  // SchemaTypeIds changing is fine, we can update the DocumentStore.
  ICING_ASSIGN_OR_RETURN(const SchemaProto* full_old_schema, GetSchema());
  result.old_schema_type_ids_changed =
      SchemaTypeIdsChanged(*full_old_schema, full_new_schema);

  // We can force set the schema if the caller has told us to ignore any errors
  result.success = result.success || ignore_errors_and_delete_documents;

  // Step 3: Apply the schema change if success. This updates persisted files
  // and derived data structures.
  if (result.success) {
    ICING_RETURN_IF_ERROR(ApplySchemaChange(std::move(full_new_schema)));
    has_schema_successfully_set_ = true;
  }

  // Convert schema types to SchemaTypeIds after the new schema is applied.
  if (feature_flags_->enable_scorable_properties()) {
    for (const std::string& schema_type :
         result.schema_types_scorable_property_inconsistent_by_name) {
      libtextclassifier3::StatusOr<SchemaTypeId> schema_type_id_or =
          GetSchemaTypeId(schema_type);
      if (!schema_type_id_or.ok()) {
        if (absl_ports::IsNotFound(schema_type_id_or.status())) {
          continue;
        }
        return schema_type_id_or.status();
      }
      result.schema_types_scorable_property_inconsistent_by_id.insert(
          schema_type_id_or.ValueOrDie());
    }
  }

  return result;
}

libtextclassifier3::Status SchemaStore::ApplySchemaChange(
    SchemaProto new_schema) {
  // We need to ensure that we either 1) successfully set the schema and
  // update all derived data structures or 2) fail and leave the schema store
  // unchanged.
  // So, first, we create an empty temporary directory to build a new schema
  // store in.
  std::string temp_schema_store_dir_path = base_dir_ + "_temp";
  if (!filesystem_->DeleteDirectoryRecursively(
          temp_schema_store_dir_path.c_str())) {
    ICING_LOG(ERROR) << "Recursively deleting "
                     << temp_schema_store_dir_path.c_str();
    return absl_ports::InternalError(
        "Unable to delete temp directory to prepare to build new schema "
        "store.");
  }

  DestructibleDirectory temp_schema_store_dir(
      filesystem_, std::move(temp_schema_store_dir_path));
  if (!temp_schema_store_dir.is_valid()) {
    return absl_ports::InternalError(
        "Unable to create temp directory to build new schema store.");
  }

  // Then we create our new schema store with the new schema.
  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<SchemaStore> new_schema_store,
      SchemaStore::Create(filesystem_, temp_schema_store_dir.dir(), clock_,
                          feature_flags_, std::move(new_schema)));

  // Then we swap the new schema file + new derived files with the old files.
  if (!filesystem_->SwapFiles(base_dir_.c_str(),
                              temp_schema_store_dir.dir().c_str())) {
    return absl_ports::InternalError(
        "Unable to apply new schema due to failed swap!");
  }

  std::string old_base_dir = std::move(base_dir_);
  *this = std::move(*new_schema_store);

  // After the std::move, the filepaths saved in this instance and in the
  // schema_file_ instance will still be the one from temp_schema_store_dir
  // even though they now point to files that are within old_base_dir.
  // Manually set them to the correct paths.
  base_dir_ = std::move(old_base_dir);
  schema_file_->SetSwappedFilepath(MakeSchemaFilename(base_dir_));
  if (overlay_schema_file_ != nullptr) {
    overlay_schema_file_->SetSwappedFilepath(
        MakeOverlaySchemaFilename(base_dir_));
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<const SchemaTypeConfigProto*>
SchemaStore::GetSchemaTypeConfig(std::string_view schema_type) const {
  ICING_RETURN_IF_ERROR(CheckSchemaSet());
  const auto& type_config_iter =
      type_config_map_.find(std::string(schema_type));
  if (type_config_iter == type_config_map_.end()) {
    return absl_ports::NotFoundError(
        absl_ports::StrCat("Schema type config '", schema_type, "' not found"));
  }
  return &type_config_iter->second;
}

libtextclassifier3::StatusOr<SchemaTypeId> SchemaStore::GetSchemaTypeId(
    std::string_view schema_type) const {
  ICING_RETURN_IF_ERROR(CheckSchemaSet());
  return schema_type_mapper_->Get(schema_type);
}

libtextclassifier3::StatusOr<const std::string*> SchemaStore::GetSchemaType(
    SchemaTypeId schema_type_id) const {
  ICING_RETURN_IF_ERROR(CheckSchemaSet());
  if (const auto it = reverse_schema_type_mapper_.find(schema_type_id);
      it == reverse_schema_type_mapper_.end()) {
    return absl_ports::InvalidArgumentError("Invalid schema type id");
  } else {
    return &it->second;
  }
}

libtextclassifier3::StatusOr<const std::unordered_set<SchemaTypeId>*>
SchemaStore::GetSchemaTypeIdsWithChildren(std::string_view schema_type) const {
  ICING_ASSIGN_OR_RETURN(SchemaTypeId schema_type_id,
                         GetSchemaTypeId(schema_type));
  auto iter = schema_subtype_id_map_.find(schema_type_id);
  if (iter == schema_subtype_id_map_.end()) {
    // This should never happen, unless there is an inconsistency or IO error.
    return absl_ports::InternalError(absl_ports::StrCat(
        "Schema type '", schema_type, "' is not found in the subtype map."));
  }
  return &iter->second;
}

libtextclassifier3::StatusOr<const SectionMetadata*>
SchemaStore::GetSectionMetadata(SchemaTypeId schema_type_id,
                                SectionId section_id) const {
  ICING_RETURN_IF_ERROR(CheckSchemaSet());
  return schema_type_manager_->section_manager().GetSectionMetadata(
      schema_type_id, section_id);
}

libtextclassifier3::StatusOr<SectionGroup> SchemaStore::ExtractSections(
    const DocumentProto& document) const {
  ICING_RETURN_IF_ERROR(CheckSchemaSet());
  return schema_type_manager_->section_manager().ExtractSections(document);
}

libtextclassifier3::StatusOr<const JoinablePropertyMetadata*>
SchemaStore::GetJoinablePropertyMetadata(
    SchemaTypeId schema_type_id, const std::string& property_path) const {
  ICING_RETURN_IF_ERROR(CheckSchemaSet());
  return schema_type_manager_->joinable_property_manager()
      .GetJoinablePropertyMetadata(schema_type_id, property_path);
}

libtextclassifier3::StatusOr<const JoinablePropertyMetadata*>
SchemaStore::GetJoinablePropertyMetadata(
    SchemaTypeId schema_type_id,
    JoinablePropertyId joinable_property_id) const {
  ICING_RETURN_IF_ERROR(CheckSchemaSet());
  return schema_type_manager_->joinable_property_manager()
      .GetJoinablePropertyMetadata(schema_type_id, joinable_property_id);
}

libtextclassifier3::StatusOr<JoinablePropertyGroup>
SchemaStore::ExtractJoinableProperties(const DocumentProto& document) const {
  ICING_RETURN_IF_ERROR(CheckSchemaSet());
  return schema_type_manager_->joinable_property_manager()
      .ExtractJoinableProperties(document);
}

libtextclassifier3::StatusOr<std::optional<int>>
SchemaStore::GetScorablePropertyIndex(SchemaTypeId schema_type_id,
                                      std::string_view property_path) const {
  ICING_RETURN_IF_ERROR(CheckSchemaSet());
  if (!feature_flags_->enable_scorable_properties()) {
    return std::nullopt;
  }
  return scorable_property_manager_->GetScorablePropertyIndex(
      schema_type_id, property_path, type_config_map_,
      reverse_schema_type_mapper_);
}

libtextclassifier3::StatusOr<
    const std::vector<ScorablePropertyManager::ScorablePropertyInfo>*>
SchemaStore::GetOrderedScorablePropertyInfo(SchemaTypeId schema_type_id) const {
  ICING_RETURN_IF_ERROR(CheckSchemaSet());
  if (!feature_flags_->enable_scorable_properties()) {
    return nullptr;
  }
  return scorable_property_manager_->GetOrderedScorablePropertyInfo(
      schema_type_id, type_config_map_, reverse_schema_type_mapper_);
}

libtextclassifier3::Status SchemaStore::PersistToDisk() {
  if (!has_schema_successfully_set_) {
    return libtextclassifier3::Status::OK;
  }
  ICING_RETURN_IF_ERROR(schema_type_mapper_->PersistToDisk());
  ICING_RETURN_IF_ERROR(UpdateChecksum());
  ICING_RETURN_IF_ERROR(header_->PersistToDisk());
  return libtextclassifier3::Status::OK;
}

SchemaStoreStorageInfoProto SchemaStore::GetStorageInfo() const {
  SchemaStoreStorageInfoProto storage_info;
  int64_t directory_size = filesystem_->GetDiskUsage(base_dir_.c_str());
  storage_info.set_schema_store_size(
      Filesystem::SanitizeFileSize(directory_size));
  ICING_ASSIGN_OR_RETURN(const SchemaProto* schema, GetSchema(), storage_info);
  storage_info.set_num_schema_types(schema->types().size());
  int total_sections = 0;
  int num_types_sections_exhausted = 0;
  for (const SchemaTypeConfigProto& type : schema->types()) {
    auto sections_list_or =
        schema_type_manager_->section_manager().GetMetadataList(
            type.schema_type());
    if (!sections_list_or.ok()) {
      continue;
    }
    total_sections += sections_list_or.ValueOrDie()->size();
    if (sections_list_or.ValueOrDie()->size() == kTotalNumSections) {
      ++num_types_sections_exhausted;
    }
  }

  storage_info.set_num_total_sections(total_sections);
  storage_info.set_num_schema_types_sections_exhausted(
      num_types_sections_exhausted);
  return storage_info;
}

libtextclassifier3::StatusOr<const std::vector<SectionMetadata>*>
SchemaStore::GetSectionMetadata(const std::string& schema_type) const {
  return schema_type_manager_->section_manager().GetMetadataList(schema_type);
}

bool SchemaStore::IsPropertyDefinedInSchema(
    SchemaTypeId schema_type_id, const std::string& property_path) const {
  auto schema_name_itr = reverse_schema_type_mapper_.find(schema_type_id);
  if (schema_name_itr == reverse_schema_type_mapper_.end()) {
    return false;
  }
  const std::string* current_type_name = &schema_name_itr->second;

  std::vector<std::string_view> property_path_parts =
      property_util::SplitPropertyPathExpr(property_path);
  for (int i = 0; i < property_path_parts.size(); ++i) {
    auto type_config_itr = type_config_map_.find(*current_type_name);
    if (type_config_itr == type_config_map_.end()) {
      return false;
    }
    std::string_view property_name = property_path_parts.at(i);
    const PropertyConfigProto* selected_property = nullptr;
    for (const PropertyConfigProto& property :
         type_config_itr->second.properties()) {
      if (property.property_name() == property_name) {
        selected_property = &property;
        break;
      }
    }
    if (selected_property == nullptr) {
      return false;
    }
    if (i == property_path_parts.size() - 1) {
      // We've found a property at the final part of the path.
      return true;
    }
    if (selected_property->data_type() !=
        PropertyConfigProto::DataType::DOCUMENT) {
      // If this isn't final part of the path, but this property isn't a
      // document, so we know that this path doesn't exist.
      return false;
    }
    current_type_name = &selected_property->schema_type();
  }

  // We should never reach this point.
  return false;
}

libtextclassifier3::StatusOr<SchemaDebugInfoProto> SchemaStore::GetDebugInfo()
    const {
  SchemaDebugInfoProto debug_info;
  if (has_schema_successfully_set_) {
    ICING_ASSIGN_OR_RETURN(const SchemaProto* schema, GetSchema());
    *debug_info.mutable_schema() = *schema;
  }
  ICING_ASSIGN_OR_RETURN(Crc32 crc, GetChecksum());
  debug_info.set_crc(crc.Get());
  return debug_info;
}

std::vector<SchemaStore::ExpandedTypePropertyMask>
SchemaStore::ExpandTypePropertyMasks(
    const google::protobuf::RepeatedPtrField<TypePropertyMask>& type_property_masks)
    const {
  std::unordered_map<SchemaTypeId, ExpandedTypePropertyMask> result_map;
  for (const TypePropertyMask& type_field_mask : type_property_masks) {
    if (type_field_mask.schema_type() == kSchemaTypeWildcard) {
      ExpandedTypePropertyMask entry{type_field_mask.schema_type(),
                                     /*paths=*/{}};
      entry.paths.insert(type_field_mask.paths().begin(),
                         type_field_mask.paths().end());
      result_map.insert({kInvalidSchemaTypeId, std::move(entry)});
    } else {
      auto schema_type_ids_or =
          GetSchemaTypeIdsWithChildren(type_field_mask.schema_type());
      // If we can't find the SchemaTypeIds, just throw it away
      if (!schema_type_ids_or.ok()) {
        continue;
      }
      const std::unordered_set<SchemaTypeId>* schema_type_ids =
          schema_type_ids_or.ValueOrDie();
      for (SchemaTypeId schema_type_id : *schema_type_ids) {
        auto schema_type_name_iter =
            reverse_schema_type_mapper_.find(schema_type_id);
        if (schema_type_name_iter == reverse_schema_type_mapper_.end()) {
          // This should never happen, unless there is an inconsistency or IO
          // error.
          ICING_LOG(ERROR) << "Got unknown schema type id: " << schema_type_id;
          continue;
        }

        auto iter = result_map.find(schema_type_id);
        if (iter == result_map.end()) {
          ExpandedTypePropertyMask entry{schema_type_name_iter->second,
                                         /*paths=*/{}};
          iter = result_map.insert({schema_type_id, std::move(entry)}).first;
        }
        iter->second.paths.insert(type_field_mask.paths().begin(),
                                  type_field_mask.paths().end());
      }
    }
  }
  std::vector<ExpandedTypePropertyMask> result;
  result.reserve(result_map.size());
  for (auto& entry : result_map) {
    result.push_back(std::move(entry.second));
  }
  return result;
}

libtextclassifier3::StatusOr<
    std::unordered_map<std::string, std::vector<std::string>>>
SchemaStore::ConstructBlobPropertyMap() const {
  ICING_ASSIGN_OR_RETURN(const SchemaProto* schema, GetSchema());
  std::unordered_map<std::string, std::vector<std::string>> blob_property_map;
  for (const SchemaTypeConfigProto& type_config : schema->types()) {
    SchemaPropertyIterator iterator(type_config, type_config_map_);
    std::vector<std::string> blob_properties;

    libtextclassifier3::Status status = iterator.Advance();
    while (status.ok()) {
      if (iterator.GetCurrentPropertyConfig().data_type() ==
          PropertyConfigProto::DataType::BLOB_HANDLE) {
        blob_properties.push_back(iterator.GetCurrentPropertyPath());
      }
      status = iterator.Advance();
    }
    if (!absl_ports::IsOutOfRange(status)) {
      return status;
    }
    if (!blob_properties.empty()) {
      blob_property_map.insert(
          {type_config.schema_type(), std::move(blob_properties)});
    }
  }
  return blob_property_map;
}

libtextclassifier3::StatusOr<std::string> SchemaStore::ValidateAndGetDatabase(
    const SchemaProto& new_schema) const {
  std::string database;

  if (!feature_flags_->enable_schema_database() || new_schema.types().empty()) {
    return database;
  }

  database = new_schema.types(0).database();
  // Loop through new_schema's types and validate it. The input SchemaProto
  // contains a list of SchemaTypeConfigProtos without deduplication. We need to
  // check that:
  // 1. All SchemaTypeConfigProtos have the same database value.
  // 2. The SchemaTypeConfigProtos's schema_type field is unique within both
  //    new_schema, as well as the existing schema (recorded in
  //    type_config_map_).
  for (const SchemaTypeConfigProto& type_config : new_schema.types()) {
    // Check database consistency.
    if (database != type_config.database()) {
      return absl_ports::InvalidArgumentError(
          "SetSchema only accepts a SchemaProto with types from a single "
          "database at a time. Please make separate calls for each database if "
          "you need to set the schema for multiple databases.");
    }

    // Check type name uniqueness. This is only necessary if there is a
    // pre-existing schema.
    if (has_schema_successfully_set_) {
      auto iter = type_config_map_.find(type_config.schema_type());
      if (iter != type_config_map_.end() &&
          database != iter->second.database()) {
        return absl_ports::AlreadyExistsError(
            absl_ports::StrCat("schema_type name: '", type_config.schema_type(),
                               "' is already in use by a different database."));
      }
    }
  }
  return database;
}

libtextclassifier3::StatusOr<SchemaProto>
SchemaStore::GetFullSchemaProtoWithUpdatedDb(
    SchemaProto input_database_schema) const {
  if (!feature_flags_->enable_schema_database()) {
    // If the schema database is not enabled, the input schema is already the
    // full schema, so we don't need to do any merges.
    return input_database_schema;
  }

  libtextclassifier3::StatusOr<const SchemaProto*> schema_proto = GetSchema();
  if (absl_ports::IsNotFound(schema_proto.status())) {
    // We don't have a pre-existing schema -- we can return the input database
    // schema as it's already the full schema.
    return input_database_schema;
  }

  if (!schema_proto.ok()) {
    // Real error.
    return schema_proto.status();
  }

  if (!has_schema_successfully_set_) {
    return absl_ports::InternalError(
        "Schema store was not initialized properly.");
  }

  // At this point, we have a pre-existing schema -- we need to merge the
  // updated database with the existing schema.
  if (input_database_schema.types().empty()) {
    return *schema_proto.ValueOrDie();
  }

  std::string input_database = input_database_schema.types(0).database();
  if (database_type_map_.size() == 1 &&
      database_type_map_.find(input_database) != database_type_map_.end()) {
    // No other databases in the schema -- we can return the input database
    // schema.
    return input_database_schema;
  }

  const SchemaProto* existing_schema = schema_proto.ValueOrDie();
  SchemaProto full_schema;

  // 1. Add types from the existing schema, replacing existing types with the
  // input types if the database is the one being updated by the input schema.
  // - For the input_database, we replace the existing types with the input
  //   types. An exisiting type is deleted if it's not included in
  //   input_database.
  // - If there are more input types than existing types for the input_database,
  //   the rest of the input types are appended to the end of the full_schema.
  // - If there are fewer input types than existing types for the
  //   input_database, we shift all existing that come after input_database
  //   forward.
  // - For existing types from other databases, we add the types in their
  //   original order to full_schema. Note that the type-ids of existing types
  //   might still change if some types deleted in input_database as this will
  //   cause all subsequent types ids to shift forward.
  int input_schema_index = 0, existing_schema_index = 0;
  while (input_schema_index < input_database_schema.types().size() &&
         existing_schema_index < existing_schema->types().size()) {
    const SchemaTypeConfigProto& existing_type_config =
        existing_schema->types(existing_schema_index);
    SchemaTypeConfigProto& input_type_config =
        *input_database_schema.mutable_types(input_schema_index);

    if (input_type_config.database() != input_database) {
      return absl_ports::InvalidArgumentError(
          "Can only update a single database at a time.");
    }

    if (existing_type_config.database() == input_database) {
      // If the database is the one being updated by the input schema, replace
      // the existing type with a type from the input schema.
      *full_schema.add_types() = std::move(input_type_config);
      ++input_schema_index;
    } else {
      *full_schema.add_types() = existing_type_config;
    }
    ++existing_schema_index;
  }

  // 2. Append remaining types to the end of the SchemaProto.
  for (; input_schema_index < input_database_schema.types().size();
       ++input_schema_index) {
    // Case 1: Append all remaining types from the input schema. This happens
    // when more types are added in input_database_schema than what's in the
    // existing schema. In this case, we've used up the space for the database
    // in the existing schema, so we can just append the rest of the types to
    // the end.
    SchemaTypeConfigProto& input_type_config =
        *input_database_schema.mutable_types(input_schema_index);
    *full_schema.add_types() = std::move(input_type_config);
  }
  for (; existing_schema_index < existing_schema->types().size();
       ++existing_schema_index) {
    // Case 2: Add remaining types from the existing schema, but skip the ones
    // that are from input_database, since existing types from input_database
    // are replaced with input_database_schema.
    if (existing_schema->types(existing_schema_index).database() !=
        input_database) {
      *full_schema.add_types() = existing_schema->types(existing_schema_index);
    }
  }

  return full_schema;
}

}  // namespace lib
}  // namespace icing
