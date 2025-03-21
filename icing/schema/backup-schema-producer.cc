// Copyright (C) 2023 Google LLC
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

#include "icing/schema/backup-schema-producer.h"

#include <algorithm>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/feature-flags.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/schema/property-util.h"
#include "icing/schema/section-manager.h"
#include "icing/schema/section.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

// Creates a map of property to indexed id count based on the list of indexed
// properties provided by metadata_list.
// For all non-document properties, the value will always be 1.
// For document properties, the value will be the number of nested properties
// that are indexed with that document type.
std::unordered_map<std::string_view, int> CreateIndexedIdCountMap(
    const std::vector<SectionMetadata>* metadata_list) {
  std::unordered_map<std::string_view, int> property_indexed_id_count_map;
  for (const SectionMetadata& metadata : *metadata_list) {
    std::string_view top_level_property;
    size_t separator_pos =
        metadata.path.find(property_util::kPropertyPathSeparator);
    if (separator_pos == std::string::npos) {
      top_level_property = metadata.path;
    } else {
      top_level_property =
          std::string_view(metadata.path.c_str(), separator_pos);
    }
    int& count = property_indexed_id_count_map[top_level_property];
    ++count;
  }
  return property_indexed_id_count_map;
}

bool PropertyHasInvalidIndexingType(const PropertyConfigProto& property) {
  return property.string_indexing_config().tokenizer_type() ==
         StringIndexingConfig::TokenizerType::RFC822;
}

bool PropertyHasInvalidDataType(const PropertyConfigProto& property,
                                const FeatureFlags& feature_flags) {
  if (feature_flags.enable_embedding_backup_generation()) {
    return property.data_type() == PropertyConfigProto::DataType::VECTOR;
  }
  return false;
}

// Returns the indices (within schema.types()) of all types that are rollback
// incompatible (old code cannot handle these types if they are unmodified).
//
// Currently, this means types that:
//   1. Use RFC822 tokenization for any properties
//   2. Use more than 16 indexed properties
libtextclassifier3::StatusOr<std::vector<int>>
GetRollbackIncompatibleTypeIndices(const SchemaProto& schema,
                                   const SectionManager& type_manager,
                                   const FeatureFlags& feature_flags) {
  std::vector<int> invalid_type_indices;
  for (int i = 0; i < schema.types_size(); ++i) {
    const SchemaTypeConfigProto& type = schema.types(i);
    bool rollback_incompatible = false;
    for (const PropertyConfigProto& property : type.properties()) {
      if (PropertyHasInvalidIndexingType(property) ||
          (PropertyHasInvalidDataType(property, feature_flags))) {
        rollback_incompatible = true;
        break;
      }
    }
    if (rollback_incompatible) {
      invalid_type_indices.push_back(i);
      continue;
    }

    ICING_ASSIGN_OR_RETURN(const std::vector<SectionMetadata>* metadata_list,
                           type_manager.GetMetadataList(type.schema_type()));
    if (metadata_list->size() > kOldTotalNumSections) {
      invalid_type_indices.push_back(i);
    }
  }
  return invalid_type_indices;
}

// Simulates the effects of marking property_name as unindexed. To do this, it:
// 1. Decrements num_indexed_sections by the number of indexed ids consumed by
//    property_name (and any sub-properties, if applicable).
// 2. Removes property_name from property_indexed_id_count_map.
void RemovePropertyIndexedIdCount(
    std::string_view property_name, int& num_indexed_sections,
    std::unordered_map<std::string_view, int>& property_indexed_id_count_map) {
  auto indexed_count_itr = property_indexed_id_count_map.find(property_name);
  if (indexed_count_itr != property_indexed_id_count_map.end()) {
    num_indexed_sections -= indexed_count_itr->second;
    property_indexed_id_count_map.erase(indexed_count_itr);
  }
}

// Checks type for any properties that have invalid indexing types. Those
// properties are marked as unindexed. num_indexed_sections and
// property_indexed_id_count_map are updated to reflect this change.
void HandleInvalidIndexingTypeProperties(
    SchemaTypeConfigProto* type, int& num_indexed_sections,
    std::unordered_map<std::string_view, int>& property_indexed_id_count_map) {
  for (PropertyConfigProto& property : *type->mutable_properties()) {
    // If the property uses the RFC tokenizer, then we need to set it to NONE
    // and set match type UNKNOWN.
    if (PropertyHasInvalidIndexingType(property)) {
      property.clear_string_indexing_config();
      RemovePropertyIndexedIdCount(property.property_name(),
                                   num_indexed_sections,
                                   property_indexed_id_count_map);
    }
  }
}

// Checks type for any properties that have invalid data types. Those properties
// are removed entirely. num_indexed_sections and
// property_indexed_id_count_map are updated to reflect this change.
void RemoveInvalidDataTypeProperties(
    SchemaTypeConfigProto* type, int& num_indexed_sections,
    std::unordered_map<std::string_view, int>& property_indexed_id_count_map,
    const FeatureFlags& feature_flags) {
  auto itr = std::remove_if(
      type->mutable_properties()->begin(), type->mutable_properties()->end(),
      [&feature_flags](const PropertyConfigProto& property) {
        return PropertyHasInvalidDataType(property, feature_flags);
      });
  // std::remove_if will simply move all of the matching elements to the end of
  // the list and return an iterator to that first matching element. So we can
  // iterate from that point to update the indexed id count and then erase the
  // matching elements.
  for (auto i = itr; i != type->mutable_properties()->end(); ++i) {
    RemovePropertyIndexedIdCount(i->property_name(), num_indexed_sections,
                                 property_indexed_id_count_map);
  }
  type->mutable_properties()->erase(itr, type->mutable_properties()->end());
}

}  // namespace

libtextclassifier3::StatusOr<BackupSchemaProducer::BackupSchemaResult>
BackupSchemaProducer::Produce(const SchemaProto& schema,
                              const SectionManager& type_manager) {
  ICING_ASSIGN_OR_RETURN(
      std::vector<int> invalid_type_indices,
      GetRollbackIncompatibleTypeIndices(schema, type_manager, feature_flags_));
  if (invalid_type_indices.empty()) {
    return BackupSchemaResult();
  }

  SchemaProto backup_schema(schema);
  std::unordered_map<std::string_view, int> type_indexed_property_count;
  for (int i : invalid_type_indices) {
    SchemaTypeConfigProto* type = backup_schema.mutable_types(i);

    // 1. Retrieve metadata on the set of indexed proeprties and (if necessary)
    // populate the variables needed to track the indexed property counts.
    //
    // This should never cause an error - every type should have an entry in the
    // type_manager.
    ICING_ASSIGN_OR_RETURN(const std::vector<SectionMetadata>* metadata_list,
                           type_manager.GetMetadataList(type->schema_type()));
    int num_indexed_sections = metadata_list->size();
    std::unordered_map<std::string_view, int> property_indexed_id_count_map;
    if (num_indexed_sections > kOldTotalNumSections) {
      property_indexed_id_count_map = CreateIndexedIdCountMap(metadata_list);
    }

    // 2. Remove any properties that are invalid for the backup schema.
    if (feature_flags_.enable_embedding_backup_generation()) {
      RemoveInvalidDataTypeProperties(type, num_indexed_sections,
                                      property_indexed_id_count_map,
                                      feature_flags_);
    }

    // 3. Mark any properties with an invalid indexing type as unindexed.
    HandleInvalidIndexingTypeProperties(type, num_indexed_sections,
                                        property_indexed_id_count_map);

    // 4. If there are any types that exceed the old indexed property limit,
    // then mark indexed properties as unindexed until we're back under the
    // limit.
    if (num_indexed_sections <= kOldTotalNumSections) {
      continue;
    }

    // We expect that the last properties were the ones added most recently and
    // are the least crucial, so we do removal in reverse order. This is a bit
    // arbitrary, but we don't really have sufficient information to make this
    // judgment anyways.
    for (auto itr = type->mutable_properties()->rbegin();
         itr != type->mutable_properties()->rend(); ++itr) {
      auto indexed_count_itr =
          property_indexed_id_count_map.find(itr->property_name());
      if (indexed_count_itr == property_indexed_id_count_map.end()) {
        continue;
      }

      // Mark this property as unindexed and subtract all indexed property ids
      // consumed by this property.
      PropertyConfigProto& property = *itr;
      property.clear_document_indexing_config();
      property.clear_string_indexing_config();
      property.clear_integer_indexing_config();
      num_indexed_sections -= indexed_count_itr->second;
      if (num_indexed_sections <= kOldTotalNumSections) {
        break;
      }
    }
  }
  return BackupSchemaResult(std::move(backup_schema));
}

}  // namespace lib
}  // namespace icing
