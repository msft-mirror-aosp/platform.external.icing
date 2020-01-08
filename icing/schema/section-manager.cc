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

#include "icing/schema/section-manager.h"

#include <algorithm>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <iterator>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/status_macros.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/schema/schema-util.h"
#include "icing/schema/section.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/key-mapper.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {
namespace {

using TypeSectionMap =
    std::unordered_map<std::string, const std::vector<SectionMetadata>>;

// This state helps detect infinite loops (e.g. two type configs referencing
// each other) when assigning sections. The combination of 'number of section
// assigned' and 'current schema name' represents a unique state in the
// section-assign process. If the same state is seen the second time, that means
// an infinite loop.
struct SectionAssigningState {
  size_t num_sections_assigned;
  std::string current_schema_name;

  SectionAssigningState(size_t num_sections_assigned_in,
                        std::string&& current_schema_name_in)
      : num_sections_assigned(num_sections_assigned_in),
        current_schema_name(std::move(current_schema_name_in)) {}
};

// Provides a hash value of this struct so that it can be stored in a hash
// set.
struct SectionAssigningStateHasher {
  size_t operator()(const SectionAssigningState& state) const {
    size_t str_hash = std::hash<std::string>()(state.current_schema_name);
    size_t int_hash = std::hash<size_t>()(state.num_sections_assigned);
    // Combine the two hashes by taking the upper 16-bits of the string hash and
    // the lower 16-bits of the int hash.
    return (str_hash & 0xFFFF0000) | (int_hash & 0x0000FFFF);
  }
};

bool operator==(const SectionAssigningState& lhs,
                const SectionAssigningState& rhs) {
  return lhs.num_sections_assigned == rhs.num_sections_assigned &&
         lhs.current_schema_name == rhs.current_schema_name;
}

// Helper function to concatenate a path and a property name
std::string ConcatenatePath(const std::string& path,
                            const std::string& next_property_name) {
  if (path.empty()) {
    return next_property_name;
  }
  return absl_ports::StrCat(path, kPropertySeparator, next_property_name);
}

// Helper function to recursively identify sections from a type config and add
// them to a section metadata list
libtextclassifier3::Status AssignSections(
    const SchemaTypeConfigProto& type_config,
    const std::string& current_section_path,
    const SchemaUtil::TypeConfigMap& type_config_map,
    std::unordered_set<SectionAssigningState, SectionAssigningStateHasher>*
        visited_states,
    std::vector<SectionMetadata>* metadata_list) {
  if (!visited_states
           ->emplace(metadata_list->size(),
                     std::string(type_config.schema_type()))
           .second) {
    // Failed to insert, the same state has been seen before, there's an
    // infinite loop in type configs
    return absl_ports::InvalidArgumentError(
        "Infinite loop detected in type configs");
  }

  // Sorts properties by name's alphabetical order so that order doesn't affect
  // section assigning.
  auto sorted_properties = type_config.properties();
  std::sort(sorted_properties.pointer_begin(), sorted_properties.pointer_end(),
            [](const PropertyConfigProto* p1, const PropertyConfigProto* p2) {
              return p1->property_name() < p2->property_name();
            });
  for (const auto& property_config : sorted_properties) {
    if (property_config.indexing_config().term_match_type() ==
        TermMatchType::UNKNOWN) {
      // No need to create section for current property
      continue;
    }

    // Creates section metadata according to data type
    if (property_config.data_type() == PropertyConfigProto::DataType::STRING ||
        property_config.data_type() == PropertyConfigProto::DataType::INT64 ||
        property_config.data_type() == PropertyConfigProto::DataType::DOUBLE) {
      // Validates next section id, makes sure that section id is the same as
      // the list index so that we could find any section metadata by id in O(1)
      // later.
      auto new_section_id = static_cast<SectionId>(metadata_list->size());
      if (!IsSectionIdValid(new_section_id)) {
        // Max number of sections reached
        return absl_ports::OutOfRangeError(IcingStringUtil::StringPrintf(
            "Too many properties to be indexed, max number of properties "
            "allowed: %d",
            kMaxSectionId - kMinSectionId + 1));
      }
      // Creates section metadata from property config
      metadata_list->emplace_back(
          new_section_id, property_config.indexing_config().term_match_type(),
          property_config.indexing_config().tokenizer_type(),
          ConcatenatePath(current_section_path,
                          property_config.property_name()));
    } else if (property_config.data_type() ==
               PropertyConfigProto::DataType::DOCUMENT) {
      // Tries to find sections recursively
      auto nested_type_config_iter =
          type_config_map.find(property_config.schema_type());
      if (nested_type_config_iter == type_config_map.end()) {
        return absl_ports::NotFoundError(absl_ports::StrCat(
            "type config not found: ", property_config.schema_type()));
      }
      const SchemaTypeConfigProto& nested_type_config =
          nested_type_config_iter->second;
      ICING_RETURN_IF_ERROR(
          AssignSections(nested_type_config,
                         ConcatenatePath(current_section_path,
                                         property_config.property_name()),
                         type_config_map, visited_states, metadata_list));
    }
    // NOTE: we don't create sections for BOOLEAN and BYTES data types.
  }
  return libtextclassifier3::Status::OK;
}

// Builds a vector of vectors that holds SectionMetadatas for all the schema
// types. The outer vector's index corresponds with a type's SchemaTypeId. The
// inner vector's index corresponds to the section's SectionId.
libtextclassifier3::StatusOr<std::vector<std::vector<SectionMetadata>>>
BuildSectionMetadataCache(const SchemaUtil::TypeConfigMap& type_config_map,
                          const KeyMapper<SchemaTypeId>& schema_type_mapper) {
  // Create our vector and reserve the number of schema types we have
  std::vector<std::vector<SectionMetadata>> section_metadata_cache(
      schema_type_mapper.num_keys());

  std::unordered_set<SectionAssigningState, SectionAssigningStateHasher>
      visited_states;
  for (const auto& name_and_type : type_config_map) {
    // Assigns sections for each type config
    visited_states.clear();
    const std::string& type_config_name = name_and_type.first;
    const SchemaTypeConfigProto& type_config = name_and_type.second;
    std::vector<SectionMetadata> metadata_list;
    ICING_RETURN_IF_ERROR(
        AssignSections(type_config, /*current_section_path*/ "",
                       type_config_map, &visited_states, &metadata_list));

    // Insert the section metadata list at the index of the type's SchemaTypeId
    TC3_ASSIGN_OR_RETURN(SchemaTypeId schema_type_id,
                           schema_type_mapper.Get(type_config_name));
    section_metadata_cache[schema_type_id] = std::move(metadata_list);
  }
  return section_metadata_cache;
}

// Helper function to get string content from a property. Repeated values are
// joined into one string. We only care about STRING, INT64, and DOUBLE data
// types.
std::vector<std::string> GetPropertyContent(const PropertyProto& property) {
  std::vector<std::string> values;
  if (!property.string_values().empty()) {
    std::copy(property.string_values().begin(), property.string_values().end(),
              std::back_inserter(values));
  } else if (!property.int64_values().empty()) {
    std::transform(
        property.int64_values().begin(), property.int64_values().end(),
        std::back_inserter(values),
        [](int64_t i) { return IcingStringUtil::StringPrintf("%" PRId64, i); });
  } else {
    std::transform(
        property.double_values().begin(), property.double_values().end(),
        std::back_inserter(values),
        [](double d) { return IcingStringUtil::StringPrintf("%f", d); });
  }
  return values;
}

// Helper function to get metadata list of a type config
libtextclassifier3::StatusOr<std::vector<SectionMetadata>> GetMetadataList(
    const KeyMapper<SchemaTypeId>& schema_type_mapper,
    const std::vector<std::vector<SectionMetadata>>& section_metadata_cache,
    const std::string& type_config_name) {
  TC3_ASSIGN_OR_RETURN(SchemaTypeId schema_type_id,
                         schema_type_mapper.Get(type_config_name));
  return section_metadata_cache.at(schema_type_id);
}

}  // namespace

SectionManager::SectionManager(
    const KeyMapper<SchemaTypeId>* schema_type_mapper,
    std::vector<std::vector<SectionMetadata>>&& section_metadata_cache)
    : schema_type_mapper_(*schema_type_mapper),
      section_metadata_cache_(std::move(section_metadata_cache)) {}

libtextclassifier3::StatusOr<std::unique_ptr<SectionManager>>
SectionManager::Create(const SchemaUtil::TypeConfigMap& type_config_map,
                       const KeyMapper<SchemaTypeId>* schema_type_mapper) {
  ICING_RETURN_ERROR_IF_NULL(schema_type_mapper);

  TC3_ASSIGN_OR_RETURN(
      std::vector<std::vector<SectionMetadata>> section_metadata_cache,
      BuildSectionMetadataCache(type_config_map, *schema_type_mapper));
  return std::unique_ptr<SectionManager>(new SectionManager(
      schema_type_mapper, std::move(section_metadata_cache)));
}

libtextclassifier3::StatusOr<std::vector<std::string>>
SectionManager::GetSectionContent(const DocumentProto& document,
                                  std::string_view section_path) const {
  // Finds the first property name in section_path
  size_t separator_position = section_path.find(kPropertySeparator);
  std::string_view current_property_name =
      (separator_position == std::string::npos)
          ? section_path
          : section_path.substr(0, separator_position);

  // Tries to match the property name with the ones in document
  auto property_iterator =
      std::find_if(document.properties().begin(), document.properties().end(),
                   [current_property_name](const PropertyProto& property) {
                     return property.name() == current_property_name;
                   });

  if (property_iterator == document.properties().end()) {
    // Property name not found, it could be one of the following 2 cases:
    // 1. The property is optional and it's not in the document
    // 2. The property name is invalid
    return absl_ports::NotFoundError(
        absl_ports::StrCat("Section path ", section_path,
                           " not found in type config ", document.schema()));
  }

  if (separator_position == std::string::npos) {
    // Current property name is the last one in section path
    std::vector<std::string> content = GetPropertyContent(*property_iterator);
    if (content.empty()) {
      // The content of property is explicitly set to empty, we'll treat it as
      // NOT_FOUND because the index doesn't care about empty strings.
      return absl_ports::NotFoundError(
          absl_ports::StrCat("Section path ", section_path,
                             " not found in type config ", document.schema()));
    }
    return content;
  }

  // Gets section content recursively
  std::string_view sub_section_path =
      section_path.substr(separator_position + 1);
  std::vector<std::string> nested_document_content;
  for (const auto& nested_document : property_iterator->document_values()) {
    auto content_or = GetSectionContent(nested_document, sub_section_path);
    if (content_or.ok()) {
      std::vector<std::string> content = std::move(content_or).ValueOrDie();
      std::move(content.begin(), content.end(),
                std::back_inserter(nested_document_content));
    }
  }
  if (nested_document_content.empty()) {
    return absl_ports::NotFoundError(
        absl_ports::StrCat("Section path ", section_path,
                           " not found in type config ", document.schema()));
  }
  return nested_document_content;
}

libtextclassifier3::StatusOr<std::vector<std::string>>
SectionManager::GetSectionContent(const DocumentProto& document,
                                  SectionId section_id) const {
  if (!IsSectionIdValid(section_id)) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "Section id %d is greater than the max value %d", section_id,
        kMaxSectionId));
  }
  TC3_ASSIGN_OR_RETURN(
      const std::vector<SectionMetadata>& metadata_list,
      GetMetadataList(schema_type_mapper_, section_metadata_cache_,
                      document.schema()));
  if (section_id >= metadata_list.size()) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "Section with id %d doesn't exist in type config %s", section_id,
        document.schema().c_str()));
  }
  // The index of metadata list is the same as the section id, so we can use
  // section id as the index.
  return GetSectionContent(document, metadata_list[section_id].path);
}

libtextclassifier3::StatusOr<const SectionMetadata*>
SectionManager::GetSectionMetadata(SchemaTypeId schema_type_id,
                                   SectionId section_id) const {
  if (!IsSectionIdValid(section_id)) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "Section id %d is greater than the max value %d", section_id,
        kMaxSectionId));
  }
  const std::vector<SectionMetadata>& section_metadatas =
      section_metadata_cache_[schema_type_id];
  if (section_id >= section_metadatas.size()) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "Section with id %d doesn't exist in type config with id %d",
        section_id, schema_type_id));
  }

  // The index of metadata list is the same as the section id, so we can use
  // section id as the index.
  return &section_metadatas[section_id];
}

libtextclassifier3::StatusOr<std::vector<Section>>
SectionManager::ExtractSections(const DocumentProto& document) const {
  TC3_ASSIGN_OR_RETURN(
      const std::vector<SectionMetadata>& metadata_list,
      GetMetadataList(schema_type_mapper_, section_metadata_cache_,
                      document.schema()));
  std::vector<Section> sections;
  for (const auto& section_metadata : metadata_list) {
    auto section_content_or =
        GetSectionContent(document, section_metadata.path);
    // Adds to result vector if section is found in document
    if (section_content_or.ok()) {
      sections.emplace_back(SectionMetadata(section_metadata),
                            std::move(section_content_or).ValueOrDie());
    }
  }
  return sections;
}

}  // namespace lib
}  // namespace icing
