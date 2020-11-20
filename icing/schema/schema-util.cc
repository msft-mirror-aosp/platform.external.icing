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

#include "icing/schema/schema-util.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/absl_ports/annotate.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/absl_ports/str_join.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

bool IsCardinalityCompatible(const PropertyConfigProto& old_property,
                             const PropertyConfigProto& new_property) {
  if (old_property.cardinality() < new_property.cardinality()) {
    // We allow a new, less restrictive cardinality (i.e. a REQUIRED field
    // can become REPEATED or OPTIONAL, but not the other way around).
    ICING_VLOG(1) << absl_ports::StrCat(
        "Cardinality is more restrictive than before ",
        PropertyConfigProto::Cardinality::Code_Name(old_property.cardinality()),
        "->",
        PropertyConfigProto::Cardinality::Code_Name(
            new_property.cardinality()));
    return false;
  }
  return true;
}

bool IsDataTypeCompatible(const PropertyConfigProto& old_property,
                          const PropertyConfigProto& new_property) {
  if (old_property.data_type() != new_property.data_type()) {
    // TODO(cassiewang): Maybe we can be a bit looser with this, e.g. we just
    // string cast an int64_t to a string. But for now, we'll stick with
    // simplistics.
    ICING_VLOG(1) << absl_ports::StrCat(
        "Data type ",
        PropertyConfigProto::DataType::Code_Name(old_property.data_type()),
        "->",
        PropertyConfigProto::DataType::Code_Name(new_property.data_type()));
    return false;
  }
  return true;
}

bool IsSchemaTypeCompatible(const PropertyConfigProto& old_property,
                            const PropertyConfigProto& new_property) {
  if (old_property.schema_type() != new_property.schema_type()) {
    ICING_VLOG(1) << absl_ports::StrCat("Schema type ",
                                        old_property.schema_type(), "->",
                                        new_property.schema_type());
    return false;
  }
  return true;
}

bool IsPropertyCompatible(const PropertyConfigProto& old_property,
                          const PropertyConfigProto& new_property) {
  return IsDataTypeCompatible(old_property, new_property) &&
         IsSchemaTypeCompatible(old_property, new_property) &&
         IsCardinalityCompatible(old_property, new_property);
}

bool IsTermMatchTypeCompatible(const StringIndexingConfig& old_indexed,
                               const StringIndexingConfig& new_indexed) {
  return old_indexed.term_match_type() == new_indexed.term_match_type() &&
         old_indexed.tokenizer_type() == new_indexed.tokenizer_type();
}

}  // namespace

libtextclassifier3::Status SchemaUtil::Validate(const SchemaProto& schema) {
  // Tracks SchemaTypeConfigs that we've validated already.
  std::unordered_set<std::string_view> known_schema_types;

  // Tracks SchemaTypeConfigs that have been mentioned (by other
  // SchemaTypeConfigs), but we haven't validated yet.
  std::unordered_set<std::string_view> unknown_schema_types;

  // Tracks PropertyConfigs within a SchemaTypeConfig that we've validated
  // already.
  std::unordered_set<std::string_view> known_property_names;

  for (const auto& type_config : schema.types()) {
    std::string_view schema_type(type_config.schema_type());
    ICING_RETURN_IF_ERROR(ValidateSchemaType(schema_type));

    // We can't have duplicate schema_types
    if (!known_schema_types.insert(schema_type).second) {
      return absl_ports::AlreadyExistsError(absl_ports::StrCat(
          "Field 'schema_type' '", schema_type, "' is already defined"));
    }
    unknown_schema_types.erase(schema_type);

    // We only care about properties being unique within one type_config
    known_property_names.clear();
    for (const auto& property_config : type_config.properties()) {
      std::string_view property_name(property_config.property_name());
      ICING_RETURN_IF_ERROR(ValidatePropertyName(property_name, schema_type));

      // Property names must be unique
      if (!known_property_names.insert(property_name).second) {
        return absl_ports::AlreadyExistsError(absl_ports::StrCat(
            "Field 'property_name' '", property_name,
            "' is already defined for schema '", schema_type, "'"));
      }

      auto data_type = property_config.data_type();
      ICING_RETURN_IF_ERROR(
          ValidateDataType(data_type, schema_type, property_name));

      if (data_type == PropertyConfigProto::DataType::DOCUMENT) {
        // Need to know what schema_type these Document properties should be
        // validated against
        std::string_view property_schema_type(property_config.schema_type());
        libtextclassifier3::Status validated_status =
            ValidateSchemaType(property_schema_type);
        if (!validated_status.ok()) {
          return absl_ports::Annotate(
              validated_status,
              absl_ports::StrCat("Field 'schema_type' is required for DOCUMENT "
                                 "data_types in schema property '",
                                 schema_type, ".", property_name, "'"));
        }

        // Need to make sure we eventually see/validate this schema_type
        if (known_schema_types.count(property_schema_type) == 0) {
          unknown_schema_types.insert(property_schema_type);
        }
      }

      ICING_RETURN_IF_ERROR(ValidateCardinality(property_config.cardinality(),
                                                schema_type, property_name));

      if (data_type == PropertyConfigProto::DataType::STRING) {
        ICING_RETURN_IF_ERROR(ValidateStringIndexingConfig(
            property_config.string_indexing_config(), data_type, schema_type,
            property_name));
      }
    }
  }

  // An Document property claimed to be of a schema_type that we never
  // saw/validated
  if (!unknown_schema_types.empty()) {
    return absl_ports::UnknownError(
        absl_ports::StrCat("Undefined 'schema_type's: ",
                           absl_ports::StrJoin(unknown_schema_types, ",")));
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status SchemaUtil::ValidateSchemaType(
    std::string_view schema_type) {
  // Require a schema_type
  if (schema_type.empty()) {
    return absl_ports::InvalidArgumentError(
        "Field 'schema_type' cannot be empty.");
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status SchemaUtil::ValidatePropertyName(
    std::string_view property_name, std::string_view schema_type) {
  // Require a property_name
  if (property_name.empty()) {
    return absl_ports::InvalidArgumentError(
        absl_ports::StrCat("Field 'property_name' for schema '", schema_type,
                           "' cannot be empty."));
  }

  // Only support alphanumeric values.
  for (char c : property_name) {
    if (!std::isalnum(c)) {
      return absl_ports::InvalidArgumentError(
          absl_ports::StrCat("Field 'property_name' '", property_name,
                             "' can only contain alphanumeric characters."));
    }
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status SchemaUtil::ValidateDataType(
    PropertyConfigProto::DataType::Code data_type, std::string_view schema_type,
    std::string_view property_name) {
  // UNKNOWN is the default enum value and should only be used for backwards
  // compatibility
  if (data_type == PropertyConfigProto::DataType::UNKNOWN) {
    return absl_ports::InvalidArgumentError(absl_ports::StrCat(
        "Field 'data_type' cannot be UNKNOWN for schema property '",
        schema_type, ".", property_name, "'"));
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status SchemaUtil::ValidateCardinality(
    PropertyConfigProto::Cardinality::Code cardinality,
    std::string_view schema_type, std::string_view property_name) {
  // UNKNOWN is the default enum value and should only be used for backwards
  // compatibility
  if (cardinality == PropertyConfigProto::Cardinality::UNKNOWN) {
    return absl_ports::InvalidArgumentError(absl_ports::StrCat(
        "Field 'cardinality' cannot be UNKNOWN for schema property '",
        schema_type, ".", property_name, "'"));
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status SchemaUtil::ValidateStringIndexingConfig(
    const StringIndexingConfig& config,
    PropertyConfigProto::DataType::Code data_type, std::string_view schema_type,
    std::string_view property_name) {
  if (config.term_match_type() == TermMatchType::UNKNOWN &&
      config.tokenizer_type() != StringIndexingConfig::TokenizerType::NONE) {
    // They set a tokenizer type, but no term match type.
    return absl_ports::InvalidArgumentError(absl_ports::StrCat(
        "Indexed string property '", schema_type, ".", property_name,
        "' cannot have a term match type UNKNOWN"));
  }

  if (config.term_match_type() != TermMatchType::UNKNOWN &&
      config.tokenizer_type() == StringIndexingConfig::TokenizerType::NONE) {
    // They set a term match type, but no tokenizer type
    return absl_ports::InvalidArgumentError(
        absl_ports::StrCat("Indexed string property '", property_name,
                           "' cannot have a tokenizer type of NONE"));
  }

  return libtextclassifier3::Status::OK;
}

void SchemaUtil::BuildTypeConfigMap(
    const SchemaProto& schema, SchemaUtil::TypeConfigMap* type_config_map) {
  type_config_map->clear();
  for (const SchemaTypeConfigProto& type_config : schema.types()) {
    type_config_map->emplace(type_config.schema_type(), type_config);
  }
}

SchemaUtil::ParsedPropertyConfigs SchemaUtil::ParsePropertyConfigs(
    const SchemaTypeConfigProto& type_config) {
  ParsedPropertyConfigs parsed_property_configs;

  // TODO(samzheng): consider caching property_config_map for some properties,
  // e.g. using LRU cache. Or changing schema.proto to use go/protomap.
  for (const PropertyConfigProto& property_config : type_config.properties()) {
    parsed_property_configs.property_config_map.emplace(
        property_config.property_name(), &property_config);
    if (property_config.cardinality() ==
        PropertyConfigProto::Cardinality::REQUIRED) {
      parsed_property_configs.num_required_properties++;
    }

    // A non-default term_match_type indicates that this property is meant to be
    // indexed.
    if (property_config.string_indexing_config().term_match_type() !=
        TermMatchType::UNKNOWN) {
      parsed_property_configs.num_indexed_properties++;
    }
  }

  return parsed_property_configs;
}

const SchemaUtil::SchemaDelta SchemaUtil::ComputeCompatibilityDelta(
    const SchemaProto& old_schema, const SchemaProto& new_schema) {
  SchemaDelta schema_delta;
  schema_delta.index_incompatible = false;

  TypeConfigMap new_type_config_map;
  BuildTypeConfigMap(new_schema, &new_type_config_map);

  // Iterate through and check each field of the old schema
  for (const auto& old_type_config : old_schema.types()) {
    auto new_schema_type_and_config =
        new_type_config_map.find(old_type_config.schema_type());

    if (new_schema_type_and_config == new_type_config_map.end()) {
      // Didn't find the old schema type in the new schema, all the old
      // documents of this schema type are invalid without the schema
      ICING_VLOG(1) << absl_ports::StrCat("Previously defined schema type '",
                                          old_type_config.schema_type(),
                                          "' was not defined in new schema");
      schema_delta.schema_types_deleted.insert(old_type_config.schema_type());
      continue;
    }

    ParsedPropertyConfigs new_parsed_property_configs =
        ParsePropertyConfigs(new_schema_type_and_config->second);

    // We only need to check the old, existing properties to see if they're
    // compatible since we'll have old data that may be invalidated or need to
    // be reindexed.
    int32_t old_required_properties = 0;
    int32_t old_indexed_properties = 0;
    for (const auto& old_property_config : old_type_config.properties()) {
      auto new_property_name_and_config =
          new_parsed_property_configs.property_config_map.find(
              old_property_config.property_name());

      if (new_property_name_and_config ==
          new_parsed_property_configs.property_config_map.end()) {
        // Didn't find the old property
        ICING_VLOG(1) << absl_ports::StrCat(
            "Previously defined property type '", old_type_config.schema_type(),
            ".", old_property_config.property_name(),
            "' was not defined in new schema");
        schema_delta.schema_types_incompatible.insert(
            old_type_config.schema_type());
        continue;
      }

      const PropertyConfigProto* new_property_config =
          new_property_name_and_config->second;

      if (!IsPropertyCompatible(old_property_config, *new_property_config)) {
        ICING_VLOG(1) << absl_ports::StrCat(
            "Property '", old_type_config.schema_type(), ".",
            old_property_config.property_name(), "' is incompatible.");
        schema_delta.schema_types_incompatible.insert(
            old_type_config.schema_type());
      }

      if (old_property_config.cardinality() ==
          PropertyConfigProto::Cardinality::REQUIRED) {
        ++old_required_properties;
      }

      // A non-default term_match_type indicates that this property is meant to
      // be indexed.
      if (old_property_config.string_indexing_config().term_match_type() !=
          TermMatchType::UNKNOWN) {
        ++old_indexed_properties;
      }

      // Any change in the indexed property requires a reindexing
      if (!IsTermMatchTypeCompatible(
              old_property_config.string_indexing_config(),
              new_property_config->string_indexing_config())) {
        schema_delta.index_incompatible = true;
      }
    }

    // We can't have new properties that are REQUIRED since we won't know how
    // to backfill the data, and the existing data will be invalid. We're
    // guaranteed from our previous checks that all the old properties are also
    // present in the new property config, so we can do a simple int comparison
    // here to detect new required properties.
    if (new_parsed_property_configs.num_required_properties >
        old_required_properties) {
      ICING_VLOG(1) << absl_ports::StrCat(
          "New schema '", old_type_config.schema_type(),
          "' has REQUIRED properties that are not "
          "present in the previously defined schema");
      schema_delta.schema_types_incompatible.insert(
          old_type_config.schema_type());
    }

    // If we've gained any new indexed properties, then the section ids may
    // change. Since the section ids are stored in the index, we'll need to
    // reindex everything.
    if (new_parsed_property_configs.num_indexed_properties >
        old_indexed_properties) {
      ICING_VLOG(1) << absl_ports::StrCat(
          "Set of indexed properties in schema type '",
          old_type_config.schema_type(),
          "' has  changed, required reindexing.");
      schema_delta.index_incompatible = true;
    }
  }

  return schema_delta;
}

}  // namespace lib
}  // namespace icing
