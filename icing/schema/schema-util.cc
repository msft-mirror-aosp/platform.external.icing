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

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstring>
#include <optional>
#include <queue>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/annotate.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/absl_ports/str_join.h"
#include "icing/feature-flags.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/schema/property-util.h"
#include "icing/util/logging.h"
#include "icing/util/sha256.h"
#include "icing/util/status-macros.h"
#include <google/protobuf/repeated_field.h>

namespace icing {
namespace lib {

namespace {

bool AreStringIndexingConfigsEqual(const StringIndexingConfig& old_config,
                                   const StringIndexingConfig& new_config) {
  return old_config.term_match_type() == new_config.term_match_type() &&
         old_config.tokenizer_type() == new_config.tokenizer_type();
}

bool AreDocumentIndexingConfigsEqual(const DocumentIndexingConfig& old_config,
                                     const DocumentIndexingConfig& new_config) {
  // TODO(b/265304217): This could mark the new schema as incompatible and
  // generate some unnecessary index rebuilds if the two schemas have an
  // equivalent set of indexed properties, but changed the way that it is
  // declared.
  if (old_config.index_nested_properties() !=
      new_config.index_nested_properties()) {
    return false;
  }

  if (old_config.indexable_nested_properties_list().size() !=
      new_config.indexable_nested_properties_list().size()) {
    return false;
  }

  std::unordered_set<std::string_view> old_indexable_nested_properies_set(
      old_config.indexable_nested_properties_list().begin(),
      old_config.indexable_nested_properties_list().end());
  for (const auto& property : new_config.indexable_nested_properties_list()) {
    if (old_indexable_nested_properies_set.find(property) ==
        old_indexable_nested_properies_set.end()) {
      return false;
    }
  }
  return true;
}

bool AreIntegerIndexingConfigsEqual(const IntegerIndexingConfig& old_config,
                                    const IntegerIndexingConfig& new_config) {
  return old_config.numeric_match_type() == new_config.numeric_match_type();
}

bool AreJoinableConfigsEqual(const JoinableConfig& old_config,
                             const JoinableConfig& new_config) {
  return old_config.value_type() == new_config.value_type() &&
         old_config.delete_propagation_type() ==
             new_config.delete_propagation_type();
}

bool AreEmbeddingIndexingConfigsEqual(
    const EmbeddingIndexingConfig& old_config,
    const EmbeddingIndexingConfig& new_config) {
  return old_config.embedding_indexing_type() ==
             new_config.embedding_indexing_type() &&
         old_config.quantization_type() == new_config.quantization_type();
}

bool ArePropertiesEqual(const PropertyConfigProto& old_property,
                        const PropertyConfigProto& new_property) {
  return old_property.property_name() == new_property.property_name() &&
         old_property.description() == new_property.description() &&
         old_property.data_type() == new_property.data_type() &&
         old_property.schema_type() == new_property.schema_type() &&
         old_property.cardinality() == new_property.cardinality() &&
         old_property.scorable_type() == new_property.scorable_type() &&
         AreStringIndexingConfigsEqual(old_property.string_indexing_config(),
                                       new_property.string_indexing_config()) &&
         AreDocumentIndexingConfigsEqual(
             old_property.document_indexing_config(),
             new_property.document_indexing_config()) &&
         AreIntegerIndexingConfigsEqual(
             old_property.integer_indexing_config(),
             new_property.integer_indexing_config()) &&
         AreJoinableConfigsEqual(old_property.joinable_config(),
                                 new_property.joinable_config()) &&
         AreEmbeddingIndexingConfigsEqual(
             old_property.embedding_indexing_config(),
             new_property.embedding_indexing_config());
}

bool IsCardinalityCompatible(const PropertyConfigProto& old_property,
                             const PropertyConfigProto& new_property) {
  if (old_property.cardinality() < new_property.cardinality()) {
    // We allow a new, less restrictive cardinality (i.e. a REQUIRED field
    // can become REPEATED or OPTIONAL, but not the other way around).
    ICING_LOG(INFO) << absl_ports::StrCat(
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
    ICING_LOG(INFO) << absl_ports::StrCat(
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
    ICING_LOG(INFO) << absl_ports::StrCat("Schema type ",
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

// Check account properties compatibility with full support for nested paths:
// 1. Demoting: Changing an existing property from an account property to a
//    regular property is COMPATIBLE.
// 2. Promoting: Changing an existing regular property into an account
//    property is INCOMPATIBLE.
// 3. New property: Introducing a completely new property and defining it
//    as an account property is COMPATIBLE.
bool IsAccountPropertyIncompatible(
    const SchemaTypeConfigProto& old_type_config,
    const SchemaTypeConfigProto& new_type_config,
    const SchemaUtil::TypeConfigMap& old_type_config_map) {
  // Track all account properties that already existed in the older definition.
  std::unordered_set<std::string_view> old_account_properties;
  for (const auto& prop : old_type_config.account_properties()) {
    old_account_properties.insert(prop);
  }

  // Iterate through each account property path declared in the new schema
  // version.
  for (std::string_view new_account_path :
       new_type_config.account_properties()) {
    // If the path was already an account property in the past, it remains fully
    // compatible.
    if (old_account_properties.count(new_account_path) > 0) {
      continue;
    }

    // Trace down the path components inside the old schema to evaluate if this
    // property chain is newly introduced or if it's an existing regular field
    // being upgraded.
    std::vector<std::string_view> path_segments =
        property_util::SplitPropertyPathExpr(new_account_path);

    const SchemaTypeConfigProto* current_old_type = &old_type_config;
    bool property_existed_in_old_schema = true;

    for (size_t i = 0; i < path_segments.size(); ++i) {
      property_util::PropertyInfo prop_info =
          property_util::ParsePropertyNameExpr(path_segments[i]);

      const PropertyConfigProto* matched_old_prop = nullptr;
      for (const auto& old_prop : current_old_type->properties()) {
        if (old_prop.property_name() == prop_info.name) {
          matched_old_prop = &old_prop;
          break;
        }
      }

      // If any node in the path is missing from the old schema layout, it
      // counts as a brand-new property path addition, which is safe and
      // compatible.
      if (matched_old_prop == nullptr) {
        property_existed_in_old_schema = false;
        break;
      }

      // If there are more segments to explore, descend into the nested
      // document.
      if (i < path_segments.size() - 1) {
        if (matched_old_prop->data_type() !=
            PropertyConfigProto::DataType::DOCUMENT) {
          property_existed_in_old_schema = false;
          break;
        }

        auto old_lookup_it =
            old_type_config_map.find(matched_old_prop->schema_type());
        if (old_lookup_it == old_type_config_map.end()) {
          property_existed_in_old_schema = false;
          break;
        }
        current_old_type = &old_lookup_it->second;
      }
    }

    // If the entire property path previously existed but lacked an account
    // designation, it means a normal structural field was promoted. This breaks
    // backfilling compatibility.
    if (property_existed_in_old_schema) {
      ICING_LOG(INFO) << absl_ports::StrCat(
          "Property path '", old_type_config.schema_type(), ".",
          new_account_path,
          "' was promoted to an account property, which is incompatible.");
      return true;  // Found an incompatibility.
    }
  }

  return false;  // All account properties are compatible.
}

// Validates that all path expressions defined in 'account_properties' across
// all schema types point to valid, existing properties within the schema
// definition. This executes as a top-down verification phase after basic
// property checks pass.
libtextclassifier3::Status ValidateAllAccountProperties(
    const SchemaProto& schema) {
  // 1. Build a forward lookup map from schema_type name to its config proto.
  // This allows O(1) random-access retrieval of any child type config during
  // deep nested document path traversal, keeping overall complexity at O(N).
  std::unordered_map<std::string_view, const SchemaTypeConfigProto*>
      schema_type_lookup;
  for (const auto& type_config : schema.types()) {
    schema_type_lookup[type_config.schema_type()] = &type_config;
  }

  // 2. Iterate through every schema type defined in the master schema.
  for (const auto& type_config : schema.types()) {
    std::string_view schema_type(type_config.schema_type());

    // 3. Process each account property path expression defined for the current
    // type.
    for (const auto& account_property : type_config.account_properties()) {
      std::vector<std::string_view> path_segments =
          property_util::SplitPropertyPathExpr(account_property);

      const SchemaTypeConfigProto* current_type = &type_config;

      // 4. Trace the path segments sequentially from top to bottom.
      for (size_t i = 0; i < path_segments.size(); ++i) {
        property_util::PropertyInfo prop_info =
            property_util::ParsePropertyNameExpr(path_segments[i]);

        const PropertyConfigProto* matched_prop = nullptr;
        // Search for a matching property declaration inside the current tier's
        // configuration.
        for (const auto& prop : current_type->properties()) {
          if (prop.property_name() == prop_info.name) {
            matched_prop = &prop;
            break;
          }
        }

        if (matched_prop == nullptr) {
          return absl_ports::InvalidArgumentError(absl_ports::StrCat(
              "Account property path '", account_property,
              "' is invalid or does not exist in schema type '", schema_type,
              "'"));
        }

        // 5. If this is an intermediate segment (not the leaf node), we must
        // descend further.
        if (i < path_segments.size() - 1) {
          if (matched_prop->data_type() !=
              PropertyConfigProto::DataType::DOCUMENT) {
            return absl_ports::InvalidArgumentError(
                absl_ports::StrCat("Account property path '", account_property,
                                   "' is invalid because '", prop_info.name,
                                   "' is not a DOCUMENT type"));
          }

          auto lookup_it = schema_type_lookup.find(matched_prop->schema_type());
          if (lookup_it == schema_type_lookup.end()) {
            return absl_ports::InvalidArgumentError(
                absl_ports::StrCat("Account property path '", account_property,
                                   "' references a non-existent schema type '",
                                   matched_prop->schema_type(), "'"));
          }

          // Advance our structural pointer deeper into the child document
          // definition for the next loop cycle.
          current_type = lookup_it->second;
        }
      }
    }
  }
  return libtextclassifier3::Status::OK;
}

// Propagates schema type incompatibilities through the dependency graph.
// Every type already present in `incompatible_delta` is treated as a seed: any
// type that (transitively) depends on a seed is itself incompatible and gets
// added to `incompatible_delta`. This lets callers collect all directly
// incompatible types first and then propagate them in a single BFS pass,
// rather than running a separate BFS per seed type.
void PropagateIncompatibleChangeToDelta(
    std::unordered_set<std::string>& incompatible_delta,
    const SchemaUtil::DependentMap& new_schema_dependent_map,
    const SchemaUtil::TypeConfigMap& old_type_config_map) {
  // Seed the BFS queue with all types that are already marked incompatible.
  std::queue<std::string_view> queue;
  for (const std::string& incompatible_type : incompatible_delta) {
    queue.push(incompatible_type);
  }

  while (!queue.empty()) {
    std::string_view curr_type = queue.front();
    queue.pop();
    auto dependent_types_itr = new_schema_dependent_map.find(curr_type);
    if (dependent_types_itr == new_schema_dependent_map.end()) {
      continue;
    }
    for (const auto& [dependent_type, _] : dependent_types_itr->second) {
      // The types from new_schema that depend on the current type may not be
      // present in old_schema. Those types will be listed at
      // schema_delta.schema_types_new instead.
      std::string dependent_type_str(dependent_type);
      if (old_type_config_map.find(dependent_type_str) !=
          old_type_config_map.end()) {
        if (incompatible_delta.insert(std::move(dependent_type_str)).second) {
          queue.push(dependent_type);
        }
      }
    }
  }
}

// Returns if C1 <= C2 based on the following rule, where C1 and C2 are
// cardinalities that can be one of REPEATED, OPTIONAL, or REQUIRED.
//
// Rule: REQUIRED < OPTIONAL < REPEATED
bool CardinalityLessThanEq(PropertyConfigProto::Cardinality::Code C1,
                           PropertyConfigProto::Cardinality::Code C2) {
  if (C1 == C2) {
    return true;
  }
  if (C1 == PropertyConfigProto::Cardinality::REQUIRED) {
    return C2 == PropertyConfigProto::Cardinality::OPTIONAL ||
           C2 == PropertyConfigProto::Cardinality::REPEATED;
  }
  if (C1 == PropertyConfigProto::Cardinality::OPTIONAL) {
    return C2 == PropertyConfigProto::Cardinality::REPEATED;
  }
  return false;
}

// Check if set1 is a subset of set2.
template <typename T>
bool IsSubset(const std::unordered_set<T>& set1,
              const std::unordered_set<T>& set2) {
  for (const auto& item : set1) {
    if (set2.find(item) == set2.end()) {
      return false;
    }
  }
  return true;
}

// Builds a map of {schema_type -> set of scorable property names}
std::unordered_map<std::string_view, std::unordered_set<std::string_view>>
BuildTypeToScorablePropertyNamesMap(
    const SchemaUtil::TypeConfigMap& type_config_map) {
  std::unordered_map<std::string_view, std::unordered_set<std::string_view>>
      type_to_scorable_property_names_map;
  for (const auto& [schema_type, schema_type_config] : type_config_map) {
    for (const PropertyConfigProto& property_config :
         schema_type_config.properties()) {
      if (property_config.scorable_type() ==
          PropertyConfigProto::ScorableType::ENABLED) {
        type_to_scorable_property_names_map[schema_type].insert(
            property_config.property_name());
      }
    }
  }
  return type_to_scorable_property_names_map;
}

// Finds the schema types that have inconsistent scorable properties, which will
// be added in place in the `schema_delta`.
void FindScorablePropertyInconsistentTypes(
    const SchemaUtil::TypeConfigMap& old_type_config_map,
    const SchemaUtil::TypeConfigMap& new_type_config_map,
    const SchemaUtil::DependentMap& new_schema_dependent_map,
    SchemaUtil::SchemaDelta* schema_delta) {
  std::unordered_map<std::string_view, std::unordered_set<std::string_view>>
      new_type_to_scorable_property_names_map =
          BuildTypeToScorablePropertyNamesMap(new_type_config_map);
  std::unordered_map<std::string_view, std::unordered_set<std::string_view>>
      old_type_to_scorable_property_names_map =
          BuildTypeToScorablePropertyNamesMap(old_type_config_map);
  for (const auto& [schema_type, _] : old_type_config_map) {
    if (new_type_config_map.find(schema_type) == new_type_config_map.end()) {
      // The type has been deleted in the new schema.
      continue;
    }
    auto old_schema_type_property_names_iter =
        old_type_to_scorable_property_names_map.find(schema_type);
    auto new_schema_type_property_names_iter =
        new_type_to_scorable_property_names_map.find(schema_type);
    bool has_scorable_properties_in_old_schema =
        old_schema_type_property_names_iter !=
        old_type_to_scorable_property_names_map.end();
    bool has_scorable_properties_in_new_schema =
        new_schema_type_property_names_iter !=
        new_type_to_scorable_property_names_map.end();
    if (has_scorable_properties_in_old_schema &&
        !has_scorable_properties_in_new_schema) {
      schema_delta->schema_types_scorable_property_inconsistent.insert(
          schema_type);
    } else if (!has_scorable_properties_in_old_schema &&
               has_scorable_properties_in_new_schema) {
      schema_delta->schema_types_scorable_property_inconsistent.insert(
          schema_type);
    } else if (has_scorable_properties_in_old_schema &&
               has_scorable_properties_in_new_schema) {
      // The sets of scorable properties from the old and new schema are
      // different.
      if (old_schema_type_property_names_iter->second !=
          new_schema_type_property_names_iter->second) {
        schema_delta->schema_types_scorable_property_inconsistent.insert(
            schema_type);
      }
    }
  }

  // Now, look up the DependentMap of the new schema config and find the parent
  // types that depend on the currently discovered inconsistent types.
  std::vector<std::string_view> parent_types;
  for (const std::string& schema_type :
       schema_delta->schema_types_scorable_property_inconsistent) {
    auto parent_type_maps_iter = new_schema_dependent_map.find(schema_type);
    if (parent_type_maps_iter == new_schema_dependent_map.end()) {
      continue;
    }
    for (const auto& [parent_type, _] : parent_type_maps_iter->second) {
      parent_types.push_back(parent_type);
    }
  }
  schema_delta->schema_types_scorable_property_inconsistent.insert(
      parent_types.begin(), parent_types.end());
}

}  // namespace

// SchemaUtil::TypeConfigInfoCache methods.
libtextclassifier3::Status SchemaUtil::TypeConfigInfoCache::AddTypeConfig(
    SchemaTypeConfigProto&& type_config) {
  const std::string& type_name = type_config.schema_type();
  if (!enable_schema_definition_deduping_) {
    // Schema definition deduping is disabled. Just insert the type config
    // directly.
    type_config_map_.insert({type_name, std::move(type_config)});
    return libtextclassifier3::Status::OK;
  }

  // Schema definition deduping enabled
  // Step 1: Check that the type config is not already in the type_config_map_.
  if (type_config_map_.find(type_name) != type_config_map_.end()) {
    ICING_VLOG(1) << "Schema type '" << type_name
                  << "' not added because it already exists in the "
                     "type_config_info_cache.";
    return libtextclassifier3::Status::OK;
  }

  // Step 2: Compute the properties digest.
  std::optional<Sha256Digest> properties_digest =
      SchemaUtil::GetSchemaPropertiesDigest(type_config);
  Sha256Digest properties_digest_value;
  if (properties_digest) {
    properties_digest_value = std::move(*properties_digest);
  } else {
    // The properties digest is empty or corrupted. We can still recompute the
    // properties digest if either:
    // 1. The properties digest is empty (this indicates that this is the first
    //    time the config is provided and it has not been processed yet)
    // 2. The properties field is not empty. This means that the current digest
    //    value must be corrupted. Therefore, we will recalculate this.
    //
    // Populate the properties digest field once recomputed so that the correct
    // digest is stored in the type_config_map_.
    if (type_config.properties_digest().empty() ||
        !type_config.properties().empty()) {
      properties_digest_value =
          SchemaUtil::PopulatePropertiesDigestField(type_config);
    } else {
      return absl_ports::InternalError(absl_ports::StrCat(
          "Cannot add schema type config '", type_name,
          "' because its digest is corrupted and cannot be recomputed."));
    }
  }

  // Step 3: Insert the type config into the maps.
  std::vector<std::string>& schema_types =
      properties_sha256_digest_map_[properties_digest_value];
  if (schema_types.empty()) {
    // First type with this properties digest. Insert into both maps directly as
    // is.
    schema_types.push_back(type_name);
    type_config_map_.insert({type_name, std::move(type_config)});
    return libtextclassifier3::Status::OK;
  }

  // Guaranteed to have at least one type matching this properties digest at
  // this point.
  if (type_config.properties().empty()) {
    // Type has already been deduped.
    // - This happens when we're adding types to create the TypeConfigInfoCache
    //   from an existing, already deduped schema.
    // Insert into both maps directly.
    schema_types.push_back(type_name);
    type_config_map_.insert({type_name, std::move(type_config)});
    return libtextclassifier3::Status::OK;
  }

  // The input type config is a fully defined type config. We need to check if
  // it should be deduped before inserting into the type-config map.
  auto first_type_itr = type_config_map_.find(schema_types.front());
  if (first_type_itr == type_config_map_.end()) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Type '", schema_types.front(),
        "exists in the properties_sha256_digest_map_ but not the "
        "type_config_map_. This should never happen."));
  }
  const SchemaTypeConfigProto& first_type_config = first_type_itr->second;
  if (first_type_config.properties().empty()) {
    // First type in the digest vector is not the canonical type config.
    // - This happens when we're adding types to create the TypeConfigInfoCache
    //   from an existing, already deduped schema, and deduped types are added
    //   before the canonical type config.
    // The input type will be the canonical type for this property digest.
    //
    // Insert to front of the vector and do not dedupe.
    schema_types.insert(schema_types.begin(), type_name);
  } else {
    // Otherwise, push to the back of the vector.
    // This is a duplicate type config that should not have any property
    // definitions. Clear the properties field before inserting into the map.
    schema_types.push_back(type_name);
    type_config.clear_properties();
  }

  type_config_map_.insert({type_name, std::move(type_config)});
  return libtextclassifier3::Status::OK;
}

libtextclassifier3::StatusOr<SchemaUtil::TypeConfigMap>
SchemaUtil::TypeConfigInfoCache::CalculateSchemaUpdatePlan(
    std::vector<SchemaTypeConfigProto>&& types_to_add,
    std::unordered_set<std::string_view>&& types_to_remove) const {
  TypeConfigMap types_to_update;
  types_to_update.reserve(types_to_add.size());

  if (!enable_schema_definition_deduping_) {
    // Deduping is disabled. All type configs can be removed/added directly.
    // Move all types to add into the type_config_map and return.
    for (SchemaTypeConfigProto& type : types_to_add) {
      types_to_update.insert({type.schema_type(), std::move(type)});
    }
    return types_to_update;
  }

  // Step 1: Handle type additions
  std::unordered_map<Sha256Digest, std::vector<std::string>>
      added_type_names_by_digest;

  for (SchemaTypeConfigProto& type : types_to_add) {
    // Populate properties digest field and decide if this type should be
    // deduped.
    Sha256Digest digest = SchemaUtil::PopulatePropertiesDigestField(type);
    std::string type_name = type.schema_type();
    added_type_names_by_digest[digest].push_back(type_name);

    bool already_in_cache = properties_sha256_digest_map_.count(digest) > 0;
    bool is_first_new_addition =
        (added_type_names_by_digest[digest].size() == 1);
    if (already_in_cache || !is_first_new_addition) {
      // This type is a duplicate of something in the cache OR
      // a duplicate of another type being added in this same batch.
      //
      // This type can be deduped. Clear the properties field before adding to
      // the update map.
      type.clear_properties();
    }
    types_to_update.insert({type_name, std::move(type)});
  }

  // Step 2: Handle type removals
  std::unordered_map<Sha256Digest, std::unordered_set<std::string>>
      removed_names_by_digest;
  for (std::string_view type_view : types_to_remove) {
    std::string type_name(type_view);
    auto itr = type_config_map_.find(type_name);
    if (itr == type_config_map_.end()) {
      // Type doesn't exist in the current cache. Skip it.
      continue;
    }
    std::optional<Sha256Digest> digest = GetSchemaPropertiesDigest(itr->second);
    if (!digest) {
      return absl_ports::InternalError(absl_ports::StrCat(
          "Cannot delete type due to corrupted digest: ", type_name));
    }
    removed_names_by_digest[*digest].insert(std::move(type_name));
  }

  for (auto& [digest, removed_types] : removed_names_by_digest) {
    auto cache_it = properties_sha256_digest_map_.find(digest);
    if (cache_it == properties_sha256_digest_map_.end() ||
        cache_it->second.empty()) {
      continue;
    }

    const std::vector<std::string>& existing_types = cache_it->second;
    const std::string& original_canonical = existing_types.front();
    // Case 1: We can safely remove all types in matching_types_to_remove
    // without needing to transfer canonical type definitions.
    //
    // This happens in these 2 scenarios:
    // 1. The original canonical type for this digest still exists in the
    //    cache after this update (i.e. it's not in types_to_remove).
    // 2. No more types will be left for this digest after this update (i.e.
    //    all types for this digest are being removed and none were added).
    bool removing_canonical = removed_types.count(original_canonical) > 0;
    bool removing_all_existing =
        (removed_types.size() == existing_types.size());
    bool adding_new = added_type_names_by_digest.count(digest) > 0;
    if (!removing_canonical || (removing_all_existing && !adding_new)) {
      continue;
    }

    // Case 2: We need to find a new canonical type and transfer the property
    // definitions.
    //
    // There are 2 sub-cases:
    // a. We added new types for this digest in step 1 (type addition step)
    // b. We did not add new types for this digest in step 1.
    //
    // First make sure we can get the original canonical type.
    auto original_canonical_itr = type_config_map_.find(original_canonical);
    if (original_canonical_itr == type_config_map_.end()) {
      return absl_ports::InternalError(absl_ports::StrCat(
          "Cannot find the original canonical type config '",
          original_canonical,
          "' in the type_config_map. This should never happen."));
    }
    const SchemaTypeConfigProto& original_canonical_proto =
        original_canonical_itr->second;
    std::string target_canonical_name;
    if (adding_new) {
      // Sub-case a: We added new types for this digest in step 1.
      // Use the first added type as the new canonical type.
      target_canonical_name = added_type_names_by_digest[digest].front();
    } else {
      // Sub-case b: We did not add new types for this digest in step 1.
      // A previously deduped type will be promoted to canonical.
      for (const std::string& existing_type : existing_types) {
        if (removed_types.count(existing_type) == 0) {
          target_canonical_name = existing_type;
          break;
        }
      }
    }

    if (!target_canonical_name.empty()) {
      // If the target is a new type, it would have already been added to
      // types_to_update in step 1.
      auto update_itr = types_to_update.find(target_canonical_name);
      if (update_itr == types_to_update.end()) {
        // Target canonical is a previously deduped type. Look it up from the
        // cache and add its copy to the update map.
        auto existing_itr = type_config_map_.find(target_canonical_name);
        if (existing_itr == type_config_map_.end()) {
          return absl_ports::InternalError(absl_ports::StrCat(
              "Cannot find new target canonical type config '",
              target_canonical_name,
              "' in the type_config_map. This should never happen."));
        }
        types_to_update[target_canonical_name] =
            SchemaTypeConfigProto(existing_itr->second);
      }

      // Guaranteed that the target canonical is in the update map at this
      // point. Copy the properties from the original canonical type to the
      // target canonical type.
      SchemaTypeConfigProto& target_canonical_proto =
          types_to_update[target_canonical_name];
      target_canonical_proto.mutable_properties()->CopyFrom(
          original_canonical_proto.properties());
    }
  }

  return types_to_update;
}

libtextclassifier3::StatusOr<SchemaUtil::TypeConfigInfoCache::TypeConfigHolder>
SchemaUtil::TypeConfigInfoCache::GetFullSchemaTypeConfigHolder(
    std::string_view schema_type) const {
  auto type_config_itr = type_config_map_.find(
      std::string(schema_type.data(), schema_type.size()));
  if (type_config_itr == type_config_map_.end()) {
    return absl_ports::NotFoundError(
        absl_ports::StrCat("Schema type config '", schema_type, "' not found"));
  }

  const SchemaTypeConfigProto& type_config = type_config_itr->second;
  // Schema definitions are not deduped, or the type config has already been
  // fully defined. We can just return the type config as-is.
  if (!enable_schema_definition_deduping_ ||
      !type_config.properties().empty()) {
    return TypeConfigHolder(type_config, type_config.properties());
  }

  // This type config has been deduped. Construct the full proto by looking
  // up the properties_sha256_digest_map_.
  std::optional<Sha256Digest> properties_digest =
      SchemaUtil::GetSchemaPropertiesDigest(type_config);
  if (!properties_digest) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Cannot get properties digest for type '", schema_type, "'"));
  }
  auto duplicate_types_itr =
      properties_sha256_digest_map_.find(*properties_digest);
  if (duplicate_types_itr == properties_sha256_digest_map_.end() ||
      duplicate_types_itr->second.empty()) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "properties_digest for type '", schema_type,
        "' does not exist in the properties_sha256_digest_map_. This should "
        "never happen."));
  }

  // Return a TypeConfigHolder with references to the properties from the
  // canonical type config.
  auto canonical_type_config_itr =
      type_config_map_.find(duplicate_types_itr->second.front());
  if (canonical_type_config_itr == type_config_map_.end()) {
    return absl_ports::InternalError(absl_ports::StrCat(
        "Failed to find the canonical type config for type '", schema_type,
        "' in the type_config_map. This should never happen."));
  }

  return TypeConfigHolder(type_config,
                          canonical_type_config_itr->second.properties());
}

libtextclassifier3::StatusOr<const SchemaTypeConfigProto*>
SchemaUtil::TypeConfigInfoCache::GetRawSchemaTypeConfigPointer(
    std::string_view schema_type) const {
  auto type_config_itr = type_config_map_.find(
      std::string(schema_type.data(), schema_type.size()));
  if (type_config_itr == type_config_map_.end()) {
    return absl_ports::NotFoundError(
        absl_ports::StrCat("Schema type config '", schema_type, "' not found"));
  }

  return &type_config_itr->second;
}

libtextclassifier3::StatusOr<bool>
SchemaUtil::TypeConfigInfoCache::IsSchemaTypeConfigDeduped(
    std::string_view schema_type) const {
  if (!enable_schema_definition_deduping_) {
    return false;
  }

  ICING_ASSIGN_OR_RETURN(const SchemaTypeConfigProto* raw_type_config,
                         GetRawSchemaTypeConfigPointer(schema_type));
  if (!raw_type_config->properties().empty()) {
    return false;
  }
  ICING_ASSIGN_OR_RETURN(TypeConfigHolder type_config_holder,
                         GetFullSchemaTypeConfigHolder(schema_type));
  // The (possibly de-duped) type config had no properties. Therefore, this is
  // deduped *unless* the type actually has no properties.
  return !type_config_holder.properties().empty();
}

// SchemaUtil methods.
libtextclassifier3::Status CalculateTransitiveNestedTypeRelations(
    const SchemaUtil::DependentMap& direct_nested_types_map,
    const std::unordered_set<std::string_view>& joinable_types,
    std::string_view type, bool path_contains_joinable_property,
    SchemaUtil::DependentMap* expanded_nested_types_map,
    std::unordered_map<std::string_view, bool>&&
        pending_expansion_paths_indexable,
    std::unordered_set<std::string_view>* sink_types) {
  // TODO(b/280698121): Implement optimizations to this code to avoid reentering
  // a node after it's already been expanded.

  auto itr = direct_nested_types_map.find(type);
  if (itr == direct_nested_types_map.end()) {
    // It's a sink node. Just return.
    sink_types->insert(type);
    return libtextclassifier3::Status::OK;
  }
  std::unordered_map<std::string_view, std::vector<const PropertyConfigProto*>>
      expanded_relations;

  // Add all of the adjacent outgoing relations.
  expanded_relations.reserve(itr->second.size());
  expanded_relations.insert(itr->second.begin(), itr->second.end());

  // Iterate through each adjacent outgoing relation and add their indirect
  // outgoing relations.
  for (const auto& [adjacent_type, adjacent_property_protos] : itr->second) {
    // Make a copy of pending_expansion_paths_indexable for every iteration.
    std::unordered_map<std::string_view, bool> pending_expansion_paths_copy(
        pending_expansion_paths_indexable);

    // 1. Check the nested indexable config of the edge (type -> adjacent_type),
    //    and the joinable config of the current path up to adjacent_type.
    //
    // The nested indexable config is true if any of the PropertyConfigProtos
    // representing the connecting edge has index_nested_properties=true.
    bool is_edge_nested_indexable = std::any_of(
        adjacent_property_protos.begin(), adjacent_property_protos.end(),
        [](const PropertyConfigProto* property_config) {
          return property_config->document_indexing_config()
              .index_nested_properties();
        });
    // TODO(b/265304217): change this once we add joinable_properties_list.
    // Check if addition of the new edge (type->adjacent_type) makes the path
    // joinable.
    bool new_path_contains_joinable_property =
        joinable_types.count(type) > 0 || path_contains_joinable_property;
    // Set is_nested_indexable field for the current edge
    pending_expansion_paths_copy[type] = is_edge_nested_indexable;

    // If is_edge_nested_indexable=false, then all paths to adjacent_type
    // currently in the pending_expansions map are also not nested indexable.
    if (!is_edge_nested_indexable) {
      for (auto& pending_expansion : pending_expansion_paths_copy) {
        pending_expansion.second = false;
      }
    }

    // 2. Check if we're in the middle of expanding this type - IOW
    // there's a cycle!
    //
    // This cycle is not allowed if either:
    //  1. The cycle starting at adjacent_type is nested indexable, OR
    //  2. The current path contains a joinable property.
    auto adjacent_itr = pending_expansion_paths_copy.find(adjacent_type);
    if (adjacent_itr != pending_expansion_paths_copy.end()) {
      if (adjacent_itr->second || new_path_contains_joinable_property) {
        return absl_ports::InvalidArgumentError(absl_ports::StrCat(
            "Invalid cycle detected in type configs. '", type,
            "' references itself and is nested-indexable or nested-joinable."));
      }
      // The cycle is allowed and there's no need to keep iterating the loop.
      // Move on to the next adjacent value.
      continue;
    }

    // 3. Expand this type as needed.
    ICING_RETURN_IF_ERROR(CalculateTransitiveNestedTypeRelations(
        direct_nested_types_map, joinable_types, adjacent_type,
        new_path_contains_joinable_property, expanded_nested_types_map,
        std::move(pending_expansion_paths_copy), sink_types));
    if (sink_types->count(adjacent_type) > 0) {
      // "adjacent" is a sink node. Just skip to the next.
      continue;
    }

    // 4. "adjacent" has been fully expanded. Add all of its transitive
    // outgoing relations to this type's transitive outgoing relations.
    auto adjacent_expanded_itr = expanded_nested_types_map->find(adjacent_type);
    for (const auto& [transitive_reachable, _] :
         adjacent_expanded_itr->second) {
      // Insert a transitive reachable node `transitive_reachable` for `type` if
      // it wasn't previously reachable.
      // Since there is no direct edge between `type` and `transitive_reachable`
      // we insert an empty vector into the dependent map.
      expanded_relations.insert({transitive_reachable, {}});
    }
  }
  for (const auto& kvp : expanded_relations) {
    expanded_nested_types_map->operator[](type).insert(kvp);
  }
  return libtextclassifier3::Status::OK;
}

template <typename T>
libtextclassifier3::Status CalculateAcyclicTransitiveRelations(
    const SchemaUtil::TypeRelationMap<T>& direct_relation_map,
    std::string_view type,
    SchemaUtil::TypeRelationMap<T>* expanded_relation_map,
    std::unordered_set<std::string_view>* pending_expansions,
    std::unordered_set<std::string_view>* sink_types) {
  auto expanded_itr = expanded_relation_map->find(type);
  if (expanded_itr != expanded_relation_map->end()) {
    // We've already expanded this type. Just return.
    return libtextclassifier3::Status::OK;
  }
  auto itr = direct_relation_map.find(type);
  if (itr == direct_relation_map.end()) {
    // It's a sink node. Just return.
    sink_types->insert(type);
    return libtextclassifier3::Status::OK;
  }
  pending_expansions->insert(type);
  std::unordered_map<std::string_view, T> expanded_relations;

  // Add all of the adjacent outgoing relations.
  expanded_relations.reserve(itr->second.size());
  expanded_relations.insert(itr->second.begin(), itr->second.end());

  // Iterate through each adjacent outgoing relation and add their indirect
  // outgoing relations.
  for (const auto& [adjacent, _] : itr->second) {
    // 1. Check if we're in the middle of expanding this type - IOW there's a
    // cycle!
    if (pending_expansions->count(adjacent) > 0) {
      return absl_ports::InvalidArgumentError(
          absl_ports::StrCat("Invalid cycle detected in type configs. '", type,
                             "' references or inherits from itself."));
    }

    // 2. Expand this type as needed.
    ICING_RETURN_IF_ERROR(CalculateAcyclicTransitiveRelations(
        direct_relation_map, adjacent, expanded_relation_map,
        pending_expansions, sink_types));
    if (sink_types->count(adjacent) > 0) {
      // "adjacent" is a sink node. Just skip to the next.
      continue;
    }

    // 3. "adjacent" has been fully expanded. Add all of its transitive outgoing
    // relations to this type's transitive outgoing relations.
    auto adjacent_expanded_itr = expanded_relation_map->find(adjacent);
    for (const auto& [transitive_reachable, _] :
         adjacent_expanded_itr->second) {
      // Insert a transitive reachable node `transitive_reachable` for `type`.
      // Also since there is no direct edge between `type` and
      // `transitive_reachable`, the direct edge is initialized by default.
      expanded_relations.insert({transitive_reachable, T()});
    }
  }
  expanded_relation_map->insert({type, std::move(expanded_relations)});
  pending_expansions->erase(type);
  return libtextclassifier3::Status::OK;
}

// Calculate and return the expanded nested-type map from
// direct_nested_type_map. This expands the direct_nested_type_map to also
// include indirect nested-type relations.
//
// Ex. Suppose we have the following relations in direct_nested_type_map.
//
// C -> B (Schema type B has a document property of type C)
// B -> A (Schema type A has a document property of type B)
//
// Then, this function would expand the map by adding C -> A to the map.
libtextclassifier3::StatusOr<SchemaUtil::DependentMap>
CalculateTransitiveNestedTypeRelations(
    const SchemaUtil::DependentMap& direct_nested_type_map,
    const std::unordered_set<std::string_view>& joinable_types,
    bool allow_circular_schema_definitions) {
  SchemaUtil::DependentMap expanded_nested_type_map;
  // Types that have no outgoing relations.
  std::unordered_set<std::string_view> sink_types;

  if (allow_circular_schema_definitions) {
    // Map of nodes that are pending expansion -> whether the path from each key
    // node to the 'current' node is nested_indexable.
    // A copy of this map is made for each new node that we expand.
    std::unordered_map<std::string_view, bool>
        pending_expansion_paths_indexable;
    for (const auto& kvp : direct_nested_type_map) {
      ICING_RETURN_IF_ERROR(CalculateTransitiveNestedTypeRelations(
          direct_nested_type_map, joinable_types, kvp.first,
          /*path_contains_joinable_property=*/false, &expanded_nested_type_map,
          std::unordered_map<std::string_view, bool>(
              pending_expansion_paths_indexable),
          &sink_types));
    }
  } else {
    // If allow_circular_schema_definitions is false, then fallback to the old
    // way of detecting cycles.
    // Types that we are expanding.
    std::unordered_set<std::string_view> pending_expansions;
    for (const auto& kvp : direct_nested_type_map) {
      ICING_RETURN_IF_ERROR(CalculateAcyclicTransitiveRelations(
          direct_nested_type_map, kvp.first, &expanded_nested_type_map,
          &pending_expansions, &sink_types));
    }
  }
  return expanded_nested_type_map;
}

// Calculate and return the expanded inheritance map from
// direct_nested_type_map. This expands the direct_inheritance_map to also
// include indirect inheritance relations.
//
// Ex. Suppose we have the following relations in direct_inheritance_map.
//
// C -> B (Schema type C is B's parent_type )
// B -> A (Schema type B is A's parent_type)
//
// Then, this function would expand the map by adding C -> A to the map.
libtextclassifier3::StatusOr<SchemaUtil::InheritanceMap>
CalculateTransitiveInheritanceRelations(
    const SchemaUtil::InheritanceMap& direct_inheritance_map) {
  SchemaUtil::InheritanceMap expanded_inheritance_map;

  // Types that we are expanding.
  std::unordered_set<std::string_view> pending_expansions;

  // Types that have no outgoing relation.
  std::unordered_set<std::string_view> sink_types;
  for (const auto& kvp : direct_inheritance_map) {
    ICING_RETURN_IF_ERROR(CalculateAcyclicTransitiveRelations(
        direct_inheritance_map, kvp.first, &expanded_inheritance_map,
        &pending_expansions, &sink_types));
  }
  return expanded_inheritance_map;
}

// Builds a transitive dependent map. Types with no dependents will not be
// present in the map as keys.
//
// Ex. Suppose we have a schema with four types A, B, C, D. A has a property of
// type B and B has a property of type C. C and D only have non-document
// properties.
//
// The transitive dependent map for this schema would be:
// C -> A, B (both A and B depend on C)
// B -> A (A depends on B)
//
// A and D will not be present in the map as keys because no type depends on
// them.
//
// RETURNS:
//   On success, a transitive dependent map of all types in the schema.
//   INVALID_ARGUMENT if the schema contains a cycle or an undefined type.
//   ALREADY_EXISTS if a schema type is specified more than once in the schema
libtextclassifier3::StatusOr<SchemaUtil::DependentMap>
BuildTransitiveDependentGraph(const SchemaProto& schema,
                              bool allow_circular_schema_definitions) {
  // We expand the nested-type dependent map and inheritance map differently
  // when calculating transitive relations. These two types of relations also
  // should not be transitive so we keep these as separate maps.
  //
  // e.g. For schema type A, B and C, B depends on A through inheritance, and
  // C depends on B by having a property with type B, we will have the two
  // relations {A, B} and {B, C} in the dependent map, but will not have {A, C}
  // in the map.
  SchemaUtil::DependentMap direct_nested_type_map;
  SchemaUtil::InheritanceMap direct_inheritance_map;

  // Set of schema types that have at least one joinable property.
  std::unordered_set<std::string_view> joinable_types;

  // Add all first-order dependents.
  std::unordered_set<std::string_view> known_types;
  std::unordered_set<std::string_view> unknown_types;
  for (const auto& type_config : schema.types()) {
    std::string_view schema_type(type_config.schema_type());
    if (known_types.count(schema_type) > 0) {
      return absl_ports::AlreadyExistsError(absl_ports::StrCat(
          "Field 'schema_type' '", schema_type, "' is already defined"));
    }
    known_types.insert(schema_type);
    unknown_types.erase(schema_type);
    // Insert inheritance relations into the inheritance map.
    for (std::string_view parent_schema_type : type_config.parent_types()) {
      if (known_types.count(parent_schema_type) == 0) {
        unknown_types.insert(parent_schema_type);
      }
      direct_inheritance_map[parent_schema_type][schema_type] = true;
    }
    for (const auto& property_config : type_config.properties()) {
      if (property_config.joinable_config().value_type() !=
          JoinableConfig::ValueType::NONE) {
        joinable_types.insert(schema_type);
      }
      // Insert nested-type relations into the nested-type map.
      if (property_config.data_type() ==
          PropertyConfigProto::DataType::DOCUMENT) {
        // Need to know what schema_type these Document properties should be
        // validated against
        std::string_view property_schema_type(property_config.schema_type());
        if (known_types.count(property_schema_type) == 0) {
          unknown_types.insert(property_schema_type);
        }
        direct_nested_type_map[property_schema_type][schema_type].push_back(
            &property_config);
      }
    }
  }
  if (!unknown_types.empty()) {
    return absl_ports::InvalidArgumentError(absl_ports::StrCat(
        "Undefined 'schema_type's: ", absl_ports::StrJoin(unknown_types, ",")));
  }

  // Merge two expanded maps into a single dependent_map, without making
  // inheritance and nested-type relations transitive.
  ICING_ASSIGN_OR_RETURN(SchemaUtil::DependentMap merged_dependent_map,
                         CalculateTransitiveNestedTypeRelations(
                             direct_nested_type_map, joinable_types,
                             allow_circular_schema_definitions));
  ICING_ASSIGN_OR_RETURN(
      SchemaUtil::InheritanceMap expanded_inheritance_map,
      CalculateTransitiveInheritanceRelations(direct_inheritance_map));
  for (const auto& [parent_type, inheritance_relation] :
       expanded_inheritance_map) {
    // Insert the parent_type into the dependent map if it is not present
    // already.
    merged_dependent_map.insert({parent_type, {}});
    for (const auto& [child_type, _] : inheritance_relation) {
      // Insert the child_type into parent_type's dependent map if it's not
      // present already, in which case the value will be an empty vector.
      merged_dependent_map[parent_type].insert({child_type, {}});
    }
  }
  return merged_dependent_map;
}

libtextclassifier3::StatusOr<SchemaUtil::InheritanceMap>
SchemaUtil::BuildTransitiveInheritanceGraph(const SchemaProto& schema) {
  SchemaUtil::InheritanceMap direct_inheritance_map;
  for (const auto& type_config : schema.types()) {
    for (std::string_view parent_schema_type : type_config.parent_types()) {
      direct_inheritance_map[parent_schema_type][type_config.schema_type()] =
          true;
    }
  }
  return CalculateTransitiveInheritanceRelations(direct_inheritance_map);
}

libtextclassifier3::StatusOr<SchemaUtil::DependentMap> SchemaUtil::Validate(
    const SchemaProto& schema, const FeatureFlags& feature_flags) {
  // 1. Build the dependent map. This will detect any cycles, non-existent or
  // duplicate types in the schema.
  ICING_ASSIGN_OR_RETURN(
      SchemaUtil::DependentMap dependent_map,
      BuildTransitiveDependentGraph(
          schema, feature_flags.allow_circular_schema_definitions()));

  // Tracks PropertyConfigs within a SchemaTypeConfig that we've validated
  // already.
  std::unordered_set<std::string_view> known_property_names;

  // Tracks PropertyConfigs containing joinable properties.
  std::unordered_set<std::string_view> schema_types_with_joinable_property;

  // 2. Validate the properties of each type.
  for (const auto& type_config : schema.types()) {
    std::string_view schema_type(type_config.schema_type());
    ICING_RETURN_IF_ERROR(ValidateSchemaType(schema_type));

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

        ICING_RETURN_IF_ERROR(ValidateDocumentIndexingConfig(
            property_config.document_indexing_config(), schema_type,
            property_name));
      }

      ICING_RETURN_IF_ERROR(ValidateCardinality(property_config.cardinality(),
                                                schema_type, property_name));
      // The scorable properties feature has been fully rolled out.
      ICING_RETURN_IF_ERROR(ValidateScorableType(schema_type, property_config));

      if (data_type == PropertyConfigProto::DataType::STRING) {
        ICING_RETURN_IF_ERROR(ValidateStringIndexingConfig(
            property_config.string_indexing_config(), data_type, schema_type,
            property_name));
      }

      ICING_RETURN_IF_ERROR(
          ValidateJoinableConfig(property_config.joinable_config(), data_type,
                                 property_config.cardinality(), schema_type,
                                 property_name, feature_flags));
      if (property_config.joinable_config().value_type() !=
          JoinableConfig::ValueType::NONE) {
        schema_types_with_joinable_property.insert(schema_type);
      }
    }
  }

  // BFS traverse the dependent graph to make sure that no nested levels
  // (properties with DOCUMENT data type) have REPEATED cardinality while
  // depending on schema types with joinable property.
  std::queue<std::string_view> frontier;
  for (const auto& schema_type : schema_types_with_joinable_property) {
    frontier.push(schema_type);
  }
  std::unordered_set<std::string_view> traversed =
      std::move(schema_types_with_joinable_property);
  while (!frontier.empty()) {
    std::string_view schema_type = frontier.front();
    frontier.pop();

    const auto it = dependent_map.find(schema_type);
    if (it == dependent_map.end()) {
      continue;
    }

    // Check every type that has a property of type schema_type.
    for (const auto& [next_schema_type, property_configs] : it->second) {
      // Check all properties in "next_schema_type" that are of type
      // "schema_type".
      for (const PropertyConfigProto* property_config : property_configs) {
        if (property_config != nullptr &&
            property_config->cardinality() ==
                PropertyConfigProto::Cardinality::REPEATED) {
          return absl_ports::InvalidArgumentError(absl_ports::StrCat(
              "Schema type '", next_schema_type,
              "' cannot have REPEATED nested document property '",
              property_config->property_name(),
              "' while connecting to some joinable properties"));
        }
      }

      if (traversed.count(next_schema_type) == 0) {
        traversed.insert(next_schema_type);
        frontier.push(next_schema_type);
      }
    }
  }

  // Verify that every child type's property set has included all compatible
  // properties from parent types.
  ICING_RETURN_IF_ERROR(ValidateInheritedProperties(schema));

  if (feature_flags.enable_account_property_incompatibility_check()) {
    ICING_RETURN_IF_ERROR(ValidateAllAccountProperties(schema));
  }
  return dependent_map;
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

libtextclassifier3::Status SchemaUtil::ValidateScorableType(
    std::string_view schema_type,
    const PropertyConfigProto& property_config_proto) {
  if (property_config_proto.data_type() ==
      PropertyConfigProto::DataType::DOCUMENT) {
    if (property_config_proto.scorable_type() !=
        PropertyConfigProto::ScorableType::UNKNOWN) {
      return absl_ports::InvalidArgumentError(absl_ports::StrCat(
          "Field 'scorable_type' shouldn't be explicitly set for data type "
          "DOCUMENT. It is considered scorable if any of its or its "
          "dependency's property is scorable."));
    }
  }

  if (property_config_proto.scorable_type() ==
          PropertyConfigProto::ScorableType::DISABLED ||
      property_config_proto.scorable_type() ==
          PropertyConfigProto::ScorableType::UNKNOWN) {
    return libtextclassifier3::Status::OK;
  }

  switch (property_config_proto.data_type()) {
    case PropertyConfigProto::DataType::INT64:
    case PropertyConfigProto::DataType::DOUBLE:
    case PropertyConfigProto::DataType::BOOLEAN:
      return libtextclassifier3::Status::OK;
    default:
      return absl_ports::InvalidArgumentError(absl_ports::StrCat(
          "Field 'scorable_type' cannot be enabled for data type '",
          PropertyConfigProto::DataType::Code_Name(
              property_config_proto.data_type()),
          "' for schema property '", schema_type, ".",
          property_config_proto.property_name(), "'"));
  }
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

libtextclassifier3::Status SchemaUtil::ValidateJoinableConfig(
    const JoinableConfig& config, PropertyConfigProto::DataType::Code data_type,
    PropertyConfigProto::Cardinality::Code cardinality,
    std::string_view schema_type, std::string_view property_name,
    const FeatureFlags& feature_flags) {
  if (config.value_type() == JoinableConfig::ValueType::QUALIFIED_ID) {
    if (data_type != PropertyConfigProto::DataType::STRING) {
      return absl_ports::InvalidArgumentError(
          absl_ports::StrCat("Qualified id joinable property '", property_name,
                             "' is required to have STRING data type"));
    }

    if (!feature_flags.enable_repeated_field_joins() &&
        cardinality == PropertyConfigProto::Cardinality::REPEATED) {
      return absl_ports::InvalidArgumentError(
          absl_ports::StrCat("Qualified id joinable property '", property_name,
                             "' cannot have REPEATED cardinality"));
    }
  }

  if (config.delete_propagation_type() !=
          JoinableConfig::DeletePropagationType::NONE &&
      config.value_type() != JoinableConfig::ValueType::QUALIFIED_ID) {
    return absl_ports::InvalidArgumentError(
        absl_ports::StrCat("Field 'property_name' '", property_name,
                           "' is required to have QUALIFIED_ID joinable "
                           "value type with delete propagation enabled"));
  }

  return libtextclassifier3::Status::OK;
}

libtextclassifier3::Status SchemaUtil::ValidateDocumentIndexingConfig(
    const DocumentIndexingConfig& config, std::string_view schema_type,
    std::string_view property_name) {
  if (!config.indexable_nested_properties_list().empty() &&
      config.index_nested_properties()) {
    return absl_ports::InvalidArgumentError(absl_ports::StrCat(
        "DocumentIndexingConfig.index_nested_properties is required to be "
        "false when providing a non-empty indexable_nested_properties_list "
        "for property '",
        schema_type, ".", property_name, "'"));
  }
  return libtextclassifier3::Status::OK;
}

/* static */ bool SchemaUtil::IsIndexedProperty(
    const PropertyConfigProto& property_config) {
  switch (property_config.data_type()) {
    case PropertyConfigProto::DataType::STRING:
      return property_config.string_indexing_config().term_match_type() !=
                 TermMatchType::UNKNOWN &&
             property_config.string_indexing_config().tokenizer_type() !=
                 StringIndexingConfig::TokenizerType::NONE;
    case PropertyConfigProto::DataType::INT64:
      return property_config.integer_indexing_config().numeric_match_type() !=
             IntegerIndexingConfig::NumericMatchType::UNKNOWN;
    case PropertyConfigProto::DataType::DOCUMENT:
      // A document property is considered indexed if it has
      // index_nested_properties=true, or a non-empty
      // indexable_nested_properties_list.
      return property_config.document_indexing_config()
                 .index_nested_properties() ||
             !property_config.document_indexing_config()
                  .indexable_nested_properties_list()
                  .empty();
    case PropertyConfigProto::DataType::VECTOR:
      return property_config.embedding_indexing_config()
                 .embedding_indexing_type() !=
             EmbeddingIndexingConfig::EmbeddingIndexingType::UNKNOWN;
    case PropertyConfigProto::DataType::UNKNOWN:
    case PropertyConfigProto::DataType::DOUBLE:
    case PropertyConfigProto::DataType::BOOLEAN:
    case PropertyConfigProto::DataType::BYTES:
    case PropertyConfigProto::DataType::BLOB_HANDLE:
      return false;
  }
}

bool SchemaUtil::IsParent(const SchemaUtil::InheritanceMap& inheritance_map,
                          std::string_view parent_type,
                          std::string_view child_type) {
  auto iter = inheritance_map.find(parent_type);
  if (iter == inheritance_map.end()) {
    return false;
  }
  return iter->second.count(child_type) > 0;
}

bool SchemaUtil::IsInheritedPropertyCompatible(
    const SchemaUtil::InheritanceMap& inheritance_map,
    const PropertyConfigProto& child_property_config,
    const PropertyConfigProto& parent_property_config) {
  // Check if child_property_config->cardinality() <=
  // parent_property_config->cardinality().
  // Subtype may require a stricter cardinality, but cannot loosen cardinality
  // requirements.
  if (!CardinalityLessThanEq(child_property_config.cardinality(),
                             parent_property_config.cardinality())) {
    return false;
  }

  // Now we can assume T1 and T2 are not nullptr, and cardinality check passes.
  if (child_property_config.data_type() !=
          PropertyConfigProto::DataType::DOCUMENT ||
      parent_property_config.data_type() !=
          PropertyConfigProto::DataType::DOCUMENT) {
    return child_property_config.data_type() ==
           parent_property_config.data_type();
  }

  // Now we can assume T1 and T2 are both document type.
  return child_property_config.schema_type() ==
             parent_property_config.schema_type() ||
         IsParent(inheritance_map, parent_property_config.schema_type(),
                  child_property_config.schema_type());
}

libtextclassifier3::Status SchemaUtil::ValidateInheritedProperties(
    const SchemaProto& schema) {
  // Create a inheritance map
  ICING_ASSIGN_OR_RETURN(SchemaUtil::InheritanceMap inheritance_map,
                         BuildTransitiveInheritanceGraph(schema));

  // Create a map that maps from type name to property names, and then from
  // property names to PropertyConfigProto.
  std::unordered_map<
      std::string, std::unordered_map<std::string, const PropertyConfigProto*>>
      property_map;
  for (const SchemaTypeConfigProto& type_config : schema.types()) {
    // Skipping building entries for types without any child or parent, since
    // such entry will never be used.
    if (type_config.parent_types().empty() &&
        inheritance_map.count(type_config.schema_type()) == 0) {
      continue;
    }
    auto& curr_property_map = property_map[type_config.schema_type()];
    for (const PropertyConfigProto& property_config :
         type_config.properties()) {
      curr_property_map[property_config.property_name()] = &property_config;
    }
  }

  // Validate child properties.
  for (const SchemaTypeConfigProto& type_config : schema.types()) {
    const std::string& child_type_name = type_config.schema_type();
    auto& child_property_map = property_map[child_type_name];

    for (const std::string& parent_type_name : type_config.parent_types()) {
      auto& parent_property_map = property_map[parent_type_name];

      for (const auto& [property_name, parent_property_config] :
           parent_property_map) {
        auto child_property_iter = child_property_map.find(property_name);
        if (child_property_iter == child_property_map.end()) {
          return absl_ports::InvalidArgumentError(absl_ports::StrCat(
              "Property ", property_name, " is not present in child type ",
              child_type_name, ", but it is defined in the parent type ",
              parent_type_name, "."));
        }
        if (!IsInheritedPropertyCompatible(inheritance_map,
                                           *child_property_iter->second,
                                           *parent_property_config)) {
          return absl_ports::InvalidArgumentError(absl_ports::StrCat(
              "Property ", property_name, " from child type ", child_type_name,
              " is not compatible to the parent type ", parent_type_name, "."));
        }
      }
    }
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
libtextclassifier3::Status SchemaUtil::BuildTypeConfigInfoCache(
    const SchemaProto& schema,
    SchemaUtil::TypeConfigInfoCache* type_config_info_cache) {
  type_config_info_cache->Clear();
  for (const SchemaTypeConfigProto& type_config : schema.types()) {
    ICING_RETURN_IF_ERROR(type_config_info_cache->AddTypeConfig(type_config));
  }
  return libtextclassifier3::Status::OK;
}

SchemaUtil::ParsedPropertyConfigs SchemaUtil::ParsePropertyConfigs(
    const google::protobuf::RepeatedPtrField<PropertyConfigProto>& properties) {
  ParsedPropertyConfigs parsed_property_configs;

  // TODO(cassiewang): consider caching property_config_map for some properties,
  // e.g. using LRU cache. Or changing schema.proto to use go/protomap.
  for (int position = 0; position < properties.size(); ++position) {
    const PropertyConfigProto& property_config = properties.Get(position);
    std::string_view property_name = property_config.property_name();
    parsed_property_configs.property_config_map.emplace(
        property_name, PropertyConfigInfo{&property_config, position});
    if (property_config.cardinality() ==
        PropertyConfigProto::Cardinality::REQUIRED) {
      parsed_property_configs.required_properties.insert(property_name);
    }

    // A non-default term_match_type indicates that this property is meant to be
    // indexed.
    if (IsIndexedProperty(property_config)) {
      parsed_property_configs.indexed_properties.insert(property_name);
    }

    // A non-default value_type indicates that this property is meant to be
    // joinable.
    if (property_config.joinable_config().value_type() !=
        JoinableConfig::ValueType::NONE) {
      parsed_property_configs.joinable_properties.insert(property_name);
    }

    // Also keep track of how many nested document properties there are. Adding
    // new nested document properties will result in join-index rebuild.
    if (property_config.data_type() ==
        PropertyConfigProto::DataType::DOCUMENT) {
      parsed_property_configs.nested_document_properties.insert(property_name);
    }
  }

  return parsed_property_configs;
}

SchemaUtil::SchemaDelta SchemaUtil::ComputeCompatibilityDelta(
    const SchemaProto& old_schema, const SchemaProto& new_schema,
    const DependentMap& new_schema_dependent_map,
    const FeatureFlags& feature_flags) {
  SchemaDelta schema_delta;

  TypeConfigMap old_type_config_map, new_type_config_map;
  BuildTypeConfigMap(old_schema, &old_type_config_map);
  BuildTypeConfigMap(new_schema, &new_type_config_map);

  // The scorable properties feature has been fully rolled out.
  FindScorablePropertyInconsistentTypes(
      old_type_config_map, new_type_config_map, new_schema_dependent_map,
      &schema_delta);

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
        ParsePropertyConfigs(new_schema_type_and_config->second.properties());

    // We only need to check the old, existing properties to see if they're
    // compatible since we'll have old data that may be invalidated or need to
    // be reindexed.
    std::unordered_set<std::string_view> old_required_properties;
    std::unordered_set<std::string_view> old_indexed_properties;
    std::unordered_set<std::string_view> old_joinable_properties;
    std::unordered_set<std::string_view> old_nested_document_properties;

    // If there is a different number of properties, then there must have been a
    // change.
    bool has_property_changed =
        old_type_config.properties_size() !=
        new_schema_type_and_config->second.properties_size();
    bool is_incompatible = false;
    bool is_index_incompatible = false;
    bool is_join_incompatible = false;
    for (int position = 0; position < old_type_config.properties_size();
         ++position) {
      const PropertyConfigProto& old_property_config =
          old_type_config.properties(position);
      std::string_view property_name = old_property_config.property_name();
      if (old_property_config.cardinality() ==
          PropertyConfigProto::Cardinality::REQUIRED) {
        old_required_properties.insert(property_name);
      }

      // A non-default term_match_type indicates that this property is meant to
      // be indexed.
      bool is_indexed_property = IsIndexedProperty(old_property_config);
      if (is_indexed_property) {
        old_indexed_properties.insert(property_name);
      }

      bool is_joinable_property =
          old_property_config.joinable_config().value_type() !=
          JoinableConfig::ValueType::NONE;
      if (is_joinable_property) {
        old_joinable_properties.insert(property_name);
      }

      // A nested-document property is a property of DataType::DOCUMENT.
      bool is_nested_document_property =
          old_property_config.data_type() ==
          PropertyConfigProto::DataType::DOCUMENT;
      if (is_nested_document_property) {
        old_nested_document_properties.insert(property_name);
      }

      auto new_property_name_and_config =
          new_parsed_property_configs.property_config_map.find(
              old_property_config.property_name());

      if (new_property_name_and_config ==
          new_parsed_property_configs.property_config_map.end()) {
        // Didn't find the old property
        ICING_LOG(INFO) << absl_ports::StrCat(
            "Previously defined property type '", old_type_config.schema_type(),
            ".", old_property_config.property_name(),
            "' was not defined in new schema");
        is_incompatible = true;
        is_index_incompatible |= is_indexed_property;
        is_join_incompatible |=
            is_joinable_property || is_nested_document_property;
        continue;
      }

      const PropertyConfigProto* new_property_config =
          new_property_name_and_config->second.property_config;
      bool property_order_changed =
          feature_flags.enable_schema_definition_deduping() &&
          position != new_property_name_and_config->second.position;
      if (!has_property_changed &&
          (!ArePropertiesEqual(old_property_config, *new_property_config) ||
           property_order_changed)) {
        // Found a property that changed. A property change is either a
        // PropertyConfigProto change or (when schema deduping is enabled) a
        // change in the property's position in the type config's repeated
        // properties field.
        has_property_changed = true;
      }

      if (!IsPropertyCompatible(old_property_config, *new_property_config)) {
        ICING_LOG(INFO) << absl_ports::StrCat(
            "Property '", old_type_config.schema_type(), ".",
            old_property_config.property_name(), "' is incompatible.");
        is_incompatible = true;
      }

      // Any change in the indexed property requires a reindexing
      if (!AreStringIndexingConfigsEqual(
              old_property_config.string_indexing_config(),
              new_property_config->string_indexing_config()) ||
          !AreIntegerIndexingConfigsEqual(
              old_property_config.integer_indexing_config(),
              new_property_config->integer_indexing_config()) ||
          !AreDocumentIndexingConfigsEqual(
              old_property_config.document_indexing_config(),
              new_property_config->document_indexing_config()) ||
          !AreEmbeddingIndexingConfigsEqual(
              old_property_config.embedding_indexing_config(),
              new_property_config->embedding_indexing_config())) {
        is_index_incompatible = true;
      }

      if (!AreJoinableConfigsEqual(old_property_config.joinable_config(),
                                   new_property_config->joinable_config())) {
        is_join_incompatible = true;
      }
    }

    // We can't have new properties that are REQUIRED since we won't know how
    // to backfill the data, and the existing data will be invalid. We're
    // guaranteed from our previous checks that all the old properties are also
    // present in the new property config, so we can do a simple int comparison
    // here to detect new required properties.
    if (!IsSubset(new_parsed_property_configs.required_properties,
                  old_required_properties)) {
      ICING_LOG(INFO) << absl_ports::StrCat(
          "New schema '", old_type_config.schema_type(),
          "' has REQUIRED properties that are not "
          "present in the previously defined schema");
      is_incompatible = true;
    }

    // If we've gained any new indexed properties (this includes gaining new
    // indexed nested document properties), then the section ids may change.
    // Since the section ids are stored in the index, we'll need to
    // reindex everything.
    if (!IsSubset(new_parsed_property_configs.indexed_properties,
                  old_indexed_properties)) {
      ICING_LOG(INFO) << "Set of indexed properties in schema type '"
                      << old_type_config.schema_type()
                      << "' has changed, required reindexing.";
      is_index_incompatible = true;
    }

    // If we've gained any new joinable properties, then the joinable property
    // ids may change. Since the joinable property ids are stored in the cache,
    // we'll need to reconstruct join index.
    // If we've gained any new nested document properties, we also rebuild the
    // join index. This is because we index all nested joinable properties, so
    // adding a nested document property will most probably result in having
    // more joinable properties.
    if (!IsSubset(new_parsed_property_configs.joinable_properties,
                  old_joinable_properties) ||
        !IsSubset(new_parsed_property_configs.nested_document_properties,
                  old_nested_document_properties)) {
      ICING_LOG(INFO)
          << "Set of joinable properties in schema type '"
          << old_type_config.schema_type()
          << "' has changed, required reconstructing joinable cache.";
      is_join_incompatible = true;
    }

    if (feature_flags.enable_account_property_incompatibility_check() &&
        IsAccountPropertyIncompatible(old_type_config,
                                      new_schema_type_and_config->second,
                                      old_type_config_map)) {
      is_incompatible = true;
    }

    if (is_incompatible) {
      schema_delta.schema_types_incompatible.insert(
          old_type_config.schema_type());
    }

    if (is_index_incompatible) {
      schema_delta.schema_types_index_incompatible.insert(
          old_type_config.schema_type());
    }

    if (is_join_incompatible) {
      schema_delta.schema_types_join_incompatible.insert(
          old_type_config.schema_type());
    }

    // Scorable-property inconsistent types are already added to the schema
    // delta in FindScorablePropertyInconsistentTypes above.
    bool is_scorable_property_cache_incompatible =
        !schema_delta.schema_types_scorable_property_inconsistent.empty() &&
        schema_delta.schema_types_scorable_property_inconsistent.find(
            old_type_config.schema_type()) !=
            schema_delta.schema_types_scorable_property_inconsistent.end();

    if (!is_incompatible && !is_index_incompatible && !is_join_incompatible &&
        !is_scorable_property_cache_incompatible && has_property_changed) {
      schema_delta.schema_types_changed_fully_compatible.insert(
          old_type_config.schema_type());
    }

    // Lastly, remove this type from the map. We know that this type can't
    // come up in future iterations through the old schema types because the old
    // type config has unique types.
    new_type_config_map.erase(old_type_config.schema_type());
  }

  // Now that all directly-incompatible types have been collected, propagate
  // each kind of incompatibility through the schema dependency graph in a
  // single BFS pass per delta set (instead of one BFS per seed type).
  for (std::unordered_set<std::string>* delta_set : {
           &schema_delta.schema_types_incompatible,
           &schema_delta.schema_types_index_incompatible,
           &schema_delta.schema_types_join_incompatible,
       }) {
    PropagateIncompatibleChangeToDelta(*delta_set, new_schema_dependent_map,
                                       old_type_config_map);
  }

  // Any types that are still present in the new_type_config_map are newly added
  // types.
  schema_delta.schema_types_new.reserve(new_type_config_map.size());
  for (auto& kvp : new_type_config_map) {
    schema_delta.schema_types_new.insert(std::move(kvp.first));
  }

  return schema_delta;
}

Sha256Digest SchemaUtil::ComputeSchemaPropertiesSha256Digest(
    const SchemaTypeConfigProto& type_config) {
  SchemaTypeConfigProto properties_only_type_config;
  *properties_only_type_config.mutable_properties() = type_config.properties();
  std::string serialized_properties =
      properties_only_type_config.SerializeAsString();
  const uint8_t* properties_data =
      reinterpret_cast<const uint8_t*>(serialized_properties.data());

  Sha256 sha256;
  sha256.Update(properties_data, serialized_properties.size());
  return std::move(sha256).Finalize();
}

Sha256Digest SchemaUtil::PopulatePropertiesDigestField(
    SchemaTypeConfigProto& type_config) {
  Sha256Digest properties_sha256_digest =
      ComputeSchemaPropertiesSha256Digest(type_config);
  type_config.set_properties_digest(
      reinterpret_cast<const char*>(properties_sha256_digest.data()),
      properties_sha256_digest.size());

  return properties_sha256_digest;
}

std::optional<Sha256Digest> SchemaUtil::GetSchemaPropertiesDigest(
    const SchemaTypeConfigProto& type_config) {
  const std::string& digest_bytes = type_config.properties_digest();

  if (digest_bytes.size() == kSha256DigestBytes) {
    Sha256Digest properties_sha256_digest;
    std::memcpy(properties_sha256_digest.data(), digest_bytes.data(),
                kSha256DigestBytes);
    return properties_sha256_digest;
  }
  return std::nullopt;
}

}  // namespace lib
}  // namespace icing
