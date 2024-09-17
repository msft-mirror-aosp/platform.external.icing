// Copyright (C) 2024 Google LLC
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

#include "icing/schema/scorable_property_manager.h"

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/schema/schema-util.h"
#include "icing/store/document-filter-data.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<int>
ScorablePropertyManager::GetScorablePropertyIndex(
    SchemaTypeId schema_type_id, const std::string& property_name,
    const SchemaUtil::TypeConfigMap& type_config_map,
    const std::unordered_map<SchemaTypeId, std::string>&
        schema_id_to_type_map) {
  ICING_ASSIGN_OR_RETURN(auto cache_iter, LookupAndMaybeUpdateCache(
                                              schema_type_id, type_config_map,
                                              schema_id_to_type_map));
  auto iter = cache_iter->second.property_name_to_index_map.find(property_name);
  if (iter == cache_iter->second.property_name_to_index_map.end()) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "Scorable property %s, not found under schema type id %d",
        property_name.c_str(), schema_type_id));
  }
  return iter->second;
}

libtextclassifier3::StatusOr<const std::vector<std::string>*>
ScorablePropertyManager::GetOrderedScorablePropertyNames(
    SchemaTypeId schema_type_id,
    const SchemaUtil::TypeConfigMap& type_config_map,
    const std::unordered_map<SchemaTypeId, std::string>&
        schema_id_to_type_map) {
  ICING_ASSIGN_OR_RETURN(auto cache_iter, LookupAndMaybeUpdateCache(
                                              schema_type_id, type_config_map,
                                              schema_id_to_type_map));
  return &cache_iter->second.ordered_scorable_property_names;
}

libtextclassifier3::StatusOr<std::unordered_map<
    SchemaTypeId,
    ScorablePropertyManager::DerivedScorablePropertySchema>::iterator>
ScorablePropertyManager::LookupAndMaybeUpdateCache(
    SchemaTypeId schema_type_id,
    const SchemaUtil::TypeConfigMap& type_config_map,
    const std::unordered_map<SchemaTypeId, std::string>&
        schema_id_to_type_map) {
  auto cache_iter = scorable_property_schema_cache_.find(schema_type_id);
  if (cache_iter == scorable_property_schema_cache_.end()) {
    if (UpdateCache(schema_type_id, type_config_map, schema_id_to_type_map)) {
      cache_iter = scorable_property_schema_cache_.find(schema_type_id);
    } else {
      // schema type id not found neither in the cache nor in the schema
      // config.
      return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
          "LookupAndMaybeUpdateCache failed: Schema type id %d not found",
          schema_type_id));
    }
  }
  return cache_iter;
}

bool ScorablePropertyManager::UpdateCache(
    SchemaTypeId schema_type_id,
    const SchemaUtil::TypeConfigMap& type_config_map,
    const std::unordered_map<SchemaTypeId, std::string>&
        schema_id_to_type_map) {
  auto schema_id_iter = schema_id_to_type_map.find(schema_type_id);
  if (schema_id_iter == schema_id_to_type_map.end()) {
    return false;
  }
  auto schema_config_iter = type_config_map.find(schema_id_iter->second);
  if (schema_config_iter == type_config_map.end()) {
    return false;
  }
  std::vector<std::string> scorable_property_names;
  std::unordered_map<std::string, int> index_map;
  for (const PropertyConfigProto& property_config :
       schema_config_iter->second.properties()) {
    if (property_config.scorable_type() ==
        PropertyConfigProto::ScorableType::ENABLED) {
      index_map[property_config.property_name()] =
          scorable_property_names.size();
      scorable_property_names.push_back(property_config.property_name());
    }
  }
  scorable_property_schema_cache_.insert(
      {schema_type_id,
       DerivedScorablePropertySchema(std::move(scorable_property_names),
                                     std::move(index_map))});
  return true;
}

}  // namespace lib
}  // namespace icing
