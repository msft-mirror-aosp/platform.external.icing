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

#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/schema/schema-property-iterator.h"
#include "icing/schema/schema-util.h"
#include "icing/store/document-filter-data.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<std::optional<int>>
ScorablePropertyManager::GetScorablePropertyIndex(
    SchemaTypeId schema_type_id, std::string_view property_path,
    const SchemaUtil::TypeConfigMap& type_config_map,
    const std::unordered_map<SchemaTypeId, std::string>&
        schema_id_to_type_map) {
  ICING_ASSIGN_OR_RETURN(auto cache_iter, LookupAndMaybeUpdateCache(
                                              schema_type_id, type_config_map,
                                              schema_id_to_type_map));
  auto iter =
      cache_iter->second.property_path_to_index_map.find(property_path.data());
  if (iter == cache_iter->second.property_path_to_index_map.end()) {
    return std::nullopt;
  }
  return iter->second;
}

libtextclassifier3::StatusOr<
    const std::vector<ScorablePropertyManager::ScorablePropertyInfo>*>
ScorablePropertyManager::GetOrderedScorablePropertyInfo(
    SchemaTypeId schema_type_id,
    const SchemaUtil::TypeConfigMap& type_config_map,
    const std::unordered_map<SchemaTypeId, std::string>&
        schema_id_to_type_map) {
  ICING_ASSIGN_OR_RETURN(auto cache_iter, LookupAndMaybeUpdateCache(
                                              schema_type_id, type_config_map,
                                              schema_id_to_type_map));
  return &cache_iter->second.ordered_scorable_property_info;
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
  std::vector<ScorablePropertyInfo> property_info_vector;
  std::unordered_map<std::string, int> index_map;

  SchemaPropertyIterator iterator(schema_config_iter->second, type_config_map);
  while (true) {
    libtextclassifier3::Status status = iterator.Advance();
    if (!status.ok()) {
      if (absl_ports::IsOutOfRange(status)) {
        break;
      }
      // Swallow other type of errors which should not happen if the schema has
      // been validated already.
      return false;
    }
    if (iterator.GetCurrentPropertyConfig().scorable_type() ==
        PropertyConfigProto::ScorableType::ENABLED) {
      index_map[iterator.GetCurrentPropertyPath()] =
          property_info_vector.size();
      ScorablePropertyInfo scorable_property_info;
      scorable_property_info.property_path = iterator.GetCurrentPropertyPath();
      scorable_property_info.data_type =
          iterator.GetCurrentPropertyConfig().data_type();
      property_info_vector.push_back(std::move(scorable_property_info));
    }
  }
  scorable_property_schema_cache_.insert(
      {schema_type_id,
       DerivedScorablePropertySchema(std::move(property_info_vector),
                                     std::move(index_map))});
  return true;
}

}  // namespace lib
}  // namespace icing
