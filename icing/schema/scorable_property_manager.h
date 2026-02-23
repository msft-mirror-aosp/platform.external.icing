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

#ifndef ICING_SCHEMA_SCORABLE_PROPERTY_MANAGER_H_
#define ICING_SCHEMA_SCORABLE_PROPERTY_MANAGER_H_

#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/schema/schema-util.h"
#include "icing/store/document-filter-data.h"

namespace icing {
namespace lib {

// This class serves as a cache layer to access schema configs for scorable
// properties, with the schema-store being the source of truth.
class ScorablePropertyManager {
 public:
  struct ScorablePropertyInfo {
    // Scorable property's property_path in the schema config, eg:
    // "property_name", "parent_schema.nested_child_schema.property_name".
    std::string property_path;
    // Data type of the property.
    PropertyConfigProto::DataType::Code data_type;
  };

  ScorablePropertyManager() = default;

  // Delete copy constructor and assignment operator.
  ScorablePropertyManager(const ScorablePropertyManager&) = delete;
  ScorablePropertyManager& operator=(const ScorablePropertyManager&) = delete;

  // Gets the index of the given |property_path|, where the index N means that
  // it is the Nth scorable property path in the schema config of the given
  // |schema_type_id|, in lexicographical order.
  //
  // Returns:
  //   - Index on success
  //   - std::nullopt if the |property_path| doesnn't point to a scorable
  //     property under the |schema_type_id|
  //   - INVALID_ARGUMENT if |schema_type_id| is invalid
  libtextclassifier3::StatusOr<std::optional<int>> GetScorablePropertyIndex(
      SchemaTypeId schema_type_id, std::string_view property_path,
      const SchemaUtil::TypeConfigMap& type_config_map,
      const std::unordered_map<SchemaTypeId, std::string>&
          schema_id_to_type_map);

  // Returns the list of ScorablePropertyInfo for the given |schema_type_id|,
  // in lexicographical order of its property path.
  //
  // Returns:
  //   - Vector of scorable property info on success. The vector can be empty
  //     if no scorable property is found under the schema config of
  //     |schema_type_id|.
  //   - INVALID_ARGUMENT if |schema_type_id| is invalid.
  libtextclassifier3::StatusOr<const std::vector<ScorablePropertyInfo>*>
  GetOrderedScorablePropertyInfo(
      SchemaTypeId schema_type_id,
      const SchemaUtil::TypeConfigMap& type_config_map,
      const std::unordered_map<SchemaTypeId, std::string>&
          schema_id_to_type_map);

 private:
  struct DerivedScorablePropertySchema {
    // vector of ScorablePropertyInfo, in lexicographical order of its property
    // path.
    std::vector<ScorablePropertyInfo> ordered_scorable_property_info;
    // Map of scorable property path to its index, where index N means that the
    // property is the Nth scorable property in the schema config, in
    // lexicographical order.
    std::unordered_map<std::string, int> property_path_to_index_map;

    explicit DerivedScorablePropertySchema(
        std::vector<ScorablePropertyInfo>&& ordered_properties,
        std::unordered_map<std::string, int>&& index_map)
        : ordered_scorable_property_info(std::move(ordered_properties)),
          property_path_to_index_map(std::move(index_map)) {}
  };

  // Looks up the entry in |scorable_property_schema_cache_| for the key
  // |schema_type_id|. If the entry is not found, try to update the cache and
  // return the iterator of the updated entry.
  //
  // Returns:
  //   - INVALID_ARGUMENT if |schema_type_id| is not found from the schema.
  libtextclassifier3::StatusOr<
      std::unordered_map<SchemaTypeId, DerivedScorablePropertySchema>::iterator>
  LookupAndMaybeUpdateCache(SchemaTypeId schema_type_id,
                            const SchemaUtil::TypeConfigMap& type_config_map,
                            const std::unordered_map<SchemaTypeId, std::string>&
                                schema_id_to_type_map);

  // Updates the entry in |scorable_property_schema_cache_| for the key
  // |schema_type_id|.
  //
  // Returns true if an entry is successfully inserted to the cache.
  bool UpdateCache(SchemaTypeId schema_type_id,
                   const SchemaUtil::TypeConfigMap& type_config_map,
                   const std::unordered_map<SchemaTypeId, std::string>&
                       schema_id_to_type_map);

  std::unordered_map<SchemaTypeId, DerivedScorablePropertySchema>
      scorable_property_schema_cache_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_SCHEMA_SCORABLE_PROPERTY_MANAGER_H_
