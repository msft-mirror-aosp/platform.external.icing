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

#include <string>
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
  ScorablePropertyManager() = default;

  // Delete copy constructor and assignment operator.
  ScorablePropertyManager(const ScorablePropertyManager&) = delete;
  ScorablePropertyManager& operator=(const ScorablePropertyManager&) = delete;

  // Gets the index of the scorable property with name |property_name|, where
  // the index N means that the property is the Nth scorable property in the
  // schema config of the given |schema_type_id|.
  //
  // Returns:
  //   - INVALID_ARGUMENT if |schema_type_id| is invalid, or |property_name| is
  //     not a scorable property under the schema config of |schema_type_id|.
  libtextclassifier3::StatusOr<int> GetScorablePropertyIndex(
      SchemaTypeId schema_type_id, const std::string& property_name,
      const SchemaUtil::TypeConfigMap& type_config_map,
      const std::unordered_map<SchemaTypeId, std::string>&
          schema_id_to_type_map);

  // Returns the ordered list of scorable property names for the given
  // |schema_type_id|.
  //
  // Returns:
  //   - Vector of scorable property names on success. The vector can be empty
  //     if no scorable property is found under the schema config of
  //     |schema_type_id|.
  //   - INVALID_ARGUMENT if |schema_type_id| is invalid.
  libtextclassifier3::StatusOr<const std::vector<std::string>*>
  GetOrderedScorablePropertyNames(
      SchemaTypeId schema_type_id,
      const SchemaUtil::TypeConfigMap& type_config_map,
      const std::unordered_map<SchemaTypeId, std::string>&
          schema_id_to_type_map);

 private:
  struct DerivedScorablePropertySchema {
    // Ordered list of scorable property names in a schema config.
    std::vector<std::string> ordered_scorable_property_names;
    // Map of scorable property name to its index, where index N means that the
    // property is the Nth scorable property in the schema config.
    std::unordered_map<std::string, int> property_name_to_index_map;

    explicit DerivedScorablePropertySchema(
        std::vector<std::string>&& property_names_map,
        std::unordered_map<std::string, int>&& index_map)
        : ordered_scorable_property_names(std::move(property_names_map)),
          property_name_to_index_map(std::move(index_map)) {}
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
