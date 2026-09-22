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

#ifndef ICING_SCHEMA_SCHEMA_PROPERTY_ITERATOR_H_
#define ICING_SCHEMA_SCHEMA_PROPERTY_ITERATOR_H_

#include <algorithm>
#include <numeric>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema/property-util.h"
#include "icing/schema/schema-util.h"

namespace icing {
namespace lib {

// SchemaPropertyIterator: a class for iterating through all properties of a
// given SchemaTypeConfigProto in lexicographical order. Only leaf
// (non-document-type) properties will be returned, and for document type
// properties, the iterator will traverse down to the next nested level of
// schema.
//
// REQUIRED:
//   - The schema in which this SchemaTypeConfigProto is defined must have
//     already passed the validation step during SetSchema.
class SchemaPropertyIterator {
 public:
  explicit SchemaPropertyIterator(
      const SchemaUtil::TypeConfigInfoCache::TypeConfigHolder& base_type_holder,
      const SchemaUtil::TypeConfigInfoCache& type_config_info_cache)
      : type_config_info_cache_(type_config_info_cache) {
    levels_.push_back(LevelInfo(base_type_holder,
                                /*base_property_path=*/"",
                                /*all_nested_properties_indexable=*/true));
    parent_type_config_names_.insert(
        base_type_holder.base_type_config().schema_type());
  }

  // Gets the current property config.
  //
  // REQUIRES: The preceding call for Advance() is OK.
  const PropertyConfigProto& GetCurrentPropertyConfig() const {
    return levels_.back().GetCurrentPropertyConfig();
  }

  // Gets the current property path.
  //
  // REQUIRES: The preceding call for Advance() is OK.
  std::string GetCurrentPropertyPath() const {
    return levels_.back().GetCurrentPropertyPath();
  }

  // Returns whether the current property is indexable. This would be true if
  // either the current level is nested indexable, or if the current property is
  // declared indexable in the indexable_nested_properties_list of the top-level
  // schema type.
  //
  // REQUIRES: The preceding call for Advance() is OK.
  bool GetCurrentPropertyIndexable() const {
    return levels_.back().GetCurrentPropertyIndexable();
  }

  // Returns whether the current schema level is nested indexable. If this is
  // true, all properties in the level are indexed.
  //
  // REQUIRES: The preceding call for Advance() is OK.
  bool GetLevelNestedIndexable() const {
    return levels_.back().GetLevelNestedIndexable();
  }

  // The set of indexable nested properties that are defined in the
  // indexable_nested_properties_list but are not found in the schema
  // definition. These properties still consume sectionIds, but will not be
  // indexed.
  const std::vector<std::string>& unknown_indexable_nested_property_paths()
      const {
    return unknown_indexable_nested_property_paths_;
  }

  // Advances to the next leaf property.
  //
  // Returns:
  //   - OK on success
  //   - OUT_OF_RANGE_ERROR if there is no more leaf property
  //   - INVALID_ARGUMENT_ERROR if cycle dependency is detected in the nested
  //     schema
  //   - NOT_FOUND_ERROR if any nested schema name is not found in
  //     type_config_info_cache
  libtextclassifier3::Status Advance();

 private:
  // An inner class for maintaining the iterating state of a (nested) level.
  // Nested SchemaTypeConfig is a tree structure, so we have to traverse it
  // recursively to all leaf properties.
  class LevelInfo {
   public:
    explicit LevelInfo(
        SchemaUtil::TypeConfigInfoCache::TypeConfigHolder type_config_holder,
        std::string base_property_path, bool all_nested_properties_indexable)
        : schema_type_config_holder_(std::move(type_config_holder)),
          base_property_path_(std::move(base_property_path)),
          sorted_property_indices_(
              schema_type_config_holder_.properties().size()),
          current_vec_idx_(-1),
          sorted_property_indexable_(
              schema_type_config_holder_.properties().size()),
          all_nested_properties_indexable_(all_nested_properties_indexable) {
      // Index sort property by lexicographical order.
      std::iota(sorted_property_indices_.begin(),
                sorted_property_indices_.end(),
                /*value=*/0);
      const google::protobuf::RepeatedPtrField<PropertyConfigProto>& properties =
          schema_type_config_holder_.properties();
      std::sort(sorted_property_indices_.begin(),
                sorted_property_indices_.end(),
                [&properties](int lhs_idx, int rhs_idx) -> bool {
                  return properties.Get(lhs_idx).property_name() <
                         properties.Get(rhs_idx).property_name();
                });
    }

    bool Advance() {
      return ++current_vec_idx_ < sorted_property_indices_.size();
    }

    const PropertyConfigProto& GetCurrentPropertyConfig() const {
      return schema_type_config_holder_.properties().Get(
          sorted_property_indices_[current_vec_idx_]);
    }

    std::string GetCurrentPropertyPath() const {
      return property_util::ConcatenatePropertyPathExpr(
          base_property_path_, GetCurrentPropertyConfig().property_name());
    }

    bool GetLevelNestedIndexable() const {
      return all_nested_properties_indexable_;
    }

    bool GetCurrentPropertyIndexable() const {
      return sorted_property_indexable_[current_vec_idx_];
    }

    void SetCurrentPropertyIndexable(bool indexable) {
      sorted_property_indexable_[current_vec_idx_] = indexable;
    }

    std::string_view GetSchemaTypeName() const {
      return schema_type_config_holder_.base_type_config().schema_type();
    }

   private:
    SchemaUtil::TypeConfigInfoCache::TypeConfigHolder
        schema_type_config_holder_;

    // Concatenated property path of all parent levels.
    std::string base_property_path_;

    // We perform index sort (comparing property name) in order to iterate all
    // leaf properties in lexicographical order. This vector is for storing
    // these sorted indices.
    std::vector<int> sorted_property_indices_;
    int current_vec_idx_;

    // Vector indicating whether each property in the current level is
    // indexable. We can declare different indexable settings for properties in
    // the same level using indexable_nested_properties_list.
    //
    // Element indices in this vector correspond to property indices in the
    // sorted order.
    std::vector<bool> sorted_property_indexable_;

    // Indicates if all properties in the current level is nested indexable.
    // This would be true for a level if the document declares
    // index_nested_properties=true. If any of parent document type
    // property sets its flag false, then this would be false for all its child
    // properties.
    bool all_nested_properties_indexable_;
  };

  const SchemaUtil::TypeConfigInfoCache&
      type_config_info_cache_;  // Does not own

  // For maintaining the stack of recursive nested schema type traversal. We use
  // std::vector instead of std::stack to avoid memory allocate and free too
  // frequently.
  std::vector<LevelInfo> levels_;

  // Maintaining all traversed parent schema type config names of the current
  // stack (levels_). It is used to detect nested schema cycle dependency.
  std::unordered_multiset<std::string_view> parent_type_config_names_;

  // Sorted list of indexable nested properties for the top-level schema.
  std::vector<std::string> sorted_top_level_indexable_nested_properties_;

  // Current iteration index in the sorted_top_level_indexable_nested_properties
  // list.
  int current_top_level_indexable_nested_properties_idx_ = 0;

  // Vector of indexable nested properties defined in the
  // indexable_nested_properties_list, but not found in the schema definition.
  // These properties still consume sectionIds, but will not be indexed.
  // Properties are inserted into this vector in sorted order.
  //
  // TODO(b/289152024): Implement support for indexing these properties if they
  // are in the child types of polymorphic nested properties.
  std::vector<std::string> unknown_indexable_nested_property_paths_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_SCHEMA_SCHEMA_PROPERTY_ITERATOR_H_
