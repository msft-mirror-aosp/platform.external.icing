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

#include "icing/util/scorable_property_set.h"

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/proto/internal/scorable_property_set.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-filter-data.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<std::unique_ptr<ScorablePropertySet>>
ScorablePropertySet::Create(
    ScorablePropertySetProto&& scorable_property_set_proto,
    SchemaTypeId schema_type_id, const SchemaStore* schema_store) {
  ICING_ASSIGN_OR_RETURN(
      const std::vector<std::string>* ordered_scorable_property_names,
      schema_store->GetOrderedScorablePropertyNames(schema_type_id));
  if (ordered_scorable_property_names == nullptr ||
      ordered_scorable_property_names->size() !=
          scorable_property_set_proto.properties_size()) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "ScorablePropertySetProto data is inconsistent with the schema config "
        "of type id %d",
        schema_type_id));
  }
  return std::unique_ptr<ScorablePropertySet>(new ScorablePropertySet(
      std::move(scorable_property_set_proto), schema_type_id, schema_store));
}

libtextclassifier3::StatusOr<std::unique_ptr<ScorablePropertySet>>
ScorablePropertySet::Create(const DocumentProto& document,
                            SchemaTypeId schema_type_id,
                            const SchemaStore* schema_store) {
  ICING_ASSIGN_OR_RETURN(
      const std::vector<std::string>* ordered_scorable_property_names,
      schema_store->GetOrderedScorablePropertyNames(schema_type_id));
  if (ordered_scorable_property_names == nullptr) {
    // It should never happen
    return absl_ports::InternalError(
        "SchemaStore::GetOrderedScorablePropertyNames returned nullptr");
  }
  if (ordered_scorable_property_names->empty()) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "No scorable property defined under the config of type id %d",
        schema_type_id));
  }

  std::unordered_map<std::string_view, int> document_property_name_to_index_map;
  for (int i = 0; i < document.properties_size(); ++i) {
    const PropertyProto& doc_property = document.properties(i);
    document_property_name_to_index_map[doc_property.name()] = i;
  }

  ScorablePropertySetProto scorable_property_set_proto_to_build;
  for (const std::string& property_name : *ordered_scorable_property_names) {
    ScorablePropertyProto* new_property =
        scorable_property_set_proto_to_build.add_properties();
    auto iter = document_property_name_to_index_map.find(property_name);
    if (iter == document_property_name_to_index_map.end()) {
      // When the document doesn't populate the scorable property, icing will
      // populate an empty ScorablePropertyProto as a placeholder.
      continue;
    }
    const PropertyProto& doc_property = document.properties(iter->second);
    if (doc_property.int64_values_size() > 0) {
      new_property->mutable_int64_values()->Add(
          doc_property.int64_values().begin(),
          doc_property.int64_values().end());
    } else if (doc_property.double_values_size() > 0) {
      new_property->mutable_double_values()->Add(
          doc_property.double_values().begin(),
          doc_property.double_values().end());
    } else if (doc_property.boolean_values_size() > 0) {
      new_property->mutable_boolean_values()->Add(
          doc_property.boolean_values().begin(),
          doc_property.boolean_values().end());
    }
  }
  return std::unique_ptr<ScorablePropertySet>(
      new ScorablePropertySet(std::move(scorable_property_set_proto_to_build),
                              schema_type_id, schema_store));
}

const ScorablePropertyProto* ScorablePropertySet::GetScorablePropertyProto(
    const std::string& property_name) const {
  libtextclassifier3::StatusOr<std::optional<int>> index_or =
      schema_store_->GetScorablePropertyIndex(schema_type_id_, property_name);
  if (!index_or.ok() || !index_or.ValueOrDie().has_value()) {
    return nullptr;
  }
  return &scorable_property_set_proto_.properties(
      index_or.ValueOrDie().value());
}

ScorablePropertySet::ScorablePropertySet(
    ScorablePropertySetProto&& scorable_property_set_proto,
    SchemaTypeId schema_type_id, const SchemaStore* schema_store)
    : scorable_property_set_proto_(std::move(scorable_property_set_proto)),
      schema_type_id_(schema_type_id),
      schema_store_(schema_store) {}

}  // namespace lib
}  // namespace icing
