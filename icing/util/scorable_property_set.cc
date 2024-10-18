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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/legacy/core/icing-string-util.h"
#include "icing/proto/internal/scorable_property_set.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema/property-util.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/scorable_property_manager.h"
#include "icing/store/document-filter-data.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<std::unique_ptr<ScorablePropertySet>>
ScorablePropertySet::Create(
    ScorablePropertySetProto&& scorable_property_set_proto,
    SchemaTypeId schema_type_id, const SchemaStore* schema_store) {
  ICING_ASSIGN_OR_RETURN(
      const std::vector<ScorablePropertyManager::ScorablePropertyInfo>*
          ordered_scorable_property_info,
      schema_store->GetOrderedScorablePropertyInfo(schema_type_id));
  if (ordered_scorable_property_info == nullptr ||
      ordered_scorable_property_info->size() !=
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
      const std::vector<ScorablePropertyManager::ScorablePropertyInfo>*
          ordered_scorable_property_info,
      schema_store->GetOrderedScorablePropertyInfo(schema_type_id));
  if (ordered_scorable_property_info == nullptr) {
    // It should never happen
    return absl_ports::InternalError(
        "SchemaStore::ordered_scorable_property_paths returned nullptr");
  }
  if (ordered_scorable_property_info->empty()) {
    return absl_ports::InvalidArgumentError(IcingStringUtil::StringPrintf(
        "No scorable property defined under the config of type id %d",
        schema_type_id));
  }

  ScorablePropertySetProto scorable_property_set_proto_to_build;
  for (const ScorablePropertyManager::ScorablePropertyInfo&
           scorable_property_info : *ordered_scorable_property_info) {
    ScorablePropertyProto* new_property =
        scorable_property_set_proto_to_build.add_properties();

    if (scorable_property_info.data_type ==
        PropertyConfigProto::DataType::DOUBLE) {
      libtextclassifier3::StatusOr<std::vector<double>> content_or =
          property_util::ExtractPropertyValuesFromDocument<double>(
              document, scorable_property_info.property_path);
      if (content_or.ok()) {
        new_property->mutable_double_values()->Add(
            content_or.ValueOrDie().begin(), content_or.ValueOrDie().end());
      }
    } else if (scorable_property_info.data_type ==
               PropertyConfigProto::DataType::INT64) {
      libtextclassifier3::StatusOr<std::vector<int64_t>> content_or =
          property_util::ExtractPropertyValuesFromDocument<int64_t>(
              document, scorable_property_info.property_path);
      if (content_or.ok()) {
        new_property->mutable_int64_values()->Add(
            content_or.ValueOrDie().begin(), content_or.ValueOrDie().end());
      }
    } else if (scorable_property_info.data_type ==
               PropertyConfigProto::DataType::BOOLEAN) {
      libtextclassifier3::StatusOr<std::vector<bool>> content_or =
          property_util::ExtractPropertyValuesFromDocument<bool>(
              document, scorable_property_info.property_path);
      if (content_or.ok()) {
        new_property->mutable_boolean_values()->Add(
            content_or.ValueOrDie().begin(), content_or.ValueOrDie().end());
      }
    }
  }
  return std::unique_ptr<ScorablePropertySet>(
      new ScorablePropertySet(std::move(scorable_property_set_proto_to_build),
                              schema_type_id, schema_store));
}

const ScorablePropertyProto* ScorablePropertySet::GetScorablePropertyProto(
    const std::string& property_path) const {
  libtextclassifier3::StatusOr<std::optional<int>> index_or =
      schema_store_->GetScorablePropertyIndex(schema_type_id_, property_path);
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
