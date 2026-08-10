// Copyright (C) 2026 Google LLC
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

#include "icing/result/result-utils.h"

#include <optional>
#include <string_view>

#include "icing/proto/search.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-store.h"
#include "icing/store/namespace-id.h"

namespace icing {
namespace lib {

namespace result_utils {

std::optional<ResultGroupingEntryId> EncodeResultGroupingEntryId(
    const SchemaStore& schema_store, const DocumentStore& document_store,
    ResultSpecProto::ResultGroupingType result_group_type,
    std::string_view name_space, std::string_view schema) {
  auto namespace_id_or = document_store.GetNamespaceId(name_space);
  auto schema_type_id_or = schema_store.GetSchemaTypeId(schema);

  NamespaceId namespace_id =
      namespace_id_or.ok() ? namespace_id_or.ValueOrDie() : kInvalidNamespaceId;
  SchemaTypeId schema_type_id = schema_type_id_or.ok()
                                    ? schema_type_id_or.ValueOrDie()
                                    : kInvalidSchemaTypeId;
  return EncodeResultGroupingEntryId(result_group_type, namespace_id,
                                     schema_type_id);
}

std::optional<ResultGroupingEntryId> EncodeResultGroupingEntryId(
    ResultSpecProto::ResultGroupingType result_group_type,
    NamespaceId namespace_id, SchemaTypeId schema_type_id) {
  static_assert(sizeof(NamespaceId) * 8 <= 16,
                "Current ResultGroupingEntryId encoding only supports "
                "namespace id up to 16 bits.");
  static_assert(sizeof(SchemaTypeId) * 8 <= 16,
                "Current ResultGroupingEntryId encoding only supports schema "
                "type id up to 16 bits.");

  // Note: this encoding method only works for a single
  // ResultSpecProto::ResultGroupingType in a single search request. If multiple
  // types can be used in the same search request, this encoding method needs to
  // be updated since there will be encoded id collisions for NAMESPACE and
  // SCHEMA_TYPE.

  switch (result_group_type) {
    case ResultSpecProto::ResultGroupingType::
        ResultSpecProto_ResultGroupingType_NONE:
      return std::nullopt;
    case ResultSpecProto::ResultGroupingType::
        ResultSpecProto_ResultGroupingType_SCHEMA_TYPE: {
      if (schema_type_id == kInvalidSchemaTypeId) {
        return std::nullopt;
      }
      return schema_type_id;
    }
    case ResultSpecProto::ResultGroupingType::
        ResultSpecProto_ResultGroupingType_NAMESPACE: {
      if (namespace_id == kInvalidNamespaceId) {
        return std::nullopt;
      }
      return namespace_id;
    }
    case ResultSpecProto::ResultGroupingType::
        ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE: {
      if (namespace_id == kInvalidNamespaceId ||
          schema_type_id == kInvalidSchemaTypeId) {
        return std::nullopt;
      }
      // TODO(b/258715421): Temporary workaround to get a ResultGroupingEntryId
      //                    given the Namespace Id and SchemaType Id.
      return (static_cast<ResultGroupingEntryId>(namespace_id) << 16) |
             schema_type_id;
    }
  }
}

}  // namespace result_utils

}  // namespace lib
}  // namespace icing
