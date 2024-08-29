// Copyright (C) 2022 Google LLC
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

#include "icing/query/query-utils.h"

#include <string_view>
#include <unordered_set>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/index/iterator/doc-hit-info-iterator-filter.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-store.h"
#include "icing/store/namespace-id.h"

namespace icing {
namespace lib {

namespace {

std::unordered_set<NamespaceId> ConvertNamespaceToIds(
    const DocumentStore& document_store, const SearchSpecProto& search_spec) {
  std::unordered_set<NamespaceId> ids;
  for (std::string_view name_space : search_spec.namespace_filters()) {
    auto namespace_id_or = document_store.GetNamespaceId(name_space);

    // If we can't find the NamespaceId, just throw it away
    if (namespace_id_or.ok()) {
      ids.insert(namespace_id_or.ValueOrDie());
    }
  }
  return ids;
}

std::unordered_set<SchemaTypeId> ConvertExactSchemaTypeToIds(
    const SchemaStore& schema_store, const SearchSpecProto& search_spec) {
  std::unordered_set<SchemaTypeId> ids;
  ids.reserve(search_spec.schema_type_filters_size());
  for (std::string_view schema_type : search_spec.schema_type_filters()) {
    libtextclassifier3::StatusOr<SchemaTypeId> schema_type_id_or =
        schema_store.GetSchemaTypeId(schema_type);

    // If we can't find the SchemaTypeId, just throw it away
    if (schema_type_id_or.ok()) {
      ids.insert(schema_type_id_or.ValueOrDie());
    }
  }
  return ids;
}

}  // namespace

DocHitInfoIteratorFilter::Options GetFilterOptions(
    const SearchSpecProto& search_spec, const DocumentStore& document_store,
    const SchemaStore& schema_store) {
  DocHitInfoIteratorFilter::Options options;

  // Precompute all the NamespaceIds
  options.filter_by_namespace_id_enabled =
      !search_spec.namespace_filters().empty();
  options.target_namespace_ids =
      ConvertNamespaceToIds(document_store, search_spec);

  // Precompute all the SchemaTypeIds
  options.filter_by_schema_type_id_enabled =
      !search_spec.schema_type_filters().empty();
  options.target_schema_type_ids =
      ConvertExactSchemaTypeToIds(schema_store, search_spec);
  return options;
}

}  // namespace lib
}  // namespace icing
