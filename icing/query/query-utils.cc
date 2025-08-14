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

#include "third_party/icing/query/query-utils.h"

#include <cstdint>
#include <memory>
#include <string_view>
#include <unordered_set>

#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/statusor.h"
#include "third_party/icing/index/iterator/document-filter-predicate.h"
#include "third_party/icing/schema/schema-store.h"
#include "third_party/icing/store/document-filter-data.h"
#include "third_party/icing/store/document-id.h"
#include "third_party/icing/store/document-store.h"
#include "third_party/icing/store/namespace-id.h"

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

class DocumentFilterPredicateBySchemaAndNamespace
    : public DocumentFilterPredicate {
 public:
  DocumentFilterPredicateBySchemaAndNamespace(
      const SearchSpecProto& search_spec, const DocumentStore& document_store,
      const SchemaStore& schema_store, int64_t current_time_ms)
      : document_store_(document_store), current_time_ms_(current_time_ms) {
    // Precompute all the NamespaceIds
    filter_by_namespace_id_enabled = !search_spec.namespace_filters().empty();
    target_namespace_ids = ConvertNamespaceToIds(document_store, search_spec);

    // Precompute all the SchemaTypeIds
    filter_by_schema_type_id_enabled =
        !search_spec.schema_type_filters().empty();
    target_schema_type_ids =
        ConvertExactSchemaTypeToIds(schema_store, search_spec);
  }

  bool operator()(DocumentId document_id) const override {
    // Try to get the DocumentFilterData
    auto document_filter_data = document_store_.GetAliveDocumentFilterData(
        document_id, current_time_ms_);
    if (!document_filter_data) {
      // Didn't find the DocumentFilterData in the filter cache. This could be
      // because the Document doesn't exist or the DocumentId isn't valid or the
      // filter cache is in some invalid state. This is bad, but not the query's
      // responsibility to fix, so just skip this result for now.
      return false;
    }
    // We should be guaranteed that filter data exists now.
    if (filter_by_namespace_id_enabled &&
        target_namespace_ids.count(document_filter_data->namespace_id()) == 0) {
      // Doesn't match one of the specified namespaces.
      return false;
    }

    if (filter_by_schema_type_id_enabled &&
        target_schema_type_ids.count(document_filter_data->schema_type_id()) ==
            0) {
      // Doesn't match one of the specified schema types.
      return false;
    }

    return true;
  }

 private:
  // List of namespace ids that documents must have.
  // filter_by_namespace_id_enabled=false means that all namespaces are valid,
  // and no documents will be filtered out.
  //
  // Note that if we want to reference the strings in namespaces later, ensure
  // that the caller who passed the Options class outlives the
  // DocHitInfoIteratorFilter.
  std::unordered_set<NamespaceId> target_namespace_ids;

  // List of schema type ids that documents must have.
  // filter_by_schema_type_id_enabled=false means that all schema types are
  // valid, and no documents will be filtered out.
  //
  // Note that if we want to reference the strings in schema types later,
  // ensure that the caller who passed the Options class outlives the
  // DocHitInfoIteratorFilter.
  std::unordered_set<SchemaTypeId> target_schema_type_ids;

  bool filter_by_schema_type_id_enabled = false;
  bool filter_by_namespace_id_enabled = false;

  const DocumentStore& document_store_;
  int64_t current_time_ms_;
};

}  // namespace

std::unique_ptr<DocumentFilterPredicate> GetFilterPredicateBySchemaAndNamespace(
    const SearchSpecProto& search_spec, const DocumentStore& document_store,
    const SchemaStore& schema_store, int64_t current_time_ms) {
  return std::make_unique<DocumentFilterPredicateBySchemaAndNamespace>(
      search_spec, document_store, schema_store, current_time_ms);
}

}  // namespace lib
}  // namespace icing
