// Copyright (C) 2021 Google LLC
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

#ifndef ICING_STORE_SUGGESTION_RESULT_CHECKER_IMPL_H_
#define ICING_STORE_SUGGESTION_RESULT_CHECKER_IMPL_H_

#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/store/namespace-id.h"
#include "icing/store/suggestion-result-checker.h"

namespace icing {
namespace lib {

class SuggestionResultCheckerImpl : public SuggestionResultChecker {
 public:
  explicit SuggestionResultCheckerImpl(
      const DocumentStore* document_store,
      std::unordered_set<NamespaceId> target_namespace_ids,
      std::unordered_map<NamespaceId, std::unordered_set<DocumentId>>
          document_id_filter_map,
      std::unordered_set<SchemaTypeId> target_schema_type_ids,
      std::unordered_map<SchemaTypeId, SectionIdMask> property_filter_map)
      : document_store_(*document_store),
        target_namespace_ids_(std::move(target_namespace_ids)),
        document_id_filter_map_(std::move(document_id_filter_map)),
        target_schema_type_ids_(std::move(target_schema_type_ids)),
        property_filter_map_(std::move(property_filter_map)) {}

  bool BelongsToTargetResults(DocumentId document_id,
                              SectionId section_id) const override {
    // Get the document filter data first.
    auto document_filter_data_optional_ =
        document_store_.GetAliveDocumentFilterData(document_id);
    if (!document_filter_data_optional_) {
      // The document doesn't exist.
      return false;
    }
    DocumentFilterData document_filter_data =
        document_filter_data_optional_.value();

    // 1: Check the namespace filter
    if (!target_namespace_ids_.empty() &&
        target_namespace_ids_.find(document_filter_data.namespace_id()) ==
            target_namespace_ids_.end()) {
      // User gives a namespace filter, and the current namespace isn't desired.
      return false;
    }

    // 2: Check the document id filter
    if (!document_id_filter_map_.empty()) {
      auto document_ids_itr =
          document_id_filter_map_.find(document_filter_data.namespace_id());
      if (document_ids_itr != document_id_filter_map_.end() &&
          document_ids_itr->second.find(document_id) ==
              document_ids_itr->second.end()) {
        // The client doesn't set desired document ids in this namespace, or the
        // client doesn't want this document.
        return false;
      }
    }

    // 3: Check the schema type filter
    if (!target_schema_type_ids_.empty() &&
        target_schema_type_ids_.find(document_filter_data.schema_type_id()) ==
            target_schema_type_ids_.end()) {
      // User gives a schema type filter, and the current schema type isn't
      // desired.
      return false;
    }

    if (!property_filter_map_.empty()) {
      auto section_mask_itr =
          property_filter_map_.find(document_filter_data.schema_type_id());
      if (section_mask_itr != property_filter_map_.end() &&
          (section_mask_itr->second & (UINT64_C(1) << section_id)) == 0) {
        // The client doesn't set desired properties in this schema, or the
        // client doesn't want this property.
        return false;
      }
    }
    return true;
  }
  const DocumentStore& document_store_;
  std::unordered_set<NamespaceId> target_namespace_ids_;
  std::unordered_map<NamespaceId, std::unordered_set<DocumentId>>
      document_id_filter_map_;
  std::unordered_set<SchemaTypeId> target_schema_type_ids_;
  std::unordered_map<SchemaTypeId, SectionIdMask> property_filter_map_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_STORE_SUGGESTION_RESULT_CHECKER_IMPL_H_