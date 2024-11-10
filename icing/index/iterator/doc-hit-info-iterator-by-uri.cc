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

#include "icing/index/iterator/doc-hit-info-iterator-by-uri.h"

#include <algorithm>
#include <functional>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<std::unique_ptr<DocHitInfoIteratorByUri>>
DocHitInfoIteratorByUri::Create(const DocumentStore* document_store,
                                const SearchSpecProto& search_spec) {
  ICING_RETURN_ERROR_IF_NULL(document_store);

  if (search_spec.document_uri_filters().empty()) {
    return absl_ports::InvalidArgumentError(
        "Cannot create DocHitInfoIteratorByUri with empty "
        "document_uri_filters");
  }

  std::unordered_set<std::string> all_namespaces(
      search_spec.namespace_filters().begin(),
      search_spec.namespace_filters().end());
  std::vector<DocumentId> target_document_ids;
  for (const NamespaceDocumentUriGroup& namespace_document_uri_group :
       search_spec.document_uri_filters()) {
    const std::string& current_namespace =
        namespace_document_uri_group.namespace_();
    if (!all_namespaces.empty() &&
        all_namespaces.count(current_namespace) == 0) {
      // The current namespace doesn't appear in the namespace filter.
      return absl_ports::InvalidArgumentError(absl_ports::StrCat(
          "The namespace : ", current_namespace,
          " appears in the document uri filter, but does not appear in the "
          "namespace filter."));
    }

    if (namespace_document_uri_group.document_uris().empty()) {
      return absl_ports::InvalidArgumentError(absl_ports::StrCat(
          "The namespace ", current_namespace,
          " has not specified any document in the document uri filter. Please "
          "use the namespace filter to exclude a namespace instead."));
    }
    for (const std::string& document_uri :
         namespace_document_uri_group.document_uris()) {
      auto document_id_or =
          document_store->GetDocumentId(current_namespace, document_uri);
      if (!document_id_or.ok()) {
        continue;
      }
      target_document_ids.push_back(document_id_or.ValueOrDie());
    }
  }
  // Sort the document ids in descending order, which is a requirement for all
  // iterators.
  std::sort(target_document_ids.begin(), target_document_ids.end(),
            std::greater<DocumentId>());

  return std::unique_ptr<DocHitInfoIteratorByUri>(
      new DocHitInfoIteratorByUri(std::move(target_document_ids)));
}

libtextclassifier3::Status DocHitInfoIteratorByUri::Advance() {
  current_document_id_index_++;

  if (current_document_id_index_ >= target_document_ids_.size()) {
    doc_hit_info_ = DocHitInfo(kInvalidDocumentId);
    return absl_ports::ResourceExhaustedError(
        "No more DocHitInfos in iterator");
  }
  doc_hit_info_.set_document_id(
      target_document_ids_[current_document_id_index_]);
  return libtextclassifier3::Status::OK;
}

}  // namespace lib
}  // namespace icing
