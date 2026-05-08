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

#include "icing/store/document-group-info.h"

#include <algorithm>
#include <iterator>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/store/document-id.h"

namespace icing {
namespace lib {

void DocumentGroupInfo::AddDocument(DocumentMetadata document_metadata) {
  DocumentGroupKey key = {
      .schema_type_name = std::move(document_metadata.schema_type_name),
      .name_space = std::move(document_metadata.name_space)};
  DocumentUriIdPair uri_id_pair = {
      .uri = std::move(document_metadata.uri),
      .document_id = document_metadata.document_id};

  doc_group_info_map_[key].push_back(std::move(uri_id_pair));
}

void DocumentGroupInfo::Merge(DocumentGroupInfo&& other) {
  for (auto& [key, other_uri_id_pair_list] : other.doc_group_info_map_) {
    std::vector<DocumentUriIdPair>& uri_id_pair_list_ref =
        this->doc_group_info_map_[key];
    uri_id_pair_list_ref.reserve(uri_id_pair_list_ref.size() +
                                 other_uri_id_pair_list.size());
    std::move(other_uri_id_pair_list.begin(), other_uri_id_pair_list.end(),
              std::back_inserter(uri_id_pair_list_ref));
  }
}

int DocumentGroupInfo::GetTotalNumDocs() const {
  int total_num_docs = 0;
  for (const auto& [_, document_uri_id_pair_list] : doc_group_info_map_) {
    total_num_docs += static_cast<int>(document_uri_id_pair_list.size());
  }
  return total_num_docs;
}

std::unordered_set<DocumentId> DocumentGroupInfo::GetAllDocumentIds() const {
  std::unordered_set<DocumentId> all_document_ids;
  for (const auto& [_, document_uri_id_pair_list] : doc_group_info_map_) {
    for (const auto& [uri, document_id] : document_uri_id_pair_list) {
      all_document_ids.insert(document_id);
    }
  }
  return all_document_ids;
}

}  // namespace lib
}  // namespace icing
