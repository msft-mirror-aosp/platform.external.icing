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

#include "icing/result/snippet-context.h"

#include <utility>

#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/util/document-util.h"

namespace icing {
namespace lib {

void SnippetContext::Optimize(
    const DocumentStore::OptimizeResult& optimize_result) {
  // Convert document ids in embedding_match_info_map.
  DocumentEmbeddingMatchInfoMap optimized_embedding_match_info_map;
  optimized_embedding_match_info_map.reserve(embedding_match_info_map.size());
  for (auto& [doc_id, match_info_list] : embedding_match_info_map) {
    DocumentId optimized_doc_id = document_util::GetOptimizedDocumentId(
        doc_id, optimize_result.document_id_old_to_new);
    if (optimized_doc_id != kInvalidDocumentId) {
      optimized_embedding_match_info_map[optimized_doc_id] =
          std::move(match_info_list);
    }
  }
  embedding_match_info_map = std::move(optimized_embedding_match_info_map);
}

}  // namespace lib
}  // namespace icing
