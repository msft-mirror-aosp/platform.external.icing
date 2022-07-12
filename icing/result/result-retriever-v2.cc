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

#include "icing/result/result-retriever-v2.h"

#include <memory>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/result/page-result.h"
#include "icing/result/projection-tree.h"
#include "icing/result/projector.h"
#include "icing/result/snippet-context.h"
#include "icing/result/snippet-retriever.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/store/document-store.h"
#include "icing/store/namespace-id.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/transform/normalizer.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

bool GroupResultLimiterV2::ShouldBeRemoved(
    const ScoredDocumentHit& scored_document_hit,
    const std::unordered_map<NamespaceId, int>& namespace_group_id_map,
    const DocumentStore& document_store,
    std::vector<int>& group_result_limits) const {
  auto document_filter_data_optional =
      document_store.GetAliveDocumentFilterData(
          scored_document_hit.document_id());
  if (!document_filter_data_optional) {
    // The document doesn't exist.
    return true;
  }
  NamespaceId namespace_id =
      document_filter_data_optional.value().namespace_id();
  auto iter = namespace_group_id_map.find(namespace_id);
  if (iter == namespace_group_id_map.end()) {
    // If a namespace id isn't found in namespace_group_id_map, then there are
    // no limits placed on results from this namespace.
    return false;
  }
  int& count = group_result_limits.at(iter->second);
  if (count <= 0) {
    return true;
  }
  --count;
  return false;
}

libtextclassifier3::StatusOr<std::unique_ptr<ResultRetrieverV2>>
ResultRetrieverV2::Create(
    const DocumentStore* doc_store, const SchemaStore* schema_store,
    const LanguageSegmenter* language_segmenter, const Normalizer* normalizer,
    std::unique_ptr<const GroupResultLimiterV2> group_result_limiter) {
  ICING_RETURN_ERROR_IF_NULL(doc_store);
  ICING_RETURN_ERROR_IF_NULL(schema_store);
  ICING_RETURN_ERROR_IF_NULL(language_segmenter);
  ICING_RETURN_ERROR_IF_NULL(normalizer);
  ICING_RETURN_ERROR_IF_NULL(group_result_limiter);

  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<SnippetRetriever> snippet_retriever,
      SnippetRetriever::Create(schema_store, language_segmenter, normalizer));

  return std::unique_ptr<ResultRetrieverV2>(
      new ResultRetrieverV2(doc_store, std::move(snippet_retriever),
                            std::move(group_result_limiter)));
}

std::pair<PageResult, bool> ResultRetrieverV2::RetrieveNextPage(
    ResultStateV2& result_state) const {
  absl_ports::unique_lock l(&result_state.mutex);

  // For calculating page
  int original_scored_document_hits_ranker_size =
      result_state.scored_document_hits_ranker->size();
  int num_results_with_snippets = 0;

  const SnippetContext& snippet_context = result_state.snippet_context();
  const std::unordered_map<std::string, ProjectionTree>& projection_tree_map =
      result_state.projection_tree_map();
  auto wildcard_projection_tree_itr = projection_tree_map.find(
      std::string(ProjectionTree::kSchemaTypeWildcard));

  // Calculates how many snippets to return for this page.
  int remaining_num_to_snippet =
      snippet_context.snippet_spec.num_to_snippet() - result_state.num_returned;
  if (remaining_num_to_snippet < 0) {
    remaining_num_to_snippet = 0;
  }

  // Retrieve info
  std::vector<SearchResultProto::ResultProto> results;
  while (results.size() < result_state.num_per_page() &&
         !result_state.scored_document_hits_ranker->empty()) {
    ScoredDocumentHit next_best_document_hit =
        result_state.scored_document_hits_ranker->PopNext();
    if (group_result_limiter_->ShouldBeRemoved(
            next_best_document_hit, result_state.namespace_group_id_map(),
            doc_store_, result_state.group_result_limits)) {
      continue;
    }

    libtextclassifier3::StatusOr<DocumentProto> document_or =
        doc_store_.Get(next_best_document_hit.document_id());
    if (!document_or.ok()) {
      // Skip the document if getting errors.
      ICING_LOG(WARNING) << "Fail to fetch document from document store: "
                         << document_or.status().error_message();
      continue;
    }

    DocumentProto document = std::move(document_or).ValueOrDie();
    // Apply projection
    auto itr = projection_tree_map.find(document.schema());
    if (itr != projection_tree_map.end()) {
      projector::Project(itr->second.root().children, &document);
    } else if (wildcard_projection_tree_itr != projection_tree_map.end()) {
      projector::Project(wildcard_projection_tree_itr->second.root().children,
                         &document);
    }

    SearchResultProto::ResultProto result;
    // Add the snippet if requested.
    if (snippet_context.snippet_spec.num_matches_per_property() > 0 &&
        remaining_num_to_snippet > results.size()) {
      SnippetProto snippet_proto = snippet_retriever_->RetrieveSnippet(
          snippet_context.query_terms, snippet_context.match_type,
          snippet_context.snippet_spec, document,
          next_best_document_hit.hit_section_id_mask());
      *result.mutable_snippet() = std::move(snippet_proto);
      ++num_results_with_snippets;
    }

    // Add the document, itself.
    *result.mutable_document() = std::move(document);
    result.set_score(next_best_document_hit.score());
    results.push_back(std::move(result));
  }

  // Update numbers in ResultState
  result_state.num_returned += results.size();
  result_state.IncrementNumTotalHits(
      result_state.scored_document_hits_ranker->size() -
      original_scored_document_hits_ranker_size);

  bool has_more_results = !result_state.scored_document_hits_ranker->empty();

  return std::make_pair(
      PageResult(std::move(results), num_results_with_snippets,
                 result_state.num_per_page()),
      has_more_results);
}

}  // namespace lib
}  // namespace icing
