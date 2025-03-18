// Copyright (C) 2023 Google LLC
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

#include "icing/result/result-adjustment-info.h"

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/index/embed/embedding-query-results.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/query/query-terms.h"
#include "icing/result/projection-tree.h"
#include "icing/result/snippet-context.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/util/logging.h"

namespace icing {
namespace lib {

namespace {

// Returns a map of document_id to a vector of SectionEmbeddingMatchInfoEntries
// for the given embedding query results. For each document, only results for
// the top max_sections_per_doc sections are returned. Only results for the
// given document_ids are returned and only includes the top
// max_sections_per_doc sections per document.
SnippetContext::DocumentEmbeddingMatchInfoMap GetMatchInfoByDocumentAndSection(
    const EmbeddingQueryResults& embedding_query_results,
    const std::unordered_set<DocumentId>& documents_to_include,
    int num_matches_per_property) {
  SnippetContext::DocumentEmbeddingMatchInfoMap result_map;
  // Maps from (document_id, section_id) to the match count for that section.
  std::unordered_map<DocumentId, std::unordered_map<SectionId, int>>
      section_match_count;

  for (const auto& [query_vector_index, metric_type_map] :
       embedding_query_results.result_infos) {
    for (const auto& [metric_type, info_map] : metric_type_map) {
      for (const auto& [doc_id, match_infos] : info_map) {
        if (documents_to_include.find(doc_id) == documents_to_include.end()) {
          continue;
        }

        if (match_infos.section_infos == nullptr) {
          // No section info indicates that embedding match info is not
          // enabled.
          continue;
        }

        if (match_infos.section_infos->size() != match_infos.scores.size()) {
          // This should never happen.
          ICING_LOG(ERROR)
              << "EmbeddingMatchInfos has mismatched section_infos and "
                 "scores vectors for document with id "
              << doc_id
              << ". Section_infos size: " << match_infos.section_infos->size()
              << ", scores size: " << match_infos.scores.size();
          continue;
        }
        for (int i = 0; i < match_infos.scores.size(); ++i) {
          SectionId section_id = match_infos.section_infos->at(i).section_id;
          if (section_match_count[doc_id][section_id] >=
              num_matches_per_property) {
            continue;
          }
          result_map[doc_id].push_back(SnippetContext::EmbeddingMatchInfoEntry(
              match_infos.scores[i], metric_type,
              match_infos.section_infos->at(i).position, query_vector_index,
              section_id));
          ++section_match_count[doc_id][section_id];
        }
      }
    }
  }
  return result_map;
}

SnippetContext CreateSnippetContext(
    const SearchSpecProto& search_spec, const ResultSpecProto& result_spec,
    const EmbeddingQueryResults& embedding_query_results,
    const std::unordered_set<DocumentId>& documents_to_snippet_hint,
    SectionRestrictQueryTermsMap query_terms) {
  if (result_spec.snippet_spec().num_to_snippet() > 0 &&
      result_spec.snippet_spec().num_matches_per_property() > 0) {
    // Needs snippeting
    SnippetContext::EmbeddingQueryVectorMetadataMap
        embedding_query_vector_metadata;
    SnippetContext::DocumentEmbeddingMatchInfoMap embedding_match_info_map;
    if (result_spec.snippet_spec().get_embedding_match_info()) {
      for (int i = 0; i < search_spec.embedding_query_vectors_size(); ++i) {
        const PropertyProto::VectorProto& query_vector =
            search_spec.embedding_query_vectors(i);
        int dimension = query_vector.values().size();
        std::string model_signature = query_vector.model_signature();
        embedding_query_vector_metadata[dimension][std::move(model_signature)]
            .insert(i);
      }
      embedding_match_info_map = GetMatchInfoByDocumentAndSection(
          embedding_query_results, documents_to_snippet_hint,
          result_spec.snippet_spec().num_matches_per_property());
    }
    return SnippetContext(
        std::move(query_terms), std::move(embedding_query_vector_metadata),
        std::move(embedding_match_info_map), result_spec.snippet_spec(),
        search_spec.term_match_type());
  }
  return SnippetContext(/*query_terms_in=*/{},
                        /*embedding_query_vector_metadata_in=*/{},
                        /*embedding_match_info_map_in=*/{},
                        ResultSpecProto::SnippetSpecProto::default_instance(),
                        TermMatchType::UNKNOWN);
}

}  // namespace

ResultAdjustmentInfo::ResultAdjustmentInfo(
    const SearchSpecProto& search_spec, const ScoringSpecProto& scoring_spec,
    const ResultSpecProto& result_spec, const SchemaStore* schema_store,
    const EmbeddingQueryResults& embedding_query_results,
    std::unordered_set<DocumentId> documents_to_snippet_hint,
    SectionRestrictQueryTermsMap query_terms)
    : snippet_context(CreateSnippetContext(
          search_spec, result_spec, embedding_query_results,
          documents_to_snippet_hint, std::move(query_terms))),
      remaining_num_to_snippet(result_spec.snippet_spec().num_to_snippet()) {
  for (const SchemaStore::ExpandedTypePropertyMask& type_field_mask :
       schema_store->ExpandTypePropertyMasks(
           result_spec.type_property_masks())) {
    projection_tree_map.insert(
        {type_field_mask.schema_type, ProjectionTree(type_field_mask)});
  }
}

}  // namespace lib
}  // namespace icing
