// Copyright (C) 2019 Google LLC
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

#include "icing/result/result-retriever.h"

#include <string_view>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/result/page-result-state.h"
#include "icing/result/projection-tree.h"
#include "icing/result/snippet-context.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

void Project(const std::vector<ProjectionTree::Node>& projection_tree,
             proto2::RepeatedPtrField<PropertyProto>* properties) {
  int num_kept = 0;
  for (int cur_pos = 0; cur_pos < properties->size(); ++cur_pos) {
    PropertyProto* prop = properties->Mutable(cur_pos);
    auto itr = std::find_if(projection_tree.begin(), projection_tree.end(),
                            [&prop](const ProjectionTree::Node& node) {
                              return node.name == prop->name();
                            });
    if (itr == projection_tree.end()) {
      // Property is not present in the projection tree. Just skip it.
      continue;
    }
    // This property should be kept.
    properties->SwapElements(num_kept, cur_pos);
    ++num_kept;
    if (itr->children.empty()) {
      // A field mask does refer to this property, but it has no children. So
      // we should take the entire property, with all of its
      // subproperties/values
      continue;
    }
    // The field mask refers to children of this property. Recurse through the
    // document values that this property holds and project the children
    // requested by this field mask.
    for (DocumentProto& subproperty : *(prop->mutable_document_values())) {
      Project(itr->children, subproperty.mutable_properties());
    }
  }
  properties->DeleteSubrange(num_kept, properties->size() - num_kept);
}

}  // namespace

libtextclassifier3::StatusOr<std::unique_ptr<ResultRetriever>>
ResultRetriever::Create(const DocumentStore* doc_store,
                        const SchemaStore* schema_store,
                        const LanguageSegmenter* language_segmenter,
                        const Normalizer* normalizer,
                        bool ignore_bad_document_ids) {
  ICING_RETURN_ERROR_IF_NULL(doc_store);
  ICING_RETURN_ERROR_IF_NULL(schema_store);
  ICING_RETURN_ERROR_IF_NULL(language_segmenter);

  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<SnippetRetriever> snippet_retriever,
      SnippetRetriever::Create(schema_store, language_segmenter, normalizer));

  return std::unique_ptr<ResultRetriever>(new ResultRetriever(
      doc_store, std::move(snippet_retriever), ignore_bad_document_ids));
}

libtextclassifier3::StatusOr<std::vector<SearchResultProto::ResultProto>>
ResultRetriever::RetrieveResults(
    const PageResultState& page_result_state) const {
  std::vector<SearchResultProto::ResultProto> search_results;
  search_results.reserve(page_result_state.scored_document_hits.size());

  const SnippetContext& snippet_context = page_result_state.snippet_context;
  // Calculates how many snippets to return for this page.
  int remaining_num_to_snippet = snippet_context.snippet_spec.num_to_snippet() -
                                 page_result_state.num_previously_returned;

  if (remaining_num_to_snippet < 0) {
    remaining_num_to_snippet = 0;
  }

  for (const auto& scored_document_hit :
       page_result_state.scored_document_hits) {
    libtextclassifier3::StatusOr<DocumentProto> document_or =
        doc_store_.Get(scored_document_hit.document_id());

    if (!document_or.ok()) {
      // Internal errors from document store are IO errors, return directly.
      if (absl_ports::IsInternal(document_or.status())) {
        return document_or.status();
      }

      if (ignore_bad_document_ids_) {
        continue;
      } else {
        return document_or.status();
      }
    }

    // Apply projection
    auto itr = page_result_state.projection_tree_map.find(
        document_or.ValueOrDie().schema());
    if (itr != page_result_state.projection_tree_map.end()) {
      Project(itr->second.root().children,
              document_or.ValueOrDie().mutable_properties());
    }

    SearchResultProto::ResultProto result;
    // Add the snippet if requested.
    if (snippet_context.snippet_spec.num_matches_per_property() > 0 &&
        remaining_num_to_snippet > search_results.size()) {
      SnippetProto snippet_proto = snippet_retriever_->RetrieveSnippet(
          snippet_context.query_terms, snippet_context.match_type,
          snippet_context.snippet_spec, document_or.ValueOrDie(),
          scored_document_hit.hit_section_id_mask());
      *result.mutable_snippet() = std::move(snippet_proto);
    }

    // Add the document, itself.
    *result.mutable_document() = std::move(document_or).ValueOrDie();
    search_results.push_back(std::move(result));
  }
  return search_results;
}

}  // namespace lib
}  // namespace icing
