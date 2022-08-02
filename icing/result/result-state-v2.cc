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

#include "icing/result/result-state-v2.h"

#include <atomic>
#include <memory>

#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/result/projection-tree.h"
#include "icing/result/snippet-context.h"
#include "icing/scoring/scored-document-hits-ranker.h"

namespace icing {
namespace lib {

namespace {
SnippetContext CreateSnippetContext(SectionRestrictQueryTermsMap query_terms,
                                    const SearchSpecProto& search_spec,
                                    const ResultSpecProto& result_spec) {
  if (result_spec.snippet_spec().num_to_snippet() > 0 &&
      result_spec.snippet_spec().num_matches_per_property() > 0) {
    // Needs snippeting
    return SnippetContext(std::move(query_terms), result_spec.snippet_spec(),
                          search_spec.term_match_type());
  }
  return SnippetContext(/*query_terms_in=*/{},
                        ResultSpecProto::SnippetSpecProto::default_instance(),
                        TermMatchType::UNKNOWN);
}
}  // namespace

ResultStateV2::ResultStateV2(
    std::unique_ptr<ScoredDocumentHitsRanker> scored_document_hits_ranker_in,
    SectionRestrictQueryTermsMap query_terms,
    const SearchSpecProto& search_spec, const ScoringSpecProto& scoring_spec,
    const ResultSpecProto& result_spec, const DocumentStore& document_store)
    : scored_document_hits_ranker(std::move(scored_document_hits_ranker_in)),
      num_returned(0),
      snippet_context_(CreateSnippetContext(std::move(query_terms), search_spec,
                                            result_spec)),
      num_per_page_(result_spec.num_per_page()),
      num_total_hits_(nullptr) {
  for (const TypePropertyMask& type_field_mask :
       result_spec.type_property_masks()) {
    projection_tree_map_.insert(
        {type_field_mask.schema_type(), ProjectionTree(type_field_mask)});
  }

  for (const ResultSpecProto::ResultGrouping& result_grouping :
       result_spec.result_groupings()) {
    int group_id = group_result_limits.size();
    group_result_limits.push_back(result_grouping.max_results());
    for (const std::string& name_space : result_grouping.namespaces()) {
      auto namespace_id_or = document_store.GetNamespaceId(name_space);
      if (!namespace_id_or.ok()) {
        continue;
      }
      namespace_group_id_map_.insert({namespace_id_or.ValueOrDie(), group_id});
    }
  }
}

ResultStateV2::~ResultStateV2() {
  IncrementNumTotalHits(-1 * scored_document_hits_ranker->size());
}

void ResultStateV2::RegisterNumTotalHits(std::atomic<int>* num_total_hits) {
  // Decrement the original num_total_hits_ before registering a new one.
  IncrementNumTotalHits(-1 * scored_document_hits_ranker->size());
  num_total_hits_ = num_total_hits;
  IncrementNumTotalHits(scored_document_hits_ranker->size());
}

void ResultStateV2::IncrementNumTotalHits(int increment_by) {
  if (num_total_hits_ != nullptr) {
    *num_total_hits_ += increment_by;
  }
}

}  // namespace lib
}  // namespace icing
