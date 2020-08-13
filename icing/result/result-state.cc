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

#include "icing/result/result-state.h"

#include "icing/scoring/ranker.h"
#include "icing/util/logging.h"

namespace icing {
namespace lib {

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

ResultState::ResultState(std::vector<ScoredDocumentHit> scored_document_hits,
                         SectionRestrictQueryTermsMap query_terms,
                         const SearchSpecProto& search_spec,
                         const ScoringSpecProto& scoring_spec,
                         const ResultSpecProto& result_spec)
    : scored_document_hits_(std::move(scored_document_hits)),
      snippet_context_(CreateSnippetContext(std::move(query_terms), search_spec,
                                            result_spec)),
      num_per_page_(result_spec.num_per_page()),
      num_returned_(0),
      scored_document_hit_comparator_(scoring_spec.order_by() ==
                                      ScoringSpecProto::Order::DESC) {
  BuildHeapInPlace(&scored_document_hits_, scored_document_hit_comparator_);
}

std::vector<ScoredDocumentHit> ResultState::GetNextPage() {
  std::vector<ScoredDocumentHit> scored_document_hits = PopTopResultsFromHeap(
      &scored_document_hits_, num_per_page_, scored_document_hit_comparator_);
  num_returned_ += scored_document_hits.size();
  return scored_document_hits;
}

void ResultState::TruncateHitsTo(int new_size) {
  if (new_size < 0 || scored_document_hits_.size() <= new_size) {
    return;
  }

  // Copying the best new_size results.
  scored_document_hits_ = PopTopResultsFromHeap(
      &scored_document_hits_, new_size, scored_document_hit_comparator_);
}

}  // namespace lib
}  // namespace icing
