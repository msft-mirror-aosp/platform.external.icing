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

#ifndef ICING_RESULT_RESULT_STATE_V2_H_
#define ICING_RESULT_RESULT_STATE_V2_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "icing/absl_ports/mutex.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/result/projection-tree.h"
#include "icing/result/snippet-context.h"
#include "icing/scoring/scored-document-hits-ranker.h"
#include "icing/store/document-store.h"
#include "icing/store/namespace-id.h"

namespace icing {
namespace lib {

// Used to hold information needed across multiple pagination requests of the
// same query. Stored in ResultStateManager.
class ResultStateV2 {
 public:
  explicit ResultStateV2(
      std::unique_ptr<ScoredDocumentHitsRanker> scored_document_hits_ranker_in,
      SectionRestrictQueryTermsMap query_terms,
      const SearchSpecProto& search_spec, const ScoringSpecProto& scoring_spec,
      const ResultSpecProto& result_spec, const DocumentStore& document_store);

  ~ResultStateV2();

  // Register num_total_hits_ and add current scored_document_hits_ranker.size()
  // to it. When re-registering, it will subtract
  // scored_document_hits_ranker.size() from the original counter.
  void RegisterNumTotalHits(std::atomic<int>* num_total_hits)
      ICING_EXCLUSIVE_LOCKS_REQUIRED(mutex);

  // Increment the global counter num_total_hits_ by increment_by, if
  // num_total_hits_ has been registered (is not nullptr).
  // Note that providing a negative value for increment_by is a valid usage,
  // which will actually decrement num_total_hits_.
  //
  // It has to be called when we change scored_document_hits_ranker.
  void IncrementNumTotalHits(int increment_by)
      ICING_EXCLUSIVE_LOCKS_REQUIRED(mutex);

  const SnippetContext& snippet_context() const
      ICING_SHARED_LOCKS_REQUIRED(mutex) {
    return snippet_context_;
  }

  const std::unordered_map<std::string, ProjectionTree>& projection_tree_map()
      const ICING_SHARED_LOCKS_REQUIRED(mutex) {
    return projection_tree_map_;
  }

  const std::unordered_map<int32_t, int>& entry_id_group_id_map() const
      ICING_SHARED_LOCKS_REQUIRED(mutex) {
    return entry_id_group_id_map_;
  }

  int num_per_page() const ICING_SHARED_LOCKS_REQUIRED(mutex) {
    return num_per_page_;
  }

  int32_t num_total_bytes_per_page_threshold() const
      ICING_SHARED_LOCKS_REQUIRED(mutex) {
    return num_total_bytes_per_page_threshold_;
  }

  ResultSpecProto::ResultGroupingType result_group_type()
      ICING_SHARED_LOCKS_REQUIRED(mutex) {
    return result_group_type_;
  }

  absl_ports::shared_mutex mutex;

  // When evaluating the next top K hits from scored_document_hits_ranker, some
  // of them may be filtered out by group_result_limits and won't return to the
  // client, so they shouldn't be counted into num_returned. Also the logic of
  // group result limiting depends on retrieval, so it is impossible for
  // ResultState itself to correctly modify these fields. Thus, we make them
  // public, so users of this class can modify them directly.

  // The scored document hits ranker.
  std::unique_ptr<ScoredDocumentHitsRanker> scored_document_hits_ranker
      ICING_GUARDED_BY(mutex);

  // The count of remaining results to return for a group where group id is the
  // index.
  std::vector<int> group_result_limits ICING_GUARDED_BY(mutex);

  // Number of results that have already been returned.
  int num_returned ICING_GUARDED_BY(mutex);

 private:
  // Information needed for snippeting.
  SnippetContext snippet_context_ ICING_GUARDED_BY(mutex);

  // Information needed for projection.
  std::unordered_map<std::string, ProjectionTree> projection_tree_map_
      ICING_GUARDED_BY(mutex);

  // A map between result grouping entry id and the id of the group that it
  // appears in.
  std::unordered_map<int32_t, int> entry_id_group_id_map_
      ICING_GUARDED_BY(mutex);

  // Number of results to return in each page.
  int num_per_page_ ICING_GUARDED_BY(mutex);

  // The threshold of total bytes of all documents to cutoff, in order to limit
  // # of bytes in a single page.
  // Note that it doesn't guarantee the result # of bytes will be smaller, equal
  // to, or larger than the threshold. Instead, it is just a threshold to
  // cutoff, and only guarantees total bytes of search results won't exceed the
  // threshold too much.
  int32_t num_total_bytes_per_page_threshold_ ICING_GUARDED_BY(mutex);

  // Pointer to a global counter to sum up the size of scored_document_hits in
  // all ResultStates.
  // Does not own.
  std::atomic<int>* num_total_hits_ ICING_GUARDED_BY(mutex);

  // Value that the search results will get grouped by.
  ResultSpecProto::ResultGroupingType result_group_type_
      ICING_GUARDED_BY(mutex);
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_RESULT_RESULT_STATE_V2_H_
