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
#include "icing/absl_ports/thread_annotations.h"
#include "icing/proto/search.pb.h"
#include "icing/result/result-adjustment-info.h"
#include "icing/result/result-utils.h"
#include "icing/schema/schema-store.h"
#include "icing/scoring/scored-document-hits-ranker.h"
#include "icing/store/document-store.h"

namespace icing {
namespace lib {

// Used to hold information needed across multiple pagination requests of the
// same query. Stored in ResultStateManager.
//
// Each member (except the mutex) belongs to one of the following categories:
// - HITS: matched document information.
// - ADJUSTMENT_INFO: snippet and projection information.
// - GROUPING: result grouping information.
// - PAGINATION: pagination information.
// - OTHER
//
// Each member (except the mutex) will have at least one of the following tags:
// - CONSTANT: The member is constant and will not change after initialization.
// - STATEFUL: The member is stateful.
//   - May be changed when fetching next page. E.g. counters.
//   - Or may be changed when optimizing. E.g. entry_id_group_index_map.
// - NEED_OPTIMIZE: The member contains Icing internal ids or other information
//   that needs to remap during optimization.
// - EXTERNAL_DEP: The member is a dependent (pointer or reference to a
//   variable) outside of the class.
class ResultStateV2 {
 public:
  explicit ResultStateV2(
      std::unique_ptr<ScoredDocumentHitsRanker> scored_document_hits_ranker_in,
      std::unique_ptr<ResultAdjustmentInfo> parent_adjustment_info,
      std::unique_ptr<ResultAdjustmentInfo> child_adjustment_info,
      const ResultSpecProto& result_spec, const SchemaStore& schema_store,
      const DocumentStore& document_store);

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

  int32_t num_per_page() const ICING_SHARED_LOCKS_REQUIRED(mutex) {
    return num_per_page_;
  }

  int32_t num_total_bytes_per_page_threshold() const
      ICING_SHARED_LOCKS_REQUIRED(mutex) {
    return num_total_bytes_per_page_threshold_;
  }

  int32_t max_joined_children_per_parent_to_return() const
      ICING_SHARED_LOCKS_REQUIRED(mutex) {
    return max_joined_children_per_parent_to_return_;
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

  // Category: HITS.
  // The scored document hits ranker.
  //
  // STATEFUL, NEED_OPTIMIZE.
  std::unique_ptr<ScoredDocumentHitsRanker> scored_document_hits_ranker
      ICING_GUARDED_BY(mutex);

  // Category: ADJUSTMENT_INFO.
  // Adjustment information for parent documents, including snippet and
  // projection. Can be nullptr if there is no adjustment info for parent
  // documents.
  //
  // STATEFUL, NEED_OPTIMIZE.
  std::unique_ptr<ResultAdjustmentInfo> parent_adjustment_info
      ICING_GUARDED_BY(mutex);

  // Category: ADJUSTMENT_INFO.
  // Adjustment information for child documents, including snippet and
  // projection. This is only used for join query. Can be nullptr if there is no
  // adjustment info for child documents.
  //
  // STATEFUL, NEED_OPTIMIZE.
  std::unique_ptr<ResultAdjustmentInfo> child_adjustment_info
      ICING_GUARDED_BY(mutex);

  // Category: GROUPING.
  // A map between result grouping entry id and the index of the group that it
  // appears in.
  //
  // STATEFUL, NEED_OPTIMIZE.
  std::unordered_map<result_utils::ResultGroupingEntryId, int>
      entry_id_group_index_map ICING_GUARDED_BY(mutex);

  // Category: GROUPING.
  // The count of remaining results to return for a group. The index is assigned
  // by entry_id_group_index_map_.
  //
  // STATEFUL.
  std::vector<int> group_result_limits ICING_GUARDED_BY(mutex);

  // Category: PAGINATION.
  // Number of results that have already been returned.
  //
  // STATEFUL.
  int num_returned ICING_GUARDED_BY(mutex);

 private:
  // Category: PAGINATION.
  // Number of results to return in each page.
  //
  // CONSTANT.
  int32_t num_per_page_ ICING_GUARDED_BY(mutex);

  // Category: PAGINATION.
  // The threshold of total bytes of all documents to cutoff, in order to limit
  // # of bytes in a single page.
  // Note that it doesn't guarantee the result # of bytes will be smaller, equal
  // to, or larger than the threshold. Instead, it is just a threshold to
  // cutoff, and only guarantees total bytes of search results won't exceed the
  // threshold too much.
  //
  // CONSTANT.
  int32_t num_total_bytes_per_page_threshold_ ICING_GUARDED_BY(mutex);

  // Category: PAGINATION.
  // Max # of joined child documents to be attached in the result for each
  // parent document.
  //
  // CONSTANT.
  int32_t max_joined_children_per_parent_to_return_ ICING_GUARDED_BY(mutex);

  // Category: GROUPING.
  // Value that the search results will get grouped by.
  //
  // CONSTANT.
  ResultSpecProto::ResultGroupingType result_group_type_
      ICING_GUARDED_BY(mutex);

  // Category: OTHER.
  // Pointer to a global counter to sum up the size of scored_document_hits in
  // all ResultStates.
  // Does not own.
  //
  // STATEFUL, EXTERNAL_DEP: scored_document_hits_ranker.
  std::atomic<int>* num_total_hits_ ICING_GUARDED_BY(mutex);
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_RESULT_RESULT_STATE_V2_H_
