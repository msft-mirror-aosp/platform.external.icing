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

#include "icing/result/result-state-manager.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <queue>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/mutex.h"
#include "icing/proto/logging.pb.h"
#include "icing/result/page-result.h"
#include "icing/result/result-adjustment-info.h"
#include "icing/result/result-retriever-v2.h"
#include "icing/result/result-state-v2.h"
#include "icing/scoring/scored-document-hits-ranker.h"
#include "icing/store/document-store.h"
#include "icing/util/clock.h"
#include "icing/util/logging.h"

namespace icing {
namespace lib {

ResultStateManager::ResultStateManager(int max_total_hits,
                                       const DocumentStore& document_store)
    : document_store_(document_store),
      max_total_hits_(max_total_hits),
      num_total_hits_(0),
      random_generator_(GetSteadyTimeNanoseconds()) {}

libtextclassifier3::StatusOr<std::pair<uint64_t, PageResult>>
ResultStateManager::CacheAndRetrieveFirstPage(
    std::unique_ptr<ScoredDocumentHitsRanker> ranker,
    std::unique_ptr<ResultAdjustmentInfo> parent_adjustment_info,
    std::unique_ptr<ResultAdjustmentInfo> child_adjustment_info,
    const ResultSpecProto& result_spec, const DocumentStore& document_store,
    const ResultRetrieverV2& result_retriever, int64_t current_time_ms,
    QueryStatsProto* query_stats) {
  if (ranker == nullptr) {
    return absl_ports::InvalidArgumentError("Should not provide null ranker");
  }

  // Create shared pointer of ResultState.
  // ResultState should be created by ResultStateManager only.
  std::shared_ptr<ResultStateV2> result_state = std::make_shared<ResultStateV2>(
      std::move(ranker), std::move(parent_adjustment_info),
      std::move(child_adjustment_info), result_spec, document_store);

  // Retrieve docs outside of ResultStateManager critical section.
  // Will enter ResultState critical section inside ResultRetriever.
  auto [page_result, has_more_results] = result_retriever.RetrieveNextPage(
      *result_state,
      /*max_results=*/std::numeric_limits<int32_t>::max(), current_time_ms);
  if (!has_more_results) {
    // No more pages, won't store ResultState, returns directly
    return std::make_pair(kInvalidNextPageToken, std::move(page_result));
  }

  // ResultState has multiple pages, storing it
  {
    // ResultState critical section
    absl_ports::unique_lock l(&result_state->mutex);

    result_state->scored_document_hits_ranker->TruncateHitsTo(max_total_hits_);
    result_state->RegisterNumTotalHits(&num_total_hits_);
  }

  // It is fine to exit ResultState critical section, since it is just created
  // above and only this thread (this call stack) has access to it. Thus, it
  // won't be changed during the gap before we enter ResultStateManager critical
  // section.
  uint64_t next_page_token = kInvalidNextPageToken;
  {
    // ResultStateManager critical section
    absl_ports::unique_lock l(&mutex_);

    // Remove expired result states first.
    InternalRemoveExpiredResultStates(kDefaultResultStateTtlInMs,
                                      current_time_ms);
    // Remove states to make room for this new state.
    RemoveStatesIfNeeded(query_stats);
    // Generate a new unique token and add it into result_state_map_.
    next_page_token = Add(std::move(result_state), current_time_ms);
  }

  return std::make_pair(next_page_token, std::move(page_result));
}

uint64_t ResultStateManager::Add(std::shared_ptr<ResultStateV2> result_state,
                                 int64_t current_time_ms) {
  uint64_t new_token = GetUniqueToken();

  result_state_map_.emplace(new_token, std::move(result_state));
  // Tracks the insertion order
  token_queue_.push(TokenInfo(new_token, current_time_ms));

  return new_token;
}

libtextclassifier3::StatusOr<std::pair<uint64_t, PageResult>>
ResultStateManager::GetNextPage(uint64_t next_page_token, int32_t max_results,
                                const ResultRetrieverV2& result_retriever,
                                int64_t current_time_ms) {
  std::shared_ptr<ResultStateV2> result_state = nullptr;
  {
    // ResultStateManager critical section
    absl_ports::unique_lock l(&mutex_);

    // Remove expired result states before fetching
    InternalRemoveExpiredResultStates(kDefaultResultStateTtlInMs,
                                      current_time_ms);

    const auto& state_iterator = result_state_map_.find(next_page_token);
    if (state_iterator == result_state_map_.end()) {
      return absl_ports::NotFoundError("next_page_token not found");
    }
    result_state = state_iterator->second;
  }

  // Retrieve docs outside of ResultStateManager critical section.
  // Will enter ResultState critical section inside ResultRetriever.
  auto [page_result, has_more_results] = result_retriever.RetrieveNextPage(
      *result_state, max_results, current_time_ms);

  if (!has_more_results) {
    {
      // ResultStateManager critical section
      absl_ports::unique_lock l(&mutex_);

      InternalInvalidateResultState(next_page_token);
    }

    next_page_token = kInvalidNextPageToken;
  }
  return std::make_pair(next_page_token, std::move(page_result));
}

int ResultStateManager::GetNumActiveResultStates(int64_t current_time_ms) {
  absl_ports::unique_lock l(&mutex_);

  InternalRemoveExpiredResultStates(kDefaultResultStateTtlInMs,
                                    current_time_ms);
  return static_cast<int>(result_state_map_.size());
}

void ResultStateManager::InvalidateResultState(uint64_t next_page_token) {
  if (next_page_token == kInvalidNextPageToken) {
    return;
  }

  absl_ports::unique_lock l(&mutex_);

  InternalInvalidateResultState(next_page_token);
}

ResultStateManager::TokenRemovalStats
ResultStateManager::RemoveAllResultStates() {
  absl_ports::unique_lock l(&mutex_);

  return InternalRemoveAllResultStates();
}

ResultStateManager::TokenRemovalStats
ResultStateManager::InternalRemoveAllResultStates() {
  TokenRemovalStats removal_stats = {
      .num_active_tokens_removed = static_cast<int>(result_state_map_.size()),
      .num_invalidated_tokens_removed =
          static_cast<int>(invalidated_token_set_.size())};

  // We don't have to reset num_total_hits_ (to 0) here, since clearing
  // result_state_map_ will "eventually" invoke the destructor of ResultState
  // (which decrements num_total_hits_) and num_total_hits_ will become 0.
  result_state_map_.clear();
  invalidated_token_set_.clear();
  token_queue_ = std::queue<TokenInfo>();

  return removal_stats;
}

uint64_t ResultStateManager::GetUniqueToken() {
  uint64_t new_token = random_generator_();
  // There's a small chance of collision between the random numbers, here we're
  // trying to avoid any collisions by checking the keys.
  while (result_state_map_.find(new_token) != result_state_map_.end() ||
         invalidated_token_set_.find(new_token) !=
             invalidated_token_set_.end() ||
         new_token == kInvalidNextPageToken) {
    new_token = random_generator_();
  }
  return new_token;
}

void ResultStateManager::RemoveStatesIfNeeded(QueryStatsProto* query_stats) {
  if (result_state_map_.empty() || token_queue_.empty()) {
    return;
  }

  // If we're over budget, remove states from oldest to newest until we fit into
  // our budget.
  //
  // Note:
  // - The corresponding ResultState of the front token may have already been
  //   invalidated previously (removed from result_state_map_ and added to
  //   invalidated_token_set_). In this case:
  //   - num_total_hits_ was likely to be decremented already.
  //   - Removing the front token from token_queue_ in this round will not
  //     affect num_total_hits_, so we might need to remove more states.
  // - If the corresponding ResultState of the front token is still active:
  //   - num_total_hits_ may still not be decremented immediately after
  //     removing the ResultState from result_state_map_ , since other threads
  //     may still hold the shared pointer.
  //   - Thus, we have to check if token_queue_ is empty or not, since it is
  //     possible that num_total_hits_ is non-zero and still greater than
  //     max_total_hits_ when token_queue_ is empty. Still "eventually" it will
  //     be decremented after the last thread releases the shared pointer.
  TokenRemovalStats removal_stats;
  while (!token_queue_.empty() && num_total_hits_ > max_total_hits_) {
    ICING_LOG(WARNING) << "Evicting result state from token_queue_ due to "
                          "budget limit. Current num_total_hits_: "
                       << num_total_hits_;
    removal_stats += InternalRemoveFrontToken();
  }

  if (removal_stats.num_active_tokens_removed > 0) {
    ICING_LOG(WARNING) << "Evicted " << removal_stats.num_active_tokens_removed
                       << " active states. After eviction: " << num_total_hits_
                       << " hits and " << token_queue_.size() << " states.";
    if (query_stats != nullptr) {
      query_stats->set_num_result_states_evicted(
          removal_stats.num_active_tokens_removed);
    }
  }
}

void ResultStateManager::InternalInvalidateResultState(uint64_t token) {
  // Removes the entry in result_state_map_ and insert the token into
  // invalidated_token_set_. The entry in token_queue_ can't be easily removed
  // right now (may need O(n) time), so we leave it there and later completely
  // remove the token in RemoveStatesIfNeeded().
  auto itr = result_state_map_.find(token);
  if (itr != result_state_map_.end()) {
    // We don't have to decrement num_total_hits_ here, since erasing the shared
    // ptr instance will "eventually" invoke the destructor of ResultState and
    // it will handle this.
    result_state_map_.erase(itr);
    invalidated_token_set_.insert(token);
  }
}

ResultStateManager::TokenRemovalStats
ResultStateManager::InternalRemoveFrontToken() {
  TokenRemovalStats removal_stats;
  if (token_queue_.empty()) {
    return removal_stats;
  }

  // The front token should be in either result_state_map_ or
  // invalidated_token_set_.
  //
  // NOTE: we don't have to decrement num_total_hits_ if removing result state
  // from result_state_map_, since erasing the shared ptr instance will
  // "eventually" invoke the destructor of ResultState and it will handle this.
  auto itr_map = result_state_map_.find(token_queue_.front().token);
  auto itr_invalidated =
      invalidated_token_set_.find(token_queue_.front().token);
  if (itr_map != result_state_map_.end() &&
      itr_invalidated != invalidated_token_set_.end()) {
    // This should never happen, unless there is a bug in our code that causes
    // token collision.
    ICING_LOG(ERROR) << "Token " << token_queue_.front().token
                     << " is in both result_state_map_ and "
                        "invalidated_token_set_. This should never happen.";
    result_state_map_.erase(itr_map);
    invalidated_token_set_.erase(itr_invalidated);
    ++removal_stats.num_active_tokens_removed;
    ++removal_stats.num_invalidated_tokens_removed;
  } else if (itr_map != result_state_map_.end()) {
    result_state_map_.erase(itr_map);
    ++removal_stats.num_active_tokens_removed;
  } else if (itr_invalidated != invalidated_token_set_.end()) {
    invalidated_token_set_.erase(itr_invalidated);
    ++removal_stats.num_invalidated_tokens_removed;
  } else {
    // This should never happen.
    ICING_LOG(ERROR) << "Token " << token_queue_.front().token
                     << " is not in either result_state_map_ or "
                        "invalidated_token_set_. This should never happen.";
  }

  token_queue_.pop();
  return removal_stats;
}

ResultStateManager::TokenRemovalStats
ResultStateManager::InternalRemoveExpiredResultStates(int64_t result_state_ttl,
                                                      int64_t current_time_ms) {
  TokenRemovalStats removal_stats;
  while (!token_queue_.empty() &&
         current_time_ms - token_queue_.front().creation_timestamp_ms >=
             result_state_ttl) {
    removal_stats += InternalRemoveFrontToken();
  }
  ICING_VLOG(1) << "Removed " << removal_stats.num_active_tokens_removed
                << " expired tokens and "
                << removal_stats.num_invalidated_tokens_removed
                << " tokens that were already invalidated before expiration.";
  return removal_stats;
}

}  // namespace lib
}  // namespace icing
