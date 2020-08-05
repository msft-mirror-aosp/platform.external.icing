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

#include "icing/proto/search.pb.h"
#include "icing/util/clock.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

ResultStateManager::ResultStateManager(int max_hits_per_query,
                                       int max_result_states)
    : max_hits_per_query_(max_hits_per_query),
      max_result_states_(max_result_states),
      random_generator_(GetSteadyTimeNanoseconds()) {}

libtextclassifier3::StatusOr<PageResultState>
ResultStateManager::RankAndPaginate(ResultState result_state) {
  if (!result_state.HasMoreResults()) {
    return absl_ports::InvalidArgumentError("ResultState has no results");
  }

  // Truncates scored document hits so that they don't take up too much space.
  result_state.TruncateHitsTo(max_hits_per_query_);

  // Gets the number before calling GetNextPage() because num_returned() may
  // change after returning more results.
  int num_previously_returned = result_state.num_returned();

  std::vector<ScoredDocumentHit> page_result_document_hits =
      result_state.GetNextPage();

  if (!result_state.HasMoreResults()) {
    // No more pages, won't store ResultState, returns directly
    return PageResultState(
        std::move(page_result_document_hits), kInvalidNextPageToken,
        result_state.snippet_context(), num_previously_returned);
  }

  absl_ports::unique_lock l(&mutex_);

  // ResultState has multiple pages, storing it
  SnippetContext snippet_context_copy = result_state.snippet_context();
  uint64_t next_page_token = Add(std::move(result_state));

  return PageResultState(std::move(page_result_document_hits), next_page_token,
                         std::move(snippet_context_copy),
                         num_previously_returned);
}

uint64_t ResultStateManager::Add(ResultState result_state) {
  RemoveStatesIfNeeded();

  uint64_t new_token = GetUniqueToken();

  result_state_map_.emplace(new_token, std::move(result_state));
  // Tracks the insertion order
  token_queue_.push(new_token);

  return new_token;
}

libtextclassifier3::StatusOr<PageResultState> ResultStateManager::GetNextPage(
    uint64_t next_page_token) {
  absl_ports::unique_lock l(&mutex_);

  const auto& state_iterator = result_state_map_.find(next_page_token);
  if (state_iterator == result_state_map_.end()) {
    return absl_ports::NotFoundError("next_page_token not found");
  }

  int num_returned = state_iterator->second.num_returned();
  std::vector<ScoredDocumentHit> result_of_page =
      state_iterator->second.GetNextPage();
  if (result_of_page.empty()) {
    // This shouldn't happen, all our active states should contain results, but
    // a sanity check here in case of any data inconsistency.
    InternalInvalidateResultState(next_page_token);
    return absl_ports::NotFoundError(
        "No more results, token has been invalidated.");
  }

  // Copies the SnippetContext in case the ResultState is invalidated.
  SnippetContext snippet_context_copy =
      state_iterator->second.snippet_context();

  if (!state_iterator->second.HasMoreResults()) {
    InternalInvalidateResultState(next_page_token);
  }

  return PageResultState(result_of_page, next_page_token,
                         std::move(snippet_context_copy), num_returned);
}

void ResultStateManager::InvalidateResultState(uint64_t next_page_token) {
  if (next_page_token == kInvalidNextPageToken) {
    return;
  }

  absl_ports::unique_lock l(&mutex_);

  InternalInvalidateResultState(next_page_token);
}

void ResultStateManager::InvalidateAllResultStates() {
  absl_ports::unique_lock l(&mutex_);

  result_state_map_.clear();
  invalidated_token_set_.clear();
  token_queue_ = {};
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

void ResultStateManager::RemoveStatesIfNeeded() {
  if (result_state_map_.empty() || token_queue_.empty()) {
    return;
  }

  // Removes any tokens that were previously invalidated.
  while (!token_queue_.empty() &&
         invalidated_token_set_.find(token_queue_.front()) !=
             invalidated_token_set_.end()) {
    invalidated_token_set_.erase(token_queue_.front());
    token_queue_.pop();
  }

  // Removes the oldest state
  if (result_state_map_.size() >= max_result_states_ && !token_queue_.empty()) {
    result_state_map_.erase(token_queue_.front());
    token_queue_.pop();
  }
}

void ResultStateManager::InternalInvalidateResultState(uint64_t token) {
  // Removes the entry in result_state_map_ and insert the token into
  // invalidated_token_set_. The entry in token_queue_ can't be easily removed
  // right now (may need O(n) time), so we leave it there and later completely
  // remove the token in RemoveStatesIfNeeded().
  if (result_state_map_.erase(token) > 0) {
    invalidated_token_set_.insert(token);
  }
}

}  // namespace lib
}  // namespace icing
