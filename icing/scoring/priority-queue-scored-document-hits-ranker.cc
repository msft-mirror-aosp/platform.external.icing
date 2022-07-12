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

#include "icing/scoring/priority-queue-scored-document-hits-ranker.h"

#include <queue>
#include <vector>

#include "icing/scoring/scored-document-hit.h"

namespace icing {
namespace lib {

PriorityQueueScoredDocumentHitsRanker::PriorityQueueScoredDocumentHitsRanker(
    const std::vector<ScoredDocumentHit>& scored_document_hits,
    bool is_descending)
    : comparator_(/*is_ascending=*/!is_descending),
      scored_document_hits_pq_(scored_document_hits.begin(),
                               scored_document_hits.end(), comparator_) {}

ScoredDocumentHit PriorityQueueScoredDocumentHitsRanker::PopNext() {
  ScoredDocumentHit ret = scored_document_hits_pq_.top();
  scored_document_hits_pq_.pop();
  return ret;
}

void PriorityQueueScoredDocumentHitsRanker::TruncateHitsTo(int new_size) {
  if (new_size < 0 || scored_document_hits_pq_.size() <= new_size) {
    return;
  }

  // Copying the best new_size results.
  std::priority_queue<ScoredDocumentHit, std::vector<ScoredDocumentHit>,
                      Comparator>
      new_pq(comparator_);
  for (int i = 0; i < new_size; ++i) {
    new_pq.push(scored_document_hits_pq_.top());
    scored_document_hits_pq_.pop();
  }
  scored_document_hits_pq_ = std::move(new_pq);
}

}  // namespace lib
}  // namespace icing
