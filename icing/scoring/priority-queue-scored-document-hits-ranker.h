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

#ifndef ICING_SCORING_PRIORITY_QUEUE_SCORED_DOCUMENT_HITS_RANKER_H_
#define ICING_SCORING_PRIORITY_QUEUE_SCORED_DOCUMENT_HITS_RANKER_H_

#include <queue>
#include <vector>

#include "icing/scoring/scored-document-hit.h"
#include "icing/scoring/scored-document-hits-ranker.h"

namespace icing {
namespace lib {

// ScoredDocumentHitsRanker interface implementation, based on
// std::priority_queue. We can get next top hit in O(lgN) time.
class PriorityQueueScoredDocumentHitsRanker : public ScoredDocumentHitsRanker {
 public:
  explicit PriorityQueueScoredDocumentHitsRanker(
      const std::vector<ScoredDocumentHit>& scored_document_hits,
      bool is_descending = true);

  ~PriorityQueueScoredDocumentHitsRanker() override = default;

  ScoredDocumentHit PopNext() override;

  void TruncateHitsTo(int new_size) override;

  int size() const override { return scored_document_hits_pq_.size(); }

  bool empty() const override { return scored_document_hits_pq_.empty(); }

 private:
  // Comparator for std::priority_queue. Since std::priority is a max heap
  // (descending order), reverse it if we want ascending order.
  class Comparator {
   public:
    explicit Comparator(bool is_ascending) : is_ascending_(is_ascending) {}

    bool operator()(const ScoredDocumentHit& lhs,
                    const ScoredDocumentHit& rhs) const {
      return is_ascending_ == !(lhs < rhs);
    }

   private:
    bool is_ascending_;
  };

  Comparator comparator_;

  // Use priority queue to get top K hits in O(KlgN) time.
  std::priority_queue<ScoredDocumentHit, std::vector<ScoredDocumentHit>,
                      Comparator>
      scored_document_hits_pq_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_SCORING_PRIORITY_QUEUE_SCORED_DOCUMENT_HITS_RANKER_H_
