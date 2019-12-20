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

#ifndef ICING_SCORING_RANKER_H_
#define ICING_SCORING_RANKER_H_

#include <vector>

#include "icing/scoring/scored-document-hit.h"

// Provides functionality to get the top N results from an unsorted vector.
namespace icing {
namespace lib {

// Returns the top num_result results from scored_document_hits. The returned
// vector will be sorted and contain no more than num_result elements.
// is_descending indicates whether the result is in a descending score order
// or an ascending score order.
std::vector<ScoredDocumentHit> GetTopNFromScoredDocumentHits(
    std::vector<ScoredDocumentHit> scored_document_hits, int num_result,
    bool is_descending);

}  // namespace lib
}  // namespace icing

#endif  // ICING_SCORING_RANKER_H_
