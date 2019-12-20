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

#ifndef ICING_SCORING_SCORING_PROCESSOR_H_
#define ICING_SCORING_SCORING_PROCESSOR_H_

#include <memory>
#include <utility>
#include <vector>

#include "utils/base/statusor.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/proto/scoring.pb.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/scoring/scorer.h"
#include "icing/store/document-store.h"

namespace icing {
namespace lib {

// ScoringProcessor is the top-level class that handles scoring.
class ScoringProcessor {
 public:
  // Factory function to create a Scorer with its subcomponents according to the
  // scoring spec.
  //
  // Returns:
  //   A Scorer on success
  //   INVALID_ARGUMENT if unable to create what the spec specifies
  static libtextclassifier3::StatusOr<std::unique_ptr<ScoringProcessor>> Create(
      const ScoringSpecProto& scoring_spec,
      const DocumentStore* document_store);

  // Returns a vector of ScoredDocHit sorted by their scores. The size of it is
  // no more than num_to_return.
  std::vector<ScoredDocumentHit> ScoreAndRank(
      std::unique_ptr<DocHitInfoIterator> doc_hit_info_iterator,
      int num_to_return);

 private:
  explicit ScoringProcessor(std::unique_ptr<Scorer> scorer, bool is_descending)
      : scorer_(std::move(scorer)), is_descending_(is_descending) {}

  std::unique_ptr<Scorer> scorer_;

  // If true, the final result will be sorted in a descending order, otherwise
  // ascending.
  bool is_descending_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_SCORING_SCORING_PROCESSOR_H_
