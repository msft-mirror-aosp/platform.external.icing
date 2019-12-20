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

#ifndef ICING_SCORING_SCORER_H_
#define ICING_SCORING_SCORER_H_

#include <memory>

#include "utils/base/statusor.h"
#include "icing/proto/scoring.pb.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"

namespace icing {
namespace lib {

// Scorer calculates scores for documents.
class Scorer {
 public:
  virtual ~Scorer() = default;

  // Factory function to create a Scorer according to the ranking strategy and
  // default score. The default score will be returned only if the scorer fails
  // to find or calculate a score for the document.
  //
  // Returns:
  //   A Scorer on success
  //   INVALID_ARGUMENT if fails to create an instance
  static libtextclassifier3::StatusOr<std::unique_ptr<Scorer>> Create(
      ScoringSpecProto::RankingStrategy::Code rank_by, float default_score,
      const DocumentStore* document_store);

  // Returns a non-negative score of a document. The score can be a
  // document-associated score which comes from the DocumentProto directly, an
  // accumulated score, or even an inferred score. If it fails to find or
  // calculate a score, the user-provided default score will be returned.
  //
  // Some examples of possible scores:
  // 1. Document-associated scores: document score, creation timestamp score.
  // 2. Accumulated scores: usage count score.
  // 3. Inferred scores: a score calculated by a machine learning model.
  //
  // NOTE: This method is performance-sensitive as it's called for every
  // potential result document. We're trying to avoid returning StatusOr<float>
  // to save a little more time and memory.
  virtual float GetScore(DocumentId document_id) = 0;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_SCORING_SCORER_H_
