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

#include "icing/scoring/scorer.h"

#include <memory>

#include "utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/status_macros.h"
#include "icing/proto/scoring.pb.h"
#include "icing/store/document-associated-score-data.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"

namespace icing {
namespace lib {

class DocumentScoreScorer : public Scorer {
 public:
  explicit DocumentScoreScorer(const DocumentStore* document_store,
                               float default_score)
      : document_store_(*document_store), default_score_(default_score) {}

  float GetScore(DocumentId document_id) override {
    ICING_ASSIGN_OR_RETURN_VAL(
        DocumentAssociatedScoreData score_data,
        document_store_.GetDocumentAssociatedScoreData(document_id),
        default_score_);

    return static_cast<float>(score_data.document_score());
  }

 private:
  const DocumentStore& document_store_;
  float default_score_;
};

class DocumentCreationTimestampScorer : public Scorer {
 public:
  explicit DocumentCreationTimestampScorer(const DocumentStore* document_store,
                                           float default_score)
      : document_store_(*document_store), default_score_(default_score) {}

  float GetScore(DocumentId document_id) override {
    ICING_ASSIGN_OR_RETURN_VAL(
        DocumentAssociatedScoreData score_data,
        document_store_.GetDocumentAssociatedScoreData(document_id),
        default_score_);

    return score_data.creation_timestamp_secs();
  }

 private:
  const DocumentStore& document_store_;
  float default_score_;
};

libtextclassifier3::StatusOr<std::unique_ptr<Scorer>> Scorer::Create(
    ScoringSpecProto::RankingStrategy::Code rank_by, float default_score,
    const DocumentStore* document_store) {
  switch (rank_by) {
    case ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE:
      return std::make_unique<DocumentScoreScorer>(document_store,
                                                   default_score);
    case ScoringSpecProto::RankingStrategy::CREATION_TIMESTAMP:
      return std::make_unique<DocumentCreationTimestampScorer>(document_store,
                                                               default_score);
    case ScoringSpecProto::RankingStrategy::NONE:
      return absl_ports::InvalidArgumentError(
          "RankingStrategy NONE not supported");
  }
}

}  // namespace lib
}  // namespace icing
