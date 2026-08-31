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

#include "icing/scoring/scored-document-hit.h"

#include <optional>
#include <utility>
#include <vector>

#include "icing/store/document-id.h"
#include "icing/util/document-util.h"

namespace icing {
namespace lib {

JoinedScoredDocumentHit ScoredDocumentHit::Converter::operator()(
    ScoredDocumentHit&& scored_doc_hit) const {
  double final_score = scored_doc_hit.score();
  return JoinedScoredDocumentHit(
      final_score,
      /*parent_scored_document_hit=*/std::move(scored_doc_hit),
      /*child_scored_document_hits=*/{});
}

JoinedScoredDocumentHit ScoredDocumentHit::Converter::operator()(
    const ScoredDocumentHit& scored_doc_hit) const {
  return JoinedScoredDocumentHit(scored_doc_hit.score(),
                                 /*parent_scored_document_hit=*/scored_doc_hit,
                                 /*child_scored_document_hits=*/{});
}

std::optional<ScoredDocumentHit> ScoredDocumentHit::Optimize(
    const std::vector<DocumentId>& document_id_old_to_new) && {
  DocumentId new_doc_id = document_util::GetOptimizedDocumentId(
      document_id_, document_id_old_to_new);
  if (new_doc_id == kInvalidDocumentId) {
    return std::nullopt;
  }

  return std::make_optional<ScoredDocumentHit>(
      new_doc_id, hit_section_id_mask_, score_, std::move(additional_scores_));
}

std::optional<JoinedScoredDocumentHit> JoinedScoredDocumentHit::Optimize(
    const std::vector<DocumentId>& document_id_old_to_new) && {
  std::optional<ScoredDocumentHit> new_parent_scored_doc_hit =
      std::move(parent_scored_document_hit_).Optimize(document_id_old_to_new);
  if (new_parent_scored_doc_hit == std::nullopt) {
    // Exclude this JoinedScoredDocumentHit given that the parent document is
    // invalid after optimization.
    return std::nullopt;
  }

  // Optimize child documents.
  std::vector<ScoredDocumentHit> new_child_scored_doc_hits;
  for (ScoredDocumentHit& child_scored_doc_hit : child_scored_document_hits_) {
    std::optional<ScoredDocumentHit> new_child_scored_doc_hit =
        std::move(child_scored_doc_hit).Optimize(document_id_old_to_new);
    if (new_child_scored_doc_hit == std::nullopt) {
      // The child document is invalid after optimization. Skip it.
      // Note: this will cause the final score to be calculated based on
      //   potentially different number of child documents, but we CANNOT change
      //   the final score during optimization since it may change the ranking
      //   order of the results.
      //
      //   This may cause some minor inconsistency for the final score, but it's
      //   the best we can do.
      continue;
    }
    new_child_scored_doc_hits.push_back(std::move(*new_child_scored_doc_hit));
  }

  return std::make_optional<JoinedScoredDocumentHit>(
      final_score_, std::move(*new_parent_scored_doc_hit),
      std::move(new_child_scored_doc_hits));
}

}  // namespace lib
}  // namespace icing
