// Copyright (C) 2024 Google LLC
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

#include "icing/index/iterator/doc-hit-info-iterator-match-score-expression.h"

#include <memory>
#include <string_view>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/scoring/advanced_scoring/score-expression.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

libtextclassifier3::Status DocHitInfoIteratorMatchScoreExpression::Advance() {
  while (delegate_->Advance().ok()) {
    libtextclassifier3::StatusOr<double> score_or =
        scoring_expression_->EvaluateDouble(delegate_->doc_hit_info(),
                                            /*query_it=*/nullptr);
    if (!score_or.ok()) {
      continue;
    }
    double score = score_or.ValueOrDie();
    if (score < score_low_ || score > score_high_) {
      // Score is outside of the desired range, skip this result.
      continue;
    }
    doc_hit_info_ = delegate_->doc_hit_info();
    return libtextclassifier3::Status::OK;
  }

  // Didn't find anything on the delegate iterator.
  doc_hit_info_ = DocHitInfo(kInvalidDocumentId);
  return absl_ports::ResourceExhaustedError("No more DocHitInfos in iterator");
}

}  // namespace lib
}  // namespace icing
