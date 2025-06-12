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

#ifndef ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_MATCH_SCORE_EXPRESSION_H_
#define ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_MATCH_SCORE_EXPRESSION_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/index/iterator/doc-hit-info-iterator-all-document-id.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/schema/section.h"
#include "icing/scoring/advanced_scoring/score-expression.h"
#include "icing/store/document-id.h"

namespace icing {
namespace lib {

class DocHitInfoIteratorMatchScoreExpression : public DocHitInfoIterator {
 public:
  explicit DocHitInfoIteratorMatchScoreExpression(
      DocumentId last_added_document_id,
      std::unique_ptr<ScoreExpression> scoring_expression, double score_low,
      double score_high)
      : delegate_(std::make_unique<DocHitInfoIteratorAllDocumentId>(
            last_added_document_id)),
        scoring_expression_(std::move(scoring_expression)),
        score_low_(score_low),
        score_high_(score_high) {};

  libtextclassifier3::Status Advance() override;

  libtextclassifier3::StatusOr<TrimmedNode> TrimRightMostNode() && override {
    // TODO(b/377215223): Decide on the correct behavior for this function when
    // supporting query language optimizations and the delegate is not
    // DocHitInfoIteratorAllDocumentId.
    return absl_ports::InvalidArgumentError(
        "Query suggestions for the matchScoreExpression function are not "
        "supported");
  }

  void MapChildren(const ChildrenMapper& mapper) override {
    delegate_ = mapper(std::move(delegate_));
  }

  CallStats GetCallStats() const override { return delegate_->GetCallStats(); }

  std::string ToString() const override {
    // TODO(b/377215223): Consider having a ToString method for ScoreExpression,
    // so that it can be displayed here.
    return absl_ports::StrCat("(matchScoreExpression: ", delegate_->ToString(),
                              ")");
  }

  void PopulateMatchedTermsStats(
      std::vector<TermMatchInfo>* matched_terms_stats,
      SectionIdMask filtering_section_mask = kSectionIdMaskAll) const override {
    // TODO(b/377215223): Decide on the correct behavior for this function when
    // supporting query language optimizations and the delegate is not
    // DocHitInfoIteratorAllDocumentId.
  }

 private:
  // TODO(b/377215223): Currently, the delegate can only be
  // DocHitInfoIteratorAllDocumentId, but filtering on all documents is
  // inefficient. Consider the following optimizations:
  // 1. Implement a query language optimization so that queries like "foo AND
  //    matchScoreExpression(...)" can be optimized by letting
  //    DocHitInfoIteratorMatchScoreExpression filter on "foo".
  // 2. Apply other filter iterators at the bottom level instead of the top
  //    level, similar to how section restrictions are handled.
  std::unique_ptr<DocHitInfoIterator> delegate_;

  std::unique_ptr<ScoreExpression> scoring_expression_;
  double score_low_;
  double score_high_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_INDEX_ITERATOR_DOC_HIT_INFO_ITERATOR_MATCH_SCORE_EXPRESSION_H_
