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

#ifndef ICING_SCORING_ADVANCED_SCORING_ADVANCED_SCORER_H_
#define ICING_SCORING_ADVANCED_SCORING_ADVANCED_SCORER_H_

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/feature-flags.h"
#include "icing/index/embed/embedding-query-results.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/join/join-children-fetcher.h"
#include "icing/proto/scoring.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/scoring/advanced_scoring/score-expression.h"
#include "icing/scoring/bm25f-calculator.h"
#include "icing/scoring/scorer.h"
#include "icing/scoring/section-weights.h"
#include "icing/store/document-store.h"
#include "icing/util/logging.h"

namespace icing {
namespace lib {

class AdvancedScorer : public Scorer {
 public:
  // Returns:
  //   A AdvancedScorer instance on success
  //   FAILED_PRECONDITION on any null pointer input
  //   INVALID_ARGUMENT if fails to create an instance
  static libtextclassifier3::StatusOr<std::unique_ptr<AdvancedScorer>> Create(
      const ScoringSpecProto& scoring_spec, double default_score,
      SearchSpecProto::EmbeddingQueryMetricType::Code
          default_semantic_metric_type,
      const DocumentStore* document_store, const SchemaStore* schema_store,
      int64_t current_time_ms, const JoinChildrenFetcher* join_children_fetcher,
      const EmbeddingQueryResults* embedding_query_results,
      const FeatureFlags* feature_flags);

  double GetScore(const DocHitInfo& hit_info,
                  const DocHitInfoIterator* query_it) override {
    return GetScoreFromExpression(score_expression_.get(), hit_info, query_it);
  }

  std::vector<double> GetAdditionalScores(
      const DocHitInfo& hit_info, const DocHitInfoIterator* query_it) override {
    std::vector<double> additional_scores;
    additional_scores.reserve(additional_score_expressions_.size());
    for (const auto& additional_score_expression :
         additional_score_expressions_) {
      additional_scores.push_back(GetScoreFromExpression(
          additional_score_expression.get(), hit_info, query_it));
    }
    return additional_scores;
  }

  void PrepareToScore(
      std::unordered_map<std::string, std::unique_ptr<DocHitInfoIterator>>*
          query_term_iterators) override {
    if (query_term_iterators == nullptr || query_term_iterators->empty()) {
      return;
    }
    bm25f_calculator_->PrepareToScore(query_term_iterators);
  }

  bool is_constant() const { return score_expression_->is_constant(); }

 private:
  explicit AdvancedScorer(
      std::unique_ptr<ScoreExpression> score_expression,
      std::vector<std::unique_ptr<ScoreExpression>>
          additional_score_expressions,
      std::unique_ptr<SectionWeights> section_weights,
      std::unique_ptr<Bm25fCalculator> bm25f_calculator,
      std::unique_ptr<SchemaTypeAliasMap> alias_schema_type_map,
      std::unique_ptr<std::unordered_set<ScoringFeatureType>>
          scoring_feature_types_enabled,
      double default_score)
      : score_expression_(std::move(score_expression)),
        additional_score_expressions_(std::move(additional_score_expressions)),
        section_weights_(std::move(section_weights)),
        bm25f_calculator_(std::move(bm25f_calculator)),
        alias_schema_type_map_(std::move(alias_schema_type_map)),
        scoring_feature_types_enabled_(
            std::move(scoring_feature_types_enabled)),
        default_score_(default_score) {
    if (is_constant()) {
      ICING_LOG(WARNING)
          << "The advanced scoring expression will evaluate to a constant.";
    }
  }

  double GetScoreFromExpression(ScoreExpression* expression,
                                const DocHitInfo& hit_info,
                                const DocHitInfoIterator* query_it) {
    libtextclassifier3::StatusOr<double> result =
        expression->EvaluateDouble(hit_info, query_it);
    if (!result.ok()) {
      ICING_LOG(ERROR) << "Got an error when scoring a document:\n"
                       << result.status().error_message();
      return default_score_;
    }
    return std::move(result).ValueOrDie();
  }

  std::unique_ptr<ScoreExpression> score_expression_;
  // Additional score expressions that are used to return extra helpful scores
  // for clients.
  std::vector<std::unique_ptr<ScoreExpression>> additional_score_expressions_;
  std::unique_ptr<SectionWeights> section_weights_;
  std::unique_ptr<Bm25fCalculator> bm25f_calculator_;
  std::unique_ptr<SchemaTypeAliasMap> alias_schema_type_map_;
  std::unique_ptr<std::unordered_set<ScoringFeatureType>>
      scoring_feature_types_enabled_;
  double default_score_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_SCORING_ADVANCED_SCORING_ADVANCED_SCORER_H_
