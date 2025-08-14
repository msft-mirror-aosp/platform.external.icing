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

#ifndef ICING_SCORING_ADVANCED_SCORING_SCORE_EXPRESSION_UTIL_H_
#define ICING_SCORING_ADVANCED_SCORING_SCORE_EXPRESSION_UTIL_H_

#include <cstdint>
#include <memory>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/feature-flags.h"
#include "icing/index/embed/embedding-query-results.h"
#include "icing/join/join-children-fetcher.h"
#include "icing/proto/scoring.pb.h"
#include "icing/query/advanced_query_parser/abstract-syntax-tree.h"
#include "icing/query/advanced_query_parser/lexer.h"
#include "icing/query/advanced_query_parser/parser.h"
#include "icing/schema/schema-store.h"
#include "icing/scoring/advanced_scoring/score-expression.h"
#include "icing/scoring/advanced_scoring/scoring-visitor.h"
#include "icing/scoring/bm25f-calculator.h"
#include "icing/scoring/section-weights.h"
#include "icing/store/document-store.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {
namespace score_expression_util {

// Returns a ScoreExpression instance for the given scoring expression.
//
// join_children_fetcher, embedding_query_results, section_weights,
// bm25f_calculator, schema_type_alias_map are allowed to be nullptr if the
// corresponding scoring expression does not use them.
//
// Returns:
//   - A ScoreExpression instance on success.
//   - Any syntax or semantics error from Lexer, Parser, or ScoringVisitor.
inline libtextclassifier3::StatusOr<std::unique_ptr<ScoreExpression>>
GetScoreExpression(std::string_view scoring_expression, double default_score,
                   SearchSpecProto::EmbeddingQueryMetricType::Code
                       default_semantic_metric_type,
                   const DocumentStore* document_store,
                   const SchemaStore* schema_store, int64_t current_time_ms,
                   const JoinChildrenFetcher* join_children_fetcher,
                   const EmbeddingQueryResults* embedding_query_results,
                   SectionWeights* section_weights,
                   Bm25fCalculator* bm25f_calculator,
                   const SchemaTypeAliasMap* schema_type_alias_map,
                   const FeatureFlags* feature_flags,
                   const std::unordered_set<ScoringFeatureType>*
                       scoring_feature_types_enabled) {
  ICING_RETURN_ERROR_IF_NULL(document_store);
  ICING_RETURN_ERROR_IF_NULL(schema_store);
  ICING_RETURN_ERROR_IF_NULL(feature_flags);

  Lexer lexer(scoring_expression, Lexer::Language::SCORING);
  ICING_ASSIGN_OR_RETURN(std::vector<Lexer::LexerToken> lexer_tokens,
                         std::move(lexer).ExtractTokens());
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<Node> tree_root,
                         parser.ConsumeScoring());
  ScoringVisitor visitor(
      default_score, default_semantic_metric_type, document_store, schema_store,
      section_weights, bm25f_calculator, join_children_fetcher,
      embedding_query_results, schema_type_alias_map, feature_flags,
      scoring_feature_types_enabled, current_time_ms);
  tree_root->Accept(&visitor);

  ICING_ASSIGN_OR_RETURN(std::unique_ptr<ScoreExpression> expression,
                         std::move(visitor).Expression());
  if (expression->type() != ScoreExpressionType::kDouble) {
    return absl_ports::InvalidArgumentError(
        "The root scoring expression is not of double type.");
  }
  return expression;
}

inline std::unique_ptr<std::unordered_set<ScoringFeatureType>>
GetEnabledScoringFeatureTypes(const ScoringSpecProto& scoring_spec) {
  auto scoring_feature_types_enabled =
      std::make_unique<std::unordered_set<ScoringFeatureType>>();
  for (int feature_type : scoring_spec.scoring_feature_types_enabled()) {
    scoring_feature_types_enabled->insert(
        static_cast<ScoringFeatureType>(feature_type));
  }
  return scoring_feature_types_enabled;
}

}  // namespace score_expression_util

}  // namespace lib
}  // namespace icing

#endif  // ICING_SCORING_ADVANCED_SCORING_SCORE_EXPRESSION_UTIL_H_
