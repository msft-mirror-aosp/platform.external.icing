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

#include "icing/scoring/advanced_scoring/advanced-scorer.h"

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/feature-flags.h"
#include "icing/index/embed/embedding-query-results.h"
#include "icing/join/join-children-fetcher.h"
#include "icing/proto/scoring.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/scoring/advanced_scoring/score-expression-util.h"
#include "icing/scoring/advanced_scoring/score-expression.h"
#include "icing/scoring/bm25f-calculator.h"
#include "icing/scoring/section-weights.h"
#include "icing/store/document-store.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

std::unique_ptr<SchemaTypeAliasMap> BuildSchemaTypeAliasMap(
    const ScoringSpecProto& scoring_spec) {
  std::unique_ptr<SchemaTypeAliasMap> schema_type_alias_map =
      std::make_unique<SchemaTypeAliasMap>();
  for (const SchemaTypeAliasMapProto& alias_map_proto :
       scoring_spec.schema_type_alias_map_protos()) {
    (*schema_type_alias_map)[alias_map_proto.alias_schema_type()] =
        std::unordered_set<std::string>(alias_map_proto.schema_types().begin(),
                                        alias_map_proto.schema_types().end());
  }
  return schema_type_alias_map;
}

}  // namespace

libtextclassifier3::StatusOr<std::unique_ptr<AdvancedScorer>>
AdvancedScorer::Create(const ScoringSpecProto& scoring_spec,
                       double default_score,
                       SearchSpecProto::EmbeddingQueryMetricType::Code
                           default_semantic_metric_type,
                       const DocumentStore* document_store,
                       const SchemaStore* schema_store, int64_t current_time_ms,
                       const JoinChildrenFetcher* join_children_fetcher,
                       const EmbeddingQueryResults* embedding_query_results,
                       const FeatureFlags* feature_flags) {
  ICING_RETURN_ERROR_IF_NULL(document_store);
  ICING_RETURN_ERROR_IF_NULL(schema_store);
  ICING_RETURN_ERROR_IF_NULL(embedding_query_results);
  ICING_RETURN_ERROR_IF_NULL(feature_flags);

  ICING_ASSIGN_OR_RETURN(std::unique_ptr<SectionWeights> section_weights,
                         SectionWeights::Create(schema_store, scoring_spec));
  std::unique_ptr<Bm25fCalculator> bm25f_calculator =
      std::make_unique<Bm25fCalculator>(document_store, section_weights.get(),
                                        current_time_ms);
  std::unique_ptr<SchemaTypeAliasMap> schema_type_alias_map =
      BuildSchemaTypeAliasMap(scoring_spec);
  std::unique_ptr<std::unordered_set<ScoringFeatureType>>
      scoring_feature_types_enabled =
          score_expression_util::GetEnabledScoringFeatureTypes(scoring_spec);

  ICING_ASSIGN_OR_RETURN(
      std::unique_ptr<ScoreExpression> score_expression,
      score_expression_util::GetScoreExpression(
          scoring_spec.advanced_scoring_expression(), default_score,
          default_semantic_metric_type, document_store, schema_store,
          current_time_ms, join_children_fetcher, embedding_query_results,
          section_weights.get(), bm25f_calculator.get(),
          schema_type_alias_map.get(), feature_flags,
          scoring_feature_types_enabled.get()));

  std::vector<std::unique_ptr<ScoreExpression>> additional_score_expressions;
  additional_score_expressions.reserve(
      scoring_spec.additional_advanced_scoring_expressions_size());
  for (const auto& additional_expression :
       scoring_spec.additional_advanced_scoring_expressions()) {
    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<ScoreExpression> additional_score_expression,
        score_expression_util::GetScoreExpression(
            additional_expression, default_score, default_semantic_metric_type,
            document_store, schema_store, current_time_ms,
            join_children_fetcher, embedding_query_results,
            section_weights.get(), bm25f_calculator.get(),
            schema_type_alias_map.get(), feature_flags,
            scoring_feature_types_enabled.get()));

    additional_score_expressions.push_back(
        std::move(additional_score_expression));
  }
  return std::unique_ptr<AdvancedScorer>(new AdvancedScorer(
      std::move(score_expression), std::move(additional_score_expressions),
      std::move(section_weights), std::move(bm25f_calculator),
      std::move(schema_type_alias_map),
      std::move(scoring_feature_types_enabled), default_score));
}

}  // namespace lib
}  // namespace icing
