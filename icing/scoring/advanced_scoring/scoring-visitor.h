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

#ifndef THIRD_PARTY_ICING_SCORING_ADVANCED_SCORING_SCORING_VISITOR_H_
#define THIRD_PARTY_ICING_SCORING_ADVANCED_SCORING_SCORING_VISITOR_H_

#include <cstdint>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/status.h"
#include "knowledge/cerebra/sense/text_classifier/lib3/utils/base/statusor.h"
#include "third_party/icing/absl_ports/canonical_errors.h"
#include "third_party/icing/feature-flags.h"
#include "third_party/icing/index/embed/embedding-query-results.h"
#include "third_party/icing/join/join-children-fetcher.h"
#include "third_party/icing/legacy/core/icing-string-util.h"
#include "third_party/icing/proto/scoring.proto.h"
#include "third_party/icing/query/advanced_query_parser/abstract-syntax-tree.h"
#include "third_party/icing/schema/schema-store.h"
#include "third_party/icing/scoring/advanced_scoring/score-expression.h"
#include "third_party/icing/scoring/bm25f-calculator.h"
#include "third_party/icing/scoring/section-weights.h"
#include "third_party/icing/store/document-store.h"

namespace icing {
namespace lib {

class ScoringVisitor : public AbstractSyntaxTreeVisitor {
 public:
  // join_children_fetcher, embedding_query_results, section_weights,
  // bm25f_calculator, schema_type_alias_map are allowed to be nullptr if the
  // corresponding scoring expression does not use them.
  explicit ScoringVisitor(double default_score,
                          SearchSpecProto::EmbeddingQueryMetricType::Code
                              default_semantic_metric_type,
                          const DocumentStore* document_store,
                          const SchemaStore* schema_store,
                          SectionWeights* section_weights,
                          Bm25fCalculator* bm25f_calculator,
                          const JoinChildrenFetcher* join_children_fetcher,
                          const EmbeddingQueryResults* embedding_query_results,
                          const SchemaTypeAliasMap* schema_type_alias_map,
                          const FeatureFlags* feature_flags,
                          const std::unordered_set<ScoringFeatureType>*
                              scoring_feature_types_enabled,
                          int64_t current_time_ms)
      : default_score_(default_score),
        default_semantic_metric_type_(default_semantic_metric_type),
        document_store_(*document_store),
        schema_store_(*schema_store),
        section_weights_(section_weights),
        bm25f_calculator_(bm25f_calculator),
        join_children_fetcher_(join_children_fetcher),
        embedding_query_results_(embedding_query_results),
        schema_type_alias_map_(schema_type_alias_map),
        feature_flags_(*feature_flags),
        scoring_feature_types_enabled_(*scoring_feature_types_enabled),
        current_time_ms_(current_time_ms) {}

  void VisitString(const StringNode* node) override;
  void VisitText(const TextNode* node) override;
  void VisitMember(const MemberNode* node) override;

  void VisitFunction(const FunctionNode* node) override {
    return VisitFunctionHelper(node, /*is_member_function=*/false);
  }

  void VisitUnaryOperator(const UnaryOperatorNode* node) override;
  void VisitNaryOperator(const NaryOperatorNode* node) override;

  // RETURNS:
  //   - An ScoreExpression instance able to evaluate the expression on success.
  //   - INVALID_ARGUMENT if the AST does not conform to supported expressions,
  //   such as type errors.
  //   - INTERNAL if there are inconsistencies.
  libtextclassifier3::StatusOr<std::unique_ptr<ScoreExpression>>
  Expression() && {
    if (has_pending_error()) {
      return pending_error_;
    }
    if (stack_.size() != 1) {
      return absl_ports::InternalError(IcingStringUtil::StringPrintf(
          "Expect to get only one result from "
          "ScoringVisitor, but got %zu. There must be inconsistencies.",
          stack_.size()));
    }
    return std::move(stack_[0]);
  }

 private:
  // Visit function node. If is_member_function is true, a ThisExpression will
  // be added as the first function argument.
  void VisitFunctionHelper(const FunctionNode* node, bool is_member_function);

  bool has_pending_error() const { return !pending_error_.ok(); }

  std::unique_ptr<ScoreExpression> pop_stack() {
    std::unique_ptr<ScoreExpression> result = std::move(stack_.back());
    stack_.pop_back();
    return result;
  }

  double default_score_;
  const SearchSpecProto::EmbeddingQueryMetricType::Code
      default_semantic_metric_type_;
  const DocumentStore& document_store_;  // Does not own.
  const SchemaStore& schema_store_;      // Does not own.

  SectionWeights* section_weights_;    // nullable, does not own.
  Bm25fCalculator* bm25f_calculator_;  // nullable, does not own.
  // A non-null join_children_fetcher_ indicates scoring in a join.
  const JoinChildrenFetcher* join_children_fetcher_;  // nullable, does not own.
  const EmbeddingQueryResults*
      embedding_query_results_;                      // nullable, does not own.
  const SchemaTypeAliasMap* schema_type_alias_map_;  // nullable, does not own.

  const FeatureFlags& feature_flags_;  // Does not own.
  const std::unordered_set<ScoringFeatureType>&
      scoring_feature_types_enabled_;  // Does not own.

  libtextclassifier3::Status pending_error_;
  std::vector<std::unique_ptr<ScoreExpression>> stack_;
  int64_t current_time_ms_;
};

}  // namespace lib
}  // namespace icing

#endif  // THIRD_PARTY_ICING_SCORING_ADVANCED_SCORING_SCORING_VISITOR_H_
