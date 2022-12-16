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

#include <memory>

#include "icing/query/advanced_query_parser/lexer.h"
#include "icing/query/advanced_query_parser/parser.h"
#include "icing/scoring/advanced_scoring/score-expression.h"
#include "icing/scoring/advanced_scoring/scoring-visitor.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<std::unique_ptr<AdvancedScorer>>
AdvancedScorer::Create(const ScoringSpecProto& scoring_spec,
                       double default_score,
                       const DocumentStore* document_store,
                       const SchemaStore* schema_store) {
  ICING_RETURN_ERROR_IF_NULL(document_store);
  ICING_RETURN_ERROR_IF_NULL(schema_store);

  Lexer lexer(scoring_spec.advanced_scoring_expression(),
              Lexer::Language::SCORING);
  ICING_ASSIGN_OR_RETURN(std::vector<Lexer::LexerToken> lexer_tokens,
                         lexer.ExtractTokens());
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSIGN_OR_RETURN(std::unique_ptr<Node> tree_root,
                         parser.ConsumeScoring());

  ScoringVisitor visitor(default_score);
  tree_root->Accept(&visitor);

  ICING_ASSIGN_OR_RETURN(std::unique_ptr<ScoreExpression> expression,
                         std::move(visitor).Expression());
  if (expression->is_document_type()) {
    return absl_ports::InvalidArgumentError(
        "The root scoring expression will always be evaluated to a document, "
        "but a number is expected.");
  }
  return std::unique_ptr<AdvancedScorer>(
      new AdvancedScorer(std::move(expression), default_score));
}

}  // namespace lib
}  // namespace icing
