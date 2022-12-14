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

#include "icing/scoring/advanced_scoring/scoring-visitor.h"

#include "icing/absl_ports/str_cat.h"

namespace icing {
namespace lib {

void ScoringVisitor::VisitFunctionName(const FunctionNameNode* node) {
  pending_error_ = absl_ports::InternalError(
      "FunctionNameNode should be handled in VisitFunction!");
}

void ScoringVisitor::VisitString(const StringNode* node) {
  pending_error_ =
      absl_ports::InvalidArgumentError("Scoring does not support String!");
}

void ScoringVisitor::VisitText(const TextNode* node) {
  pending_error_ =
      absl_ports::InternalError("TextNode should be handled in VisitMember!");
}

void ScoringVisitor::VisitMember(const MemberNode* node) {
  std::string value;
  if (node->children().size() == 1) {
    // If a member has only one child, then it can be a numeric literal,
    // or "this" if the member is a reference to a member function.
    value = node->children()[0]->value();
    if (value == "this") {
      stack.push_back(ThisExpression::Create());
      return;
    }
  } else if (node->children().size() == 2) {
    // If a member has two children, then it can only represent a floating point
    // number, so we need to join them by "." to build the numeric literal.
    value = absl_ports::StrCat(node->children()[0]->value(), ".",
                               node->children()[1]->value());
  } else {
    pending_error_ = absl_ports::InvalidArgumentError(
        "MemberNode must have 1 or 2 children.");
    return;
  }
  char* end;
  double number = std::strtod(value.c_str(), &end);
  if (end != value.c_str() + value.length()) {
    // While it would be doable to support property references in the scoring
    // grammar, we currently don't have an efficient way to support such a
    // lookup (we'd have to read each document). As such, it's simpler to just
    // restrict the scoring language to not include properties.
    pending_error_ = absl_ports::InvalidArgumentError(
        absl_ports::StrCat("Expect a numeric literal, but got ", value));
    return;
  }
  stack.push_back(ConstantScoreExpression::Create(number));
}

void ScoringVisitor::VisitFunction(const FunctionNode* node) {
  std::vector<std::unique_ptr<ScoreExpression>> children;
  for (const auto& arg : node->args()) {
    arg->Accept(this);
    if (has_pending_error()) {
      return;
    }
    children.push_back(pop_stack());
  }
  const std::string& function_name = node->function_name()->value();
  libtextclassifier3::StatusOr<std::unique_ptr<ScoreExpression>> expression =
      absl_ports::InvalidArgumentError(
          absl_ports::StrCat("Unknown function: ", function_name));

  // Math functions
  if (MathFunctionScoreExpression::kFunctionNames.find(function_name) !=
      MathFunctionScoreExpression::kFunctionNames.end()) {
    expression = MathFunctionScoreExpression::Create(
        MathFunctionScoreExpression::kFunctionNames.at(function_name),
        std::move(children));
  }

  if (!expression.ok()) {
    pending_error_ = expression.status();
    return;
  }
  stack.push_back(std::move(expression).ValueOrDie());
}

void ScoringVisitor::VisitUnaryOperator(const UnaryOperatorNode* node) {
  if (node->operator_text() != "MINUS") {
    pending_error_ = absl_ports::InvalidArgumentError(
        absl_ports::StrCat("Unknown unary operator: ", node->operator_text()));
    return;
  }
  node->child()->Accept(this);
  if (has_pending_error()) {
    return;
  }
  std::vector<std::unique_ptr<ScoreExpression>> children;
  children.push_back(pop_stack());

  libtextclassifier3::StatusOr<std::unique_ptr<OperatorScoreExpression>>
      expression = OperatorScoreExpression::Create(
          OperatorScoreExpression::OperatorType::kNegative,
          std::move(children));
  if (!expression.ok()) {
    pending_error_ = expression.status();
    return;
  }
  stack.push_back(std::move(expression).ValueOrDie());
}

void ScoringVisitor::VisitNaryOperator(const NaryOperatorNode* node) {
  std::vector<std::unique_ptr<ScoreExpression>> children;
  for (const auto& arg : node->children()) {
    arg->Accept(this);
    if (has_pending_error()) {
      return;
    }
    children.push_back(pop_stack());
  }

  libtextclassifier3::StatusOr<std::unique_ptr<OperatorScoreExpression>>
      expression = absl_ports::InvalidArgumentError(
          absl_ports::StrCat("Unknown Nary operator: ", node->operator_text()));

  if (node->operator_text() == "PLUS") {
    expression = OperatorScoreExpression::Create(
        OperatorScoreExpression::OperatorType::kPlus, std::move(children));
  } else if (node->operator_text() == "MINUS") {
    expression = OperatorScoreExpression::Create(
        OperatorScoreExpression::OperatorType::kMinus, std::move(children));
  } else if (node->operator_text() == "TIMES") {
    expression = OperatorScoreExpression::Create(
        OperatorScoreExpression::OperatorType::kTimes, std::move(children));
  } else if (node->operator_text() == "DIV") {
    expression = OperatorScoreExpression::Create(
        OperatorScoreExpression::OperatorType::kDiv, std::move(children));
  }
  if (!expression.ok()) {
    pending_error_ = expression.status();
    return;
  }
  stack.push_back(std::move(expression).ValueOrDie());
}

}  // namespace lib
}  // namespace icing
