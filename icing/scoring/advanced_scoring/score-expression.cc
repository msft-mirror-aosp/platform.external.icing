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

#include "icing/scoring/advanced_scoring/score-expression.h"

namespace icing {
namespace lib {

libtextclassifier3::StatusOr<std::unique_ptr<OperatorScoreExpression>>
OperatorScoreExpression::Create(
    OperatorType op, std::vector<std::unique_ptr<ScoreExpression>> children) {
  if (children.empty()) {
    return absl_ports::InvalidArgumentError(
        "OperatorScoreExpression must have at least one argument.");
  }
  for (const auto& child : children) {
    ICING_RETURN_ERROR_IF_NULL(child);
    if (child->is_document_type()) {
      return absl_ports::InvalidArgumentError(
          "Operators are not supported for document type.");
    }
  }
  if (op == OperatorType::kNegative) {
    if (children.size() != 1) {
      return absl_ports::InvalidArgumentError(
          "Negative operator must have only 1 argument.");
    }
  }
  return std::unique_ptr<OperatorScoreExpression>(
      new OperatorScoreExpression(op, std::move(children)));
}

libtextclassifier3::StatusOr<double> OperatorScoreExpression::eval(
    const DocHitInfo& hit_info, const DocHitInfoIterator* query_it) {
  // The Create factory guarantees that an operator will have at least one
  // child.
  ICING_ASSIGN_OR_RETURN(double res, children_.at(0)->eval(hit_info, query_it));

  if (op_ == OperatorType::kNegative) {
    return -res;
  }

  for (int i = 1; i < children_.size(); ++i) {
    ICING_ASSIGN_OR_RETURN(double v, children_.at(i)->eval(hit_info, query_it));
    switch (op_) {
      case OperatorType::kPlus:
        res += v;
        break;
      case OperatorType::kMinus:
        res -= v;
        break;
      case OperatorType::kTimes:
        res *= v;
        break;
      case OperatorType::kDiv:
        res /= v;
        break;
      case OperatorType::kNegative:
        return absl_ports::InternalError("Should never reach here.");
    }
    if (!std::isfinite(res)) {
      return absl_ports::InvalidArgumentError(
          "Got a non-finite value while evaluating operator score expression.");
    }
  }
  return res;
}

const std::unordered_map<std::string, MathFunctionScoreExpression::FunctionType>
    MathFunctionScoreExpression::kFunctionNames = {
        {"log", FunctionType::kLog},   {"pow", FunctionType::kPow},
        {"max", FunctionType::kMax},   {"min", FunctionType::kMin},
        {"sqrt", FunctionType::kSqrt}, {"abs", FunctionType::kAbs},
        {"sin", FunctionType::kSin},   {"cos", FunctionType::kCos},
        {"tan", FunctionType::kTan}};

libtextclassifier3::StatusOr<std::unique_ptr<MathFunctionScoreExpression>>
MathFunctionScoreExpression::Create(
    FunctionType function_type,
    std::vector<std::unique_ptr<ScoreExpression>> children) {
  if (children.empty()) {
    return absl_ports::InvalidArgumentError(
        "Math functions must have at least one argument.");
  }
  for (const auto& child : children) {
    ICING_RETURN_ERROR_IF_NULL(child);
    if (child->is_document_type()) {
      return absl_ports::InvalidArgumentError(
          "Math functions are not supported for document type.");
    }
  }
  switch (function_type) {
    case FunctionType::kLog:
      if (children.size() != 1 && children.size() != 2) {
        return absl_ports::InvalidArgumentError(
            "log must have 1 or 2 arguments.");
      }
      break;
    case FunctionType::kPow:
      if (children.size() != 2) {
        return absl_ports::InvalidArgumentError("pow must have 2 arguments.");
      }
      break;
    case FunctionType::kSqrt:
      if (children.size() != 1) {
        return absl_ports::InvalidArgumentError("sqrt must have 1 argument.");
      }
      break;
    case FunctionType::kAbs:
      if (children.size() != 1) {
        return absl_ports::InvalidArgumentError("abs must have 1 argument.");
      }
      break;
    case FunctionType::kSin:
      if (children.size() != 1) {
        return absl_ports::InvalidArgumentError("sin must have 1 argument.");
      }
      break;
    case FunctionType::kCos:
      if (children.size() != 1) {
        return absl_ports::InvalidArgumentError("cos must have 1 argument.");
      }
      break;
    case FunctionType::kTan:
      if (children.size() != 1) {
        return absl_ports::InvalidArgumentError("tan must have 1 argument.");
      }
      break;
    // max and min support variable length arguments
    case FunctionType::kMax:
      [[fallthrough]];
    case FunctionType::kMin:
      break;
  }
  return std::unique_ptr<MathFunctionScoreExpression>(
      new MathFunctionScoreExpression(function_type, std::move(children)));
}

libtextclassifier3::StatusOr<double> MathFunctionScoreExpression::eval(
    const DocHitInfo& hit_info, const DocHitInfoIterator* query_it) {
  std::vector<double> values;
  for (const auto& child : children_) {
    ICING_ASSIGN_OR_RETURN(double v, child->eval(hit_info, query_it));
    values.push_back(v);
  }

  double res = 0;
  switch (function_type_) {
    case FunctionType::kLog:
      if (values.size() == 1) {
        res = log(values[0]);
      } else {
        // argument 0 is log base
        // argument 1 is the value
        res = log(values[1]) / log(values[0]);
      }
      break;
    case FunctionType::kPow:
      res = pow(values[0], values[1]);
      break;
    case FunctionType::kMax:
      res = *std::max_element(values.begin(), values.end());
      break;
    case FunctionType::kMin:
      res = *std::min_element(values.begin(), values.end());
      break;
    case FunctionType::kSqrt:
      res = sqrt(values[0]);
      break;
    case FunctionType::kAbs:
      res = abs(values[0]);
      break;
    case FunctionType::kSin:
      res = sin(values[0]);
      break;
    case FunctionType::kCos:
      res = cos(values[0]);
      break;
    case FunctionType::kTan:
      res = tan(values[0]);
      break;
  }
  if (!std::isfinite(res)) {
    return absl_ports::InvalidArgumentError(
        "Got a non-finite value while evaluating math function score "
        "expression.");
  }
  return res;
}

}  // namespace lib
}  // namespace icing
