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

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/absl_ports/canonical_errors.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/index/embed/embedding-query-results.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/join/join-children-fetcher.h"
#include "icing/schema/section.h"
#include "icing/scoring/bm25f-calculator.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/scoring/section-weights.h"
#include "icing/store/document-associated-score-data.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/util/embedding-util.h"
#include "icing/util/logging.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

namespace {

libtextclassifier3::Status CheckChildrenNotNull(
    const std::vector<std::unique_ptr<ScoreExpression>>& children) {
  for (const auto& child : children) {
    ICING_RETURN_ERROR_IF_NULL(child);
  }
  return libtextclassifier3::Status::OK;
}

}  // namespace

libtextclassifier3::StatusOr<std::unique_ptr<ScoreExpression>>
OperatorScoreExpression::Create(
    OperatorType op, std::vector<std::unique_ptr<ScoreExpression>> children) {
  if (children.empty()) {
    return absl_ports::InvalidArgumentError(
        "OperatorScoreExpression must have at least one argument.");
  }
  ICING_RETURN_IF_ERROR(CheckChildrenNotNull(children));

  bool children_all_constant_double = true;
  for (const auto& child : children) {
    if (child->type() != ScoreExpressionType::kDouble) {
      return absl_ports::InvalidArgumentError(
          "Operators are only supported for double type.");
    }
    if (!child->is_constant()) {
      children_all_constant_double = false;
    }
  }
  if (op == OperatorType::kNegative) {
    if (children.size() != 1) {
      return absl_ports::InvalidArgumentError(
          "Negative operator must have only 1 argument.");
    }
  }
  std::unique_ptr<ScoreExpression> expression =
      std::unique_ptr<OperatorScoreExpression>(
          new OperatorScoreExpression(op, std::move(children)));
  if (children_all_constant_double) {
    // Because all of the children are constants, this expression does not
    // depend on the DocHitInto or query_it that are passed into it.
    ICING_ASSIGN_OR_RETURN(double constant_value,
                           expression->EvaluateDouble(DocHitInfo(),
                                                      /*query_it=*/nullptr));
    return ConstantScoreExpression::Create(constant_value);
  }
  return expression;
}

libtextclassifier3::StatusOr<double> OperatorScoreExpression::EvaluateDouble(
    const DocHitInfo& hit_info, const DocHitInfoIterator* query_it) const {
  // The Create factory guarantees that an operator will have at least one
  // child.
  ICING_ASSIGN_OR_RETURN(double res,
                         children_.at(0)->EvaluateDouble(hit_info, query_it));

  if (op_ == OperatorType::kNegative) {
    return -res;
  }

  for (int i = 1; i < children_.size(); ++i) {
    ICING_ASSIGN_OR_RETURN(double v,
                           children_.at(i)->EvaluateDouble(hit_info, query_it));
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
        {"log", FunctionType::kLog},
        {"pow", FunctionType::kPow},
        {"max", FunctionType::kMax},
        {"min", FunctionType::kMin},
        {"len", FunctionType::kLen},
        {"sum", FunctionType::kSum},
        {"avg", FunctionType::kAvg},
        {"sqrt", FunctionType::kSqrt},
        {"abs", FunctionType::kAbs},
        {"sin", FunctionType::kSin},
        {"cos", FunctionType::kCos},
        {"tan", FunctionType::kTan},
        {"maxOrDefault", FunctionType::kMaxOrDefault},
        {"minOrDefault", FunctionType::kMinOrDefault}};

const std::unordered_set<MathFunctionScoreExpression::FunctionType>
    MathFunctionScoreExpression::kVariableArgumentsFunctions = {
        FunctionType::kMax, FunctionType::kMin, FunctionType::kLen,
        FunctionType::kSum, FunctionType::kAvg};

const std::unordered_set<MathFunctionScoreExpression::FunctionType>
    MathFunctionScoreExpression::kListArgumentFunctions = {
        FunctionType::kMaxOrDefault, FunctionType::kMinOrDefault};

libtextclassifier3::StatusOr<std::unique_ptr<ScoreExpression>>
MathFunctionScoreExpression::Create(
    FunctionType function_type,
    std::vector<std::unique_ptr<ScoreExpression>> args) {
  if (args.empty()) {
    return absl_ports::InvalidArgumentError(
        "Math functions must have at least one argument.");
  }
  ICING_RETURN_IF_ERROR(CheckChildrenNotNull(args));

  // Return early for functions that support variable length arguments and the
  // first argument is a list.
  if (args.size() == 1 && args[0]->type() == ScoreExpressionType::kDoubleList &&
      kVariableArgumentsFunctions.count(function_type) > 0) {
    return std::unique_ptr<MathFunctionScoreExpression>(
        new MathFunctionScoreExpression(function_type, std::move(args)));
  }

  bool args_all_constant_double = false;
  if (kListArgumentFunctions.count(function_type) > 0) {
    if (args[0]->type() != ScoreExpressionType::kDoubleList) {
      return absl_ports::InvalidArgumentError(
          "Got an invalid type for the math function. Should expect a list "
          "type value in the first argument.");
    }
  } else {
    args_all_constant_double = true;
    for (const auto& child : args) {
      if (child->type() != ScoreExpressionType::kDouble) {
        return absl_ports::InvalidArgumentError(
            "Got an invalid type for the math function. Should expect a double "
            "type argument.");
      }
      if (!child->is_constant()) {
        args_all_constant_double = false;
      }
    }
  }
  switch (function_type) {
    case FunctionType::kLog:
      if (args.size() != 1 && args.size() != 2) {
        return absl_ports::InvalidArgumentError(
            "log must have 1 or 2 arguments.");
      }
      break;
    case FunctionType::kPow:
      if (args.size() != 2) {
        return absl_ports::InvalidArgumentError("pow must have 2 arguments.");
      }
      break;
    case FunctionType::kSqrt:
      if (args.size() != 1) {
        return absl_ports::InvalidArgumentError("sqrt must have 1 argument.");
      }
      break;
    case FunctionType::kAbs:
      if (args.size() != 1) {
        return absl_ports::InvalidArgumentError("abs must have 1 argument.");
      }
      break;
    case FunctionType::kSin:
      if (args.size() != 1) {
        return absl_ports::InvalidArgumentError("sin must have 1 argument.");
      }
      break;
    case FunctionType::kCos:
      if (args.size() != 1) {
        return absl_ports::InvalidArgumentError("cos must have 1 argument.");
      }
      break;
    case FunctionType::kTan:
      if (args.size() != 1) {
        return absl_ports::InvalidArgumentError("tan must have 1 argument.");
      }
      break;
    case FunctionType::kMaxOrDefault:
      if (args.size() != 2) {
        return absl_ports::InvalidArgumentError(
            "maxOrDefault must have 2 arguments.");
      }
      if (args[1]->type() != ScoreExpressionType::kDouble) {
        return absl_ports::InvalidArgumentError(
            "maxOrDefault must have a double type as the second argument.");
      }
      break;
    case FunctionType::kMinOrDefault:
      if (args.size() != 2) {
        return absl_ports::InvalidArgumentError(
            "minOrDefault must have 2 arguments.");
      }
      if (args[1]->type() != ScoreExpressionType::kDouble) {
        return absl_ports::InvalidArgumentError(
            "minOrDefault must have a double type as the second argument.");
      }
      break;
    // Functions that support variable length arguments
    case FunctionType::kMax:
      [[fallthrough]];
    case FunctionType::kMin:
      [[fallthrough]];
    case FunctionType::kLen:
      [[fallthrough]];
    case FunctionType::kSum:
      [[fallthrough]];
    case FunctionType::kAvg:
      break;
  }
  std::unique_ptr<ScoreExpression> expression =
      std::unique_ptr<MathFunctionScoreExpression>(
          new MathFunctionScoreExpression(function_type, std::move(args)));
  if (args_all_constant_double) {
    // Because all of the arguments are constants, this expression does not
    // depend on the DocHitInto or query_it that are passed into it.
    ICING_ASSIGN_OR_RETURN(double constant_value,
                           expression->EvaluateDouble(DocHitInfo(),
                                                      /*query_it=*/nullptr));
    return ConstantScoreExpression::Create(constant_value);
  }
  return expression;
}

libtextclassifier3::StatusOr<double>
MathFunctionScoreExpression::EvaluateDouble(
    const DocHitInfo& hit_info, const DocHitInfoIterator* query_it) const {
  std::vector<double> values;
  int ind = 0;
  if (args_.at(0)->type() == ScoreExpressionType::kDoubleList) {
    ICING_ASSIGN_OR_RETURN(values,
                           args_.at(0)->EvaluateList(hit_info, query_it));
    ind = 1;
  }
  for (; ind < args_.size(); ++ind) {
    ICING_ASSIGN_OR_RETURN(double v,
                           args_.at(ind)->EvaluateDouble(hit_info, query_it));
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
      if (values.empty()) {
        return absl_ports::InvalidArgumentError(
            "Got an empty parameter set in max function");
      }
      res = *std::max_element(values.begin(), values.end());
      break;
    case FunctionType::kMin:
      if (values.empty()) {
        return absl_ports::InvalidArgumentError(
            "Got an empty parameter set in min function");
      }
      res = *std::min_element(values.begin(), values.end());
      break;
    case FunctionType::kLen:
      res = values.size();
      break;
    case FunctionType::kSum:
      res = std::reduce(values.begin(), values.end());
      break;
    case FunctionType::kAvg:
      if (values.empty()) {
        return absl_ports::InvalidArgumentError(
            "Got an empty parameter set in avg function.");
      }
      res = std::reduce(values.begin(), values.end()) / values.size();
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
    // For the following two functions, the last value is the default value.
    // If values.size() == 1, then it means the provided list is empty.
    case FunctionType::kMaxOrDefault:
      if (values.size() == 1) {
        res = values[0];
      } else {
        res = *std::max_element(values.begin(), values.end() - 1);
      }
      break;
    case FunctionType::kMinOrDefault:
      if (values.size() == 1) {
        res = values[0];
      } else {
        res = *std::min_element(values.begin(), values.end() - 1);
      }
      break;
  }
  if (!std::isfinite(res)) {
    return absl_ports::InvalidArgumentError(
        "Got a non-finite value while evaluating math function score "
        "expression.");
  }
  return res;
}

const std::unordered_map<std::string,
                         ListOperationFunctionScoreExpression::FunctionType>
    ListOperationFunctionScoreExpression::kFunctionNames = {
        {"filterByRange", FunctionType::kFilterByRange}};

libtextclassifier3::StatusOr<std::unique_ptr<ScoreExpression>>
ListOperationFunctionScoreExpression::Create(
    FunctionType function_type,
    std::vector<std::unique_ptr<ScoreExpression>> args) {
  if (args.empty()) {
    return absl_ports::InvalidArgumentError(
        "List operation functions must have at least one argument.");
  }
  ICING_RETURN_IF_ERROR(CheckChildrenNotNull(args));

  switch (function_type) {
    case FunctionType::kFilterByRange:
      if (args.size() != 3) {
        return absl_ports::InvalidArgumentError(
            "filterByRange must have 3 arguments.");
      }
      if (args[0]->type() != ScoreExpressionType::kDoubleList) {
        return absl_ports::InvalidArgumentError(
            "Should expect a list type value for the first argument of "
            "filterByRange.");
      }
      if (args.at(1)->type() != ScoreExpressionType::kDouble ||
          args.at(2)->type() != ScoreExpressionType::kDouble) {
        return absl_ports::InvalidArgumentError(
            "Should expect double type values for the second and third "
            "arguments of filterByRange.");
      }
      break;
  }
  return std::unique_ptr<ListOperationFunctionScoreExpression>(
      new ListOperationFunctionScoreExpression(function_type, std::move(args)));
}

libtextclassifier3::StatusOr<std::vector<double>>
ListOperationFunctionScoreExpression::EvaluateList(
    const DocHitInfo& hit_info, const DocHitInfoIterator* query_it) const {
  switch (function_type_) {
    case FunctionType::kFilterByRange:
      ICING_ASSIGN_OR_RETURN(std::vector<double> list_value,
                             args_.at(0)->EvaluateList(hit_info, query_it));
      ICING_ASSIGN_OR_RETURN(double low,
                             args_.at(1)->EvaluateDouble(hit_info, query_it));
      ICING_ASSIGN_OR_RETURN(double high,
                             args_.at(2)->EvaluateDouble(hit_info, query_it));
      if (low > high) {
        return absl_ports::InvalidArgumentError(
            "The lower bound cannot be greater than the upper bound.");
      }
      auto new_end =
          std::remove_if(list_value.begin(), list_value.end(),
                         [low, high](double v) { return v < low || v > high; });
      list_value.erase(new_end, list_value.end());
      return list_value;
      break;
  }
  return absl_ports::InternalError("Should never reach here.");
}

const std::unordered_map<std::string,
                         DocumentFunctionScoreExpression::FunctionType>
    DocumentFunctionScoreExpression::kFunctionNames = {
        {"documentScore", FunctionType::kDocumentScore},
        {"creationTimestamp", FunctionType::kCreationTimestamp},
        {"usageCount", FunctionType::kUsageCount},
        {"usageLastUsedTimestamp", FunctionType::kUsageLastUsedTimestamp}};

libtextclassifier3::StatusOr<std::unique_ptr<DocumentFunctionScoreExpression>>
DocumentFunctionScoreExpression::Create(
    FunctionType function_type,
    std::vector<std::unique_ptr<ScoreExpression>> args,
    const DocumentStore* document_store, double default_score,
    int64_t current_time_ms) {
  if (args.empty()) {
    return absl_ports::InvalidArgumentError(
        "Document-based functions must have at least one argument.");
  }
  ICING_RETURN_IF_ERROR(CheckChildrenNotNull(args));

  if (args[0]->type() != ScoreExpressionType::kDocument) {
    return absl_ports::InvalidArgumentError(
        "The first parameter of document-based functions must be \"this\".");
  }
  switch (function_type) {
    case FunctionType::kDocumentScore:
      [[fallthrough]];
    case FunctionType::kCreationTimestamp:
      if (args.size() != 1) {
        return absl_ports::InvalidArgumentError(
            "DocumentScore/CreationTimestamp must have 1 argument.");
      }
      break;
    case FunctionType::kUsageCount:
      [[fallthrough]];
    case FunctionType::kUsageLastUsedTimestamp:
      if (args.size() != 2 || args[1]->type() != ScoreExpressionType::kDouble) {
        return absl_ports::InvalidArgumentError(
            "UsageCount/UsageLastUsedTimestamp must have 2 arguments. The "
            "first argument should be \"this\", and the second argument "
            "should be the usage type.");
      }
      break;
  }
  return std::unique_ptr<DocumentFunctionScoreExpression>(
      new DocumentFunctionScoreExpression(function_type, std::move(args),
                                          document_store, default_score,
                                          current_time_ms));
}

libtextclassifier3::StatusOr<double>
DocumentFunctionScoreExpression::EvaluateDouble(
    const DocHitInfo& hit_info, const DocHitInfoIterator* query_it) const {
  switch (function_type_) {
    case FunctionType::kDocumentScore:
      [[fallthrough]];
    case FunctionType::kCreationTimestamp: {
      ICING_ASSIGN_OR_RETURN(DocumentAssociatedScoreData score_data,
                             document_store_.GetDocumentAssociatedScoreData(
                                 hit_info.document_id()),
                             default_score_);
      if (function_type_ == FunctionType::kDocumentScore) {
        return static_cast<double>(score_data.document_score());
      }
      return static_cast<double>(score_data.creation_timestamp_ms());
    }
    case FunctionType::kUsageCount:
      [[fallthrough]];
    case FunctionType::kUsageLastUsedTimestamp: {
      ICING_ASSIGN_OR_RETURN(double raw_usage_type,
                             args_[1]->EvaluateDouble(hit_info, query_it));
      int usage_type = (int)raw_usage_type;
      if (usage_type < 1 || usage_type > 3 || raw_usage_type != usage_type) {
        return absl_ports::InvalidArgumentError(
            "Usage type must be an integer from 1 to 3");
      }
      std::optional<UsageStore::UsageScores> usage_scores =
          document_store_.GetUsageScores(hit_info.document_id(),
                                         current_time_ms_);
      if (!usage_scores) {
        // If there's no UsageScores entry present for this doc, then just
        // treat it as a default instance.
        usage_scores = UsageStore::UsageScores();
      }
      if (function_type_ == FunctionType::kUsageCount) {
        if (usage_type == 1) {
          return usage_scores->usage_type1_count;
        } else if (usage_type == 2) {
          return usage_scores->usage_type2_count;
        } else {
          return usage_scores->usage_type3_count;
        }
      }
      if (usage_type == 1) {
        return usage_scores->usage_type1_last_used_timestamp_s * 1000.0;
      } else if (usage_type == 2) {
        return usage_scores->usage_type2_last_used_timestamp_s * 1000.0;
      } else {
        return usage_scores->usage_type3_last_used_timestamp_s * 1000.0;
      }
    }
  }
}

libtextclassifier3::StatusOr<
    std::unique_ptr<RelevanceScoreFunctionScoreExpression>>
RelevanceScoreFunctionScoreExpression::Create(
    std::vector<std::unique_ptr<ScoreExpression>> args,
    Bm25fCalculator* bm25f_calculator, double default_score) {
  if (args.size() != 1) {
    return absl_ports::InvalidArgumentError(
        "relevanceScore must have 1 argument.");
  }
  ICING_RETURN_IF_ERROR(CheckChildrenNotNull(args));

  if (args[0]->type() != ScoreExpressionType::kDocument) {
    return absl_ports::InvalidArgumentError(
        "relevanceScore must take \"this\" as its argument.");
  }
  return std::unique_ptr<RelevanceScoreFunctionScoreExpression>(
      new RelevanceScoreFunctionScoreExpression(bm25f_calculator,
                                                default_score));
}

libtextclassifier3::StatusOr<double>
RelevanceScoreFunctionScoreExpression::EvaluateDouble(
    const DocHitInfo& hit_info, const DocHitInfoIterator* query_it) const {
  if (query_it == nullptr) {
    return default_score_;
  }
  return static_cast<double>(
      bm25f_calculator_.ComputeScore(query_it, hit_info, default_score_));
}

libtextclassifier3::StatusOr<
    std::unique_ptr<ChildrenRankingSignalsFunctionScoreExpression>>
ChildrenRankingSignalsFunctionScoreExpression::Create(
    std::vector<std::unique_ptr<ScoreExpression>> args,
    const JoinChildrenFetcher* join_children_fetcher) {
  if (args.size() != 1) {
    return absl_ports::InvalidArgumentError(
        "childrenRankingSignals must have 1 argument.");
  }
  ICING_RETURN_IF_ERROR(CheckChildrenNotNull(args));

  if (args[0]->type() != ScoreExpressionType::kDocument) {
    return absl_ports::InvalidArgumentError(
        "childrenRankingSignals must take \"this\" as its argument.");
  }
  if (join_children_fetcher == nullptr) {
    return absl_ports::InvalidArgumentError(
        "childrenRankingSignals must only be used with join, but "
        "JoinChildrenFetcher "
        "is not provided.");
  }
  return std::unique_ptr<ChildrenRankingSignalsFunctionScoreExpression>(
      new ChildrenRankingSignalsFunctionScoreExpression(
          *join_children_fetcher));
}

libtextclassifier3::StatusOr<std::vector<double>>
ChildrenRankingSignalsFunctionScoreExpression::EvaluateList(
    const DocHitInfo& hit_info, const DocHitInfoIterator* query_it) const {
  ICING_ASSIGN_OR_RETURN(
      std::vector<ScoredDocumentHit> children_hits,
      join_children_fetcher_.GetChildren(hit_info.document_id()));
  std::vector<double> children_scores;
  children_scores.reserve(children_hits.size());
  for (const ScoredDocumentHit& child_hit : children_hits) {
    children_scores.push_back(child_hit.score());
  }
  return std::move(children_scores);
}

libtextclassifier3::StatusOr<
    std::unique_ptr<PropertyWeightsFunctionScoreExpression>>
PropertyWeightsFunctionScoreExpression::Create(
    std::vector<std::unique_ptr<ScoreExpression>> args,
    const DocumentStore* document_store, const SectionWeights* section_weights,
    int64_t current_time_ms) {
  if (args.size() != 1) {
    return absl_ports::InvalidArgumentError(
        "propertyWeights must have 1 argument.");
  }
  ICING_RETURN_IF_ERROR(CheckChildrenNotNull(args));

  if (args[0]->type() != ScoreExpressionType::kDocument) {
    return absl_ports::InvalidArgumentError(
        "propertyWeights must take \"this\" as its argument.");
  }
  return std::unique_ptr<PropertyWeightsFunctionScoreExpression>(
      new PropertyWeightsFunctionScoreExpression(
          document_store, section_weights, current_time_ms));
}

libtextclassifier3::StatusOr<std::vector<double>>
PropertyWeightsFunctionScoreExpression::EvaluateList(
    const DocHitInfo& hit_info, const DocHitInfoIterator*) const {
  std::vector<double> weights;
  SectionIdMask sections = hit_info.hit_section_ids_mask();
  SchemaTypeId schema_type_id = GetSchemaTypeId(hit_info.document_id());

  while (sections != 0) {
    SectionId section_id = __builtin_ctzll(sections);
    sections &= ~(UINT64_C(1) << section_id);
    weights.push_back(section_weights_.GetNormalizedSectionWeight(
        schema_type_id, section_id));
  }
  return weights;
}

SchemaTypeId PropertyWeightsFunctionScoreExpression::GetSchemaTypeId(
    DocumentId document_id) const {
  auto filter_data_optional =
      document_store_.GetAliveDocumentFilterData(document_id, current_time_ms_);
  if (!filter_data_optional) {
    // This should never happen. The only failure case for
    // GetAliveDocumentFilterData is if the document_id is outside of the range
    // of allocated document_ids, which shouldn't be possible since we're
    // getting this document_id from the posting lists.
    ICING_LOG(WARNING) << "No document filter data for document ["
                       << document_id << "]";
    return kInvalidSchemaTypeId;
  }
  return filter_data_optional.value().schema_type_id();
}

libtextclassifier3::StatusOr<std::unique_ptr<ScoreExpression>>
GetEmbeddingParameterFunctionScoreExpression::Create(
    std::vector<std::unique_ptr<ScoreExpression>> args) {
  ICING_RETURN_IF_ERROR(CheckChildrenNotNull(args));

  if (args.size() != 1) {
    return absl_ports::InvalidArgumentError(
        absl_ports::StrCat(kFunctionName, " must have 1 argument."));
  }
  if (args[0]->type() != ScoreExpressionType::kDouble) {
    return absl_ports::InvalidArgumentError(
        absl_ports::StrCat(kFunctionName, " got invalid argument type."));
  }
  bool is_constant = args[0]->is_constant();
  std::unique_ptr<ScoreExpression> expression =
      std::unique_ptr<GetEmbeddingParameterFunctionScoreExpression>(
          new GetEmbeddingParameterFunctionScoreExpression(std::move(args[0])));
  if (is_constant) {
    ICING_ASSIGN_OR_RETURN(double constant_value,
                           expression->EvaluateDouble(DocHitInfo(),
                                                      /*query_it=*/nullptr));
    return ConstantScoreExpression::Create(constant_value, expression->type());
  }
  return expression;
}

libtextclassifier3::StatusOr<double>
GetEmbeddingParameterFunctionScoreExpression::EvaluateDouble(
    const DocHitInfo& hit_info, const DocHitInfoIterator* query_it) const {
  ICING_ASSIGN_OR_RETURN(double raw_query_index,
                         arg_->EvaluateDouble(hit_info, query_it));
  if (raw_query_index < 0) {
    return absl_ports::InvalidArgumentError(
        "The index of an embedding query must be a non-negative integer.");
  }
  if (raw_query_index > std::numeric_limits<uint32_t>::max()) {
    return absl_ports::InvalidArgumentError(
        "The index of an embedding query exceeds the maximum value of uint32.");
  }
  uint32_t query_index = (uint32_t)raw_query_index;
  if (query_index != raw_query_index) {
    return absl_ports::InvalidArgumentError(
        "The index of an embedding query must be an integer.");
  }
  return query_index;
}

libtextclassifier3::StatusOr<
    std::unique_ptr<MatchedSemanticScoresFunctionScoreExpression>>
MatchedSemanticScoresFunctionScoreExpression::Create(
    std::vector<std::unique_ptr<ScoreExpression>> args,
    SearchSpecProto::EmbeddingQueryMetricType::Code default_metric_type,
    const EmbeddingQueryResults* embedding_query_results) {
  ICING_RETURN_ERROR_IF_NULL(embedding_query_results);
  ICING_RETURN_IF_ERROR(CheckChildrenNotNull(args));

  if (args.empty() || args[0]->type() != ScoreExpressionType::kDocument) {
    return absl_ports::InvalidArgumentError(
        absl_ports::StrCat(kFunctionName, " is not called with \"this\""));
  }
  if (args.size() != 2 && args.size() != 3) {
    return absl_ports::InvalidArgumentError(
        absl_ports::StrCat(kFunctionName, " got invalid number of arguments."));
  }
  ScoreExpression* embedding_index_arg = args[1].get();
  if (embedding_index_arg->type() != ScoreExpressionType::kVectorIndex) {
    return absl_ports::InvalidArgumentError(absl_ports::StrCat(
        kFunctionName, " got invalid argument type for embedding vector."));
  }
  if (args.size() == 3 && args[2]->type() != ScoreExpressionType::kString) {
    return absl_ports::InvalidArgumentError(
        "Embedding metric can only be given as a string.");
  }

  SearchSpecProto::EmbeddingQueryMetricType::Code metric_type =
      default_metric_type;
  if (args.size() == 3) {
    if (!args[2]->is_constant()) {
      return absl_ports::InvalidArgumentError(
          "Embedding metric can only be given as a constant string.");
    }
    ICING_ASSIGN_OR_RETURN(std::string_view metric, args[2]->EvaluateString());
    ICING_ASSIGN_OR_RETURN(
        metric_type,
        embedding_util::GetEmbeddingQueryMetricTypeFromName(metric));
  }
  if (embedding_index_arg->is_constant()) {
    ICING_ASSIGN_OR_RETURN(
        uint32_t embedding_index,
        embedding_index_arg->EvaluateDouble(DocHitInfo(),
                                            /*query_it=*/nullptr));
    if (embedding_query_results->GetScoreMap(embedding_index, metric_type) ==
        nullptr) {
      return absl_ports::InvalidArgumentError(absl_ports::StrCat(
          "The embedding query index ", std::to_string(embedding_index),
          " with metric type ",
          SearchSpecProto::EmbeddingQueryMetricType::Code_Name(metric_type),
          " has not been queried."));
    }
  }
  return std::unique_ptr<MatchedSemanticScoresFunctionScoreExpression>(
      new MatchedSemanticScoresFunctionScoreExpression(
          std::move(args), metric_type, *embedding_query_results));
}

libtextclassifier3::StatusOr<std::vector<double>>
MatchedSemanticScoresFunctionScoreExpression::EvaluateList(
    const DocHitInfo& hit_info, const DocHitInfoIterator* query_it) const {
  ICING_ASSIGN_OR_RETURN(double raw_query_index,
                         args_[1]->EvaluateDouble(hit_info, query_it));
  uint32_t query_index = (uint32_t)raw_query_index;
  const std::vector<double>* scores =
      embedding_query_results_.GetMatchedScoresForDocument(
          query_index, metric_type_, hit_info.document_id());
  if (scores == nullptr) {
    return std::vector<double>();
  }
  return *scores;
}

}  // namespace lib
}  // namespace icing
