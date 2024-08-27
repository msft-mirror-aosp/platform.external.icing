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

#ifndef ICING_SCORING_ADVANCED_SCORING_SCORE_EXPRESSION_H_
#define ICING_SCORING_ADVANCED_SCORING_SCORE_EXPRESSION_H_

#include <algorithm>
#include <cmath>
#include <memory>
#include <unordered_map>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/scoring/bm25f-calculator.h"
#include "icing/store/document-store.h"
#include "icing/util/status-macros.h"

namespace icing {
namespace lib {

// TODO(b/261474063) Simplify every ScoreExpression node to
// ConstantScoreExpression if its evaluation does not depend on a document.
class ScoreExpression {
 public:
  virtual ~ScoreExpression() = default;

  // Evaluate the score expression to double with the current document.
  //
  // RETURNS:
  //   - The evaluated result as a double on success.
  //   - INVALID_ARGUMENT if a non-finite value is reached while evaluating the
  //                      expression.
  //   - INTERNAL if there are inconsistencies.
  virtual libtextclassifier3::StatusOr<double> eval(
      const DocHitInfo& hit_info, const DocHitInfoIterator* query_it) = 0;

  // Indicate whether the current expression is of document type
  virtual bool is_document_type() const { return false; }
};

class ThisExpression : public ScoreExpression {
 public:
  static std::unique_ptr<ThisExpression> Create() {
    return std::unique_ptr<ThisExpression>(new ThisExpression());
  }

  libtextclassifier3::StatusOr<double> eval(
      const DocHitInfo& hit_info, const DocHitInfoIterator* query_it) override {
    return absl_ports::InternalError(
        "Should never reach here to evaluate a document type as double. "
        "There must be inconsistencies.");
  }

  bool is_document_type() const override { return true; }

 private:
  ThisExpression() = default;
};

class ConstantScoreExpression : public ScoreExpression {
 public:
  static std::unique_ptr<ConstantScoreExpression> Create(double c) {
    return std::unique_ptr<ConstantScoreExpression>(
        new ConstantScoreExpression(c));
  }

  libtextclassifier3::StatusOr<double> eval(
      const DocHitInfo&, const DocHitInfoIterator*) override {
    return c_;
  }

 private:
  explicit ConstantScoreExpression(double c) : c_(c) {}

  double c_;
};

class OperatorScoreExpression : public ScoreExpression {
 public:
  enum class OperatorType { kPlus, kMinus, kNegative, kTimes, kDiv };

  // RETURNS:
  //   - An OperatorScoreExpression instance on success.
  //   - FAILED_PRECONDITION on any null pointer in children.
  //   - INVALID_ARGUMENT on type errors.
  static libtextclassifier3::StatusOr<std::unique_ptr<OperatorScoreExpression>>
  Create(OperatorType op,
         std::vector<std::unique_ptr<ScoreExpression>> children);

  libtextclassifier3::StatusOr<double> eval(
      const DocHitInfo& hit_info, const DocHitInfoIterator* query_it) override;

 private:
  explicit OperatorScoreExpression(
      OperatorType op, std::vector<std::unique_ptr<ScoreExpression>> children)
      : op_(op), children_(std::move(children)) {}

  OperatorType op_;
  std::vector<std::unique_ptr<ScoreExpression>> children_;
};

class MathFunctionScoreExpression : public ScoreExpression {
 public:
  enum class FunctionType {
    kLog,
    kPow,
    kMax,
    kMin,
    kSqrt,
    kAbs,
    kSin,
    kCos,
    kTan
  };

  static const std::unordered_map<std::string, FunctionType> kFunctionNames;

  // RETURNS:
  //   - A MathFunctionScoreExpression instance on success.
  //   - FAILED_PRECONDITION on any null pointer in children.
  //   - INVALID_ARGUMENT on type errors.
  static libtextclassifier3::StatusOr<
      std::unique_ptr<MathFunctionScoreExpression>>
  Create(FunctionType function_type,
         std::vector<std::unique_ptr<ScoreExpression>> children);

  libtextclassifier3::StatusOr<double> eval(
      const DocHitInfo& hit_info, const DocHitInfoIterator* query_it) override;

 private:
  explicit MathFunctionScoreExpression(
      FunctionType function_type,
      std::vector<std::unique_ptr<ScoreExpression>> children)
      : function_type_(function_type), children_(std::move(children)) {}

  FunctionType function_type_;
  std::vector<std::unique_ptr<ScoreExpression>> children_;
};

class DocumentFunctionScoreExpression : public ScoreExpression {
 public:
  enum class FunctionType {
    kDocumentScore,
    kCreationTimestamp,
    kUsageCount,
    kUsageLastUsedTimestamp,
  };

  static const std::unordered_map<std::string, FunctionType> kFunctionNames;

  // RETURNS:
  //   - A DocumentFunctionScoreExpression instance on success.
  //   - FAILED_PRECONDITION on any null pointer in children.
  //   - INVALID_ARGUMENT on type errors.
  static libtextclassifier3::StatusOr<
      std::unique_ptr<DocumentFunctionScoreExpression>>
  Create(FunctionType function_type,
         std::vector<std::unique_ptr<ScoreExpression>> children,
         const DocumentStore* document_store, double default_score);

  libtextclassifier3::StatusOr<double> eval(
      const DocHitInfo& hit_info, const DocHitInfoIterator* query_it) override;

 private:
  explicit DocumentFunctionScoreExpression(
      FunctionType function_type,
      std::vector<std::unique_ptr<ScoreExpression>> children,
      const DocumentStore* document_store, double default_score)
      : children_(std::move(children)),
        document_store_(*document_store),
        default_score_(default_score),
        function_type_(function_type) {}

  std::vector<std::unique_ptr<ScoreExpression>> children_;
  const DocumentStore& document_store_;
  double default_score_;
  FunctionType function_type_;
};

class RelevanceScoreFunctionScoreExpression : public ScoreExpression {
 public:
  static constexpr std::string_view kFunctionName = "relevanceScore";

  // RETURNS:
  //   - A RelevanceScoreFunctionScoreExpression instance on success.
  //   - FAILED_PRECONDITION on any null pointer in children.
  //   - INVALID_ARGUMENT on type errors.
  static libtextclassifier3::StatusOr<
      std::unique_ptr<RelevanceScoreFunctionScoreExpression>>
  Create(std::vector<std::unique_ptr<ScoreExpression>> children,
         Bm25fCalculator* bm25f_calculator, double default_score);

  libtextclassifier3::StatusOr<double> eval(
      const DocHitInfo& hit_info, const DocHitInfoIterator* query_it) override;

 private:
  explicit RelevanceScoreFunctionScoreExpression(
      std::vector<std::unique_ptr<ScoreExpression>> children,
      Bm25fCalculator* bm25f_calculator, double default_score)
      : children_(std::move(children)),
        bm25f_calculator_(*bm25f_calculator),
        default_score_(default_score) {}

  std::vector<std::unique_ptr<ScoreExpression>> children_;
  Bm25fCalculator& bm25f_calculator_;
  double default_score_;
};

}  // namespace lib
}  // namespace icing

#endif  // ICING_SCORING_ADVANCED_SCORING_SCORE_EXPRESSION_H_
