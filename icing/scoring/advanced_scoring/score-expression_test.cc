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

#include <cmath>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {

namespace {

using ::testing::Eq;

class NonConstantScoreExpression : public ScoreExpression {
 public:
  static std::unique_ptr<NonConstantScoreExpression> Create() {
    return std::make_unique<NonConstantScoreExpression>();
  }

  libtextclassifier3::StatusOr<double> eval(
      const DocHitInfo&, const DocHitInfoIterator*) override {
    return 0;
  }

  bool is_constant_double() const override { return false; }
};

template <typename... Args>
std::vector<std::unique_ptr<ScoreExpression>> MakeChildren(Args... args) {
  std::vector<std::unique_ptr<ScoreExpression>> children;
  (children.push_back(std::move(args)), ...);
  return children;
}

TEST(ScoreExpressionTest, OperatorSimplification) {
  // 1 + 1 = 2
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScoreExpression> expression,
      OperatorScoreExpression::Create(
          OperatorScoreExpression::OperatorType::kPlus,
          MakeChildren(ConstantScoreExpression::Create(1),
                       ConstantScoreExpression::Create(1))));
  ASSERT_TRUE(expression->is_constant_double());
  EXPECT_THAT(expression->eval(DocHitInfo(), nullptr), IsOkAndHolds(Eq(2)));

  // 1 - 2 - 3 = -4
  ICING_ASSERT_OK_AND_ASSIGN(
      expression, OperatorScoreExpression::Create(
                      OperatorScoreExpression::OperatorType::kMinus,
                      MakeChildren(ConstantScoreExpression::Create(1),
                                   ConstantScoreExpression::Create(2),
                                   ConstantScoreExpression::Create(3))));
  ASSERT_TRUE(expression->is_constant_double());
  EXPECT_THAT(expression->eval(DocHitInfo(), nullptr), IsOkAndHolds(Eq(-4)));

  // 1 * 2 * 3 * 4 = 24
  ICING_ASSERT_OK_AND_ASSIGN(
      expression, OperatorScoreExpression::Create(
                      OperatorScoreExpression::OperatorType::kTimes,
                      MakeChildren(ConstantScoreExpression::Create(1),
                                   ConstantScoreExpression::Create(2),
                                   ConstantScoreExpression::Create(3),
                                   ConstantScoreExpression::Create(4))));
  ASSERT_TRUE(expression->is_constant_double());
  EXPECT_THAT(expression->eval(DocHitInfo(), nullptr), IsOkAndHolds(Eq(24)));

  // 1 / 2 / 4 = 0.125
  ICING_ASSERT_OK_AND_ASSIGN(
      expression, OperatorScoreExpression::Create(
                      OperatorScoreExpression::OperatorType::kDiv,
                      MakeChildren(ConstantScoreExpression::Create(1),
                                   ConstantScoreExpression::Create(2),
                                   ConstantScoreExpression::Create(4))));
  ASSERT_TRUE(expression->is_constant_double());
  EXPECT_THAT(expression->eval(DocHitInfo(), nullptr), IsOkAndHolds(Eq(0.125)));

  // -(2) = -2
  ICING_ASSERT_OK_AND_ASSIGN(
      expression, OperatorScoreExpression::Create(
                      OperatorScoreExpression::OperatorType::kNegative,
                      MakeChildren(ConstantScoreExpression::Create(2))));
  ASSERT_TRUE(expression->is_constant_double());
  EXPECT_THAT(expression->eval(DocHitInfo(), nullptr), IsOkAndHolds(Eq(-2)));
}

TEST(ScoreExpressionTest, MathFunctionSimplification) {
  // pow(2, 2) = 4
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScoreExpression> expression,
      MathFunctionScoreExpression::Create(
          MathFunctionScoreExpression::FunctionType::kPow,
          MakeChildren(ConstantScoreExpression::Create(2),
                       ConstantScoreExpression::Create(2))));
  ASSERT_TRUE(expression->is_constant_double());
  EXPECT_THAT(expression->eval(DocHitInfo(), nullptr), IsOkAndHolds(Eq(4)));

  // abs(-2) = 2
  ICING_ASSERT_OK_AND_ASSIGN(
      expression, MathFunctionScoreExpression::Create(
                      MathFunctionScoreExpression::FunctionType::kAbs,
                      MakeChildren(ConstantScoreExpression::Create(-2))));
  ASSERT_TRUE(expression->is_constant_double());
  EXPECT_THAT(expression->eval(DocHitInfo(), nullptr), IsOkAndHolds(Eq(2)));

  // log(e) = 1
  ICING_ASSERT_OK_AND_ASSIGN(
      expression, MathFunctionScoreExpression::Create(
                      MathFunctionScoreExpression::FunctionType::kLog,
                      MakeChildren(ConstantScoreExpression::Create(M_E))));
  ASSERT_TRUE(expression->is_constant_double());
  EXPECT_THAT(expression->eval(DocHitInfo(), nullptr), IsOkAndHolds(Eq(1)));
}

TEST(ScoreExpressionTest, CannotSimplifyNonConstant) {
  // 1 + non_constant = non_constant
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScoreExpression> expression,
      OperatorScoreExpression::Create(
          OperatorScoreExpression::OperatorType::kPlus,
          MakeChildren(ConstantScoreExpression::Create(1),
                       NonConstantScoreExpression::Create())));
  ASSERT_FALSE(expression->is_constant_double());

  // non_constant * non_constant = non_constant
  ICING_ASSERT_OK_AND_ASSIGN(
      expression, OperatorScoreExpression::Create(
                      OperatorScoreExpression::OperatorType::kTimes,
                      MakeChildren(NonConstantScoreExpression::Create(),
                                   NonConstantScoreExpression::Create())));
  ASSERT_FALSE(expression->is_constant_double());

  // -(non_constant) = non_constant
  ICING_ASSERT_OK_AND_ASSIGN(
      expression, OperatorScoreExpression::Create(
                      OperatorScoreExpression::OperatorType::kNegative,
                      MakeChildren(NonConstantScoreExpression::Create())));
  ASSERT_FALSE(expression->is_constant_double());

  // pow(non_constant, 2) = non_constant
  ICING_ASSERT_OK_AND_ASSIGN(
      expression, MathFunctionScoreExpression::Create(
                      MathFunctionScoreExpression::FunctionType::kPow,
                      MakeChildren(NonConstantScoreExpression::Create(),
                                   ConstantScoreExpression::Create(2))));
  ASSERT_FALSE(expression->is_constant_double());

  // abs(non_constant) = non_constant
  ICING_ASSERT_OK_AND_ASSIGN(
      expression, MathFunctionScoreExpression::Create(
                      MathFunctionScoreExpression::FunctionType::kAbs,
                      MakeChildren(NonConstantScoreExpression::Create())));
  ASSERT_FALSE(expression->is_constant_double());

  // log(non_constant) = non_constant
  ICING_ASSERT_OK_AND_ASSIGN(
      expression, MathFunctionScoreExpression::Create(
                      MathFunctionScoreExpression::FunctionType::kLog,
                      MakeChildren(NonConstantScoreExpression::Create())));
  ASSERT_FALSE(expression->is_constant_double());
}

}  // namespace

}  // namespace lib
}  // namespace icing
