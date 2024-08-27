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

#include "icing/query/advanced_query_parser/abstract-syntax-tree.h"

#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/query/advanced_query_parser/abstract-syntax-tree-test-utils.h"

namespace icing {
namespace lib {
namespace {

using ::testing::ElementsAre;

TEST(AbstractSyntaxTreeTest, Simple) {
  // foo
  std::unique_ptr<Node> root = std::make_unique<TextNode>("foo");
  SimpleVisitor visitor;
  root->Accept(&visitor);

  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("foo", NodeType::kText)));
}

TEST(AbstractSyntaxTreeTest, Composite) {
  // (foo bar) OR baz
  std::vector<std::unique_ptr<Node>> and_args;
  and_args.push_back(std::make_unique<TextNode>("foo"));
  and_args.push_back(std::make_unique<TextNode>("bar"));
  auto and_node =
      std::make_unique<NaryOperatorNode>("AND", std::move(and_args));

  std::vector<std::unique_ptr<Node>> or_args;
  or_args.push_back(std::move(and_node));
  or_args.push_back(std::make_unique<TextNode>("baz"));
  std::unique_ptr<Node> root =
      std::make_unique<NaryOperatorNode>("OR", std::move(or_args));

  SimpleVisitor visitor;
  root->Accept(&visitor);

  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("foo", NodeType::kText),
                          EqualsNodeInfo("bar", NodeType::kText),
                          EqualsNodeInfo("AND", NodeType::kNaryOperator),
                          EqualsNodeInfo("baz", NodeType::kText),
                          EqualsNodeInfo("OR", NodeType::kNaryOperator)));
}

TEST(AbstractSyntaxTreeTest, Function) {
  // foo()
  std::unique_ptr<Node> root =
      std::make_unique<FunctionNode>(std::make_unique<FunctionNameNode>("foo"));
  SimpleVisitor visitor;
  root->Accept(&visitor);

  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("foo", NodeType::kFunctionName),
                          EqualsNodeInfo("", NodeType::kFunction)));

  // foo("bar")
  std::vector<std::unique_ptr<Node>> args;
  args.push_back(std::make_unique<StringNode>("bar"));
  root = std::make_unique<FunctionNode>(
      std::make_unique<FunctionNameNode>("foo"), std::move(args));
  visitor = SimpleVisitor();
  root->Accept(&visitor);

  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("foo", NodeType::kFunctionName),
                          EqualsNodeInfo("bar", NodeType::kString),
                          EqualsNodeInfo("", NodeType::kFunction)));

  // foo(bar("baz"))
  std::vector<std::unique_ptr<Node>> inner_args;
  inner_args.push_back(std::make_unique<StringNode>("baz"));
  args.clear();
  args.push_back(std::make_unique<FunctionNode>(
      std::make_unique<FunctionNameNode>("bar"), std::move(inner_args)));
  root = std::make_unique<FunctionNode>(
      std::make_unique<FunctionNameNode>("foo"), std::move(args));
  visitor = SimpleVisitor();
  root->Accept(&visitor);

  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("foo", NodeType::kFunctionName),
                          EqualsNodeInfo("bar", NodeType::kFunctionName),
                          EqualsNodeInfo("baz", NodeType::kString),
                          EqualsNodeInfo("", NodeType::kFunction),
                          EqualsNodeInfo("", NodeType::kFunction)));
}

TEST(AbstractSyntaxTreeTest, Restriction) {
  // sender.name:(IMPORTANT OR URGENT)
  std::vector<std::unique_ptr<TextNode>> member_args;
  member_args.push_back(std::make_unique<TextNode>("sender"));
  member_args.push_back(std::make_unique<TextNode>("name"));

  std::vector<std::unique_ptr<Node>> or_args;
  or_args.push_back(std::make_unique<TextNode>("IMPORTANT"));
  or_args.push_back(std::make_unique<TextNode>("URGENT"));

  std::vector<std::unique_ptr<Node>> has_args;
  has_args.push_back(std::make_unique<MemberNode>(std::move(member_args),
                                                  /*function=*/nullptr));
  has_args.push_back(
      std::make_unique<NaryOperatorNode>("OR", std::move(or_args)));

  std::unique_ptr<Node> root =
      std::make_unique<NaryOperatorNode>(":", std::move(has_args));

  SimpleVisitor visitor;
  root->Accept(&visitor);

  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("sender", NodeType::kText),
                          EqualsNodeInfo("name", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("IMPORTANT", NodeType::kText),
                          EqualsNodeInfo("URGENT", NodeType::kText),
                          EqualsNodeInfo("OR", NodeType::kNaryOperator),
                          EqualsNodeInfo(":", NodeType::kNaryOperator)));
}

}  // namespace
}  // namespace lib
}  // namespace icing
