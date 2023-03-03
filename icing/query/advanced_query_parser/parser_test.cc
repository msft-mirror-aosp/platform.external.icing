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

#include "icing/query/advanced_query_parser/parser.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/query/advanced_query_parser/abstract-syntax-tree-test-utils.h"
#include "icing/query/advanced_query_parser/abstract-syntax-tree.h"
#include "icing/query/advanced_query_parser/lexer.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::ElementsAreArray;
using ::testing::IsNull;

TEST(ParserTest, EmptyQuery) {
  std::vector<Lexer::LexerToken> lexer_tokens;
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());
  EXPECT_THAT(tree_root, IsNull());
}

TEST(ParserTest, EmptyScoring) {
  std::vector<Lexer::LexerToken> lexer_tokens;
  Parser parser = Parser::Create(std::move(lexer_tokens));
  EXPECT_THAT(parser.ConsumeScoring(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(ParserTest, SingleTerm) {
  // Query: "foo"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"foo", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());

  // Expected AST:
  // member
  //   |
  //  text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  // SimpleVisitor ordering
  //   { text, member }
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("foo", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember)));
}

TEST(ParserTest, ImplicitAnd) {
  // Query: "foo bar"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"foo", Lexer::TokenType::TEXT}, {"bar", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());

  // Expected AST:
  //      AND
  //     /   \
  // member  member
  //   |       |
  //  text    text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  // SimpleVisitor ordering
  //   { text, member, text, member, AND }
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("foo", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("bar", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("AND", NodeType::kNaryOperator)));
}

TEST(ParserTest, Or) {
  // Query: "foo OR bar"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"foo", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::OR},
      {"bar", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());

  // Expected AST:
  //      OR
  //     /   \
  // member  member
  //   |       |
  //  text    text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  // SimpleVisitor ordering
  //   { text, member, text, member, OR }
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("foo", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("bar", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("OR", NodeType::kNaryOperator)));
}

TEST(ParserTest, And) {
  // Query: "foo AND bar"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"foo", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::AND},
      {"bar", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());

  // Expected AST:
  //      AND
  //     /   \
  // member  member
  //   |       |
  //  text    text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  // SimpleVisitor ordering
  //   { text, member, text, member, AND }
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("foo", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("bar", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("AND", NodeType::kNaryOperator)));
}

TEST(ParserTest, Not) {
  // Query: "NOT foo"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"", Lexer::TokenType::NOT}, {"foo", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());

  // Expected AST:
  //  NOT
  //   |
  // member
  //   |
  //  text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  // SimpleVisitor ordering
  //   { text, member, NOT }
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("foo", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("NOT", NodeType::kUnaryOperator)));
}

TEST(ParserTest, Minus) {
  // Query: "-foo"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"", Lexer::TokenType::MINUS}, {"foo", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());

  // Expected AST:
  //  MINUS
  //   |
  // member
  //   |
  //  text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  // SimpleVisitor ordering
  //   { text, member, MINUS }
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("foo", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("MINUS", NodeType::kUnaryOperator)));
}

TEST(ParserTest, Has) {
  // Query: "subject:foo"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"subject", Lexer::TokenType::TEXT},
      {":", Lexer::TokenType::COMPARATOR},
      {"foo", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());

  // Expected AST:
  //        :
  //      /   \
  // member  member
  //   |       |
  //  text    text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  // SimpleVisitor ordering
  //   { text, member, text, member, binaryOp }
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("subject", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("foo", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo(":", NodeType::kNaryOperator)));
}

TEST(ParserTest, HasNested) {
  // Query: "sender.name:foo"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"sender", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::DOT},
      {"name", Lexer::TokenType::TEXT},
      {":", Lexer::TokenType::COMPARATOR},
      {"foo", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());

  // Expected AST:
  //        :
  //      /   \
  //   member  member
  //   /   \     |
  // text text  text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  // SimpleVisitor ordering
  //   { text, text, member, text, member, binaryOp }
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("sender", NodeType::kText),
                          EqualsNodeInfo("name", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("foo", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo(":", NodeType::kNaryOperator)));
}

TEST(ParserTest, EmptyFunction) {
  // Query: "foo()"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"foo", Lexer::TokenType::FUNCTION_NAME},
      {"", Lexer::TokenType::LPAREN},
      {"", Lexer::TokenType::RPAREN}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());

  // Expected AST:
  //    function
  //       |
  // function_name
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  // SimpleVisitor ordering
  //   { function_name, function }
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("foo", NodeType::kFunctionName),
                          EqualsNodeInfo("", NodeType::kFunction)));
}

TEST(ParserTest, FunctionSingleArg) {
  // Query: "foo("bar")"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"foo", Lexer::TokenType::FUNCTION_NAME},
      {"", Lexer::TokenType::LPAREN},
      {"bar", Lexer::TokenType::STRING},
      {"", Lexer::TokenType::RPAREN}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());

  // Expected AST:
  //           function
  //           /     \
  // function_name  string
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  // SimpleVisitor ordering
  //   { function_name, string, function }
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("foo", NodeType::kFunctionName),
                          EqualsNodeInfo("bar", NodeType::kString),
                          EqualsNodeInfo("", NodeType::kFunction)));
}

TEST(ParserTest, FunctionMultiArg) {
  // Query: "foo("bar", "baz")"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"foo", Lexer::TokenType::FUNCTION_NAME}, {"", Lexer::TokenType::LPAREN},
      {"bar", Lexer::TokenType::STRING},        {"", Lexer::TokenType::COMMA},
      {"baz", Lexer::TokenType::STRING},        {"", Lexer::TokenType::RPAREN}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());

  // Expected AST:
  //                function
  //              /    |    \
  // function_name  string  string
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  // SimpleVisitor ordering
  //   { function_name, string, string, function }
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("foo", NodeType::kFunctionName),
                          EqualsNodeInfo("bar", NodeType::kString),
                          EqualsNodeInfo("baz", NodeType::kString),
                          EqualsNodeInfo("", NodeType::kFunction)));
}

TEST(ParserTest, FunctionNested) {
  // Query: "foo(bar())"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"foo", Lexer::TokenType::FUNCTION_NAME}, {"", Lexer::TokenType::LPAREN},
      {"bar", Lexer::TokenType::FUNCTION_NAME}, {"", Lexer::TokenType::LPAREN},
      {"", Lexer::TokenType::RPAREN},           {"", Lexer::TokenType::RPAREN}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());

  // Expected AST:
  //          function
  //          /      \
  // function_name  function
  //                    |
  //              function_name
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  // SimpleVisitor ordering
  //   { function_name, function_name, function, function }
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("foo", NodeType::kFunctionName),
                          EqualsNodeInfo("bar", NodeType::kFunctionName),
                          EqualsNodeInfo("", NodeType::kFunction),
                          EqualsNodeInfo("", NodeType::kFunction)));
}

TEST(ParserTest, FunctionWithTrailingSequence) {
  // Query: "foo() OR bar"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"foo", Lexer::TokenType::FUNCTION_NAME},
      {"", Lexer::TokenType::LPAREN},
      {"", Lexer::TokenType::RPAREN},
      {"", Lexer::TokenType::OR},
      {"bar", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());

  // Expected AST:
  //             OR
  //          /      \
  //     function   member
  //        |         |
  //  function_name  text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  // SimpleVisitor ordering
  //   { function_name, function, text, member, OR }
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("foo", NodeType::kFunctionName),
                          EqualsNodeInfo("", NodeType::kFunction),
                          EqualsNodeInfo("bar", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("OR", NodeType::kNaryOperator)));
}

TEST(ParserTest, Composite) {
  // Query: "foo OR (bar baz)"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"foo", Lexer::TokenType::TEXT}, {"", Lexer::TokenType::OR},
      {"", Lexer::TokenType::LPAREN},  {"bar", Lexer::TokenType::TEXT},
      {"baz", Lexer::TokenType::TEXT}, {"", Lexer::TokenType::RPAREN}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());

  // Expected AST:
  //             OR
  //          /      \
  //     member      AND
  //       |        /   \
  //     text    member member
  //                |     |
  //              text   text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  // SimpleVisitor ordering
  //   { text, member, text, member, text, member, AND, OR }
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("foo", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("bar", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("baz", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("AND", NodeType::kNaryOperator),
                          EqualsNodeInfo("OR", NodeType::kNaryOperator)));
}

TEST(ParserTest, CompositeWithTrailingSequence) {
  // Query: "(bar baz) OR foo"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"", Lexer::TokenType::LPAREN},  {"bar", Lexer::TokenType::TEXT},
      {"baz", Lexer::TokenType::TEXT}, {"", Lexer::TokenType::RPAREN},
      {"", Lexer::TokenType::OR},      {"foo", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());

  // Expected AST:
  //             OR
  //          /      \
  //       AND      member
  //      /   \       |
  //  member member  text
  //    |     |
  //  text   text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  // SimpleVisitor ordering
  //   { text, member, text, member, AND, text, member, OR }
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("bar", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("baz", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("AND", NodeType::kNaryOperator),
                          EqualsNodeInfo("foo", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("OR", NodeType::kNaryOperator)));
}

TEST(ParserTest, Complex) {
  // Query: "foo bar:baz OR pal("bat")"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"foo", Lexer::TokenType::TEXT},
      {"bar", Lexer::TokenType::TEXT},
      {":", Lexer::TokenType::COMPARATOR},
      {"baz", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::OR},
      {"pal", Lexer::TokenType::FUNCTION_NAME},
      {"", Lexer::TokenType::LPAREN},
      {"bat", Lexer::TokenType::STRING},
      {"", Lexer::TokenType::RPAREN}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());

  // Expected AST:
  //               AND
  //            /        \
  //     member            OR
  //       |          /         \
  //     text      :             function
  //              / \            /       \
  //         member member  function_name string
  //           |       |
  //          text    text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  // SimpleVisitor ordering
  //   { text, member, text, member, text, member, :, function_name, string,
  //     function, OR, AND }
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("foo", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("bar", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("baz", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo(":", NodeType::kNaryOperator),
                          EqualsNodeInfo("pal", NodeType::kFunctionName),
                          EqualsNodeInfo("bat", NodeType::kString),
                          EqualsNodeInfo("", NodeType::kFunction),
                          EqualsNodeInfo("OR", NodeType::kNaryOperator),
                          EqualsNodeInfo("AND", NodeType::kNaryOperator)));
}

TEST(ParserTest, InvalidHas) {
  // Query: "foo:"  No right hand operand to :
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"foo", Lexer::TokenType::TEXT}, {":", Lexer::TokenType::COMPARATOR}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  EXPECT_THAT(parser.ConsumeQuery(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(ParserTest, InvalidComposite) {
  // Query: "(foo bar"  No terminating RPAREN
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"", Lexer::TokenType::LPAREN},
      {"foo", Lexer::TokenType::TEXT},
      {"bar", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  EXPECT_THAT(parser.ConsumeQuery(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(ParserTest, InvalidMember) {
  // Query: "foo."  DOT must have succeeding TEXT
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"foo", Lexer::TokenType::TEXT}, {"", Lexer::TokenType::DOT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  EXPECT_THAT(parser.ConsumeQuery(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(ParserTest, InvalidOr) {
  // Query: "foo OR"   No right hand operand to OR
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"foo", Lexer::TokenType::TEXT}, {"", Lexer::TokenType::OR}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  EXPECT_THAT(parser.ConsumeQuery(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(ParserTest, InvalidAnd) {
  // Query: "foo AND"   No right hand operand to AND
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"foo", Lexer::TokenType::TEXT}, {"", Lexer::TokenType::AND}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  EXPECT_THAT(parser.ConsumeQuery(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(ParserTest, InvalidNot) {
  // Query: "NOT"   No right hand operand to NOT
  std::vector<Lexer::LexerToken> lexer_tokens = {{"", Lexer::TokenType::NOT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  EXPECT_THAT(parser.ConsumeQuery(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(ParserTest, InvalidMinus) {
  // Query: "-"   No right hand operand to -
  std::vector<Lexer::LexerToken> lexer_tokens = {{"", Lexer::TokenType::MINUS}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  EXPECT_THAT(parser.ConsumeQuery(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(ParserTest, InvalidFunctionCallNoRparen) {
  // Query: "foo("   No terminating RPAREN
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"foo", Lexer::TokenType::FUNCTION_NAME}, {"", Lexer::TokenType::LPAREN}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  EXPECT_THAT(parser.ConsumeQuery(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(ParserTest, InvalidFunctionCallNoLparen) {
  // Query: "foo bar"   foo labeled FUNCTION_NAME despite no LPAREN
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"foo", Lexer::TokenType::FUNCTION_NAME},
      {"bar", Lexer::TokenType::FUNCTION_NAME}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  EXPECT_THAT(parser.ConsumeQuery(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(ParserTest, InvalidFunctionArgsHangingComma) {
  // Query: "foo("bar",)"   no valid arg following COMMA
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"foo", Lexer::TokenType::FUNCTION_NAME},
      {"", Lexer::TokenType::LPAREN},
      {"bar", Lexer::TokenType::STRING},
      {"", Lexer::TokenType::COMMA},
      {"", Lexer::TokenType::RPAREN}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  EXPECT_THAT(parser.ConsumeQuery(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(ParserTest, ScoringPlus) {
  // Scoring: "1 + 1 + 1"
  std::vector<Lexer::LexerToken> lexer_tokens = {{"1", Lexer::TokenType::TEXT},
                                                 {"", Lexer::TokenType::PLUS},
                                                 {"1", Lexer::TokenType::TEXT},
                                                 {"", Lexer::TokenType::PLUS},
                                                 {"1", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeScoring());

  // Expected AST:
  //           PLUS
  //     /      |       \
  // member   member   member
  //   |        |        |
  //  text     text     text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("1", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("1", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("1", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("PLUS", NodeType::kNaryOperator)));
}

TEST(ParserTest, ScoringMinus) {
  // Scoring: "1 - 1 - 1"
  std::vector<Lexer::LexerToken> lexer_tokens = {{"1", Lexer::TokenType::TEXT},
                                                 {"", Lexer::TokenType::MINUS},
                                                 {"1", Lexer::TokenType::TEXT},
                                                 {"", Lexer::TokenType::MINUS},
                                                 {"1", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeScoring());

  // Expected AST:
  //          MINUS
  //     /      |       \
  // member   member   member
  //   |        |        |
  //  text     text     text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("1", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("1", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("1", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("MINUS", NodeType::kNaryOperator)));
}

TEST(ParserTest, ScoringUnaryMinus) {
  // Scoring: "1 + -1 + 1"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"1", Lexer::TokenType::TEXT}, {"", Lexer::TokenType::PLUS},
      {"", Lexer::TokenType::MINUS}, {"1", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::PLUS},  {"1", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeScoring());

  // Expected AST:
  //          PLUS
  //     /      |      \
  // member   MINUS   member
  //   |        |        |
  //  text    member    text
  //            |
  //           text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("1", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("1", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("MINUS", NodeType::kUnaryOperator),
                          EqualsNodeInfo("1", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("PLUS", NodeType::kNaryOperator)));
}

TEST(ParserTest, ScoringPlusMinus) {
  // Scoring: "11 + 12 - 13 + 14"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"11", Lexer::TokenType::TEXT}, {"", Lexer::TokenType::PLUS},
      {"12", Lexer::TokenType::TEXT}, {"", Lexer::TokenType::MINUS},
      {"13", Lexer::TokenType::TEXT}, {"", Lexer::TokenType::PLUS},
      {"14", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeScoring());

  // Expected AST:
  //                          PLUS
  //                       /        \
  //                MINUS          member
  //             /         \          |
  //       PLUS           member     text
  //     /      \            |
  // member    member       text
  //   |         |
  //  text      text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("11", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("12", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("PLUS", NodeType::kNaryOperator),
                          EqualsNodeInfo("13", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("MINUS", NodeType::kNaryOperator),
                          EqualsNodeInfo("14", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("PLUS", NodeType::kNaryOperator)));
}

TEST(ParserTest, ScoringTimes) {
  // Scoring: "1 * 1 * 1"
  std::vector<Lexer::LexerToken> lexer_tokens = {{"1", Lexer::TokenType::TEXT},
                                                 {"", Lexer::TokenType::TIMES},
                                                 {"1", Lexer::TokenType::TEXT},
                                                 {"", Lexer::TokenType::TIMES},
                                                 {"1", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeScoring());

  // Expected AST:
  //           TIMES
  //     /      |       \
  // member   member   member
  //   |        |        |
  //  text     text     text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("1", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("1", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("1", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("TIMES", NodeType::kNaryOperator)));
}

TEST(ParserTest, ScoringDiv) {
  // Scoring: "1 / 1 / 1"
  std::vector<Lexer::LexerToken> lexer_tokens = {{"1", Lexer::TokenType::TEXT},
                                                 {"", Lexer::TokenType::DIV},
                                                 {"1", Lexer::TokenType::TEXT},
                                                 {"", Lexer::TokenType::DIV},
                                                 {"1", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeScoring());

  // Expected AST:
  //           DIV
  //     /      |       \
  // member   member   member
  //   |        |        |
  //  text     text     text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("1", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("1", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("1", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("DIV", NodeType::kNaryOperator)));
}

TEST(ParserTest, ScoringTimesDiv) {
  // Scoring: "11 / 12 * 13 / 14 / 15"
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"11", Lexer::TokenType::TEXT}, {"", Lexer::TokenType::DIV},
      {"12", Lexer::TokenType::TEXT}, {"", Lexer::TokenType::TIMES},
      {"13", Lexer::TokenType::TEXT}, {"", Lexer::TokenType::DIV},
      {"14", Lexer::TokenType::TEXT}, {"", Lexer::TokenType::DIV},
      {"15", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeScoring());

  // Expected AST:
  //                                DIV
  //                        /        |         \
  //                 TIMES          member    member
  //             /          \        |           |
  //        DIV           member    text        text
  //     /      \            |
  // member    member       text
  //   |         |
  //  text      text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("11", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("12", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("DIV", NodeType::kNaryOperator),
                          EqualsNodeInfo("13", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("TIMES", NodeType::kNaryOperator),
                          EqualsNodeInfo("14", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("15", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("DIV", NodeType::kNaryOperator)));
}

TEST(ParserTest, ComplexScoring) {
  // Scoring: "1 + pow((2 * sin(3)), 4) + -5 / 6"
  // With parentheses in function arguments.
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"1", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::PLUS},
      {"pow", Lexer::TokenType::FUNCTION_NAME},
      {"", Lexer::TokenType::LPAREN},
      {"", Lexer::TokenType::LPAREN},
      {"2", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::TIMES},
      {"sin", Lexer::TokenType::FUNCTION_NAME},
      {"", Lexer::TokenType::LPAREN},
      {"3", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::RPAREN},
      {"", Lexer::TokenType::RPAREN},
      {"", Lexer::TokenType::COMMA},
      {"4", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::RPAREN},
      {"", Lexer::TokenType::PLUS},
      {"", Lexer::TokenType::MINUS},
      {"5", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::DIV},
      {"6", Lexer::TokenType::TEXT},
  };
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeScoring());
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  std::vector<NodeInfo> node = visitor.nodes();
  EXPECT_THAT(node,
              ElementsAre(EqualsNodeInfo("1", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("pow", NodeType::kFunctionName),
                          EqualsNodeInfo("2", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("sin", NodeType::kFunctionName),
                          EqualsNodeInfo("3", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("", NodeType::kFunction),
                          EqualsNodeInfo("TIMES", NodeType::kNaryOperator),
                          EqualsNodeInfo("4", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("", NodeType::kFunction),
                          EqualsNodeInfo("5", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("MINUS", NodeType::kUnaryOperator),
                          EqualsNodeInfo("6", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("DIV", NodeType::kNaryOperator),
                          EqualsNodeInfo("PLUS", NodeType::kNaryOperator)));

  // Scoring: "1 + pow(2 * sin(3), 4) + -5 / 6"
  // Without parentheses in function arguments.
  lexer_tokens = {
      {"1", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::PLUS},
      {"pow", Lexer::TokenType::FUNCTION_NAME},
      {"", Lexer::TokenType::LPAREN},
      {"2", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::TIMES},
      {"sin", Lexer::TokenType::FUNCTION_NAME},
      {"", Lexer::TokenType::LPAREN},
      {"3", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::RPAREN},
      {"", Lexer::TokenType::COMMA},
      {"4", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::RPAREN},
      {"", Lexer::TokenType::PLUS},
      {"", Lexer::TokenType::MINUS},
      {"5", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::DIV},
      {"6", Lexer::TokenType::TEXT},
  };
  parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(tree_root, parser.ConsumeScoring());
  visitor = SimpleVisitor();
  tree_root->Accept(&visitor);
  EXPECT_THAT(visitor.nodes(), ElementsAreArray(node));
}

TEST(ParserTest, ScoringMemberFunction) {
  // Scoring: this.CreationTimestamp()
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"this", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::DOT},
      {"CreationTimestamp", Lexer::TokenType::FUNCTION_NAME},
      {"", Lexer::TokenType::LPAREN},
      {"", Lexer::TokenType::RPAREN}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeScoring());

  // Expected AST:
  //       member
  //     /        \
  //  text     function
  //               |
  //          function_name
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  EXPECT_THAT(
      visitor.nodes(),
      ElementsAre(EqualsNodeInfo("this", NodeType::kText),
                  EqualsNodeInfo("CreationTimestamp", NodeType::kFunctionName),
                  EqualsNodeInfo("", NodeType::kFunction),
                  EqualsNodeInfo("", NodeType::kMember)));
}

TEST(ParserTest, QueryMemberFunction) {
  // Query: this.foo()
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"this", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::DOT},
      {"foo", Lexer::TokenType::FUNCTION_NAME},
      {"", Lexer::TokenType::LPAREN},
      {"", Lexer::TokenType::RPAREN}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());

  // Expected AST:
  //       member
  //     /        \
  //  text     function
  //               |
  //          function_name
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("this", NodeType::kText),
                          EqualsNodeInfo("foo", NodeType::kFunctionName),
                          EqualsNodeInfo("", NodeType::kFunction),
                          EqualsNodeInfo("", NodeType::kMember)));
}

TEST(ParserTest, ScoringComplexMemberFunction) {
  // Scoring: a.b.fun(c, d)
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"a", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::DOT},
      {"b", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::DOT},
      {"fun", Lexer::TokenType::FUNCTION_NAME},
      {"", Lexer::TokenType::LPAREN},
      {"c", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::COMMA},
      {"d", Lexer::TokenType::TEXT},
      {"", Lexer::TokenType::RPAREN}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeScoring());

  // Expected AST:
  //                member
  //         /        |          \
  //  text          text         function
  //                        /        |       \
  //               function_name   member    member
  //                                 |         |
  //                                text      text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("a", NodeType::kText),
                          EqualsNodeInfo("b", NodeType::kText),
                          EqualsNodeInfo("fun", NodeType::kFunctionName),
                          EqualsNodeInfo("c", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("d", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("", NodeType::kFunction),
                          EqualsNodeInfo("", NodeType::kMember)));
}

TEST(ParserTest, QueryComplexMemberFunction) {
  // Query: this.abc.fun(def, ghi)
  std::vector<Lexer::LexerToken> lexer_tokens = {
      {"this", Lexer::TokenType::TEXT},         {"", Lexer::TokenType::DOT},
      {"abc", Lexer::TokenType::TEXT},          {"", Lexer::TokenType::DOT},
      {"fun", Lexer::TokenType::FUNCTION_NAME}, {"", Lexer::TokenType::LPAREN},
      {"def", Lexer::TokenType::TEXT},          {"", Lexer::TokenType::COMMA},
      {"ghi", Lexer::TokenType::TEXT},          {"", Lexer::TokenType::RPAREN}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> tree_root,
                             parser.ConsumeQuery());

  // Expected AST:
  //                member
  //         /        |          \
  //  text          text         function
  //                        /        |       \
  //               function_name   member    member
  //                                 |         |
  //                                text      text
  SimpleVisitor visitor;
  tree_root->Accept(&visitor);
  EXPECT_THAT(visitor.nodes(),
              ElementsAre(EqualsNodeInfo("this", NodeType::kText),
                          EqualsNodeInfo("abc", NodeType::kText),
                          EqualsNodeInfo("fun", NodeType::kFunctionName),
                          EqualsNodeInfo("def", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("ghi", NodeType::kText),
                          EqualsNodeInfo("", NodeType::kMember),
                          EqualsNodeInfo("", NodeType::kFunction),
                          EqualsNodeInfo("", NodeType::kMember)));
}

TEST(ParserTest, InvalidScoringToken) {
  // Scoring: "1 + NOT 1"
  std::vector<Lexer::LexerToken> lexer_tokens = {{"1", Lexer::TokenType::TEXT},
                                                 {"", Lexer::TokenType::PLUS},
                                                 {"", Lexer::TokenType::NOT},
                                                 {"1", Lexer::TokenType::TEXT}};
  Parser parser = Parser::Create(std::move(lexer_tokens));
  EXPECT_THAT(parser.ConsumeScoring(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

}  // namespace

}  // namespace lib
}  // namespace icing
