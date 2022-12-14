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

#include "icing/query/advanced_query_parser/query-visitor.h"

#include <cstdint>
#include <limits>
#include <memory>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/index/iterator/doc-hit-info-iterator-test-util.h"
#include "icing/index/numeric/dummy-numeric-index.h"
#include "icing/index/numeric/numeric-index.h"
#include "icing/query/advanced_query_parser/abstract-syntax-tree.h"
#include "icing/query/advanced_query_parser/lexer.h"
#include "icing/query/advanced_query_parser/parser.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;

constexpr DocumentId kDocumentId0 = 0;
constexpr DocumentId kDocumentId1 = 1;
constexpr DocumentId kDocumentId2 = 2;

constexpr SectionId kSectionId0 = 0;
constexpr SectionId kSectionId1 = 1;
constexpr SectionId kSectionId2 = 2;

TEST(QueryVisitorTest, SimpleLessThan) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values 0, 1 and 2
  // respectively.
  DummyNumericIndex<int64_t> numeric_index;
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index.Edit("price", kDocumentId0, kSectionId0);
  editor->BufferKey(0);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("price", kDocumentId1, kSectionId1);
  editor->BufferKey(1);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("price", kDocumentId2, kSectionId2);
  editor->BufferKey(2);
  editor->IndexAllBufferedKeys();

  std::string query = "price < 2";
  Lexer lexer(query, Lexer::Language::QUERY);
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Lexer::LexerToken> lexer_tokens,
                             lexer.ExtractTokens());
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             parser.ConsumeQuery());

  // Retrieve the root_iterator from the visitor.
  QueryVisitor query_visitor(&numeric_index);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());

  EXPECT_THAT(GetDocumentIds(root_iterator.get()),
              ElementsAre(kDocumentId1, kDocumentId0));
}

TEST(QueryVisitorTest, SimpleLessThanEq) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values 0, 1 and 2
  // respectively.
  DummyNumericIndex<int64_t> numeric_index;
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index.Edit("price", kDocumentId0, kSectionId0);
  editor->BufferKey(0);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("price", kDocumentId1, kSectionId1);
  editor->BufferKey(1);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("price", kDocumentId2, kSectionId2);
  editor->BufferKey(2);
  editor->IndexAllBufferedKeys();

  std::string query = "price <= 1";
  Lexer lexer(query, Lexer::Language::QUERY);
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Lexer::LexerToken> lexer_tokens,
                             lexer.ExtractTokens());
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             parser.ConsumeQuery());

  // Retrieve the root_iterator from the visitor.
  QueryVisitor query_visitor(&numeric_index);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());

  EXPECT_THAT(GetDocumentIds(root_iterator.get()),
              ElementsAre(kDocumentId1, kDocumentId0));
}

TEST(QueryVisitorTest, SimpleEqual) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values 0, 1 and 2
  // respectively.
  DummyNumericIndex<int64_t> numeric_index;
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index.Edit("price", kDocumentId0, kSectionId0);
  editor->BufferKey(0);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("price", kDocumentId1, kSectionId1);
  editor->BufferKey(1);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("price", kDocumentId2, kSectionId2);
  editor->BufferKey(2);
  editor->IndexAllBufferedKeys();

  std::string query = "price == 2";
  Lexer lexer(query, Lexer::Language::QUERY);
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Lexer::LexerToken> lexer_tokens,
                             lexer.ExtractTokens());
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             parser.ConsumeQuery());

  // Retrieve the root_iterator from the visitor.
  QueryVisitor query_visitor(&numeric_index);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());

  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId2));
}

TEST(QueryVisitorTest, SimpleGreaterThanEq) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values 0, 1 and 2
  // respectively.
  DummyNumericIndex<int64_t> numeric_index;
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index.Edit("price", kDocumentId0, kSectionId0);
  editor->BufferKey(0);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("price", kDocumentId1, kSectionId1);
  editor->BufferKey(1);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("price", kDocumentId2, kSectionId2);
  editor->BufferKey(2);
  editor->IndexAllBufferedKeys();

  std::string query = "price >= 1";
  Lexer lexer(query, Lexer::Language::QUERY);
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Lexer::LexerToken> lexer_tokens,
                             lexer.ExtractTokens());
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             parser.ConsumeQuery());

  // Retrieve the root_iterator from the visitor.
  QueryVisitor query_visitor(&numeric_index);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());

  EXPECT_THAT(GetDocumentIds(root_iterator.get()),
              ElementsAre(kDocumentId2, kDocumentId1));
}

TEST(QueryVisitorTest, SimpleGreaterThan) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values 0, 1 and 2
  // respectively.
  DummyNumericIndex<int64_t> numeric_index;
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index.Edit("price", kDocumentId0, kSectionId0);
  editor->BufferKey(0);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("price", kDocumentId1, kSectionId1);
  editor->BufferKey(1);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("price", kDocumentId2, kSectionId2);
  editor->BufferKey(2);
  editor->IndexAllBufferedKeys();

  std::string query = "price > 1";
  Lexer lexer(query, Lexer::Language::QUERY);
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Lexer::LexerToken> lexer_tokens,
                             lexer.ExtractTokens());
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             parser.ConsumeQuery());

  // Retrieve the root_iterator from the visitor.
  QueryVisitor query_visitor(&numeric_index);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());

  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId2));
}

// TODO(b/208654892) Properly handle negative numbers in query expressions.
TEST(QueryVisitorTest, DISABLED_IntMinLessThanEqual) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values INT_MIN,
  // INT_MAX and INT_MIN + 1 respectively.
  int64_t int_min = std::numeric_limits<int64_t>::min();
  DummyNumericIndex<int64_t> numeric_index;
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index.Edit("price", kDocumentId0, kSectionId0);
  editor->BufferKey(int_min);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("price", kDocumentId1, kSectionId1);
  editor->BufferKey(std::numeric_limits<int64_t>::max());
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("price", kDocumentId2, kSectionId2);
  editor->BufferKey(int_min + 1);
  editor->IndexAllBufferedKeys();

  std::string query = "price <= " + std::to_string(int_min);
  Lexer lexer(query, Lexer::Language::QUERY);
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Lexer::LexerToken> lexer_tokens,
                             lexer.ExtractTokens());
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             parser.ConsumeQuery());

  // Retrieve the root_iterator from the visitor.
  QueryVisitor query_visitor(&numeric_index);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());

  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId0));
}

TEST(QueryVisitorTest, IntMaxGreaterThanEqual) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values INT_MIN,
  // INT_MAX and INT_MAX - 1 respectively.
  int64_t int_max = std::numeric_limits<int64_t>::max();
  DummyNumericIndex<int64_t> numeric_index;
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index.Edit("price", kDocumentId0, kSectionId0);
  editor->BufferKey(std::numeric_limits<int64_t>::min());
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("price", kDocumentId1, kSectionId1);
  editor->BufferKey(int_max);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("price", kDocumentId2, kSectionId2);
  editor->BufferKey(int_max - 1);
  editor->IndexAllBufferedKeys();

  std::string query = "price >= " + std::to_string(int_max);
  Lexer lexer(query, Lexer::Language::QUERY);
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Lexer::LexerToken> lexer_tokens,
                             lexer.ExtractTokens());
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             parser.ConsumeQuery());

  // Retrieve the root_iterator from the visitor.
  QueryVisitor query_visitor(&numeric_index);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());

  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId1));
}

TEST(QueryVisitorTest, NestedPropertyLessThan) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values 0, 1 and 2
  // respectively.
  DummyNumericIndex<int64_t> numeric_index;
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index.Edit("subscription.price", kDocumentId0, kSectionId0);
  editor->BufferKey(0);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("subscription.price", kDocumentId1, kSectionId1);
  editor->BufferKey(1);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("subscription.price", kDocumentId2, kSectionId2);
  editor->BufferKey(2);
  editor->IndexAllBufferedKeys();

  std::string query = "subscription.price < 2";
  Lexer lexer(query, Lexer::Language::QUERY);
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Lexer::LexerToken> lexer_tokens,
                             lexer.ExtractTokens());
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             parser.ConsumeQuery());

  // Retrieve the root_iterator from the visitor.
  QueryVisitor query_visitor(&numeric_index);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());

  EXPECT_THAT(GetDocumentIds(root_iterator.get()),
              ElementsAre(kDocumentId1, kDocumentId0));
}

TEST(QueryVisitorTest, IntParsingError) {
  DummyNumericIndex<int64_t> numeric_index;

  std::string query = "subscription.price < fruit";
  Lexer lexer(query, Lexer::Language::QUERY);
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Lexer::LexerToken> lexer_tokens,
                             lexer.ExtractTokens());
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             parser.ConsumeQuery());

  // Retrieve the root_iterator from the visitor.
  QueryVisitor query_visitor(&numeric_index);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).root(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(QueryVisitorTest, NotEqualsUnsupported) {
  DummyNumericIndex<int64_t> numeric_index;

  std::string query = "subscription.price != 3";
  Lexer lexer(query, Lexer::Language::QUERY);
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Lexer::LexerToken> lexer_tokens,
                             lexer.ExtractTokens());
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             parser.ConsumeQuery());

  // Retrieve the root_iterator from the visitor.
  QueryVisitor query_visitor(&numeric_index);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).root(),
              StatusIs(libtextclassifier3::StatusCode::UNIMPLEMENTED));
}

TEST(QueryVisitorTest, UnrecognizedOperatorTooLongUnsupported) {
  DummyNumericIndex<int64_t> numeric_index;

  // Create an AST for the query 'subscription.price !<= 3'
  std::string query = "subscription.price !<= 3";
  Lexer lexer(query, Lexer::Language::QUERY);
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Lexer::LexerToken> lexer_tokens,
                             lexer.ExtractTokens());
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             parser.ConsumeQuery());

  // There is no support for the 'not less than or equal to' operator.
  QueryVisitor query_visitor(&numeric_index);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).root(),
              StatusIs(libtextclassifier3::StatusCode::UNIMPLEMENTED));
}

TEST(QueryVisitorTest, LessThanTooManyOperandsInvalid) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values 0, 1 and 2
  // respectively.
  DummyNumericIndex<int64_t> numeric_index;
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index.Edit("subscription.price", kDocumentId0, kSectionId0);
  editor->BufferKey(0);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("subscription.price", kDocumentId1, kSectionId1);
  editor->BufferKey(1);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("subscription.price", kDocumentId2, kSectionId2);
  editor->BufferKey(2);
  editor->IndexAllBufferedKeys();

  // Create an invalid AST for the query '3 < subscription.price 25' where '<'
  // has three operands
  auto property_node = std::make_unique<TextNode>("subscription");
  auto subproperty_node = std::make_unique<TextNode>("price");
  std::vector<std::unique_ptr<TextNode>> member_args;
  member_args.push_back(std::move(property_node));
  member_args.push_back(std::move(subproperty_node));
  auto member_node = std::make_unique<MemberNode>(std::move(member_args),
                                                  /*function=*/nullptr);

  auto value_node = std::make_unique<TextNode>("3");
  auto extra_value_node = std::make_unique<TextNode>("25");
  std::vector<std::unique_ptr<Node>> args;
  args.push_back(std::move(value_node));
  args.push_back(std::move(member_node));
  args.push_back(std::move(extra_value_node));
  auto root_node = std::make_unique<NaryOperatorNode>("<", std::move(args));

  // Retrieve the root_iterator from the visitor.
  QueryVisitor query_visitor(&numeric_index);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).root(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(QueryVisitorTest, LessThanTooFewOperandsInvalid) {
  DummyNumericIndex<int64_t> numeric_index;

  // Create an invalid AST for the query 'subscription.price <' where '<'
  // has a single operand
  auto property_node = std::make_unique<TextNode>("subscription");
  auto subproperty_node = std::make_unique<TextNode>("price");
  std::vector<std::unique_ptr<TextNode>> member_args;
  member_args.push_back(std::move(property_node));
  member_args.push_back(std::move(subproperty_node));
  auto member_node = std::make_unique<MemberNode>(std::move(member_args),
                                                  /*function=*/nullptr);

  std::vector<std::unique_ptr<Node>> args;
  args.push_back(std::move(member_node));
  auto root_node = std::make_unique<NaryOperatorNode>("<", std::move(args));

  // Retrieve the root_iterator from the visitor.
  QueryVisitor query_visitor(&numeric_index);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).root(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(QueryVisitorTest, LessThanNonExistentPropertyNotFound) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values 0, 1 and 2
  // respectively.
  DummyNumericIndex<int64_t> numeric_index;
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index.Edit("subscription.price", kDocumentId0, kSectionId0);
  editor->BufferKey(0);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("subscription.price", kDocumentId1, kSectionId1);
  editor->BufferKey(1);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("subscription.price", kDocumentId2, kSectionId2);
  editor->BufferKey(2);
  editor->IndexAllBufferedKeys();

  // Create an invalid AST for the query 'time < 25' where '<'
  // has three operands
  std::string query = "time < 25";
  Lexer lexer(query, Lexer::Language::QUERY);
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Lexer::LexerToken> lexer_tokens,
                             lexer.ExtractTokens());
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             parser.ConsumeQuery());

  // Retrieve the root_iterator from the visitor.
  QueryVisitor query_visitor(&numeric_index);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).root(),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST(QueryVisitorTest, NeverVisitedReturnsInvalid) {
  DummyNumericIndex<int64_t> numeric_index;
  QueryVisitor query_visitor(&numeric_index);
  EXPECT_THAT(std::move(query_visitor).root(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

// TODO(b/208654892) Properly handle negative numbers in query expressions.
TEST(QueryVisitorTest, DISABLED_IntMinLessThanInvalid) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values INT_MIN,
  // INT_MAX and INT_MIN + 1 respectively.
  int64_t int_min = std::numeric_limits<int64_t>::min();
  DummyNumericIndex<int64_t> numeric_index;
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index.Edit("price", kDocumentId0, kSectionId0);
  editor->BufferKey(int_min);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("price", kDocumentId1, kSectionId1);
  editor->BufferKey(std::numeric_limits<int64_t>::max());
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("price", kDocumentId2, kSectionId2);
  editor->BufferKey(int_min + 1);
  editor->IndexAllBufferedKeys();

  std::string query = "price <" + std::to_string(int_min);
  Lexer lexer(query, Lexer::Language::QUERY);
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Lexer::LexerToken> lexer_tokens,
                             lexer.ExtractTokens());
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             parser.ConsumeQuery());

  // Retrieve the root_iterator from the visitor.
  QueryVisitor query_visitor(&numeric_index);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).root(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(QueryVisitorTest, IntMaxGreaterThanInvalid) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values INT_MIN,
  // INT_MAX and INT_MAX - 1 respectively.
  int64_t int_max = std::numeric_limits<int64_t>::max();
  DummyNumericIndex<int64_t> numeric_index;
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index.Edit("price", kDocumentId0, kSectionId0);
  editor->BufferKey(std::numeric_limits<int64_t>::min());
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("price", kDocumentId1, kSectionId1);
  editor->BufferKey(int_max);
  editor->IndexAllBufferedKeys();

  editor = numeric_index.Edit("price", kDocumentId2, kSectionId2);
  editor->BufferKey(int_max - 1);
  editor->IndexAllBufferedKeys();

  std::string query = "price >" + std::to_string(int_max);
  Lexer lexer(query, Lexer::Language::QUERY);
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Lexer::LexerToken> lexer_tokens,
                             lexer.ExtractTokens());
  Parser parser = Parser::Create(std::move(lexer_tokens));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             parser.ConsumeQuery());

  // Retrieve the root_iterator from the visitor.
  QueryVisitor query_visitor(&numeric_index);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).root(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

}  // namespace

}  // namespace lib
}  // namespace icing
