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
#include "icing/document-builder.h"
#include "icing/index/index.h"
#include "icing/index/iterator/doc-hit-info-iterator-test-util.h"
#include "icing/index/numeric/dummy-numeric-index.h"
#include "icing/index/numeric/numeric-index.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/portable/platform.h"
#include "icing/query/advanced_query_parser/abstract-syntax-tree.h"
#include "icing/query/advanced_query_parser/lexer.h"
#include "icing/query/advanced_query_parser/parser.h"
#include "icing/schema-builder.h"
#include "icing/query/query-features.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/icu-data-file-helper.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"
#include "icing/transform/normalizer-factory.h"
#include "icing/transform/normalizer.h"

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

class QueryVisitorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = GetTestTempDir() + "/icing";
    index_dir_ = test_dir_ + "/index";
    store_dir_ = test_dir_ + "/store";
    schema_store_dir_ = test_dir_ + "/schema_store";
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(index_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(store_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());

    if (!IsCfStringTokenization() && !IsReverseJniTokenization()) {
      // If we've specified using the reverse-JNI method for segmentation (i.e.
      // not ICU), then we won't have the ICU data file included to set up.
      // Technically, we could choose to use reverse-JNI for segmentation AND
      // include an ICU data file, but that seems unlikely and our current BUILD
      // setup doesn't do this.
      ICING_ASSERT_OK(
          // File generated via icu_data_file rule in //icing/BUILD.
          icu_data_file_helper::SetUpICUDataFile(
              GetTestFilePath("icing/icu.dat")));
    }

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &clock_));

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(&filesystem_, store_dir_, &clock_,
                              schema_store_.get()));
    document_store_ = std::move(create_result.document_store);

    Index::Options options(index_dir_.c_str(),
                           /*index_merge_size=*/1024 * 1024);
    ICING_ASSERT_OK_AND_ASSIGN(
        index_, Index::Create(options, &filesystem_, &icing_filesystem_));

    numeric_index_ = std::make_unique<DummyNumericIndex<int64_t>>();

    ICING_ASSERT_OK_AND_ASSIGN(normalizer_, normalizer_factory::Create(
                                                /*max_term_byte_size=*/1000));
  }

  libtextclassifier3::StatusOr<std::unique_ptr<Node>> ParseQueryHelper(
      std::string_view query) {
    Lexer lexer(query, Lexer::Language::QUERY);
    ICING_ASSIGN_OR_RETURN(std::vector<Lexer::LexerToken> lexer_tokens,
                           lexer.ExtractTokens());
    Parser parser = Parser::Create(std::move(lexer_tokens));
    return parser.ConsumeQuery();
  }

  Filesystem filesystem_;
  IcingFilesystem icing_filesystem_;
  std::string test_dir_;
  std::string index_dir_;
  std::string schema_store_dir_;
  std::string store_dir_;
  Clock clock_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
  std::unique_ptr<Index> index_;
  std::unique_ptr<DummyNumericIndex<int64_t>> numeric_index_;
  std::unique_ptr<Normalizer> normalizer_;
};

TEST_F(QueryVisitorTest, SimpleLessThan) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values 0, 1 and 2
  // respectively.
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index_->Edit("price", kDocumentId0, kSectionId0);
  editor->BufferKey(0);
  editor->IndexAllBufferedKeys();

  editor = numeric_index_->Edit("price", kDocumentId1, kSectionId1);
  editor->BufferKey(1);
  editor->IndexAllBufferedKeys();

  editor = numeric_index_->Edit("price", kDocumentId2, kSectionId2);
  editor->BufferKey(2);
  editor->IndexAllBufferedKeys();

  std::string query = "price < 2";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);

  EXPECT_THAT(query_visitor.features(), ElementsAre(kNumericSearchFeature));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()),
              ElementsAre(kDocumentId1, kDocumentId0));
}

TEST_F(QueryVisitorTest, SimpleLessThanEq) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values 0, 1 and 2
  // respectively.
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index_->Edit("price", kDocumentId0, kSectionId0);
  editor->BufferKey(0);
  editor->IndexAllBufferedKeys();

  editor = numeric_index_->Edit("price", kDocumentId1, kSectionId1);
  editor->BufferKey(1);
  editor->IndexAllBufferedKeys();

  editor = numeric_index_->Edit("price", kDocumentId2, kSectionId2);
  editor->BufferKey(2);
  editor->IndexAllBufferedKeys();

  std::string query = "price <= 1";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);

  EXPECT_THAT(query_visitor.features(), ElementsAre(kNumericSearchFeature));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()),
              ElementsAre(kDocumentId1, kDocumentId0));
}

TEST_F(QueryVisitorTest, SimpleEqual) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values 0, 1 and 2
  // respectively.
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index_->Edit("price", kDocumentId0, kSectionId0);
  editor->BufferKey(0);
  editor->IndexAllBufferedKeys();

  editor = numeric_index_->Edit("price", kDocumentId1, kSectionId1);
  editor->BufferKey(1);
  editor->IndexAllBufferedKeys();

  editor = numeric_index_->Edit("price", kDocumentId2, kSectionId2);
  editor->BufferKey(2);
  editor->IndexAllBufferedKeys();

  std::string query = "price == 2";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);

  EXPECT_THAT(query_visitor.features(), ElementsAre(kNumericSearchFeature));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId2));
}

TEST_F(QueryVisitorTest, SimpleGreaterThanEq) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values 0, 1 and 2
  // respectively.
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index_->Edit("price", kDocumentId0, kSectionId0);
  editor->BufferKey(0);
  editor->IndexAllBufferedKeys();

  editor = numeric_index_->Edit("price", kDocumentId1, kSectionId1);
  editor->BufferKey(1);
  editor->IndexAllBufferedKeys();

  editor = numeric_index_->Edit("price", kDocumentId2, kSectionId2);
  editor->BufferKey(2);
  editor->IndexAllBufferedKeys();

  std::string query = "price >= 1";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);

  EXPECT_THAT(query_visitor.features(), ElementsAre(kNumericSearchFeature));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()),
              ElementsAre(kDocumentId2, kDocumentId1));
}

TEST_F(QueryVisitorTest, SimpleGreaterThan) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values 0, 1 and 2
  // respectively.
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index_->Edit("price", kDocumentId0, kSectionId0);
  editor->BufferKey(0);
  editor->IndexAllBufferedKeys();

  editor = numeric_index_->Edit("price", kDocumentId1, kSectionId1);
  editor->BufferKey(1);
  editor->IndexAllBufferedKeys();

  editor = numeric_index_->Edit("price", kDocumentId2, kSectionId2);
  editor->BufferKey(2);
  editor->IndexAllBufferedKeys();

  std::string query = "price > 1";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);

  EXPECT_THAT(query_visitor.features(), ElementsAre(kNumericSearchFeature));
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId2));
}

// TODO(b/208654892) Properly handle negative numbers in query expressions.
TEST_F(QueryVisitorTest, DISABLED_IntMinLessThanEqual) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values INT_MIN,
  // INT_MAX and INT_MIN + 1 respectively.
  int64_t int_min = std::numeric_limits<int64_t>::min();
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index_->Edit("price", kDocumentId0, kSectionId0);
  editor->BufferKey(int_min);
  editor->IndexAllBufferedKeys();

  editor = numeric_index_->Edit("price", kDocumentId1, kSectionId1);
  editor->BufferKey(std::numeric_limits<int64_t>::max());
  editor->IndexAllBufferedKeys();

  editor = numeric_index_->Edit("price", kDocumentId2, kSectionId2);
  editor->BufferKey(int_min + 1);
  editor->IndexAllBufferedKeys();

  std::string query = "price <= " + std::to_string(int_min);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId0));
}

TEST_F(QueryVisitorTest, IntMaxGreaterThanEqual) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values INT_MIN,
  // INT_MAX and INT_MAX - 1 respectively.
  int64_t int_max = std::numeric_limits<int64_t>::max();
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index_->Edit("price", kDocumentId0, kSectionId0);
  editor->BufferKey(std::numeric_limits<int64_t>::min());
  editor->IndexAllBufferedKeys();

  editor = numeric_index_->Edit("price", kDocumentId1, kSectionId1);
  editor->BufferKey(int_max);
  editor->IndexAllBufferedKeys();

  editor = numeric_index_->Edit("price", kDocumentId2, kSectionId2);
  editor->BufferKey(int_max - 1);
  editor->IndexAllBufferedKeys();

  std::string query = "price >= " + std::to_string(int_max);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId1));
}

TEST_F(QueryVisitorTest, NestedPropertyLessThan) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values 0, 1 and 2
  // respectively.
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index_->Edit("subscription.price", kDocumentId0, kSectionId0);
  editor->BufferKey(0);
  editor->IndexAllBufferedKeys();

  editor =
      numeric_index_->Edit("subscription.price", kDocumentId1, kSectionId1);
  editor->BufferKey(1);
  editor->IndexAllBufferedKeys();

  editor =
      numeric_index_->Edit("subscription.price", kDocumentId2, kSectionId2);
  editor->BufferKey(2);
  editor->IndexAllBufferedKeys();

  std::string query = "subscription.price < 2";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()),
              ElementsAre(kDocumentId1, kDocumentId0));
}

TEST_F(QueryVisitorTest, IntParsingError) {
  std::string query = "subscription.price < fruit";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).root(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(QueryVisitorTest, NotEqualsUnsupported) {
  std::string query = "subscription.price != 3";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).root(),
              StatusIs(libtextclassifier3::StatusCode::UNIMPLEMENTED));
}

TEST_F(QueryVisitorTest, LessThanTooManyOperandsInvalid) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values 0, 1 and 2
  // respectively.
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index_->Edit("subscription.price", kDocumentId0, kSectionId0);
  editor->BufferKey(0);
  editor->IndexAllBufferedKeys();

  editor =
      numeric_index_->Edit("subscription.price", kDocumentId1, kSectionId1);
  editor->BufferKey(1);
  editor->IndexAllBufferedKeys();

  editor =
      numeric_index_->Edit("subscription.price", kDocumentId2, kSectionId2);
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
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).root(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(QueryVisitorTest, LessThanTooFewOperandsInvalid) {
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
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).root(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(QueryVisitorTest, LessThanNonExistentPropertyNotFound) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values 0, 1 and 2
  // respectively.
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index_->Edit("subscription.price", kDocumentId0, kSectionId0);
  editor->BufferKey(0);
  editor->IndexAllBufferedKeys();

  editor =
      numeric_index_->Edit("subscription.price", kDocumentId1, kSectionId1);
  editor->BufferKey(1);
  editor->IndexAllBufferedKeys();

  editor =
      numeric_index_->Edit("subscription.price", kDocumentId2, kSectionId2);
  editor->BufferKey(2);
  editor->IndexAllBufferedKeys();

  // Create an invalid AST for the query 'time < 25' where '<'
  // has three operands
  std::string query = "time < 25";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).root(),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(QueryVisitorTest, NeverVisitedReturnsInvalid) {
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  EXPECT_THAT(std::move(query_visitor).root(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

// TODO(b/208654892) Properly handle negative numbers in query expressions.
TEST_F(QueryVisitorTest, DISABLED_IntMinLessThanInvalid) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values INT_MIN,
  // INT_MAX and INT_MIN + 1 respectively.
  int64_t int_min = std::numeric_limits<int64_t>::min();
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index_->Edit("price", kDocumentId0, kSectionId0);
  editor->BufferKey(int_min);
  editor->IndexAllBufferedKeys();

  editor = numeric_index_->Edit("price", kDocumentId1, kSectionId1);
  editor->BufferKey(std::numeric_limits<int64_t>::max());
  editor->IndexAllBufferedKeys();

  editor = numeric_index_->Edit("price", kDocumentId2, kSectionId2);
  editor->BufferKey(int_min + 1);
  editor->IndexAllBufferedKeys();

  std::string query = "price <" + std::to_string(int_min);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).root(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(QueryVisitorTest, IntMaxGreaterThanInvalid) {
  // Setup the numeric index with docs 0, 1 and 2 holding the values INT_MIN,
  // INT_MAX and INT_MAX - 1 respectively.
  int64_t int_max = std::numeric_limits<int64_t>::max();
  std::unique_ptr<NumericIndex<int64_t>::Editor> editor =
      numeric_index_->Edit("price", kDocumentId0, kSectionId0);
  editor->BufferKey(std::numeric_limits<int64_t>::min());
  editor->IndexAllBufferedKeys();

  editor = numeric_index_->Edit("price", kDocumentId1, kSectionId1);
  editor->BufferKey(int_max);
  editor->IndexAllBufferedKeys();

  editor = numeric_index_->Edit("price", kDocumentId2, kSectionId2);
  editor->BufferKey(int_max - 1);
  editor->IndexAllBufferedKeys();

  std::string query = "price >" + std::to_string(int_max);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).root(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(QueryVisitorTest, SingleTerm) {
  // Setup the index with docs 0, 1 and 2 holding the values "foo", "foo" and
  // "bar" respectively.
  Index::Editor editor = index_->Edit(kDocumentId0, kSectionId1,
                                      TERM_MATCH_PREFIX, /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId1, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId2, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("bar");
  editor.IndexAllBufferedTerms();

  std::string query = "foo";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()),
              ElementsAre(kDocumentId1, kDocumentId0));
}

TEST_F(QueryVisitorTest, SingleVerbatimTerm) {
  // Setup the index with docs 0, 1 and 2 holding the values "foo:bar(baz)",
  // "foo:bar(baz)" and "bar:baz(foo)" respectively.
  Index::Editor editor = index_->Edit(kDocumentId0, kSectionId1,
                                      TERM_MATCH_PREFIX, /*namespace_id=*/0);
  editor.BufferTerm("foo:bar(baz)");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId1, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("foo:bar(baz)");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId2, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("bar:baz(foo)");
  editor.IndexAllBufferedTerms();

  std::string query = "\"foo:bar(baz)\"";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()),
              ElementsAre(kDocumentId1, kDocumentId0));
}

// There are three primary cases to worry about for escaping:
//
// NOTE: The following comments use ` chars to denote the beginning and end of
// the verbatim term rather than " chars to avoid confusion. Additionally, the
// raw chars themselves are shown. So `foobar\\` in actual c++ would be written
// as std::string verbatim_term = "foobar\\\\";
//
// 1. How does a user represent a quote char (") without terminating the
//    verbatim term?
//    Example: verbatim_term = `foobar"`
//    Answer: quote char must be escaped. verbatim_query = `foobar\"`
TEST_F(QueryVisitorTest, VerbatimTermEscapingQuote) {
  // Setup the index with docs 0, 1 and 2 holding the values "foobary",
  // "foobar\" and "foobar"" respectively.
  Index::Editor editor = index_->Edit(kDocumentId0, kSectionId1,
                                      TERM_MATCH_EXACT, /*namespace_id=*/0);
  editor.BufferTerm(R"(foobary)");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId1, kSectionId1, TERM_MATCH_EXACT,
                        /*namespace_id=*/0);
  editor.BufferTerm(R"(foobar\)");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId2, kSectionId1, TERM_MATCH_EXACT,
                        /*namespace_id=*/0);
  editor.BufferTerm(R"(foobar")");
  editor.IndexAllBufferedTerms();

  // From the comment above, verbatim_term = `foobar"` and verbatim_query =
  // `foobar\"`
  std::string query = R"("foobar\"")";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId2));
}

// 2. How does a user represent a escape char (\) that immediately precedes the
//    end of the verbatim term
//    Example: verbatim_term = `foobar\`
//    Answer: escape chars can be escaped. verbatim_query = `foobar\\`
TEST_F(QueryVisitorTest, VerbatimTermEscapingEscape) {
  // Setup the index with docs 0, 1 and 2 holding the values "foobary",
  // "foobar\" and "foobar"" respectively.
  Index::Editor editor = index_->Edit(kDocumentId0, kSectionId1,
                                      TERM_MATCH_EXACT, /*namespace_id=*/0);
  editor.BufferTerm(R"(foobary)");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId1, kSectionId1, TERM_MATCH_EXACT,
                        /*namespace_id=*/0);
  // From the comment above, verbatim_term = `foobar\`.
  editor.BufferTerm(R"(foobar\)");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId2, kSectionId1, TERM_MATCH_EXACT,
                        /*namespace_id=*/0);
  editor.BufferTerm(R"(foobar")");
  editor.IndexAllBufferedTerms();

  // Issue a query for the verbatim token `foobar\`.
  std::string query = R"("foobar\\")";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId1));
}

// 3. How do we handle other escaped chars?
//    Example: verbatim_query = `foobar\y`.
//    Answer: all chars preceded by an escape character are blindly escaped (as
//            in, consume the escape char and add the char like we do for the
//            quote char). So the above query would match the verbatim_term
//            `foobary`.
TEST_F(QueryVisitorTest, VerbatimTermEscapingNonSpecialChar) {
  // Setup the index with docs 0, 1 and 2 holding the values "foobary",
  // "foobar\" and "foobar"" respectively.
  Index::Editor editor = index_->Edit(kDocumentId0, kSectionId1,
                                      TERM_MATCH_EXACT, /*namespace_id=*/0);
  // From the comment above, verbatim_term = `foobary`.
  editor.BufferTerm(R"(foobary)");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId1, kSectionId1, TERM_MATCH_EXACT,
                        /*namespace_id=*/0);
  editor.BufferTerm(R"(foobar\)");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId2, kSectionId1, TERM_MATCH_EXACT,
                        /*namespace_id=*/0);
  editor.BufferTerm(R"(foobar\y)");
  editor.IndexAllBufferedTerms();

  // Issue a query for the verbatim token `foobary`.
  std::string query = R"("foobar\y")";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId0));

  // Issue a query for the verbatim token `foobar\y`.
  query = R"("foobar\\y")";
  ICING_ASSERT_OK_AND_ASSIGN(root_node, ParseQueryHelper(query));
  QueryVisitor query_visitor_two(index_.get(), numeric_index_.get(),
                                 document_store_.get(), schema_store_.get(),
                                 normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor_two);
  ICING_ASSERT_OK_AND_ASSIGN(root_iterator,
                             std::move(query_visitor_two).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId2));
}

// This isn't a special case, but is fairly useful for demonstrating. There are
// a number of escaped sequences in c++, including the new line character '\n'.
// It is worth emphasizing that the new line character, like the others in c++,
// is its own separate ascii value. For a query `foobar\n`, the parser will see
// the character sequence [`f`, `o`, `o`, `b`, `a`, `r`, `\n`] - it *won't* ever
// see `\` and `n`.
TEST_F(QueryVisitorTest, VerbatimTermNewLine) {
  // Setup the index with docs 0, 1 and 2 holding the values "foobar\n",
  // `foobar\` and `foobar\n` respectively.
  Index::Editor editor = index_->Edit(kDocumentId0, kSectionId1,
                                      TERM_MATCH_EXACT, /*namespace_id=*/0);
  // From the comment above, verbatim_term = `foobar` + '\n'.
  editor.BufferTerm("foobar\n");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId1, kSectionId1, TERM_MATCH_EXACT,
                        /*namespace_id=*/0);
  editor.BufferTerm(R"(foobar\)");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId2, kSectionId1, TERM_MATCH_EXACT,
                        /*namespace_id=*/0);
  // verbatim_term = `foobar\n`. This is distinct from the term added above.
  editor.BufferTerm(R"(foobar\n)");
  editor.IndexAllBufferedTerms();

  // Issue a query for the verbatim token `foobar` + '\n'.
  std::string query = "\"foobar\n\"";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId0));

  // Now, issue a query for the verbatim token `foobar\n`.
  query = R"("foobar\\n")";
  ICING_ASSERT_OK_AND_ASSIGN(root_node, ParseQueryHelper(query));
  QueryVisitor query_visitor_two(index_.get(), numeric_index_.get(),
                                 document_store_.get(), schema_store_.get(),
                                 normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor_two);
  ICING_ASSERT_OK_AND_ASSIGN(root_iterator,
                             std::move(query_visitor_two).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId2));
}

TEST_F(QueryVisitorTest, VerbatimTermEscapingComplex) {
  // Setup the index with docs 0, 1 and 2 holding the values `foo\"bar\nbaz"`,
  // `foo\\\"bar\\nbaz\"` and `foo\\"bar\\nbaz"` respectively.
  Index::Editor editor = index_->Edit(kDocumentId0, kSectionId1,
                                      TERM_MATCH_EXACT, /*namespace_id=*/0);
  editor.BufferTerm(R"(foo\"bar\nbaz")");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId1, kSectionId1, TERM_MATCH_EXACT,
                        /*namespace_id=*/0);
  // Add the verbatim_term from doc 0 but with all of the escapes left in
  editor.BufferTerm(R"(foo\\\"bar\\nbaz\")");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId2, kSectionId1, TERM_MATCH_EXACT,
                        /*namespace_id=*/0);
  // Add the verbatim_term from doc 0 but with the escapes for '\' chars left in
  editor.BufferTerm(R"(foo\\"bar\\nbaz")");
  editor.IndexAllBufferedTerms();

  // Issue a query for the verbatim token `foo\"bar\nbaz"`.
  std::string query = R"("foo\\\"bar\\nbaz\"")";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId0));
}

TEST_F(QueryVisitorTest, SingleMinusTerm) {
  // Setup the index with docs 0, 1 and 2 holding the values "foo", "foo" and
  // "bar" respectively.
  ICING_ASSERT_OK(schema_store_->SetSchema(
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("type"))
          .Build()));

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri0").SetSchema("type").Build()));
  Index::Editor editor = index_->Edit(kDocumentId0, kSectionId1,
                                      TERM_MATCH_PREFIX, /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri1").SetSchema("type").Build()));
  editor = index_->Edit(kDocumentId1, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri2").SetSchema("type").Build()));
  editor = index_->Edit(kDocumentId2, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("bar");
  editor.IndexAllBufferedTerms();

  std::string query = "-foo";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId2));
}

TEST_F(QueryVisitorTest, SingleNotTerm) {
  // Setup the index with docs 0, 1 and 2 holding the values "foo", "foo" and
  // "bar" respectively.
  ICING_ASSERT_OK(schema_store_->SetSchema(
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("type"))
          .Build()));

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri0").SetSchema("type").Build()));
  Index::Editor editor = index_->Edit(kDocumentId0, kSectionId1,
                                      TERM_MATCH_PREFIX, /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri1").SetSchema("type").Build()));
  editor = index_->Edit(kDocumentId1, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri2").SetSchema("type").Build()));
  editor = index_->Edit(kDocumentId2, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("bar");
  editor.IndexAllBufferedTerms();

  std::string query = "NOT foo";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId2));
}
TEST_F(QueryVisitorTest, ImplicitAndTerms) {
  Index::Editor editor = index_->Edit(kDocumentId0, kSectionId1,
                                      TERM_MATCH_PREFIX, /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId1, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.BufferTerm("bar");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId2, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("bar");
  editor.IndexAllBufferedTerms();

  std::string query = "foo bar";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId1));
}

TEST_F(QueryVisitorTest, ExplicitAndTerms) {
  Index::Editor editor = index_->Edit(kDocumentId0, kSectionId1,
                                      TERM_MATCH_PREFIX, /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId1, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.BufferTerm("bar");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId2, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("bar");
  editor.IndexAllBufferedTerms();

  std::string query = "foo AND bar";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId1));
}

TEST_F(QueryVisitorTest, OrTerms) {
  Index::Editor editor = index_->Edit(kDocumentId0, kSectionId1,
                                      TERM_MATCH_PREFIX, /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId1, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("fo");
  editor.BufferTerm("ba");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId2, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("bar");
  editor.IndexAllBufferedTerms();

  std::string query = "foo OR bar";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()),
              ElementsAre(kDocumentId2, kDocumentId0));
}

TEST_F(QueryVisitorTest, AndOrTermPrecedence) {
  Index::Editor editor = index_->Edit(kDocumentId0, kSectionId1,
                                      TERM_MATCH_PREFIX, /*namespace_id=*/0);
  editor.BufferTerm("bar");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId1, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.BufferTerm("bar");
  editor.IndexAllBufferedTerms();

  editor = index_->Edit(kDocumentId2, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.BufferTerm("baz");
  editor.IndexAllBufferedTerms();

  // Should be interpreted like `foo (bar OR baz)`
  std::string query = "foo bar OR baz";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()),
              ElementsAre(kDocumentId2, kDocumentId1));

  // Should be interpreted like `(bar OR baz) foo`
  query = "bar OR baz foo";
  ICING_ASSERT_OK_AND_ASSIGN(root_node, ParseQueryHelper(query));
  QueryVisitor query_visitor_two(index_.get(), numeric_index_.get(),
                                 document_store_.get(), schema_store_.get(),
                                 normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor_two);
  ICING_ASSERT_OK_AND_ASSIGN(root_iterator,
                             std::move(query_visitor_two).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()),
              ElementsAre(kDocumentId2, kDocumentId1));

  query = "(bar OR baz) foo";
  ICING_ASSERT_OK_AND_ASSIGN(root_node, ParseQueryHelper(query));
  QueryVisitor query_visitor_three(index_.get(), numeric_index_.get(),
                                   document_store_.get(), schema_store_.get(),
                                   normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor_three);
  ICING_ASSERT_OK_AND_ASSIGN(root_iterator,
                             std::move(query_visitor_three).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()),
              ElementsAre(kDocumentId2, kDocumentId1));
}

TEST_F(QueryVisitorTest, AndOrNotPrecedence) {
  ICING_ASSERT_OK(schema_store_->SetSchema(
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("type").AddProperty(
              PropertyConfigBuilder()
                  .SetName("prop1")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build()));

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri0").SetSchema("type").Build()));
  Index::Editor editor = index_->Edit(kDocumentId0, kSectionId1,
                                      TERM_MATCH_PREFIX, /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri1").SetSchema("type").Build()));
  editor = index_->Edit(kDocumentId1, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.BufferTerm("bar");
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri2").SetSchema("type").Build()));
  editor = index_->Edit(kDocumentId2, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.BufferTerm("baz");
  editor.IndexAllBufferedTerms();

  // Should be interpreted like `foo ((NOT bar) OR baz)`
  std::string query = "foo NOT bar OR baz";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()),
              ElementsAre(kDocumentId2, kDocumentId0));

  query = "foo NOT (bar OR baz)";
  ICING_ASSERT_OK_AND_ASSIGN(root_node, ParseQueryHelper(query));
  QueryVisitor query_visitor_two(index_.get(), numeric_index_.get(),
                                 document_store_.get(), schema_store_.get(),
                                 normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor_two);
  ICING_ASSERT_OK_AND_ASSIGN(root_iterator,
                             std::move(query_visitor_two).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId0));
}

TEST_F(QueryVisitorTest, PropertyFilter) {
  ICING_ASSERT_OK(schema_store_->SetSchema(
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("type")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop1")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop2")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build()));
  // Section ids are assigned alphabetically.
  SectionId prop1_section_id = 0;
  SectionId prop2_section_id = 1;

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri0").SetSchema("type").Build()));
  Index::Editor editor = index_->Edit(kDocumentId0, prop1_section_id,
                                      TERM_MATCH_PREFIX, /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri1").SetSchema("type").Build()));
  editor = index_->Edit(kDocumentId1, prop1_section_id, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri2").SetSchema("type").Build()));
  editor = index_->Edit(kDocumentId2, prop2_section_id, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.IndexAllBufferedTerms();

  std::string query = "prop1:foo";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()),
              ElementsAre(kDocumentId1, kDocumentId0));
}

TEST_F(QueryVisitorTest, PropertyFilterWithGrouping) {
  ICING_ASSERT_OK(schema_store_->SetSchema(
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("type")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop1")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop2")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build()));
  // Section ids are assigned alphabetically.
  SectionId prop1_section_id = 0;
  SectionId prop2_section_id = 1;

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri0").SetSchema("type").Build()));
  Index::Editor editor = index_->Edit(kDocumentId0, prop1_section_id,
                                      TERM_MATCH_PREFIX, /*namespace_id=*/0);
  editor.BufferTerm("bar");
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri1").SetSchema("type").Build()));
  editor = index_->Edit(kDocumentId1, prop1_section_id, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri2").SetSchema("type").Build()));
  editor = index_->Edit(kDocumentId2, prop2_section_id, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.IndexAllBufferedTerms();

  std::string query = "prop1:(foo OR bar)";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()),
              ElementsAre(kDocumentId1, kDocumentId0));
}

TEST_F(QueryVisitorTest, PropertyFilterWithNot) {
  ICING_ASSERT_OK(schema_store_->SetSchema(
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("type")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop1")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop2")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build()));
  // Section ids are assigned alphabetically.
  SectionId prop1_section_id = 0;
  SectionId prop2_section_id = 1;

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri0").SetSchema("type").Build()));
  Index::Editor editor = index_->Edit(kDocumentId0, prop1_section_id,
                                      TERM_MATCH_PREFIX, /*namespace_id=*/0);
  editor.BufferTerm("bar");
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri1").SetSchema("type").Build()));
  editor = index_->Edit(kDocumentId1, prop1_section_id, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri2").SetSchema("type").Build()));
  editor = index_->Edit(kDocumentId2, prop2_section_id, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.IndexAllBufferedTerms();

  std::string query = "-prop1:(foo OR bar)";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(index_.get(), numeric_index_.get(),
                             document_store_.get(), schema_store_.get(),
                             normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<DocHitInfoIterator> root_iterator,
                             std::move(query_visitor).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId2));

  query = "NOT prop1:(foo OR bar)";
  ICING_ASSERT_OK_AND_ASSIGN(root_node, ParseQueryHelper(query));
  QueryVisitor query_visitor_two(index_.get(), numeric_index_.get(),
                                 document_store_.get(), schema_store_.get(),
                                 normalizer_.get(), TERM_MATCH_PREFIX);
  root_node->Accept(&query_visitor_two);
  ICING_ASSERT_OK_AND_ASSIGN(root_iterator,
                             std::move(query_visitor_two).root());
  EXPECT_THAT(GetDocumentIds(root_iterator.get()), ElementsAre(kDocumentId2));
}

}  // namespace

}  // namespace lib
}  // namespace icing
