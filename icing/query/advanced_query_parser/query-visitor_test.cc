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

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/index/index.h"
#include "icing/index/iterator/doc-hit-info-iterator-test-util.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/index/numeric/dummy-numeric-index.h"
#include "icing/index/numeric/numeric-index.h"
#include "icing/jni/jni-cache.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/portable/platform.h"
#include "icing/query/advanced_query_parser/abstract-syntax-tree.h"
#include "icing/query/advanced_query_parser/lexer.h"
#include "icing/query/advanced_query_parser/parser.h"
#include "icing/query/query-features.h"
#include "icing/schema-builder.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/icu-data-file-helper.h"
#include "icing/testing/jni-test-helpers.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/tokenization/tokenizer-factory.h"
#include "icing/tokenization/tokenizer.h"
#include "icing/transform/normalizer-factory.h"
#include "icing/transform/normalizer.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::UnorderedElementsAre;

constexpr DocumentId kDocumentId0 = 0;
constexpr DocumentId kDocumentId1 = 1;
constexpr DocumentId kDocumentId2 = 2;

constexpr SectionId kSectionId0 = 0;
constexpr SectionId kSectionId1 = 1;
constexpr SectionId kSectionId2 = 2;

template <typename T, typename U>
std::vector<T> ExtractKeys(const std::unordered_map<T, U>& map) {
  std::vector<T> keys;
  keys.reserve(map.size());
  for (const auto& [key, value] : map) {
    keys.push_back(key);
  }
  return keys;
}

class QueryVisitorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = GetTestTempDir() + "/icing";
    index_dir_ = test_dir_ + "/index";
    numeric_index_dir_ = test_dir_ + "/numeric_index";
    store_dir_ = test_dir_ + "/store";
    schema_store_dir_ = test_dir_ + "/schema_store";
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(index_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(store_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());

    jni_cache_ = GetTestJniCache();

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

    ICING_ASSERT_OK_AND_ASSIGN(
        numeric_index_,
        DummyNumericIndex<int64_t>::Create(filesystem_, numeric_index_dir_));

    ICING_ASSERT_OK_AND_ASSIGN(normalizer_, normalizer_factory::Create(
                                                /*max_term_byte_size=*/1000));

    language_segmenter_factory::SegmenterOptions segmenter_options(
        ULOC_US, jni_cache_.get());
    ICING_ASSERT_OK_AND_ASSIGN(
        language_segmenter_,
        language_segmenter_factory::Create(segmenter_options));

    ICING_ASSERT_OK_AND_ASSIGN(tokenizer_,
                               tokenizer_factory::CreateIndexingTokenizer(
                                   StringIndexingConfig::TokenizerType::PLAIN,
                                   language_segmenter_.get()));
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
  std::string numeric_index_dir_;
  std::string schema_store_dir_;
  std::string store_dir_;
  Clock clock_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
  std::unique_ptr<Index> index_;
  std::unique_ptr<DummyNumericIndex<int64_t>> numeric_index_;
  std::unique_ptr<Normalizer> normalizer_;
  std::unique_ptr<LanguageSegmenter> language_segmenter_;
  std::unique_ptr<Tokenizer> tokenizer_;
  std::unique_ptr<const JniCache> jni_cache_;
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use,
              ElementsAre(kNumericSearchFeature));
  // "price" is a property restrict here and "2" isn't a "term" - its a numeric
  // value. So QueryTermIterators should be empty.
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators), IsEmpty());
  EXPECT_THAT(query_results.query_terms, IsEmpty());
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use,
              ElementsAre(kNumericSearchFeature));
  // "price" is a property restrict here and "1" isn't a "term" - its a numeric
  // value. So QueryTermIterators should be empty.
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators), IsEmpty());
  EXPECT_THAT(query_results.query_terms, IsEmpty());
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use,
              ElementsAre(kNumericSearchFeature));
  // "price" is a property restrict here and "2" isn't a "term" - its a numeric
  // value. So QueryTermIterators should be empty.
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators), IsEmpty());
  EXPECT_THAT(query_results.query_terms, IsEmpty());
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId2));
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use,
              ElementsAre(kNumericSearchFeature));
  // "price" is a property restrict here and "1" isn't a "term" - its a numeric
  // value. So QueryTermIterators should be empty.
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators), IsEmpty());
  EXPECT_THAT(query_results.query_terms, IsEmpty());
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use,
              ElementsAre(kNumericSearchFeature));
  // "price" is a property restrict here and "1" isn't a "term" - its a numeric
  // value. So QueryTermIterators should be empty.
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators), IsEmpty());
  EXPECT_THAT(query_results.query_terms, IsEmpty());
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId2));
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use,
              ElementsAre(kNumericSearchFeature));
  // "price" is a property restrict here and int_min isn't a "term" - its a
  // numeric value. So QueryTermIterators should be empty.
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators), IsEmpty());
  EXPECT_THAT(query_results.query_terms, IsEmpty());
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId0));
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use,
              ElementsAre(kNumericSearchFeature));
  // "price" is a property restrict here and int_max isn't a "term" - its a
  // numeric value. So QueryTermIterators should be empty.
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators), IsEmpty());
  EXPECT_THAT(query_results.query_terms, IsEmpty());
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId1));
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use,
              ElementsAre(kNumericSearchFeature));
  // "subscription.price" is a property restrict here and int_max isn't a "term"
  // - its a numeric value. So QueryTermIterators should be empty.
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators), IsEmpty());
  EXPECT_THAT(query_results.query_terms, IsEmpty());
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId1, kDocumentId0));
}

TEST_F(QueryVisitorTest, IntParsingError) {
  std::string query = "subscription.price < fruit";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).ConsumeResults(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(QueryVisitorTest, NotEqualsUnsupported) {
  std::string query = "subscription.price != 3";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).ConsumeResults(),
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).ConsumeResults(),
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).ConsumeResults(),
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).ConsumeResults(),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(QueryVisitorTest, NeverVisitedReturnsInvalid) {
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  EXPECT_THAT(std::move(query_visitor).ConsumeResults(),
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).ConsumeResults(),
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).ConsumeResults(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(QueryVisitorTest, NumericComparisonPropertyStringIsInvalid) {
  // "price" is a STRING token, which cannot be a property name.
  std::string query = R"("price" > 7)";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).ConsumeResults(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(QueryVisitorTest, SingleTermTermFrequencyEnabled) {
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""], UnorderedElementsAre("foo"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foo"));

  ASSERT_THAT(query_results.root_iterator->Advance(), IsOk());
  std::vector<TermMatchInfo> match_infos;
  query_results.root_iterator->PopulateMatchedTermsStats(&match_infos);
  std::unordered_map<SectionId, Hit::TermFrequency>
      expected_section_ids_tf_map = {{kSectionId1, 1}};
  EXPECT_THAT(match_infos, ElementsAre(EqualsTermMatchInfo(
                               "foo", expected_section_ids_tf_map)));

  ASSERT_THAT(query_results.root_iterator->Advance(), IsOk());
  match_infos.clear();
  query_results.root_iterator->PopulateMatchedTermsStats(&match_infos);
  EXPECT_THAT(match_infos, ElementsAre(EqualsTermMatchInfo(
                               "foo", expected_section_ids_tf_map)));

  EXPECT_THAT(query_results.root_iterator->Advance(),
              StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
}

TEST_F(QueryVisitorTest, SingleTermTermFrequencyDisabled) {
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/false);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""], UnorderedElementsAre("foo"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators), IsEmpty());

  ASSERT_THAT(query_results.root_iterator->Advance(), IsOk());
  std::vector<TermMatchInfo> match_infos;
  query_results.root_iterator->PopulateMatchedTermsStats(&match_infos);
  std::unordered_map<SectionId, Hit::TermFrequency>
      expected_section_ids_tf_map = {{kSectionId1, 0}};
  EXPECT_THAT(match_infos, ElementsAre(EqualsTermMatchInfo(
                               "foo", expected_section_ids_tf_map)));

  ASSERT_THAT(query_results.root_iterator->Advance(), IsOk());
  match_infos.clear();
  query_results.root_iterator->PopulateMatchedTermsStats(&match_infos);
  EXPECT_THAT(match_infos, ElementsAre(EqualsTermMatchInfo(
                               "foo", expected_section_ids_tf_map)));

  EXPECT_THAT(query_results.root_iterator->Advance(),
              StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use,
              ElementsAre(kVerbatimSearchFeature));
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""],
              UnorderedElementsAre("foo:bar(baz)"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foo:bar(baz)"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use,
              ElementsAre(kVerbatimSearchFeature));
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""],
              UnorderedElementsAre(R"(foobar")"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre(R"(foobar")"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId2));
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use,
              ElementsAre(kVerbatimSearchFeature));
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""],
              UnorderedElementsAre(R"(foobar\)"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre(R"(foobar\)"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId1));
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use,
              ElementsAre(kVerbatimSearchFeature));
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""],
              UnorderedElementsAre(R"(foobary)"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre(R"(foobary)"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId0));

  // Issue a query for the verbatim token `foobar\y`.
  query = R"("foobar\\y")";
  ICING_ASSERT_OK_AND_ASSIGN(root_node, ParseQueryHelper(query));
  QueryVisitor query_visitor_two(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor_two);
  ICING_ASSERT_OK_AND_ASSIGN(query_results,
                             std::move(query_visitor_two).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use,
              ElementsAre(kVerbatimSearchFeature));
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""],
              UnorderedElementsAre(R"(foobar\y)"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre(R"(foobar\y)"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId2));
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use,
              ElementsAre(kVerbatimSearchFeature));
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""], UnorderedElementsAre("foobar\n"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foobar\n"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId0));

  // Now, issue a query for the verbatim token `foobar\n`.
  query = R"("foobar\\n")";
  ICING_ASSERT_OK_AND_ASSIGN(root_node, ParseQueryHelper(query));
  QueryVisitor query_visitor_two(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor_two);
  ICING_ASSERT_OK_AND_ASSIGN(query_results,
                             std::move(query_visitor_two).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use,
              ElementsAre(kVerbatimSearchFeature));
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""],
              UnorderedElementsAre(R"(foobar\n)"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre(R"(foobar\n)"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId2));
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use,
              ElementsAre(kVerbatimSearchFeature));
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""],
              UnorderedElementsAre(R"(foo\"bar\nbaz")"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre(R"(foo\"bar\nbaz")"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId0));
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(ExtractKeys(query_results.query_terms), IsEmpty());
  EXPECT_THAT(query_results.query_term_iterators, IsEmpty());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId2));
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.query_terms, IsEmpty());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(query_results.query_term_iterators, IsEmpty());
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId2));
}

TEST_F(QueryVisitorTest, NestedNotTerms) {
  // Setup the index with docs 0, 1 and 2 holding the values
  // ["foo", "bar", "baz"], ["foo", "baz"] and ["bar", "baz"] respectively.
  ICING_ASSERT_OK(schema_store_->SetSchema(
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("type"))
          .Build()));

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri0").SetSchema("type").Build()));
  Index::Editor editor = index_->Edit(kDocumentId0, kSectionId1,
                                      TERM_MATCH_PREFIX, /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.BufferTerm("bar");
  editor.BufferTerm("baz");
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri1").SetSchema("type").Build()));
  editor = index_->Edit(kDocumentId1, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.BufferTerm("baz");
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri2").SetSchema("type").Build()));
  editor = index_->Edit(kDocumentId2, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("bar");
  editor.BufferTerm("baz");
  editor.IndexAllBufferedTerms();

  // Double negative could be rewritten as `(foo AND NOT bar) baz`
  std::string query = "NOT (-foo OR bar) baz";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""],
              UnorderedElementsAre("foo", "baz"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foo", "baz"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId1));
}

TEST_F(QueryVisitorTest, DeeplyNestedNotTerms) {
  // Setup the index with docs 0, 1 and 2 holding the values
  // ["foo", "bar", "baz"], ["foo", "baz"] and ["bar", "baz"] respectively.
  ICING_ASSERT_OK(schema_store_->SetSchema(
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("type"))
          .Build()));

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri0").SetSchema("type").Build()));
  Index::Editor editor = index_->Edit(kDocumentId0, kSectionId1,
                                      TERM_MATCH_PREFIX, /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.BufferTerm("bar");
  editor.BufferTerm("baz");
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri1").SetSchema("type").Build()));
  editor = index_->Edit(kDocumentId1, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("foo");
  editor.BufferTerm("baz");
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri2").SetSchema("type").Build()));
  editor = index_->Edit(kDocumentId2, kSectionId1, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("bar");
  editor.BufferTerm("baz");
  editor.IndexAllBufferedTerms();

  // Simplifying:
  //   NOT (-(NOT (foo -bar) baz) -bat) NOT bass
  //   NOT (-((-foo OR bar) baz) -bat) NOT bass
  //   NOT (((foo -bar) OR -baz) -bat) NOT bass
  //   (((-foo OR bar) baz) OR bat) NOT bass
  //
  // Doc 0 : (((-TRUE OR TRUE) TRUE) OR FALSE) NOT FALSE ->
  //         ((FALSE OR TRUE) TRUE) TRUE -> ((TRUE) TRUE) TRUE -> TRUE
  // Doc 1 : (((-TRUE OR FALSE) TRUE) OR FALSE) NOT FALSE
  //         ((FALSE OR FALSE) TRUE) TRUE -> ((FALSE) TRUE) TRUE -> FALSE
  // Doc 2 : (((-FALSE OR TRUE) TRUE) OR FALSE) NOT FALSE
  //         ((TRUE OR TRUE) TRUE) TRUE -> ((TRUE) TRUE) TRUE -> TRUE
  std::string query = "NOT (-(NOT (foo -bar) baz) -bat) NOT bass";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""],
              UnorderedElementsAre("bar", "baz", "bat"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("bar", "baz", "bat"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId2, kDocumentId0));
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""],
              UnorderedElementsAre("foo", "bar"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foo", "bar"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId1));
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""],
              UnorderedElementsAre("foo", "bar"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foo", "bar"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId1));
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""],
              UnorderedElementsAre("foo", "bar"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foo", "bar"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""],
              UnorderedElementsAre("foo", "bar", "baz"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foo", "bar", "baz"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId2, kDocumentId1));

  // Should be interpreted like `(bar OR baz) foo`
  query = "bar OR baz foo";
  ICING_ASSERT_OK_AND_ASSIGN(root_node, ParseQueryHelper(query));
  QueryVisitor query_visitor_two(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor_two);
  ICING_ASSERT_OK_AND_ASSIGN(query_results,
                             std::move(query_visitor_two).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""],
              UnorderedElementsAre("foo", "bar", "baz"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foo", "bar", "baz"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId2, kDocumentId1));

  query = "(bar OR baz) foo";
  ICING_ASSERT_OK_AND_ASSIGN(root_node, ParseQueryHelper(query));
  QueryVisitor query_visitor_three(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor_three);
  ICING_ASSERT_OK_AND_ASSIGN(query_results,
                             std::move(query_visitor_three).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""],
              UnorderedElementsAre("foo", "bar", "baz"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foo", "bar", "baz"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""],
              UnorderedElementsAre("foo", "baz"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foo", "baz"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId2, kDocumentId0));

  query = "foo NOT (bar OR baz)";
  ICING_ASSERT_OK_AND_ASSIGN(root_node, ParseQueryHelper(query));
  QueryVisitor query_visitor_two(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor_two);
  ICING_ASSERT_OK_AND_ASSIGN(query_results,
                             std::move(query_visitor_two).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  EXPECT_THAT(query_results.query_terms[""], UnorderedElementsAre("foo"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foo"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId0));
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(ExtractKeys(query_results.query_terms),
              UnorderedElementsAre("prop1"));
  EXPECT_THAT(query_results.query_terms["prop1"], UnorderedElementsAre("foo"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foo"));
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId1, kDocumentId0));
}

TEST_F(QueryVisitorTest, PropertyFilterStringIsInvalid) {
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

  // "prop1" is a STRING token, which cannot be a property name.
  std::string query = R"("prop1":foo)";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  EXPECT_THAT(std::move(query_visitor).ConsumeResults(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(QueryVisitorTest, PropertyFilterNonNormalized) {
  ICING_ASSERT_OK(schema_store_->SetSchema(
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("type")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("PROP1")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("PROP2")
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

  std::string query = "PROP1:foo";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(ExtractKeys(query_results.query_terms),
              UnorderedElementsAre("PROP1"));
  EXPECT_THAT(query_results.query_terms["PROP1"], UnorderedElementsAre("foo"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foo"));
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_terms),
              UnorderedElementsAre("prop1"));
  EXPECT_THAT(query_results.query_terms["prop1"],
              UnorderedElementsAre("foo", "bar"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foo", "bar"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId1, kDocumentId0));
}

TEST_F(QueryVisitorTest, ValidNestedPropertyFilter) {
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

  std::string query = "prop1:(prop1:foo)";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_terms),
              UnorderedElementsAre("prop1"));
  EXPECT_THAT(query_results.query_terms["prop1"], UnorderedElementsAre("foo"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foo"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId1));

  query = "prop1:(prop1:(prop1:(prop1:(prop1:foo))))";
  ICING_ASSERT_OK_AND_ASSIGN(root_node, ParseQueryHelper(query));
  QueryVisitor query_visitor_two(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor_two);
  ICING_ASSERT_OK_AND_ASSIGN(query_results,
                             std::move(query_visitor_two).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_terms),
              UnorderedElementsAre("prop1"));
  EXPECT_THAT(query_results.query_terms["prop1"], UnorderedElementsAre("foo"));
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foo"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId1));
}

TEST_F(QueryVisitorTest, InvalidNestedPropertyFilter) {
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

  std::string query = "prop1:(prop2:foo)";
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_terms), IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foo"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()), IsEmpty());

  query = "prop1:(prop2:(prop1:(prop2:(prop1:foo))))";
  ICING_ASSERT_OK_AND_ASSIGN(root_node, ParseQueryHelper(query));
  QueryVisitor query_visitor_two(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor_two);
  ICING_ASSERT_OK_AND_ASSIGN(query_results,
                             std::move(query_visitor_two).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_terms), IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
              UnorderedElementsAre("foo"));
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()), IsEmpty());
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
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_terms), IsEmpty());
  EXPECT_THAT(query_results.query_term_iterators, IsEmpty());
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId2));

  query = "NOT prop1:(foo OR bar)";
  ICING_ASSERT_OK_AND_ASSIGN(root_node, ParseQueryHelper(query));
  QueryVisitor query_visitor_two(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor_two);
  ICING_ASSERT_OK_AND_ASSIGN(query_results,
                             std::move(query_visitor_two).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_terms), IsEmpty());
  EXPECT_THAT(query_results.query_term_iterators, IsEmpty());
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId2));
}

TEST_F(QueryVisitorTest, SegmentationTest) {
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

  // ICU segmentation will break this into "每天" and "上班".
  // CFStringTokenizer (ios) will break this into "每", "天" and "上班"
  std::string query = "每天上班";
  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri0").SetSchema("type").Build()));
  Index::Editor editor = index_->Edit(kDocumentId0, prop1_section_id,
                                      TERM_MATCH_PREFIX, /*namespace_id=*/0);
  editor.BufferTerm("上班");
  editor.IndexAllBufferedTerms();
  editor = index_->Edit(kDocumentId0, prop2_section_id, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  if (IsCfStringTokenization()) {
    editor.BufferTerm("每");
    editor.BufferTerm("天");
  } else {
    editor.BufferTerm("每天");
  }
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri1").SetSchema("type").Build()));
  editor = index_->Edit(kDocumentId1, prop1_section_id, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  editor.BufferTerm("上班");
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK(document_store_->Put(
      DocumentBuilder().SetKey("ns", "uri2").SetSchema("type").Build()));
  editor = index_->Edit(kDocumentId2, prop2_section_id, TERM_MATCH_PREFIX,
                        /*namespace_id=*/0);
  if (IsCfStringTokenization()) {
    editor.BufferTerm("每");
    editor.BufferTerm("天");
  } else {
    editor.BufferTerm("每天");
  }
  editor.IndexAllBufferedTerms();

  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<Node> root_node,
                             ParseQueryHelper(query));
  QueryVisitor query_visitor(
      index_.get(), numeric_index_.get(), document_store_.get(),
      schema_store_.get(), normalizer_.get(), tokenizer_.get(),
      DocHitInfoIteratorFilter::Options(), TERM_MATCH_PREFIX,
      /*needs_term_frequency_info_=*/true);
  root_node->Accept(&query_visitor);
  ICING_ASSERT_OK_AND_ASSIGN(QueryResults query_results,
                             std::move(query_visitor).ConsumeResults());
  EXPECT_THAT(query_results.features_in_use, IsEmpty());
  EXPECT_THAT(ExtractKeys(query_results.query_terms), UnorderedElementsAre(""));
  if (IsCfStringTokenization()) {
    EXPECT_THAT(query_results.query_terms[""],
                UnorderedElementsAre("每", "天", "上班"));
    EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
                UnorderedElementsAre("每", "天", "上班"));
  } else {
    EXPECT_THAT(query_results.query_terms[""],
                UnorderedElementsAre("每天", "上班"));
    EXPECT_THAT(ExtractKeys(query_results.query_term_iterators),
                UnorderedElementsAre("每天", "上班"));
  }
  EXPECT_THAT(GetDocumentIds(query_results.root_iterator.get()),
              ElementsAre(kDocumentId0));
}

}  // namespace

}  // namespace lib
}  // namespace icing
