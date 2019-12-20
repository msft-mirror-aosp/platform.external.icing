// Copyright (C) 2019 Google LLC
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

#include "icing/query/query-processor.h"

#include <memory>
#include <string>

#include "utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/index/hit/doc-hit-info.h"
#include "icing/index/index.h"
#include "icing/index/iterator/doc-hit-info-iterator-test-util.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/legacy/index/icing-filesystem.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/transform/normalizer.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::SizeIs;
using ::testing::Test;
using ::testing::UnorderedElementsAre;

class QueryProcessorTest : public Test {
 protected:
  QueryProcessorTest()
      : test_dir_(GetTestTempDir() + "/icing"),
        index_dir_(test_dir_ + "/index"),
        store_dir_(test_dir_ + "/store") {}

  void SetUp() override {
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(index_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(store_dir_.c_str());

    ICING_ASSERT_OK(
        // File generated via icu_data_file rule in //icing/BUILD.
        SetUpICUDataFile("icing/icu.dat"));

    Index::Options options(index_dir_,
                           /*index_merge_size=*/1024 * 1024);
    ICING_ASSERT_OK_AND_ASSIGN(index_,
                               Index::Create(options, &icing_filesystem_));

    ICING_ASSERT_OK_AND_ASSIGN(language_segmenter_,
                               LanguageSegmenter::Create(GetLangIdModelPath()));

    ICING_ASSERT_OK_AND_ASSIGN(normalizer_,
                               Normalizer::Create(/*max_term_byte_size=*/1000));

    SchemaProto schema;

    // Message schema
    auto type_config = schema.add_types();
    type_config->set_schema_type("message");

    // Add an indexed property so we generate section metadata on it
    auto property = type_config->add_properties();
    property->set_property_name("foo");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
    property->mutable_indexing_config()->set_term_match_type(
        TermMatchType::EXACT_ONLY);
    property->mutable_indexing_config()->set_tokenizer_type(
        IndexingConfig::TokenizerType::PLAIN);

    // Add another indexed property so we generate section metadata on it
    property = type_config->add_properties();
    property->set_property_name(indexed_property_);
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
    property->mutable_indexing_config()->set_term_match_type(
        TermMatchType::EXACT_ONLY);
    property->mutable_indexing_config()->set_tokenizer_type(
        IndexingConfig::TokenizerType::PLAIN);

    // Since we order indexed properties alphabetically, "foo" gets section id
    // 0, and "subject" gets section id 1 for messages
    indexed_message_section_id_ = 1;

    // Email schema
    type_config = schema.add_types();
    type_config->set_schema_type("email");

    // Add an indexed property so we generate section metadata on it
    property = type_config->add_properties();
    property->set_property_name(indexed_property_);
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
    property->mutable_indexing_config()->set_term_match_type(
        TermMatchType::EXACT_ONLY);
    property->mutable_indexing_config()->set_tokenizer_type(
        IndexingConfig::TokenizerType::PLAIN);

    // First and only indexed property, so it gets the first id of 0
    indexed_email_section_id_ = 0;

    // Add an unindexed property
    property = type_config->add_properties();
    property->set_property_name(unindexed_property_);
    property->set_data_type(PropertyConfigProto::DataType::STRING);

    ICING_ASSERT_OK_AND_ASSIGN(schema_store_,
                               SchemaStore::Create(&filesystem_, test_dir_));
    ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

    ICING_ASSERT_OK_AND_ASSIGN(
        document_store_,
        DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                              schema_store_.get()));
  }

  libtextclassifier3::Status AddTokenToIndex(
      DocumentId document_id, SectionId section_id,
      TermMatchType::Code term_match_type, const std::string& token) {
    Index::Editor editor =
        index_->Edit(document_id, section_id, term_match_type);
    return editor.AddHit(token.c_str());
  }

  void TearDown() override {
    document_store_.reset();
    schema_store_.reset();
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  std::unique_ptr<Index> index_;
  std::unique_ptr<LanguageSegmenter> language_segmenter_;
  std::unique_ptr<Normalizer> normalizer_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
  FakeClock fake_clock_;
  const std::string indexed_property_ = "subject";
  const std::string unindexed_property_ = "to";
  int indexed_email_section_id_;
  int indexed_message_section_id_;

 private:
  IcingFilesystem icing_filesystem_;
  Filesystem filesystem_;
  const std::string test_dir_;
  const std::string index_dir_;
  const std::string store_dir_;
};

TEST_F(QueryProcessorTest, EmptyGroupMatchAllDocuments) {
  // We don't need to insert anything in the index since the empty query will
  // match all DocumentIds from the DocumentStore

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  search_spec.set_query("()");

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocumentIds(results.root_iterator.get()),
              ElementsAre(document_id2, document_id1));
  EXPECT_THAT(results.query_terms, IsEmpty());
}

TEST_F(QueryProcessorTest, EmptyQueryMatchAllDocuments) {
  // We don't need to insert anything in the index since the empty query will
  // match all DocumentIds from the DocumentStore

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  search_spec.set_query("");

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocumentIds(results.root_iterator.get()),
              ElementsAre(document_id2, document_id1));
  EXPECT_THAT(results.query_terms, IsEmpty());
}

TEST_F(QueryProcessorTest, QueryTermNormalized) {
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "world"),
      IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  search_spec.set_query("hElLo WORLD");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("hello", "world"));
}

TEST_F(QueryProcessorTest, OneTermPrefixMatch) {
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::PREFIX;

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  search_spec.set_query("he");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("he"));
}

TEST_F(QueryProcessorTest, OneTermExactMatch) {
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  search_spec.set_query("hello");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("hello"));
}

TEST_F(QueryProcessorTest, AndTwoTermExactMatch) {
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're just
  // inserting the documents so that the DocHitInfoIterators will see that the
  // document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "world"),
      IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  search_spec.set_query("hello world");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("hello", "world"));
}

TEST_F(QueryProcessorTest, AndTwoTermPrefixMatch) {
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::PREFIX;

  // These documents don't actually match to the tokens in the index. We're just
  // inserting the documents so that the DocHitInfoIterators will see that the
  // document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "world"),
      IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  search_spec.set_query("he wo");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("he", "wo"));
}

TEST_F(QueryProcessorTest, AndTwoTermPrefixAndExactMatch) {
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;

  // These documents don't actually match to the tokens in the index. We're just
  // inserting the documents so that the DocHitInfoIterators will see that the
  // document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  EXPECT_THAT(AddTokenToIndex(document_id, section_id,
                              TermMatchType::EXACT_ONLY, "hello"),
              IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, TermMatchType::PREFIX, "world"),
      IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  search_spec.set_query("hello wo");
  search_spec.set_term_match_type(TermMatchType::PREFIX);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("hello", "wo"));
}

TEST_F(QueryProcessorTest, OrTwoTermExactMatch) {
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're just
  // inserting the documents so that the DocHitInfoIterators will see that the
  // document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "hello"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "world"),
      IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  search_spec.set_query("hello OR world");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id2, section_id_mask),
                          DocHitInfo(document_id1, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("hello", "world"));
}

TEST_F(QueryProcessorTest, OrTwoTermPrefixMatch) {
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::PREFIX;

  // These documents don't actually match to the tokens in the index. We're just
  // inserting the documents so that the DocHitInfoIterators will see that the
  // document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "hello"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "world"),
      IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  search_spec.set_query("he OR wo");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id2, section_id_mask),
                          DocHitInfo(document_id1, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("he", "wo"));
}

TEST_F(QueryProcessorTest, OrTwoTermPrefixAndExactMatch) {
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;

  // These documents don't actually match to the tokens in the index. We're just
  // inserting the documents so that the DocHitInfoIterators will see that the
  // document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  EXPECT_THAT(AddTokenToIndex(document_id1, section_id,
                              TermMatchType::EXACT_ONLY, "hello"),
              IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, TermMatchType::PREFIX, "world"),
      IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  search_spec.set_query("hello OR wo");
  search_spec.set_term_match_type(TermMatchType::PREFIX);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id2, section_id_mask),
                          DocHitInfo(document_id1, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("hello", "wo"));
}

TEST_F(QueryProcessorTest, CombinedAndOrTerms) {
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're just
  // inserting the documents so that the DocHitInfoIterators will see that the
  // document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Document 1 has content "animal puppy dog"
  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "animal"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "puppy"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());

  // Document 2 has content "animal kitten cat"
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "animal"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "kitten"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);

  {
    // OR gets precedence over AND, this is parsed as ((puppy OR kitten) AND
    // dog)
    SearchSpecProto search_spec;
    search_spec.set_query("puppy OR kitten dog");
    search_spec.set_term_match_type(term_match_type);

    ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                               query_processor.ParseSearch(search_spec));

    // Only Document 1 matches since it has puppy AND dog
    EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
                ElementsAre(DocHitInfo(document_id1, section_id_mask)));
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""],
                UnorderedElementsAre("puppy", "kitten", "dog"));
  }

  {
    // OR gets precedence over AND, this is parsed as (animal AND (puppy OR
    // kitten))
    SearchSpecProto search_spec;
    search_spec.set_query("animal puppy OR kitten");
    search_spec.set_term_match_type(term_match_type);

    ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                               query_processor.ParseSearch(search_spec));

    // Both Document 1 and 2 match since Document 1 has puppy AND dog, and
    // Document 2 has kitten
    // Descending order of valid DocumentIds
    EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
                ElementsAre(DocHitInfo(document_id2, section_id_mask),
                            DocHitInfo(document_id1, section_id_mask)));
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""],
                UnorderedElementsAre("animal", "puppy", "kitten"));
  }

  {
    // OR gets precedence over AND, this is parsed as (kitten AND ((foo OR bar)
    // OR cat))
    SearchSpecProto search_spec;
    search_spec.set_query("kitten foo OR bar OR cat");
    search_spec.set_term_match_type(term_match_type);

    ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                               query_processor.ParseSearch(search_spec));

    // Only Document 2 matches since it has both kitten and cat
    EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
                ElementsAre(DocHitInfo(document_id2, section_id_mask)));
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""],
                UnorderedElementsAre("kitten", "foo", "bar", "cat"));
  }
}

TEST_F(QueryProcessorTest, OneGroup) {
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're just
  // inserting the documents so that the DocHitInfoIterators will see that the
  // document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Document 1 has content "puppy dog"
  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "puppy"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());

  // Document 2 has content "kitten cat"
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "kitten"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);

  // Without grouping, this would be parsed as ((puppy OR kitten) AND foo) and
  // no documents would match. But with grouping, Document 1 matches puppy
  SearchSpecProto search_spec;
  search_spec.set_query("puppy OR (kitten foo)");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id1, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""],
              UnorderedElementsAre("puppy", "kitten", "foo"));
}

TEST_F(QueryProcessorTest, TwoGroups) {
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're just
  // inserting the documents so that the DocHitInfoIterators will see that the
  // document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Document 1 has content "puppy dog"
  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "puppy"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());

  // Document 2 has content "kitten cat"
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "kitten"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);

  // Without grouping, this would be parsed as (puppy AND (dog OR kitten) AND
  // cat) and wouldn't match any documents. But with grouping, Document 1
  // matches (puppy AND dog) and Document 2 matches (kitten and cat).
  SearchSpecProto search_spec;
  search_spec.set_query("(puppy dog) OR (kitten cat)");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id2, section_id_mask),
                          DocHitInfo(document_id1, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""],
              UnorderedElementsAre("puppy", "dog", "kitten", "cat"));
}

TEST_F(QueryProcessorTest, ManyLevelNestedGrouping) {
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're just
  // inserting the documents so that the DocHitInfoIterators will see that the
  // document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Document 1 has content "puppy dog"
  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "puppy"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());

  // Document 2 has content "kitten cat"
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "kitten"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);

  // Without grouping, this would be parsed as ((puppy OR kitten) AND foo) and
  // no documents would match. But with grouping, Document 1 matches puppy
  SearchSpecProto search_spec;
  search_spec.set_query("puppy OR ((((kitten foo))))");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id1, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""],
              UnorderedElementsAre("puppy", "kitten", "foo"));
}

TEST_F(QueryProcessorTest, OneLevelNestedGrouping) {
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're just
  // inserting the documents so that the DocHitInfoIterators will see that the
  // document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Document 1 has content "puppy dog"
  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "puppy"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());

  // Document 2 has content "kitten cat"
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "kitten"),
      IsOk());
  EXPECT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  // Document 1 will match puppy and Document 2 matches (kitten AND (cat))
  SearchSpecProto search_spec;
  search_spec.set_query("puppy OR (kitten(cat))");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id2, section_id_mask),
                          DocHitInfo(document_id1, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""],
              UnorderedElementsAre("puppy", "kitten", "cat"));
}

TEST_F(QueryProcessorTest, ExcludeTerm) {
  SectionId section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're just
  // inserting the documents so that they'll bump the last_added_document_id,
  // which will give us the proper exclusion results
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  ASSERT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "hello"),
      IsOk());
  ASSERT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "world"),
      IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  search_spec.set_query("-hello");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // We don't know have the section mask to indicate what section "world" came.
  // It doesn't matter which section it was in since the query doesn't care.  It
  // just wanted documents that didn't have "hello"
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id2, kSectionIdMaskNone)));
  EXPECT_THAT(results.query_terms, IsEmpty());
}

TEST_F(QueryProcessorTest, ExcludeNonexistentTerm) {
  SectionId section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're just
  // inserting the documents so that they'll bump the last_added_document_id,
  // which will give us the proper exclusion results
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  ASSERT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "hello"),
      IsOk());
  ASSERT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "world"),
      IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  search_spec.set_query("-foo");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id2, kSectionIdMaskNone),
                          DocHitInfo(document_id1, kSectionIdMaskNone)));
  EXPECT_THAT(results.query_terms, IsEmpty());
}

TEST_F(QueryProcessorTest, ExcludeAnd) {
  SectionId section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're just
  // inserting the documents so that they'll bump the last_added_document_id,
  // which will give us the proper exclusion results
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Document 1 has content "animal dog"
  ASSERT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "animal"),
      IsOk());
  ASSERT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());

  // Document 2 has content "animal cat"
  ASSERT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "animal"),
      IsOk());
  ASSERT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  {
    SearchSpecProto search_spec;
    search_spec.set_query("-dog -cat");
    search_spec.set_term_match_type(term_match_type);

    ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                               query_processor.ParseSearch(search_spec));

    // The query is interpreted as "exclude all documents that have animal, and
    // exclude all documents that have cat". Since both documents contain
    // animal, there are no results.
    EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()), IsEmpty());
    EXPECT_THAT(results.query_terms, IsEmpty());
  }

  {
    SearchSpecProto search_spec;
    search_spec.set_query("-animal cat");
    search_spec.set_term_match_type(term_match_type);

    ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                               query_processor.ParseSearch(search_spec));

    // The query is interpreted as "exclude all documents that have animal, and
    // include all documents that have cat". Since both documents contain
    // animal, there are no results.
    EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()), IsEmpty());
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("cat"));
  }
}

TEST_F(QueryProcessorTest, ExcludeOr) {
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're just
  // inserting the documents so that they'll bump the last_added_document_id,
  // which will give us the proper exclusion results
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Document 1 has content "animal dog"
  ASSERT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "animal"),
      IsOk());
  ASSERT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());

  // Document 2 has content "animal cat"
  ASSERT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "animal"),
      IsOk());
  ASSERT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  {
    SearchSpecProto search_spec;
    search_spec.set_query("-animal OR -cat");
    search_spec.set_term_match_type(term_match_type);

    ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                               query_processor.ParseSearch(search_spec));

    // We don't have a section mask indicating which sections in this document
    // matched the query since it's not based on section-term matching. It's
    // more based on the fact that the query excluded all the other documents.
    EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
                ElementsAre(DocHitInfo(document_id1, kSectionIdMaskNone)));
    EXPECT_THAT(results.query_terms, IsEmpty());
  }

  {
    SearchSpecProto search_spec;
    search_spec.set_query("animal OR -cat");
    search_spec.set_term_match_type(term_match_type);

    ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                               query_processor.ParseSearch(search_spec));

    // Descending order of valid DocumentIds
    EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
                ElementsAre(DocHitInfo(document_id2, section_id_mask),
                            DocHitInfo(document_id1, section_id_mask)));
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("animal"));
  }
}

TEST_F(QueryProcessorTest, DeletedFilter) {
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("email")
                                                      .Build()));
  EXPECT_THAT(document_store_->Delete("namespace", "1"), IsOk());

  // Document 1 has content "animal dog"
  ASSERT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "animal"),
      IsOk());
  ASSERT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());

  // Document 2 has content "animal cat"
  ASSERT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "animal"),
      IsOk());
  ASSERT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  search_spec.set_query("animal");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id2, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, NamespaceFilter) {
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace2", "2")
                                                      .SetSchema("email")
                                                      .Build()));

  // Document 1 has content "animal dog"
  ASSERT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "animal"),
      IsOk());
  ASSERT_THAT(AddTokenToIndex(document_id1, section_id, term_match_type, "dog"),
              IsOk());

  // Document 2 has content "animal cat"
  ASSERT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "animal"),
      IsOk());
  ASSERT_THAT(AddTokenToIndex(document_id2, section_id, term_match_type, "cat"),
              IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  search_spec.set_query("animal");
  search_spec.set_term_match_type(term_match_type);
  search_spec.add_namespace_filters("namespace1");

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id1, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, SchemaTypeFilter) {
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("message")
                                                      .Build()));

  // Document 1 has content "animal dog"
  ASSERT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "animal"),
      IsOk());

  // Document 2 has content "animal cat"
  ASSERT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "animal"),
      IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  search_spec.set_query("animal");
  search_spec.set_term_match_type(term_match_type);
  search_spec.add_schema_type_filters("email");

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id1, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, SectionFilterForOneDocument) {
  SectionIdMask section_id_mask = 1U << indexed_email_section_id_;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Document has content "animal"
  ASSERT_THAT(AddTokenToIndex(document_id, indexed_email_section_id_,
                              term_match_type, "animal"),
              IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>'
  search_spec.set_query(indexed_property_ + ":animal");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[indexed_property_],
              UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, SectionFilterAcrossSchemaTypes) {
  SectionIdMask email_section_id_mask = 1U << indexed_email_section_id_;
  SectionIdMask message_section_id_mask = 1U << indexed_message_section_id_;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("message")
                                                      .Build()));

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, indexed_email_section_id_,
                              term_match_type, "animal"),
              IsOk());

  // Message document has content "animal"
  ASSERT_THAT(AddTokenToIndex(message_document_id, indexed_message_section_id_,
                              term_match_type, "animal"),
              IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>'
  search_spec.set_query(indexed_property_ + ":animal");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Ordered by descending DocumentId, so message comes first since it was
  // inserted last
  EXPECT_THAT(
      GetDocHitInfos(results.root_iterator.get()),
      ElementsAre(DocHitInfo(message_document_id, message_section_id_mask),
                  DocHitInfo(email_document_id, email_section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[indexed_property_],
              UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, SectionFilterWithinSchemaType) {
  SectionIdMask email_section_id_mask = 1U << indexed_email_section_id_;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("message")
                                                      .Build()));

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, indexed_email_section_id_,
                              term_match_type, "animal"),
              IsOk());

  // Message document has content "animal"
  ASSERT_THAT(AddTokenToIndex(message_document_id, indexed_message_section_id_,
                              term_match_type, "animal"),
              IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>', but only look within
  // documents of email schema
  search_spec.set_query(indexed_property_ + ":animal");
  search_spec.add_schema_type_filters("email");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Shouldn't include the message document since we're only looking at email
  // types
  EXPECT_THAT(
      GetDocHitInfos(results.root_iterator.get()),
      ElementsAre(DocHitInfo(email_document_id, email_section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[indexed_property_],
              UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, SectionFilterRespectsDifferentSectionIds) {
  SectionIdMask email_section_id_mask = 1U << indexed_email_section_id_;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("message")
                                                      .Build()));

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, indexed_email_section_id_,
                              term_match_type, "animal"),
              IsOk());

  // Message document has content "animal", but put in in the same section id as
  // the indexed email section id, the same id as indexed property "foo" in the
  // message type
  ASSERT_THAT(AddTokenToIndex(message_document_id, indexed_email_section_id_,
                              term_match_type, "animal"),
              IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>', but only look within
  // documents of email schema
  search_spec.set_query(indexed_property_ + ":animal");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Even though the section id is the same, we should be able to tell that it
  // doesn't match to the name of the section filter
  EXPECT_THAT(
      GetDocHitInfos(results.root_iterator.get()),
      ElementsAre(DocHitInfo(email_document_id, email_section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[indexed_property_],
              UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, NonexistentSectionFilterReturnsEmptyResults) {
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, indexed_email_section_id_,
                              term_match_type, "animal"),
              IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>', but only look within
  // documents of email schema
  search_spec.set_query("nonexistent.section:animal");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Even though the section id is the same, we should be able to tell that it
  // doesn't match to the name of the section filter
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()), IsEmpty());
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms["nonexistent.section"],
              UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, UnindexedSectionFilterReturnsEmptyResults) {
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, indexed_email_section_id_,
                              term_match_type, "animal"),
              IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>', but only look within
  // documents of email schema
  search_spec.set_query(unindexed_property_ + ":animal");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Even though the section id is the same, we should be able to tell that it
  // doesn't match to the name of the section filter
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()), IsEmpty());
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[unindexed_property_],
              UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, SectionFilterTermAndUnrestrictedTerm) {
  SectionIdMask email_section_id_mask = 1U << indexed_email_section_id_;
  SectionIdMask message_section_id_mask = 1U << indexed_message_section_id_;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "2")
                                                      .SetSchema("message")
                                                      .Build()));

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, indexed_email_section_id_,
                              term_match_type, "animal"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(email_document_id, indexed_email_section_id_,
                              term_match_type, "cat"),
              IsOk());

  // Message document has content "animal"
  ASSERT_THAT(AddTokenToIndex(message_document_id, indexed_message_section_id_,
                              term_match_type, "animal"),
              IsOk());

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock_);
  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>'
  search_spec.set_query("cat OR " + indexed_property_ + ":animal");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  // Ordered by descending DocumentId, so message comes first since it was
  // inserted last
  EXPECT_THAT(
      GetDocHitInfos(results.root_iterator.get()),
      ElementsAre(DocHitInfo(message_document_id, message_section_id_mask),
                  DocHitInfo(email_document_id, email_section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(2));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("cat"));
  EXPECT_THAT(results.query_terms[indexed_property_],
              UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, DocumentBeforeTtlNotFilteredOut) {
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id,
      document_store_->Put(DocumentBuilder()
                               .SetKey("namespace", "1")
                               .SetSchema("email")
                               .SetCreationTimestampSecs(0)
                               .SetTtlSecs(100)
                               .Build()));

  EXPECT_THAT(AddTokenToIndex(document_id, indexed_email_section_id_,
                              term_match_type, "hello"),
              IsOk());

  // Arbitrary value, just has to be less than the document's creation
  // timestamp + ttl
  FakeClock fake_clock;
  fake_clock.SetSeconds(50);

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock);
  SearchSpecProto search_spec;
  search_spec.set_query("hello");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  SectionIdMask section_id_mask = 1U << indexed_email_section_id_;
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id, section_id_mask)));
}

TEST_F(QueryProcessorTest, DocumentPastTtlFilteredOut) {
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id,
      document_store_->Put(DocumentBuilder()
                               .SetKey("namespace", "1")
                               .SetSchema("email")
                               .SetCreationTimestampSecs(0)
                               .SetTtlSecs(100)
                               .Build()));

  EXPECT_THAT(AddTokenToIndex(document_id, indexed_email_section_id_,
                              term_match_type, "hello"),
              IsOk());

  // Arbitrary value, just has to be greater than the document's creation
  // timestamp + ttl
  FakeClock fake_clock;
  fake_clock.SetSeconds(200);

  QueryProcessor query_processor(index_.get(), language_segmenter_.get(),
                                 normalizer_.get(), document_store_.get(),
                                 schema_store_.get(), &fake_clock);
  SearchSpecProto search_spec;
  search_spec.set_query("hello");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor.ParseSearch(search_spec));

  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()), IsEmpty());
}

}  // namespace

}  // namespace lib
}  // namespace icing
