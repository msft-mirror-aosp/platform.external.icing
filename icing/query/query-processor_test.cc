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

#include "icing/jni/jni-cache.h"
#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/helpers/icu/icu-data-file-helper.h"
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
#include "icing/testing/jni-test-helpers.h"
#include "icing/testing/platform.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/transform/normalizer-factory.h"
#include "icing/transform/normalizer.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::SizeIs;
using ::testing::Test;
using ::testing::UnorderedElementsAre;

SchemaTypeConfigProto* AddSchemaType(SchemaProto* schema,
                                     std::string schema_type) {
  SchemaTypeConfigProto* type_config = schema->add_types();
  type_config->set_schema_type(schema_type);
  return type_config;
}

void AddIndexedProperty(SchemaTypeConfigProto* type_config, std::string name) {
  PropertyConfigProto* property_config = type_config->add_properties();
  property_config->set_property_name(name);
  property_config->set_data_type(PropertyConfigProto::DataType::STRING);
  property_config->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
  property_config->mutable_string_indexing_config()->set_term_match_type(
      TermMatchType::EXACT_ONLY);
  property_config->mutable_string_indexing_config()->set_tokenizer_type(
      StringIndexingConfig::TokenizerType::PLAIN);
}

void AddUnindexedProperty(SchemaTypeConfigProto* type_config,
                          std::string name) {
  PropertyConfigProto* property_config = type_config->add_properties();
  property_config->set_property_name(name);
  property_config->set_data_type(PropertyConfigProto::DataType::STRING);
}

class QueryProcessorTest : public Test {
 protected:
  QueryProcessorTest()
      : test_dir_(GetTestTempDir() + "/icing"),
        store_dir_(test_dir_ + "/store"),
        index_dir_(test_dir_ + "/index") {}

  void SetUp() override {
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(index_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(store_dir_.c_str());

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

    Index::Options options(index_dir_,
                           /*index_merge_size=*/1024 * 1024);
    ICING_ASSERT_OK_AND_ASSIGN(
        index_, Index::Create(options, &filesystem_, &icing_filesystem_));

    language_segmenter_factory::SegmenterOptions segmenter_options(
        ULOC_US, jni_cache_.get());
    ICING_ASSERT_OK_AND_ASSIGN(
        language_segmenter_,
        language_segmenter_factory::Create(segmenter_options));

    ICING_ASSERT_OK_AND_ASSIGN(normalizer_, normalizer_factory::Create(
                                                /*max_term_byte_size=*/1000));
  }

  libtextclassifier3::Status AddTokenToIndex(
      DocumentId document_id, SectionId section_id,
      TermMatchType::Code term_match_type, const std::string& token) {
    Index::Editor editor = index_->Edit(document_id, section_id,
                                        term_match_type, /*namespace_id=*/0);
    return editor.AddHit(token.c_str());
  }

  void TearDown() override {
    document_store_.reset();
    schema_store_.reset();
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  Filesystem filesystem_;
  const std::string test_dir_;
  const std::string store_dir_;
  std::unique_ptr<Index> index_;
  std::unique_ptr<LanguageSegmenter> language_segmenter_;
  std::unique_ptr<Normalizer> normalizer_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
  FakeClock fake_clock_;
  std::unique_ptr<const JniCache> jni_cache_ = GetTestJniCache();

 private:
  IcingFilesystem icing_filesystem_;
  const std::string index_dir_;
};

TEST_F(QueryProcessorTest, CreationWithNullPointerShouldFail) {
  EXPECT_THAT(
      QueryProcessor::Create(/*index=*/nullptr, language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(
      QueryProcessor::Create(index_.get(), /*language_segmenter=*/nullptr,
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             /*normalizer=*/nullptr, document_store_.get(),
                             schema_store_.get(), &fake_clock_),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), /*document_store=*/nullptr,
                             schema_store_.get(), &fake_clock_),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                                     normalizer_.get(), document_store_.get(),
                                     /*schema_store=*/nullptr, &fake_clock_),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                                     normalizer_.get(), document_store_.get(),
                                     schema_store_.get(), /*clock=*/nullptr),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(QueryProcessorTest, EmptyGroupMatchAllDocuments) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

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

  // We don't need to insert anything in the index since the empty query will
  // match all DocumentIds from the DocumentStore

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  search_spec.set_query("()");

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocumentIds(results.root_iterator.get()),
              ElementsAre(document_id2, document_id1));
  EXPECT_THAT(results.query_terms, IsEmpty());
}

TEST_F(QueryProcessorTest, EmptyQueryMatchAllDocuments) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

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

  // We don't need to insert anything in the index since the empty query will
  // match all DocumentIds from the DocumentStore

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  search_spec.set_query("");

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocumentIds(results.root_iterator.get()),
              ElementsAre(document_id2, document_id1));
  EXPECT_THAT(results.query_terms, IsEmpty());
}

TEST_F(QueryProcessorTest, QueryTermNormalized) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "world"),
      IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  search_spec.set_query("hElLo WORLD");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("hello", "world"));
}

TEST_F(QueryProcessorTest, OneTermPrefixMatch) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::PREFIX;

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  search_spec.set_query("he");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("he"));
}

TEST_F(QueryProcessorTest, OneTermExactMatch) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // namespaces populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace1", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  search_spec.set_query("hello");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("hello"));
}

TEST_F(QueryProcessorTest, AndTwoTermExactMatch) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "world"),
      IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  search_spec.set_query("hello world");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("hello", "world"));
}

TEST_F(QueryProcessorTest, AndTwoTermPrefixMatch) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::PREFIX;

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "world"),
      IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  search_spec.set_query("he wo");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("he", "wo"));
}

TEST_F(QueryProcessorTest, AndTwoTermPrefixAndExactMatch) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::PREFIX;

  EXPECT_THAT(AddTokenToIndex(document_id, section_id,
                              TermMatchType::EXACT_ONLY, "hello"),
              IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "world"),
      IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  search_spec.set_query("hello wo");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("hello", "wo"));
}

TEST_F(QueryProcessorTest, OrTwoTermExactMatch) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
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

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "hello"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "world"),
      IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  search_spec.set_query("hello OR world");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id2, section_id_mask),
                          DocHitInfo(document_id1, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("hello", "world"));
}

TEST_F(QueryProcessorTest, OrTwoTermPrefixMatch) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
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

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::PREFIX;

  EXPECT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "hello"),
      IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "world"),
      IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  search_spec.set_query("he OR wo");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id2, section_id_mask),
                          DocHitInfo(document_id1, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("he", "wo"));
}

TEST_F(QueryProcessorTest, OrTwoTermPrefixAndExactMatch) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
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

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;

  EXPECT_THAT(AddTokenToIndex(document_id1, section_id,
                              TermMatchType::EXACT_ONLY, "hello"),
              IsOk());
  EXPECT_THAT(
      AddTokenToIndex(document_id2, section_id, TermMatchType::PREFIX, "world"),
      IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  search_spec.set_query("hello OR wo");
  search_spec.set_term_match_type(TermMatchType::PREFIX);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id2, section_id_mask),
                          DocHitInfo(document_id1, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("hello", "wo"));
}

TEST_F(QueryProcessorTest, CombinedAndOrTerms) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
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
  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

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

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  {
    // OR gets precedence over AND, this is parsed as ((puppy OR kitten) AND
    // dog)
    SearchSpecProto search_spec;
    search_spec.set_query("puppy OR kitten dog");
    search_spec.set_term_match_type(term_match_type);

    ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                               query_processor->ParseSearch(search_spec));

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
                               query_processor->ParseSearch(search_spec));

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
    // OR gets precedence over AND, this is parsed as (kitten AND ((foo OR
    // bar) OR cat))
    SearchSpecProto search_spec;
    search_spec.set_query("kitten foo OR bar OR cat");
    search_spec.set_term_match_type(term_match_type);

    ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                               query_processor->ParseSearch(search_spec));

    // Only Document 2 matches since it has both kitten and cat
    EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
                ElementsAre(DocHitInfo(document_id2, section_id_mask)));
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""],
                UnorderedElementsAre("kitten", "foo", "bar", "cat"));
  }
}

TEST_F(QueryProcessorTest, OneGroup) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
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

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

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

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  // Without grouping, this would be parsed as ((puppy OR kitten) AND foo) and
  // no documents would match. But with grouping, Document 1 matches puppy
  SearchSpecProto search_spec;
  search_spec.set_query("puppy OR (kitten foo)");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id1, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""],
              UnorderedElementsAre("puppy", "kitten", "foo"));
}

TEST_F(QueryProcessorTest, TwoGroups) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
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

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

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

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  // Without grouping, this would be parsed as (puppy AND (dog OR kitten) AND
  // cat) and wouldn't match any documents. But with grouping, Document 1
  // matches (puppy AND dog) and Document 2 matches (kitten and cat).
  SearchSpecProto search_spec;
  search_spec.set_query("(puppy dog) OR (kitten cat)");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id2, section_id_mask),
                          DocHitInfo(document_id1, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""],
              UnorderedElementsAre("puppy", "dog", "kitten", "cat"));
}

TEST_F(QueryProcessorTest, ManyLevelNestedGrouping) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
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

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

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

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  // Without grouping, this would be parsed as ((puppy OR kitten) AND foo) and
  // no documents would match. But with grouping, Document 1 matches puppy
  SearchSpecProto search_spec;
  search_spec.set_query("puppy OR ((((kitten foo))))");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id1, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""],
              UnorderedElementsAre("puppy", "kitten", "foo"));
}

TEST_F(QueryProcessorTest, OneLevelNestedGrouping) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that the DocHitInfoIterators will see
  // that the document exists and not filter out the DocumentId as deleted.
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

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

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

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  // Document 1 will match puppy and Document 2 matches (kitten AND (cat))
  SearchSpecProto search_spec;
  search_spec.set_query("puppy OR (kitten(cat))");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id2, section_id_mask),
                          DocHitInfo(document_id1, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""],
              UnorderedElementsAre("puppy", "kitten", "cat"));
}

TEST_F(QueryProcessorTest, ExcludeTerm) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that they'll bump the
  // last_added_document_id, which will give us the proper exclusion results
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

  // Populate the index
  SectionId section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  ASSERT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "hello"),
      IsOk());
  ASSERT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "world"),
      IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  search_spec.set_query("-hello");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // We don't know have the section mask to indicate what section "world"
  // came. It doesn't matter which section it was in since the query doesn't
  // care.  It just wanted documents that didn't have "hello"
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id2, kSectionIdMaskNone)));
  EXPECT_THAT(results.query_terms, IsEmpty());
}

TEST_F(QueryProcessorTest, ExcludeNonexistentTerm) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that they'll bump the
  // last_added_document_id, which will give us the proper exclusion results
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
  // Populate the index
  SectionId section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  ASSERT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "hello"),
      IsOk());
  ASSERT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "world"),
      IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  search_spec.set_query("-foo");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id2, kSectionIdMaskNone),
                          DocHitInfo(document_id1, kSectionIdMaskNone)));
  EXPECT_THAT(results.query_terms, IsEmpty());
}

TEST_F(QueryProcessorTest, ExcludeAnd) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that they'll bump the
  // last_added_document_id, which will give us the proper exclusion results
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

  // Populate the index
  SectionId section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

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

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  {
    SearchSpecProto search_spec;
    search_spec.set_query("-dog -cat");
    search_spec.set_term_match_type(term_match_type);

    ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                               query_processor->ParseSearch(search_spec));

    // The query is interpreted as "exclude all documents that have animal,
    // and exclude all documents that have cat". Since both documents contain
    // animal, there are no results.
    EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()), IsEmpty());
    EXPECT_THAT(results.query_terms, IsEmpty());
  }

  {
    SearchSpecProto search_spec;
    search_spec.set_query("-animal cat");
    search_spec.set_term_match_type(term_match_type);

    ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                               query_processor->ParseSearch(search_spec));

    // The query is interpreted as "exclude all documents that have animal,
    // and include all documents that have cat". Since both documents contain
    // animal, there are no results.
    EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()), IsEmpty());
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("cat"));
  }
}

TEST_F(QueryProcessorTest, ExcludeOr) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // just inserting the documents so that they'll bump the
  // last_added_document_id, which will give us the proper exclusion results
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

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

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

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  {
    SearchSpecProto search_spec;
    search_spec.set_query("-animal OR -cat");
    search_spec.set_term_match_type(term_match_type);

    ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                               query_processor->ParseSearch(search_spec));

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
                               query_processor->ParseSearch(search_spec));

    // Descending order of valid DocumentIds
    EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
                ElementsAre(DocHitInfo(document_id2, section_id_mask),
                            DocHitInfo(document_id1, section_id_mask)));
    EXPECT_THAT(results.query_terms, SizeIs(1));
    EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("animal"));
  }
}

TEST_F(QueryProcessorTest, DeletedFilter) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

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

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

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

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  search_spec.set_query("animal");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id2, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, NamespaceFilter) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

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

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

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

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  search_spec.set_query("animal");
  search_spec.set_term_match_type(term_match_type);
  search_spec.add_namespace_filters("namespace1");

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id1, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, SchemaTypeFilter) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");
  AddSchemaType(&schema, "message");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

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

  // Populate the index
  SectionId section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Document 1 has content "animal dog"
  ASSERT_THAT(
      AddTokenToIndex(document_id1, section_id, term_match_type, "animal"),
      IsOk());

  // Document 2 has content "animal cat"
  ASSERT_THAT(
      AddTokenToIndex(document_id2, section_id, term_match_type, "animal"),
      IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  search_spec.set_query("animal");
  search_spec.set_term_match_type(term_match_type);
  search_spec.add_schema_type_filters("email");

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id1, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, SectionFilterForOneDocument) {
  // Create the schema and document store
  SchemaProto schema;
  SchemaTypeConfigProto* email_type = AddSchemaType(&schema, "email");

  // First and only indexed property, so it gets a section_id of 0
  AddIndexedProperty(email_type, "subject");
  int subject_section_id = 0;

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  SectionIdMask section_id_mask = 1U << subject_section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Document has content "animal"
  ASSERT_THAT(AddTokenToIndex(document_id, subject_section_id, term_match_type,
                              "animal"),
              IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>'
  search_spec.set_query("subject:animal");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Descending order of valid DocumentIds
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id, section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms["subject"], UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, SectionFilterAcrossSchemaTypes) {
  // Create the schema and document store
  SchemaProto schema;
  SchemaTypeConfigProto* email_type = AddSchemaType(&schema, "email");
  // SectionIds are assigned in ascending order per schema type,
  // alphabetically.
  AddIndexedProperty(email_type, "a");  // Section "a" would get sectionId 0
  AddIndexedProperty(email_type, "foo");
  int email_foo_section_id = 1;

  SchemaTypeConfigProto* message_type = AddSchemaType(&schema, "message");
  // SectionIds are assigned in ascending order per schema type,
  // alphabetically.
  AddIndexedProperty(message_type, "foo");
  int message_foo_section_id = 0;

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

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

  // Populate the index
  SectionIdMask email_section_id_mask = 1U << email_foo_section_id;
  SectionIdMask message_section_id_mask = 1U << message_foo_section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, email_foo_section_id,
                              term_match_type, "animal"),
              IsOk());

  // Message document has content "animal"
  ASSERT_THAT(AddTokenToIndex(message_document_id, message_foo_section_id,
                              term_match_type, "animal"),
              IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>'
  search_spec.set_query("foo:animal");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Ordered by descending DocumentId, so message comes first since it was
  // inserted last
  EXPECT_THAT(
      GetDocHitInfos(results.root_iterator.get()),
      ElementsAre(DocHitInfo(message_document_id, message_section_id_mask),
                  DocHitInfo(email_document_id, email_section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms["foo"], UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, SectionFilterWithinSchemaType) {
  // Create the schema and document store
  SchemaProto schema;
  SchemaTypeConfigProto* email_type = AddSchemaType(&schema, "email");
  // SectionIds are assigned in ascending order per schema type,
  // alphabetically.
  AddIndexedProperty(email_type, "foo");
  int email_foo_section_id = 0;

  SchemaTypeConfigProto* message_type = AddSchemaType(&schema, "message");
  // SectionIds are assigned in ascending order per schema type,
  // alphabetically.
  AddIndexedProperty(message_type, "foo");
  int message_foo_section_id = 0;

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

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

  // Populate the index
  SectionIdMask email_section_id_mask = 1U << email_foo_section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, email_foo_section_id,
                              term_match_type, "animal"),
              IsOk());

  // Message document has content "animal"
  ASSERT_THAT(AddTokenToIndex(message_document_id, message_foo_section_id,
                              term_match_type, "animal"),
              IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>', but only look
  // within documents of email schema
  search_spec.set_query("foo:animal");
  search_spec.add_schema_type_filters("email");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Shouldn't include the message document since we're only looking at email
  // types
  EXPECT_THAT(
      GetDocHitInfos(results.root_iterator.get()),
      ElementsAre(DocHitInfo(email_document_id, email_section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms["foo"], UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, SectionFilterRespectsDifferentSectionIds) {
  // Create the schema and document store
  SchemaProto schema;
  SchemaTypeConfigProto* email_type = AddSchemaType(&schema, "email");
  // SectionIds are assigned in ascending order per schema type,
  // alphabetically.
  AddIndexedProperty(email_type, "foo");
  int email_foo_section_id = 0;

  SchemaTypeConfigProto* message_type = AddSchemaType(&schema, "message");
  // SectionIds are assigned in ascending order per schema type,
  // alphabetically.
  AddIndexedProperty(message_type, "bar");
  int message_foo_section_id = 0;

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

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

  // Populate the index
  SectionIdMask email_section_id_mask = 1U << email_foo_section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, email_foo_section_id,
                              term_match_type, "animal"),
              IsOk());

  // Message document has content "animal", but put in in the same section id
  // as the indexed email section id, the same id as indexed property "foo" in
  // the message type
  ASSERT_THAT(AddTokenToIndex(message_document_id, message_foo_section_id,
                              term_match_type, "animal"),
              IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>', but only look
  // within documents of email schema
  search_spec.set_query("foo:animal");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Even though the section id is the same, we should be able to tell that it
  // doesn't match to the name of the section filter
  EXPECT_THAT(
      GetDocHitInfos(results.root_iterator.get()),
      ElementsAre(DocHitInfo(email_document_id, email_section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms["foo"], UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, NonexistentSectionFilterReturnsEmptyResults) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, /*section_id=*/0,
                              term_match_type, "animal"),
              IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>', but only look
  // within documents of email schema
  search_spec.set_query("nonexistent:animal");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Even though the section id is the same, we should be able to tell that it
  // doesn't match to the name of the section filter
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()), IsEmpty());
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms["nonexistent"],
              UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, UnindexedSectionFilterReturnsEmptyResults) {
  // Create the schema and document store
  SchemaProto schema;
  SchemaTypeConfigProto* email_type = AddSchemaType(&schema, "email");
  AddUnindexedProperty(email_type, "foo");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  // These documents don't actually match to the tokens in the index. We're
  // inserting the documents to get the appropriate number of documents and
  // schema types populated.
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .Build()));

  // Populate the index
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, /*section_id=*/0,
                              term_match_type, "animal"),
              IsOk());

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>', but only look
  // within documents of email schema
  search_spec.set_query("foo:animal");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Even though the section id is the same, we should be able to tell that it
  // doesn't match to the name of the section filter
  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()), IsEmpty());
  EXPECT_THAT(results.query_terms, SizeIs(1));
  EXPECT_THAT(results.query_terms["foo"], UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, SectionFilterTermAndUnrestrictedTerm) {
  // Create the schema and document store
  SchemaProto schema;
  SchemaTypeConfigProto* email_type = AddSchemaType(&schema, "email");
  // SectionIds are assigned in ascending order per schema type,
  // alphabetically.
  AddIndexedProperty(email_type, "foo");
  int email_foo_section_id = 0;

  SchemaTypeConfigProto* message_type = AddSchemaType(&schema, "message");
  // SectionIds are assigned in ascending order per schema type,
  // alphabetically.
  AddIndexedProperty(message_type, "foo");
  int message_foo_section_id = 0;

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

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

  // Poplate the index
  SectionIdMask email_section_id_mask = 1U << email_foo_section_id;
  SectionIdMask message_section_id_mask = 1U << message_foo_section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  // Email document has content "animal"
  ASSERT_THAT(AddTokenToIndex(email_document_id, email_foo_section_id,
                              term_match_type, "animal"),
              IsOk());
  ASSERT_THAT(AddTokenToIndex(email_document_id, email_foo_section_id,
                              term_match_type, "cat"),
              IsOk());

  // Message document has content "animal"
  ASSERT_THAT(AddTokenToIndex(message_document_id, message_foo_section_id,
                              term_match_type, "animal"),
              IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  // Create a section filter '<section name>:<query term>'
  search_spec.set_query("cat OR foo:animal");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  // Ordered by descending DocumentId, so message comes first since it was
  // inserted last
  EXPECT_THAT(
      GetDocHitInfos(results.root_iterator.get()),
      ElementsAre(DocHitInfo(message_document_id, message_section_id_mask),
                  DocHitInfo(email_document_id, email_section_id_mask)));
  EXPECT_THAT(results.query_terms, SizeIs(2));
  EXPECT_THAT(results.query_terms[""], UnorderedElementsAre("cat"));
  EXPECT_THAT(results.query_terms["foo"], UnorderedElementsAre("animal"));
}

TEST_F(QueryProcessorTest, DocumentBeforeTtlNotFilteredOut) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .SetCreationTimestampMs(0)
                                                      .SetTtlMs(100)
                                                      .Build()));

  // Populate the index
  int section_id = 0;
  SectionIdMask section_id_mask = 1U << section_id;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());

  // Arbitrary value, just has to be less than the document's creation
  // timestamp + ttl
  FakeClock fake_clock;
  fake_clock.SetSystemTimeMilliseconds(50);

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock_));

  SearchSpecProto search_spec;
  search_spec.set_query("hello");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()),
              ElementsAre(DocHitInfo(document_id, section_id_mask)));
}

TEST_F(QueryProcessorTest, DocumentPastTtlFilteredOut) {
  // Create the schema and document store
  SchemaProto schema;
  AddSchemaType(&schema, "email");

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store_,
      SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));
  ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::CreateResult create_result,
      DocumentStore::Create(&filesystem_, store_dir_, &fake_clock_,
                            schema_store_.get()));
  document_store_ = std::move(create_result.document_store);

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             document_store_->Put(DocumentBuilder()
                                                      .SetKey("namespace", "1")
                                                      .SetSchema("email")
                                                      .SetCreationTimestampMs(0)
                                                      .SetTtlMs(100)
                                                      .Build()));

  // Populate the index
  int section_id = 0;
  TermMatchType::Code term_match_type = TermMatchType::EXACT_ONLY;

  EXPECT_THAT(
      AddTokenToIndex(document_id, section_id, term_match_type, "hello"),
      IsOk());

  // Arbitrary value, just has to be greater than the document's creation
  // timestamp + ttl
  FakeClock fake_clock;
  fake_clock.SetSystemTimeMilliseconds(200);

  // Perform query
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QueryProcessor> query_processor,
      QueryProcessor::Create(index_.get(), language_segmenter_.get(),
                             normalizer_.get(), document_store_.get(),
                             schema_store_.get(), &fake_clock));

  SearchSpecProto search_spec;
  search_spec.set_query("hello");
  search_spec.set_term_match_type(term_match_type);

  ICING_ASSERT_OK_AND_ASSIGN(QueryProcessor::QueryResults results,
                             query_processor->ParseSearch(search_spec));

  EXPECT_THAT(GetDocHitInfos(results.root_iterator.get()), IsEmpty());
}

}  // namespace

}  // namespace lib
}  // namespace icing
