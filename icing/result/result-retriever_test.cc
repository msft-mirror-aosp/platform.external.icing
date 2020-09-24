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

#include "icing/result/result-retriever.h"

#include <limits>
#include <memory>

#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/mock-filesystem.h"
#include "icing/helpers/icu/icu-data-file-helper.h"
#include "icing/portable/equals-proto.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/snippet-helpers.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/transform/normalizer-factory.h"
#include "icing/transform/normalizer.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

namespace {
using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::Return;
using ::testing::SizeIs;

class ResultRetrieverTest : public testing::Test {
 protected:
  ResultRetrieverTest() : test_dir_(GetTestTempDir() + "/icing") {
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());
  }

  void SetUp() override {
    ICING_ASSERT_OK(
        // File generated via icu_data_file rule in //icing/BUILD.
        icu_data_file_helper::SetUpICUDataFile(
            GetTestFilePath("icing/icu.dat")));
    language_segmenter_factory::SegmenterOptions options(ULOC_US);
    ICING_ASSERT_OK_AND_ASSIGN(
        language_segmenter_,
        language_segmenter_factory::Create(std::move(options)));

    ICING_ASSERT_OK_AND_ASSIGN(schema_store_,
                               SchemaStore::Create(&filesystem_, test_dir_));
    ICING_ASSERT_OK_AND_ASSIGN(normalizer_, normalizer_factory::Create(
                                                /*max_term_byte_size=*/10000));

    SchemaProto schema;
    auto type_config = schema.add_types();
    type_config->set_schema_type("email");
    PropertyConfigProto* prop_config = type_config->add_properties();
    prop_config->set_property_name("subject");
    prop_config->set_data_type(PropertyConfigProto::DataType::STRING);
    prop_config->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
    prop_config->mutable_indexing_config()->set_term_match_type(
        TermMatchType::PREFIX);
    prop_config->mutable_indexing_config()->set_tokenizer_type(
        IndexingConfig::TokenizerType::PLAIN);
    prop_config = type_config->add_properties();
    prop_config->set_property_name("body");
    prop_config->set_data_type(PropertyConfigProto::DataType::STRING);
    prop_config->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
    prop_config->mutable_indexing_config()->set_term_match_type(
        TermMatchType::EXACT_ONLY);
    prop_config->mutable_indexing_config()->set_tokenizer_type(
        IndexingConfig::TokenizerType::PLAIN);
    ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  const Filesystem filesystem_;
  const std::string test_dir_;
  std::unique_ptr<LanguageSegmenter> language_segmenter_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<Normalizer> normalizer_;
  FakeClock fake_clock_;
};

ResultSpecProto::SnippetSpecProto CreateSnippetSpec() {
  ResultSpecProto::SnippetSpecProto snippet_spec;
  snippet_spec.set_num_to_snippet(std::numeric_limits<int>::max());
  snippet_spec.set_num_matches_per_property(std::numeric_limits<int>::max());
  snippet_spec.set_max_window_bytes(1024);
  return snippet_spec;
}

DocumentProto CreateDocument(int id) {
  return DocumentBuilder()
      .SetKey("icing", "email/" + std::to_string(id))
      .SetSchema("email")
      .AddStringProperty("subject", "subject foo " + std::to_string(id))
      .AddStringProperty("body", "body bar " + std::to_string(id))
      .SetCreationTimestampMs(1574365086666 + id)
      .Build();
}

TEST_F(ResultRetrieverTest, CreationWithNullPointerShouldFail) {
  EXPECT_THAT(
      ResultRetriever::Create(/*doc_store=*/nullptr, schema_store_.get(),
                              language_segmenter_.get(), normalizer_.get()),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));

  EXPECT_THAT(
      ResultRetriever::Create(doc_store.get(), /*schema_store=*/nullptr,
                              language_segmenter_.get(), normalizer_.get()),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                                      /*language_segmenter=*/nullptr,
                                      normalizer_.get()),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                                      language_segmenter_.get(),
                                      /*normalizer=*/nullptr),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(ResultRetrieverTest, ShouldRetrieveSimpleResults) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(CreateDocument(/*id=*/2)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             doc_store->Put(CreateDocument(/*id=*/3)));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id3, /*hit_section_id_mask=*/0b00000011, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetriever> result_retriever,
      ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                              language_segmenter_.get(), normalizer_.get()));

  SearchResultProto::ResultProto result1;
  *result1.mutable_document() = CreateDocument(/*id=*/1);
  SearchResultProto::ResultProto result2;
  *result2.mutable_document() = CreateDocument(/*id=*/2);
  SearchResultProto::ResultProto result3;
  *result3.mutable_document() = CreateDocument(/*id=*/3);

  SnippetContext snippet_context(
      /*query_terms_in=*/{},
      ResultSpecProto::SnippetSpecProto::default_instance(),
      TermMatchType::EXACT_ONLY);
  PageResultState page_result_state(
      std::move(scored_document_hits), /*next_page_token_in=*/1,
      std::move(snippet_context), /*num_previously_returned_in=*/0);
  EXPECT_THAT(
      result_retriever->RetrieveResults(page_result_state),
      IsOkAndHolds(ElementsAre(EqualsProto(result1), EqualsProto(result2),
                               EqualsProto(result3))));
}

TEST_F(ResultRetrieverTest, IgnoreErrors) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(CreateDocument(/*id=*/2)));

  DocumentId invalid_document_id = -1;
  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {invalid_document_id, /*hit_section_id_mask=*/0b00000011, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetriever> result_retriever,
      ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                              language_segmenter_.get(), normalizer_.get(),
                              /*ignore_bad_document_ids=*/true));

  SearchResultProto::ResultProto result1;
  *result1.mutable_document() = CreateDocument(/*id=*/1);
  SearchResultProto::ResultProto result2;
  *result2.mutable_document() = CreateDocument(/*id=*/2);

  SnippetContext snippet_context(
      /*query_terms_in=*/{},
      ResultSpecProto::SnippetSpecProto::default_instance(),
      TermMatchType::EXACT_ONLY);
  PageResultState page_result_state(
      std::move(scored_document_hits), /*next_page_token_in=*/1,
      std::move(snippet_context), /*num_previously_returned_in=*/0);
  EXPECT_THAT(
      result_retriever->RetrieveResults(page_result_state),
      IsOkAndHolds(ElementsAre(EqualsProto(result1), EqualsProto(result2))));
}

TEST_F(ResultRetrieverTest, NotIgnoreErrors) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(CreateDocument(/*id=*/2)));

  DocumentId invalid_document_id = -1;
  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {invalid_document_id, /*hit_section_id_mask=*/0b00000011, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetriever> result_retriever,
      ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                              language_segmenter_.get(), normalizer_.get(),
                              /*ignore_bad_document_ids=*/false));

  SnippetContext snippet_context(
      /*query_terms_in=*/{},
      ResultSpecProto::SnippetSpecProto::default_instance(),
      TermMatchType::EXACT_ONLY);
  PageResultState page_result_state(
      std::move(scored_document_hits), /*next_page_token_in=*/1,
      std::move(snippet_context), /*num_previously_returned_in=*/0);
  EXPECT_THAT(result_retriever->RetrieveResults(page_result_state),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  DocumentId non_existing_document_id = 4;
  page_result_state.scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {non_existing_document_id, /*hit_section_id_mask=*/0b00000011,
       /*score=*/0}};
  EXPECT_THAT(result_retriever->RetrieveResults(page_result_state),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(ResultRetrieverTest, IOErrorShouldReturnInternalError) {
  MockFilesystem mock_filesystem;
  ON_CALL(mock_filesystem, OpenForRead(_)).WillByDefault(Return(false));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&mock_filesystem, test_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(CreateDocument(/*id=*/2)));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0}};

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetriever> result_retriever,
      ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                              language_segmenter_.get(), normalizer_.get(),
                              /*ignore_bad_document_ids=*/true));

  SnippetContext snippet_context(
      /*query_terms_in=*/{},
      ResultSpecProto::SnippetSpecProto::default_instance(),
      TermMatchType::EXACT_ONLY);
  PageResultState page_result_state(
      std::move(scored_document_hits), /*next_page_token_in=*/1,
      std::move(snippet_context), /*num_previously_returned_in=*/0);
  EXPECT_THAT(result_retriever->RetrieveResults(page_result_state),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL));
}

TEST_F(ResultRetrieverTest, DefaultSnippetSpecShouldDisableSnippeting) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(CreateDocument(/*id=*/2)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             doc_store->Put(CreateDocument(/*id=*/3)));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id3, /*hit_section_id_mask=*/0b00000011, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetriever> result_retriever,
      ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                              language_segmenter_.get(), normalizer_.get()));

  SnippetContext snippet_context(
      /*query_terms_in=*/{},
      ResultSpecProto::SnippetSpecProto::default_instance(),
      TermMatchType::EXACT_ONLY);
  PageResultState page_result_state(
      std::move(scored_document_hits), /*next_page_token_in=*/1,
      std::move(snippet_context), /*num_previously_returned_in=*/0);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<SearchResultProto::ResultProto> results,
      result_retriever->RetrieveResults(page_result_state));
  ASSERT_THAT(results, SizeIs(3));
  EXPECT_THAT(results.at(0).snippet(),
              EqualsProto(SnippetProto::default_instance()));
  EXPECT_THAT(results.at(1).snippet(),
              EqualsProto(SnippetProto::default_instance()));
  EXPECT_THAT(results.at(2).snippet(),
              EqualsProto(SnippetProto::default_instance()));
}

TEST_F(ResultRetrieverTest, SimpleSnippeted) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(CreateDocument(/*id=*/2)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             doc_store->Put(CreateDocument(/*id=*/3)));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id3, /*hit_section_id_mask=*/0b00000011, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetriever> result_retriever,
      ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                              language_segmenter_.get(), normalizer_.get()));

  SnippetContext snippet_context(
      /*query_terms_in=*/{{"", {"foo", "bar"}}}, CreateSnippetSpec(),
      TermMatchType::EXACT_ONLY);
  PageResultState page_result_state(
      std::move(scored_document_hits), /*next_page_token_in=*/1,
      std::move(snippet_context), /*num_previously_returned_in=*/0);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<SearchResultProto::ResultProto> result,
      result_retriever->RetrieveResults(page_result_state));
  EXPECT_THAT(result, SizeIs(3));
  EXPECT_THAT(result[0].document(), EqualsProto(CreateDocument(/*id=*/1)));
  EXPECT_THAT(
      GetWindow(result[0].document(), result[0].snippet(), "subject", 0),
      Eq("subject foo 1"));
  EXPECT_THAT(GetMatch(result[0].document(), result[0].snippet(), "subject", 0),
              Eq("foo"));
  EXPECT_THAT(GetWindow(result[0].document(), result[0].snippet(), "body", 0),
              Eq("body bar 1"));
  EXPECT_THAT(GetMatch(result[0].document(), result[0].snippet(), "body", 0),
              Eq("bar"));

  EXPECT_THAT(result[1].document(), EqualsProto(CreateDocument(/*id=*/2)));
  EXPECT_THAT(
      GetWindow(result[1].document(), result[1].snippet(), "subject", 0),
      Eq("subject foo 2"));
  EXPECT_THAT(GetMatch(result[1].document(), result[1].snippet(), "subject", 0),
              Eq("foo"));
  EXPECT_THAT(GetWindow(result[1].document(), result[1].snippet(), "body", 0),
              Eq("body bar 2"));
  EXPECT_THAT(GetMatch(result[1].document(), result[1].snippet(), "body", 0),
              Eq("bar"));

  EXPECT_THAT(result[2].document(), EqualsProto(CreateDocument(/*id=*/3)));
  EXPECT_THAT(
      GetWindow(result[2].document(), result[2].snippet(), "subject", 0),
      Eq("subject foo 3"));
  EXPECT_THAT(GetMatch(result[2].document(), result[2].snippet(), "subject", 0),
              Eq("foo"));
  EXPECT_THAT(GetWindow(result[2].document(), result[2].snippet(), "body", 0),
              Eq("body bar 3"));
  EXPECT_THAT(GetMatch(result[2].document(), result[2].snippet(), "body", 0),
              Eq("bar"));
}

TEST_F(ResultRetrieverTest, OnlyOneDocumentSnippeted) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(CreateDocument(/*id=*/2)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             doc_store->Put(CreateDocument(/*id=*/3)));

  ResultSpecProto::SnippetSpecProto snippet_spec = CreateSnippetSpec();
  snippet_spec.set_num_to_snippet(1);

  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id3, /*hit_section_id_mask=*/0b00000011, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetriever> result_retriever,
      ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                              language_segmenter_.get(), normalizer_.get()));

  SnippetContext snippet_context(/*query_terms_in=*/{{"", {"foo", "bar"}}},
                                 snippet_spec, TermMatchType::EXACT_ONLY);
  PageResultState page_result_state(
      std::move(scored_document_hits), /*next_page_token_in=*/1,
      std::move(snippet_context), /*num_previously_returned_in=*/0);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<SearchResultProto::ResultProto> result,
      result_retriever->RetrieveResults(page_result_state));
  EXPECT_THAT(result, SizeIs(3));
  EXPECT_THAT(result[0].document(), EqualsProto(CreateDocument(/*id=*/1)));
  EXPECT_THAT(
      GetWindow(result[0].document(), result[0].snippet(), "subject", 0),
      Eq("subject foo 1"));
  EXPECT_THAT(GetMatch(result[0].document(), result[0].snippet(), "subject", 0),
              Eq("foo"));
  EXPECT_THAT(GetWindow(result[0].document(), result[0].snippet(), "body", 0),
              Eq("body bar 1"));
  EXPECT_THAT(GetMatch(result[0].document(), result[0].snippet(), "body", 0),
              Eq("bar"));

  EXPECT_THAT(result[1].document(), EqualsProto(CreateDocument(/*id=*/2)));
  EXPECT_THAT(result[1].snippet(),
              EqualsProto(SnippetProto::default_instance()));

  EXPECT_THAT(result[2].document(), EqualsProto(CreateDocument(/*id=*/3)));
  EXPECT_THAT(result[2].snippet(),
              EqualsProto(SnippetProto::default_instance()));
}

TEST_F(ResultRetrieverTest, ShouldSnippetAllResults) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(CreateDocument(/*id=*/2)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             doc_store->Put(CreateDocument(/*id=*/3)));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id3, /*hit_section_id_mask=*/0b00000011, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetriever> result_retriever,
      ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                              language_segmenter_.get(), normalizer_.get()));

  ResultSpecProto::SnippetSpecProto snippet_spec = CreateSnippetSpec();
  snippet_spec.set_num_to_snippet(5);
  SnippetContext snippet_context(
      /*query_terms_in=*/{{"", {"foo", "bar"}}}, std::move(snippet_spec),
      TermMatchType::EXACT_ONLY);
  PageResultState page_result_state(
      std::move(scored_document_hits), /*next_page_token_in=*/1,
      std::move(snippet_context), /*num_previously_returned_in=*/0);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<SearchResultProto::ResultProto> result,
      result_retriever->RetrieveResults(page_result_state));
  // num_to_snippet = 5, num_previously_returned_in = 0,
  // We can return 5 - 0 = 5 snippets at most. We're able to return all 3
  // snippets here.
  ASSERT_THAT(result, SizeIs(3));
  EXPECT_THAT(result[0].snippet().entries(), Not(IsEmpty()));
  EXPECT_THAT(result[1].snippet().entries(), Not(IsEmpty()));
  EXPECT_THAT(result[2].snippet().entries(), Not(IsEmpty()));
}

TEST_F(ResultRetrieverTest, ShouldSnippetSomeResults) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(CreateDocument(/*id=*/2)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             doc_store->Put(CreateDocument(/*id=*/3)));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id3, /*hit_section_id_mask=*/0b00000011, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetriever> result_retriever,
      ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                              language_segmenter_.get(), normalizer_.get()));

  ResultSpecProto::SnippetSpecProto snippet_spec = CreateSnippetSpec();
  snippet_spec.set_num_to_snippet(5);
  SnippetContext snippet_context(
      /*query_terms_in=*/{{"", {"foo", "bar"}}}, std::move(snippet_spec),
      TermMatchType::EXACT_ONLY);
  PageResultState page_result_state(
      std::move(scored_document_hits), /*next_page_token_in=*/1,
      std::move(snippet_context), /*num_previously_returned_in=*/3);

  // num_to_snippet = 5, num_previously_returned_in = 3,
  // We can return 5 - 3 = 2 snippets.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<SearchResultProto::ResultProto> result,
      result_retriever->RetrieveResults(page_result_state));
  ASSERT_THAT(result, SizeIs(3));
  EXPECT_THAT(result[0].snippet().entries(), Not(IsEmpty()));
  EXPECT_THAT(result[1].snippet().entries(), Not(IsEmpty()));
  EXPECT_THAT(result[2].snippet().entries(), IsEmpty());
}

TEST_F(ResultRetrieverTest, ShouldNotSnippetAnyResults) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(CreateDocument(/*id=*/1)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(CreateDocument(/*id=*/2)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             doc_store->Put(CreateDocument(/*id=*/3)));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id3, /*hit_section_id_mask=*/0b00000011, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetriever> result_retriever,
      ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                              language_segmenter_.get(), normalizer_.get()));

  ResultSpecProto::SnippetSpecProto snippet_spec = CreateSnippetSpec();
  snippet_spec.set_num_to_snippet(5);
  SnippetContext snippet_context(
      /*query_terms_in=*/{{"", {"foo", "bar"}}}, std::move(snippet_spec),
      TermMatchType::EXACT_ONLY);
  PageResultState page_result_state(
      std::move(scored_document_hits), /*next_page_token_in=*/1,
      std::move(snippet_context), /*num_previously_returned_in=*/6);

  // num_to_snippet = 5, num_previously_returned_in = 6,
  // We can't return any snippets for this page.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<SearchResultProto::ResultProto> result,
      result_retriever->RetrieveResults(page_result_state));
  ASSERT_THAT(result, SizeIs(3));
  EXPECT_THAT(result[0].snippet().entries(), IsEmpty());
  EXPECT_THAT(result[1].snippet().entries(), IsEmpty());
  EXPECT_THAT(result[2].snippet().entries(), IsEmpty());
}

}  // namespace

}  // namespace lib
}  // namespace icing
