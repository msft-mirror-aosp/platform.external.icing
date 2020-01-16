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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/mock-filesystem.h"
#include "icing/portable/equals-proto.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/query/query-terms.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/snippet-helpers.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {
using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::Return;
using ::testing::SizeIs;

class ResultRetrieverTest : public testing::Test {
 protected:
  ResultRetrieverTest() : test_dir_(GetTestTempDir() + "/icing") {
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());
    test_document1_ = DocumentBuilder()
        .SetKey("icing", "email/1")
        .SetSchema("email")
        .AddStringProperty("subject", "subject foo")
        .AddStringProperty("body", "body bar")
        .SetCreationTimestampMs(1574365086666)
                          .Build();
    test_document2_ = DocumentBuilder()
        .SetKey("icing", "email/2")
        .SetSchema("email")
        .AddStringProperty("subject", "subject foo 2")
        .AddStringProperty("body", "body bar 2")
        .SetCreationTimestampMs(1574365087777)
                          .Build();
    test_document3_ = DocumentBuilder()
        .SetKey("icing", "email/3")
        .SetSchema("email")
        .AddStringProperty("subject", "subject foo 3")
        .AddStringProperty("body", "body bar 3")
        .SetCreationTimestampMs(1574365088888)
                          .Build();
  }

  void SetUp() override {
    ICING_ASSERT_OK(
        // File generated via icu_data_file rule in //icing/BUILD.
        SetUpICUDataFile("icing/icu.dat"));
    ICING_ASSERT_OK_AND_ASSIGN(language_segmenter_,
                               LanguageSegmenter::Create());

    ICING_ASSERT_OK_AND_ASSIGN(schema_store_,
                               SchemaStore::Create(&filesystem_, test_dir_));

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

    result_spec_no_snippet_ = ResultSpecProto::default_instance();

    result_spec_snippet_.mutable_snippet_spec()->set_num_to_snippet(
        std::numeric_limits<int>::max());
    result_spec_snippet_.mutable_snippet_spec()->set_num_matches_per_property(
        std::numeric_limits<int>::max());
    result_spec_snippet_.mutable_snippet_spec()->set_max_window_bytes(1024);
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  const Filesystem filesystem_;
  ResultSpecProto result_spec_no_snippet_;
  ResultSpecProto result_spec_snippet_;
  const std::string test_dir_;
  DocumentProto test_document1_;
  DocumentProto test_document2_;
  DocumentProto test_document3_;
  std::unique_ptr<LanguageSegmenter> language_segmenter_;
  std::unique_ptr<SchemaStore> schema_store_;
  FakeClock fake_clock_;
};

TEST_F(ResultRetrieverTest, CreationWithNullPointerShouldFail) {
  EXPECT_THAT(
      ResultRetriever::Create(/*doc_store=*/nullptr, schema_store_.get(),
                              language_segmenter_.get()),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));

  EXPECT_THAT(ResultRetriever::Create(doc_store.get(), /*schema_store=*/nullptr,
                                      language_segmenter_.get()),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                                      /*language_segmenter=*/nullptr),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(ResultRetrieverTest, Simple) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(test_document1_));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(test_document2_));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             doc_store->Put(test_document3_));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id3, /*hit_section_id_mask=*/0b00000011, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetriever> result_retriever,
      ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                              language_segmenter_.get()));

  SearchResultProto::ResultProto result1;
  *result1.mutable_document() = test_document1_;
  SearchResultProto::ResultProto result2;
  *result2.mutable_document() = test_document2_;
  SearchResultProto::ResultProto result3;
  *result3.mutable_document() = test_document3_;

  SectionRestrictQueryTermsMap query_terms{};
  EXPECT_THAT(
      result_retriever->RetrieveResults(result_spec_no_snippet_, query_terms,
                                        TermMatchType::EXACT_ONLY,
                                        scored_document_hits),
      IsOkAndHolds(ElementsAre(EqualsProto(result1), EqualsProto(result2),
                               EqualsProto(result3))));
}

TEST_F(ResultRetrieverTest, OnlyOneResultRequested) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(test_document1_));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(test_document2_));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             doc_store->Put(test_document3_));

  result_spec_no_snippet_.set_num_to_retrieve(1);

  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id3, /*hit_section_id_mask=*/0b00000011, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetriever> result_retriever,
      ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                              language_segmenter_.get()));

  SearchResultProto::ResultProto result1;
  *result1.mutable_document() = test_document1_;

  SectionRestrictQueryTermsMap query_terms{};
  EXPECT_THAT(result_retriever->RetrieveResults(
                  result_spec_no_snippet_, query_terms,
                  TermMatchType::EXACT_ONLY, scored_document_hits),
              IsOkAndHolds(ElementsAre(EqualsProto(result1))));
}

TEST_F(ResultRetrieverTest, IgnoreErrors) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(test_document1_));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(test_document2_));

  DocumentId invalid_document_id = -1;
  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {invalid_document_id, /*hit_section_id_mask=*/0b00000011, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetriever> result_retriever,
      ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                              language_segmenter_.get(),
                              /*ignore_bad_document_ids=*/true));

  SearchResultProto::ResultProto result1;
  *result1.mutable_document() = test_document1_;
  SearchResultProto::ResultProto result2;
  *result2.mutable_document() = test_document2_;

  SectionRestrictQueryTermsMap query_terms{};
  EXPECT_THAT(
      result_retriever->RetrieveResults(result_spec_no_snippet_, query_terms,
                                        TermMatchType::EXACT_ONLY,
                                        scored_document_hits),
      IsOkAndHolds(ElementsAre(EqualsProto(result1), EqualsProto(result2))));
}

TEST_F(ResultRetrieverTest, NotIgnoreErrors) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(test_document1_));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(test_document2_));

  DocumentId invalid_document_id = -1;
  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {invalid_document_id, /*hit_section_id_mask=*/0b00000011, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetriever> result_retriever,
      ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                              language_segmenter_.get(),
                              /*ignore_bad_document_ids=*/false));

  SectionRestrictQueryTermsMap query_terms{};
  EXPECT_THAT(result_retriever->RetrieveResults(
                  result_spec_no_snippet_, query_terms,
                  TermMatchType::EXACT_ONLY, scored_document_hits),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  DocumentId non_existing_document_id = 4;
  scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {non_existing_document_id, /*hit_section_id_mask=*/0b00000011,
       /*score=*/0}};
  EXPECT_THAT(result_retriever->RetrieveResults(
                  result_spec_no_snippet_, query_terms,
                  TermMatchType::EXACT_ONLY, scored_document_hits),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(ResultRetrieverTest, IOError) {
  MockFilesystem mock_filesystem;
  ON_CALL(mock_filesystem, OpenForRead(_)).WillByDefault(Return(false));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&mock_filesystem, test_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(test_document1_));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(test_document2_));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0}};

  SectionRestrictQueryTermsMap query_terms{};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetriever> result_retriever,
      ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                              language_segmenter_.get(),
                              /*ignore_bad_document_ids=*/true));
  EXPECT_THAT(result_retriever->RetrieveResults(
                  result_spec_no_snippet_, query_terms,
                  TermMatchType::EXACT_ONLY, scored_document_hits),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL));
}

TEST_F(ResultRetrieverTest, SnippetingDisabled) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(test_document1_));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(test_document2_));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             doc_store->Put(test_document3_));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id3, /*hit_section_id_mask=*/0b00000011, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetriever> result_retriever,
      ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                              language_segmenter_.get()));

  SectionRestrictQueryTermsMap query_terms{};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<SearchResultProto::ResultProto> results,
      result_retriever->RetrieveResults(result_spec_no_snippet_, query_terms,
                                        TermMatchType::EXACT_ONLY,
                                        scored_document_hits));
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
                             doc_store->Put(test_document1_));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(test_document2_));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             doc_store->Put(test_document3_));

  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id3, /*hit_section_id_mask=*/0b00000011, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetriever> result_retriever,
      ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                              language_segmenter_.get()));

  SectionRestrictQueryTermsMap query_terms{{"", {"foo", "bar"}}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<SearchResultProto::ResultProto> result,
      result_retriever->RetrieveResults(result_spec_snippet_, query_terms,
                                        TermMatchType::EXACT_ONLY,
                                        scored_document_hits));
  EXPECT_THAT(result, SizeIs(3));
  EXPECT_THAT(result[0].document(), EqualsProto(test_document1_));
  EXPECT_THAT(
      GetWindow(result[0].document(), result[0].snippet(), "subject", 0),
      Eq("subject foo"));
  EXPECT_THAT(GetMatch(result[0].document(), result[0].snippet(), "subject", 0),
              Eq("foo"));
  EXPECT_THAT(GetWindow(result[0].document(), result[0].snippet(), "body", 0),
              Eq("body bar"));
  EXPECT_THAT(GetMatch(result[0].document(), result[0].snippet(), "body", 0),
              Eq("bar"));

  EXPECT_THAT(result[1].document(), EqualsProto(test_document2_));
  EXPECT_THAT(
      GetWindow(result[1].document(), result[1].snippet(), "subject", 0),
      Eq("subject foo 2"));
  EXPECT_THAT(GetMatch(result[1].document(), result[1].snippet(), "subject", 0),
              Eq("foo"));
  EXPECT_THAT(GetWindow(result[1].document(), result[1].snippet(), "body", 0),
              Eq("body bar 2"));
  EXPECT_THAT(GetMatch(result[1].document(), result[1].snippet(), "body", 0),
              Eq("bar"));

  EXPECT_THAT(result[2].document(), EqualsProto(test_document3_));
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
                             doc_store->Put(test_document1_));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(test_document2_));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             doc_store->Put(test_document3_));

  result_spec_snippet_.mutable_snippet_spec()->set_num_to_snippet(1);

  std::vector<ScoredDocumentHit> scored_document_hits = {
      {document_id1, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id2, /*hit_section_id_mask=*/0b00000011, /*score=*/0},
      {document_id3, /*hit_section_id_mask=*/0b00000011, /*score=*/0}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ResultRetriever> result_retriever,
      ResultRetriever::Create(doc_store.get(), schema_store_.get(),
                              language_segmenter_.get()));

  SectionRestrictQueryTermsMap query_terms{{"", {"foo", "bar"}}};
  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<SearchResultProto::ResultProto> result,
      result_retriever->RetrieveResults(result_spec_snippet_, query_terms,
                                        TermMatchType::EXACT_ONLY,
                                        scored_document_hits));
  EXPECT_THAT(result, SizeIs(3));
  EXPECT_THAT(result[0].document(), EqualsProto(test_document1_));
  EXPECT_THAT(
      GetWindow(result[0].document(), result[0].snippet(), "subject", 0),
      Eq("subject foo"));
  EXPECT_THAT(GetMatch(result[0].document(), result[0].snippet(), "subject", 0),
              Eq("foo"));
  EXPECT_THAT(GetWindow(result[0].document(), result[0].snippet(), "body", 0),
              Eq("body bar"));
  EXPECT_THAT(GetMatch(result[0].document(), result[0].snippet(), "body", 0),
              Eq("bar"));

  EXPECT_THAT(result[1].document(), EqualsProto(test_document2_));
  EXPECT_THAT(result[1].snippet(),
              EqualsProto(SnippetProto::default_instance()));

  EXPECT_THAT(result[2].document(), EqualsProto(test_document3_));
  EXPECT_THAT(result[2].snippet(),
              EqualsProto(SnippetProto::default_instance()));
}

}  // namespace

}  // namespace lib
}  // namespace icing
