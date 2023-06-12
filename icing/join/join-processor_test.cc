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

#include "icing/join/join-processor.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/join/qualified-id-join-indexing-handler.h"
#include "icing/join/qualified-id-type-joinable-index.h"
#include "icing/portable/platform.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/document_wrapper.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/icu-data-file-helper.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/util/status-macros.h"
#include "icing/util/tokenized-document.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::IsTrue;

class JoinProcessorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = GetTestTempDir() + "/icing_join_processor_test";
    ASSERT_THAT(filesystem_.CreateDirectoryRecursively(test_dir_.c_str()),
                IsTrue());

    schema_store_dir_ = test_dir_ + "/schema_store";
    doc_store_dir_ = test_dir_ + "/doc_store";
    qualified_id_join_index_dir_ = test_dir_ + "/qualified_id_join_index";

    if (!IsCfStringTokenization() && !IsReverseJniTokenization()) {
      ICING_ASSERT_OK(
          // File generated via icu_data_file rule in //icing/BUILD.
          icu_data_file_helper::SetUpICUDataFile(
              GetTestFilePath("icing/icu.dat")));
    }

    language_segmenter_factory::SegmenterOptions options(ULOC_US);
    ICING_ASSERT_OK_AND_ASSIGN(
        lang_segmenter_,
        language_segmenter_factory::Create(std::move(options)));

    ASSERT_THAT(
        filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str()),
        IsTrue());
    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_));

    SchemaProto schema =
        SchemaBuilder()
            .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
                PropertyConfigBuilder()
                    .SetName("Name")
                    .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                    .SetCardinality(CARDINALITY_OPTIONAL)))
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("Email")
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("subject")
                                     .SetDataTypeString(TERM_MATCH_EXACT,
                                                        TOKENIZER_PLAIN)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("sender")
                                     .SetDataTypeJoinableString(
                                         JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                     .SetCardinality(CARDINALITY_OPTIONAL)))
            .Build();
    ASSERT_THAT(schema_store_->SetSchema(
                    schema, /*ignore_errors_and_delete_documents=*/false,
                    /*allow_circular_schema_definitions=*/false),
                IsOk());

    ASSERT_THAT(filesystem_.CreateDirectoryRecursively(doc_store_dir_.c_str()),
                IsTrue());
    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(&filesystem_, doc_store_dir_, &fake_clock_,
                              schema_store_.get(),
                              /*force_recovery_and_revalidate_documents=*/false,
                              /*namespace_id_fingerprint=*/false,
                              PortableFileBackedProtoLog<
                                  DocumentWrapper>::kDeflateCompressionLevel,
                              /*initialize_stats=*/nullptr));
    doc_store_ = std::move(create_result.document_store);

    ICING_ASSERT_OK_AND_ASSIGN(
        qualified_id_join_index_,
        QualifiedIdTypeJoinableIndex::Create(
            filesystem_, qualified_id_join_index_dir_,
            /*pre_mapping_fbv=*/false, /*use_persistent_hash_map=*/false));
  }

  void TearDown() override {
    qualified_id_join_index_.reset();
    doc_store_.reset();
    schema_store_.reset();
    lang_segmenter_.reset();
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  libtextclassifier3::StatusOr<DocumentId> PutAndIndexDocument(
      const DocumentProto& document) {
    ICING_ASSIGN_OR_RETURN(DocumentId document_id, doc_store_->Put(document));
    ICING_ASSIGN_OR_RETURN(
        TokenizedDocument tokenized_document,
        TokenizedDocument::Create(schema_store_.get(), lang_segmenter_.get(),
                                  document));

    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<QualifiedIdJoinIndexingHandler> handler,
        QualifiedIdJoinIndexingHandler::Create(&fake_clock_,
                                               qualified_id_join_index_.get()));
    ICING_RETURN_IF_ERROR(handler->Handle(tokenized_document, document_id,
                                          /*recovery_mode=*/false,
                                          /*put_document_stats=*/nullptr));
    return document_id;
  }

  libtextclassifier3::StatusOr<std::vector<JoinedScoredDocumentHit>> Join(
      const JoinSpecProto& join_spec,
      std::vector<ScoredDocumentHit>&& parent_scored_document_hits,
      std::vector<ScoredDocumentHit>&& child_scored_document_hits) {
    JoinProcessor join_processor(
        doc_store_.get(), schema_store_.get(), qualified_id_join_index_.get(),
        /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds());
    ICING_ASSIGN_OR_RETURN(
        JoinChildrenFetcher join_children_fetcher,
        join_processor.GetChildrenFetcher(
            join_spec, std::move(child_scored_document_hits)));
    return join_processor.Join(join_spec,
                               std::move(parent_scored_document_hits),
                               join_children_fetcher);
  }

  Filesystem filesystem_;
  std::string test_dir_;
  std::string schema_store_dir_;
  std::string doc_store_dir_;
  std::string qualified_id_join_index_dir_;

  std::unique_ptr<LanguageSegmenter> lang_segmenter_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> doc_store_;
  std::unique_ptr<QualifiedIdTypeJoinableIndex> qualified_id_join_index_;

  FakeClock fake_clock_;
};

TEST_F(JoinProcessorTest, JoinByQualifiedId) {
  DocumentProto person1 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person1")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey(R"(pkg$db/name#space\\)", "person2")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Bob")
                              .Build();

  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 1")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();
  DocumentProto email2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email2")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 2")
          .AddStringProperty("sender",
                             R"(pkg$db/name\#space\\\\#person2)")  // escaped
          .Build();
  DocumentProto email3 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email3")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 3")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             PutAndIndexDocument(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             PutAndIndexDocument(person2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             PutAndIndexDocument(email1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id4,
                             PutAndIndexDocument(email2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id5,
                             PutAndIndexDocument(email3));

  ScoredDocumentHit scored_doc_hit1(document_id1, kSectionIdMaskNone,
                                    /*score=*/0.0);
  ScoredDocumentHit scored_doc_hit2(document_id2, kSectionIdMaskNone,
                                    /*score=*/0.0);
  ScoredDocumentHit scored_doc_hit3(document_id3, kSectionIdMaskNone,
                                    /*score=*/3.0);
  ScoredDocumentHit scored_doc_hit4(document_id4, kSectionIdMaskNone,
                                    /*score=*/4.0);
  ScoredDocumentHit scored_doc_hit5(document_id5, kSectionIdMaskNone,
                                    /*score=*/5.0);

  // Parent ScoredDocumentHits: all Person documents
  std::vector<ScoredDocumentHit> parent_scored_document_hits = {
      scored_doc_hit2, scored_doc_hit1};

  // Child ScoredDocumentHits: all Email documents
  std::vector<ScoredDocumentHit> child_scored_document_hits = {
      scored_doc_hit5, scored_doc_hit4, scored_doc_hit3};

  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec.set_child_property_expression("sender");
  join_spec.set_aggregation_scoring_strategy(
      JoinSpecProto::AggregationScoringStrategy::COUNT);
  join_spec.mutable_nested_spec()->mutable_scoring_spec()->set_order_by(
      ScoringSpecProto::Order::DESC);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<JoinedScoredDocumentHit> joined_result_document_hits,
      Join(join_spec, std::move(parent_scored_document_hits),
           std::move(child_scored_document_hits)));
  EXPECT_THAT(
      joined_result_document_hits,
      ElementsAre(EqualsJoinedScoredDocumentHit(JoinedScoredDocumentHit(
                      /*final_score=*/1.0,
                      /*parent_scored_document_hit=*/scored_doc_hit2,
                      /*child_scored_document_hits=*/{scored_doc_hit4})),
                  EqualsJoinedScoredDocumentHit(JoinedScoredDocumentHit(
                      /*final_score=*/2.0,
                      /*parent_scored_document_hit=*/scored_doc_hit1,
                      /*child_scored_document_hits=*/
                      {scored_doc_hit5, scored_doc_hit3}))));
}

TEST_F(JoinProcessorTest, ShouldIgnoreChildDocumentsWithoutJoiningProperty) {
  DocumentProto person1 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person1")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Alice")
                              .Build();

  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 1")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();
  DocumentProto email2 = DocumentBuilder()
                             .SetKey("pkg$db/namespace", "email2")
                             .SetSchema("Email")
                             .AddStringProperty("subject", "test subject 2")
                             .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             PutAndIndexDocument(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             PutAndIndexDocument(email1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             PutAndIndexDocument(email2));

  ScoredDocumentHit scored_doc_hit1(document_id1, kSectionIdMaskNone,
                                    /*score=*/0.0);
  ScoredDocumentHit scored_doc_hit2(document_id2, kSectionIdMaskNone,
                                    /*score=*/5.0);
  ScoredDocumentHit scored_doc_hit3(document_id3, kSectionIdMaskNone,
                                    /*score=*/6.0);

  // Parent ScoredDocumentHits: all Person documents
  std::vector<ScoredDocumentHit> parent_scored_document_hits = {
      scored_doc_hit1};

  // Child ScoredDocumentHits: all Email documents
  std::vector<ScoredDocumentHit> child_scored_document_hits = {scored_doc_hit2,
                                                               scored_doc_hit3};

  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec.set_child_property_expression("sender");
  join_spec.set_aggregation_scoring_strategy(
      JoinSpecProto::AggregationScoringStrategy::COUNT);
  join_spec.mutable_nested_spec()->mutable_scoring_spec()->set_order_by(
      ScoringSpecProto::Order::DESC);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<JoinedScoredDocumentHit> joined_result_document_hits,
      Join(join_spec, std::move(parent_scored_document_hits),
           std::move(child_scored_document_hits)));
  // Since Email2 doesn't have "sender" property, it should be ignored.
  EXPECT_THAT(
      joined_result_document_hits,
      ElementsAre(EqualsJoinedScoredDocumentHit(JoinedScoredDocumentHit(
          /*final_score=*/1.0, /*parent_scored_document_hit=*/scored_doc_hit1,
          /*child_scored_document_hits=*/{scored_doc_hit2}))));
}

TEST_F(JoinProcessorTest, ShouldIgnoreChildDocumentsWithInvalidQualifiedId) {
  DocumentProto person1 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person1")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Alice")
                              .Build();

  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 1")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();
  DocumentProto email2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email2")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 2")
          .AddStringProperty(
              "sender",
              "pkg$db/namespace#person2")  // qualified id is invalid since
                                           // person2 doesn't exist.
          .Build();
  DocumentProto email3 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email3")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 3")
          .AddStringProperty("sender",
                             R"(pkg$db/namespace\#person1)")  // invalid format
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             PutAndIndexDocument(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             PutAndIndexDocument(email1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             PutAndIndexDocument(email2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id4,
                             PutAndIndexDocument(email3));

  ScoredDocumentHit scored_doc_hit1(document_id1, kSectionIdMaskNone,
                                    /*score=*/0.0);
  ScoredDocumentHit scored_doc_hit2(document_id2, kSectionIdMaskNone,
                                    /*score=*/0.0);
  ScoredDocumentHit scored_doc_hit3(document_id3, kSectionIdMaskNone,
                                    /*score=*/0.0);
  ScoredDocumentHit scored_doc_hit4(document_id4, kSectionIdMaskNone,
                                    /*score=*/0.0);

  // Parent ScoredDocumentHits: all Person documents
  std::vector<ScoredDocumentHit> parent_scored_document_hits = {
      scored_doc_hit1};

  // Child ScoredDocumentHits: all Email documents
  std::vector<ScoredDocumentHit> child_scored_document_hits = {
      scored_doc_hit2, scored_doc_hit3, scored_doc_hit4};

  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec.set_child_property_expression("sender");
  join_spec.set_aggregation_scoring_strategy(
      JoinSpecProto::AggregationScoringStrategy::COUNT);
  join_spec.mutable_nested_spec()->mutable_scoring_spec()->set_order_by(
      ScoringSpecProto::Order::DESC);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<JoinedScoredDocumentHit> joined_result_document_hits,
      Join(join_spec, std::move(parent_scored_document_hits),
           std::move(child_scored_document_hits)));
  // Email 2 and email 3 (document id 3 and 4) contain invalid qualified ids.
  // Join processor should ignore them.
  EXPECT_THAT(joined_result_document_hits,
              ElementsAre(EqualsJoinedScoredDocumentHit(JoinedScoredDocumentHit(
                  /*final_score=*/1.0,
                  /*parent_scored_document_hit=*/scored_doc_hit1,
                  /*child_scored_document_hits=*/{scored_doc_hit2}))));
}

TEST_F(JoinProcessorTest, LeftJoinShouldReturnParentWithoutChildren) {
  DocumentProto person1 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person1")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey(R"(pkg$db/name#space\\)", "person2")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Bob")
                              .Build();

  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 1")
          .AddStringProperty("sender",
                             R"(pkg$db/name\#space\\\\#person2)")  // escaped
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             PutAndIndexDocument(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             PutAndIndexDocument(person2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             PutAndIndexDocument(email1));

  ScoredDocumentHit scored_doc_hit1(document_id1, kSectionIdMaskNone,
                                    /*score=*/0.0);
  ScoredDocumentHit scored_doc_hit2(document_id2, kSectionIdMaskNone,
                                    /*score=*/0.0);
  ScoredDocumentHit scored_doc_hit3(document_id3, kSectionIdMaskNone,
                                    /*score=*/3.0);

  // Parent ScoredDocumentHits: all Person documents
  std::vector<ScoredDocumentHit> parent_scored_document_hits = {
      scored_doc_hit2, scored_doc_hit1};

  // Child ScoredDocumentHits: all Email documents
  std::vector<ScoredDocumentHit> child_scored_document_hits = {scored_doc_hit3};

  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec.set_child_property_expression("sender");
  join_spec.set_aggregation_scoring_strategy(
      JoinSpecProto::AggregationScoringStrategy::COUNT);
  join_spec.mutable_nested_spec()->mutable_scoring_spec()->set_order_by(
      ScoringSpecProto::Order::DESC);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<JoinedScoredDocumentHit> joined_result_document_hits,
      Join(join_spec, std::move(parent_scored_document_hits),
           std::move(child_scored_document_hits)));
  // Person1 has no child documents, but left join should also include it.
  EXPECT_THAT(
      joined_result_document_hits,
      ElementsAre(EqualsJoinedScoredDocumentHit(JoinedScoredDocumentHit(
                      /*final_score=*/1.0,
                      /*parent_scored_document_hit=*/scored_doc_hit2,
                      /*child_scored_document_hits=*/{scored_doc_hit3})),
                  EqualsJoinedScoredDocumentHit(JoinedScoredDocumentHit(
                      /*final_score=*/0.0,
                      /*parent_scored_document_hit=*/scored_doc_hit1,
                      /*child_scored_document_hits=*/{}))));
}

TEST_F(JoinProcessorTest, ShouldSortChildDocumentsByRankingStrategy) {
  DocumentProto person1 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person1")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Alice")
                              .Build();

  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 1")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();
  DocumentProto email2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email2")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 2")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();
  DocumentProto email3 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email3")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 3")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             PutAndIndexDocument(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             PutAndIndexDocument(email1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3,
                             PutAndIndexDocument(email2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id4,
                             PutAndIndexDocument(email3));

  ScoredDocumentHit scored_doc_hit1(document_id1, kSectionIdMaskNone,
                                    /*score=*/0.0);
  ScoredDocumentHit scored_doc_hit2(document_id2, kSectionIdMaskNone,
                                    /*score=*/2.0);
  ScoredDocumentHit scored_doc_hit3(document_id3, kSectionIdMaskNone,
                                    /*score=*/5.0);
  ScoredDocumentHit scored_doc_hit4(document_id4, kSectionIdMaskNone,
                                    /*score=*/3.0);

  // Parent ScoredDocumentHits: all Person documents
  std::vector<ScoredDocumentHit> parent_scored_document_hits = {
      scored_doc_hit1};

  // Child ScoredDocumentHits: all Email documents
  std::vector<ScoredDocumentHit> child_scored_document_hits = {
      scored_doc_hit2, scored_doc_hit3, scored_doc_hit4};

  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec.set_child_property_expression("sender");
  join_spec.set_aggregation_scoring_strategy(
      JoinSpecProto::AggregationScoringStrategy::COUNT);
  join_spec.mutable_nested_spec()->mutable_scoring_spec()->set_order_by(
      ScoringSpecProto::Order::DESC);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<JoinedScoredDocumentHit> joined_result_document_hits,
      Join(join_spec, std::move(parent_scored_document_hits),
           std::move(child_scored_document_hits)));
  // Child documents should be sorted according to the (nested) ranking
  // strategy.
  EXPECT_THAT(
      joined_result_document_hits,
      ElementsAre(EqualsJoinedScoredDocumentHit(JoinedScoredDocumentHit(
          /*final_score=*/3.0, /*parent_scored_document_hit=*/scored_doc_hit1,
          /*child_scored_document_hits=*/
          {scored_doc_hit3, scored_doc_hit4, scored_doc_hit2}))));
}

TEST_F(JoinProcessorTest, ShouldAllowSelfJoining) {
  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 1")
          .AddStringProperty("sender", "pkg$db/namespace#email1")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             PutAndIndexDocument(email1));

  ScoredDocumentHit scored_doc_hit1(document_id1, kSectionIdMaskNone,
                                    /*score=*/0.0);

  // Parent ScoredDocumentHits: all Person documents
  std::vector<ScoredDocumentHit> parent_scored_document_hits = {
      scored_doc_hit1};

  // Child ScoredDocumentHits: all Email documents
  std::vector<ScoredDocumentHit> child_scored_document_hits = {scored_doc_hit1};

  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec.set_child_property_expression("sender");
  join_spec.set_aggregation_scoring_strategy(
      JoinSpecProto::AggregationScoringStrategy::COUNT);
  join_spec.mutable_nested_spec()->mutable_scoring_spec()->set_order_by(
      ScoringSpecProto::Order::DESC);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::vector<JoinedScoredDocumentHit> joined_result_document_hits,
      Join(join_spec, std::move(parent_scored_document_hits),
           std::move(child_scored_document_hits)));
  EXPECT_THAT(joined_result_document_hits,
              ElementsAre(EqualsJoinedScoredDocumentHit(JoinedScoredDocumentHit(
                  /*final_score=*/1.0,
                  /*parent_scored_document_hit=*/scored_doc_hit1,
                  /*child_scored_document_hits=*/{scored_doc_hit1}))));
}

// TODO(b/256022027): add unit tests for non-joinable property. If joinable
//                    value type is unset, then qualifed id join should not
//                    include the child document even if it contains a valid
//                    qualified id string.

}  // namespace

}  // namespace lib
}  // namespace icing
