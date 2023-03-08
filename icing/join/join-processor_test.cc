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
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/scoring/scored-document-hit.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;

class JoinProcessorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = GetTestTempDir() + "/icing_join_processor_test";
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_,
        SchemaStore::Create(&filesystem_, test_dir_, &fake_clock_));

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
    ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(&filesystem_, test_dir_, &fake_clock_,
                              schema_store_.get()));
    doc_store_ = std::move(create_result.document_store);
  }

  void TearDown() override {
    doc_store_.reset();
    schema_store_.reset();
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  libtextclassifier3::StatusOr<std::vector<JoinedScoredDocumentHit>> Join(
      const JoinSpecProto& join_spec,
      std::vector<ScoredDocumentHit>&& parent_scored_document_hits,
      std::vector<ScoredDocumentHit>&& child_scored_document_hits) {
    JoinProcessor join_processor(doc_store_.get());
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
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> doc_store_;
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

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1, doc_store_->Put(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2, doc_store_->Put(person2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3, doc_store_->Put(email1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id4, doc_store_->Put(email2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id5, doc_store_->Put(email3));

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
  join_spec.set_max_joined_child_count(100);
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

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1, doc_store_->Put(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2, doc_store_->Put(email1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3, doc_store_->Put(email2));

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
  join_spec.set_max_joined_child_count(100);
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

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1, doc_store_->Put(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2, doc_store_->Put(email1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3, doc_store_->Put(email2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id4, doc_store_->Put(email3));

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
  join_spec.set_max_joined_child_count(100);
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

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1, doc_store_->Put(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2, doc_store_->Put(person2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3, doc_store_->Put(email1));

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
  join_spec.set_max_joined_child_count(100);
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

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1, doc_store_->Put(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2, doc_store_->Put(email1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3, doc_store_->Put(email2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id4, doc_store_->Put(email3));

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
  join_spec.set_max_joined_child_count(100);
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

TEST_F(JoinProcessorTest,
       ShouldTruncateByRankingStrategyIfExceedingMaxJoinedChildCount) {
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
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();
  DocumentProto email3 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email3")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 3")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();
  DocumentProto email4 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email4")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 4")
          .AddStringProperty("sender",
                             R"(pkg$db/name\#space\\\\#person2)")  // escaped
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1, doc_store_->Put(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2, doc_store_->Put(person2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id3, doc_store_->Put(email1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id4, doc_store_->Put(email2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id5, doc_store_->Put(email3));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id6, doc_store_->Put(email4));

  ScoredDocumentHit scored_doc_hit1(document_id1, kSectionIdMaskNone,
                                    /*score=*/0.0);
  ScoredDocumentHit scored_doc_hit2(document_id2, kSectionIdMaskNone,
                                    /*score=*/0.0);
  ScoredDocumentHit scored_doc_hit3(document_id3, kSectionIdMaskNone,
                                    /*score=*/2.0);
  ScoredDocumentHit scored_doc_hit4(document_id4, kSectionIdMaskNone,
                                    /*score=*/5.0);
  ScoredDocumentHit scored_doc_hit5(document_id5, kSectionIdMaskNone,
                                    /*score=*/3.0);
  ScoredDocumentHit scored_doc_hit6(document_id6, kSectionIdMaskNone,
                                    /*score=*/1.0);

  // Parent ScoredDocumentHits: all Person documents
  std::vector<ScoredDocumentHit> parent_scored_document_hits = {
      scored_doc_hit1, scored_doc_hit2};

  // Child ScoredDocumentHits: all Email documents
  std::vector<ScoredDocumentHit> child_scored_document_hits = {
      scored_doc_hit3, scored_doc_hit4, scored_doc_hit5, scored_doc_hit6};

  JoinSpecProto join_spec;
  join_spec.set_max_joined_child_count(2);
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
  // Since we set max_joind_child_count as 2 and use DESC as the (nested)
  // ranking strategy, parent document with # of child documents more than 2
  // should only keep 2 child documents with higher scores and the rest should
  // be truncated.
  EXPECT_THAT(
      joined_result_document_hits,
      ElementsAre(EqualsJoinedScoredDocumentHit(JoinedScoredDocumentHit(
                      /*final_score=*/2.0,
                      /*parent_scored_document_hit=*/scored_doc_hit1,
                      /*child_scored_document_hits=*/
                      {scored_doc_hit4, scored_doc_hit5})),
                  EqualsJoinedScoredDocumentHit(JoinedScoredDocumentHit(
                      /*final_score=*/1.0,
                      /*parent_scored_document_hit=*/scored_doc_hit2,
                      /*child_scored_document_hits=*/{scored_doc_hit6}))));
}

TEST_F(JoinProcessorTest, ShouldAllowSelfJoining) {
  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("subject", "test subject 1")
          .AddStringProperty("sender", "pkg$db/namespace#email1")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1, doc_store_->Put(email1));

  ScoredDocumentHit scored_doc_hit1(document_id1, kSectionIdMaskNone,
                                    /*score=*/0.0);

  // Parent ScoredDocumentHits: all Person documents
  std::vector<ScoredDocumentHit> parent_scored_document_hits = {
      scored_doc_hit1};

  // Child ScoredDocumentHits: all Email documents
  std::vector<ScoredDocumentHit> child_scored_document_hits = {scored_doc_hit1};

  JoinSpecProto join_spec;
  join_spec.set_max_joined_child_count(100);
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
