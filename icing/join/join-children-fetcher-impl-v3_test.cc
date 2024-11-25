// Copyright (C) 2024 Google LLC
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

#include "icing/join/join-children-fetcher-impl-v3.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/feature-flags.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/join/document-join-id-pair.h"
#include "icing/join/join-processor.h"
#include "icing/join/qualified-id-join-index-impl-v1.h"
#include "icing/join/qualified-id-join-index-impl-v2.h"
#include "icing/join/qualified-id-join-index-impl-v3.h"
#include "icing/join/qualified-id-join-indexing-handler.h"
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
#include "icing/testing/test-data.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/util/icu-data-file-helper.h"
#include "icing/util/status-macros.h"
#include "icing/util/tokenized-document.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::IsEmpty;
using ::testing::IsTrue;

class JoinChildrenFetcherImplV3Test : public ::testing::Test {
 protected:
  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    base_dir_ = GetTestTempDir() + "/icing_test";
    ASSERT_THAT(filesystem_.CreateDirectoryRecursively(base_dir_.c_str()),
                IsTrue());

    schema_store_dir_ = base_dir_ + "/schema_store";
    doc_store_dir_ = base_dir_ + "/doc_store";
    qualified_id_join_index_dir_ = base_dir_ + "/qualified_id_join_index";

    if (!IsCfStringTokenization() && !IsReverseJniTokenization()) {
      ICING_ASSERT_OK(
          // File generated via icu_data_file rule in //icing/BUILD.
          icu_data_file_helper::SetUpIcuDataFile(
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
        schema_store_, SchemaStore::Create(&filesystem_, schema_store_dir_,
                                           &fake_clock_, feature_flags_.get()));

    SchemaProto schema =
        SchemaBuilder()
            .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
                PropertyConfigBuilder()
                    .SetName("Name")
                    .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                    .SetCardinality(CARDINALITY_OPTIONAL)))
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("Message")
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("sender")
                                     .SetDataTypeJoinableString(
                                         JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("receiver")
                                     .SetDataTypeJoinableString(
                                         JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                     .SetCardinality(CARDINALITY_OPTIONAL)))
            .AddType(SchemaTypeConfigBuilder().SetType("Email").AddProperty(
                PropertyConfigBuilder()
                    .SetName("sender")
                    .SetDataTypeJoinableString(JOINABLE_VALUE_TYPE_QUALIFIED_ID)
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
                              schema_store_.get(), feature_flags_.get(),
                              /*force_recovery_and_revalidate_documents=*/false,
                              /*pre_mapping_fbv=*/false,
                              /*use_persistent_hash_map=*/true,
                              PortableFileBackedProtoLog<
                                  DocumentWrapper>::kDefaultCompressionLevel,
                              /*initialize_stats=*/nullptr));
    doc_store_ = std::move(create_result.document_store);

    ICING_ASSERT_OK_AND_ASSIGN(
        qualified_id_join_index_,
        QualifiedIdJoinIndexImplV3::Create(
            filesystem_, qualified_id_join_index_dir_, *feature_flags_));
  }

  void TearDown() override {
    qualified_id_join_index_.reset();
    doc_store_.reset();
    schema_store_.reset();
    lang_segmenter_.reset();

    filesystem_.DeleteDirectoryRecursively(base_dir_.c_str());
  }

  libtextclassifier3::StatusOr<DocumentId> PutAndIndexDocument(
      const DocumentProto& document) {
    ICING_ASSIGN_OR_RETURN(DocumentStore::PutResult put_result,
                           doc_store_->Put(document));
    ICING_ASSIGN_OR_RETURN(
        TokenizedDocument tokenized_document,
        TokenizedDocument::Create(schema_store_.get(), lang_segmenter_.get(),
                                  document));

    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<QualifiedIdJoinIndexingHandler> handler,
        QualifiedIdJoinIndexingHandler::Create(&fake_clock_, doc_store_.get(),
                                               qualified_id_join_index_.get()));
    ICING_RETURN_IF_ERROR(
        handler->Handle(tokenized_document, put_result.new_document_id,
                        put_result.old_document_id, /*recovery_mode=*/false,
                        /*put_document_stats=*/nullptr));
    return put_result.new_document_id;
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  Filesystem filesystem_;
  FakeClock fake_clock_;

  std::string base_dir_;
  std::string schema_store_dir_;
  std::string doc_store_dir_;
  std::string qualified_id_join_index_dir_;

  std::unique_ptr<LanguageSegmenter> lang_segmenter_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> doc_store_;
  std::unique_ptr<QualifiedIdJoinIndexImplV3> qualified_id_join_index_;
};

TEST_F(JoinChildrenFetcherImplV3Test, CreationWithNullPointerShouldFail) {
  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec.set_child_property_expression("sender");
  join_spec.mutable_nested_spec()->mutable_scoring_spec()->set_order_by(
      ScoringSpecProto::Order::ASC);

  EXPECT_THAT(JoinChildrenFetcherImplV3::Create(
                  join_spec, /*schema_store=*/nullptr, doc_store_.get(),
                  qualified_id_join_index_.get(),
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
                  /*child_scored_document_hits=*/{}),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));

  EXPECT_THAT(JoinChildrenFetcherImplV3::Create(
                  join_spec, schema_store_.get(), /*doc_store=*/nullptr,
                  qualified_id_join_index_.get(),
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
                  /*child_scored_document_hits=*/{}),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));

  EXPECT_THAT(JoinChildrenFetcherImplV3::Create(
                  join_spec, schema_store_.get(), doc_store_.get(),
                  /*qualified_id_join_index=*/nullptr,
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
                  /*child_scored_document_hits=*/{}),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(JoinChildrenFetcherImplV3Test,
       CreationWithWrongVersionJoinIndexShouldFail) {
  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec.set_child_property_expression("sender");
  join_spec.mutable_nested_spec()->mutable_scoring_spec()->set_order_by(
      ScoringSpecProto::Order::ASC);

  std::string qualified_id_join_index_dir_v1 =
      qualified_id_join_index_dir_ + "_v1";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QualifiedIdJoinIndexImplV1> qualified_id_join_index_v1,
      QualifiedIdJoinIndexImplV1::Create(
          filesystem_, std::move(qualified_id_join_index_dir_v1),
          /*pre_mapping_fbv=*/false, /*use_persistent_hash_map=*/true));

  std::string qualified_id_join_index_dir_v2 =
      qualified_id_join_index_dir_ + "_v2";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QualifiedIdJoinIndexImplV2> qualified_id_join_index_v2,
      QualifiedIdJoinIndexImplV2::Create(
          filesystem_, std::move(qualified_id_join_index_dir_v2),
          /*pre_mapping_fbv=*/false));

  EXPECT_THAT(JoinChildrenFetcherImplV3::Create(
                  join_spec, schema_store_.get(), doc_store_.get(),
                  qualified_id_join_index_v1.get(),
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
                  /*child_scored_document_hits=*/{}),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(JoinChildrenFetcherImplV3::Create(
                  join_spec, schema_store_.get(), doc_store_.get(),
                  qualified_id_join_index_v2.get(),
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
                  /*child_scored_document_hits=*/{}),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(JoinChildrenFetcherImplV3Test, GetChildren) {
  // Simulate the following scenario:
  // - Email child schema has 1 joinable property: "sender" with joinable
  //   property id 0.
  //   - email1 joins with person1 by property "sender".
  //   - email2 joins with person1 by property "sender".
  //   - email3 joins with person2 by property "sender".
  // - email1, email2 and email3 are in the child result set.
  DocumentProto person1 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person1")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person2")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Bob")
                              .Build();

  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();
  DocumentProto email2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email2")
          .SetSchema("Email")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();
  DocumentProto email3 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email3")
          .SetSchema("Email")
          .AddStringProperty("sender", "pkg$db/namespace#person2")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person1_id,
                             PutAndIndexDocument(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person2_id,
                             PutAndIndexDocument(person2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email1_id, PutAndIndexDocument(email1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email2_id, PutAndIndexDocument(email2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email3_id, PutAndIndexDocument(email3));

  ScoredDocumentHit scored_doc_hit_email1(email1_id, kSectionIdMaskNone,
                                          /*score=*/1.0);
  ScoredDocumentHit scored_doc_hit_email2(email2_id, kSectionIdMaskNone,
                                          /*score=*/2.0);
  ScoredDocumentHit scored_doc_hit_email3(email3_id, kSectionIdMaskNone,
                                          /*score=*/3.0);

  // Join by property "sender".
  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec.set_child_property_expression("sender");
  join_spec.mutable_nested_spec()->mutable_scoring_spec()->set_order_by(
      ScoringSpecProto::Order::ASC);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<JoinChildrenFetcherImplV3> fetcher,
      JoinChildrenFetcherImplV3::Create(
          join_spec, schema_store_.get(), doc_store_.get(),
          qualified_id_join_index_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
          /*child_scored_document_hits=*/
          {scored_doc_hit_email1, scored_doc_hit_email2,
           scored_doc_hit_email3}));

  EXPECT_THAT(fetcher->GetChildren(person1_id),
              IsOkAndHolds(
                  ElementsAre(EqualsScoredDocumentHit(scored_doc_hit_email1),
                              EqualsScoredDocumentHit(scored_doc_hit_email2))));
  EXPECT_THAT(fetcher->GetChildren(person2_id),
              IsOkAndHolds(
                  ElementsAre(EqualsScoredDocumentHit(scored_doc_hit_email3))));
}

TEST_F(JoinChildrenFetcherImplV3Test,
       GetChildrenJoinChildrenFromMultipleSchemas) {
  // Simulate the following scenario:
  // - Email child schema has 1 joinable property: "sender" with joinable
  //   property id 0.
  //   - email1 joins with person1 by property "sender".
  //   - email2 joins with person2 by property "sender".
  // - Message child schema has 2 joinable properties: "receiver", "sender" with
  //   joinable property ids 0, 1 respectively.
  //   - message1 joins with person2 by property "sender".
  //   - message2 joins with person2 by property "sender".
  //   - message3 joins with person1 by property "sender".
  // - email1, email2, message1, message2 and message3 are in the child result
  //   set.
  DocumentProto person1 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person1")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person2")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Bob")
                              .Build();

  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();
  DocumentProto email2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email2")
          .SetSchema("Email")
          .AddStringProperty("sender", "pkg$db/namespace#person2")
          .Build();

  DocumentProto message1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "message1")
          .SetSchema("Message")
          .AddStringProperty("sender", "pkg$db/namespace#person2")
          .Build();
  DocumentProto message2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "message2")
          .SetSchema("Message")
          .AddStringProperty("sender", "pkg$db/namespace#person2")
          .Build();
  DocumentProto message3 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "message3")
          .SetSchema("Message")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person1_id,
                             PutAndIndexDocument(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person2_id,
                             PutAndIndexDocument(person2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email1_id, PutAndIndexDocument(email1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email2_id, PutAndIndexDocument(email2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message1_id,
                             PutAndIndexDocument(message1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message2_id,
                             PutAndIndexDocument(message2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message3_id,
                             PutAndIndexDocument(message3));

  ScoredDocumentHit scored_doc_hit_email1(email1_id, kSectionIdMaskNone,
                                          /*score=*/1.0);
  ScoredDocumentHit scored_doc_hit_email2(email2_id, kSectionIdMaskNone,
                                          /*score=*/2.0);
  ScoredDocumentHit scored_doc_hit_message1(message1_id, kSectionIdMaskNone,
                                            /*score=*/3.0);
  ScoredDocumentHit scored_doc_hit_message2(message2_id, kSectionIdMaskNone,
                                            /*score=*/4.0);
  ScoredDocumentHit scored_doc_hit_message3(message3_id, kSectionIdMaskNone,
                                            /*score=*/5.0);

  // Join by property "sender".
  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec.set_child_property_expression("sender");
  join_spec.mutable_nested_spec()->mutable_scoring_spec()->set_order_by(
      ScoringSpecProto::Order::ASC);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<JoinChildrenFetcherImplV3> fetcher,
      JoinChildrenFetcherImplV3::Create(
          join_spec, schema_store_.get(), doc_store_.get(),
          qualified_id_join_index_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
          /*child_scored_document_hits=*/
          {scored_doc_hit_email1, scored_doc_hit_email2,
           scored_doc_hit_message1, scored_doc_hit_message2,
           scored_doc_hit_message3}));

  EXPECT_THAT(fetcher->GetChildren(person1_id),
              IsOkAndHolds(ElementsAre(
                  EqualsScoredDocumentHit(scored_doc_hit_email1),
                  EqualsScoredDocumentHit(scored_doc_hit_message3))));
  EXPECT_THAT(fetcher->GetChildren(person2_id),
              IsOkAndHolds(ElementsAre(
                  EqualsScoredDocumentHit(scored_doc_hit_email2),
                  EqualsScoredDocumentHit(scored_doc_hit_message1),
                  EqualsScoredDocumentHit(scored_doc_hit_message2))));
}

TEST_F(JoinChildrenFetcherImplV3Test,
       GetChildrenShouldFilterOutUndesiredJoinablePropertyIds) {
  // Simulate the following scenario:
  // - Message child schema has 2 joinable properties: "receiver", "sender" with
  //   joinable property ids 0, 1 respectively.
  //   - message1 joins with person1 by property "receiver".
  //   - message2 joins with person1 by property "sender".
  //   - message2 joins with person2 by property "receiver".
  //   - message3 joins with person1 by property "receiver".
  // - message1, message2 and message3 are in the child result set.
  DocumentProto person1 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person1")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person2")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Bob")
                              .Build();

  DocumentProto message1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "message1")
          .SetSchema("Message")
          .AddStringProperty("receiver", "pkg$db/namespace#person1")
          .Build();
  DocumentProto message2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "message2")
          .SetSchema("Message")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .AddStringProperty("receiver", "pkg$db/namespace#person2")
          .Build();
  DocumentProto message3 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "message3")
          .SetSchema("Message")
          .AddStringProperty("receiver", "pkg$db/namespace#person1")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person1_id,
                             PutAndIndexDocument(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person2_id,
                             PutAndIndexDocument(person2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message1_id,
                             PutAndIndexDocument(message1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message2_id,
                             PutAndIndexDocument(message2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message3_id,
                             PutAndIndexDocument(message3));
  // Sanity check for the join index.
  ASSERT_THAT(qualified_id_join_index_->Get(person1_id),
              IsOkAndHolds(ElementsAre(DocumentJoinIdPair(message1_id, 0),
                                       DocumentJoinIdPair(message2_id, 1),
                                       DocumentJoinIdPair(message3_id, 0))));
  ASSERT_THAT(qualified_id_join_index_->Get(person2_id),
              IsOkAndHolds(ElementsAre(DocumentJoinIdPair(message2_id, 0))));

  ScoredDocumentHit scored_doc_hit_message1(message1_id, kSectionIdMaskNone,
                                            /*score=*/1.0);
  ScoredDocumentHit scored_doc_hit_message2(message2_id, kSectionIdMaskNone,
                                            /*score=*/2.0);
  ScoredDocumentHit scored_doc_hit_message3(message3_id, kSectionIdMaskNone,
                                            /*score=*/3.0);

  // Join by property "receiver".
  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec.set_child_property_expression("receiver");
  join_spec.mutable_nested_spec()->mutable_scoring_spec()->set_order_by(
      ScoringSpecProto::Order::ASC);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<JoinChildrenFetcherImplV3> fetcher,
      JoinChildrenFetcherImplV3::Create(
          join_spec, schema_store_.get(), doc_store_.get(),
          qualified_id_join_index_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
          /*child_scored_document_hits=*/
          {scored_doc_hit_message1, scored_doc_hit_message2,
           scored_doc_hit_message3}));

  // Person1 should get message1 and message3, but not message2 since message2
  // joins with person1 by property "sender", but join spec uses "receiver" to
  // join.
  EXPECT_THAT(fetcher->GetChildren(person1_id),
              IsOkAndHolds(ElementsAre(
                  EqualsScoredDocumentHit(scored_doc_hit_message1),
                  EqualsScoredDocumentHit(scored_doc_hit_message3))));
  // Person2 should get message2.
  EXPECT_THAT(fetcher->GetChildren(person2_id),
              IsOkAndHolds(ElementsAre(
                  EqualsScoredDocumentHit(scored_doc_hit_message2))));
}

TEST_F(JoinChildrenFetcherImplV3Test,
       GetChildrenShouldFilterOutUndesiredChildDocumentIds) {
  // Simulate the following scenario:
  // - Email child schema has 1 joinable property: "sender" with joinable
  //   property id 0.
  //   - email1 joins with person1 by property "sender".
  //   - email2 joins with person1 by property "sender".
  //   - email3 joins with person2 by property "sender".
  //   - email4 joins with person3 by property "sender".
  // - Only email1 and email4 are in the child result set.
  DocumentProto person1 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person1")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person2")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Bob")
                              .Build();
  DocumentProto person3 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person3")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Eve")
                              .Build();

  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();
  DocumentProto email2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email2")
          .SetSchema("Email")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();
  DocumentProto email3 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email3")
          .SetSchema("Email")
          .AddStringProperty("sender", "pkg$db/namespace#person2")
          .Build();
  DocumentProto email4 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email4")
          .SetSchema("Email")
          .AddStringProperty("sender", "pkg$db/namespace#person3")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person1_id,
                             PutAndIndexDocument(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person2_id,
                             PutAndIndexDocument(person2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person3_id,
                             PutAndIndexDocument(person3));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email1_id, PutAndIndexDocument(email1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email2_id, PutAndIndexDocument(email2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email3_id, PutAndIndexDocument(email3));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email4_id, PutAndIndexDocument(email4));
  // Sanity check for the join index.
  ASSERT_THAT(qualified_id_join_index_->Get(person1_id),
              IsOkAndHolds(ElementsAre(DocumentJoinIdPair(email1_id, 0),
                                       DocumentJoinIdPair(email2_id, 0))));
  ASSERT_THAT(qualified_id_join_index_->Get(person2_id),
              IsOkAndHolds(ElementsAre(DocumentJoinIdPair(email3_id, 0))));
  ASSERT_THAT(qualified_id_join_index_->Get(person3_id),
              IsOkAndHolds(ElementsAre(DocumentJoinIdPair(email4_id, 0))));

  ScoredDocumentHit scored_doc_hit_email1(email1_id, kSectionIdMaskNone,
                                          /*score=*/1.0);
  ScoredDocumentHit scored_doc_hit_email4(email4_id, kSectionIdMaskNone,
                                          /*score=*/4.0);

  // Join by property "sender".
  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec.set_child_property_expression("sender");
  join_spec.mutable_nested_spec()->mutable_scoring_spec()->set_order_by(
      ScoringSpecProto::Order::ASC);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<JoinChildrenFetcherImplV3> fetcher,
      JoinChildrenFetcherImplV3::Create(
          join_spec, schema_store_.get(), doc_store_.get(),
          qualified_id_join_index_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
          /*child_scored_document_hits=*/
          {scored_doc_hit_email1, scored_doc_hit_email4}));

  // Person1 should get email1, but not email2 since email2 is not in the result
  // set.
  EXPECT_THAT(fetcher->GetChildren(person1_id),
              IsOkAndHolds(
                  ElementsAre(EqualsScoredDocumentHit(scored_doc_hit_email1))));
  // Person2 should get empty result set since email3 is not in the result set.
  EXPECT_THAT(fetcher->GetChildren(person2_id), IsOkAndHolds(IsEmpty()));
  // Person3 should get email4.
  EXPECT_THAT(fetcher->GetChildren(person3_id),
              IsOkAndHolds(
                  ElementsAre(EqualsScoredDocumentHit(scored_doc_hit_email4))));
}

TEST_F(JoinChildrenFetcherImplV3Test,
       GetChildrenShouldSkipNonExistingJoinableProperty) {
  // Simulate the following scenario:
  // - Email child schema has 1 joinable property: "sender" with joinable
  //   property id 0.
  //   - email1 joins with person1 by property "sender".
  //   - email2 joins with person1 by property "sender".
  //   - email3 joins with person2 by property "sender".
  // - email1, email2 and email3 are in the child result set, but email3 is
  //   expired.
  DocumentProto person1 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person1")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person2")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Bob")
                              .Build();

  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();
  DocumentProto email2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email2")
          .SetSchema("Email")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();
  DocumentProto email3 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email3")
          .SetSchema("Email")
          .AddStringProperty("sender", "pkg$db/namespace#person2")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person1_id,
                             PutAndIndexDocument(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person2_id,
                             PutAndIndexDocument(person2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email1_id, PutAndIndexDocument(email1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email2_id, PutAndIndexDocument(email2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email3_id, PutAndIndexDocument(email3));

  ScoredDocumentHit scored_doc_hit_email1(email1_id, kSectionIdMaskNone,
                                          /*score=*/1.0);
  ScoredDocumentHit scored_doc_hit_email2(email2_id, kSectionIdMaskNone,
                                          /*score=*/2.0);
  ScoredDocumentHit scored_doc_hit_email3(email3_id, kSectionIdMaskNone,
                                          /*score=*/3.0);

  // Join by a non-existing property "foo".
  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec.set_child_property_expression("foo");
  join_spec.mutable_nested_spec()->mutable_scoring_spec()->set_order_by(
      ScoringSpecProto::Order::ASC);

  // Create and GetChildren should not fail.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<JoinChildrenFetcherImplV3> fetcher,
      JoinChildrenFetcherImplV3::Create(
          join_spec, schema_store_.get(), doc_store_.get(),
          qualified_id_join_index_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
          /*child_scored_document_hits=*/
          {scored_doc_hit_email1, scored_doc_hit_email2,
           scored_doc_hit_email3}));

  EXPECT_THAT(fetcher->GetChildren(person1_id), IsOkAndHolds(IsEmpty()));
  EXPECT_THAT(fetcher->GetChildren(person2_id), IsOkAndHolds(IsEmpty()));
}

TEST_F(JoinChildrenFetcherImplV3Test,
       GetChildrenShouldSkipExpiredChildDocuments) {
  fake_clock_.SetSystemTimeMilliseconds(1000);

  // Simulate the following scenario:
  // - Email child schema has 1 joinable property: "sender" with joinable
  //   property id 0.
  //   - email1 joins with person1 by property "sender".
  //   - email2 joins with person1 by property "sender".
  //   - email3 joins with person1 by property "sender".
  // - email1, email2 and email3 are in the child result set.
  DocumentProto person1 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person1")
                              .SetSchema("Person")
                              .SetTtlMs(5000)
                              .AddStringProperty("Name", "Alice")
                              .Build();

  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email1")
          .SetSchema("Email")
          .SetTtlMs(5000)
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();
  DocumentProto email2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email2")
          .SetSchema("Email")
          .SetTtlMs(5000)
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();
  DocumentProto email3 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email3")
          .SetSchema("Email")
          .SetTtlMs(1000)
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person1_id,
                             PutAndIndexDocument(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email1_id, PutAndIndexDocument(email1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email2_id, PutAndIndexDocument(email2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email3_id, PutAndIndexDocument(email3));

  ScoredDocumentHit scored_doc_hit_email1(email1_id, kSectionIdMaskNone,
                                          /*score=*/1.0);
  ScoredDocumentHit scored_doc_hit_email2(email2_id, kSectionIdMaskNone,
                                          /*score=*/2.0);
  ScoredDocumentHit scored_doc_hit_email3(email3_id, kSectionIdMaskNone,
                                          /*score=*/3.0);

  // Join by property "sender".
  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec.set_child_property_expression("sender");
  join_spec.mutable_nested_spec()->mutable_scoring_spec()->set_order_by(
      ScoringSpecProto::Order::ASC);

  // Adjust the clock to make email3 expired.
  fake_clock_.SetSystemTimeMilliseconds(3000);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<JoinChildrenFetcherImplV3> fetcher,
      JoinChildrenFetcherImplV3::Create(
          join_spec, schema_store_.get(), doc_store_.get(),
          qualified_id_join_index_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
          /*child_scored_document_hits=*/
          {scored_doc_hit_email1, scored_doc_hit_email2,
           scored_doc_hit_email3}));

  // Email3 should be skipped since it is expired.
  //
  // This scenario is unlikely to happen in practice since expired documents
  // will mostly not be included in the child result set, but we still want to
  // make sure an invalid DocumentFilterData is handled correctly.
  EXPECT_THAT(fetcher->GetChildren(person1_id),
              IsOkAndHolds(
                  ElementsAre(EqualsScoredDocumentHit(scored_doc_hit_email1),
                              EqualsScoredDocumentHit(scored_doc_hit_email2))));
}

TEST_F(JoinChildrenFetcherImplV3Test, GetChildrenShouldDoLazyLookupAndCache) {
  // Simulate the following scenario:
  // - Email child schema has 1 joinable property: "sender" with joinable
  //   property id 0.
  //   - email1 joins with person1 by property "sender".
  //   - email2 joins with person1 by property "sender".
  //   - email3 joins with person2 by property "sender".
  // - email1, email2 and email3 are in the child result set.
  DocumentProto person1 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person1")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey("pkg$db/namespace", "person2")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Bob")
                              .Build();

  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();
  DocumentProto email2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email2")
          .SetSchema("Email")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .Build();
  DocumentProto email3 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "email3")
          .SetSchema("Email")
          .AddStringProperty("sender", "pkg$db/namespace#person2")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person1_id,
                             PutAndIndexDocument(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person2_id,
                             PutAndIndexDocument(person2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email1_id, PutAndIndexDocument(email1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email2_id, PutAndIndexDocument(email2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email3_id, PutAndIndexDocument(email3));

  ScoredDocumentHit scored_doc_hit_email1(email1_id, kSectionIdMaskNone,
                                          /*score=*/1.0);
  ScoredDocumentHit scored_doc_hit_email2(email2_id, kSectionIdMaskNone,
                                          /*score=*/2.0);
  ScoredDocumentHit scored_doc_hit_email3(email3_id, kSectionIdMaskNone,
                                          /*score=*/3.0);

  // Join by property "sender".
  JoinSpecProto join_spec;
  join_spec.set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec.set_child_property_expression("sender");
  join_spec.mutable_nested_spec()->mutable_scoring_spec()->set_order_by(
      ScoringSpecProto::Order::ASC);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<JoinChildrenFetcherImplV3> fetcher,
      JoinChildrenFetcherImplV3::Create(
          join_spec, schema_store_.get(), doc_store_.get(),
          qualified_id_join_index_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
          /*child_scored_document_hits=*/
          {scored_doc_hit_email1, scored_doc_hit_email2,
           scored_doc_hit_email3}));

  EXPECT_THAT(fetcher->GetChildren(person1_id),
              IsOkAndHolds(
                  ElementsAre(EqualsScoredDocumentHit(scored_doc_hit_email1),
                              EqualsScoredDocumentHit(scored_doc_hit_email2))));
  EXPECT_THAT(fetcher->GetChildren(person2_id),
              IsOkAndHolds(
                  ElementsAre(EqualsScoredDocumentHit(scored_doc_hit_email3))));

  // Intentionally clear the join index.
  ICING_ASSERT_OK(qualified_id_join_index_->Clear());

  // GetChildren again. The last round lookup should have cached the results, so
  // even though the join index is cleared, we should still get the same result.
  EXPECT_THAT(fetcher->GetChildren(person1_id),
              IsOkAndHolds(
                  ElementsAre(EqualsScoredDocumentHit(scored_doc_hit_email1),
                              EqualsScoredDocumentHit(scored_doc_hit_email2))));
  EXPECT_THAT(fetcher->GetChildren(person2_id),
              IsOkAndHolds(
                  ElementsAre(EqualsScoredDocumentHit(scored_doc_hit_email3))));
}

}  // namespace

}  // namespace lib
}  // namespace icing
