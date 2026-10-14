// Copyright (C) 2025 Google LLC
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

#include "icing/join/delete-propagation-handler.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/feature-flags.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/join/document-join-id-pair.h"
#include "icing/join/qualified-id-join-index-impl-v2.h"
#include "icing/join/qualified-id-join-index-impl-v3.h"
#include "icing/join/qualified-id-join-index.h"
#include "icing/join/qualified-id-join-indexing-handler.h"
#include "icing/portable/gzip_stream.h"
#include "icing/portable/platform.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/document_wrapper.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-group-info.h"
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
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::IsTrue;
using ::testing::Ne;
using ::testing::Pair;
using ::testing::UnorderedElementsAre;

class DeletePropagationHandlerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    fake_clock_.SetSystemTimeMilliseconds(123);

    test_dir_ = GetTestTempDir() + "/icing_delete_propagation_handler_test";
    ASSERT_THAT(filesystem_.CreateDirectoryRecursively(test_dir_.c_str()),
                IsTrue());

    schema_store_dir_ = test_dir_ + "/schema_store";
    doc_store_dir_ = test_dir_ + "/doc_store";
    qualified_id_join_index_dir_ = test_dir_ + "/qualified_id_join_index";

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
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("Message")
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("content")
                                     .SetDataTypeString(TERM_MATCH_EXACT,
                                                        TOKENIZER_PLAIN)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("receiver")
                                     .SetDataTypeJoinableString(
                                         JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                         DELETE_PROPAGATION_TYPE_NONE)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("reporter")
                                     .SetDataTypeJoinableString(
                                         JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                         DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("sender")
                                     .SetDataTypeJoinableString(
                                         JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                         DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                                     .SetCardinality(CARDINALITY_OPTIONAL)))
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("Label")
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("text")
                                     .SetDataTypeString(TERM_MATCH_EXACT,
                                                        TOKENIZER_PLAIN)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("object")
                                     .SetDataTypeJoinableString(
                                         JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                         DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("softLink")
                                     .SetDataTypeJoinableString(
                                         JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                         DELETE_PROPAGATION_TYPE_NONE)
                                     .SetCardinality(CARDINALITY_OPTIONAL)))

            .Build();
    ASSERT_THAT(schema_store_->SetSchema(
                    schema, /*ignore_errors_and_delete_documents=*/false),
                IsOk());

    ASSERT_THAT(filesystem_.CreateDirectoryRecursively(doc_store_dir_.c_str()),
                IsTrue());
    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(
            &filesystem_, doc_store_dir_, &fake_clock_, schema_store_.get(),
            feature_flags_.get(),
            /*force_recovery_and_revalidate_documents=*/false,
            /*pre_mapping_fbv=*/false,
            /*use_persistent_hash_map=*/true,
            PortableFileBackedProtoLog<
                DocumentWrapper>::kDefaultCompressionLevel,
            PortableFileBackedProtoLog<
                DocumentWrapper>::kDefaultCompressionThresholdBytes,
            protobuf_ports::kDefaultMemLevel,
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

    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  libtextclassifier3::StatusOr<DocumentId> PutAndIndexDocument(
      DocumentProto document) {
    ICING_ASSIGN_OR_RETURN(
        TokenizedDocument tokenized_document,
        TokenizedDocument::Create(
            schema_store_.get(), lang_segmenter_.get(),
            /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
            std::move(document)));
    ICING_ASSIGN_OR_RETURN(
        DocumentStore::PutResult put_result,
        doc_store_->Put(tokenized_document.document_wrapper()));

    ICING_ASSIGN_OR_RETURN(
        std::unique_ptr<QualifiedIdJoinIndexingHandler> handler,
        QualifiedIdJoinIndexingHandler::Create(&fake_clock_, doc_store_.get(),
                                               qualified_id_join_index_.get(),
                                               feature_flags_.get()));
    ICING_RETURN_IF_ERROR(
        handler->Handle(tokenized_document, put_result.new_document_id,
                        put_result.old_document_id, /*recovery_mode=*/false,
                        /*put_document_stats=*/nullptr));
    return put_result.new_document_id;
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  Filesystem filesystem_;
  std::string test_dir_;
  std::string schema_store_dir_;
  std::string doc_store_dir_;
  std::string qualified_id_join_index_dir_;

  std::unique_ptr<LanguageSegmenter> lang_segmenter_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> doc_store_;
  std::unique_ptr<QualifiedIdJoinIndex> qualified_id_join_index_;

  FakeClock fake_clock_;
};

TEST_F(DeletePropagationHandlerTest, Create_shouldFailWithNullptr) {
  EXPECT_THAT(DeletePropagationHandler::Create(
                  /*schema_store=*/nullptr, qualified_id_join_index_.get(),
                  doc_store_.get(), fake_clock_.GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));

  EXPECT_THAT(DeletePropagationHandler::Create(
                  schema_store_.get(), /*qualified_id_join_index=*/nullptr,
                  doc_store_.get(), fake_clock_.GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));

  EXPECT_THAT(
      DeletePropagationHandler::Create(
          schema_store_.get(), qualified_id_join_index_.get(),
          /*document_store=*/nullptr, fake_clock_.GetSystemTimeMilliseconds()),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(DeletePropagationHandlerTest,
       Create_shouldFailWithJoinIndexVersionNotV3) {
  std::string join_index_v2_dir = test_dir_ + "/join_index_v2";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<QualifiedIdJoinIndex> join_index_v2,
      QualifiedIdJoinIndexImplV2::Create(filesystem_,
                                         std::move(join_index_v2_dir),
                                         /*pre_mapping_fbv=*/false));
  EXPECT_THAT(DeletePropagationHandler::Create(
                  schema_store_.get(), join_index_v2.get(), doc_store_.get(),
                  fake_clock_.GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION,
                       HasSubstr("Delete propagation is enabled but qualified "
                                 "id join index v3 is not used")));
}

TEST_F(DeletePropagationHandlerTest, Handle) {
  DocumentProto person = DocumentBuilder()
                             .SetKey("pkg$db/namespace", "person")
                             .SetSchema("Person")
                             .AddStringProperty("Name", "Alice")
                             .Build();

  // "sender" (joinable property id 2) has
  // DELETE_PROPAGATION_TYPE_PROPAGATE_FROM.
  DocumentProto message =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "message")
          .SetSchema("Message")
          .AddStringProperty("content", "test content")
          .AddStringProperty("sender", "pkg$db/namespace#person")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person_doc_id,
                             PutAndIndexDocument(person));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message_doc_id,
                             PutAndIndexDocument(message));
  // Sanity check.
  ASSERT_THAT(
      qualified_id_join_index_->GetDocumentJoinIdPairArrayView(person_doc_id),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(message_doc_id, /*joinable_property_id=*/2))));

  // Deleting the parent document should propagate the delete to its child
  // document via the joinable property with
  // DELETE_PROPAGATION_TYPE_PROPAGATE_FROM.
  ICING_ASSERT_OK_AND_ASSIGN(
      DeletePropagationHandler delete_propagation_handler,
      DeletePropagationHandler::Create(
          schema_store_.get(), qualified_id_join_index_.get(), doc_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentGroupInfo deleted_child_docs_info,
      delete_propagation_handler.Handle(/*parent_doc_ids=*/{person_doc_id}));
  EXPECT_THAT(deleted_child_docs_info.Get(),
              UnorderedElementsAre(
                  Pair(EqualsDocumentGroupKey("Message", "pkg$db/namespace"),
                       UnorderedElementsAre(
                           EqualsDocumentUriId("message", message_doc_id)))));
  EXPECT_THAT(doc_store_->GetNonDeletedDocumentFilterData(message_doc_id),
              Eq(std::nullopt));
}

TEST_F(DeletePropagationHandlerTest,
       Handle_shouldNotPropagateToChildDocumentsWithPropagateDeleteDisabled) {
  DocumentProto person = DocumentBuilder()
                             .SetKey("pkg$db/namespace", "person")
                             .SetSchema("Person")
                             .AddStringProperty("Name", "Alice")
                             .Build();

  // "receiver" (joinable property id 0) has DELETE_PROPAGATION_TYPE_NONE.
  DocumentProto message =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "message")
          .SetSchema("Message")
          .AddStringProperty("content", "test content")
          .AddStringProperty("receiver", "pkg$db/namespace#person")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person_doc_id,
                             PutAndIndexDocument(person));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message_doc_id,
                             PutAndIndexDocument(message));
  // Sanity check.
  ASSERT_THAT(
      qualified_id_join_index_->GetDocumentJoinIdPairArrayView(person_doc_id),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(message_doc_id, /*joinable_property_id=*/0))));

  // Deleting the parent document should not propagate the delete to its child
  // document via the joinable property with DELETE_PROPAGATION_TYPE_NONE.
  ICING_ASSERT_OK_AND_ASSIGN(
      DeletePropagationHandler delete_propagation_handler,
      DeletePropagationHandler::Create(
          schema_store_.get(), qualified_id_join_index_.get(), doc_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentGroupInfo deleted_child_docs_info,
      delete_propagation_handler.Handle(/*parent_doc_ids=*/{person_doc_id}));
  EXPECT_THAT(deleted_child_docs_info.Get(), IsEmpty());
  EXPECT_THAT(doc_store_->GetNonDeletedDocumentFilterData(message_doc_id),
              Ne(std::nullopt));
}

TEST_F(DeletePropagationHandlerTest,
       Handle_shouldNotPropagateToNonJoinableChildDocuments) {
  DocumentProto person = DocumentBuilder()
                             .SetKey("pkg$db/namespace", "person")
                             .SetSchema("Person")
                             .AddStringProperty("Name", "Alice")
                             .Build();

  // Put person's qualified id string in a non-joinable property.
  DocumentProto message =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "message")
          .SetSchema("Message")
          .AddStringProperty("content", "pkg$db/namespace#person")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person_doc_id,
                             PutAndIndexDocument(person));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message_doc_id,
                             PutAndIndexDocument(message));
  // Sanity check.
  ASSERT_THAT(
      qualified_id_join_index_->GetDocumentJoinIdPairArrayView(person_doc_id),
      IsOkAndHolds(IsEmpty()));

  // Deleting the parent document should not propagate the delete to
  // non-joinable child documents.
  ICING_ASSERT_OK_AND_ASSIGN(
      DeletePropagationHandler delete_propagation_handler,
      DeletePropagationHandler::Create(
          schema_store_.get(), qualified_id_join_index_.get(), doc_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentGroupInfo deleted_child_docs_info,
      delete_propagation_handler.Handle(/*parent_doc_ids=*/{person_doc_id}));
  EXPECT_THAT(deleted_child_docs_info.Get(), IsEmpty());
  EXPECT_THAT(doc_store_->GetNonDeletedDocumentFilterData(message_doc_id),
              Ne(std::nullopt));
}

TEST_F(DeletePropagationHandlerTest, Handle_propagateViaMultipleProperties) {
  DocumentProto person = DocumentBuilder()
                             .SetKey("pkg$db/namespace", "person")
                             .SetSchema("Person")
                             .AddStringProperty("Name", "Alice")
                             .Build();

  // - "sender" (joinable property id 2) has
  //   DELETE_PROPAGATION_TYPE_PROPAGATE_FROM.
  // - "receiver" (joinable property id 0) has DELETE_PROPAGATION_TYPE_NONE.
  DocumentProto message =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "message")
          .SetSchema("Message")
          .AddStringProperty("content", "test content")
          .AddStringProperty("sender", "pkg$db/namespace#person")
          .AddStringProperty("receiver", "pkg$db/namespace#person")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person_doc_id,
                             PutAndIndexDocument(person));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message_doc_id,
                             PutAndIndexDocument(message));
  // Sanity check.
  ASSERT_THAT(
      qualified_id_join_index_->GetDocumentJoinIdPairArrayView(person_doc_id),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(message_doc_id, /*joinable_property_id=*/0),
          DocumentJoinIdPair(message_doc_id, /*joinable_property_id=*/2))));

  // Deleting the parent document should propagate the delete to its child
  // document when there is at least one property with
  // DELETE_PROPAGATION_TYPE_PROPAGATE_FROM.
  ICING_ASSERT_OK_AND_ASSIGN(
      DeletePropagationHandler delete_propagation_handler,
      DeletePropagationHandler::Create(
          schema_store_.get(), qualified_id_join_index_.get(), doc_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentGroupInfo deleted_child_docs_info,
      delete_propagation_handler.Handle(/*parent_doc_ids=*/{person_doc_id}));
  EXPECT_THAT(deleted_child_docs_info.Get(),
              UnorderedElementsAre(
                  Pair(EqualsDocumentGroupKey("Message", "pkg$db/namespace"),
                       UnorderedElementsAre(
                           EqualsDocumentUriId("message", message_doc_id)))));
  EXPECT_THAT(doc_store_->GetNonDeletedDocumentFilterData(message_doc_id),
              Eq(std::nullopt));
}

TEST_F(DeletePropagationHandlerTest, Handle_propagateToMultipleChildren) {
  DocumentProto person = DocumentBuilder()
                             .SetKey("pkg$db/namespace", "person")
                             .SetSchema("Person")
                             .AddStringProperty("Name", "Alice")
                             .Build();

  // "sender" (joinable property id 2) has
  // DELETE_PROPAGATION_TYPE_PROPAGATE_FROM.
  DocumentProto message1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "message1")
          .SetSchema("Message")
          .AddStringProperty("content", "test content")
          .AddStringProperty("sender", "pkg$db/namespace#person")
          .Build();
  DocumentProto message2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "message2")
          .SetSchema("Message")
          .AddStringProperty("content", "test content")
          .AddStringProperty("sender", "pkg$db/namespace#person")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person_doc_id,
                             PutAndIndexDocument(person));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message1_doc_id,
                             PutAndIndexDocument(message1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message2_doc_id,
                             PutAndIndexDocument(message2));
  // Sanity check.
  ASSERT_THAT(
      qualified_id_join_index_->GetDocumentJoinIdPairArrayView(person_doc_id),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(message1_doc_id, /*joinable_property_id=*/2),
          DocumentJoinIdPair(message2_doc_id, /*joinable_property_id=*/2))));

  // Deleting the parent document should propagate the delete to all of its
  // child documents via the joinable property with
  // DELETE_PROPAGATION_TYPE_PROPAGATE_FROM.
  ICING_ASSERT_OK_AND_ASSIGN(
      DeletePropagationHandler delete_propagation_handler,
      DeletePropagationHandler::Create(
          schema_store_.get(), qualified_id_join_index_.get(), doc_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentGroupInfo deleted_child_docs_info,
      delete_propagation_handler.Handle(/*parent_doc_ids=*/{person_doc_id}));
  EXPECT_THAT(deleted_child_docs_info.Get(),
              UnorderedElementsAre(
                  Pair(EqualsDocumentGroupKey("Message", "pkg$db/namespace"),
                       UnorderedElementsAre(
                           EqualsDocumentUriId("message1", message1_doc_id),
                           EqualsDocumentUriId("message2", message2_doc_id)))));

  EXPECT_THAT(doc_store_->GetNonDeletedDocumentFilterData(message1_doc_id),
              Eq(std::nullopt));
  EXPECT_THAT(doc_store_->GetNonDeletedDocumentFilterData(message2_doc_id),
              Eq(std::nullopt));
}

TEST_F(DeletePropagationHandlerTest, Handle_propagateFromMultipleProperties) {
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

  // "sender" (joinable property id 2) and "reporter" (joinable property id 1)
  // have DELETE_PROPAGATION_TYPE_PROPAGATE_FROM.
  DocumentProto message =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "message")
          .SetSchema("Message")
          .AddStringProperty("content", "test content")
          .AddStringProperty("sender", "pkg$db/namespace#person1")
          .AddStringProperty("reporter", "pkg$db/namespace#person2")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person1_doc_id,
                             PutAndIndexDocument(person1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person2_doc_id,
                             PutAndIndexDocument(person2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message_doc_id,
                             PutAndIndexDocument(message));
  // Sanity check.
  ASSERT_THAT(
      qualified_id_join_index_->GetDocumentJoinIdPairArrayView(person1_doc_id),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(message_doc_id, /*joinable_property_id=*/2))));
  ASSERT_THAT(
      qualified_id_join_index_->GetDocumentJoinIdPairArrayView(person2_doc_id),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(message_doc_id, /*joinable_property_id=*/1))));

  // message document should be propagated to be deleted from both person1 and
  // person2 via "sender" and "reporter" properties respectively.
  ICING_ASSERT_OK_AND_ASSIGN(
      DeletePropagationHandler delete_propagation_handler,
      DeletePropagationHandler::Create(
          schema_store_.get(), qualified_id_join_index_.get(), doc_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentGroupInfo deleted_child_docs_info,
      delete_propagation_handler.Handle(
          /*parent_doc_ids=*/{person1_doc_id, person2_doc_id}));
  EXPECT_THAT(deleted_child_docs_info.Get(),
              UnorderedElementsAre(
                  Pair(EqualsDocumentGroupKey("Message", "pkg$db/namespace"),
                       UnorderedElementsAre(
                           EqualsDocumentUriId("message", message_doc_id)))));
  EXPECT_THAT(doc_store_->GetNonDeletedDocumentFilterData(message_doc_id),
              Eq(std::nullopt));
}

TEST_F(DeletePropagationHandlerTest, Handle_propagateToGrandChildren) {
  // Create the following relations:
  //
  //                         ("object") - label1
  //                        /
  //             message1 <-
  //            /           \
  //      ("sender")         ("softLink") - label2
  //          /
  // person <-
  //          \
  //      ("receiver")       ("object") - label3
  //            \           /
  //             message2 <-
  //                        \
  //                         ("softLink") - label4
  //
  // Note: "sender" and "object" have DELETE_PROPAGATION_TYPE_PROPAGATE_FROM,
  //       while "receiver" and "softLink" have DELETE_PROPAGATION_TYPE_NONE.
  DocumentProto person = DocumentBuilder()
                             .SetKey("pkg$db/namespace", "person")
                             .SetSchema("Person")
                             .AddStringProperty("Name", "Alice")
                             .Build();

  DocumentProto message1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "message1")
          .SetSchema("Message")
          .AddStringProperty("content", "test content")
          .AddStringProperty("sender", "pkg$db/namespace#person")
          .Build();
  DocumentProto message2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "message2")
          .SetSchema("Message")
          .AddStringProperty("content", "test content")
          .AddStringProperty("receiver", "pkg$db/namespace#person")
          .Build();

  DocumentProto label1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "label1")
          .SetSchema("Label")
          .AddStringProperty("text", " label1")
          .AddStringProperty("object", "pkg$db/namespace#message1")
          .Build();
  DocumentProto label2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "label2")
          .SetSchema("Label")
          .AddStringProperty("text", " label2")
          .AddStringProperty("softLink", "pkg$db/namespace#message1")
          .Build();
  DocumentProto label3 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "label3")
          .SetSchema("Label")
          .AddStringProperty("text", " label3")
          .AddStringProperty("object", "pkg$db/namespace#message2")
          .Build();
  DocumentProto label4 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "label4")
          .SetSchema("Label")
          .AddStringProperty("text", " label4")
          .AddStringProperty("softLink", "pkg$db/namespace#message2")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person_doc_id,
                             PutAndIndexDocument(person));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message1_doc_id,
                             PutAndIndexDocument(message1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message2_doc_id,
                             PutAndIndexDocument(message2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId label1_doc_id,
                             PutAndIndexDocument(label1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId label2_doc_id,
                             PutAndIndexDocument(label2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId label3_doc_id,
                             PutAndIndexDocument(label3));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId label4_doc_id,
                             PutAndIndexDocument(label4));

  // - For children with type "Message", only message1 should be propagated to
  //   be deleted.
  // - For grand children with type "Label", only label1 should be propagated to
  //   be deleted.
  ICING_ASSERT_OK_AND_ASSIGN(
      DeletePropagationHandler delete_propagation_handler,
      DeletePropagationHandler::Create(
          schema_store_.get(), qualified_id_join_index_.get(), doc_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentGroupInfo deleted_child_docs_info,
                             delete_propagation_handler.Handle(
                                 /*parent_doc_ids=*/{person_doc_id}));
  EXPECT_THAT(deleted_child_docs_info.Get(),
              UnorderedElementsAre(
                  Pair(EqualsDocumentGroupKey("Message", "pkg$db/namespace"),
                       UnorderedElementsAre(
                           EqualsDocumentUriId("message1", message1_doc_id))),
                  Pair(EqualsDocumentGroupKey("Label", "pkg$db/namespace"),
                       UnorderedElementsAre(
                           EqualsDocumentUriId("label1", label1_doc_id)))));

  // message1 and label1 should be deleted, while message2 and label2/3/4
  // should not be deleted.
  EXPECT_THAT(doc_store_->GetNonDeletedDocumentFilterData(message1_doc_id),
              Eq(std::nullopt));
  EXPECT_THAT(doc_store_->GetNonDeletedDocumentFilterData(label1_doc_id),
              Eq(std::nullopt));

  EXPECT_THAT(doc_store_->GetNonDeletedDocumentFilterData(message2_doc_id),
              Ne(std::nullopt));
  EXPECT_THAT(doc_store_->GetNonDeletedDocumentFilterData(label2_doc_id),
              Ne(std::nullopt));
  EXPECT_THAT(doc_store_->GetNonDeletedDocumentFilterData(label3_doc_id),
              Ne(std::nullopt));
  EXPECT_THAT(doc_store_->GetNonDeletedDocumentFilterData(label4_doc_id),
              Ne(std::nullopt));
}

TEST_F(DeletePropagationHandlerTest, Handle_cycleReference) {
  // Create the following relations:
  //
  // label1 <- label2 <- label3
  //   |                   ^
  //   |                   |
  //   +-------------------+
  DocumentProto label1 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "label1")
          .SetSchema("Label")
          .AddStringProperty("text", " label1")
          .AddStringProperty("object", "pkg$db/namespace#label3")
          .Build();
  DocumentProto label2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "label2")
          .SetSchema("Label")
          .AddStringProperty("text", " label2")
          .AddStringProperty("object", "pkg$db/namespace#label1")
          .Build();
  DocumentProto label3 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "label3")
          .SetSchema("Label")
          .AddStringProperty("text", " label3")
          .AddStringProperty("object", "pkg$db/namespace#label2")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId label1_doc_id,
                             PutAndIndexDocument(label1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId label2_doc_id,
                             PutAndIndexDocument(label2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId label3_doc_id,
                             PutAndIndexDocument(label3));

  // label1 should get children = [label2_doc_id].
  ASSERT_THAT(
      qualified_id_join_index_->GetDocumentJoinIdPairArrayView(label1_doc_id),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(label2_doc_id, /*joinable_property_id=*/0))));
  // label2 should get children = [label3_doc_id].
  ASSERT_THAT(
      qualified_id_join_index_->GetDocumentJoinIdPairArrayView(label2_doc_id),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(label3_doc_id, /*joinable_property_id=*/0))));
  // label3 should get children = [label1_doc_id]
  ASSERT_THAT(
      qualified_id_join_index_->GetDocumentJoinIdPairArrayView(label3_doc_id),
      IsOkAndHolds(
          ElementsAre(DocumentJoinIdPair(label1_doc_id,
                                         /*joinable_property_id=*/0))));

  ICING_ASSERT_OK_AND_ASSIGN(
      DeletePropagationHandler delete_propagation_handler,
      DeletePropagationHandler::Create(
          schema_store_.get(), qualified_id_join_index_.get(), doc_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds()));

  // Handle {label1_doc_id}:
  // - Propagate to label2_doc_id from label1_doc_id.
  // - Propagate to label3_doc_id from label2_doc_id.
  // - When trying to propage label3_doc_id to its children =
  //   [label1_doc_id]:
  //   - label1_doc_id is already deleted, so it should not be propagated
  //     again.
  //   - There should be no infinite propagation loop.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentGroupInfo deleted_child_docs_info,
      delete_propagation_handler.Handle(/*parent_doc_ids=*/{label1_doc_id}));
  EXPECT_THAT(
      deleted_child_docs_info.Get(),
      UnorderedElementsAre(Pair(
          EqualsDocumentGroupKey("Label", "pkg$db/namespace"),
          UnorderedElementsAre(EqualsDocumentUriId("label2", label2_doc_id),
                               EqualsDocumentUriId("label3", label3_doc_id)))));
  EXPECT_THAT(doc_store_->GetNonDeletedDocumentFilterData(label2_doc_id),
              Eq(std::nullopt));
  EXPECT_THAT(doc_store_->GetNonDeletedDocumentFilterData(label3_doc_id),
              Eq(std::nullopt));
}

TEST_F(DeletePropagationHandlerTest, Handle_selfCycleReference) {
  DocumentProto label =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "label")
          .SetSchema("Label")
          .AddStringProperty("text", " label")
          .AddStringProperty("object", "pkg$db/namespace#label")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId label_doc_id,
                             PutAndIndexDocument(label));

  // Sanity check.
  ASSERT_THAT(
      qualified_id_join_index_->GetDocumentJoinIdPairArrayView(label_doc_id),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(label_doc_id, /*joinable_property_id=*/0))));

  // Handle {label_doc_id}: should delete nothing since label_doc_id is already
  // in the deleted set. Also there should be no infinite propagation loop.
  ICING_ASSERT_OK_AND_ASSIGN(
      DeletePropagationHandler delete_propagation_handler,
      DeletePropagationHandler::Create(
          schema_store_.get(), qualified_id_join_index_.get(), doc_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentGroupInfo deleted_child_docs_info,
      delete_propagation_handler.Handle(/*parent_doc_ids=*/{label_doc_id}));
  EXPECT_THAT(deleted_child_docs_info.Get(), IsEmpty());
}

TEST_F(DeletePropagationHandlerTest, Handle_shouldPropagateToExpiredDocuments) {
  constexpr int64_t kCreationTimestampMs = 1000;
  constexpr int64_t kShortTtlMs = 1000;
  constexpr int64_t kLongTtlMs = 10000;

  // Create the following relations:
  //
  // label1 <- label2 <- label3
  DocumentProto label1 = DocumentBuilder()
                             .SetKey("pkg$db/namespace", "label1")
                             .SetSchema("Label")
                             .AddStringProperty("text", " label1")
                             .SetCreationTimestampMs(kCreationTimestampMs)
                             .SetTtlMs(kLongTtlMs)
                             .Build();
  DocumentProto label2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "label2")
          .SetSchema("Label")
          .AddStringProperty("text", " label2")
          .AddStringProperty("object", "pkg$db/namespace#label1")
          .SetCreationTimestampMs(kCreationTimestampMs)
          .SetTtlMs(kShortTtlMs)
          .Build();
  DocumentProto label3 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "label3")
          .SetSchema("Label")
          .AddStringProperty("text", " label3")
          .AddStringProperty("object", "pkg$db/namespace#label2")
          .SetCreationTimestampMs(kCreationTimestampMs)
          .SetTtlMs(kLongTtlMs)
          .Build();

  fake_clock_.SetSystemTimeMilliseconds(kCreationTimestampMs);
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId label1_doc_id,
                             PutAndIndexDocument(label1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId label2_doc_id,
                             PutAndIndexDocument(label2));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId label3_doc_id,
                             PutAndIndexDocument(label3));

  // Adjust the clock to expire label2.
  int64_t current_time_ms = kCreationTimestampMs + kShortTtlMs + 100;
  fake_clock_.SetSystemTimeMilliseconds(current_time_ms);

  // Sanity check: label2 is expired (but not deleted), and label1 and label3
  // are not expired.
  ASSERT_THAT(
      doc_store_->GetAliveDocumentFilterData(label1_doc_id, current_time_ms),
      Ne(std::nullopt));
  ASSERT_THAT(
      doc_store_->GetAliveDocumentFilterData(label2_doc_id, current_time_ms),
      Eq(std::nullopt));
  ASSERT_THAT(doc_store_->GetNonDeletedDocumentFilterData(label2_doc_id),
              Ne(std::nullopt));
  ASSERT_THAT(
      doc_store_->GetAliveDocumentFilterData(label3_doc_id, current_time_ms),
      Ne(std::nullopt));
  ASSERT_THAT(
      qualified_id_join_index_->GetDocumentJoinIdPairArrayView(label1_doc_id),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(label2_doc_id, /*joinable_property_id=*/0))));
  ASSERT_THAT(
      qualified_id_join_index_->GetDocumentJoinIdPairArrayView(label2_doc_id),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(label3_doc_id, /*joinable_property_id=*/0))));
  ASSERT_THAT(
      qualified_id_join_index_->GetDocumentJoinIdPairArrayView(label3_doc_id),
      IsOkAndHolds(IsEmpty()));

  // Handle {label1_doc_id}: should still propagate to label2_doc_id and
  // label3_doc_id even though label2_doc_id is expired.
  ICING_ASSERT_OK_AND_ASSIGN(
      DeletePropagationHandler delete_propagation_handler,
      DeletePropagationHandler::Create(
          schema_store_.get(), qualified_id_join_index_.get(), doc_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentGroupInfo deleted_child_docs_info,
      delete_propagation_handler.Handle(/*parent_doc_ids=*/{label1_doc_id}));
  EXPECT_THAT(
      deleted_child_docs_info.Get(),
      UnorderedElementsAre(Pair(
          EqualsDocumentGroupKey("Label", "pkg$db/namespace"),
          UnorderedElementsAre(EqualsDocumentUriId("label2", label2_doc_id),
                               EqualsDocumentUriId("label3", label3_doc_id)))));
  EXPECT_THAT(doc_store_->GetNonDeletedDocumentFilterData(label2_doc_id),
              Eq(std::nullopt));
  EXPECT_THAT(doc_store_->GetNonDeletedDocumentFilterData(label3_doc_id),
              Eq(std::nullopt));
}

TEST_F(DeletePropagationHandlerTest,
       Handle_shouldNotPropagateToDeletedDocuments) {
  // Create the following relations:
  //
  // label1 <- label2
  DocumentProto label1 = DocumentBuilder()
                             .SetKey("pkg$db/namespace", "label1")
                             .SetSchema("Label")
                             .AddStringProperty("text", " label1")
                             .Build();
  DocumentProto label2 =
      DocumentBuilder()
          .SetKey("pkg$db/namespace", "label2")
          .SetSchema("Label")
          .AddStringProperty("text", " label2")
          .AddStringProperty("object", "pkg$db/namespace#label1")
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId label1_doc_id,
                             PutAndIndexDocument(label1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId label2_doc_id,
                             PutAndIndexDocument(label2));

  // Delete label2.
  ICING_ASSERT_OK(doc_store_->Delete(label2_doc_id,
                                     fake_clock_.GetSystemTimeMilliseconds()));

  // Sanity check: label2 is deleted, but join data is still present.
  ASSERT_THAT(doc_store_->GetNonDeletedDocumentFilterData(label2_doc_id),
              Eq(std::nullopt));
  ASSERT_THAT(
      qualified_id_join_index_->GetDocumentJoinIdPairArrayView(label1_doc_id),
      IsOkAndHolds(ElementsAre(
          DocumentJoinIdPair(label2_doc_id, /*joinable_property_id=*/0))));
  ASSERT_THAT(
      qualified_id_join_index_->GetDocumentJoinIdPairArrayView(label2_doc_id),
      IsOkAndHolds(IsEmpty()));

  // Handle {label1_doc_id}: should not propagate to label2_doc_id.
  ICING_ASSERT_OK_AND_ASSIGN(
      DeletePropagationHandler delete_propagation_handler,
      DeletePropagationHandler::Create(
          schema_store_.get(), qualified_id_join_index_.get(), doc_store_.get(),
          fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentGroupInfo deleted_child_docs_info,
      delete_propagation_handler.Handle(/*parent_doc_ids=*/{label1_doc_id}));
  EXPECT_THAT(deleted_child_docs_info.Get(), IsEmpty());
}

}  // namespace
}  // namespace lib
}  // namespace icing
