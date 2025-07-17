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

#include "icing/join/document-dependency-processor.h"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/feature-flags.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/portable/gzip_stream.h"
#include "icing/portable/platform.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/document_wrapper.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-id.h"
#include "icing/store/document-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/test-data.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/util/document-util.h"
#include "icing/util/icu-data-file-helper.h"
#include "icing/util/tokenized-document.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::IsTrue;
using ::testing::UnorderedElementsAre;

class DocumentDependencyProcessorTest : public ::testing::Test {
 protected:
  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    ASSERT_THAT(feature_flags_->enable_repeated_field_joins(), IsTrue());

    test_dir_ = GetTestTempDir() + "/document_dependency_processor_test";
    ASSERT_THAT(filesystem_.CreateDirectoryRecursively(test_dir_.c_str()),
                IsTrue());

    schema_store_dir_ = test_dir_ + "/schema_store";
    doc_store_dir_ = test_dir_ + "/doc_store";

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
                                     .SetName("receiver")
                                     .SetDataTypeJoinableString(
                                         JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                         DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                                     .SetCardinality(CARDINALITY_REPEATED))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("sender")
                                     .SetDataTypeJoinableString(
                                         JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                         DELETE_PROPAGATION_TYPE_NONE)
                                     .SetCardinality(CARDINALITY_REPEATED)))
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("Label")
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("target")
                                     .SetDataTypeJoinableString(
                                         JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                         DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                                     .SetCardinality(CARDINALITY_REPEATED))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("softTarget")
                                     .SetDataTypeJoinableString(
                                         JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                         DELETE_PROPAGATION_TYPE_NONE)
                                     .SetCardinality(CARDINALITY_REPEATED)))

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
  }

  void TearDown() override {
    doc_store_.reset();
    schema_store_.reset();
    lang_segmenter_.reset();

    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  Filesystem filesystem_;
  FakeClock fake_clock_;
  std::string test_dir_;
  std::string schema_store_dir_;
  std::string doc_store_dir_;

  std::unique_ptr<LanguageSegmenter> lang_segmenter_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> doc_store_;
};

TEST_F(DocumentDependencyProcessorTest,
       Create_alreadyExpiredDocumentShouldFail) {
  fake_clock_.SetSystemTimeMilliseconds(500);

  DocumentProto email1 = DocumentBuilder()
                             .SetCreationTimestampMs(100)
                             .SetTtlMs(400)
                             .SetKey("namespace", "email")
                             .SetSchema("Email")
                             .Build();
  DocumentProto email2 = DocumentBuilder()
                             .SetCreationTimestampMs(100)
                             .SetTtlMs(300)
                             .SetKey("namespace", "email")
                             .SetSchema("Email")
                             .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_email1,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), email1));
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_email2,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), email2));

  std::vector<TokenizedDocument> batch_documents_to_add1;
  batch_documents_to_add1.push_back(std::move(tokenized_doc_email1));

  EXPECT_THAT(DocumentDependencyProcessor::Create(
                  doc_store_.get(), batch_documents_to_add1,
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("The new document is expired.")));

  std::vector<TokenizedDocument> batch_documents_to_add2;
  batch_documents_to_add2.push_back(std::move(tokenized_doc_email2));

  EXPECT_THAT(DocumentDependencyProcessor::Create(
                  doc_store_.get(), batch_documents_to_add2,
                  /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("The new document is expired.")));
}

TEST_F(DocumentDependencyProcessorTest, Evaluate) {
  // This is a general test case to evaluate a batch of documents with all the
  // possible scenarios:
  // - Replace existing documents (i.e. replace documents that were previously
  //   added into the document store).
  // - Add new documents.
  //
  // And both of them have dependencies on existing, replaced and new documents.

  // Create person1, person2, email1, label1, label2, label3 with the following
  // relation:
  //
  // person1        person2 --> email1        label1 --> label2 --> label3
  DocumentProto person1 = DocumentBuilder()
                              .SetKey("namespace", "person1")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey("namespace", "person2")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Bob")
                              .Build();
  DocumentProto email1 = DocumentBuilder()
                             .SetKey("namespace", "email1")
                             .SetSchema("Email")
                             .AddStringProperty("receiver", "namespace#person2")
                             .Build();
  DocumentProto label1 = DocumentBuilder()
                             .SetKey("namespace", "label1")
                             .SetSchema("Label")
                             .Build();
  DocumentProto label2 = DocumentBuilder()
                             .SetKey("namespace", "label2")
                             .SetSchema("Label")
                             .AddStringProperty("target", "namespace#label1")
                             .Build();
  DocumentProto label3 = DocumentBuilder()
                             .SetKey("namespace", "label3")
                             .SetSchema("Label")
                             .AddStringProperty("target", "namespace#label2")
                             .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_person1,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
          person1));
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_person2,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
          person2));
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_email1,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), email1));
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_label1,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), label1));
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_label2,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), label2));
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_label3,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), label3));

  std::vector<TokenizedDocument> batch_documents_to_add1;
  batch_documents_to_add1.push_back(std::move(tokenized_doc_person1));
  batch_documents_to_add1.push_back(std::move(tokenized_doc_person2));
  batch_documents_to_add1.push_back(std::move(tokenized_doc_email1));
  batch_documents_to_add1.push_back(std::move(tokenized_doc_label1));
  batch_documents_to_add1.push_back(std::move(tokenized_doc_label2));
  batch_documents_to_add1.push_back(std::move(tokenized_doc_label3));

  // Evaluate all of them together should succeed.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor processor1,
      DocumentDependencyProcessor::Create(
          doc_store_.get(), batch_documents_to_add1,
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor::EvaluateResult result1,
      processor1.Evaluate());
  // No dependency documents out of the batch.
  EXPECT_THAT(result1.outer_dependency_document_ids,
              ElementsAre(IsEmpty(), IsEmpty(), IsEmpty(), IsEmpty(), IsEmpty(),
                          IsEmpty()));
  // No replaced expired documents.
  EXPECT_THAT(result1.existing_expired_doc_ids_to_replace, IsEmpty());

  // Put them into the document store.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result_person1,
      doc_store_->Put(
          batch_documents_to_add1[0].document_wrapper()));  // person1
  ICING_ASSERT_OK(doc_store_->Put(
      batch_documents_to_add1[1].document_wrapper()));  // person2
  ICING_ASSERT_OK(doc_store_->Put(
      batch_documents_to_add1[2].document_wrapper()));  // email1
  ICING_ASSERT_OK(doc_store_->Put(
      batch_documents_to_add1[3].document_wrapper()));  // label1
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result_label2,
      doc_store_->Put(
          batch_documents_to_add1[4].document_wrapper()));  // label2
  ICING_ASSERT_OK(doc_store_->Put(
      batch_documents_to_add1[5].document_wrapper()));  // label3
  DocumentId doc_id_person1 = put_result_person1.new_document_id;
  DocumentId doc_id_label2 = put_result_label2.new_document_id;

  // Replace existing person2, email1, label1 and add new label4, label5 to make
  // the following relation:
  // (person1, label2, label3 are not changed).
  //
  // person1 -------+              +--------> label4 <--------+
  // (unchanged)    |              |          (NEW)           |
  //                v              |             |            |
  //              email1 -----> label5           |         label2 -----> label3
  //              (REPLACED)     (NEW)           |         (unchanged)
  //                ^              ^             |            ^
  //                |              |             v            |
  // person2 -------+              +--------- label1 ---------+
  // (REPLACED)                               (REPLACED)
  DocumentProto person2_to_replace = DocumentBuilder()
                                         .SetKey("namespace", "person2")
                                         .SetSchema("Person")
                                         .AddStringProperty("Name", "Robert")
                                         .Build();
  DocumentProto email1_to_replace =
      DocumentBuilder()
          .SetKey("namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("receiver", "namespace#person1",
                             "namespace#person2")
          .Build();
  DocumentProto label1_to_replace =
      DocumentBuilder()
          .SetKey("namespace", "label1")
          .SetSchema("Label")
          .AddStringProperty("target", "namespace#label4")
          .Build();
  DocumentProto label4 =
      DocumentBuilder()
          .SetKey("namespace", "label4")
          .SetSchema("Label")
          .AddStringProperty("target", "namespace#label2", "namespace#label5")
          .Build();
  DocumentProto label5 =
      DocumentBuilder()
          .SetKey("namespace", "label5")
          .SetSchema("Label")
          .AddStringProperty("target", "namespace#label1", "namespace#email1")
          .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_person2_to_replace,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
          person2_to_replace));
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_email1_to_replace,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
          email1_to_replace));
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_label1_to_replace,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
          label1_to_replace));
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_label4,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), label4));
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_label5,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), label5));

  std::vector<TokenizedDocument> batch_documents_to_add2;
  batch_documents_to_add2.push_back(
      std::move(tokenized_doc_person2_to_replace));
  batch_documents_to_add2.push_back(std::move(tokenized_doc_email1_to_replace));
  batch_documents_to_add2.push_back(std::move(tokenized_doc_label1_to_replace));
  batch_documents_to_add2.push_back(std::move(tokenized_doc_label4));
  batch_documents_to_add2.push_back(std::move(tokenized_doc_label5));

  // Evaluate all of them together should succeed.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor processor2,
      DocumentDependencyProcessor::Create(
          doc_store_.get(), batch_documents_to_add2,
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor::EvaluateResult result2,
      processor2.Evaluate());
  EXPECT_THAT(
      result2.outer_dependency_document_ids,
      ElementsAre(
          IsEmpty(),  // person2 has no outer dependency.
          UnorderedElementsAre(
              doc_id_person1),  // email1 has an outer dependency on person1.
          IsEmpty(),            // label1 has no outer dependency.
          UnorderedElementsAre(
              doc_id_label2),  // label4 has an outer dependency on label2.
          IsEmpty()            // label5 has no outer dependency.
          ));
  // No replaced expired documents.
  EXPECT_THAT(result2.existing_expired_doc_ids_to_replace, IsEmpty());
}

TEST_F(DocumentDependencyProcessorTest,
       Evaluate_singleDocumentWithoutDependency) {
  // Set the current time to 0.
  fake_clock_.SetSystemTimeMilliseconds(0);

  // Create a person document with expiration timestamp 2000.
  DocumentProto person = DocumentBuilder()
                             .SetCreationTimestampMs(0)
                             .SetTtlMs(2000)
                             .SetKey("namespace", "person")
                             .SetSchema("Person")
                             .AddStringProperty("Name", "Alice")
                             .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_person,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), person));

  std::vector<TokenizedDocument> batch_documents_to_add1;
  batch_documents_to_add1.push_back(std::move(tokenized_doc_person));

  // Evaluate person should succeed.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor processor1,
      DocumentDependencyProcessor::Create(
          doc_store_.get(), batch_documents_to_add1,
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor::EvaluateResult result1,
      processor1.Evaluate());
  // No dependency documents out of the batch.
  EXPECT_THAT(result1.outer_dependency_document_ids, ElementsAre(IsEmpty()));
  // No replaced expired documents.
  EXPECT_THAT(result1.existing_expired_doc_ids_to_replace, IsEmpty());

  // Put person into the document store.
  ICING_ASSERT_OK(doc_store_->Put(
      batch_documents_to_add1[0].document_wrapper()));  // person

  // Set the current time to 1000.
  fake_clock_.SetSystemTimeMilliseconds(1000);

  // Replace person with expiration timestamp 1300.
  DocumentProto person_to_replace1 = DocumentBuilder()
                                         .SetCreationTimestampMs(1000)
                                         .SetTtlMs(300)
                                         .SetKey("namespace", "person")
                                         .SetSchema("Person")
                                         .AddStringProperty("Name", "Bob")
                                         .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_person_to_replace1,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
          person_to_replace1));

  std::vector<TokenizedDocument> batch_documents_to_add2;
  batch_documents_to_add2.push_back(
      std::move(tokenized_doc_person_to_replace1));

  // Evaluate person (replaced 1) should succeed.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor processor2,
      DocumentDependencyProcessor::Create(
          doc_store_.get(), batch_documents_to_add2,
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor::EvaluateResult result2,
      processor2.Evaluate());
  // No dependency documents out of the batch.
  EXPECT_THAT(result2.outer_dependency_document_ids, ElementsAre(IsEmpty()));
  // No replaced expired documents.
  EXPECT_THAT(result2.existing_expired_doc_ids_to_replace, IsEmpty());

  // Put person (replaced 1) into the document store.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result_person_to_replace1,
      doc_store_->Put(batch_documents_to_add2[0]
                          .document_wrapper()));  // person_to_replace1
  DocumentId doc_id_person_to_replace1 =
      put_result_person_to_replace1.new_document_id;

  // Set the current time to 2000.
  fake_clock_.SetSystemTimeMilliseconds(2000);

  // Replace person with expiration timestamp 3000.
  DocumentProto person_to_replace2 = DocumentBuilder()
                                         .SetCreationTimestampMs(2000)
                                         .SetTtlMs(1000)
                                         .SetKey("namespace", "person")
                                         .SetSchema("Person")
                                         .AddStringProperty("Name", "Bob")
                                         .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_person_to_replace2,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
          person_to_replace2));

  std::vector<TokenizedDocument> batch_documents_to_add3;
  batch_documents_to_add3.push_back(
      std::move(tokenized_doc_person_to_replace2));

  // Evaluate person (replaced 2) should succeed.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor processor3,
      DocumentDependencyProcessor::Create(
          doc_store_.get(), batch_documents_to_add3,
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor::EvaluateResult result3,
      processor3.Evaluate());
  // No dependency documents out of the batch.
  EXPECT_THAT(result3.outer_dependency_document_ids, ElementsAre(IsEmpty()));
  // Since at t = 2000 the original person document (doc_id_person_to_replace1)
  // is expired, it should be detected.
  EXPECT_THAT(result3.existing_expired_doc_ids_to_replace,
              UnorderedElementsAre(doc_id_person_to_replace1));
}

TEST_F(DocumentDependencyProcessorTest,
       Evaluate_nonExistentReferencedDocumentShouldFail) {
  DocumentProto email = DocumentBuilder()
                            .SetKey("namespace", "email")
                            .SetSchema("Email")
                            .AddStringProperty("receiver", "namespace#person")
                            .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_email,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), email));

  std::vector<TokenizedDocument> batch_documents_to_add;
  batch_documents_to_add.push_back(std::move(tokenized_doc_email));

  // Evaluate should fail since email's referenced document ("namespace#person")
  // with delete propagation enabled doesn't exist.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor processor,
      DocumentDependencyProcessor::Create(
          doc_store_.get(), batch_documents_to_add,
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));
  EXPECT_THAT(processor.Evaluate(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("A dependency document is not found")));
}

TEST_F(DocumentDependencyProcessorTest,
       Evaluate_invalidReferencedQualifiedIdShouldFail) {
  DocumentProto email =
      DocumentBuilder()
          .SetKey("namespace", "email")
          .SetSchema("Email")
          .AddStringProperty("receiver", "invalid_qualified_id")
          .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_email,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), email));

  std::vector<TokenizedDocument> batch_documents_to_add;
  batch_documents_to_add.push_back(std::move(tokenized_doc_email));

  // Evaluate should fail since email contains an invalid qualified id in a
  // joinable property with delete propagation enabled.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor processor,
      DocumentDependencyProcessor::Create(
          doc_store_.get(), batch_documents_to_add,
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));
  EXPECT_THAT(processor.Evaluate(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("Invalid qualified id string")));
}

TEST_F(DocumentDependencyProcessorTest,
       Evaluate_emptyQualifiedIdStringShouldSucceed) {
  DocumentProto email = DocumentBuilder()
                            .SetKey("namespace", "email")
                            .SetSchema("Email")
                            .AddStringProperty("receiver", "")
                            .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_email,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), email));

  std::vector<TokenizedDocument> batch_documents_to_add;
  batch_documents_to_add.push_back(std::move(tokenized_doc_email));

  // Evaluate should succeed since empty qualified id string is allowed.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor processor,
      DocumentDependencyProcessor::Create(
          doc_store_.get(), batch_documents_to_add,
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentDependencyProcessor::EvaluateResult result,
                             processor.Evaluate());
  EXPECT_THAT(result.outer_dependency_document_ids, ElementsAre(IsEmpty()));
  EXPECT_THAT(result.existing_expired_doc_ids_to_replace, IsEmpty());
}

TEST_F(DocumentDependencyProcessorTest,
       Evaluate_allReferencedDocumentsInBatchShouldSucceed) {
  // Create person1, person2, email with the following relation:
  //
  // person1 -------+
  //                |
  //                v
  //              email
  //                ^
  //                |
  // person2 -------+
  //
  // (email has 2 parent documents person1 and person2)
  DocumentProto person1 = DocumentBuilder()
                              .SetKey("namespace", "person1")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey("namespace", "person2")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Bob")
                              .Build();
  DocumentProto email = DocumentBuilder()
                            .SetKey("namespace", "email")
                            .SetSchema("Email")
                            .AddStringProperty("receiver", "namespace#person1",
                                               "namespace#person2")
                            .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_person1,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
          person1));
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_person2,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(),
          person2));
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_email,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), email));

  std::vector<TokenizedDocument> batch_documents_to_add;
  batch_documents_to_add.push_back(std::move(tokenized_doc_person1));
  batch_documents_to_add.push_back(std::move(tokenized_doc_person2));
  batch_documents_to_add.push_back(std::move(tokenized_doc_email));

  // Evaluate all of them together should succeed.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor processor,
      DocumentDependencyProcessor::Create(
          doc_store_.get(), batch_documents_to_add,
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentDependencyProcessor::EvaluateResult result,
                             processor.Evaluate());
  // No dependency documents out of the batch.
  EXPECT_THAT(result.outer_dependency_document_ids,
              ElementsAre(IsEmpty(), IsEmpty(), IsEmpty()));
  EXPECT_THAT(result.existing_expired_doc_ids_to_replace, IsEmpty());
}

TEST_F(DocumentDependencyProcessorTest,
       Evaluate_selfReferenceInBatchShouldSucceed) {
  // Create label document having a self reference on "target" property with
  // delete propagation enabled.
  DocumentProto label = DocumentBuilder()
                            .SetKey("namespace", "label")
                            .SetSchema("Label")
                            .AddStringProperty("target", "namespace#label")
                            .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_label,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), label));

  std::vector<TokenizedDocument> batch_documents_to_add;
  batch_documents_to_add.push_back(std::move(tokenized_doc_label));

  // Evaluate should succeed since the referenced document is present in the
  // same batch of documents to add.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor processor,
      DocumentDependencyProcessor::Create(
          doc_store_.get(), batch_documents_to_add,
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentDependencyProcessor::EvaluateResult result,
                             processor.Evaluate());
  // No dependency documents out of the batch.
  EXPECT_THAT(result.outer_dependency_document_ids, ElementsAre(IsEmpty()));
  EXPECT_THAT(result.existing_expired_doc_ids_to_replace, IsEmpty());
}

TEST_F(DocumentDependencyProcessorTest,
       Evaluate_cycleReferenceInBatchShouldSucceed) {
  // Create label1, label2, label3 with the following relation:
  //
  // label1 -> label2 -> label3
  //   ^                   |
  //   |                   |
  //   +-------------------+
  DocumentProto label1 = DocumentBuilder()
                             .SetKey("namespace", "label1")
                             .SetSchema("Label")
                             .AddStringProperty("target", "namespace#label3")
                             .Build();
  DocumentProto label2 = DocumentBuilder()
                             .SetKey("namespace", "label2")
                             .SetSchema("Label")
                             .AddStringProperty("target", "namespace#label1")
                             .Build();
  DocumentProto label3 = DocumentBuilder()
                             .SetKey("namespace", "label3")
                             .SetSchema("Label")
                             .AddStringProperty("target", "namespace#label2")
                             .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_label1,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), label1));
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_label2,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), label2));
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_label3,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), label3));

  std::vector<TokenizedDocument> batch_documents_to_add;
  batch_documents_to_add.push_back(std::move(tokenized_doc_label1));
  batch_documents_to_add.push_back(std::move(tokenized_doc_label2));
  batch_documents_to_add.push_back(std::move(tokenized_doc_label3));

  // Evaluate should succeed since the all referenced documents are present in
  // the same batch of documents to add.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor processor,
      DocumentDependencyProcessor::Create(
          doc_store_.get(), batch_documents_to_add,
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentDependencyProcessor::EvaluateResult result,
                             processor.Evaluate());
  // No dependency documents out of the batch.
  EXPECT_THAT(result.outer_dependency_document_ids,
              ElementsAre(IsEmpty(), IsEmpty(), IsEmpty()));
  EXPECT_THAT(result.existing_expired_doc_ids_to_replace, IsEmpty());
}

TEST_F(DocumentDependencyProcessorTest,
       Evaluate_allReferencedDocumentsInDocumentStoreShouldSucceed) {
  // Create person1, person2, email with the following relation:
  //
  // person1 -------+
  //                |
  //                v
  //              email
  //                ^
  //                |
  // person2 -------+
  //
  // (email has 2 parent documents person1 and person2)
  DocumentProto person1 = DocumentBuilder()
                              .SetKey("namespace", "person1")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey("namespace", "person2")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Bob")
                              .Build();
  DocumentProto email = DocumentBuilder()
                            .SetKey("namespace", "email")
                            .SetSchema("Email")
                            .AddStringProperty("receiver", "namespace#person1",
                                               "namespace#person2")
                            .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_email,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), email));

  // Put person1, person2 into the document store.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result_person1,
      doc_store_->Put(document_util::CreateDocumentWrapper(person1)));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentStore::PutResult put_result_person2,
      doc_store_->Put(document_util::CreateDocumentWrapper(person2)));
  DocumentId doc_id_person1 = put_result_person1.new_document_id;
  DocumentId doc_id_person2 = put_result_person2.new_document_id;

  std::vector<TokenizedDocument> batch_documents_to_add;
  batch_documents_to_add.push_back(std::move(tokenized_doc_email));

  // Evaluate email should succeed.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor processor,
      DocumentDependencyProcessor::Create(
          doc_store_.get(), batch_documents_to_add,
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentDependencyProcessor::EvaluateResult result,
                             processor.Evaluate());
  EXPECT_THAT(
      result.outer_dependency_document_ids,
      ElementsAre(UnorderedElementsAre(doc_id_person1, doc_id_person2)));
  EXPECT_THAT(result.existing_expired_doc_ids_to_replace, IsEmpty());
}

TEST_F(DocumentDependencyProcessorTest,
       Evaluate_expiredReferencedDocumentInDocumentStoreShouldFail) {
  fake_clock_.SetSystemTimeMilliseconds(500);

  // Create person1, person2, email with the following relation:
  //
  // person1 -------+
  //                |
  //                v
  //              email
  //                ^
  //                |
  // person2 -------+
  //
  // (email has 2 parent documents person1 and person2)
  DocumentProto person1 = DocumentBuilder()
                              .SetKey("namespace", "person1")
                              .SetTtlMs(1000)
                              .SetCreationTimestampMs(500)
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey("namespace", "person2")
                              .SetTtlMs(100)
                              .SetCreationTimestampMs(500)
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Bob")
                              .Build();
  DocumentProto email = DocumentBuilder()
                            .SetKey("namespace", "email")
                            .SetSchema("Email")
                            .AddStringProperty("receiver", "namespace#person1",
                                               "namespace#person2")
                            .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_email,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), email));

  // Put person1, person2 into the document store.
  ICING_ASSERT_OK(
      doc_store_->Put(document_util::CreateDocumentWrapper(person1)));
  ICING_ASSERT_OK(
      doc_store_->Put(document_util::CreateDocumentWrapper(person2)));

  // Adjust the current time to make person2 expired, but person1 is still
  // alive.
  fake_clock_.SetSystemTimeMilliseconds(1000);

  std::vector<TokenizedDocument> batch_documents_to_add;
  batch_documents_to_add.push_back(std::move(tokenized_doc_email));

  // Evaluate email should fail since one of email's referenced documents
  // (person2) with delete propagation enabled is expired.
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor processor,
      DocumentDependencyProcessor::Create(
          doc_store_.get(), batch_documents_to_add,
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));
  EXPECT_THAT(processor.Evaluate(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("A dependency document is not alive")));
}

TEST_F(DocumentDependencyProcessorTest,
       Evaluate_deletedReferencedDocumentInDocumentStoreShouldFail) {
  // Create person1, person2, email with the following relation:
  //
  // person1 -------+
  //                |
  //                v
  //              email
  //                ^
  //                |
  // person2 -------+
  //
  // (email has 2 parent documents person1 and person2)
  DocumentProto person1 = DocumentBuilder()
                              .SetKey("namespace", "person1")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey("namespace", "person2")
                              .SetSchema("Person")
                              .AddStringProperty("Name", "Bob")
                              .Build();
  DocumentProto email = DocumentBuilder()
                            .SetKey("namespace", "email")
                            .SetSchema("Email")
                            .AddStringProperty("receiver", "namespace#person1",
                                               "namespace#person2")
                            .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_email,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), email));

  // Put person1, person2 into the document store.
  ICING_ASSERT_OK(
      doc_store_->Put(document_util::CreateDocumentWrapper(person1)));
  ICING_ASSERT_OK(
      doc_store_->Put(document_util::CreateDocumentWrapper(person2)));

  // Delete person2.
  ICING_ASSERT_OK(doc_store_->Delete(
      "namespace", "person2",
      /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));

  // Evaluate email should fail since one of email's referenced documents
  // (person2) with delete propagation enabled is deleted.
  std::vector<TokenizedDocument> batch_documents_to_add;
  batch_documents_to_add.push_back(std::move(tokenized_doc_email));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor processor,
      DocumentDependencyProcessor::Create(
          doc_store_.get(), batch_documents_to_add,
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));
  EXPECT_THAT(processor.Evaluate(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("A dependency document is not alive")));
}

TEST_F(DocumentDependencyProcessorTest,
       Evaluate_withoutDeletePropagationShouldAlwaysSucceed) {
  fake_clock_.SetSystemTimeMilliseconds(500);

  // Create email document having an invalid qualified id string on "sender"
  // property with delete propagation disabled. Evaluate should succeed since
  // Icing will ignore invalid qualified id with delete propagation disabled.
  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("namespace", "email")
          .SetSchema("Email")
          .AddStringProperty("sender", "invalid_qualified_id")
          .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_email1,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), email1));
  std::vector<TokenizedDocument> batch_documents_to_add1;
  batch_documents_to_add1.push_back(std::move(tokenized_doc_email1));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor processor1,
      DocumentDependencyProcessor::Create(
          doc_store_.get(), batch_documents_to_add1,
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor::EvaluateResult result1,
      processor1.Evaluate());
  // No dependency documents out of the batch.
  EXPECT_THAT(result1.outer_dependency_document_ids, ElementsAre(IsEmpty()));
  EXPECT_THAT(result1.existing_expired_doc_ids_to_replace, IsEmpty());

  // Create email document having a valid qualified id string on "sender"
  // property with delete propagation disabled, but the referenced document
  // doesn't exist. Evaluate should succeed since Icing will ignore non-existent
  // referenced document with delete propagation disabled.
  DocumentProto email2 = DocumentBuilder()
                             .SetKey("namespace", "email")
                             .SetSchema("Email")
                             .AddStringProperty("sender", "namespace#person")
                             .Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_email2_1,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), email2));
  std::vector<TokenizedDocument> batch_documents_to_add2_1;
  batch_documents_to_add2_1.push_back(std::move(tokenized_doc_email2_1));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor processor2_1,
      DocumentDependencyProcessor::Create(
          doc_store_.get(), batch_documents_to_add2_1,
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor::EvaluateResult result2_1,
      processor2_1.Evaluate());
  // No dependency documents out of the batch.
  EXPECT_THAT(result2_1.outer_dependency_document_ids, ElementsAre(IsEmpty()));
  EXPECT_THAT(result2_1.existing_expired_doc_ids_to_replace, IsEmpty());

  // Add person document into the document store to make email2's referenced
  // document exist.
  DocumentProto person = DocumentBuilder()
                             .SetTtlMs(100)
                             .SetCreationTimestampMs(500)
                             .SetKey("namespace", "person")
                             .SetSchema("Person")
                             .AddStringProperty("Name", "Test Name")
                             .Build();
  ICING_ASSERT_OK(
      doc_store_->Put(document_util::CreateDocumentWrapper(person)));

  // Evaluate email2 again. Should succeed.
  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_email2_2,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), email2));
  std::vector<TokenizedDocument> batch_documents_to_add2_2;
  batch_documents_to_add2_2.push_back(std::move(tokenized_doc_email2_2));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor processor2_2,
      DocumentDependencyProcessor::Create(
          doc_store_.get(), batch_documents_to_add2_2,
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor::EvaluateResult result2_2,
      processor2_2.Evaluate());
  // No dependency documents out of the batch.
  EXPECT_THAT(result2_2.outer_dependency_document_ids, ElementsAre(IsEmpty()));
  EXPECT_THAT(result2_2.existing_expired_doc_ids_to_replace, IsEmpty());

  // Evaluate email2 again with a different current time which makes person
  // document expired. Since delete propagation is disabled, Evaluate should
  // still succeed.
  fake_clock_.SetSystemTimeMilliseconds(1000);

  ICING_ASSERT_OK_AND_ASSIGN(
      TokenizedDocument tokenized_doc_email2_3,
      TokenizedDocument::Create(
          schema_store_.get(), lang_segmenter_.get(),
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds(), email2));
  std::vector<TokenizedDocument> batch_documents_to_add2_3;
  batch_documents_to_add2_3.push_back(std::move(tokenized_doc_email2_3));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor processor2_3,
      DocumentDependencyProcessor::Create(
          doc_store_.get(), batch_documents_to_add2_3,
          /*current_time_ms=*/fake_clock_.GetSystemTimeMilliseconds()));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentDependencyProcessor::EvaluateResult result2_3,
      processor2_3.Evaluate());
  // No dependency documents out of the batch.
  EXPECT_THAT(result2_3.outer_dependency_document_ids, ElementsAre(IsEmpty()));
  EXPECT_THAT(result2_3.existing_expired_doc_ids_to_replace, IsEmpty());
}

}  // namespace

}  // namespace lib
}  // namespace icing
