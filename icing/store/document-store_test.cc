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

#include "icing/store/document-store.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/document-builder.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/file/mock-filesystem.h"
#include "icing/portable/equals-proto.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/crc32.h"

namespace icing {
namespace lib {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::_;
using ::testing::Eq;
using ::testing::Gt;
using ::testing::HasSubstr;
using ::testing::IsFalse;
using ::testing::IsTrue;
using ::testing::Not;
using ::testing::Return;

class DocumentStoreTest : public ::testing::Test {
 protected:
  DocumentStoreTest()
      : test_dir_(GetTestTempDir() + "/icing"),
        document_store_dir_(test_dir_ + "/document_store"),
        schema_store_dir_(test_dir_ + "/schema_store") {
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(document_store_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());
    test_document1_ =
        DocumentBuilder()
            .SetKey("icing", "email/1")
            .SetSchema("email")
            .AddStringProperty("subject", "subject foo")
            .AddStringProperty("body", "body bar")
            .SetScore(document1_score_)
            .SetCreationTimestampSecs(
                document1_creation_timestamp_)  // A random timestamp
            .SetTtlSecs(document1_ttl_)
            .Build();
    test_document2_ =
        DocumentBuilder()
            .SetKey("icing", "email/2")
            .SetSchema("email")
            .AddStringProperty("subject", "subject foo 2")
            .AddStringProperty("body", "body bar 2")
            .SetScore(document2_score_)
            .SetCreationTimestampSecs(
                document2_creation_timestamp_)  // A random timestamp
            .SetTtlSecs(document2_ttl_)
            .Build();
  }

  void SetUp() override {
    SchemaProto schema;
    auto type_config = schema.add_types();
    type_config->set_schema_type("email");

    auto subject = type_config->add_properties();
    subject->set_property_name("subject");
    subject->set_data_type(PropertyConfigProto::DataType::STRING);
    subject->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
    subject->mutable_indexing_config()->set_term_match_type(
        TermMatchType::EXACT_ONLY);
    subject->mutable_indexing_config()->set_tokenizer_type(
        IndexingConfig::TokenizerType::PLAIN);

    auto body = type_config->add_properties();
    body->set_property_name("body");
    body->set_data_type(PropertyConfigProto::DataType::STRING);
    body->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
    body->mutable_indexing_config()->set_term_match_type(
        TermMatchType::EXACT_ONLY);
    body->mutable_indexing_config()->set_tokenizer_type(
        IndexingConfig::TokenizerType::PLAIN);

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, schema_store_dir_));
    ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  const Filesystem filesystem_;
  const std::string test_dir_;
  FakeClock fake_clock_;
  const std::string document_store_dir_;
  const std::string schema_store_dir_;
  DocumentProto test_document1_;
  DocumentProto test_document2_;
  std::unique_ptr<SchemaStore> schema_store_;

  // Document1 values
  const int document1_score_ = 1;
  const int64_t document1_creation_timestamp_ = 1;
  const int64_t document1_ttl_ = 0;
  const int64_t document1_expiration_timestamp_ =
      std::numeric_limits<int64_t>::max();  // special_case where ttl=0

  // Document2 values
  const int document2_score_ = 2;
  const int64_t document2_creation_timestamp_ = 2;
  const int64_t document2_ttl_ = 1;
  const int64_t document2_expiration_timestamp_ = 3;  // creation + ttl
};

TEST_F(DocumentStoreTest, CreationWithNullPointerShouldFail) {
  EXPECT_THAT(DocumentStore::Create(/*filesystem=*/nullptr, document_store_dir_,
                                    &fake_clock_, schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));

  EXPECT_THAT(DocumentStore::Create(&filesystem_, document_store_dir_,
                                    /*clock=*/nullptr, schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));

  EXPECT_THAT(DocumentStore::Create(&filesystem_, document_store_dir_,
                                    &fake_clock_, /*schema_store=*/nullptr),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(DocumentStoreTest, CreationWithBadFilesystemShouldFail) {
  MockFilesystem mock_filesystem;
  ON_CALL(mock_filesystem, OpenForWrite(_)).WillByDefault(Return(false));

  EXPECT_THAT(DocumentStore::Create(&mock_filesystem, document_store_dir_,
                                    &fake_clock_, schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL));
}

TEST_F(DocumentStoreTest, PutAndGetInSameNamespaceOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  // Both documents have namespace of "icing"
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(test_document1_));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(DocumentProto(test_document2_)));

  EXPECT_THAT(doc_store->Get(document_id1),
              IsOkAndHolds(EqualsProto(test_document1_)));
  EXPECT_THAT(doc_store->Get(document_id2),
              IsOkAndHolds(EqualsProto(test_document2_)));
}

TEST_F(DocumentStoreTest, PutAndGetAcrossNamespacesOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  // Can handle different namespaces with same url
  DocumentProto foo_document = DocumentBuilder()
                                   .SetKey("foo", "1")
                                   .SetSchema("email")
                                   .SetCreationTimestampSecs(0)
                                   .Build();
  DocumentProto bar_document = DocumentBuilder()
                                   .SetKey("bar", "1")
                                   .SetSchema("email")
                                   .SetCreationTimestampSecs(0)
                                   .Build();

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(foo_document));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(DocumentProto(bar_document)));

  EXPECT_THAT(doc_store->Get(document_id1),
              IsOkAndHolds(EqualsProto(foo_document)));
  EXPECT_THAT(doc_store->Get(document_id2),
              IsOkAndHolds(EqualsProto(bar_document)));
}

// Validates that putting an document with the same key will overwrite previous
// document and old doc ids are not getting reused.
TEST_F(DocumentStoreTest, PutSameKey) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  // Creates two documents with the same key (namespace + uri)
  DocumentProto document1 = DocumentProto(test_document1_);
  DocumentProto document2 = DocumentProto(test_document1_);

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(document1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(document2));
  EXPECT_THAT(document_id1, Not(document_id2));
  // document2 overrides document1, so document_id1 becomes invalid
  EXPECT_THAT(doc_store->Get(document_id1),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(doc_store->Get(document_id2),
              IsOkAndHolds(EqualsProto(document2)));

  // Makes sure that old doc ids are not getting reused.
  DocumentProto document3 = DocumentProto(test_document1_);
  document3.set_uri("another/uri/1");
  EXPECT_THAT(doc_store->Put(document3), IsOkAndHolds(Not(document_id1)));
}

TEST_F(DocumentStoreTest, IsDocumentExisting) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(DocumentProto(test_document1_)));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(DocumentProto(test_document2_)));

  EXPECT_THAT(doc_store->DoesDocumentExist(document_id1), IsTrue());
  EXPECT_THAT(doc_store->DoesDocumentExist(document_id2), IsTrue());

  DocumentId invalid_document_id_negative = -1;
  EXPECT_THAT(doc_store->DoesDocumentExist(invalid_document_id_negative),
              IsFalse());

  DocumentId invalid_document_id_greater_than_max = kMaxDocumentId + 2;
  EXPECT_THAT(
      doc_store->DoesDocumentExist(invalid_document_id_greater_than_max),
      IsFalse());

  EXPECT_THAT(doc_store->DoesDocumentExist(kInvalidDocumentId), IsFalse());

  DocumentId invalid_document_id_out_of_range = document_id2 + 1;
  EXPECT_THAT(doc_store->DoesDocumentExist(invalid_document_id_out_of_range),
              IsFalse());
}

TEST_F(DocumentStoreTest, GetDeletedDocumentNotFound) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> document_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_EXPECT_OK(document_store->Put(DocumentProto(test_document1_)));
  EXPECT_THAT(
      document_store->Get(test_document1_.namespace_(), test_document1_.uri()),
      IsOkAndHolds(EqualsProto(test_document1_)));

  ICING_EXPECT_OK(document_store->Delete(test_document1_.namespace_(),
                                         test_document1_.uri()));
  EXPECT_THAT(
      document_store->Get(test_document1_.namespace_(), test_document1_.uri()),
      StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(DocumentStoreTest, GetExpiredDocumentNotFound) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("namespace", "uri")
                               .SetSchema("email")
                               .SetCreationTimestampSecs(10)
                               .SetTtlSecs(100)
                               .Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> document_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_EXPECT_OK(document_store->Put(document));
  EXPECT_THAT(document_store->Get("namespace", "uri"),
              IsOkAndHolds(EqualsProto(document)));

  // Some arbitrary time before the document's creation time (10) + ttl (100)
  fake_clock_.SetSeconds(109);
  EXPECT_THAT(document_store->Get("namespace", "uri"),
              IsOkAndHolds(EqualsProto(document)));

  // Some arbitrary time equal to the document's creation time (10) + ttl (100)
  fake_clock_.SetSeconds(110);
  EXPECT_THAT(document_store->Get("namespace", "uri"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  // Some arbitrary time past the document's creation time (10) + ttl (100)
  fake_clock_.SetSeconds(200);
  EXPECT_THAT(document_store->Get("namespace", "uri"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(DocumentStoreTest, GetInvalidDocumentId) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             doc_store->Put(DocumentProto(test_document1_)));

  DocumentId invalid_document_id_negative = -1;
  EXPECT_THAT(doc_store->Get(invalid_document_id_negative),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  DocumentId invalid_document_id_greater_than_max = kMaxDocumentId + 2;
  EXPECT_THAT(doc_store->Get(invalid_document_id_greater_than_max),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  EXPECT_THAT(doc_store->Get(kInvalidDocumentId),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  DocumentId invalid_document_id_out_of_range = document_id + 1;
  EXPECT_THAT(doc_store->Get(invalid_document_id_out_of_range),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(DocumentStoreTest, Delete) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  // Get() after Delete() returns NOT_FOUND
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             doc_store->Put(DocumentProto(test_document1_)));
  EXPECT_THAT(doc_store->Delete("icing", "email/1"), IsOk());
  EXPECT_THAT(doc_store->Get(document_id),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  // Validates that deleting something non-existing won't append anything to
  // ground truth
  int64_t ground_truth_size_before = filesystem_.GetFileSize(
      absl_ports::StrCat(document_store_dir_, "/document_log").c_str());
  // icing + email/1 has already been deleted.
  EXPECT_THAT(doc_store->Delete("icing", "email/1"), IsOk());
  int64_t ground_truth_size_after = filesystem_.GetFileSize(
      absl_ports::StrCat(document_store_dir_, "/document_log").c_str());
  EXPECT_THAT(ground_truth_size_before, Eq(ground_truth_size_after));
}

TEST_F(DocumentStoreTest, DeleteByNamespaceOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  DocumentProto document1 = test_document1_;
  document1.set_namespace_("namespace.1");
  document1.set_uri("uri1");
  ICING_ASSERT_OK(doc_store->Put(document1));

  DocumentProto document2 = test_document1_;
  document2.set_namespace_("namespace.2");
  document2.set_uri("uri1");
  ICING_ASSERT_OK(doc_store->Put(document2));

  DocumentProto document3 = test_document1_;
  document3.set_namespace_("namespace.3");
  document3.set_uri("uri1");
  ICING_ASSERT_OK(doc_store->Put(document3));

  DocumentProto document4 = test_document1_;
  document4.set_namespace_("namespace.1");
  document4.set_uri("uri2");
  ICING_ASSERT_OK(doc_store->Put(document4));

  // DELETE namespace.1. document1 and document 4 should be deleted. document2
  // and document3 should still be retrievable.
  ICING_EXPECT_OK(doc_store->DeleteByNamespace("namespace.1"));
  EXPECT_THAT(doc_store->Get(document1.namespace_(), document1.uri()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(doc_store->Get(document2.namespace_(), document2.uri()),
              IsOkAndHolds(EqualsProto(document2)));
  EXPECT_THAT(doc_store->Get(document3.namespace_(), document3.uri()),
              IsOkAndHolds(EqualsProto(document3)));
  EXPECT_THAT(doc_store->Get(document4.namespace_(), document4.uri()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(DocumentStoreTest, DeleteByNamespaceNonexistentNamespaceOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  // Validates that deleting something non-existing won't append anything to
  // ground truth
  int64_t ground_truth_size_before = filesystem_.GetFileSize(
      absl_ports::StrCat(document_store_dir_, "/document_log").c_str());

  ICING_EXPECT_OK(doc_store->DeleteByNamespace("nonexistent_namespace"));

  int64_t ground_truth_size_after = filesystem_.GetFileSize(
      absl_ports::StrCat(document_store_dir_, "/document_log").c_str());
  EXPECT_THAT(ground_truth_size_before, Eq(ground_truth_size_after));
}

TEST_F(DocumentStoreTest, DeleteByNamespaceRecoversOk) {
  DocumentProto document1 = test_document1_;
  document1.set_namespace_("namespace.1");
  document1.set_uri("uri1");

  DocumentProto document2 = test_document1_;
  document2.set_namespace_("namespace.2");
  document2.set_uri("uri1");

  DocumentProto document3 = test_document1_;
  document3.set_namespace_("namespace.3");
  document3.set_uri("uri1");

  DocumentProto document4 = test_document1_;
  document4.set_namespace_("namespace.1");
  document4.set_uri("uri2");

  int64_t ground_truth_size_before;
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DocumentStore> doc_store,
        DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                              schema_store_.get()));
    ICING_ASSERT_OK(doc_store->Put(document1));
    ICING_ASSERT_OK(doc_store->Put(document2));
    ICING_ASSERT_OK(doc_store->Put(document3));
    ICING_ASSERT_OK(doc_store->Put(document4));

    // DELETE namespace.1. document1 and document 4 should be deleted. document2
    // and document3 should still be retrievable.
    ICING_EXPECT_OK(doc_store->DeleteByNamespace("namespace.1"));

    ground_truth_size_before = filesystem_.GetFileSize(
        absl_ports::StrCat(document_store_dir_, "/document_log").c_str());
  }  // Destructors should update checksum and persist all data to file.

  // Change the DocStore's header combined checksum so that it won't match the
  // recalculated checksum on initialization. This will force a regeneration of
  // derived files from ground truth.
  const std::string header_file =
      absl_ports::StrCat(document_store_dir_, "/document_store_header");
  DocumentStore::Header header;
  header.magic = DocumentStore::Header::kMagic;
  header.checksum = 10;  // Arbitrary garbage checksum
  filesystem_.DeleteFile(header_file.c_str());
  filesystem_.Write(header_file.c_str(), &header, sizeof(header));

  // Successfully recover from a corrupt derived file issue.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  // Make sure we didn't add anything to the ground truth after we recovered.
  int64_t ground_truth_size_after = filesystem_.GetFileSize(
      absl_ports::StrCat(document_store_dir_, "/document_log").c_str());
  EXPECT_EQ(ground_truth_size_before, ground_truth_size_after);

  EXPECT_THAT(doc_store->Get(document1.namespace_(), document1.uri()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(doc_store->Get(document2.namespace_(), document2.uri()),
              IsOkAndHolds(EqualsProto(document2)));
  EXPECT_THAT(doc_store->Get(document3.namespace_(), document3.uri()),
              IsOkAndHolds(EqualsProto(document3)));
  EXPECT_THAT(doc_store->Get(document4.namespace_(), document4.uri()),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(DocumentStoreTest, DeleteBySchemaTypeOk) {
  SchemaProto schema;
  auto type_config = schema.add_types();
  type_config->set_schema_type("email");
  type_config = schema.add_types();
  type_config->set_schema_type("message");
  type_config = schema.add_types();
  type_config->set_schema_type("person");

  std::string schema_store_dir = schema_store_dir_ + "_custom";
  filesystem_.DeleteDirectoryRecursively(schema_store_dir.c_str());
  filesystem_.CreateDirectoryRecursively(schema_store_dir.c_str());
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir));

  ICING_ASSERT_OK(schema_store->SetSchema(schema));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> document_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store.get()));

  DocumentProto email_document_1 = DocumentBuilder()
                                       .SetKey("namespace1", "1")
                                       .SetSchema("email")
                                       .SetCreationTimestampSecs(1)
                                       .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_1_document_id,
                             document_store->Put(email_document_1));

  DocumentProto email_document_2 = DocumentBuilder()
                                       .SetKey("namespace2", "2")
                                       .SetSchema("email")
                                       .SetCreationTimestampSecs(1)
                                       .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_2_document_id,
                             document_store->Put(email_document_2));

  DocumentProto message_document = DocumentBuilder()
                                       .SetKey("namespace", "3")
                                       .SetSchema("message")
                                       .SetCreationTimestampSecs(1)
                                       .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message_document_id,
                             document_store->Put(message_document));

  DocumentProto person_document = DocumentBuilder()
                                      .SetKey("namespace", "4")
                                      .SetSchema("person")
                                      .SetCreationTimestampSecs(1)
                                      .Build();
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId person_document_id,
                             document_store->Put(person_document));

  // Delete the "email" type and ensure that it works across both
  // email_document's namespaces. And that other documents aren't affected.
  ICING_EXPECT_OK(document_store->DeleteBySchemaType("email"));
  EXPECT_THAT(document_store->Get(email_1_document_id),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(document_store->Get(email_2_document_id),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(document_store->Get(message_document_id),
              IsOkAndHolds(EqualsProto(message_document)));
  EXPECT_THAT(document_store->Get(person_document_id),
              IsOkAndHolds(EqualsProto(person_document)));

  // Delete the "message" type and check that other documents aren't affected
  ICING_EXPECT_OK(document_store->DeleteBySchemaType("message"));
  EXPECT_THAT(document_store->Get(email_1_document_id),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(document_store->Get(email_2_document_id),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(document_store->Get(message_document_id),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(document_store->Get(person_document_id),
              IsOkAndHolds(EqualsProto(person_document)));
}

TEST_F(DocumentStoreTest, DeleteBySchemaTypeNonexistentSchemaTypeOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> document_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  // Validates that deleting something non-existing won't append anything to
  // ground truth
  int64_t ground_truth_size_before = filesystem_.GetFileSize(
      absl_ports::StrCat(document_store_dir_, "/document_log").c_str());

  ICING_EXPECT_OK(document_store->DeleteBySchemaType("nonexistent_type"));

  int64_t ground_truth_size_after = filesystem_.GetFileSize(
      absl_ports::StrCat(document_store_dir_, "/document_log").c_str());

  EXPECT_THAT(ground_truth_size_before, Eq(ground_truth_size_after));
}

TEST_F(DocumentStoreTest, DeleteBySchemaTypeRecoversOk) {
  SchemaProto schema;
  auto type_config = schema.add_types();
  type_config->set_schema_type("email");
  type_config = schema.add_types();
  type_config->set_schema_type("message");

  std::string schema_store_dir = schema_store_dir_ + "_custom";
  filesystem_.DeleteDirectoryRecursively(schema_store_dir.c_str());
  filesystem_.CreateDirectoryRecursively(schema_store_dir.c_str());
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir));

  ICING_ASSERT_OK(schema_store->SetSchema(schema));

  DocumentId email_document_id;
  DocumentId message_document_id;

  DocumentProto email_document = DocumentBuilder()
                                     .SetKey("namespace", "1")
                                     .SetSchema("email")
                                     .SetCreationTimestampSecs(1)
                                     .Build();

  DocumentProto message_document = DocumentBuilder()
                                       .SetKey("namespace", "2")
                                       .SetSchema("message")
                                       .SetCreationTimestampSecs(1)
                                       .Build();
  int64_t ground_truth_size_before;
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DocumentStore> document_store,
        DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                              schema_store.get()));

    ICING_ASSERT_OK_AND_ASSIGN(email_document_id,
                               document_store->Put(email_document));
    ICING_ASSERT_OK_AND_ASSIGN(message_document_id,
                               document_store->Put(message_document));

    // Delete "email". "message" documents should still be retrievable.
    ICING_EXPECT_OK(document_store->DeleteBySchemaType("email"));

    ground_truth_size_before = filesystem_.GetFileSize(
        absl_ports::StrCat(document_store_dir_, "/document_log").c_str());
  }  // Destructors should update checksum and persist all data to file.

  // Change the DocumentStore's header combined checksum so that it won't match
  // the recalculated checksum on initialization. This will force a regeneration
  // of derived files from ground truth.
  const std::string header_file =
      absl_ports::StrCat(document_store_dir_, "/document_store_header");
  DocumentStore::Header header;
  header.magic = DocumentStore::Header::kMagic;
  header.checksum = 10;  // Arbitrary garbage checksum
  filesystem_.DeleteFile(header_file.c_str());
  filesystem_.Write(header_file.c_str(), &header, sizeof(header));

  // Successfully recover from a corrupt derived file issue.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> document_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store.get()));

  // Make sure we didn't add anything to the ground truth after we recovered.
  int64_t ground_truth_size_after = filesystem_.GetFileSize(
      absl_ports::StrCat(document_store_dir_, "/document_log").c_str());
  EXPECT_EQ(ground_truth_size_before, ground_truth_size_after);

  EXPECT_THAT(document_store->Get(email_document_id),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(document_store->Get(message_document_id),
              IsOkAndHolds(EqualsProto(message_document)));
}

TEST_F(DocumentStoreTest, DeletedSchemaTypeFromSchemaStoreRecoversOk) {
  SchemaProto schema;
  auto type_config = schema.add_types();
  type_config->set_schema_type("email");
  type_config = schema.add_types();
  type_config->set_schema_type("message");

  std::string schema_store_dir = schema_store_dir_ + "_custom";
  filesystem_.DeleteDirectoryRecursively(schema_store_dir.c_str());
  filesystem_.CreateDirectoryRecursively(schema_store_dir.c_str());
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir));

  ICING_ASSERT_OK(schema_store->SetSchema(schema));

  DocumentId email_document_id;
  DocumentId message_document_id;

  DocumentProto email_document = DocumentBuilder()
                                     .SetKey("namespace", "email")
                                     .SetSchema("email")
                                     .SetCreationTimestampSecs(1)
                                     .Build();

  DocumentProto message_document = DocumentBuilder()
                                       .SetKey("namespace", "message")
                                       .SetSchema("message")
                                       .SetCreationTimestampSecs(1)
                                       .Build();
  int64_t ground_truth_size_before;
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DocumentStore> document_store,
        DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                              schema_store.get()));

    ICING_ASSERT_OK_AND_ASSIGN(email_document_id,
                               document_store->Put(email_document));
    ICING_ASSERT_OK_AND_ASSIGN(message_document_id,
                               document_store->Put(message_document));

    // Delete "email". "message" documents should still be retrievable.
    ICING_EXPECT_OK(document_store->DeleteBySchemaType("email"));

    EXPECT_THAT(document_store->Get(email_document_id),
                StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
    EXPECT_THAT(document_store->Get(message_document_id),
                IsOkAndHolds(EqualsProto(message_document)));

    ground_truth_size_before = filesystem_.GetFileSize(
        absl_ports::StrCat(document_store_dir_, "/document_log").c_str());
  }  // Destructors should update checksum and persist all data to file.

  // Change the DocumentStore's header combined checksum so that it won't match
  // the recalculated checksum on initialization. This will force a regeneration
  // of derived files from ground truth.
  const std::string header_file =
      absl_ports::StrCat(document_store_dir_, "/document_store_header");
  DocumentStore::Header header;
  header.magic = DocumentStore::Header::kMagic;
  header.checksum = 10;  // Arbitrary garbage checksum
  filesystem_.DeleteFile(header_file.c_str());
  filesystem_.Write(header_file.c_str(), &header, sizeof(header));

  SchemaProto new_schema;
  type_config = new_schema.add_types();
  type_config->set_schema_type("message");

  ICING_EXPECT_OK(schema_store->SetSchema(
      new_schema, /*ignore_errors_and_delete_documents=*/true));

  // Successfully recover from a corrupt derived file issue.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> document_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store.get()));

  // Make sure we didn't add anything to the ground truth after we recovered.
  int64_t ground_truth_size_after = filesystem_.GetFileSize(
      absl_ports::StrCat(document_store_dir_, "/document_log").c_str());
  EXPECT_EQ(ground_truth_size_before, ground_truth_size_after);

  EXPECT_THAT(document_store->Get(email_document_id),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(document_store->Get(message_document_id),
              IsOkAndHolds(EqualsProto(message_document)));
}

TEST_F(DocumentStoreTest, OptimizeInto) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace", "uri1")
                                .SetSchema("email")
                                .SetCreationTimestampSecs(100)
                                .SetTtlSecs(1000)
                                .Build();

  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace", "uri2")
                                .SetSchema("email")
                                .SetCreationTimestampSecs(100)
                                .SetTtlSecs(1000)
                                .Build();

  DocumentProto document3 = DocumentBuilder()
                                .SetKey("namespace", "uri3")
                                .SetSchema("email")
                                .SetCreationTimestampSecs(100)
                                .SetTtlSecs(100)
                                .Build();

  // Nothing should have expired yet.
  fake_clock_.SetSeconds(100);

  ICING_ASSERT_OK(doc_store->Put(document1));
  ICING_ASSERT_OK(doc_store->Put(document2));
  ICING_ASSERT_OK(doc_store->Put(document3));

  std::string original_document_log = document_store_dir_ + "/document_log";
  int64_t original_size =
      filesystem_.GetFileSize(original_document_log.c_str());

  // Optimizing into the same directory is not allowed
  EXPECT_THAT(doc_store->OptimizeInto(document_store_dir_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("directory is the same")));

  std::string optimized_dir = document_store_dir_ + "_optimize";
  std::string optimized_document_log = optimized_dir + "/document_log";

  // Validates that the optimized document log has the same size if nothing is
  // deleted
  ASSERT_TRUE(filesystem_.DeleteDirectoryRecursively(optimized_dir.c_str()));
  ASSERT_TRUE(filesystem_.CreateDirectoryRecursively(optimized_dir.c_str()));
  ICING_ASSERT_OK(doc_store->OptimizeInto(optimized_dir));
  int64_t optimized_size1 =
      filesystem_.GetFileSize(optimized_document_log.c_str());
  EXPECT_EQ(original_size, optimized_size1);

  // Validates that the optimized document log has a smaller size if something
  // is deleted
  ASSERT_TRUE(filesystem_.DeleteDirectoryRecursively(optimized_dir.c_str()));
  ASSERT_TRUE(filesystem_.CreateDirectoryRecursively(optimized_dir.c_str()));
  ICING_ASSERT_OK(doc_store->Delete("namespace", "uri1"));
  ICING_ASSERT_OK(doc_store->OptimizeInto(optimized_dir));
  int64_t optimized_size2 =
      filesystem_.GetFileSize(optimized_document_log.c_str());
  EXPECT_THAT(original_size, Gt(optimized_size2));

  // Document3 has expired since this is past its creation (100) + ttl (100).
  // But document1 and document2 should be fine since their ttl's were 1000.
  fake_clock_.SetSeconds(300);

  // Validates that the optimized document log has a smaller size if something
  // expired
  ASSERT_TRUE(filesystem_.DeleteDirectoryRecursively(optimized_dir.c_str()));
  ASSERT_TRUE(filesystem_.CreateDirectoryRecursively(optimized_dir.c_str()));
  ICING_ASSERT_OK(doc_store->OptimizeInto(optimized_dir));
  int64_t optimized_size3 =
      filesystem_.GetFileSize(optimized_document_log.c_str());
  EXPECT_THAT(optimized_size2, Gt(optimized_size3));
}

TEST_F(DocumentStoreTest, ShouldRecoverFromDataLoss) {
  DocumentId document_id1, document_id2;
  {
    // Can put and delete fine.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DocumentStore> doc_store,
        DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                              schema_store_.get()));
    ICING_ASSERT_OK_AND_ASSIGN(document_id1,
                               doc_store->Put(DocumentProto(test_document1_)));
    ICING_ASSERT_OK_AND_ASSIGN(document_id2,
                               doc_store->Put(DocumentProto(test_document2_)));
    EXPECT_THAT(doc_store->Get(document_id1),
                IsOkAndHolds(EqualsProto(test_document1_)));
    EXPECT_THAT(doc_store->Get(document_id2),
                IsOkAndHolds(EqualsProto(test_document2_)));
    EXPECT_THAT(doc_store->Delete("icing", "email/1"), IsOk());
    EXPECT_THAT(doc_store->Get(document_id1),
                StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
    EXPECT_THAT(doc_store->Get(document_id2),
                IsOkAndHolds(EqualsProto(test_document2_)));
  }

  // "Corrupt" the content written in the log by adding non-checksummed data to
  // it. This will mess up the checksum of the proto log, forcing it to rewind
  // to the last saved point.
  DocumentProto document = DocumentBuilder().SetKey("namespace", "uri").Build();
  const std::string serialized_document = document.SerializeAsString();

  const std::string document_log_file =
      absl_ports::StrCat(document_store_dir_, "/document_log");
  int64_t file_size = filesystem_.GetFileSize(document_log_file.c_str());
  filesystem_.PWrite(document_log_file.c_str(), file_size,
                     serialized_document.data(), serialized_document.size());

  // Successfully recover from a data loss issue.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));
  EXPECT_THAT(doc_store->Get(document_id1),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(doc_store->Get(document_id2),
              IsOkAndHolds(EqualsProto(test_document2_)));

  // Checks derived filter cache
  EXPECT_THAT(doc_store->GetDocumentFilterData(document_id2),
              IsOkAndHolds(DocumentFilterData(
                  /*namespace_id=*/0,
                  /*schema_type_id=*/0, document2_expiration_timestamp_)));
  // Checks derived score cache
  EXPECT_THAT(doc_store->GetDocumentAssociatedScoreData(document_id2),
              IsOkAndHolds(DocumentAssociatedScoreData(
                  document2_score_, document2_creation_timestamp_)));
}

TEST_F(DocumentStoreTest, ShouldRecoverFromCorruptDerivedFile) {
  DocumentId document_id1, document_id2;
  {
    // Can put and delete fine.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DocumentStore> doc_store,
        DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                              schema_store_.get()));
    ICING_ASSERT_OK_AND_ASSIGN(document_id1,
                               doc_store->Put(DocumentProto(test_document1_)));
    ICING_ASSERT_OK_AND_ASSIGN(document_id2,
                               doc_store->Put(DocumentProto(test_document2_)));
    EXPECT_THAT(doc_store->Get(document_id1),
                IsOkAndHolds(EqualsProto(test_document1_)));
    EXPECT_THAT(doc_store->Get(document_id2),
                IsOkAndHolds(EqualsProto(test_document2_)));
    EXPECT_THAT(doc_store->Delete("icing", "email/1"), IsOk());
    EXPECT_THAT(doc_store->Get(document_id1),
                StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
    EXPECT_THAT(doc_store->Get(document_id2),
                IsOkAndHolds(EqualsProto(test_document2_)));
  }

  // "Corrupt" one of the derived files by adding non-checksummed data to
  // it. This will mess up the checksum and throw an error on the derived file's
  // initialization.
  const std::string document_id_mapper_file =
      absl_ports::StrCat(document_store_dir_, "/document_id_mapper");
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<FileBackedVector<int64_t>> document_id_mapper,
      FileBackedVector<int64_t>::Create(
          filesystem_, document_id_mapper_file,
          MemoryMappedFile::READ_WRITE_AUTO_SYNC));
  int64_t corrupt_document_id = 3;
  int64_t corrupt_offset = 3;
  EXPECT_THAT(document_id_mapper->Set(corrupt_document_id, corrupt_offset),
              IsOk());

  // Successfully recover from a corrupt derived file issue.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));
  EXPECT_THAT(doc_store->Get(document_id1),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(doc_store->Get(document_id2),
              IsOkAndHolds(EqualsProto(test_document2_)));

  // Checks derived filter cache
  EXPECT_THAT(doc_store->GetDocumentFilterData(document_id2),
              IsOkAndHolds(DocumentFilterData(
                  /*namespace_id=*/0,
                  /*schema_type_id=*/0, document2_expiration_timestamp_)));
  // Checks derived score cache
  EXPECT_THAT(doc_store->GetDocumentAssociatedScoreData(document_id2),
              IsOkAndHolds(DocumentAssociatedScoreData(
                  document2_score_, document2_creation_timestamp_)));
}

TEST_F(DocumentStoreTest, ShouldRecoverFromBadChecksum) {
  DocumentId document_id1, document_id2;
  {
    // Can put and delete fine.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DocumentStore> doc_store,
        DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                              schema_store_.get()));
    ICING_ASSERT_OK_AND_ASSIGN(document_id1,
                               doc_store->Put(DocumentProto(test_document1_)));
    ICING_ASSERT_OK_AND_ASSIGN(document_id2,
                               doc_store->Put(DocumentProto(test_document2_)));
    EXPECT_THAT(doc_store->Get(document_id1),
                IsOkAndHolds(EqualsProto(test_document1_)));
    EXPECT_THAT(doc_store->Get(document_id2),
                IsOkAndHolds(EqualsProto(test_document2_)));
    EXPECT_THAT(doc_store->Delete("icing", "email/1"), IsOk());
    EXPECT_THAT(doc_store->Get(document_id1),
                StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
    EXPECT_THAT(doc_store->Get(document_id2),
                IsOkAndHolds(EqualsProto(test_document2_)));
  }

  // Change the DocStore's header combined checksum so that it won't match the
  // recalculated checksum on initialization. This will force a regeneration of
  // derived files from ground truth.
  const std::string header_file =
      absl_ports::StrCat(document_store_dir_, "/document_store_header");
  DocumentStore::Header header;
  header.magic = DocumentStore::Header::kMagic;
  header.checksum = 10;  // Arbitrary garbage checksum
  filesystem_.DeleteFile(header_file.c_str());
  filesystem_.Write(header_file.c_str(), &header, sizeof(header));

  // Successfully recover from a corrupt derived file issue.
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));
  EXPECT_THAT(doc_store->Get(document_id1),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(doc_store->Get(document_id2),
              IsOkAndHolds(EqualsProto(test_document2_)));

  // Checks derived filter cache
  EXPECT_THAT(doc_store->GetDocumentFilterData(document_id2),
              IsOkAndHolds(DocumentFilterData(
                  /*namespace_id=*/0,
                  /*schema_type_id=*/0, document2_expiration_timestamp_)));
  // Checks derived score cache
  EXPECT_THAT(doc_store->GetDocumentAssociatedScoreData(document_id2),
              IsOkAndHolds(DocumentAssociatedScoreData(
                  document2_score_, document2_creation_timestamp_)));
}

TEST_F(DocumentStoreTest, GetDiskUsage) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));
  ICING_ASSERT_OK_AND_ASSIGN(int64_t size1, doc_store->GetDiskUsage());
  EXPECT_THAT(size1, Gt(0));

  ICING_ASSERT_OK(doc_store->Put(test_document1_));
  ICING_ASSERT_OK_AND_ASSIGN(int64_t size2, doc_store->GetDiskUsage());
  EXPECT_THAT(size2, Gt(size1));

  ICING_ASSERT_OK(doc_store->Put(test_document2_));
  EXPECT_THAT(doc_store->GetDiskUsage(), IsOkAndHolds(Gt(size2)));
  doc_store.reset();

  // Bad file system
  MockFilesystem mock_filesystem;
  ON_CALL(mock_filesystem, GetDiskUsage(A<const char *>()))
      .WillByDefault(Return(Filesystem::kBadFileSize));
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store_with_mock_filesystem,
      DocumentStore::Create(&mock_filesystem, document_store_dir_, &fake_clock_,
                            schema_store_.get()));
  EXPECT_THAT(doc_store_with_mock_filesystem->GetDiskUsage(),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL));
}

TEST_F(DocumentStoreTest, MaxDocumentId) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  // Since the DocumentStore is empty, we get an invalid DocumentId
  EXPECT_THAT(doc_store->last_added_document_id(), Eq(kInvalidDocumentId));

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(DocumentProto(test_document1_)));
  EXPECT_THAT(doc_store->last_added_document_id(), Eq(document_id1));

  // Still returns the last DocumentId even if it was deleted
  ICING_ASSERT_OK(doc_store->Delete("icing", "email/1"));
  EXPECT_THAT(doc_store->last_added_document_id(), Eq(document_id1));

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(DocumentProto(test_document2_)));
  EXPECT_THAT(doc_store->last_added_document_id(), Eq(document_id2));
}

TEST_F(DocumentStoreTest, GetNamespaceId) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  DocumentProto document_namespace1 =
      DocumentBuilder().SetKey("namespace1", "1").SetSchema("email").Build();
  DocumentProto document_namespace2 =
      DocumentBuilder().SetKey("namespace2", "2").SetSchema("email").Build();

  ICING_ASSERT_OK(doc_store->Put(DocumentProto(document_namespace1)));
  ICING_ASSERT_OK(doc_store->Put(DocumentProto(document_namespace2)));

  // NamespaceId of 0 since it was the first namespace seen by the DocumentStore
  EXPECT_THAT(doc_store->GetNamespaceId("namespace1"), IsOkAndHolds(Eq(0)));

  // NamespaceId of 1 since it was the second namespace seen by the
  // DocumentStore
  EXPECT_THAT(doc_store->GetNamespaceId("namespace2"), IsOkAndHolds(Eq(1)));

  // NamespaceMapper doesn't care if the document has been deleted
  EXPECT_THAT(doc_store->GetNamespaceId("namespace1"), IsOkAndHolds(Eq(0)));
}

TEST_F(DocumentStoreTest, GetDuplicateNamespaceId) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  DocumentProto document1 =
      DocumentBuilder().SetKey("namespace", "1").SetSchema("email").Build();
  DocumentProto document2 =
      DocumentBuilder().SetKey("namespace", "2").SetSchema("email").Build();

  ICING_ASSERT_OK(doc_store->Put(document1));
  ICING_ASSERT_OK(doc_store->Put(document2));

  // NamespaceId of 0 since it was the first namespace seen by the DocumentStore
  EXPECT_THAT(doc_store->GetNamespaceId("namespace"), IsOkAndHolds(Eq(0)));
}

TEST_F(DocumentStoreTest, NonexistentNamespaceNotFound) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  EXPECT_THAT(doc_store->GetNamespaceId("nonexistent_namespace"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(DocumentStoreTest, FilterCacheHoldsDeletedDocumentData) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id,
                             doc_store->Put(test_document1_));

  EXPECT_THAT(
      doc_store->GetDocumentFilterData(document_id),
      IsOkAndHolds(DocumentFilterData(
          /*namespace_id=*/0,
          /*schema_type_id=*/0,
          /*expiration_timestamp_secs=*/document1_expiration_timestamp_)));

  // FilterCache doesn't care if the document has been deleted
  ICING_ASSERT_OK(doc_store->Delete("icing", "email/1"));
  EXPECT_THAT(
      doc_store->GetDocumentFilterData(document_id),
      IsOkAndHolds(DocumentFilterData(
          /*namespace_id=*/0,
          /*schema_type_id=*/0,
          /*expiration_timestamp_secs=*/document1_expiration_timestamp_)));
}

TEST_F(DocumentStoreTest,
       ExpirationTimestampIsSumOfNonZeroTtlAndCreationTimestamp) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("namespace1", "1")
                               .SetSchema("email")
                               .SetCreationTimestampSecs(100)
                               .SetTtlSecs(1000)
                               .Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id, doc_store->Put(document));

  EXPECT_THAT(
      doc_store->GetDocumentFilterData(document_id),
      IsOkAndHolds(DocumentFilterData(/*namespace_id=*/0,
                                      /*schema_type_id=*/0,
                                      /*expiration_timestamp_secs=*/1100)));
}

TEST_F(DocumentStoreTest, ExpirationTimestampIsInt64MaxIfTtlIsZero) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("namespace1", "1")
                               .SetSchema("email")
                               .SetCreationTimestampSecs(100)
                               .SetTtlSecs(0)
                               .Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id, doc_store->Put(document));

  EXPECT_THAT(
      doc_store->GetDocumentFilterData(document_id),
      IsOkAndHolds(DocumentFilterData(
          /*namespace_id=*/0,
          /*schema_type_id=*/0,
          /*expiration_timestamp_secs=*/std::numeric_limits<int64_t>::max())));
}

TEST_F(DocumentStoreTest, ExpirationTimestampIsInt64MaxOnOverflow) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("namespace1", "1")
          .SetSchema("email")
          .SetCreationTimestampSecs(std::numeric_limits<int64_t>::max() - 1)
          .SetTtlSecs(std::numeric_limits<int64_t>::max() - 1)
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id, doc_store->Put(document));

  EXPECT_THAT(
      doc_store->GetDocumentFilterData(document_id),
      IsOkAndHolds(DocumentFilterData(
          /*namespace_id=*/0,
          /*schema_type_id=*/0,
          /*expiration_timestamp_secs=*/std::numeric_limits<int64_t>::max())));
}

TEST_F(DocumentStoreTest, CreationTimestampShouldBePopulated) {
  // Creates a document without a given creation timestamp
  DocumentProto document_without_creation_timestamp =
      DocumentBuilder()
          .SetKey("icing", "email/1")
          .SetSchema("email")
          .AddStringProperty("subject", "subject foo")
          .AddStringProperty("body", "body bar")
          .Build();

  std::time_t fake_real_time = 100;
  fake_clock_.SetSeconds(fake_real_time);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentId document_id,
      doc_store->Put(document_without_creation_timestamp));

  ICING_ASSERT_OK_AND_ASSIGN(DocumentProto document_with_creation_timestamp,
                             doc_store->Get(document_id));

  // Now the creation timestamp should be set by document store.
  EXPECT_THAT(document_with_creation_timestamp.creation_timestamp_secs(),
              Eq(fake_real_time));
}

TEST_F(DocumentStoreTest, ShouldWriteAndReadScoresCorrectly) {
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("icing", "email/1")
                                .SetSchema("email")
                                .AddStringProperty("subject", "subject foo")
                                // With default doc score 0
                                .Build();
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("icing", "email/1")
                                .SetSchema("email")
                                .AddStringProperty("subject", "subject foo")
                                .SetScore(5)
                                .Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> doc_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id1,
                             doc_store->Put(document1));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId document_id2,
                             doc_store->Put(document2));

  EXPECT_THAT(doc_store->GetDocumentAssociatedScoreData(document_id1),
              IsOkAndHolds(DocumentAssociatedScoreData(
                  /*document_score=*/0, /*creation_timestamp_secs=*/0)));

  EXPECT_THAT(doc_store->GetDocumentAssociatedScoreData(document_id2),
              IsOkAndHolds(DocumentAssociatedScoreData(
                  /*document_score=*/5, /*creation_timestamp_secs=*/0)));
}

TEST_F(DocumentStoreTest, ComputeChecksumSameBetweenCalls) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> document_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  ICING_EXPECT_OK(document_store->Put(test_document1_));
  ICING_ASSERT_OK_AND_ASSIGN(Crc32 checksum, document_store->ComputeChecksum());

  // Calling ComputeChecksum again shouldn't change anything
  EXPECT_THAT(document_store->ComputeChecksum(), IsOkAndHolds(checksum));
}

TEST_F(DocumentStoreTest, ComputeChecksumSameAcrossInstances) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> document_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  ICING_EXPECT_OK(document_store->Put(test_document1_));
  ICING_ASSERT_OK_AND_ASSIGN(Crc32 checksum, document_store->ComputeChecksum());

  // Destroy the previous instance and recreate DocumentStore
  document_store.reset();
  ICING_ASSERT_OK_AND_ASSIGN(
      document_store, DocumentStore::Create(&filesystem_, document_store_dir_,
                                            &fake_clock_, schema_store_.get()));

  EXPECT_THAT(document_store->ComputeChecksum(), IsOkAndHolds(checksum));
}

TEST_F(DocumentStoreTest, ComputeChecksumChangesOnModification) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> document_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store_.get()));

  ICING_EXPECT_OK(document_store->Put(test_document1_));
  ICING_ASSERT_OK_AND_ASSIGN(Crc32 checksum, document_store->ComputeChecksum());

  ICING_EXPECT_OK(document_store->Put(test_document2_));
  EXPECT_THAT(document_store->ComputeChecksum(),
              IsOkAndHolds(Not(Eq(checksum))));
}

TEST_F(DocumentStoreTest, RegenerateDerivedFilesSkipsUnknownSchemaTypeIds) {
  const std::string schema_store_dir = schema_store_dir_ + "_custom";

  DocumentId email_document_id;
  NamespaceId email_namespace_id;
  int64_t email_expiration_timestamp;
  DocumentProto email_document = DocumentBuilder()
                                     .SetKey("namespace", "email_uri")
                                     .SetSchema("email")
                                     .SetCreationTimestampSecs(0)
                                     .Build();

  DocumentId message_document_id;
  NamespaceId message_namespace_id;
  int64_t message_expiration_timestamp;
  DocumentProto message_document = DocumentBuilder()
                                       .SetKey("namespace", "message_uri")
                                       .SetSchema("message")
                                       .SetCreationTimestampSecs(0)
                                       .Build();

  {
    // Set a schema with "email" and "message"
    filesystem_.DeleteDirectoryRecursively(schema_store_dir.c_str());
    filesystem_.CreateDirectoryRecursively(schema_store_dir.c_str());
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir));
    SchemaProto schema;
    auto type_config = schema.add_types();
    type_config->set_schema_type("email");
    type_config = schema.add_types();
    type_config->set_schema_type("message");
    ICING_EXPECT_OK(schema_store->SetSchema(schema));

    ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId email_schema_type_id,
                               schema_store->GetSchemaTypeId("email"));
    ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId message_schema_type_id,
                               schema_store->GetSchemaTypeId("message"));

    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DocumentStore> document_store,
        DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                              schema_store.get()));

    // Insert and verify a "email "document
    ICING_ASSERT_OK_AND_ASSIGN(
        email_document_id, document_store->Put(DocumentProto(email_document)));
    EXPECT_THAT(document_store->Get(email_document_id),
                IsOkAndHolds(EqualsProto(email_document)));
    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentFilterData email_data,
        document_store->GetDocumentFilterData(email_document_id));
    EXPECT_THAT(email_data.schema_type_id(), Eq(email_schema_type_id));
    email_namespace_id = email_data.namespace_id();
    email_expiration_timestamp = email_data.expiration_timestamp_secs();

    // Insert and verify a "message" document
    ICING_ASSERT_OK_AND_ASSIGN(
        message_document_id,
        document_store->Put(DocumentProto(message_document)));
    EXPECT_THAT(document_store->Get(message_document_id),
                IsOkAndHolds(EqualsProto(message_document)));
    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentFilterData message_data,
        document_store->GetDocumentFilterData(message_document_id));
    EXPECT_THAT(message_data.schema_type_id(), Eq(message_schema_type_id));
    message_namespace_id = message_data.namespace_id();
    message_expiration_timestamp = message_data.expiration_timestamp_secs();
  }  // Everything destructs and commits changes to file

  // Change the DocumentStore's header combined checksum so that it won't match
  // the recalculated checksum on initialization. This will force a regeneration
  // of derived files from ground truth.
  const std::string header_file =
      absl_ports::StrCat(document_store_dir_, "/document_store_header");
  DocumentStore::Header header;
  header.magic = DocumentStore::Header::kMagic;
  header.checksum = 10;  // Arbitrary garbage checksum
  filesystem_.DeleteFile(header_file.c_str());
  filesystem_.Write(header_file.c_str(), &header, sizeof(header));

  // Change the schema so that we don't know of the Document's type anymore.
  // Since we can't set backwards incompatible changes, we do some file-level
  // hacks to "reset" the schema. Without a previously existing schema, the new
  // schema isn't considered backwards incompatible
  filesystem_.DeleteDirectoryRecursively(schema_store_dir.c_str());
  filesystem_.CreateDirectoryRecursively(schema_store_dir.c_str());
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir));
  SchemaProto schema;
  auto type_config = schema.add_types();
  type_config->set_schema_type("email");
  ICING_EXPECT_OK(schema_store->SetSchema(schema));

  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId email_schema_type_id,
                             schema_store->GetSchemaTypeId("email"));

  // Successfully recover from a corrupt derived file issue. We don't fail just
  // because the "message" schema type is missing
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> document_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store.get()));

  // "email" document is fine
  EXPECT_THAT(document_store->Get(email_document_id),
              IsOkAndHolds(EqualsProto(email_document)));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentFilterData email_data,
      document_store->GetDocumentFilterData(email_document_id));
  EXPECT_THAT(email_data.schema_type_id(), Eq(email_schema_type_id));
  // Make sure that all the other fields are stll valid/the same
  EXPECT_THAT(email_data.namespace_id(), Eq(email_namespace_id));
  EXPECT_THAT(email_data.expiration_timestamp_secs(),
              Eq(email_expiration_timestamp));

  // "message" document has an invalid SchemaTypeId
  EXPECT_THAT(document_store->Get(message_document_id),
              IsOkAndHolds(EqualsProto(message_document)));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentFilterData message_data,
      document_store->GetDocumentFilterData(message_document_id));
  EXPECT_THAT(message_data.schema_type_id(), Eq(-1));
  // Make sure that all the other fields are stll valid/the same
  EXPECT_THAT(message_data.namespace_id(), Eq(message_namespace_id));
  EXPECT_THAT(message_data.expiration_timestamp_secs(),
              Eq(message_expiration_timestamp));
}

TEST_F(DocumentStoreTest, UpdateSchemaStoreUpdatesSchemaTypeIds) {
  const std::string schema_store_dir = test_dir_ + "_custom";
  filesystem_.DeleteDirectoryRecursively(schema_store_dir.c_str());
  filesystem_.CreateDirectoryRecursively(schema_store_dir.c_str());

  // Set a schema
  SchemaProto schema;
  auto type_config = schema.add_types();
  type_config->set_schema_type("email");
  type_config = schema.add_types();
  type_config->set_schema_type("message");

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir));
  ICING_EXPECT_OK(schema_store->SetSchema(schema));

  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId old_email_schema_type_id,
                             schema_store->GetSchemaTypeId("email"));
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId old_message_schema_type_id,
                             schema_store->GetSchemaTypeId("message"));

  DocumentProto email_document = DocumentBuilder()
                                     .SetNamespace("namespace")
                                     .SetUri("email_uri")
                                     .SetSchema("email")
                                     .Build();

  DocumentProto message_document = DocumentBuilder()
                                       .SetNamespace("namespace")
                                       .SetUri("message_uri")
                                       .SetSchema("message")
                                       .Build();

  // Add the documents and check SchemaTypeIds match
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> document_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store->Put(email_document));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentFilterData email_data,
      document_store->GetDocumentFilterData(email_document_id));
  EXPECT_THAT(email_data.schema_type_id(), Eq(old_email_schema_type_id));

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message_document_id,
                             document_store->Put(message_document));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentFilterData message_data,
      document_store->GetDocumentFilterData(message_document_id));
  EXPECT_THAT(message_data.schema_type_id(), Eq(old_message_schema_type_id));

  // Rearrange the schema types. Since SchemaTypeId is assigned based on order,
  // this should change the SchemaTypeIds.
  schema.clear_types();
  type_config = schema.add_types();
  type_config->set_schema_type("message");
  type_config = schema.add_types();
  type_config->set_schema_type("email");

  ICING_EXPECT_OK(schema_store->SetSchema(schema));

  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId new_email_schema_type_id,
                             schema_store->GetSchemaTypeId("email"));
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId new_message_schema_type_id,
                             schema_store->GetSchemaTypeId("message"));

  // SchemaTypeIds should have changed.
  EXPECT_NE(old_email_schema_type_id, new_email_schema_type_id);
  EXPECT_NE(old_message_schema_type_id, new_message_schema_type_id);

  ICING_EXPECT_OK(document_store->UpdateSchemaStore(schema_store.get()));

  // Check that the FilterCache holds the new SchemaTypeIds
  ICING_ASSERT_OK_AND_ASSIGN(
      email_data, document_store->GetDocumentFilterData(email_document_id));
  EXPECT_THAT(email_data.schema_type_id(), Eq(new_email_schema_type_id));

  ICING_ASSERT_OK_AND_ASSIGN(
      message_data, document_store->GetDocumentFilterData(message_document_id));
  EXPECT_THAT(message_data.schema_type_id(), Eq(new_message_schema_type_id));
}

TEST_F(DocumentStoreTest, UpdateSchemaStoreDeletesInvalidDocuments) {
  const std::string schema_store_dir = test_dir_ + "_custom";
  filesystem_.DeleteDirectoryRecursively(schema_store_dir.c_str());
  filesystem_.CreateDirectoryRecursively(schema_store_dir.c_str());

  // Set a schema
  SchemaProto schema;
  auto type_config = schema.add_types();
  type_config->set_schema_type("email");

  auto property_config = type_config->add_properties();
  property_config->set_property_name("subject");
  property_config->set_data_type(PropertyConfigProto::DataType::STRING);
  property_config->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
  property_config->mutable_indexing_config()->set_term_match_type(
      TermMatchType::EXACT_ONLY);
  property_config->mutable_indexing_config()->set_tokenizer_type(
      IndexingConfig::TokenizerType::PLAIN);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir));
  ICING_EXPECT_OK(schema_store->SetSchema(schema));

  // Add two documents, with and without a subject
  DocumentProto email_without_subject = DocumentBuilder()
                                            .SetNamespace("namespace")
                                            .SetUri("email_uri_without_subject")
                                            .SetSchema("email")
                                            .SetCreationTimestampSecs(0)
                                            .Build();

  DocumentProto email_with_subject = DocumentBuilder()
                                         .SetNamespace("namespace")
                                         .SetUri("email_uri_with_subject")
                                         .SetSchema("email")
                                         .AddStringProperty("subject", "foo")
                                         .SetCreationTimestampSecs(0)
                                         .Build();

  // Insert documents and check they're ok
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> document_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_without_subject_document_id,
                             document_store->Put(email_without_subject));
  EXPECT_THAT(document_store->Get(email_without_subject_document_id),
              IsOkAndHolds(EqualsProto(email_without_subject)));

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_with_subject_document_id,
                             document_store->Put(email_with_subject));
  EXPECT_THAT(document_store->Get(email_with_subject_document_id),
              IsOkAndHolds(EqualsProto(email_with_subject)));

  // Changing an OPTIONAL field to REQUIRED is backwards incompatible, and will
  // invalidate all documents that don't have this property set
  schema.mutable_types(0)->mutable_properties(0)->set_cardinality(
      PropertyConfigProto::Cardinality::REQUIRED);

  ICING_EXPECT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/true));

  ICING_EXPECT_OK(document_store->UpdateSchemaStore(schema_store.get()));

  // The email without a subject should be marked as deleted
  EXPECT_THAT(document_store->Get(email_without_subject_document_id),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  // The email with a subject should be unaffected
  EXPECT_THAT(document_store->Get(email_with_subject_document_id),
              IsOkAndHolds(EqualsProto(email_with_subject)));
}

TEST_F(DocumentStoreTest,
       UpdateSchemaStoreDeletesDocumentsByDeletedSchemaType) {
  const std::string schema_store_dir = test_dir_ + "_custom";
  filesystem_.DeleteDirectoryRecursively(schema_store_dir.c_str());
  filesystem_.CreateDirectoryRecursively(schema_store_dir.c_str());

  // Set a schema
  SchemaProto schema;
  auto type_config = schema.add_types();
  type_config->set_schema_type("email");
  type_config = schema.add_types();
  type_config->set_schema_type("message");

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir));
  ICING_EXPECT_OK(schema_store->SetSchema(schema));

  // Add a "email" and "message" document
  DocumentProto email_document = DocumentBuilder()
                                     .SetNamespace("namespace")
                                     .SetUri("email_uri")
                                     .SetSchema("email")
                                     .SetCreationTimestampSecs(0)
                                     .Build();

  DocumentProto message_document = DocumentBuilder()
                                       .SetNamespace("namespace")
                                       .SetUri("message_uri")
                                       .SetSchema("message")
                                       .SetCreationTimestampSecs(0)
                                       .Build();

  // Insert documents and check they're ok
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> document_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store->Put(email_document));
  EXPECT_THAT(document_store->Get(email_document_id),
              IsOkAndHolds(EqualsProto(email_document)));

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message_document_id,
                             document_store->Put(message_document));
  EXPECT_THAT(document_store->Get(message_document_id),
              IsOkAndHolds(EqualsProto(message_document)));

  SchemaProto new_schema;
  type_config = new_schema.add_types();
  type_config->set_schema_type("message");

  ICING_EXPECT_OK(
      schema_store->SetSchema(new_schema,
                              /*ignore_errors_and_delete_documents=*/true));

  ICING_EXPECT_OK(document_store->UpdateSchemaStore(schema_store.get()));

  // The "email" type is unknown now, so the "email" document should be deleted
  EXPECT_THAT(document_store->Get(email_document_id),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  // The "message" document should be unaffected
  EXPECT_THAT(document_store->Get(message_document_id),
              IsOkAndHolds(EqualsProto(message_document)));
}

TEST_F(DocumentStoreTest, OptimizedUpdateSchemaStoreUpdatesSchemaTypeIds) {
  const std::string schema_store_dir = test_dir_ + "_custom";
  filesystem_.DeleteDirectoryRecursively(schema_store_dir.c_str());
  filesystem_.CreateDirectoryRecursively(schema_store_dir.c_str());

  // Set a schema
  SchemaProto schema;
  auto type_config = schema.add_types();
  type_config->set_schema_type("email");
  type_config = schema.add_types();
  type_config->set_schema_type("message");

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir));
  ICING_EXPECT_OK(schema_store->SetSchema(schema));

  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId old_email_schema_type_id,
                             schema_store->GetSchemaTypeId("email"));
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId old_message_schema_type_id,
                             schema_store->GetSchemaTypeId("message"));

  DocumentProto email_document = DocumentBuilder()
                                     .SetNamespace("namespace")
                                     .SetUri("email_uri")
                                     .SetSchema("email")
                                     .Build();

  DocumentProto message_document = DocumentBuilder()
                                       .SetNamespace("namespace")
                                       .SetUri("message_uri")
                                       .SetSchema("message")
                                       .Build();

  // Add the documents and check SchemaTypeIds match
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> document_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store->Put(email_document));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentFilterData email_data,
      document_store->GetDocumentFilterData(email_document_id));
  EXPECT_THAT(email_data.schema_type_id(), Eq(old_email_schema_type_id));

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message_document_id,
                             document_store->Put(message_document));
  ICING_ASSERT_OK_AND_ASSIGN(
      DocumentFilterData message_data,
      document_store->GetDocumentFilterData(message_document_id));
  EXPECT_THAT(message_data.schema_type_id(), Eq(old_message_schema_type_id));

  // Rearrange the schema types. Since SchemaTypeId is assigned based on order,
  // this should change the SchemaTypeIds.
  schema.clear_types();
  type_config = schema.add_types();
  type_config->set_schema_type("message");
  type_config = schema.add_types();
  type_config->set_schema_type("email");

  ICING_ASSERT_OK_AND_ASSIGN(SchemaStore::SetSchemaResult set_schema_result,
                             schema_store->SetSchema(schema));

  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId new_email_schema_type_id,
                             schema_store->GetSchemaTypeId("email"));
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId new_message_schema_type_id,
                             schema_store->GetSchemaTypeId("message"));

  // SchemaTypeIds should have changed.
  EXPECT_NE(old_email_schema_type_id, new_email_schema_type_id);
  EXPECT_NE(old_message_schema_type_id, new_message_schema_type_id);

  ICING_EXPECT_OK(document_store->OptimizedUpdateSchemaStore(
      schema_store.get(), set_schema_result));

  // Check that the FilterCache holds the new SchemaTypeIds
  ICING_ASSERT_OK_AND_ASSIGN(
      email_data, document_store->GetDocumentFilterData(email_document_id));
  EXPECT_THAT(email_data.schema_type_id(), Eq(new_email_schema_type_id));

  ICING_ASSERT_OK_AND_ASSIGN(
      message_data, document_store->GetDocumentFilterData(message_document_id));
  EXPECT_THAT(message_data.schema_type_id(), Eq(new_message_schema_type_id));
}

TEST_F(DocumentStoreTest, OptimizedUpdateSchemaStoreDeletesInvalidDocuments) {
  const std::string schema_store_dir = test_dir_ + "_custom";
  filesystem_.DeleteDirectoryRecursively(schema_store_dir.c_str());
  filesystem_.CreateDirectoryRecursively(schema_store_dir.c_str());

  // Set a schema
  SchemaProto schema;
  auto type_config = schema.add_types();
  type_config->set_schema_type("email");

  auto property_config = type_config->add_properties();
  property_config->set_property_name("subject");
  property_config->set_data_type(PropertyConfigProto::DataType::STRING);
  property_config->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
  property_config->mutable_indexing_config()->set_term_match_type(
      TermMatchType::EXACT_ONLY);
  property_config->mutable_indexing_config()->set_tokenizer_type(
      IndexingConfig::TokenizerType::PLAIN);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir));
  ICING_EXPECT_OK(schema_store->SetSchema(schema));

  // Add two documents, with and without a subject
  DocumentProto email_without_subject = DocumentBuilder()
                                            .SetNamespace("namespace")
                                            .SetUri("email_uri_without_subject")
                                            .SetSchema("email")
                                            .SetCreationTimestampSecs(0)
                                            .Build();

  DocumentProto email_with_subject = DocumentBuilder()
                                         .SetNamespace("namespace")
                                         .SetUri("email_uri_with_subject")
                                         .SetSchema("email")
                                         .AddStringProperty("subject", "foo")
                                         .SetCreationTimestampSecs(0)
                                         .Build();

  // Insert documents and check they're ok
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> document_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_without_subject_document_id,
                             document_store->Put(email_without_subject));
  EXPECT_THAT(document_store->Get(email_without_subject_document_id),
              IsOkAndHolds(EqualsProto(email_without_subject)));

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_with_subject_document_id,
                             document_store->Put(email_with_subject));
  EXPECT_THAT(document_store->Get(email_with_subject_document_id),
              IsOkAndHolds(EqualsProto(email_with_subject)));

  // Changing an OPTIONAL field to REQUIRED is backwards incompatible, and will
  // invalidate all documents that don't have this property set
  schema.mutable_types(0)->mutable_properties(0)->set_cardinality(
      PropertyConfigProto::Cardinality::REQUIRED);

  ICING_ASSERT_OK_AND_ASSIGN(
      SchemaStore::SetSchemaResult set_schema_result,
      schema_store->SetSchema(schema,
                              /*ignore_errors_and_delete_documents=*/true));

  ICING_EXPECT_OK(document_store->OptimizedUpdateSchemaStore(
      schema_store.get(), set_schema_result));

  // The email without a subject should be marked as deleted
  EXPECT_THAT(document_store->Get(email_without_subject_document_id),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  // The email with a subject should be unaffected
  EXPECT_THAT(document_store->Get(email_with_subject_document_id),
              IsOkAndHolds(EqualsProto(email_with_subject)));
}

TEST_F(DocumentStoreTest,
       OptimizedUpdateSchemaStoreDeletesDocumentsByDeletedSchemaType) {
  const std::string schema_store_dir = test_dir_ + "_custom";
  filesystem_.DeleteDirectoryRecursively(schema_store_dir.c_str());
  filesystem_.CreateDirectoryRecursively(schema_store_dir.c_str());

  // Set a schema
  SchemaProto schema;
  auto type_config = schema.add_types();
  type_config->set_schema_type("email");
  type_config = schema.add_types();
  type_config->set_schema_type("message");

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir));
  ICING_EXPECT_OK(schema_store->SetSchema(schema));

  // Add a "email" and "message" document
  DocumentProto email_document = DocumentBuilder()
                                     .SetNamespace("namespace")
                                     .SetUri("email_uri")
                                     .SetSchema("email")
                                     .SetCreationTimestampSecs(0)
                                     .Build();

  DocumentProto message_document = DocumentBuilder()
                                       .SetNamespace("namespace")
                                       .SetUri("message_uri")
                                       .SetSchema("message")
                                       .SetCreationTimestampSecs(0)
                                       .Build();

  // Insert documents and check they're ok
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DocumentStore> document_store,
      DocumentStore::Create(&filesystem_, document_store_dir_, &fake_clock_,
                            schema_store.get()));
  ICING_ASSERT_OK_AND_ASSIGN(DocumentId email_document_id,
                             document_store->Put(email_document));
  EXPECT_THAT(document_store->Get(email_document_id),
              IsOkAndHolds(EqualsProto(email_document)));

  ICING_ASSERT_OK_AND_ASSIGN(DocumentId message_document_id,
                             document_store->Put(message_document));
  EXPECT_THAT(document_store->Get(message_document_id),
              IsOkAndHolds(EqualsProto(message_document)));

  SchemaProto new_schema;
  type_config = new_schema.add_types();
  type_config->set_schema_type("message");

  ICING_ASSERT_OK_AND_ASSIGN(
      SchemaStore::SetSchemaResult set_schema_result,
      schema_store->SetSchema(new_schema,
                              /*ignore_errors_and_delete_documents=*/true));

  ICING_EXPECT_OK(document_store->OptimizedUpdateSchemaStore(
      schema_store.get(), set_schema_result));

  // The "email" type is unknown now, so the "email" document should be deleted
  EXPECT_THAT(document_store->Get(email_document_id),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  // The "message" document should be unaffected
  EXPECT_THAT(document_store->Get(message_document_id),
              IsOkAndHolds(EqualsProto(message_document)));
}

}  // namespace lib
}  // namespace icing
