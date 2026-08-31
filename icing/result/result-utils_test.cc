// Copyright (C) 2026 Google LLC
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

#include "icing/result/result-utils.h"

#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/feature-flags.h"
#include "icing/file/filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/portable/equals-proto.h"
#include "icing/portable/gzip_stream.h"
#include "icing/proto/debug.pb.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/document_wrapper.pb.h"
#include "icing/proto/internal/scorable_property_set.pb.h"
#include "icing/proto/logging.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/storage.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/proto/usage.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/document-store.h"
#include "icing/store/namespace-id.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/document-util.h"

namespace icing {
namespace lib {
namespace result_utils {

namespace {

using ResultSpecProto::ResultGroupingType::
    ResultSpecProto_ResultGroupingType_NAMESPACE;
using ResultSpecProto::ResultGroupingType::
    ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE;
using ResultSpecProto::ResultGroupingType::
    ResultSpecProto_ResultGroupingType_NONE;
using ResultSpecProto::ResultGroupingType::
    ResultSpecProto_ResultGroupingType_SCHEMA_TYPE;

using ::testing::Eq;
using ::testing::IsFalse;
using ::testing::Optional;
using ::testing::Pair;

class ResultUtilsTest : public ::testing::Test {
 protected:
  ResultUtilsTest()
      : test_dir_(GetTestTempDir() + "/icing"),
        schema_store_dir_(test_dir_ + "/schema_store"),
        document_store_dir_(test_dir_ + "/document_store") {}

  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());

    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());
    filesystem_.CreateDirectoryRecursively(document_store_dir_.c_str());

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, schema_store_dir_,
                                           &fake_clock_, feature_flags_.get()));

    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult result,
        DocumentStore::Create(
            &filesystem_, document_store_dir_, &fake_clock_,
            schema_store_.get(), feature_flags_.get(),
            /*force_recovery_and_revalidate_documents=*/false,
            /*pre_mapping_fbv=*/false,
            /*use_persistent_hash_map=*/true,
            PortableFileBackedProtoLog<
                DocumentWrapper>::kDefaultCompressionLevel,
            PortableFileBackedProtoLog<
                DocumentWrapper>::kDefaultCompressionThresholdBytes,
            protobuf_ports::kDefaultMemLevel,
            /*initialize_stats=*/nullptr));
    document_store_ = std::move(result.document_store);
  }

  void TearDown() override {
    document_store_.reset();
    schema_store_.reset();

    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  Filesystem filesystem_;
  const std::string test_dir_;
  const std::string schema_store_dir_;
  const std::string document_store_dir_;
  FakeClock fake_clock_;
  std::unique_ptr<SchemaStore> schema_store_;
  std::unique_ptr<DocumentStore> document_store_;
};

TEST_F(ResultUtilsTest, EncodeResultGroupingEntryId_byFilterName) {
  // Put 2 schema types into the schema store.
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Email"))
          .AddType(SchemaTypeConfigBuilder().SetType("Message"))
          .Build();
  ICING_ASSERT_OK(schema_store_->SetSchema(
      std::move(schema), /*ignore_errors_and_delete_documents=*/false));
  ICING_ASSERT_OK(document_store_->UpdateSchemaStore(schema_store_.get()));

  // Put 3 documents into the document store to create 3 different namespaces.
  DocumentProto document0 = DocumentBuilder()
                                .SetKey("namespace0", "uri/0")
                                .SetSchema("Email")
                                .Build();
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace1", "uri/1")
                                .SetSchema("Message")
                                .Build();
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace2", "uri/2")
                                .SetSchema("Message")
                                .Build();
  ICING_ASSERT_OK(document_store_->Put(
      document_util::CreateDocumentWrapper(std::move(document0))));
  ICING_ASSERT_OK(document_store_->Put(
      document_util::CreateDocumentWrapper(std::move(document1))));
  ICING_ASSERT_OK(document_store_->Put(
      document_util::CreateDocumentWrapper(std::move(document2))));

  ASSERT_THAT(document_store_->GetNamespaceId("namespace0"), IsOkAndHolds(0));
  ASSERT_THAT(document_store_->GetNamespaceId("namespace1"), IsOkAndHolds(1));
  ASSERT_THAT(document_store_->GetNamespaceId("namespace2"), IsOkAndHolds(2));

  ASSERT_THAT(schema_store_->GetSchemaTypeId("Email"), IsOkAndHolds(0));
  ASSERT_THAT(schema_store_->GetSchemaTypeId("Message"), IsOkAndHolds(1));

  // NONE should always return std::nullopt.
  EXPECT_THAT(
      EncodeResultGroupingEntryId(*schema_store_, *document_store_,
                                  ResultSpecProto_ResultGroupingType_NONE,
                                  "namespace0", "Email"),
      IsFalse());
  EXPECT_THAT(
      EncodeResultGroupingEntryId(*schema_store_, *document_store_,
                                  ResultSpecProto_ResultGroupingType_NONE,
                                  "namespace1", "Email"),
      IsFalse());
  EXPECT_THAT(
      EncodeResultGroupingEntryId(*schema_store_, *document_store_,
                                  ResultSpecProto_ResultGroupingType_NONE,
                                  "namespace2", "Email"),
      IsFalse());
  EXPECT_THAT(
      EncodeResultGroupingEntryId(*schema_store_, *document_store_,
                                  ResultSpecProto_ResultGroupingType_NONE,
                                  "namespace0", "Message"),
      IsFalse());
  EXPECT_THAT(
      EncodeResultGroupingEntryId(*schema_store_, *document_store_,
                                  ResultSpecProto_ResultGroupingType_NONE,
                                  "namespace1", "Message"),
      IsFalse());
  EXPECT_THAT(
      EncodeResultGroupingEntryId(*schema_store_, *document_store_,
                                  ResultSpecProto_ResultGroupingType_NONE,
                                  "namespace2", "Message"),
      IsFalse());

  // SCHEMA_TYPE should return id based on the schema type and ignore the
  // namespace.
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_SCHEMA_TYPE, "namespace0",
                  "Email"),
              Optional(Eq(0)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_SCHEMA_TYPE, "namespace1",
                  "Email"),
              Optional(Eq(0)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_SCHEMA_TYPE, "namespace2",
                  "Email"),
              Optional(Eq(0)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_SCHEMA_TYPE, "namespace0",
                  "Message"),
              Optional(Eq(1)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_SCHEMA_TYPE, "namespace1",
                  "Message"),
              Optional(Eq(1)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_SCHEMA_TYPE, "namespace2",
                  "Message"),
              Optional(Eq(1)));

  // NAMESPACE should return id based on the namespace and ignore the schema
  // type.
  EXPECT_THAT(
      EncodeResultGroupingEntryId(*schema_store_, *document_store_,
                                  ResultSpecProto_ResultGroupingType_NAMESPACE,
                                  "namespace0", "Email"),
      Optional(Eq(0)));
  EXPECT_THAT(
      EncodeResultGroupingEntryId(*schema_store_, *document_store_,
                                  ResultSpecProto_ResultGroupingType_NAMESPACE,
                                  "namespace1", "Email"),
      Optional(Eq(1)));
  EXPECT_THAT(
      EncodeResultGroupingEntryId(*schema_store_, *document_store_,
                                  ResultSpecProto_ResultGroupingType_NAMESPACE,
                                  "namespace2", "Email"),
      Optional(Eq(2)));
  EXPECT_THAT(
      EncodeResultGroupingEntryId(*schema_store_, *document_store_,
                                  ResultSpecProto_ResultGroupingType_NAMESPACE,
                                  "namespace0", "Message"),
      Optional(Eq(0)));
  EXPECT_THAT(
      EncodeResultGroupingEntryId(*schema_store_, *document_store_,
                                  ResultSpecProto_ResultGroupingType_NAMESPACE,
                                  "namespace1", "Message"),
      Optional(Eq(1)));
  EXPECT_THAT(
      EncodeResultGroupingEntryId(*schema_store_, *document_store_,
                                  ResultSpecProto_ResultGroupingType_NAMESPACE,
                                  "namespace2", "Message"),
      Optional(Eq(2)));

  // NAMESPACE_AND_SCHEMA_TYPE should return id based on both namespace and
  // schema type.
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  "namespace0", "Email"),
              Optional(Eq(0x00000000)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  "namespace1", "Email"),
              Optional(Eq(0x00010000)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  "namespace2", "Email"),
              Optional(Eq(0x00020000)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  "namespace0", "Message"),
              Optional(Eq(0x00000001)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  "namespace1", "Message"),
              Optional(Eq(0x00010001)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  "namespace2", "Message"),
              Optional(Eq(0x00020001)));
}

TEST_F(ResultUtilsTest, EncodeResultGroupingEntryId_byNonExistingFilterName) {
  // Put 1 schema type into the schema store.
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("Email"))
                           .Build();
  ICING_ASSERT_OK(schema_store_->SetSchema(
      std::move(schema), /*ignore_errors_and_delete_documents=*/false));
  ICING_ASSERT_OK(document_store_->UpdateSchemaStore(schema_store_.get()));

  // Put 1 document into the document store to create 1 namespace.
  DocumentProto document0 = DocumentBuilder()
                                .SetKey("namespace0", "uri/0")
                                .SetSchema("Email")
                                .Build();
  ICING_ASSERT_OK(document_store_->Put(
      document_util::CreateDocumentWrapper(std::move(document0))));

  ASSERT_THAT(document_store_->GetNamespaceId("namespace0"), IsOkAndHolds(0));
  ASSERT_THAT(schema_store_->GetSchemaTypeId("Email"), IsOkAndHolds(0));

  // NONE should always return std::nullopt.
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_NONE,
                  "nonExistingNamespace", "nonExistingSchemaType"),
              IsFalse());
  EXPECT_THAT(
      EncodeResultGroupingEntryId(*schema_store_, *document_store_,
                                  ResultSpecProto_ResultGroupingType_NONE,
                                  "nonExistingNamespace", "Email"),
      IsFalse());
  EXPECT_THAT(
      EncodeResultGroupingEntryId(*schema_store_, *document_store_,
                                  ResultSpecProto_ResultGroupingType_NONE,
                                  "namespace0", "nonExistingSchemaType"),
      IsFalse());

  // SCHEMA_TYPE should return id based on the schema type and ignore the
  // namespace. It is ok that the namespace does not exist.
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_SCHEMA_TYPE,
                  "nonExistingNamespace", "nonExistingSchemaType"),
              IsFalse());
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_SCHEMA_TYPE,
                  "nonExistingNamespace", "Email"),
              Optional(Eq(0)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_SCHEMA_TYPE, "namespace0",
                  "nonExistingSchemaType"),
              IsFalse());

  // NAMESPACE should return id based on the namespace and ignore the schema
  // type. It is ok that the schema type does not exist.
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_NAMESPACE,
                  "nonExistingNamespace", "nonExistingSchemaType"),
              IsFalse());
  EXPECT_THAT(
      EncodeResultGroupingEntryId(*schema_store_, *document_store_,
                                  ResultSpecProto_ResultGroupingType_NAMESPACE,
                                  "nonExistingNamespace", "Email"),
      IsFalse());
  EXPECT_THAT(
      EncodeResultGroupingEntryId(*schema_store_, *document_store_,
                                  ResultSpecProto_ResultGroupingType_NAMESPACE,
                                  "namespace0", "nonExistingSchemaType"),
      Optional(Eq(0)));

  // NAMESPACE_AND_SCHEMA_TYPE should return id based on both namespace and
  // schema type. Both namespace and schema type must exist.
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  "nonExistingNamespace", "nonExistingSchemaType"),
              IsFalse());
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  "nonExistingNamespace", "Email"),
              IsFalse());
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  *schema_store_, *document_store_,
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  "namespace0", "nonExistingSchemaType"),
              IsFalse());
}

TEST_F(ResultUtilsTest, EncodeResultGroupingEntryId_byIds) {
  // EncodeResultGroupingEntryId() by id only handles the encoding and won't
  // check if the id exists (except kInvalidNamespaceId and
  // kInvalidSchemaTypeId), so we don't need to set up schema types and
  // namespaces here.

  // NONE should always return std::nullopt.
  EXPECT_THAT(
      EncodeResultGroupingEntryId(ResultSpecProto_ResultGroupingType_NONE,
                                  /*namespace_id=*/0, /*schema_type_id=*/0),
      IsFalse());
  EXPECT_THAT(
      EncodeResultGroupingEntryId(ResultSpecProto_ResultGroupingType_NONE,
                                  /*namespace_id=*/0, /*schema_type_id=*/1),
      IsFalse());
  EXPECT_THAT(
      EncodeResultGroupingEntryId(ResultSpecProto_ResultGroupingType_NONE,
                                  /*namespace_id=*/1, /*schema_type_id=*/0),
      IsFalse());
  EXPECT_THAT(
      EncodeResultGroupingEntryId(ResultSpecProto_ResultGroupingType_NONE,
                                  /*namespace_id=*/1, /*schema_type_id=*/1),
      IsFalse());
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  ResultSpecProto_ResultGroupingType_NONE,
                  /*namespace_id=*/std::numeric_limits<NamespaceId>::max(),
                  /*schema_type_id=*/std::numeric_limits<SchemaTypeId>::max()),
              IsFalse());

  // SCHEMA_TYPE should return id based on the schema type id and ignore the
  // namespace id.
  EXPECT_THAT(
      EncodeResultGroupingEntryId(
          ResultSpecProto_ResultGroupingType_SCHEMA_TYPE, /*namespace_id=*/0,
          /*schema_type_id=*/0),
      Optional(Eq(0)));
  EXPECT_THAT(
      EncodeResultGroupingEntryId(
          ResultSpecProto_ResultGroupingType_SCHEMA_TYPE, /*namespace_id=*/0,
          /*schema_type_id=*/1),
      Optional(Eq(1)));
  EXPECT_THAT(
      EncodeResultGroupingEntryId(
          ResultSpecProto_ResultGroupingType_SCHEMA_TYPE, /*namespace_id=*/0,
          /*schema_type_id=*/123),
      Optional(Eq(123)));
  EXPECT_THAT(
      EncodeResultGroupingEntryId(
          ResultSpecProto_ResultGroupingType_SCHEMA_TYPE, /*namespace_id=*/1,
          /*schema_type_id=*/0),
      Optional(Eq(0)));
  EXPECT_THAT(
      EncodeResultGroupingEntryId(
          ResultSpecProto_ResultGroupingType_SCHEMA_TYPE, /*namespace_id=*/1,
          /*schema_type_id=*/1),
      Optional(Eq(1)));
  EXPECT_THAT(
      EncodeResultGroupingEntryId(
          ResultSpecProto_ResultGroupingType_SCHEMA_TYPE, /*namespace_id=*/1,
          /*schema_type_id=*/123),
      Optional(Eq(123)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  ResultSpecProto_ResultGroupingType_SCHEMA_TYPE,
                  /*namespace_id=*/20,
                  /*schema_type_id=*/std::numeric_limits<SchemaTypeId>::max()),
              Optional(Eq(std::numeric_limits<SchemaTypeId>::max())));

  // NAMESPACE should return id based on the namespace id and ignore the schema
  // type id.
  EXPECT_THAT(
      EncodeResultGroupingEntryId(ResultSpecProto_ResultGroupingType_NAMESPACE,
                                  /*namespace_id=*/0, /*schema_type_id=*/0),
      Optional(Eq(0)));
  EXPECT_THAT(
      EncodeResultGroupingEntryId(ResultSpecProto_ResultGroupingType_NAMESPACE,
                                  /*namespace_id=*/1, /*schema_type_id=*/0),
      Optional(Eq(1)));
  EXPECT_THAT(
      EncodeResultGroupingEntryId(ResultSpecProto_ResultGroupingType_NAMESPACE,
                                  /*namespace_id=*/123, /*schema_type_id=*/0),
      Optional(Eq(123)));
  EXPECT_THAT(
      EncodeResultGroupingEntryId(ResultSpecProto_ResultGroupingType_NAMESPACE,
                                  /*namespace_id=*/0, /*schema_type_id=*/1),
      Optional(Eq(0)));
  EXPECT_THAT(
      EncodeResultGroupingEntryId(ResultSpecProto_ResultGroupingType_NAMESPACE,
                                  /*namespace_id=*/1, /*schema_type_id=*/1),
      Optional(Eq(1)));
  EXPECT_THAT(
      EncodeResultGroupingEntryId(ResultSpecProto_ResultGroupingType_NAMESPACE,
                                  /*namespace_id=*/123, /*schema_type_id=*/1),
      Optional(Eq(123)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  ResultSpecProto_ResultGroupingType_NAMESPACE,
                  /*namespace_id=*/std::numeric_limits<NamespaceId>::max(),
                  /*schema_type_id=*/20),
              Optional(Eq(std::numeric_limits<NamespaceId>::max())));

  // NAMESPACE_AND_SCHEMA_TYPE should return id based on both namespace and
  // schema type ids.
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  /*namespace_id=*/0, /*schema_type_id=*/0),
              Optional(Eq(0x00000000)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  /*namespace_id=*/0, /*schema_type_id=*/1),
              Optional(Eq(0x00000001)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  /*namespace_id=*/1, /*schema_type_id=*/0),
              Optional(Eq(0x00010000)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  /*namespace_id=*/1, /*schema_type_id=*/1),
              Optional(Eq(0x00010001)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  /*namespace_id=*/2, /*schema_type_id=*/0),
              Optional(Eq(0x00020000)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  /*namespace_id=*/2, /*schema_type_id=*/1),
              Optional(Eq(0x00020001)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  /*namespace_id=*/2, /*schema_type_id=*/2),
              Optional(Eq(0x00020002)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  /*namespace_id=*/0x1234, /*schema_type_id=*/0x5678),
              Optional(Eq(0x12345678)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  /*namespace_id=*/std::numeric_limits<NamespaceId>::max(),
                  /*schema_type_id=*/std::numeric_limits<SchemaTypeId>::max()),
              Optional(Eq(0x7fff7fff)));
}

TEST_F(ResultUtilsTest, EncodeResultGroupingEntryId_byInvalidIds) {
  // EncodeResultGroupingEntryId() by id only handles the encoding and won't
  // check if the id exists (except kInvalidNamespaceId and
  // kInvalidSchemaTypeId), so we don't need to set up schema types and
  // namespaces here.

  // NONE should always return std::nullopt.
  EXPECT_THAT(
      EncodeResultGroupingEntryId(ResultSpecProto_ResultGroupingType_NONE,
                                  kInvalidNamespaceId, kInvalidSchemaTypeId),
      IsFalse());
  EXPECT_THAT(
      EncodeResultGroupingEntryId(ResultSpecProto_ResultGroupingType_NONE,
                                  kInvalidNamespaceId, /*schema_type_id=*/0),
      IsFalse());
  EXPECT_THAT(
      EncodeResultGroupingEntryId(ResultSpecProto_ResultGroupingType_NONE,
                                  /*namespace_id=*/0, kInvalidSchemaTypeId),
      IsFalse());

  // SCHEMA_TYPE should return id based on the schema type id and ignore the
  // namespace id. It is ok that the namespace id is invalid.
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  ResultSpecProto_ResultGroupingType_SCHEMA_TYPE,
                  kInvalidNamespaceId, kInvalidSchemaTypeId),
              IsFalse());
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  ResultSpecProto_ResultGroupingType_SCHEMA_TYPE,
                  kInvalidNamespaceId, /*schema_type_id=*/0),
              Optional(Eq(0)));
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  ResultSpecProto_ResultGroupingType_SCHEMA_TYPE,
                  /*namespace_id=*/0, kInvalidSchemaTypeId),
              IsFalse());

  // NAMESPACE should return id based on the namespace id and ignore the schema
  // type id. It is ok that the schema type id is invalid.
  EXPECT_THAT(
      EncodeResultGroupingEntryId(ResultSpecProto_ResultGroupingType_NAMESPACE,
                                  kInvalidNamespaceId, kInvalidSchemaTypeId),
      IsFalse());
  EXPECT_THAT(
      EncodeResultGroupingEntryId(ResultSpecProto_ResultGroupingType_NAMESPACE,
                                  kInvalidNamespaceId, /*schema_type_id=*/0),
      IsFalse());
  EXPECT_THAT(
      EncodeResultGroupingEntryId(ResultSpecProto_ResultGroupingType_NAMESPACE,
                                  /*namespace_id=*/0, kInvalidSchemaTypeId),
      Optional(Eq(0)));
  // NAMESPACE_AND_SCHEMA_TYPE should return id based on both namespace and
  // schema type ids. Both namespace and schema type ids must be valid.
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  kInvalidNamespaceId, kInvalidSchemaTypeId),
              IsFalse());
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  kInvalidNamespaceId, /*schema_type_id=*/0),
              IsFalse());
  EXPECT_THAT(EncodeResultGroupingEntryId(
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE,
                  /*namespace_id=*/0, kInvalidSchemaTypeId),
              IsFalse());
}

TEST_F(ResultUtilsTest, DecodeResultGroupingEntryId) {
  // NONE
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/0,
                  ResultSpecProto_ResultGroupingType_NONE),
              Pair(kInvalidNamespaceId, kInvalidSchemaTypeId));
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/1,
                  ResultSpecProto_ResultGroupingType_NONE),
              Pair(kInvalidNamespaceId, kInvalidSchemaTypeId));
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/2,
                  ResultSpecProto_ResultGroupingType_NONE),
              Pair(kInvalidNamespaceId, kInvalidSchemaTypeId));
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/123,
                  ResultSpecProto_ResultGroupingType_NONE),
              Pair(kInvalidNamespaceId, kInvalidSchemaTypeId));
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/0x7fff7fff,
                  ResultSpecProto_ResultGroupingType_NONE),
              Pair(kInvalidNamespaceId, kInvalidSchemaTypeId));

  // SCHEMA_TYPE
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/0,
                  ResultSpecProto_ResultGroupingType_SCHEMA_TYPE),
              Pair(kInvalidNamespaceId, 0));
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/1,
                  ResultSpecProto_ResultGroupingType_SCHEMA_TYPE),
              Pair(kInvalidNamespaceId, 1));
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/2,
                  ResultSpecProto_ResultGroupingType_SCHEMA_TYPE),
              Pair(kInvalidNamespaceId, 2));
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/123,
                  ResultSpecProto_ResultGroupingType_SCHEMA_TYPE),
              Pair(kInvalidNamespaceId, 123));
  EXPECT_THAT(
      DecodeResultGroupingEntryId(
          /*result_grouping_entry_id=*/std::numeric_limits<SchemaTypeId>::max(),
          ResultSpecProto_ResultGroupingType_SCHEMA_TYPE),
      Pair(kInvalidNamespaceId, std::numeric_limits<SchemaTypeId>::max()));

  // NAMESPACE
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/0,
                  ResultSpecProto_ResultGroupingType_NAMESPACE),
              Pair(0, kInvalidSchemaTypeId));
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/1,
                  ResultSpecProto_ResultGroupingType_NAMESPACE),
              Pair(1, kInvalidSchemaTypeId));
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/2,
                  ResultSpecProto_ResultGroupingType_NAMESPACE),
              Pair(2, kInvalidSchemaTypeId));
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/123,
                  ResultSpecProto_ResultGroupingType_NAMESPACE),
              Pair(123, kInvalidSchemaTypeId));
  EXPECT_THAT(
      DecodeResultGroupingEntryId(
          /*result_grouping_entry_id=*/
          std::numeric_limits<NamespaceId>::max(),
          ResultSpecProto_ResultGroupingType_NAMESPACE),
      Pair(std::numeric_limits<NamespaceId>::max(), kInvalidSchemaTypeId));

  // NAMESPACE_AND_SCHEMA_TYPE
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/0x00000000,
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE),
              Pair(0, 0));
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/0x00000001,
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE),
              Pair(0, 1));
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/0x00010000,
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE),
              Pair(1, 0));
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/0x00010001,
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE),
              Pair(1, 1));
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/0x00020000,
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE),
              Pair(2, 0));
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/0x00020001,
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE),
              Pair(2, 1));
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/0x00020002,
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE),
              Pair(2, 2));
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/0x12345678,
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE),
              Pair(0x1234, 0x5678));
  EXPECT_THAT(DecodeResultGroupingEntryId(
                  /*result_grouping_entry_id=*/0x7fff7fff,
                  ResultSpecProto_ResultGroupingType_NAMESPACE_AND_SCHEMA_TYPE),
              Pair(std::numeric_limits<NamespaceId>::max(),
                   std::numeric_limits<SchemaTypeId>::max()));
}

}  // namespace

}  // namespace result_utils
}  // namespace lib
}  // namespace icing
