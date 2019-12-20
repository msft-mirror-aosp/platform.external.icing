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

#include "icing/schema/schema-store.h"

#include <memory>
#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/file/filesystem.h"
#include "icing/portable/equals-proto.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema/schema-util.h"
#include "icing/schema/section-manager.h"
#include "icing/schema/section.h"
#include "icing/store/document-filter-data.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::Not;
using ::testing::Pointee;

class SchemaStoreTest : public ::testing::Test {
 protected:
  SchemaStoreTest() : test_dir_(GetTestTempDir() + "/icing") {
    filesystem_.CreateDirectoryRecursively(test_dir_.c_str());

    auto type = schema_.add_types();
    type->set_schema_type("email");

    // Add an indexed property so we generate section metadata on it
    auto property = type->add_properties();
    property->set_property_name("subject");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
    property->mutable_indexing_config()->set_term_match_type(
        TermMatchType::EXACT_ONLY);
    property->mutable_indexing_config()->set_tokenizer_type(
        IndexingConfig::TokenizerType::PLAIN);
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(test_dir_.c_str());
  }

  const Filesystem filesystem_;
  const std::string test_dir_;
  SchemaProto schema_;
};

TEST_F(SchemaStoreTest, CorruptSchemaError) {
  {
    ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                               SchemaStore::Create(&filesystem_, test_dir_));

    // Set it for the first time
    SchemaStore::SetSchemaResult result;
    result.success = true;
    EXPECT_THAT(schema_store->SetSchema(schema_),
                IsOkAndHolds(EqualsSetSchemaResult(result)));
    ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                               schema_store->GetSchema());
    EXPECT_THAT(*actual_schema, EqualsProto(schema_));
  }

  // "Corrupt" the  ground truth schema by adding new data to it. This will mess
  // up the checksum of the schema store

  SchemaProto corrupt_schema;
  auto type = corrupt_schema.add_types();
  type->set_schema_type("corrupted");

  const std::string schema_file = absl_ports::StrCat(test_dir_, "/schema.pb");
  const std::string serialized_schema = corrupt_schema.SerializeAsString();

  filesystem_.Write(schema_file.c_str(), serialized_schema.data(),
                    serialized_schema.size());

  // If ground truth was corrupted, we won't know what to do
  EXPECT_THAT(SchemaStore::Create(&filesystem_, test_dir_),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL));
}

TEST_F(SchemaStoreTest, RecoverCorruptDerivedFileOk) {
  {
    ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                               SchemaStore::Create(&filesystem_, test_dir_));

    // Set it for the first time
    SchemaStore::SetSchemaResult result;
    result.success = true;
    EXPECT_THAT(schema_store->SetSchema(schema_),
                IsOkAndHolds(EqualsSetSchemaResult(result)));
    ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                               schema_store->GetSchema());
    EXPECT_THAT(*actual_schema, EqualsProto(schema_));

    EXPECT_THAT(schema_store->GetSchemaTypeId("email"), IsOkAndHolds(0));
  }

  // "Corrupt" the derived SchemaTypeIds by deleting the entire directory. This
  // will mess up the initialization of schema store, causing everything to be
  // regenerated from ground truth

  const std::string schema_type_mapper_dir =
      absl_ports::StrCat(test_dir_, "/schema_type_mapper");
  filesystem_.DeleteDirectoryRecursively(schema_type_mapper_dir.c_str());

  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  // Everything looks fine, ground truth and derived data
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema_));
  EXPECT_THAT(schema_store->GetSchemaTypeId("email"), IsOkAndHolds(0));
}

TEST_F(SchemaStoreTest, RecoverBadChecksumOk) {
  {
    ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                               SchemaStore::Create(&filesystem_, test_dir_));

    // Set it for the first time
    SchemaStore::SetSchemaResult result;
    result.success = true;
    EXPECT_THAT(schema_store->SetSchema(schema_),
                IsOkAndHolds(EqualsSetSchemaResult(result)));
    ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                               schema_store->GetSchema());
    EXPECT_THAT(*actual_schema, EqualsProto(schema_));

    EXPECT_THAT(schema_store->GetSchemaTypeId("email"), IsOkAndHolds(0));
  }

  // Change the SchemaStore's header combined checksum so that it won't match
  // the recalculated checksum on initialization. This will force a regeneration
  // of derived files from ground truth.
  const std::string header_file =
      absl_ports::StrCat(test_dir_, "/schema_store_header");
  SchemaStore::Header header;
  header.magic = SchemaStore::Header::kMagic;
  header.checksum = 10;  // Arbitrary garbage checksum
  filesystem_.DeleteFile(header_file.c_str());
  filesystem_.Write(header_file.c_str(), &header, sizeof(header));

  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  // Everything looks fine, ground truth and derived data
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema_));
  EXPECT_THAT(schema_store->GetSchemaTypeId("email"), IsOkAndHolds(0));
}

TEST_F(SchemaStoreTest, CreateNoPreviousSchemaOk) {
  EXPECT_THAT(SchemaStore::Create(&filesystem_, test_dir_), IsOk());
}

TEST_F(SchemaStoreTest, CreateWithPreviousSchemaOk) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  SchemaStore::SetSchemaResult result;
  result.success = true;
  EXPECT_THAT(schema_store->SetSchema(schema_),
              IsOkAndHolds(EqualsSetSchemaResult(result)));

  schema_store.reset();
  EXPECT_THAT(SchemaStore::Create(&filesystem_, test_dir_), IsOk());
}

TEST_F(SchemaStoreTest, MultipleCreateOk) {
  DocumentProto document;
  document.set_schema("email");
  auto properties = document.add_properties();
  properties->set_name("subject");
  properties->add_string_values("subject_content");

  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  SchemaStore::SetSchemaResult result;
  result.success = true;
  EXPECT_THAT(schema_store->SetSchema(schema_),
              IsOkAndHolds(EqualsSetSchemaResult(result)));

  // Verify that our in-memory structures are ok
  EXPECT_THAT(schema_store->GetSchemaTypeConfig("email"),
              IsOkAndHolds(Pointee(EqualsProto(schema_.types(0)))));
  ICING_ASSERT_OK_AND_ASSIGN(std::vector<Section> sections,
                             schema_store->ExtractSections(document));
  EXPECT_THAT(sections[0].content, ElementsAre("subject_content"));

  // Verify that our persisted data is ok
  EXPECT_THAT(schema_store->GetSchemaTypeId("email"), IsOkAndHolds(0));

  schema_store.reset();
  ICING_ASSERT_OK_AND_ASSIGN(schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  // Verify that our in-memory structures are ok
  EXPECT_THAT(schema_store->GetSchemaTypeConfig("email"),
              IsOkAndHolds(Pointee(EqualsProto(schema_.types(0)))));

  ICING_ASSERT_OK_AND_ASSIGN(sections, schema_store->ExtractSections(document));
  EXPECT_THAT(sections[0].content, ElementsAre("subject_content"));

  // Verify that our persisted data is ok
  EXPECT_THAT(schema_store->GetSchemaTypeId("email"), IsOkAndHolds(0));
}

TEST_F(SchemaStoreTest, SetNewSchemaOk) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  // Set it for the first time
  SchemaStore::SetSchemaResult result;
  result.success = true;
  EXPECT_THAT(schema_store->SetSchema(schema_),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema_));
}

TEST_F(SchemaStoreTest, SetSameSchemaOk) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  // Set it for the first time
  SchemaStore::SetSchemaResult result;
  result.success = true;
  EXPECT_THAT(schema_store->SetSchema(schema_),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema_));

  // And one more for fun
  EXPECT_THAT(schema_store->SetSchema(schema_),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema_));
}

TEST_F(SchemaStoreTest, SetIncompatibleSchemaOk) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  // Set it for the first time
  SchemaStore::SetSchemaResult result;
  result.success = true;
  EXPECT_THAT(schema_store->SetSchema(schema_),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema_));

  // Make the schema incompatible by removing a type.
  schema_.clear_types();

  // Set the incompatible schema
  result.success = false;
  result.schema_types_deleted_by_name.emplace("email");
  result.schema_types_deleted_by_id.emplace(0);
  EXPECT_THAT(schema_store->SetSchema(schema_),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
}

TEST_F(SchemaStoreTest, SetSchemaWithAddedTypeOk) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  SchemaProto schema;
  auto type = schema.add_types();
  type->set_schema_type("email");

  // Set it for the first time
  SchemaStore::SetSchemaResult result;
  result.success = true;
  EXPECT_THAT(schema_store->SetSchema(schema),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));

  // Add a type, shouldn't affect the index or cached SchemaTypeIds
  type = schema.add_types();
  type->set_schema_type("new_type");

  // Set the compatible schema
  EXPECT_THAT(schema_store->SetSchema(schema),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));
}

TEST_F(SchemaStoreTest, SetSchemaWithDeletedTypeOk) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  SchemaProto schema;
  auto type = schema.add_types();
  type->set_schema_type("email");
  type = schema.add_types();
  type->set_schema_type("message");

  // Set it for the first time
  SchemaStore::SetSchemaResult result;
  result.success = true;
  EXPECT_THAT(schema_store->SetSchema(schema),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));

  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId old_email_schema_type_id,
                             schema_store->GetSchemaTypeId("email"));
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId old_message_schema_type_id,
                             schema_store->GetSchemaTypeId("message"));

  // Remove "email" type, this also changes previous SchemaTypeIds
  schema.Clear();
  type = schema.add_types();
  type->set_schema_type("message");

  SchemaStore::SetSchemaResult incompatible_result;
  incompatible_result.success = false;
  incompatible_result.old_schema_type_ids_changed.emplace(
      old_message_schema_type_id);
  incompatible_result.schema_types_deleted_by_name.emplace("email");
  incompatible_result.schema_types_deleted_by_id.emplace(
      old_email_schema_type_id);

  // Can't set the incompatible schema
  EXPECT_THAT(schema_store->SetSchema(schema),
              IsOkAndHolds(EqualsSetSchemaResult(incompatible_result)));

  SchemaStore::SetSchemaResult force_result;
  force_result.success = true;
  force_result.old_schema_type_ids_changed.emplace(old_message_schema_type_id);
  force_result.schema_types_deleted_by_name.emplace("email");
  force_result.schema_types_deleted_by_id.emplace(old_email_schema_type_id);

  // Force set the incompatible schema
  EXPECT_THAT(schema_store->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/true),
              IsOkAndHolds(EqualsSetSchemaResult(force_result)));
  ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));
}

TEST_F(SchemaStoreTest, SetSchemaWithReorderedTypesOk) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  SchemaProto schema;
  auto type = schema.add_types();
  type->set_schema_type("email");
  type = schema.add_types();
  type->set_schema_type("message");

  // Set it for the first time
  SchemaStore::SetSchemaResult result;
  result.success = true;
  EXPECT_THAT(schema_store->SetSchema(schema),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));

  // Reorder the types
  schema.clear_types();
  type = schema.add_types();
  type->set_schema_type("message");
  type = schema.add_types();
  type->set_schema_type("email");

  // Since we assign SchemaTypeIds based on order in the SchemaProto, this will
  // cause SchemaTypeIds to change
  result.old_schema_type_ids_changed.emplace(0);  // Old SchemaTypeId of "email"
  result.old_schema_type_ids_changed.emplace(
      1);  // Old SchemaTypeId of "message"

  // Set the compatible schema
  EXPECT_THAT(schema_store->SetSchema(schema),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));
}

TEST_F(SchemaStoreTest, SetSchemaThatRequiresReindexingOk) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  SchemaProto schema;
  auto type = schema.add_types();
  type->set_schema_type("email");

  // Add an unindexed property
  auto property = type->add_properties();
  property->set_property_name("subject");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  // Set it for the first time
  SchemaStore::SetSchemaResult result;
  result.success = true;
  EXPECT_THAT(schema_store->SetSchema(schema),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));

  // Make a previously unindexed property indexed
  property = schema.mutable_types(0)->mutable_properties(0);
  property->mutable_indexing_config()->set_term_match_type(
      TermMatchType::EXACT_ONLY);
  property->mutable_indexing_config()->set_tokenizer_type(
      IndexingConfig::TokenizerType::PLAIN);

  // With a new indexed property, we'll need to reindex
  result.index_incompatible = true;

  // Set the compatible schema
  EXPECT_THAT(schema_store->SetSchema(schema),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));
}

TEST_F(SchemaStoreTest, SetSchemaWithIncompatibleTypesOk) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  SchemaProto schema;
  auto type = schema.add_types();
  type->set_schema_type("email");

  // Add a STRING property
  auto property = type->add_properties();
  property->set_property_name("subject");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  // Set it for the first time
  SchemaStore::SetSchemaResult result;
  result.success = true;
  EXPECT_THAT(schema_store->SetSchema(schema),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));

  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId old_email_schema_type_id,
                             schema_store->GetSchemaTypeId("email"));

  // Make a previously STRING property into DOUBLE
  property = schema.mutable_types(0)->mutable_properties(0);
  property->set_data_type(PropertyConfigProto::DataType::DOUBLE);

  SchemaStore::SetSchemaResult incompatible_result;
  incompatible_result.success = false;
  incompatible_result.schema_types_incompatible_by_name.emplace("email");
  incompatible_result.schema_types_incompatible_by_id.emplace(
      old_email_schema_type_id);

  // Can't set the incompatible schema
  EXPECT_THAT(schema_store->SetSchema(schema),
              IsOkAndHolds(EqualsSetSchemaResult(incompatible_result)));

  SchemaStore::SetSchemaResult force_result;
  force_result.success = true;
  force_result.schema_types_incompatible_by_name.emplace("email");
  force_result.schema_types_incompatible_by_id.emplace(
      old_email_schema_type_id);

  // Force set the incompatible schema
  EXPECT_THAT(schema_store->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/true),
              IsOkAndHolds(EqualsSetSchemaResult(force_result)));
  ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));
}

TEST_F(SchemaStoreTest, GetSchemaTypeId) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  schema_.clear_types();

  // Add a few schema types
  const std::string first_type = "first";
  auto type = schema_.add_types();
  type->set_schema_type(first_type);

  const std::string second_type = "second";
  type = schema_.add_types();
  type->set_schema_type(second_type);

  // Set it for the first time
  SchemaStore::SetSchemaResult result;
  result.success = true;
  EXPECT_THAT(schema_store->SetSchema(schema_),
              IsOkAndHolds(EqualsSetSchemaResult(result)));

  EXPECT_THAT(schema_store->GetSchemaTypeId(first_type), IsOkAndHolds(0));
  EXPECT_THAT(schema_store->GetSchemaTypeId(second_type), IsOkAndHolds(1));
}

TEST_F(SchemaStoreTest, ComputeChecksumDefaultOnEmptySchemaStore) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  Crc32 default_checksum;
  EXPECT_THAT(schema_store->ComputeChecksum(), IsOkAndHolds(default_checksum));
}

TEST_F(SchemaStoreTest, ComputeChecksumSameBetweenCalls) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  SchemaProto foo_schema;
  auto type_config = foo_schema.add_types();
  type_config->set_schema_type("foo");

  ICING_EXPECT_OK(schema_store->SetSchema(foo_schema));

  ICING_ASSERT_OK_AND_ASSIGN(Crc32 checksum, schema_store->ComputeChecksum());

  // Calling it again doesn't change the checksum
  EXPECT_THAT(schema_store->ComputeChecksum(), IsOkAndHolds(checksum));
}

TEST_F(SchemaStoreTest, ComputeChecksumSameAcrossInstances) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  SchemaProto foo_schema;
  auto type_config = foo_schema.add_types();
  type_config->set_schema_type("foo");

  ICING_EXPECT_OK(schema_store->SetSchema(foo_schema));

  ICING_ASSERT_OK_AND_ASSIGN(Crc32 checksum, schema_store->ComputeChecksum());

  // Destroy the previous instance and recreate SchemaStore
  schema_store.reset();

  ICING_ASSERT_OK_AND_ASSIGN(schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));
  EXPECT_THAT(schema_store->ComputeChecksum(), IsOkAndHolds(checksum));
}

TEST_F(SchemaStoreTest, ComputeChecksumChangesOnModification) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  SchemaProto foo_schema;
  auto type_config = foo_schema.add_types();
  type_config->set_schema_type("foo");

  ICING_EXPECT_OK(schema_store->SetSchema(foo_schema));

  ICING_ASSERT_OK_AND_ASSIGN(Crc32 checksum, schema_store->ComputeChecksum());

  // Modifying the SchemaStore changes the checksum
  SchemaProto foo_bar_schema;
  type_config = foo_bar_schema.add_types();
  type_config->set_schema_type("foo");
  type_config = foo_bar_schema.add_types();
  type_config->set_schema_type("bar");

  ICING_EXPECT_OK(schema_store->SetSchema(foo_bar_schema));

  EXPECT_THAT(schema_store->ComputeChecksum(), IsOkAndHolds(Not(Eq(checksum))));
}

TEST_F(SchemaStoreTest, PersistToDiskFineForEmptySchemaStore) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  // Persisting is fine and shouldn't affect anything
  ICING_EXPECT_OK(schema_store->PersistToDisk());
}

TEST_F(SchemaStoreTest, PersistToDiskPreservesAcrossInstances) {
  ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<SchemaStore> schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));

  SchemaProto schema;
  auto type_config = schema.add_types();
  type_config->set_schema_type("foo");

  ICING_EXPECT_OK(schema_store->SetSchema(schema));

  // Persisting shouldn't change anything
  ICING_EXPECT_OK(schema_store->PersistToDisk());

  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));

  // Modify the schema so that something different is persisted next time
  type_config = schema.add_types();
  type_config->set_schema_type("bar");
  ICING_EXPECT_OK(schema_store->SetSchema(schema));

  // Should also persist on destruction
  schema_store.reset();

  // And we get the same schema back on reinitialization
  ICING_ASSERT_OK_AND_ASSIGN(schema_store,
                             SchemaStore::Create(&filesystem_, test_dir_));
  ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));
}

}  // namespace

}  // namespace lib
}  // namespace icing
