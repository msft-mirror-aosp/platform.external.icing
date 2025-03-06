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

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "icing/text_classifier/lib3/utils/base/statusor.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/document-builder.h"
#include "icing/feature-flags.h"
#include "icing/file/file-backed-proto.h"
#include "icing/file/filesystem.h"
#include "icing/file/mock-filesystem.h"
#include "icing/file/version-util.h"
#include "icing/portable/equals-proto.h"
#include "icing/proto/debug.pb.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/logging.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/storage.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/section.h"
#include "icing/store/document-filter-data.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/crc32.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Not;
using ::testing::Pair;
using ::testing::Pointee;
using ::testing::Return;
using ::testing::SizeIs;
using ::testing::UnorderedElementsAre;

constexpr int64_t kDefaultTimestamp = 12345678;

class SchemaStoreTest : public ::testing::Test {
 protected:
  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    test_dir_ = GetTestTempDir() + "/icing";
    schema_store_dir_ = test_dir_ + "/schema_store";
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());

    schema_ = SchemaBuilder()
                  .AddType(SchemaTypeConfigBuilder()
                               .SetType("email")
                               .AddProperty(
                                   // Add an indexed property so we generate
                                   // section metadata on it
                                   PropertyConfigBuilder()
                                       .SetName("subject")
                                       .SetDataTypeString(TERM_MATCH_EXACT,
                                                          TOKENIZER_PLAIN)
                                       .SetCardinality(CARDINALITY_OPTIONAL))
                               .AddProperty(
                                   PropertyConfigBuilder()
                                       .SetName("timestamp")
                                       .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                       .SetCardinality(CARDINALITY_OPTIONAL)
                                       .SetScorableType(SCORABLE_TYPE_ENABLED)))
                  .Build();
  }

  void TearDown() override {
    // Check that the schema store directory is the *only* directory in the
    // schema_store_dir_. IOW, ensure that all temporary directories have been
    // properly cleaned up.
    std::vector<std::string> sub_dirs;
    ASSERT_TRUE(filesystem_.ListDirectory(test_dir_.c_str(), &sub_dirs));
    ASSERT_THAT(sub_dirs, ElementsAre("schema_store"));

    // Finally, clean everything up.
    ASSERT_TRUE(filesystem_.DeleteDirectoryRecursively(test_dir_.c_str()));
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  Filesystem filesystem_;
  std::string test_dir_;
  std::string schema_store_dir_;
  SchemaProto schema_;
  FakeClock fake_clock_;
};

SetSchemaRequestProto CreateSetSchemaRequestProto(
    SchemaProto schema, std::string database,
    bool ignore_errors_and_delete_documents) {
  SetSchemaRequestProto set_schema_request;

  *set_schema_request.mutable_schema() = std::move(schema);
  set_schema_request.set_database(std::move(database));
  set_schema_request.set_ignore_errors_and_delete_documents(
      ignore_errors_and_delete_documents);

  return set_schema_request;
}

TEST_F(SchemaStoreTest, CreationWithFilesystemNullPointerShouldFail) {
  EXPECT_THAT(SchemaStore::Create(/*filesystem=*/nullptr, schema_store_dir_,
                                  &fake_clock_, feature_flags_.get()),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(SchemaStoreTest, CreationWithClockNullPointerShouldFail) {
  EXPECT_THAT(SchemaStore::Create(&filesystem_, schema_store_dir_,
                                  /*clock=*/nullptr, feature_flags_.get()),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(SchemaStoreTest, CreationWithFeatureFlagsNullPointerShouldFail) {
  EXPECT_THAT(SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                                  /*feature_flags=*/nullptr),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(SchemaStoreTest, SchemaStoreMoveConstructible) {
  // Create an instance of SchemaStore.
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("type_a").AddProperty(
              PropertyConfigBuilder()
                  .SetName("prop1")
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  ICING_ASSERT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));
  ICING_ASSERT_OK_AND_ASSIGN(Crc32 expected_checksum,
                             schema_store->UpdateChecksum());

  // Move construct an instance of SchemaStore
  SchemaStore move_constructed_schema_store(std::move(*schema_store));
  EXPECT_THAT(move_constructed_schema_store.GetSchema(),
              IsOkAndHolds(Pointee(EqualsProto(schema))));
  EXPECT_THAT(move_constructed_schema_store.UpdateChecksum(),
              IsOkAndHolds(Eq(expected_checksum)));
  SectionMetadata expected_metadata(/*id_in=*/0, TYPE_STRING, TOKENIZER_PLAIN,
                                    TERM_MATCH_EXACT, NUMERIC_MATCH_UNKNOWN,
                                    EMBEDDING_INDEXING_UNKNOWN,
                                    QUANTIZATION_TYPE_NONE, "prop1");
  EXPECT_THAT(move_constructed_schema_store.GetSectionMetadata("type_a"),
              IsOkAndHolds(Pointee(ElementsAre(expected_metadata))));
}

TEST_F(SchemaStoreTest, SchemaStoreMoveAssignment) {
  // Create an instance of SchemaStore.
  SchemaProto schema1 =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("type_a").AddProperty(
              PropertyConfigBuilder()
                  .SetName("prop1")
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  ICING_ASSERT_OK(schema_store->SetSchema(
      schema1, /*ignore_errors_and_delete_documents=*/false));
  ICING_ASSERT_OK_AND_ASSIGN(Crc32 expected_checksum,
                             schema_store->UpdateChecksum());

  // Construct another instance of SchemaStore
  SchemaProto schema2 =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("type_b").AddProperty(
              PropertyConfigBuilder()
                  .SetName("prop2")
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> move_assigned_schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema2, /*ignore_errors_and_delete_documents=*/false));

  // Move assign the first instance into the second one.
  *move_assigned_schema_store = std::move(*schema_store);
  EXPECT_THAT(move_assigned_schema_store->GetSchema(),
              IsOkAndHolds(Pointee(EqualsProto(schema1))));
  EXPECT_THAT(move_assigned_schema_store->UpdateChecksum(),
              IsOkAndHolds(Eq(expected_checksum)));
  SectionMetadata expected_metadata(/*id_in=*/0, TYPE_STRING, TOKENIZER_PLAIN,
                                    TERM_MATCH_EXACT, NUMERIC_MATCH_UNKNOWN,
                                    EMBEDDING_INDEXING_UNKNOWN,
                                    QUANTIZATION_TYPE_NONE, "prop1");
  EXPECT_THAT(move_assigned_schema_store->GetSectionMetadata("type_a"),
              IsOkAndHolds(Pointee(ElementsAre(expected_metadata))));
}

TEST_F(SchemaStoreTest, CorruptSchemaError) {
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));

    // Set it for the first time
    SchemaStore::SetSchemaResult result;
    result.success = true;
    result.schema_types_new_by_name.insert(schema_.types(0).schema_type());
    EXPECT_THAT(schema_store->SetSchema(
                    schema_, /*ignore_errors_and_delete_documents=*/false),
                IsOkAndHolds(EqualsSetSchemaResult(result)));
    ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                               schema_store->GetSchema());
    EXPECT_THAT(*actual_schema, EqualsProto(schema_));
  }

  // "Corrupt" the  ground truth schema by adding new data to it. This will mess
  // up the checksum of the schema store

  SchemaProto corrupt_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("corrupted"))
          .Build();

  const std::string schema_file =
      absl_ports::StrCat(schema_store_dir_, "/schema.pb");
  const std::string serialized_schema = corrupt_schema.SerializeAsString();

  filesystem_.Write(schema_file.c_str(), serialized_schema.data(),
                    serialized_schema.size());

  // If ground truth was corrupted, we won't know what to do
  EXPECT_THAT(SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                                  feature_flags_.get()),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL));
}

TEST_F(SchemaStoreTest, RecoverCorruptDerivedFileOk) {
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));

    // Set it for the first time
    SchemaStore::SetSchemaResult result;
    result.success = true;
    result.schema_types_new_by_name.insert(schema_.types(0).schema_type());
    EXPECT_THAT(schema_store->SetSchema(
                    schema_, /*ignore_errors_and_delete_documents=*/false),
                IsOkAndHolds(EqualsSetSchemaResult(result)));
    ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                               schema_store->GetSchema());
    EXPECT_THAT(*actual_schema, EqualsProto(schema_));

    EXPECT_THAT(schema_store->GetSchemaTypeId("email"), IsOkAndHolds(0));

    // Scorable property manager working as expected.
    EXPECT_THAT(schema_store->GetOrderedScorablePropertyInfo(
                    /*schema_type_id=*/0),
                IsOkAndHolds(Pointee(ElementsAre(
                    EqualsScorablePropertyInfo("timestamp", TYPE_INT64)))));
    EXPECT_THAT(schema_store->GetScorablePropertyIndex(
                    /*schema_type_id=*/0,
                    /*property_path=*/"timestamp"),
                IsOkAndHolds(0));
  }

  // "Corrupt" the derived SchemaTypeIds by deleting the entire directory. This
  // will mess up the initialization of schema store, causing everything to be
  // regenerated from ground truth

  const std::string schema_type_mapper_dir =
      absl_ports::StrCat(schema_store_dir_, "/schema_type_mapper");
  filesystem_.DeleteDirectoryRecursively(schema_type_mapper_dir.c_str());

  InitializeStatsProto initialize_stats;
  fake_clock_.SetTimerElapsedMilliseconds(123);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get(), &initialize_stats));
  EXPECT_THAT(initialize_stats.schema_store_recovery_cause(),
              Eq(InitializeStatsProto::IO_ERROR));
  EXPECT_THAT(initialize_stats.schema_store_recovery_latency_ms(), Eq(123));

  // Everything looks fine, ground truth and derived data
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema_));
  EXPECT_THAT(schema_store->GetSchemaTypeId("email"), IsOkAndHolds(0));

  // Scorable property manager working as expected.
  EXPECT_THAT(schema_store->GetOrderedScorablePropertyInfo(
                  /*schema_type_id=*/0),
              IsOkAndHolds(Pointee(ElementsAre(
                  EqualsScorablePropertyInfo("timestamp", TYPE_INT64)))));
  EXPECT_THAT(schema_store->GetScorablePropertyIndex(
                  /*schema_type_id=*/0,
                  /*property_path=*/"timestamp"),
              IsOkAndHolds(0));
}

TEST_F(SchemaStoreTest, RecoverDiscardDerivedFilesOk) {
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));

    // Set it for the first time
    SchemaStore::SetSchemaResult result;
    result.success = true;
    result.schema_types_new_by_name.insert(schema_.types(0).schema_type());
    EXPECT_THAT(schema_store->SetSchema(
                    schema_, /*ignore_errors_and_delete_documents=*/false),
                IsOkAndHolds(EqualsSetSchemaResult(result)));
    ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                               schema_store->GetSchema());
    EXPECT_THAT(*actual_schema, EqualsProto(schema_));

    EXPECT_THAT(schema_store->GetSchemaTypeId("email"), IsOkAndHolds(0));

    // Scorable property manager working as expected.
    EXPECT_THAT(schema_store->GetOrderedScorablePropertyInfo(
                    /*schema_type_id=*/0),
                IsOkAndHolds(Pointee(ElementsAre(
                    EqualsScorablePropertyInfo("timestamp", TYPE_INT64)))));
    EXPECT_THAT(schema_store->GetScorablePropertyIndex(
                    /*schema_type_id=*/0,
                    /*property_path=*/"timestamp"),
                IsOkAndHolds(0));
  }

  ICING_ASSERT_OK(
      SchemaStore::DiscardDerivedFiles(&filesystem_, schema_store_dir_));

  InitializeStatsProto initialize_stats;
  fake_clock_.SetTimerElapsedMilliseconds(123);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get(), &initialize_stats));
  EXPECT_THAT(initialize_stats.schema_store_recovery_cause(),
              Eq(InitializeStatsProto::IO_ERROR));
  EXPECT_THAT(initialize_stats.schema_store_recovery_latency_ms(), Eq(123));

  // Everything looks fine, ground truth and derived data
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema_));
  EXPECT_THAT(schema_store->GetSchemaTypeId("email"), IsOkAndHolds(0));

  // Scorable property manager working as expected.
  EXPECT_THAT(schema_store->GetOrderedScorablePropertyInfo(
                  /*schema_type_id=*/0),
              IsOkAndHolds(Pointee(ElementsAre(
                  EqualsScorablePropertyInfo("timestamp", TYPE_INT64)))));
  EXPECT_THAT(schema_store->GetScorablePropertyIndex(
                  /*schema_type_id=*/0,
                  /*property_path=*/"timestamp"),
              IsOkAndHolds(0));
}

TEST_F(SchemaStoreTest, RecoverBadChecksumOk) {
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));

    // Set it for the first time
    SchemaStore::SetSchemaResult result;
    result.success = true;
    result.schema_types_new_by_name.insert(schema_.types(0).schema_type());
    EXPECT_THAT(schema_store->SetSchema(
                    schema_, /*ignore_errors_and_delete_documents=*/false),
                IsOkAndHolds(EqualsSetSchemaResult(result)));
    ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                               schema_store->GetSchema());
    EXPECT_THAT(*actual_schema, EqualsProto(schema_));

    EXPECT_THAT(schema_store->GetSchemaTypeId("email"), IsOkAndHolds(0));

    // Scorable property manager working as expected.
    EXPECT_THAT(schema_store->GetOrderedScorablePropertyInfo(
                    /*schema_type_id=*/0),
                IsOkAndHolds(Pointee(ElementsAre(
                    EqualsScorablePropertyInfo("timestamp", TYPE_INT64)))));
    EXPECT_THAT(schema_store->GetScorablePropertyIndex(
                    /*schema_type_id=*/0,
                    /*property_path=*/"timestamp"),
                IsOkAndHolds(0));
  }

  // Change the SchemaStore's header combined checksum so that it won't match
  // the recalculated checksum on initialization. This will force a regeneration
  // of derived files from ground truth.
  const std::string header_file =
      absl_ports::StrCat(schema_store_dir_, "/schema_store_header");
  SchemaStore::LegacyHeader header;
  header.magic = SchemaStore::Header::kMagic;
  header.checksum = 10;  // Arbitrary garbage checksum
  filesystem_.DeleteFile(header_file.c_str());
  filesystem_.Write(header_file.c_str(), &header, sizeof(header));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // Everything looks fine, ground truth and derived data
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema_));
  EXPECT_THAT(schema_store->GetSchemaTypeId("email"), IsOkAndHolds(0));

  // Scorable property manager working as expected.
  EXPECT_THAT(schema_store->GetOrderedScorablePropertyInfo(
                  /*schema_type_id=*/0),
              IsOkAndHolds(Pointee(ElementsAre(
                  EqualsScorablePropertyInfo("timestamp", TYPE_INT64)))));
  EXPECT_THAT(schema_store->GetScorablePropertyIndex(
                  /*schema_type_id=*/0,
                  /*property_path=*/"timestamp"),
              IsOkAndHolds(0));
}

TEST_F(SchemaStoreTest, CreateNoPreviousSchemaOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // The apis to retrieve information about the schema should fail gracefully.
  EXPECT_THAT(store->GetSchema(),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(store->GetSchemaTypeConfig("foo"),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(store->GetSchemaTypeId("foo"),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(store->GetSectionMetadata(/*schema_type_id=*/0, /*section_id=*/0),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(store->GetJoinablePropertyMetadata(/*schema_type_id=*/0,
                                                 /*property_path=*/"A"),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));

  // The apis to extract content from a document should fail gracefully.
  DocumentProto doc;
  PropertyProto* prop = doc.add_properties();
  prop->set_name("name");
  prop->add_string_values("foo bar baz");

  EXPECT_THAT(store->ExtractSections(doc),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  EXPECT_THAT(store->ExtractJoinableProperties(doc),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));

  // The apis to persist and checksum data should succeed.
  EXPECT_THAT(store->UpdateChecksum(), IsOkAndHolds(Crc32()));
  EXPECT_THAT(store->PersistToDisk(), IsOk());
}

TEST_F(SchemaStoreTest, CreateWithPreviousSchemaOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert(schema_.types(0).schema_type());
  EXPECT_THAT(schema_store->SetSchema(
                  schema_, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(result)));

  schema_store.reset();
  EXPECT_THAT(SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                                  feature_flags_.get()),
              IsOk());
}

TEST_F(SchemaStoreTest, MultipleCreateOk) {
  DocumentProto document;
  document.set_schema("email");
  auto subject_property = document.add_properties();
  subject_property->set_name("subject");
  subject_property->add_string_values("subject_content");
  auto timestamp_property = document.add_properties();
  timestamp_property->set_name("timestamp");
  timestamp_property->add_int64_values(kDefaultTimestamp);

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert(schema_.types(0).schema_type());
  EXPECT_THAT(schema_store->SetSchema(
                  schema_, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(result)));

  // Verify that our in-memory structures are ok
  EXPECT_THAT(schema_store->GetSchemaTypeConfig("email"),
              IsOkAndHolds(Pointee(EqualsProto(schema_.types(0)))));
  ICING_ASSERT_OK_AND_ASSIGN(SectionGroup section_group,
                             schema_store->ExtractSections(document));
  EXPECT_THAT(section_group.string_sections[0].content,
              ElementsAre("subject_content"));
  EXPECT_THAT(section_group.integer_sections[0].content,
              ElementsAre(kDefaultTimestamp));

  // Scorable property manager working as expected.
  EXPECT_THAT(schema_store->GetOrderedScorablePropertyInfo(
                  /*schema_type_id=*/0),
              IsOkAndHolds(Pointee(ElementsAre(
                  EqualsScorablePropertyInfo("timestamp", TYPE_INT64)))));
  EXPECT_THAT(schema_store->GetScorablePropertyIndex(
                  /*schema_type_id=*/0,
                  /*property_path=*/"timestamp"),
              IsOkAndHolds(0));

  // Verify that our persisted data is ok
  EXPECT_THAT(schema_store->GetSchemaTypeId("email"), IsOkAndHolds(0));

  schema_store.reset();
  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store, SchemaStore::Create(&filesystem_, schema_store_dir_,
                                        &fake_clock_, feature_flags_.get()));

  // Verify that our in-memory structures are ok
  EXPECT_THAT(schema_store->GetSchemaTypeConfig("email"),
              IsOkAndHolds(Pointee(EqualsProto(schema_.types(0)))));

  ICING_ASSERT_OK_AND_ASSIGN(section_group,
                             schema_store->ExtractSections(document));
  EXPECT_THAT(section_group.string_sections[0].content,
              ElementsAre("subject_content"));
  EXPECT_THAT(section_group.integer_sections[0].content,
              ElementsAre(kDefaultTimestamp));

  // Scorable property manager working as expected.
  EXPECT_THAT(schema_store->GetOrderedScorablePropertyInfo(
                  /*schema_type_id=*/0),
              IsOkAndHolds(Pointee(ElementsAre(
                  EqualsScorablePropertyInfo("timestamp", TYPE_INT64)))));
  EXPECT_THAT(schema_store->GetScorablePropertyIndex(
                  /*schema_type_id=*/0,
                  /*property_path=*/"timestamp"),
              IsOkAndHolds(0));

  // Verify that our persisted data is ok
  EXPECT_THAT(schema_store->GetSchemaTypeId("email"), IsOkAndHolds(0));
}

TEST_F(SchemaStoreTest, SetNewSchemaOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // Set it for the first time
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert(schema_.types(0).schema_type());
  EXPECT_THAT(schema_store->SetSchema(
                  schema_, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema_));
}

TEST_F(SchemaStoreTest, SetNewSchemaInDifferentDatabaseOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get(),
                          /*initialize_stats=*/nullptr));

  SchemaProto db1_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db1_email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .Build();
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert("db1_email");
  result.schema_types_new_by_name.insert("db1_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db1_schema, /*database=*/"db1",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  EXPECT_THAT(schema_store->GetSchema(),
              IsOkAndHolds(Pointee(EqualsProto(db1_schema))));
  EXPECT_THAT(schema_store->GetSchema("db1"),
              IsOkAndHolds(EqualsProto(db1_schema)));

  // Set a schema in a different database
  SchemaProto db2_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .Build();
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_new_by_name.insert("db2_email");
  result.schema_types_new_by_name.insert("db2_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db2_schema, /*database=*/"db2",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));

  // Check the full schema. Databases that are updated last are appended to the
  // schema proto
  SchemaProto expected_full_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db1_email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .Build();
  EXPECT_THAT(schema_store->GetSchema(),
              IsOkAndHolds(Pointee(EqualsProto(expected_full_schema))));
  EXPECT_THAT(schema_store->GetSchema("db1"),
              IsOkAndHolds(EqualsProto(db1_schema)));
  EXPECT_THAT(schema_store->GetSchema("db2"),
              IsOkAndHolds(EqualsProto(db2_schema)));
}

TEST_F(SchemaStoreTest, SetEmptyDatabaseSchemaOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("email"))
          .AddType(SchemaTypeConfigBuilder().SetType("message"))
          .Build();
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert("email");
  result.schema_types_new_by_name.insert("message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  schema, /*database=*/"",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  EXPECT_THAT(schema_store->GetSchema(),
              IsOkAndHolds(Pointee(EqualsProto(schema))));
  EXPECT_THAT(schema_store->GetSchema(""), IsOkAndHolds(EqualsProto(schema)));

  // Reset the schema. This should still reset the empty schema, and replace
  // the existing 2 types.
  schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("email_v2").SetDatabase(""))
          .AddType(
              SchemaTypeConfigBuilder().SetType("message_v2").SetDatabase(""))
          .Build();
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_new_by_name.insert("email_v2");
  result.schema_types_new_by_name.insert("message_v2");
  result.schema_types_deleted_by_name.insert("email");
  result.schema_types_deleted_by_name.insert("message");
  result.schema_types_deleted_by_id.insert(0);
  result.schema_types_deleted_by_id.insert(1);

  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  schema, /*database=*/"",
                  /*ignore_errors_and_delete_documents=*/true)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  EXPECT_THAT(schema_store->GetSchema(),
              IsOkAndHolds(Pointee(EqualsProto(schema))));
  EXPECT_THAT(schema_store->GetSchema(""), IsOkAndHolds(EqualsProto(schema)));
}

TEST_F(SchemaStoreTest, SetSameSchemaOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // Set it for the first time
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert(schema_.types(0).schema_type());
  EXPECT_THAT(schema_store->SetSchema(
                  schema_, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema_));

  // And one more for fun
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  EXPECT_THAT(schema_store->SetSchema(
                  schema_, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema_));
}

TEST_F(SchemaStoreTest, SetSameDatabaseSchemaOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get(),
                          /*initialize_stats=*/nullptr));

  // Set schema for the first time
  SchemaProto db1_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db1_email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .Build();
  SchemaProto db2_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .Build();
  SchemaProto expected_full_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db1_email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .Build();
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert("db1_email");
  result.schema_types_new_by_name.insert("db1_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db1_schema, /*database=*/"db1",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_new_by_name.insert("db2_email");
  result.schema_types_new_by_name.insert("db2_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db2_schema, /*database=*/"db2",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_full_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_full_schema, EqualsProto(expected_full_schema));

  // Reset db1 with the same SchemaProto. The schema should be exactly the same.
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db1_schema, /*database=*/"db1",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));

  // Check the schema, this should not have changed
  EXPECT_THAT(schema_store->GetSchema(),
              IsOkAndHolds(Pointee(EqualsProto(expected_full_schema))));
  EXPECT_THAT(schema_store->GetSchema("db1"),
              IsOkAndHolds(EqualsProto(db1_schema)));
  EXPECT_THAT(schema_store->GetSchema("db2"),
              IsOkAndHolds(EqualsProto(db2_schema)));
}

TEST_F(SchemaStoreTest, SetDatabaseReorderedTypesNoChange) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get(),
                          /*initialize_stats=*/nullptr));

  // Set schema for the first time
  SchemaProto db1_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db1_email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .Build();
  SchemaProto db2_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .Build();
  SchemaProto db3_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db3_email").SetDatabase("db3"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db3_message")
                       .SetDatabase("db3"))
          .Build();
  SchemaProto expected_full_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db1_email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .AddType(
              SchemaTypeConfigBuilder().SetType("db3_email").SetDatabase("db3"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db3_message")
                       .SetDatabase("db3"))
          .Build();

  // Set schema for db1
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert("db1_email");
  result.schema_types_new_by_name.insert("db1_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db1_schema, /*database=*/"db1",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  // Set schema for db2
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_new_by_name.insert("db2_email");
  result.schema_types_new_by_name.insert("db2_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db2_schema, /*database=*/"db2",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  // Set schema for db3
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_new_by_name.insert("db3_email");
  result.schema_types_new_by_name.insert("db3_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db3_schema, /*database=*/"db3",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  // Verify schema.
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_full_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_full_schema, EqualsProto(expected_full_schema));

  // Reset db2 with the types reordered. This should not change the existing
  // schema in any way.
  SchemaProto reordered_db2_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .Build();
  result = SchemaStore::SetSchemaResult();
  result.success = true;

  libtextclassifier3::StatusOr<SchemaStore::SetSchemaResult> actual_result =
      schema_store->SetSchema(CreateSetSchemaRequestProto(
          reordered_db2_schema, /*database=*/"db2",
          /*ignore_errors_and_delete_documents=*/false));
  EXPECT_THAT(actual_result, IsOkAndHolds(EqualsSetSchemaResult(result)));
  EXPECT_THAT(actual_result.ValueOrDie().old_schema_type_ids_changed,
              IsEmpty());

  // Check the schema
  EXPECT_THAT(schema_store->GetSchema(),
              IsOkAndHolds(Pointee(EqualsProto(expected_full_schema))));
  EXPECT_THAT(schema_store->GetSchema("db1"),
              IsOkAndHolds(EqualsProto(db1_schema)));

  libtextclassifier3::StatusOr<SchemaProto> actual_db2_schema =
      schema_store->GetSchema("db2");
  EXPECT_THAT(actual_db2_schema, IsOkAndHolds(EqualsProto(db2_schema)));
  EXPECT_THAT(actual_db2_schema.ValueOrDie(),
              Not(EqualsProto(reordered_db2_schema)));

  EXPECT_THAT(schema_store->GetSchema("db3"),
              IsOkAndHolds(EqualsProto(db3_schema)));
}

TEST_F(SchemaStoreTest, SetDatabaseAddedTypesPreservesSchemaTypeIds) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get(),
                          /*initialize_stats=*/nullptr));

  // Set schema for the first time
  SchemaProto db1_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db1_email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .Build();
  SchemaProto db2_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .Build();
  SchemaProto db3_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db3_email").SetDatabase("db3"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db3_message")
                       .SetDatabase("db3"))
          .Build();
  SchemaProto expected_full_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db1_email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .AddType(
              SchemaTypeConfigBuilder().SetType("db3_email").SetDatabase("db3"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db3_message")
                       .SetDatabase("db3"))
          .Build();

  // Set schema for db1
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert("db1_email");
  result.schema_types_new_by_name.insert("db1_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db1_schema, /*database=*/"db1",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  // Set schema for db2
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_new_by_name.insert("db2_email");
  result.schema_types_new_by_name.insert("db2_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db2_schema, /*database=*/"db2",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  // Set schema for db3
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_new_by_name.insert("db3_email");
  result.schema_types_new_by_name.insert("db3_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db3_schema, /*database=*/"db3",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  // Verify schema.
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_full_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_full_schema, EqualsProto(expected_full_schema));

  // Reset db2 and add a type. The added type should be appended to the end of
  // the SchemaProto, and SchemaTypeIds for db1 and db3 should not change.
  //
  // Whether or not the SchemaTypeIds for db2 change depends on the order in the
  // new db2 SchemaProto (in this case, existing type's order and ids do not
  // change)
  db2_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_recipient")
                       .SetDatabase("db2"))
          .Build();
  expected_full_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()  // db1 types
                       .SetType("db1_email")
                       .SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()  // db2 types
                       .SetType("db2_email")
                       .SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()  // db3 types
                       .SetType("db3_email")
                       .SetDatabase("db3"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db3_message")
                       .SetDatabase("db3"))
          .AddType(SchemaTypeConfigBuilder()  // Additional db2 type is appended
                                              // at the end
                       .SetType("db2_recipient")
                       .SetDatabase("db2"))
          .Build();
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_new_by_name.insert("db2_recipient");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db2_schema, /*database=*/"db2",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));

  // Check the schema
  EXPECT_THAT(schema_store->GetSchema(),
              IsOkAndHolds(Pointee(EqualsProto(expected_full_schema))));
  EXPECT_THAT(schema_store->GetSchema("db1"),
              IsOkAndHolds(EqualsProto(db1_schema)));
  EXPECT_THAT(schema_store->GetSchema("db2"),
              IsOkAndHolds(EqualsProto(db2_schema)));
  EXPECT_THAT(schema_store->GetSchema("db3"),
              IsOkAndHolds(EqualsProto(db3_schema)));
}

TEST_F(SchemaStoreTest, SetDatabaseDeletedTypesOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get(),
                          /*initialize_stats=*/nullptr));

  // Set schema for the first time
  SchemaProto db1_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db1_email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .Build();
  SchemaProto db2_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .Build();
  SchemaProto db3_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db3_email").SetDatabase("db3"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db3_message")
                       .SetDatabase("db3"))
          .Build();

  // Set schema for db1
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert("db1_email");
  result.schema_types_new_by_name.insert("db1_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db1_schema, /*database=*/"db1",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  // Set schema for db2
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_new_by_name.insert("db2_email");
  result.schema_types_new_by_name.insert("db2_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db2_schema, /*database=*/"db2",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  // Set schema for db3
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_new_by_name.insert("db3_email");
  result.schema_types_new_by_name.insert("db3_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db3_schema, /*database=*/"db3",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  // Set schema again for db2 and add a type. The added type should be appended
  // to the end of the SchemaProto.
  db2_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_recipient")
                       .SetDatabase("db2"))
          .Build();
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_new_by_name.insert("db2_recipient");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db2_schema, /*database=*/"db2",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  SchemaProto expected_full_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_email")  // SchemaTypeId 0
                       .SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")  // SchemaTypeId 1
                       .SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_email")  // SchemaTypeId 2
                       .SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")  // SchemaTypeId 3
                       .SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db3_email")  // SchemaTypeId 4
                       .SetDatabase("db3"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db3_message")  // SchemaTypeId 5
                       .SetDatabase("db3"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_recipient")  // SchemaTypeId 6
                       .SetDatabase("db2"))
          .Build();
  // Verify schema.
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_full_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_full_schema, EqualsProto(expected_full_schema));

  // Reset db2 and delete some types. All types that were originally added after
  // db2 should have their type ids changed.
  db2_schema = SchemaBuilder()
                   .AddType(SchemaTypeConfigBuilder()
                                .SetType("db2_message")
                                .SetDatabase("db2"))
                   .Build();
  expected_full_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_email")  // SchemaTypeId 0
                       .SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")  // SchemaTypeId 1
                       .SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")  // SchemaTypeId 2
                       .SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db3_email")  // SchemaTypeId 3
                       .SetDatabase("db3"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db3_message")  // SchemaTypeId 4
                       .SetDatabase("db3"))
          .Build();
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_deleted_by_name.insert("db2_email");
  result.schema_types_deleted_by_name.insert("db2_recipient");
  result.schema_types_deleted_by_id.insert(2);   // db2_email
  result.schema_types_deleted_by_id.insert(6);   // db2_recipient
  result.old_schema_type_ids_changed.insert(3);  // db2_message
  result.old_schema_type_ids_changed.insert(4);  // db3_email
  result.old_schema_type_ids_changed.insert(5);  // db3_message
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db2_schema, /*database=*/"db2",
                  /*ignore_errors_and_delete_documents=*/true)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));

  // Check the schema
  EXPECT_THAT(schema_store->GetSchema(),
              IsOkAndHolds(Pointee(EqualsProto(expected_full_schema))));
  EXPECT_THAT(schema_store->GetSchema("db1"),
              IsOkAndHolds(EqualsProto(db1_schema)));
  EXPECT_THAT(schema_store->GetSchema("db2"),
              IsOkAndHolds(EqualsProto(db2_schema)));
  EXPECT_THAT(schema_store->GetSchema("db3"),
              IsOkAndHolds(EqualsProto(db3_schema)));
}

TEST_F(SchemaStoreTest, SetEmptySchemaClearsDatabase) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get(),
                          /*initialize_stats=*/nullptr));

  // Set schema for the first time
  SchemaProto db1_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db1_email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .Build();
  SchemaProto db2_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .Build();
  SchemaProto db3_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db3_email").SetDatabase("db3"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db3_message")
                       .SetDatabase("db3"))
          .Build();

  // Set schema for db1
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert("db1_email");
  result.schema_types_new_by_name.insert("db1_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db1_schema, /*database=*/"db1",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  // Set schema for db2
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_new_by_name.insert("db2_email");
  result.schema_types_new_by_name.insert("db2_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db2_schema, /*database=*/"db2",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  // Set schema for db3
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_new_by_name.insert("db3_email");
  result.schema_types_new_by_name.insert("db3_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db3_schema, /*database=*/"db3",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  // Verify schema.
  SchemaProto expected_full_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_email")  // SchemaTypeId 0
                       .SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")  // SchemaTypeId 1
                       .SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_email")  // SchemaTypeId 2
                       .SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")  // SchemaTypeId 3
                       .SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db3_email")  // SchemaTypeId 4
                       .SetDatabase("db3"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db3_message")  // SchemaTypeId 5
                       .SetDatabase("db3"))
          .Build();
  EXPECT_THAT(schema_store->GetSchema(),
              IsOkAndHolds(Pointee(EqualsProto(expected_full_schema))));

  // Set an empty schema for db2. This deletes all types from db2, and changes
  // the type ids of types from db3 because they appear after db2 in the
  // original schema.
  db2_schema = SchemaProto();
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_deleted_by_name.insert("db2_email");
  result.schema_types_deleted_by_name.insert("db2_message");
  result.schema_types_deleted_by_id.insert(2);   // db2_email
  result.schema_types_deleted_by_id.insert(3);   // db2_message
  result.old_schema_type_ids_changed.insert(4);  // db3_email
  result.old_schema_type_ids_changed.insert(5);  // db3_message
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db2_schema, /*database=*/"db2",
                  /*ignore_errors_and_delete_documents=*/true)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));

  // Check the schema. Schemas for db1 and db3 should be unchanged.
  EXPECT_THAT(schema_store->GetSchema("db1"),
              IsOkAndHolds(EqualsProto(db1_schema)));
  EXPECT_THAT(schema_store->GetSchema("db3"),
              IsOkAndHolds(EqualsProto(db3_schema)));

  // GetSchema for db2 should return NotFoundError
  EXPECT_THAT(schema_store->GetSchema("db2"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  expected_full_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db1_email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .AddType(
              SchemaTypeConfigBuilder().SetType("db3_email").SetDatabase("db3"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db3_message")
                       .SetDatabase("db3"))
          .Build();
  EXPECT_THAT(schema_store->GetSchema(),
              IsOkAndHolds(Pointee(EqualsProto(expected_full_schema))));
}

TEST_F(SchemaStoreTest, SetIncompatibleSchemaOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // Set it for the first time
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert(schema_.types(0).schema_type());
  EXPECT_THAT(schema_store->SetSchema(
                  schema_, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema_));

  // Make the schema incompatible by removing a type.
  schema_.clear_types();

  // Set the incompatible schema
  result = SchemaStore::SetSchemaResult();
  result.success = false;
  result.schema_types_deleted_by_name.emplace("email");
  result.schema_types_deleted_by_id.emplace(0);
  EXPECT_THAT(schema_store->SetSchema(
                  schema_, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
}

TEST_F(SchemaStoreTest, SetIncompatibleInDifferentDatabaseOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get(),
                          /*initialize_stats=*/nullptr));

  // Set schema for the first time
  SchemaProto db1_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db1_email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .Build();
  SchemaProto db2_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .Build();
  SchemaProto expected_full_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db1_email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .Build();
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert("db1_email");
  result.schema_types_new_by_name.insert("db1_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db1_schema, /*database=*/"db1",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_new_by_name.insert("db2_email");
  result.schema_types_new_by_name.insert("db2_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db2_schema, /*database=*/"db2",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_full_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_full_schema, EqualsProto(expected_full_schema));

  // Make db2 incompatible by changing a type name
  SchemaProto db2_schema_incompatible =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_recipient")
                       .SetDatabase("db2"))
          .Build();
  result = SchemaStore::SetSchemaResult();
  result.success = false;
  result.schema_types_deleted_by_name.insert("db2_message");
  result.schema_types_new_by_name.insert("db2_recipient");
  result.schema_types_deleted_by_id.insert(3);  // db2_message
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db2_schema_incompatible, /*database=*/"db2",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));

  // Check the schema, this should not have changed
  EXPECT_THAT(schema_store->GetSchema(),
              IsOkAndHolds(Pointee(EqualsProto(expected_full_schema))));
  EXPECT_THAT(schema_store->GetSchema("db1"),
              IsOkAndHolds(EqualsProto(db1_schema)));
  EXPECT_THAT(schema_store->GetSchema("db2"),
              IsOkAndHolds(EqualsProto(db2_schema)));
}

TEST_F(SchemaStoreTest, SetInvalidInDifferentDatabaseFails) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get(),
                          /*initialize_stats=*/nullptr));

  // Set schema for the first time
  SchemaProto db1_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db1_email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .Build();
  SchemaProto db2_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .Build();
  SchemaProto expected_full_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db1_email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .Build();
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert("db1_email");
  result.schema_types_new_by_name.insert("db1_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db1_schema, /*database=*/"db1",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_new_by_name.insert("db2_email");
  result.schema_types_new_by_name.insert("db2_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db2_schema, /*database=*/"db2",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_full_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_full_schema, EqualsProto(expected_full_schema));

  // Make db2 invalid by duplicating a property name
  PropertyConfigProto prop =
      PropertyConfigBuilder()
          .SetName("prop0")
          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
          .SetCardinality(CARDINALITY_OPTIONAL)
          .Build();
  SchemaProto db2_schema_incompatible = SchemaBuilder()
                                            .AddType(SchemaTypeConfigBuilder()
                                                         .SetType("db2_email")
                                                         .SetDatabase("db2")
                                                         .AddProperty(prop)
                                                         .AddProperty(prop))
                                            .Build();
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db2_schema_incompatible,
                  /*database=*/"db2",
                  /*ignore_errors_and_delete_documents=*/false)),
              StatusIs(libtextclassifier3::StatusCode::ALREADY_EXISTS));

  // Check the schema, this should not have changed
  EXPECT_THAT(schema_store->GetSchema(),
              IsOkAndHolds(Pointee(EqualsProto(expected_full_schema))));
  EXPECT_THAT(schema_store->GetSchema("db1"),
              IsOkAndHolds(EqualsProto(db1_schema)));
  EXPECT_THAT(schema_store->GetSchema("db2"),
              IsOkAndHolds(EqualsProto(db2_schema)));
}

TEST_F(SchemaStoreTest, SetSchemaWithMultipleDbFails) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get(),
                          /*initialize_stats=*/nullptr));

  SchemaProto combined_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .AddType(
              SchemaTypeConfigBuilder().SetType("db1_email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .Build();
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  combined_schema, /*database=*/"db1",
                  /*ignore_errors_and_delete_documents=*/false)),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(SchemaStoreTest, SetSchemaWithMismatchedDbFails) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get(),
                          /*initialize_stats=*/nullptr));

  SchemaProto schema =
      SchemaBuilder()
          // This type does not explicitly set its database, so it defaults to
          // the empty database.
          .AddType(SchemaTypeConfigBuilder().SetType("db1_email"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .Build();

  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  schema, /*database=*/"db1",
                  /*ignore_errors_and_delete_documents=*/false)),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db1_email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .Build();
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  schema, /*database=*/"db_mismatch",
                  /*ignore_errors_and_delete_documents=*/false)),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("db1_email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .AddType(
              SchemaTypeConfigBuilder().SetType("db2_email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .Build();
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  schema, /*database=*/"",
                  /*ignore_errors_and_delete_documents=*/false)),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(SchemaStoreTest, SetSchemaWithDuplicateTypeNameAcrossDifferentDbFails) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get(),
                          /*initialize_stats=*/nullptr));

  // Set schema for the first time
  SchemaProto db1_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("email").SetDatabase("db1"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1_message")
                       .SetDatabase("db1"))
          .Build();
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert("email");
  result.schema_types_new_by_name.insert("db1_message");
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db1_schema, /*database=*/"db1",
                  /*ignore_errors_and_delete_documents=*/false)),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  EXPECT_THAT(schema_store->GetSchema(),
              IsOkAndHolds(Pointee(EqualsProto(db1_schema))));
  EXPECT_THAT(schema_store->GetSchema("db1"),
              IsOkAndHolds(EqualsProto(db1_schema)));

  // Set schema in db2 with the same type name
  SchemaProto db2_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder().SetType("email").SetDatabase("db2"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2_message")
                       .SetDatabase("db2"))
          .Build();
  EXPECT_THAT(schema_store->SetSchema(CreateSetSchemaRequestProto(
                  db2_schema, /*database=*/"db2",
                  /*ignore_errors_and_delete_documents=*/false)),
              StatusIs(libtextclassifier3::StatusCode::ALREADY_EXISTS));

  // Check schema, this should not have changed
  EXPECT_THAT(schema_store->GetSchema(),
              IsOkAndHolds(Pointee(EqualsProto(db1_schema))));
  EXPECT_THAT(schema_store->GetSchema("db1"),
              IsOkAndHolds(EqualsProto(db1_schema)));
}

TEST_F(SchemaStoreTest, SetSchemaWithAddedTypeOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType("email"))
                           .Build();

  // Set it for the first time
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert("email");
  EXPECT_THAT(schema_store->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));

  // Add a type, shouldn't affect the index or cached SchemaTypeIds
  schema = SchemaBuilder(schema)
               .AddType(SchemaTypeConfigBuilder().SetType("new_type"))
               .Build();

  // Set the compatible schema
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_new_by_name.insert("new_type");
  EXPECT_THAT(schema_store->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));
}

TEST_F(SchemaStoreTest, SetSchemaWithDeletedTypeOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("email"))
          .AddType(SchemaTypeConfigBuilder().SetType("message"))
          .Build();

  // Set it for the first time
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert("email");
  result.schema_types_new_by_name.insert("message");
  EXPECT_THAT(schema_store->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));

  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId old_email_schema_type_id,
                             schema_store->GetSchemaTypeId("email"));
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId old_message_schema_type_id,
                             schema_store->GetSchemaTypeId("message"));

  // Remove "email" type, this also changes previous SchemaTypeIds
  schema = SchemaBuilder()
               .AddType(SchemaTypeConfigBuilder().SetType("message"))
               .Build();

  SchemaStore::SetSchemaResult incompatible_result;
  incompatible_result.success = false;
  incompatible_result.old_schema_type_ids_changed.emplace(
      old_message_schema_type_id);
  incompatible_result.schema_types_deleted_by_name.emplace("email");
  incompatible_result.schema_types_deleted_by_id.emplace(
      old_email_schema_type_id);

  // Can't set the incompatible schema
  EXPECT_THAT(schema_store->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false),
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
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("email"))
          .AddType(SchemaTypeConfigBuilder().SetType("message"))
          .Build();

  // Set it for the first time
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert("email");
  result.schema_types_new_by_name.insert("message");
  EXPECT_THAT(schema_store->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));

  // Reorder the types
  SchemaProto reordered_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("message"))
          .AddType(SchemaTypeConfigBuilder().SetType("email"))
          .Build();

  // Set the compatible schema and verify with GetSchema
  if (feature_flags_->enable_schema_database()) {
    // Setting reordered types is a no-op for the new set schema after schema
    // database is enabled. So everything should be the same as before.
    result = SchemaStore::SetSchemaResult();
    result.success = true;
    EXPECT_THAT(
        schema_store->SetSchema(reordered_schema,
                                /*ignore_errors_and_delete_documents=*/false),
        IsOkAndHolds(EqualsSetSchemaResult(result)));

    ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store->GetSchema());
    EXPECT_THAT(*actual_schema, EqualsProto(schema));
  } else {
    // Since we assign SchemaTypeIds based on order in the SchemaProto, this
    // will
    // cause SchemaTypeIds to change
    result = SchemaStore::SetSchemaResult();
    result.success = true;
    result.old_schema_type_ids_changed.emplace(
        0);  // Old SchemaTypeId of "email"
    result.old_schema_type_ids_changed.emplace(
        1);  // Old SchemaTypeId of "message"

    // Set the compatible schema
    EXPECT_THAT(schema_store->SetSchema(
                    schema, /*ignore_errors_and_delete_documents=*/false),
                IsOkAndHolds(EqualsSetSchemaResult(result)));
    ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store->GetSchema());
    EXPECT_THAT(*actual_schema, EqualsProto(schema));
  }
}

TEST_F(SchemaStoreTest, IndexedPropertyChangeRequiresReindexingOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("email").AddProperty(
              // Add an unindexed property
              PropertyConfigBuilder()
                  .SetName("subject")
                  .SetDataType(TYPE_STRING)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // Set it for the first time
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert("email");
  EXPECT_THAT(schema_store->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));

  // Make a previously unindexed property indexed
  schema = SchemaBuilder()
               .AddType(SchemaTypeConfigBuilder().SetType("email").AddProperty(
                   PropertyConfigBuilder()
                       .SetName("subject")
                       .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                       .SetCardinality(CARDINALITY_OPTIONAL)))
               .Build();

  // Set the compatible schema
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_index_incompatible_by_name.insert("email");
  EXPECT_THAT(schema_store->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));
}

TEST_F(SchemaStoreTest, IndexNestedDocumentsChangeRequiresReindexingOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // Make two schemas. One that sets index_nested_properties to false and one
  // that sets it to true.
  SchemaTypeConfigProto email_type_config =
      SchemaTypeConfigBuilder()
          .SetType("email")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("subject")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  SchemaProto no_nested_index_schema =
      SchemaBuilder()
          .AddType(email_type_config)
          .AddType(SchemaTypeConfigBuilder().SetType("person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("emails")
                  .SetDataTypeDocument("email",
                                       /*index_nested_properties=*/false)
                  .SetCardinality(CARDINALITY_REPEATED)))
          .Build();

  SchemaProto nested_index_schema =
      SchemaBuilder()
          .AddType(email_type_config)
          .AddType(SchemaTypeConfigBuilder().SetType("person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("emails")
                  .SetDataTypeDocument("email",
                                       /*index_nested_properties=*/true)
                  .SetCardinality(CARDINALITY_REPEATED)))
          .Build();

  // Set schema with index_nested_properties=false to start.
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert("email");
  result.schema_types_new_by_name.insert("person");
  EXPECT_THAT(
      schema_store->SetSchema(no_nested_index_schema,
                              /*ignore_errors_and_delete_documents=*/false),
      IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(no_nested_index_schema));

  // Set schema with index_nested_properties=true and confirm that the change to
  // 'person' is index incompatible.
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_index_incompatible_by_name.insert("person");
  EXPECT_THAT(
      schema_store->SetSchema(nested_index_schema,
                              /*ignore_errors_and_delete_documents=*/false),
      IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(nested_index_schema));

  // Set schema with index_nested_properties=false and confirm that the change
  // to 'person' is index incompatible.
  result = SchemaStore::SetSchemaResult();
  result.success = true;
  result.schema_types_index_incompatible_by_name.insert("person");
  EXPECT_THAT(
      schema_store->SetSchema(no_nested_index_schema,
                              /*ignore_errors_and_delete_documents=*/false),
      IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(no_nested_index_schema));
}

TEST_F(SchemaStoreTest, SetSchemaWithIncompatibleTypesOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("email").AddProperty(
              // Add a STRING property
              PropertyConfigBuilder()
                  .SetName("subject")
                  .SetDataType(TYPE_STRING)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // Set it for the first time
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert("email");
  EXPECT_THAT(schema_store->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));

  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId old_email_schema_type_id,
                             schema_store->GetSchemaTypeId("email"));

  // Make a previously STRING property into DOUBLE
  schema = SchemaBuilder()
               .AddType(SchemaTypeConfigBuilder().SetType("email").AddProperty(
                   // Add a STRING property
                   PropertyConfigBuilder()
                       .SetName("subject")
                       .SetDataType(TYPE_DOUBLE)
                       .SetCardinality(CARDINALITY_OPTIONAL)))
               .Build();

  SchemaStore::SetSchemaResult incompatible_result;
  incompatible_result.success = false;
  incompatible_result.schema_types_incompatible_by_name.emplace("email");
  incompatible_result.schema_types_incompatible_by_id.emplace(
      old_email_schema_type_id);

  // Can't set the incompatible schema
  EXPECT_THAT(schema_store->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false),
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

TEST_F(SchemaStoreTest, SetSchemaWithIncompatibleNestedTypesOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // 1. Create a ContactPoint type with a repeated property and set that schema
  SchemaTypeConfigBuilder contact_point_repeated_label =
      SchemaTypeConfigBuilder()
          .SetType("ContactPoint")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("label")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REPEATED));
  SchemaProto old_schema =
      SchemaBuilder().AddType(contact_point_repeated_label).Build();
  ICING_EXPECT_OK(schema_store->SetSchema(
      old_schema, /*ignore_errors_and_delete_documents=*/false));
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId old_contact_point_type_id,
                             schema_store->GetSchemaTypeId("ContactPoint"));

  // 2. Create a type that references the ContactPoint type and make a backwards
  // incompatible change to ContactPoint
  SchemaTypeConfigBuilder contact_point_optional_label =
      SchemaTypeConfigBuilder()
          .SetType("ContactPoint")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("label")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL));
  SchemaTypeConfigBuilder person =
      SchemaTypeConfigBuilder().SetType("Person").AddProperty(
          PropertyConfigBuilder()
              .SetName("contactPoints")
              .SetDataTypeDocument("ContactPoint",
                                   /*index_nested_properties=*/true)
              .SetCardinality(CARDINALITY_REPEATED));
  SchemaProto new_schema = SchemaBuilder()
                               .AddType(contact_point_optional_label)
                               .AddType(person)
                               .Build();

  // 3. SetSchema should fail with ignore_errors_and_delete_documents=false and
  // the old schema should remain
  SchemaStore::SetSchemaResult expected_result;
  expected_result.success = false;
  expected_result.schema_types_incompatible_by_name.insert("ContactPoint");
  expected_result.schema_types_incompatible_by_id.insert(
      old_contact_point_type_id);
  expected_result.schema_types_new_by_name.insert("Person");
  EXPECT_THAT(
      schema_store->SetSchema(new_schema,
                              /*ignore_errors_and_delete_documents=*/false),
      IsOkAndHolds(EqualsSetSchemaResult(expected_result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(old_schema));

  // 4. SetSchema should succeed with ignore_errors_and_delete_documents=true
  // and the new schema should be set
  expected_result.success = true;
  EXPECT_THAT(
      schema_store->SetSchema(new_schema,
                              /*ignore_errors_and_delete_documents=*/true),
      IsOkAndHolds(EqualsSetSchemaResult(expected_result)));
  ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(new_schema));
}

TEST_F(SchemaStoreTest, SetSchemaWithIndexIncompatibleNestedTypesOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // 1. Create a ContactPoint type with label that matches prefix and set that
  // schema
  SchemaTypeConfigBuilder contact_point_prefix_label =
      SchemaTypeConfigBuilder()
          .SetType("ContactPoint")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("label")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REPEATED));
  SchemaProto old_schema =
      SchemaBuilder().AddType(contact_point_prefix_label).Build();
  ICING_EXPECT_OK(schema_store->SetSchema(
      old_schema, /*ignore_errors_and_delete_documents=*/false));

  // 2. Create a type that references the ContactPoint type and make a index
  // backwards incompatible change to ContactPoint
  SchemaTypeConfigBuilder contact_point_exact_label =
      SchemaTypeConfigBuilder()
          .SetType("ContactPoint")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("label")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED));
  SchemaTypeConfigBuilder person =
      SchemaTypeConfigBuilder().SetType("Person").AddProperty(
          PropertyConfigBuilder()
              .SetName("contactPoints")
              .SetDataTypeDocument("ContactPoint",
                                   /*index_nested_properties=*/true)
              .SetCardinality(CARDINALITY_REPEATED));
  SchemaProto new_schema = SchemaBuilder()
                               .AddType(contact_point_exact_label)
                               .AddType(person)
                               .Build();

  // SetSchema should succeed, and only ContactPoint should be in
  // schema_types_index_incompatible_by_name.
  SchemaStore::SetSchemaResult expected_result;
  expected_result.success = true;
  expected_result.schema_types_index_incompatible_by_name.insert(
      "ContactPoint");
  expected_result.schema_types_new_by_name.insert("Person");
  EXPECT_THAT(
      schema_store->SetSchema(new_schema,
                              /*ignore_errors_and_delete_documents=*/false),
      IsOkAndHolds(EqualsSetSchemaResult(expected_result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(new_schema));
}

TEST_F(SchemaStoreTest, SetSchemaWithCompatibleNestedTypesOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // 1. Create a ContactPoint type with a optional property and set that schema
  SchemaTypeConfigBuilder contact_point_optional_label =
      SchemaTypeConfigBuilder()
          .SetType("ContactPoint")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("label")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL));
  SchemaProto old_schema =
      SchemaBuilder().AddType(contact_point_optional_label).Build();
  ICING_EXPECT_OK(schema_store->SetSchema(
      old_schema, /*ignore_errors_and_delete_documents=*/false));

  // 2. Create a type that references the ContactPoint type and make a backwards
  // compatible change to ContactPoint
  SchemaTypeConfigBuilder contact_point_repeated_label =
      SchemaTypeConfigBuilder()
          .SetType("ContactPoint")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("label")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REPEATED));
  SchemaTypeConfigBuilder person =
      SchemaTypeConfigBuilder().SetType("Person").AddProperty(
          PropertyConfigBuilder()
              .SetName("contactPoints")
              .SetDataTypeDocument("ContactPoint",
                                   /*index_nested_properties=*/true)
              .SetCardinality(CARDINALITY_REPEATED));
  SchemaProto new_schema = SchemaBuilder()
                               .AddType(contact_point_repeated_label)
                               .AddType(person)
                               .Build();

  // 3. SetSchema should succeed, and only ContactPoint should be in
  // schema_types_changed_fully_compatible_by_name.
  SchemaStore::SetSchemaResult expected_result;
  expected_result.success = true;
  expected_result.schema_types_changed_fully_compatible_by_name.insert(
      "ContactPoint");
  expected_result.schema_types_new_by_name.insert("Person");
  EXPECT_THAT(schema_store->SetSchema(
                  new_schema, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(expected_result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(new_schema));
}

TEST_F(SchemaStoreTest, SetSchemaWithAddedIndexableNestedTypeOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // 1. Create a ContactPoint type with a optional property, and a type that
  //    references the ContactPoint type.
  SchemaTypeConfigBuilder contact_point =
      SchemaTypeConfigBuilder()
          .SetType("ContactPoint")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("label")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REPEATED));
  SchemaTypeConfigBuilder person =
      SchemaTypeConfigBuilder().SetType("Person").AddProperty(
          PropertyConfigBuilder()
              .SetName("contactPoints")
              .SetDataTypeDocument("ContactPoint",
                                   /*index_nested_properties=*/true)
              .SetCardinality(CARDINALITY_REPEATED));
  SchemaProto old_schema =
      SchemaBuilder().AddType(contact_point).AddType(person).Build();
  ICING_EXPECT_OK(schema_store->SetSchema(
      old_schema, /*ignore_errors_and_delete_documents=*/false));

  // 2. Add another nested document property to "Person" that has type
  //    "ContactPoint"
  SchemaTypeConfigBuilder new_person =
      SchemaTypeConfigBuilder()
          .SetType("Person")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("contactPoints")
                  .SetDataTypeDocument("ContactPoint",
                                       /*index_nested_properties=*/true)
                  .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("anotherContactPoint")
                  .SetDataTypeDocument("ContactPoint",
                                       /*index_nested_properties=*/true)
                  .SetCardinality(CARDINALITY_REPEATED));
  SchemaProto new_schema =
      SchemaBuilder().AddType(contact_point).AddType(new_person).Build();

  // 3. Set to new schema. "Person" should be index-incompatible since we need
  //    to index an additional property: 'anotherContactPoint.label'.
  // - "Person" is also considered join-incompatible since the added nested
  //   document property could also contain a joinable property.
  SchemaStore::SetSchemaResult expected_result;
  expected_result.success = true;
  expected_result.schema_types_index_incompatible_by_name.insert("Person");
  expected_result.schema_types_join_incompatible_by_name.insert("Person");

  EXPECT_THAT(schema_store->SetSchema(
                  new_schema, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(expected_result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(new_schema));
}

TEST_F(SchemaStoreTest, SetSchemaWithAddedJoinableNestedTypeOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // 1. Create a ContactPoint type with a optional property, and a type that
  //    references the ContactPoint type.
  SchemaTypeConfigBuilder contact_point =
      SchemaTypeConfigBuilder()
          .SetType("ContactPoint")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("label")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetJoinable(JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                               DELETE_PROPAGATION_TYPE_NONE)
                  .SetCardinality(CARDINALITY_REQUIRED));
  SchemaTypeConfigBuilder person =
      SchemaTypeConfigBuilder().SetType("Person").AddProperty(
          PropertyConfigBuilder()
              .SetName("contactPoints")
              .SetDataTypeDocument("ContactPoint",
                                   /*index_nested_properties=*/true)
              .SetCardinality(CARDINALITY_OPTIONAL));
  SchemaProto old_schema =
      SchemaBuilder().AddType(contact_point).AddType(person).Build();
  ICING_EXPECT_OK(schema_store->SetSchema(
      old_schema, /*ignore_errors_and_delete_documents=*/false));

  // 2. Add another nested document property to "Person" that has type
  //    "ContactPoint", but make it non-indexable
  SchemaTypeConfigBuilder new_person =
      SchemaTypeConfigBuilder()
          .SetType("Person")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("contactPoints")
                  .SetDataTypeDocument("ContactPoint",
                                       /*index_nested_properties=*/true)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("anotherContactPoint")
                  .SetDataTypeDocument("ContactPoint",
                                       /*index_nested_properties=*/false)
                  .SetCardinality(CARDINALITY_OPTIONAL));
  SchemaProto new_schema =
      SchemaBuilder().AddType(contact_point).AddType(new_person).Build();

  // 3. Set to new schema. "Person" should be join-incompatible but
  //    index-compatible.
  SchemaStore::SetSchemaResult expected_result;
  expected_result.success = true;
  expected_result.schema_types_join_incompatible_by_name.insert("Person");

  EXPECT_THAT(schema_store->SetSchema(
                  new_schema, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(expected_result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(new_schema));
}

TEST_F(SchemaStoreTest, SetSchemaByUpdatingScorablePropertyOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaProto old_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("email").AddProperty(
              PropertyConfigBuilder()
                  .SetName("title")
                  .SetDataType(TYPE_STRING)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();
  SchemaProto new_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("title")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("score")
                                        .SetDataType(TYPE_DOUBLE)
                                        .SetScorableType(SCORABLE_TYPE_ENABLED)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // Set old schema
  SchemaStore::SetSchemaResult expected_result;
  expected_result.success = true;
  expected_result.schema_types_new_by_name.insert("email");
  EXPECT_THAT(schema_store->SetSchema(
                  old_schema, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(expected_result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(old_schema));

  // Set new schema.
  // The new schema adds "score" as scorable_type ENABLED from type "email".
  SchemaStore::SetSchemaResult new_expected_result;
  new_expected_result.success = true;
  new_expected_result.schema_types_scorable_property_inconsistent_by_id.insert(
      0);
  new_expected_result.schema_types_changed_fully_compatible_by_name.insert(
      "email");
  EXPECT_THAT(schema_store->SetSchema(
                  new_schema, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(new_expected_result)));
  ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(new_schema));
}

TEST_F(SchemaStoreTest,
       SetSchemaWithReorderedSchemeTypesAndUpdatedScorablePropertyOk) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaProto old_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("message"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("title")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("score")
                                        .SetDataType(TYPE_DOUBLE)
                                        .SetScorableType(SCORABLE_TYPE_DISABLED)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();
  // The new schema updates "score" as scorable_type ENABLED from type "email",
  // and it also reorders the schema types of "email" and "message".
  SchemaProto new_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("title")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("score")
                                        .SetDataType(TYPE_DOUBLE)
                                        .SetScorableType(SCORABLE_TYPE_ENABLED)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder().SetType("message"))
          .Build();

  // Set old schema
  SchemaStore::SetSchemaResult expected_result;
  expected_result.success = true;
  expected_result.schema_types_new_by_name.insert("email");
  expected_result.schema_types_new_by_name.insert("message");
  EXPECT_THAT(schema_store->SetSchema(
                  old_schema, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(expected_result)));
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(old_schema));

  // Set new schema.
  SchemaStore::SetSchemaResult new_expected_result;
  new_expected_result.success = true;
  // Schema type id of "email" is updated to 0.
  SchemaTypeId email_schema_type_id = 0;
  new_expected_result.schema_types_scorable_property_inconsistent_by_id.insert(
      email_schema_type_id);
  new_expected_result.schema_types_changed_fully_compatible_by_name.insert(
      "email");
  new_expected_result.old_schema_type_ids_changed.insert(0);
  new_expected_result.old_schema_type_ids_changed.insert(1);
  EXPECT_THAT(schema_store->SetSchema(
                  new_schema, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(new_expected_result)));
  ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(new_schema));
}

TEST_F(SchemaStoreTest, GetSchemaTypeId) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

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
  result.schema_types_new_by_name.insert(first_type);
  result.schema_types_new_by_name.insert(second_type);
  EXPECT_THAT(schema_store->SetSchema(
                  schema_, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(result)));

  EXPECT_THAT(schema_store->GetSchemaTypeId(first_type), IsOkAndHolds(0));
  EXPECT_THAT(schema_store->GetSchemaTypeId(second_type), IsOkAndHolds(1));
}

TEST_F(SchemaStoreTest, UpdateChecksumDefaultOnEmptySchemaStore) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  EXPECT_THAT(schema_store->GetChecksum(), IsOkAndHolds(Crc32()));
  EXPECT_THAT(schema_store->UpdateChecksum(), IsOkAndHolds(Crc32()));
  EXPECT_THAT(schema_store->GetChecksum(), IsOkAndHolds(Crc32()));
}

TEST_F(SchemaStoreTest, UpdateChecksumSameBetweenCalls) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaProto foo_schema =
      SchemaBuilder().AddType(SchemaTypeConfigBuilder().SetType("foo")).Build();

  ICING_EXPECT_OK(schema_store->SetSchema(
      foo_schema, /*ignore_errors_and_delete_documents=*/false));

  ICING_ASSERT_OK_AND_ASSIGN(Crc32 checksum, schema_store->GetChecksum());
  EXPECT_THAT(schema_store->UpdateChecksum(), IsOkAndHolds(checksum));
  EXPECT_THAT(schema_store->GetChecksum(), IsOkAndHolds(checksum));

  // Calling it again doesn't change the checksum
  EXPECT_THAT(schema_store->UpdateChecksum(), IsOkAndHolds(checksum));
  EXPECT_THAT(schema_store->GetChecksum(), IsOkAndHolds(checksum));
}

TEST_F(SchemaStoreTest, UpdateChecksumSameAcrossInstances) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaProto foo_schema =
      SchemaBuilder().AddType(SchemaTypeConfigBuilder().SetType("foo")).Build();

  ICING_EXPECT_OK(schema_store->SetSchema(
      foo_schema, /*ignore_errors_and_delete_documents=*/false));

  ICING_ASSERT_OK_AND_ASSIGN(Crc32 checksum, schema_store->GetChecksum());
  EXPECT_THAT(schema_store->UpdateChecksum(), IsOkAndHolds(checksum));
  EXPECT_THAT(schema_store->GetChecksum(), IsOkAndHolds(checksum));

  // Destroy the previous instance and recreate SchemaStore
  schema_store.reset();

  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store, SchemaStore::Create(&filesystem_, schema_store_dir_,
                                        &fake_clock_, feature_flags_.get()));
  EXPECT_THAT(schema_store->GetChecksum(), IsOkAndHolds(checksum));
  EXPECT_THAT(schema_store->UpdateChecksum(), IsOkAndHolds(checksum));
  EXPECT_THAT(schema_store->GetChecksum(), IsOkAndHolds(checksum));
}

TEST_F(SchemaStoreTest, UpdateChecksumChangesOnModification) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaProto foo_schema =
      SchemaBuilder().AddType(SchemaTypeConfigBuilder().SetType("foo")).Build();

  ICING_EXPECT_OK(schema_store->SetSchema(
      foo_schema, /*ignore_errors_and_delete_documents=*/false));

  ICING_ASSERT_OK_AND_ASSIGN(Crc32 checksum, schema_store->GetChecksum());
  EXPECT_THAT(schema_store->UpdateChecksum(), IsOkAndHolds(checksum));
  EXPECT_THAT(schema_store->GetChecksum(), IsOkAndHolds(checksum));

  // Modifying the SchemaStore changes the checksum
  SchemaProto foo_bar_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("foo"))
          .AddType(SchemaTypeConfigBuilder().SetType("bar"))
          .Build();

  ICING_EXPECT_OK(schema_store->SetSchema(
      foo_bar_schema, /*ignore_errors_and_delete_documents=*/false));

  ICING_ASSERT_OK_AND_ASSIGN(Crc32 updated_checksum,
                             schema_store->GetChecksum());
  EXPECT_THAT(updated_checksum, Not(Eq(checksum)));
  EXPECT_THAT(schema_store->UpdateChecksum(), IsOkAndHolds(updated_checksum));
  EXPECT_THAT(schema_store->GetChecksum(), IsOkAndHolds(updated_checksum));
}

TEST_F(SchemaStoreTest, PersistToDiskFineForEmptySchemaStore) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // Persisting is fine and shouldn't affect anything
  ICING_EXPECT_OK(schema_store->PersistToDisk());
}

TEST_F(SchemaStoreTest, UpdateChecksumAvoidsRecovery) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaProto schema =
      SchemaBuilder().AddType(SchemaTypeConfigBuilder().SetType("foo")).Build();

  ICING_EXPECT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));

  // UpdateChecksum should update the schema store checksum. Therefore, we
  // should not need a recovery on reinitialization.
  ICING_ASSERT_OK_AND_ASSIGN(Crc32 crc, schema_store->GetChecksum());
  EXPECT_THAT(schema_store->UpdateChecksum(), IsOkAndHolds(crc));
  EXPECT_THAT(schema_store->GetChecksum(), IsOkAndHolds(crc));

  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));

  // And we get the same schema back on reinitialization
  InitializeStatsProto initialize_stats;
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store_two,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get(), &initialize_stats));
  EXPECT_THAT(initialize_stats.schema_store_recovery_cause(),
              Eq(InitializeStatsProto::NONE));
  ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store_two->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));

  // The checksum should be the same.
  EXPECT_THAT(schema_store_two->GetChecksum(), IsOkAndHolds(crc));
  EXPECT_THAT(schema_store_two->UpdateChecksum(), IsOkAndHolds(crc));
  EXPECT_THAT(schema_store_two->GetChecksum(), IsOkAndHolds(crc));
}

TEST_F(SchemaStoreTest, PersistToDiskPreservesAcrossInstances) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaProto schema =
      SchemaBuilder().AddType(SchemaTypeConfigBuilder().SetType("foo")).Build();

  ICING_EXPECT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));

  // Persisting shouldn't change anything
  ICING_EXPECT_OK(schema_store->PersistToDisk());

  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* actual_schema,
                             schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));

  // Modify the schema so that something different is persisted next time
  schema = SchemaBuilder(schema)
               .AddType(SchemaTypeConfigBuilder().SetType("bar"))
               .Build();
  ICING_EXPECT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));

  // Should also persist on destruction
  schema_store.reset();

  // And we get the same schema back on reinitialization
  InitializeStatsProto initialize_stats;
  ICING_ASSERT_OK_AND_ASSIGN(
      schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get(), &initialize_stats));
  EXPECT_THAT(initialize_stats.schema_store_recovery_cause(),
              Eq(InitializeStatsProto::NONE));
  ICING_ASSERT_OK_AND_ASSIGN(actual_schema, schema_store->GetSchema());
  EXPECT_THAT(*actual_schema, EqualsProto(schema));
}

TEST_F(SchemaStoreTest, SchemaStoreStorageInfoProto) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // Create a schema with two types: one simple type and one type that uses all
  // 64 sections.
  PropertyConfigProto prop =
      PropertyConfigBuilder()
          .SetName("subject")
          .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
          .SetCardinality(CARDINALITY_OPTIONAL)
          .Build();
  SchemaTypeConfigBuilder full_sections_type_builder =
      SchemaTypeConfigBuilder().SetType("fullSectionsType");
  for (int i = 0; i < 64; ++i) {
    full_sections_type_builder.AddProperty(
        PropertyConfigBuilder(prop).SetName("prop" + std::to_string(i)));
  }
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("email").AddProperty(
              PropertyConfigBuilder(prop)))
          .AddType(full_sections_type_builder)
          .Build();

  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert("email");
  result.schema_types_new_by_name.insert("fullSectionsType");
  EXPECT_THAT(schema_store->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(result)));

  SchemaStoreStorageInfoProto storage_info = schema_store->GetStorageInfo();
  EXPECT_THAT(storage_info.schema_store_size(), Ge(0));
  EXPECT_THAT(storage_info.num_schema_types(), Eq(2));
  EXPECT_THAT(storage_info.num_total_sections(), Eq(65));
  EXPECT_THAT(storage_info.num_schema_types_sections_exhausted(), Eq(1));
}

TEST_F(SchemaStoreTest, GetDebugInfo) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // Set schema
  ASSERT_THAT(
      schema_store->SetSchema(schema_,
                              /*ignore_errors_and_delete_documents=*/false),
      IsOkAndHolds(EqualsSetSchemaResult(SchemaStore::SetSchemaResult{
          .success = true,
          .schema_types_new_by_name = {schema_.types(0).schema_type()}})));

  // Check debug info
  ICING_ASSERT_OK_AND_ASSIGN(SchemaDebugInfoProto out,
                             schema_store->GetDebugInfo());
  EXPECT_THAT(out.schema(), EqualsProto(schema_));
  EXPECT_THAT(out.crc(), Gt(0));
}

TEST_F(SchemaStoreTest, GetDebugInfoForEmptySchemaStore) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // Check debug info before setting a schema
  ICING_ASSERT_OK_AND_ASSIGN(SchemaDebugInfoProto out,
                             schema_store->GetDebugInfo());
  SchemaDebugInfoProto expected_out;
  expected_out.set_crc(0);
  EXPECT_THAT(out, EqualsProto(expected_out));
}

TEST_F(SchemaStoreTest, InitializeRegenerateDerivedFilesFailure) {
  // This test covers the first point that RegenerateDerivedFiles could fail.
  // This should simply result in SetSchema::Create returning an INTERNAL error.

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    SchemaProto schema = SchemaBuilder()
                             .AddType(SchemaTypeConfigBuilder().SetType("Type"))
                             .Build();
    ICING_ASSERT_OK(schema_store->SetSchema(
        std::move(schema), /*ignore_errors_and_delete_documents=*/false));
  }

  auto mock_filesystem = std::make_unique<MockFilesystem>();
  ON_CALL(*mock_filesystem,
          CreateDirectoryRecursively(HasSubstr("key_mapper_dir")))
      .WillByDefault(Return(false));
  {
    EXPECT_THAT(SchemaStore::Create(mock_filesystem.get(), schema_store_dir_,
                                    &fake_clock_, feature_flags_.get()),
                StatusIs(libtextclassifier3::StatusCode::INTERNAL));
  }
}

TEST_F(SchemaStoreTest, SetSchemaRegenerateDerivedFilesFailure) {
  // This test covers the second point that RegenerateDerivedFiles could fail.
  // If handled correctly, the schema store and section manager should still be
  // in the original, valid state.
  SchemaTypeConfigProto type =
      SchemaTypeConfigBuilder()
          .SetType("Type")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("intProp1")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("stringProp1")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    SchemaProto schema = SchemaBuilder().AddType(type).Build();
    ICING_ASSERT_OK(schema_store->SetSchema(
        std::move(schema), /*ignore_errors_and_delete_documents=*/false));
  }

  {
    auto mock_filesystem = std::make_unique<MockFilesystem>();
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(mock_filesystem.get(), schema_store_dir_,
                            &fake_clock_, feature_flags_.get()));

    ON_CALL(*mock_filesystem,
            CreateDirectoryRecursively(HasSubstr("key_mapper_dir")))
        .WillByDefault(Return(false));
    SchemaProto schema =
        SchemaBuilder()
            .AddType(type)
            .AddType(SchemaTypeConfigBuilder().SetType("Type2"))
            .Build();
    EXPECT_THAT(
        schema_store->SetSchema(std::move(schema),
                                /*ignore_errors_and_delete_documents=*/false),
        StatusIs(libtextclassifier3::StatusCode::INTERNAL));
    DocumentProto document =
        DocumentBuilder()
            .SetSchema("Type")
            .AddInt64Property("intProp1", 1, 2, 3)
            .AddStringProperty("stringProp1", "foo bar baz")
            .Build();
    SectionMetadata expected_int_prop1_metadata(
        /*id_in=*/0, TYPE_INT64, TOKENIZER_NONE, TERM_MATCH_UNKNOWN,
        NUMERIC_MATCH_RANGE, EMBEDDING_INDEXING_UNKNOWN, QUANTIZATION_TYPE_NONE,
        "intProp1");
    SectionMetadata expected_string_prop1_metadata(
        /*id_in=*/1, TYPE_STRING, TOKENIZER_PLAIN, TERM_MATCH_EXACT,
        NUMERIC_MATCH_UNKNOWN, EMBEDDING_INDEXING_UNKNOWN,
        QUANTIZATION_TYPE_NONE, "stringProp1");
    ICING_ASSERT_OK_AND_ASSIGN(SectionGroup section_group,
                               schema_store->ExtractSections(document));
    ASSERT_THAT(section_group.string_sections, SizeIs(1));
    EXPECT_THAT(section_group.string_sections.at(0).metadata,
                Eq(expected_string_prop1_metadata));
    EXPECT_THAT(section_group.string_sections.at(0).content,
                ElementsAre("foo bar baz"));
    ASSERT_THAT(section_group.integer_sections, SizeIs(1));
    EXPECT_THAT(section_group.integer_sections.at(0).metadata,
                Eq(expected_int_prop1_metadata));
    EXPECT_THAT(section_group.integer_sections.at(0).content,
                ElementsAre(1, 2, 3));
  }
}

TEST_F(SchemaStoreTest, CanCheckForPropertiesDefinedInSchema) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // Set it for the first time
  SchemaStore::SetSchemaResult result;
  result.success = true;
  result.schema_types_new_by_name.insert(schema_.types(0).schema_type());

  // Don't use schema_ defined in the test suite, as we want to make sure that
  // the test is written correctly without referring to what the suite has
  // defined.
  SchemaProto schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("email")
                  .AddProperty(
                      // Add an indexed property so we generate
                      // section metadata on it
                      PropertyConfigBuilder()
                          .SetName("subject")
                          .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("timestamp")
                                   .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                   .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  EXPECT_THAT(schema_store->SetSchema(
                  schema, /*ignore_errors_and_delete_documents=*/false),
              IsOkAndHolds(EqualsSetSchemaResult(result)));
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId schema_id,
                             schema_store->GetSchemaTypeId("email"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(schema_id, "subject"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(schema_id, "timestamp"));
  EXPECT_FALSE(schema_store->IsPropertyDefinedInSchema(schema_id, "foobar"));
}

TEST_F(SchemaStoreTest, GetSchemaTypeIdsWithChildren) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // Create a schema with the following inheritance relation:
  //       A
  //     /   \
  //    B     E
  //   /  \
  //  C    D
  //       |
  //       F
  SchemaTypeConfigProto type_a = SchemaTypeConfigBuilder().SetType("A").Build();
  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder().SetType("B").AddParentType("A").Build();
  SchemaTypeConfigProto type_c =
      SchemaTypeConfigBuilder().SetType("C").AddParentType("B").Build();
  SchemaTypeConfigProto type_d =
      SchemaTypeConfigBuilder().SetType("D").AddParentType("B").Build();
  SchemaTypeConfigProto type_e =
      SchemaTypeConfigBuilder().SetType("E").AddParentType("A").Build();
  SchemaTypeConfigProto type_f =
      SchemaTypeConfigBuilder().SetType("F").AddParentType("D").Build();
  SchemaProto schema = SchemaBuilder()
                           .AddType(type_a)
                           .AddType(type_b)
                           .AddType(type_c)
                           .AddType(type_d)
                           .AddType(type_e)
                           .AddType(type_f)
                           .Build();
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));

  // Get schema type id for each type.
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId type_a_id,
                             schema_store->GetSchemaTypeId("A"));
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId type_b_id,
                             schema_store->GetSchemaTypeId("B"));
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId type_c_id,
                             schema_store->GetSchemaTypeId("C"));
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId type_d_id,
                             schema_store->GetSchemaTypeId("D"));
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId type_e_id,
                             schema_store->GetSchemaTypeId("E"));
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId type_f_id,
                             schema_store->GetSchemaTypeId("F"));

  // Check the results from GetSchemaTypeIdsWithChildren
  EXPECT_THAT(
      schema_store->GetSchemaTypeIdsWithChildren("A"),
      IsOkAndHolds(Pointee(UnorderedElementsAre(
          type_a_id, type_b_id, type_c_id, type_d_id, type_e_id, type_f_id))));
  EXPECT_THAT(schema_store->GetSchemaTypeIdsWithChildren("B"),
              IsOkAndHolds(Pointee(UnorderedElementsAre(
                  type_b_id, type_c_id, type_d_id, type_f_id))));
  EXPECT_THAT(schema_store->GetSchemaTypeIdsWithChildren("C"),
              IsOkAndHolds(Pointee(UnorderedElementsAre(type_c_id))));
  EXPECT_THAT(
      schema_store->GetSchemaTypeIdsWithChildren("D"),
      IsOkAndHolds(Pointee(UnorderedElementsAre(type_d_id, type_f_id))));
  EXPECT_THAT(schema_store->GetSchemaTypeIdsWithChildren("E"),
              IsOkAndHolds(Pointee(UnorderedElementsAre(type_e_id))));
  EXPECT_THAT(schema_store->GetSchemaTypeIdsWithChildren("F"),
              IsOkAndHolds(Pointee(UnorderedElementsAre(type_f_id))));
}

TEST_F(SchemaStoreTest, DiamondGetSchemaTypeIdsWithChildren) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // Create a schema with the following inheritance relation:
  //       A
  //     /   \
  //    B     E
  //   /  \  /
  //  C    D
  //   \  /
  //     F
  SchemaTypeConfigProto type_a = SchemaTypeConfigBuilder().SetType("A").Build();
  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder().SetType("B").AddParentType("A").Build();
  SchemaTypeConfigProto type_c =
      SchemaTypeConfigBuilder().SetType("C").AddParentType("B").Build();
  SchemaTypeConfigProto type_d = SchemaTypeConfigBuilder()
                                     .SetType("D")
                                     .AddParentType("B")
                                     .AddParentType("E")
                                     .Build();
  SchemaTypeConfigProto type_e =
      SchemaTypeConfigBuilder().SetType("E").AddParentType("A").Build();
  SchemaTypeConfigProto type_f = SchemaTypeConfigBuilder()
                                     .SetType("F")
                                     .AddParentType("C")
                                     .AddParentType("D")
                                     .Build();
  SchemaProto schema = SchemaBuilder()
                           .AddType(type_a)
                           .AddType(type_b)
                           .AddType(type_c)
                           .AddType(type_d)
                           .AddType(type_e)
                           .AddType(type_f)
                           .Build();
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));

  // Get schema type id for each type.
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId type_a_id,
                             schema_store->GetSchemaTypeId("A"));
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId type_b_id,
                             schema_store->GetSchemaTypeId("B"));
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId type_c_id,
                             schema_store->GetSchemaTypeId("C"));
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId type_d_id,
                             schema_store->GetSchemaTypeId("D"));
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId type_e_id,
                             schema_store->GetSchemaTypeId("E"));
  ICING_ASSERT_OK_AND_ASSIGN(SchemaTypeId type_f_id,
                             schema_store->GetSchemaTypeId("F"));

  // Check the results from GetSchemaTypeIdsWithChildren
  EXPECT_THAT(
      schema_store->GetSchemaTypeIdsWithChildren("A"),
      IsOkAndHolds(Pointee(UnorderedElementsAre(
          type_a_id, type_b_id, type_c_id, type_d_id, type_e_id, type_f_id))));
  EXPECT_THAT(schema_store->GetSchemaTypeIdsWithChildren("B"),
              IsOkAndHolds(Pointee(UnorderedElementsAre(
                  type_b_id, type_c_id, type_d_id, type_f_id))));
  EXPECT_THAT(
      schema_store->GetSchemaTypeIdsWithChildren("C"),
      IsOkAndHolds(Pointee(UnorderedElementsAre(type_c_id, type_f_id))));
  EXPECT_THAT(
      schema_store->GetSchemaTypeIdsWithChildren("D"),
      IsOkAndHolds(Pointee(UnorderedElementsAre(type_d_id, type_f_id))));
  EXPECT_THAT(schema_store->GetSchemaTypeIdsWithChildren("E"),
              IsOkAndHolds(Pointee(
                  UnorderedElementsAre(type_e_id, type_d_id, type_f_id))));
  EXPECT_THAT(schema_store->GetSchemaTypeIdsWithChildren("F"),
              IsOkAndHolds(Pointee(UnorderedElementsAre(type_f_id))));
}

TEST_F(SchemaStoreTest, IndexableFieldsAreDefined) {
  SchemaTypeConfigProto email_type =
      SchemaTypeConfigBuilder()
          .SetType("Email")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("subject")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("senderQualifiedId")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetJoinable(JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                               DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("recipients")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("recipientIds")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("timestamp")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .Build();

  SchemaProto schema = SchemaBuilder().AddType(email_type).Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));
  constexpr SchemaTypeId kTypeEmailSchemaId = 0;

  // Indexables.
  EXPECT_TRUE(
      schema_store->IsPropertyDefinedInSchema(kTypeEmailSchemaId, "subject"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeEmailSchemaId,
                                                      "senderQualifiedId"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeEmailSchemaId,
                                                      "recipients"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeEmailSchemaId,
                                                      "recipientIds"));
  EXPECT_TRUE(
      schema_store->IsPropertyDefinedInSchema(kTypeEmailSchemaId, "timestamp"));
}

TEST_F(SchemaStoreTest, JoinableFieldsAreDefined) {
  SchemaTypeConfigProto email_type =
      SchemaTypeConfigBuilder()
          .SetType("Email")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("tagQualifiedId")
                           .SetDataType(TYPE_STRING)
                           .SetJoinable(JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                        DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("senderQualifiedId")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetJoinable(JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                               DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .Build();

  SchemaProto schema = SchemaBuilder().AddType(email_type).Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));
  constexpr SchemaTypeId kTypeEmailSchemaId = 0;

  // Joinables.
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeEmailSchemaId,
                                                      "tagQualifiedId"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeEmailSchemaId,
                                                      "senderQualifiedId"));
}

TEST_F(SchemaStoreTest, NonIndexableFieldsAreDefined) {
  SchemaTypeConfigProto email_type =
      SchemaTypeConfigBuilder()
          .SetType("Email")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("text")
                  .SetDataTypeString(TERM_MATCH_UNKNOWN, TOKENIZER_NONE)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("attachment")
                           .SetDataType(TYPE_BYTES)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("nonindexableInteger")
                           .SetDataType(TYPE_INT64)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .Build();

  SchemaProto schema = SchemaBuilder().AddType(email_type).Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));
  constexpr SchemaTypeId kTypeEmailSchemaId = 0;

  // Non-indexables.
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeEmailSchemaId,
                                                      "attachment"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeEmailSchemaId,
                                                      "nonindexableInteger"));
  EXPECT_TRUE(
      schema_store->IsPropertyDefinedInSchema(kTypeEmailSchemaId, "text"));
}

TEST_F(SchemaStoreTest, NonExistentFieldsAreUndefined) {
  SchemaTypeConfigProto email_type =
      SchemaTypeConfigBuilder()
          .SetType("Email")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("subject")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("senderQualifiedId")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetJoinable(JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                               DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("timestamp")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("nonindexableInteger")
                           .SetDataType(TYPE_INT64)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .Build();

  SchemaProto schema = SchemaBuilder().AddType(email_type).Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));
  constexpr SchemaTypeId kTypeEmailSchemaId = 0;

  // Non-existents.
  EXPECT_FALSE(
      schema_store->IsPropertyDefinedInSchema(kTypeEmailSchemaId, "foobar"));
  EXPECT_FALSE(schema_store->IsPropertyDefinedInSchema(kTypeEmailSchemaId,
                                                       "timestamp.foo"));
  EXPECT_FALSE(
      schema_store->IsPropertyDefinedInSchema(kTypeEmailSchemaId, "time"));
}

TEST_F(SchemaStoreTest, NestedIndexableFieldsAreDefined) {
  SchemaTypeConfigProto email_type =
      SchemaTypeConfigBuilder()
          .SetType("Email")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("tagQualifiedId")
                           .SetDataType(TYPE_STRING)
                           .SetJoinable(JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                        DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("subject")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("text")
                  .SetDataTypeString(TERM_MATCH_UNKNOWN, TOKENIZER_NONE)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("timestamp")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .Build();

  SchemaTypeConfigProto conversation_type =
      SchemaTypeConfigBuilder()
          .SetType("Conversation")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("emails")
                           .SetDataTypeDocument(
                               "Email", /*index_nested_properties=*/true)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("nestedNonIndexable")
                  .SetDataTypeDocument("Email",
                                       /*index_nested_properties=*/false)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  SchemaProto schema =
      SchemaBuilder().AddType(email_type).AddType(conversation_type).Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));
  constexpr SchemaTypeId kTypeConversationSchemaId = 1;

  // Indexables.
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeConversationSchemaId,
                                                      "emails.subject"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeConversationSchemaId,
                                                      "emails.timestamp"));
}

TEST_F(SchemaStoreTest, NestedJoinableFieldsAreDefined) {
  SchemaTypeConfigProto email_type =
      SchemaTypeConfigBuilder()
          .SetType("Email")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("tagQualifiedId")
                           .SetDataType(TYPE_STRING)
                           .SetJoinable(JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                        DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("subject")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("text")
                  .SetDataTypeString(TERM_MATCH_UNKNOWN, TOKENIZER_NONE)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("timestamp")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .Build();

  SchemaTypeConfigProto conversation_type =
      SchemaTypeConfigBuilder()
          .SetType("Conversation")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("emails")
                           .SetDataTypeDocument(
                               "Email", /*index_nested_properties=*/true)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("nestedNonIndexable")
                  .SetDataTypeDocument("Email",
                                       /*index_nested_properties=*/false)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  SchemaProto schema =
      SchemaBuilder().AddType(email_type).AddType(conversation_type).Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));
  constexpr SchemaTypeId kTypeConversationSchemaId = 1;

  // Joinables.
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeConversationSchemaId,
                                                      "emails.tagQualifiedId"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(
      kTypeConversationSchemaId, "nestedNonIndexable.tagQualifiedId"));
}

TEST_F(SchemaStoreTest, NestedNonIndexableFieldsAreDefined) {
  SchemaTypeConfigProto email_type =
      SchemaTypeConfigBuilder()
          .SetType("Email")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("tagQualifiedId")
                           .SetDataType(TYPE_STRING)
                           .SetJoinable(JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                        DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("subject")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("text")
                  .SetDataTypeString(TERM_MATCH_UNKNOWN, TOKENIZER_NONE)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("timestamp")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .Build();

  SchemaTypeConfigProto conversation_type =
      SchemaTypeConfigBuilder()
          .SetType("Conversation")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("emails")
                           .SetDataTypeDocument(
                               "Email", /*index_nested_properties=*/true)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("nestedNonIndexable")
                  .SetDataTypeDocument("Email",
                                       /*index_nested_properties=*/false)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  SchemaProto schema =
      SchemaBuilder().AddType(email_type).AddType(conversation_type).Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));
  constexpr SchemaTypeId kTypeConversationSchemaId = 1;

  // Non-indexables.
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeConversationSchemaId,
                                                      "emails.text"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(
      kTypeConversationSchemaId, "nestedNonIndexable.subject"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(
      kTypeConversationSchemaId, "nestedNonIndexable.text"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(
      kTypeConversationSchemaId, "nestedNonIndexable.timestamp"));
}

TEST_F(SchemaStoreTest, NestedNonExistentFieldsAreUndefined) {
  SchemaTypeConfigProto email_type =
      SchemaTypeConfigBuilder()
          .SetType("Email")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("tagQualifiedId")
                           .SetDataType(TYPE_STRING)
                           .SetJoinable(JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                        DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("subject")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("text")
                  .SetDataTypeString(TERM_MATCH_UNKNOWN, TOKENIZER_NONE)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("timestamp")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .Build();

  SchemaTypeConfigProto conversation_type =
      SchemaTypeConfigBuilder()
          .SetType("Conversation")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("emails")
                           .SetDataTypeDocument(
                               "Email", /*index_nested_properties=*/true)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("nestedNonIndexable")
                  .SetDataTypeDocument("Email",
                                       /*index_nested_properties=*/false)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  SchemaProto schema =
      SchemaBuilder().AddType(email_type).AddType(conversation_type).Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));
  constexpr SchemaTypeId kTypeConversationSchemaId = 1;

  // Non-existents.
  EXPECT_FALSE(schema_store->IsPropertyDefinedInSchema(
      kTypeConversationSchemaId, "emails.foobar"));
  EXPECT_FALSE(schema_store->IsPropertyDefinedInSchema(
      kTypeConversationSchemaId, "nestedNonIndexable.foobar"));
  EXPECT_FALSE(schema_store->IsPropertyDefinedInSchema(
      kTypeConversationSchemaId, "emails.timestamp.foo"));
  EXPECT_FALSE(schema_store->IsPropertyDefinedInSchema(
      kTypeConversationSchemaId, "emails.time"));
}

TEST_F(SchemaStoreTest, IntermediateDocumentPropertiesAreDefined) {
  SchemaTypeConfigProto email_type =
      SchemaTypeConfigBuilder()
          .SetType("Email")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("tagQualifiedId")
                           .SetDataType(TYPE_STRING)
                           .SetJoinable(JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                        DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("subject")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("text")
                  .SetDataTypeString(TERM_MATCH_UNKNOWN, TOKENIZER_NONE)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("timestamp")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .Build();

  SchemaTypeConfigProto conversation_type =
      SchemaTypeConfigBuilder()
          .SetType("Conversation")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("emails")
                           .SetDataTypeDocument(
                               "Email", /*index_nested_properties=*/true)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("nestedNonIndexable")
                  .SetDataTypeDocument("Email",
                                       /*index_nested_properties=*/false)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  SchemaProto schema =
      SchemaBuilder().AddType(email_type).AddType(conversation_type).Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));
  constexpr SchemaTypeId kTypeConversationSchemaId = 1;

  // Intermediate documents props.
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeConversationSchemaId,
                                                      "emails"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeConversationSchemaId,
                                                      "nestedNonIndexable"));
}

TEST_F(SchemaStoreTest, CyclePathsAreDefined) {
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("subject")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("b")
                  .SetDataTypeDocument("B", /*index_nested_properties=*/true)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();

  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("B")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("body")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("a")
                  .SetDataTypeDocument("A", /*index_nested_properties=*/false)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type_a).AddType(type_b).Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));
  constexpr SchemaTypeId kTypeASchemaId = 0;
  constexpr SchemaTypeId kTypeBSchemaId = 1;

  // A's top-level properties
  EXPECT_TRUE(
      schema_store->IsPropertyDefinedInSchema(kTypeASchemaId, "subject"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeASchemaId, "b"));

  // A's nested properties in B
  EXPECT_TRUE(
      schema_store->IsPropertyDefinedInSchema(kTypeASchemaId, "b.body"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeASchemaId, "b.a"));

  // A's nested properties in B's nested property in A
  EXPECT_TRUE(
      schema_store->IsPropertyDefinedInSchema(kTypeASchemaId, "b.a.subject"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeASchemaId, "b.a.b"));

  // B's top-level properties
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeBSchemaId, "body"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeBSchemaId, "a"));

  // B's nested properties in A
  EXPECT_TRUE(
      schema_store->IsPropertyDefinedInSchema(kTypeBSchemaId, "a.subject"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeBSchemaId, "a.b"));

  // B's nested properties in A's nested property in B
  EXPECT_TRUE(
      schema_store->IsPropertyDefinedInSchema(kTypeBSchemaId, "a.b.body"));
  EXPECT_TRUE(schema_store->IsPropertyDefinedInSchema(kTypeBSchemaId, "a.b.a"));
}

TEST_F(SchemaStoreTest, WrongTypeCyclePathsAreUndefined) {
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("subject")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("b")
                  .SetDataTypeDocument("B", /*index_nested_properties=*/true)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();

  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("B")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("body")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("a")
                  .SetDataTypeDocument("A", /*index_nested_properties=*/false)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type_a).AddType(type_b).Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));
  constexpr SchemaTypeId kTypeASchemaId = 0;
  constexpr SchemaTypeId kTypeBSchemaId = 1;

  // The same paths as above, but we check the wrong types instead.
  // A's top-level properties
  EXPECT_FALSE(
      schema_store->IsPropertyDefinedInSchema(kTypeBSchemaId, "subject"));
  EXPECT_FALSE(schema_store->IsPropertyDefinedInSchema(kTypeBSchemaId, "b"));

  // A's nested properties in B
  EXPECT_FALSE(
      schema_store->IsPropertyDefinedInSchema(kTypeBSchemaId, "b.body"));
  EXPECT_FALSE(schema_store->IsPropertyDefinedInSchema(kTypeBSchemaId, "b.a"));

  // A's nested properties in B's nested property in A
  EXPECT_FALSE(
      schema_store->IsPropertyDefinedInSchema(kTypeBSchemaId, "b.a.subject"));
  EXPECT_FALSE(
      schema_store->IsPropertyDefinedInSchema(kTypeBSchemaId, "b.a.b"));

  // B's top-level properties
  EXPECT_FALSE(schema_store->IsPropertyDefinedInSchema(kTypeASchemaId, "body"));
  EXPECT_FALSE(schema_store->IsPropertyDefinedInSchema(kTypeASchemaId, "a"));

  // B's nested properties in A
  EXPECT_FALSE(
      schema_store->IsPropertyDefinedInSchema(kTypeASchemaId, "a.subject"));
  EXPECT_FALSE(schema_store->IsPropertyDefinedInSchema(kTypeASchemaId, "a.b"));

  // B's nested properties in A's nested property in B
  EXPECT_FALSE(
      schema_store->IsPropertyDefinedInSchema(kTypeASchemaId, "a.b.body"));
  EXPECT_FALSE(
      schema_store->IsPropertyDefinedInSchema(kTypeASchemaId, "a.b.a"));
}

TEST_F(SchemaStoreTest, CyclePathsNonexistentPropertiesAreUndefined) {
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("subject")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("b")
                  .SetDataTypeDocument("B", /*index_nested_properties=*/true)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();

  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("B")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("body")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("a")
                  .SetDataTypeDocument("A", /*index_nested_properties=*/false)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type_a).AddType(type_b).Build();
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));
  constexpr SchemaTypeId kTypeASchemaId = 0;
  constexpr SchemaTypeId kTypeBSchemaId = 1;

  // Undefined paths in A
  EXPECT_FALSE(
      schema_store->IsPropertyDefinedInSchema(kTypeASchemaId, "b.subject"));
  EXPECT_FALSE(
      schema_store->IsPropertyDefinedInSchema(kTypeASchemaId, "b.a.body"));
  EXPECT_FALSE(
      schema_store->IsPropertyDefinedInSchema(kTypeASchemaId, "b.a.a"));
  EXPECT_FALSE(
      schema_store->IsPropertyDefinedInSchema(kTypeASchemaId, "b.a.subject.b"));

  // Undefined paths in B
  EXPECT_FALSE(
      schema_store->IsPropertyDefinedInSchema(kTypeBSchemaId, "a.body"));
  EXPECT_FALSE(
      schema_store->IsPropertyDefinedInSchema(kTypeBSchemaId, "a.b.subject"));
  EXPECT_FALSE(
      schema_store->IsPropertyDefinedInSchema(kTypeBSchemaId, "a.b.b"));
  EXPECT_FALSE(
      schema_store->IsPropertyDefinedInSchema(kTypeBSchemaId, "a.b.body.a"));
}

TEST_F(SchemaStoreTest, LoadsOverlaySchemaOnInit) {
  // Create a schema that is rollback incompatible and will trigger us to create
  // an overlay schema.
  PropertyConfigBuilder indexed_string_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN);
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("type_a")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("propRfc")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_RFC822))
          .Build();
  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("type_b")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type_a).AddType(type_b).Build();

  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }

  {
    // Create a new of the schema store and check that the same schema is
    // present.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));

    // The overlay should exist
    std::string overlay_schema_path = schema_store_dir_ + "/overlay_schema.pb";
    ASSERT_TRUE(filesystem_.FileExists(overlay_schema_path.c_str()));

    // The base schema should hold a compatible schema
    SchemaTypeConfigProto modified_type_a =
        SchemaTypeConfigBuilder()
            .SetType("type_a")
            .AddProperty(indexed_string_property_builder.SetName("prop0"))
            .AddProperty(PropertyConfigBuilder()
                             .SetName("propRfc")
                             .SetCardinality(CARDINALITY_OPTIONAL)
                             .SetDataType(TYPE_STRING))
            .Build();
    SchemaProto expected_base_schema =
        SchemaBuilder().AddType(modified_type_a).AddType(type_b).Build();
    std::string base_schema_path = schema_store_dir_ + "/schema.pb";
    auto base_schema_file_ = std::make_unique<FileBackedProto<SchemaProto>>(
        filesystem_, base_schema_path);
    ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* base_schema,
                               base_schema_file_->Read());
    EXPECT_THAT(*base_schema, EqualsProto(expected_base_schema));
  }
}

TEST_F(SchemaStoreTest, LoadsBaseSchemaWithNoOverlayOnInit) {
  // Create a normal schema that won't require an overlay.
  PropertyConfigBuilder indexed_string_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN);
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("type_a")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("propRfc")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN))
          .Build();
  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("type_b")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type_a).AddType(type_b).Build();

  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }

  {
    // Create a new instance of the schema store and check that the same schema
    // is present.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));

    // Additionally, the overlay should not exist
    std::string overlay_schema_path = schema_store_dir_ + "/overlay_schema.pb";
    ASSERT_FALSE(filesystem_.FileExists(overlay_schema_path.c_str()));
  }
}

TEST_F(SchemaStoreTest, LoadSchemaBackupSchemaMissing) {
  // Create a schema that is rollback incompatible and will trigger us to create
  // a backup schema.
  PropertyConfigBuilder indexed_string_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN);
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("type_a")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("propRfc")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_RFC822))
          .Build();
  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("type_b")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type_a).AddType(type_b).Build();

  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }

  // Delete the backup schema.
  std::string backup_schema_path = schema_store_dir_ + "/schema.pb";
  ASSERT_TRUE(filesystem_.DeleteFile(backup_schema_path.c_str()));

  {
    // Create a new instance of the schema store and check that it fails because
    // the backup schema is not available.
    EXPECT_THAT(SchemaStore::Create(&filesystem_, schema_store_dir_,
                                    &fake_clock_, feature_flags_.get()),
                StatusIs(libtextclassifier3::StatusCode::INTERNAL));
  }
}

TEST_F(SchemaStoreTest, LoadSchemaOverlaySchemaMissing) {
  // Create a schema that is rollback incompatible and will trigger us to create
  // a backup schema.
  PropertyConfigBuilder indexed_string_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN);
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("type_a")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("propRfc")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_RFC822))
          .Build();
  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("type_b")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type_a).AddType(type_b).Build();

  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }

  // Delete the overlay schema.
  std::string overlay_schema_path = schema_store_dir_ + "/overlay_schema.pb";
  ASSERT_TRUE(filesystem_.DeleteFile(overlay_schema_path.c_str()));

  {
    // Create a new instance of the schema store and check that it fails because
    // the overlay schema is not available when we expected it to be.
    EXPECT_THAT(SchemaStore::Create(&filesystem_, schema_store_dir_,
                                    &fake_clock_, feature_flags_.get()),
                StatusIs(libtextclassifier3::StatusCode::INTERNAL));
  }
}

TEST_F(SchemaStoreTest, LoadSchemaHeaderMissing) {
  // Create a schema that is rollback incompatible and will trigger us to create
  // a backup schema.
  PropertyConfigBuilder indexed_string_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN);
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("type_a")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("propRfc")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_RFC822))
          .Build();
  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("type_b")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type_a).AddType(type_b).Build();

  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }

  // Delete the overlay schema.
  std::string schema_header_path = schema_store_dir_ + "/schema_store_header";
  ASSERT_TRUE(filesystem_.DeleteFile(schema_header_path.c_str()));

  {
    // Create a new of the schema store and check that the same schema is
    // present.
    EXPECT_THAT(SchemaStore::Create(&filesystem_, schema_store_dir_,
                                    &fake_clock_, feature_flags_.get()),
                StatusIs(libtextclassifier3::StatusCode::INTERNAL));
  }
}

TEST_F(SchemaStoreTest, LoadSchemaNoOverlayHeaderMissing) {
  // Create a normal schema that won't require a backup.
  PropertyConfigBuilder indexed_string_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN);
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("type_a")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("propRfc")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN))
          .Build();
  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("type_b")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type_a).AddType(type_b).Build();

  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }

  // Delete the schema header.
  std::string schema_header_path = schema_store_dir_ + "/schema_store_header";
  ASSERT_TRUE(filesystem_.DeleteFile(schema_header_path.c_str()));

  {
    // Create a new instance of the schema store and check that it fails because
    // the schema header (which is now a part of the ground truth) is not
    // available.
    EXPECT_THAT(SchemaStore::Create(&filesystem_, schema_store_dir_,
                                    &fake_clock_, feature_flags_.get()),
                StatusIs(libtextclassifier3::StatusCode::INTERNAL));
  }
}

TEST_F(SchemaStoreTest, MigrateSchemaCompatibleNoChange) {
  // Create a schema that is rollback incompatible and will trigger us to create
  // a backup schema.
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("type_a")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("propRfc")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_RFC822))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type_a).Build();

  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }

  ICING_EXPECT_OK(SchemaStore::MigrateSchema(
      &filesystem_, schema_store_dir_, version_util::StateChange::kCompatible,
      version_util::kVersion, /*perform_schema_database_migration=*/false));

  {
    // Create a new of the schema store and check that the same schema is
    // present.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }
}

TEST_F(SchemaStoreTest, MigrateSchemaUpgradeNoChange) {
  // Create a schema that is rollback incompatible and will trigger us to create
  // a backup schema.
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("type_a")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("propRfc")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_RFC822))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type_a).Build();

  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }

  ICING_EXPECT_OK(SchemaStore::MigrateSchema(
      &filesystem_, schema_store_dir_, version_util::StateChange::kUpgrade,
      version_util::kVersion + 1, /*perform_schema_database_migration=*/false));

  {
    // Create a new of the schema store and check that the same schema is
    // present.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }
}

TEST_F(SchemaStoreTest, MigrateSchemaVersionZeroUpgradeNoChange) {
  // Because we are upgrading from version zero, the schema must be compatible
  // with version zero.
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("type_a")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("propRfc")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type_a).Build();

  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }

  ICING_EXPECT_OK(SchemaStore::MigrateSchema(
      &filesystem_, schema_store_dir_,
      version_util::StateChange::kVersionZeroUpgrade,
      version_util::kVersion + 1, /*perform_schema_database_migration=*/false));

  {
    // Create a new of the schema store and check that the same schema is
    // present.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }
}

TEST_F(SchemaStoreTest,
       MigrateSchemaRollbackDiscardsIncompatibleOverlaySchema) {
  // Because we are upgrading from version zero, the schema must be compatible
  // with version zero.
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("type_a")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("propRfc")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_RFC822))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type_a).Build();

  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }

  // Rollback to a version before kVersionOne. The schema header will declare
  // that the overlay is compatible with any version starting with kVersionOne.
  // So kVersionOne - 1 is incompatible and will throw out the schema.
  ICING_EXPECT_OK(SchemaStore::MigrateSchema(
      &filesystem_, schema_store_dir_, version_util::StateChange::kRollBack,
      version_util::kVersionOne - 1,
      /*perform_schema_database_migration=*/false));

  {
    // Create a new of the schema store and check that we fell back to the
    // base schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));

    SchemaTypeConfigProto other_type_a =
        SchemaTypeConfigBuilder()
            .SetType("type_a")
            .AddProperty(PropertyConfigBuilder()
                             .SetName("propRfc")
                             .SetCardinality(CARDINALITY_OPTIONAL)
                             .SetDataType(TYPE_STRING))
            .Build();
    SchemaProto base_schema = SchemaBuilder().AddType(other_type_a).Build();
    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(base_schema))));
  }
}

TEST_F(SchemaStoreTest, MigrateSchemaRollbackKeepsCompatibleOverlaySchema) {
  // Because we are upgrading from version zero, the schema must be compatible
  // with version zero.
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("type_a")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("propRfc")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_RFC822))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type_a).Build();

  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }

  // Rollback to kVersion. The schema header will declare that the overlay is
  // compatible with any version starting with kVersion. So we will be
  // compatible and retain the overlay schema.
  ICING_EXPECT_OK(SchemaStore::MigrateSchema(
      &filesystem_, schema_store_dir_, version_util::StateChange::kRollBack,
      version_util::kVersion, /*perform_schema_database_migration=*/false));

  {
    // Create a new of the schema store and check that the same schema is
    // present.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }
}

TEST_F(SchemaStoreTest, MigrateSchemaRollforwardRetainsBaseSchema) {
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("type_a")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("propRfc")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_RFC822))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type_a).Build();
  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }

  // Rollback to a version before kVersionOne. The schema header will declare
  // that the overlay is compatible with any version starting with kVersionOne.
  // So kVersionOne - 1 is incompatible and will throw out the schema.
  ICING_EXPECT_OK(SchemaStore::MigrateSchema(
      &filesystem_, schema_store_dir_, version_util::StateChange::kRollBack,
      version_util::kVersionOne - 1,
      /*perform_schema_database_migration=*/false));

  SchemaTypeConfigProto other_type_a =
      SchemaTypeConfigBuilder()
          .SetType("type_a")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("propRfc")
                           .SetCardinality(CARDINALITY_OPTIONAL)
                           .SetDataType(TYPE_STRING))
          .Build();
  SchemaProto base_schema = SchemaBuilder().AddType(other_type_a).Build();

  {
    // Create a new of the schema store and check that we fell back to the
    // base schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(base_schema))));
  }

  // Now rollforward to a new version. This should accept whatever schema is
  // present (currently base schema)
  ICING_EXPECT_OK(SchemaStore::MigrateSchema(
      &filesystem_, schema_store_dir_, version_util::StateChange::kRollForward,
      version_util::kVersion, /*perform_schema_database_migration=*/false));
  {
    // Create a new of the schema store and check that we fell back to the
    // base schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(base_schema))));
  }
}

TEST_F(SchemaStoreTest, MigrateSchemaRollforwardRetainsOverlaySchema) {
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("type_a")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("propRfc")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_RFC822))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type_a).Build();
  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }

  // Rollback to kVersion. The schema header will declare that the overlay is
  // compatible with any version starting with kVersion. So we will be
  // compatible and retain the overlay schema.
  ICING_EXPECT_OK(SchemaStore::MigrateSchema(
      &filesystem_, schema_store_dir_, version_util::StateChange::kRollBack,
      version_util::kVersion, /*perform_schema_database_migration=*/false));

  {
    // Create a new of the schema store and check that the same schema is
    // present.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }

  // Now rollforward to a new version. This should accept whatever schema is
  // present (currently overlay schema)
  ICING_EXPECT_OK(SchemaStore::MigrateSchema(
      &filesystem_, schema_store_dir_, version_util::StateChange::kRollForward,
      version_util::kVersion, /*perform_schema_database_migration=*/false));
  {
    // Create a new of the schema store and check that the same schema is
    // present.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }
}

TEST_F(SchemaStoreTest,
       MigrateSchemaVersionZeroRollforwardDiscardsOverlaySchema) {
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("type_a")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("propRfc")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_RFC822))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type_a).Build();
  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }

  // A VersionZeroRollforward will always discard the overlay schema because it
  // could be stale.
  ICING_EXPECT_OK(SchemaStore::MigrateSchema(
      &filesystem_, schema_store_dir_,
      version_util::StateChange::kVersionZeroRollForward,
      version_util::kVersion, /*perform_schema_database_migration=*/false));

  SchemaTypeConfigProto other_type_a =
      SchemaTypeConfigBuilder()
          .SetType("type_a")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("propRfc")
                           .SetCardinality(CARDINALITY_OPTIONAL)
                           .SetDataType(TYPE_STRING))
          .Build();
  SchemaProto base_schema = SchemaBuilder().AddType(other_type_a).Build();

  {
    // Create a new of the schema store and check that we fell back to the
    // base schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(base_schema))));
  }
}

TEST_F(SchemaStoreTest, MigrateSchemaVersionUndeterminedDiscardsOverlaySchema) {
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("type_a")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("propRfc")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_RFC822))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type_a).Build();
  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema))));
  }

  // An Undetermined will always discard the overlay schema because it doesn't
  // know which state we're in and so it fallback to the base schema because
  // it should always be valid.
  ICING_EXPECT_OK(SchemaStore::MigrateSchema(
      &filesystem_, schema_store_dir_, version_util::StateChange::kUndetermined,
      version_util::kVersion, /*perform_schema_database_migration=*/false));

  SchemaTypeConfigProto other_type_a =
      SchemaTypeConfigBuilder()
          .SetType("type_a")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("propRfc")
                           .SetCardinality(CARDINALITY_OPTIONAL)
                           .SetDataType(TYPE_STRING))
          .Build();
  SchemaProto base_schema = SchemaBuilder().AddType(other_type_a).Build();

  {
    // Create a new of the schema store and check that we fell back to the
    // base schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(base_schema))));
  }
}

TEST_F(SchemaStoreTest, GetTypeWithBlobProperties) {
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("blob")
                           .SetDataType(TYPE_BLOB_HANDLE)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("nonBlob")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();

  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("B")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("typeA")
                  .SetDataTypeDocument("A", /*index_nested_properties=*/false)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("nonBlob")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();

  SchemaTypeConfigProto type_c =
      SchemaTypeConfigBuilder()
          .SetType("C")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("nonBlob")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();

  // type_a contains blob property.
  // type_b contains nested type_a, which contains blob property.
  // type_c contains no blob property.
  SchemaProto schema =
      SchemaBuilder().AddType(type_a).AddType(type_b).AddType(type_c).Build();

  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(schema_store->ConstructBlobPropertyMap(),
                IsOkAndHolds(UnorderedElementsAre(
                    Pair("A", UnorderedElementsAre("blob")),
                    Pair("B", UnorderedElementsAre("typeA.blob")))));
  }
}

TEST_F(SchemaStoreTest, GetTypeWithMultiLevelBlobProperties) {
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("blob")
                           .SetDataType(TYPE_BLOB_HANDLE)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("nonBlob")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();

  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("B")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("typeA")
                  .SetDataTypeDocument("A", /*index_nested_properties=*/false)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("nonBlob")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();

  SchemaTypeConfigProto type_c =
      SchemaTypeConfigBuilder()
          .SetType("C")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("typeB")
                  .SetDataTypeDocument("B", /*index_nested_properties=*/false)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("blob")
                           .SetDataType(TYPE_BLOB_HANDLE)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("nonBlob")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();

  // type_a contains blob property.
  // type_b contains nested type_a, which contains blob property.
  // type_c contains blob property and nested type_b, which contains blob
  // property.
  SchemaProto schema =
      SchemaBuilder().AddType(type_a).AddType(type_b).AddType(type_c).Build();
  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        schema, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(
        schema_store->ConstructBlobPropertyMap(),
        IsOkAndHolds(UnorderedElementsAre(
            Pair("A", UnorderedElementsAre("blob")),
            Pair("B", UnorderedElementsAre("typeA.blob")),
            Pair("C", UnorderedElementsAre("blob", "typeB.typeA.blob")))));
  }
}

TEST_F(SchemaStoreTest, GetScorablePropertyIndex_SchemaNotSet) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  EXPECT_THAT(schema_store->GetScorablePropertyIndex(
                  /*schema_type_id=*/0,
                  /*property_path=*/"timestamp"),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(SchemaStoreTest, GetScorablePropertyIndex_InvalidSchemaTypeId) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // Set schema
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema_, /*ignore_errors_and_delete_documents=*/false));

  // non-existing schema type id
  EXPECT_THAT(schema_store->GetScorablePropertyIndex(
                  /*schema_type_id=*/100,
                  /*property_path=*/"timestamp"),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(SchemaStoreTest, GetScorablePropertyIndex_InvalidPropertyName) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaProto schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("email")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("subject")
                          .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("scoreDouble")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetCardinality(CARDINALITY_OPTIONAL)
                          .SetScorableType(SCORABLE_TYPE_ENABLED))
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("timestamp")
                                   .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                   .SetCardinality(CARDINALITY_OPTIONAL)
                                   .SetScorableType(SCORABLE_TYPE_ENABLED)))
          .Build();

  // Set schema
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));

  // non-scorable property
  EXPECT_THAT(schema_store->GetScorablePropertyIndex(
                  /*schema_type_id=*/0,
                  /*property_path=*/"subject"),
              IsOkAndHolds(Eq(std::nullopt)));
  // non-existing property
  EXPECT_THAT(schema_store->GetScorablePropertyIndex(
                  /*schema_type_id=*/0,
                  /*property_path=*/"non_existing"),
              IsOkAndHolds(Eq(std::nullopt)));
}

TEST_F(SchemaStoreTest, GetScorablePropertyIndex_Ok) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // Set schema
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema_, /*ignore_errors_and_delete_documents=*/false));

  EXPECT_THAT(schema_store->GetScorablePropertyIndex(
                  /*schema_type_id=*/0,
                  /*property_path=*/"timestamp"),
              IsOkAndHolds(0));
}

TEST_F(SchemaStoreTest, GetOrderedScorablePropertyPaths_SchemaNotSet) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  EXPECT_THAT(schema_store->GetOrderedScorablePropertyInfo(
                  /*schema_type_id=*/0),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(SchemaStoreTest, GetOrderedScorablePropertyPaths_InvalidSchemaTypeId) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // Set schema
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema_, /*ignore_errors_and_delete_documents=*/false));

  EXPECT_THAT(schema_store->GetOrderedScorablePropertyInfo(
                  /*schema_type_id=*/100),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(SchemaStoreTest, GetOrderedScorablePropertyPaths_Ok) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaProto schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("email")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("subject")
                          .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("scoreDouble")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetCardinality(CARDINALITY_OPTIONAL)
                          .SetScorableType(SCORABLE_TYPE_ENABLED))
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("timestamp")
                                   .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                   .SetCardinality(CARDINALITY_OPTIONAL)
                                   .SetScorableType(SCORABLE_TYPE_ENABLED)))
          .AddType(SchemaTypeConfigBuilder().SetType("message").AddProperty(
              PropertyConfigBuilder()
                  .SetName("subject")
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // Set schema
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema, /*ignore_errors_and_delete_documents=*/false));
  EXPECT_THAT(schema_store->GetSchemaTypeId("email"), IsOkAndHolds(0));
  EXPECT_THAT(schema_store->GetSchemaTypeId("message"), IsOkAndHolds(1));

  // no scorable properties under the schema, 'message'.
  EXPECT_THAT(schema_store->GetOrderedScorablePropertyInfo(
                  /*schema_type_id=*/1),
              IsOkAndHolds(Pointee(ElementsAre())));

  EXPECT_THAT(schema_store->GetOrderedScorablePropertyInfo(
                  /*schema_type_id=*/0),
              IsOkAndHolds(Pointee(ElementsAre(
                  EqualsScorablePropertyInfo("scoreDouble", TYPE_DOUBLE),
                  EqualsScorablePropertyInfo("timestamp", TYPE_INT64)))));
}

TEST_F(SchemaStoreTest, ScorablePropertyManagerUpdatesUponSchemaChange) {
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  // Sets the initial schema
  ICING_ASSERT_OK(schema_store->SetSchema(
      schema_, /*ignore_errors_and_delete_documents=*/false));

  EXPECT_THAT(schema_store->GetScorablePropertyIndex(
                  /*schema_type_id=*/0,
                  /*property_path=*/"timestamp"),
              IsOkAndHolds(0));
  EXPECT_THAT(schema_store->GetOrderedScorablePropertyInfo(
                  /*schema_type_id=*/0),
              IsOkAndHolds(Pointee(ElementsAre(
                  EqualsScorablePropertyInfo("timestamp", TYPE_INT64)))));

  // The new schema drops the type 'email', and adds a new type 'message'.
  SchemaProto new_schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("message")
                  .AddProperty(
                      // Add an indexed property so we generate
                      // section metadata on it
                      PropertyConfigBuilder()
                          .SetName("content")
                          .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("scoreInt")
                                   .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                   .SetCardinality(CARDINALITY_OPTIONAL)
                                   .SetScorableType(SCORABLE_TYPE_ENABLED))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("scoreDouble")
                          .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                          .SetCardinality(CARDINALITY_OPTIONAL)
                          .SetScorableType(SCORABLE_TYPE_ENABLED)))
          .Build();

  // Force updates the schema.
  ICING_ASSERT_OK(schema_store->SetSchema(
      new_schema, /*ignore_errors_and_delete_documents=*/true));

  // "timestamp" is no longer a valid property name.
  EXPECT_THAT(schema_store->GetScorablePropertyIndex(
                  /*schema_type_id=*/0,
                  /*property_path=*/"timestamp"),
              IsOkAndHolds(Eq(std::nullopt)));

  // ok cases for the new schema.
  EXPECT_THAT(schema_store->GetScorablePropertyIndex(
                  /*schema_type_id=*/0,
                  /*property_path=*/"scoreInt"),
              IsOkAndHolds(1));
  EXPECT_THAT(schema_store->GetScorablePropertyIndex(
                  /*schema_type_id=*/0,
                  /*property_path=*/"scoreDouble"),
              IsOkAndHolds(0));
  EXPECT_THAT(schema_store->GetOrderedScorablePropertyInfo(
                  /*schema_type_id=*/0),
              IsOkAndHolds(Pointee(ElementsAre(
                  EqualsScorablePropertyInfo("scoreDouble", TYPE_DOUBLE),
                  EqualsScorablePropertyInfo("scoreInt", TYPE_INT64)))));
}

class SchemaStoreTestWithParam
    : public SchemaStoreTest,
      public testing::WithParamInterface<version_util::StateChange> {};

TEST_P(SchemaStoreTestWithParam, MigrateSchemaWithDatabaseMigration) {
  SchemaProto schema_no_database =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db1/email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("db1Subject")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_RFC822)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db2/email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("db2Subject")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        schema_no_database, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(schema_no_database))));
  }

  SchemaTypeConfigProto db1_email_rfc =
      SchemaTypeConfigBuilder()
          .SetType("db1/email")
          .SetDatabase("db1")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("db1Subject")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_RFC822))
          .Build();
  SchemaTypeConfigProto db1_email_no_rfc =
      SchemaTypeConfigBuilder()
          .SetType("db1/email")
          .SetDatabase("db1")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("db1Subject")
                           .SetCardinality(CARDINALITY_OPTIONAL)
                           .SetDataType(TYPE_STRING))
          .Build();
  SchemaTypeConfigProto db2_email =
      SchemaTypeConfigBuilder()
          .SetType("db2/email")
          .SetDatabase("db2")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("db2Subject")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  SchemaProto full_schema_with_database_rfc =
      SchemaBuilder().AddType(db1_email_rfc).AddType(db2_email).Build();
  SchemaProto full_schema_with_database_no_rfc =
      SchemaBuilder().AddType(db1_email_no_rfc).AddType(db2_email).Build();
  SchemaProto db1_schema_rfc = SchemaBuilder().AddType(db1_email_rfc).Build();
  SchemaProto db1_schema_no_rfc =
      SchemaBuilder().AddType(db1_email_no_rfc).Build();
  SchemaProto db2_schema = SchemaBuilder().AddType(db2_email).Build();

  ICING_EXPECT_OK(SchemaStore::MigrateSchema(
      &filesystem_, schema_store_dir_, /*version_state_change=*/GetParam(),
      version_util::kVersion, /*perform_schema_database_migration=*/true));
  {
    // Create a new instance of the schema store and check that the database
    // field is populated.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));

    if (GetParam() == version_util::StateChange::kVersionZeroRollForward ||
        GetParam() == version_util::StateChange::kUndetermined) {
      // For these cases, the overlay schema is discarded and we fall back to
      // the backup schema (no rfc version).
      EXPECT_THAT(
          schema_store->GetSchema(),
          IsOkAndHolds(Pointee(EqualsProto(full_schema_with_database_no_rfc))));
      EXPECT_THAT(schema_store->GetSchema("db1"),
                  IsOkAndHolds(EqualsProto(db1_schema_no_rfc)));
      EXPECT_THAT(schema_store->GetSchema("db2"),
                  IsOkAndHolds(EqualsProto(db2_schema)));
    } else {
      EXPECT_THAT(
          schema_store->GetSchema(),
          IsOkAndHolds(Pointee(EqualsProto(full_schema_with_database_rfc))));
      EXPECT_THAT(schema_store->GetSchema("db1"),
                  IsOkAndHolds(EqualsProto(db1_schema_rfc)));
      EXPECT_THAT(schema_store->GetSchema("db2"),
                  IsOkAndHolds(EqualsProto(db2_schema)));

      DocumentProto db1_email_doc =
          DocumentBuilder()
              .SetKey("namespace", "uri1")
              .SetSchema("db1/email")
              .AddStringProperty("db1Subject", "db1_subject")
              .Build();
      DocumentProto db2_email_doc =
          DocumentBuilder()
              .SetKey("namespace", "uri3")
              .SetSchema("db2/email")
              .AddStringProperty("db2Subject", "db2_subject")
              .Build();

      // Verify that our in-memory structures are ok
      ICING_ASSERT_OK_AND_ASSIGN(SectionGroup section_group,
                                 schema_store->ExtractSections(db1_email_doc));
      EXPECT_THAT(section_group.string_sections[0].content,
                  ElementsAre("db1_subject"));
      ICING_ASSERT_OK_AND_ASSIGN(section_group,
                                 schema_store->ExtractSections(db2_email_doc));
      EXPECT_THAT(section_group.string_sections[0].content,
                  ElementsAre("db2_subject"));

      // Verify that our persisted data are ok
      EXPECT_THAT(schema_store->GetSchemaTypeId("db1/email"), IsOkAndHolds(0));
      EXPECT_THAT(schema_store->GetSchemaTypeId("db2/email"), IsOkAndHolds(1));
    }
  }
}

TEST_P(SchemaStoreTestWithParam,
       MigrateSchemaWithDatabaseMigration_noDbPrefix) {
  SchemaTypeConfigProto email_rfc =
      SchemaTypeConfigBuilder()
          .SetType("email")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("subject")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_RFC822))
          .Build();
  SchemaTypeConfigProto email_no_rfc =
      SchemaTypeConfigBuilder()
          .SetType("email")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("subject")
                           .SetCardinality(CARDINALITY_OPTIONAL)
                           .SetDataType(TYPE_STRING))
          .Build();
  SchemaTypeConfigProto message =
      SchemaTypeConfigBuilder()
          .SetType("message")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("content")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();

  SchemaProto original_schema =
      SchemaBuilder().AddType(email_rfc).AddType(message).Build();

  {
    // Create an instance of the schema store and set the schema.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));
    ICING_ASSERT_OK(schema_store->SetSchema(
        original_schema, /*ignore_errors_and_delete_documents=*/false));

    EXPECT_THAT(schema_store->GetSchema(),
                IsOkAndHolds(Pointee(EqualsProto(original_schema))));
  }

  SchemaProto backup_schema =
      SchemaBuilder().AddType(email_no_rfc).AddType(message).Build();

  ICING_EXPECT_OK(SchemaStore::MigrateSchema(
      &filesystem_, schema_store_dir_, /*version_state_change=*/GetParam(),
      version_util::kVersion, /*perform_schema_database_migration=*/true));

  {
    // Create a new instance of the schema store and check that the database
    // field is populated.
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                            feature_flags_.get()));

    if (GetParam() == version_util::StateChange::kVersionZeroRollForward ||
        GetParam() == version_util::StateChange::kUndetermined) {
      // For these cases, the overlay schema is discarded and we fall back to
      // the backup schema (no rfc version).
      EXPECT_THAT(schema_store->GetSchema(),
                  IsOkAndHolds(Pointee(EqualsProto(backup_schema))));
      EXPECT_THAT(schema_store->GetSchema("db"),
                  StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
    } else {
      EXPECT_THAT(schema_store->GetSchema(),
                  IsOkAndHolds(Pointee(EqualsProto(original_schema))));
      EXPECT_THAT(schema_store->GetSchema("db"),
                  StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

      DocumentProto email_doc = DocumentBuilder()
                                    .SetKey("namespace", "uri1")
                                    .SetSchema("email")
                                    .AddStringProperty("subject", "subject")
                                    .Build();
      DocumentProto message_doc = DocumentBuilder()
                                      .SetKey("namespace", "uri2")
                                      .SetSchema("message")
                                      .AddStringProperty("content", "content")
                                      .Build();

      // Verify that our in-memory structures are ok
      ICING_ASSERT_OK_AND_ASSIGN(SectionGroup section_group,
                                 schema_store->ExtractSections(email_doc));
      EXPECT_THAT(section_group.string_sections[0].content,
                  ElementsAre("subject"));
      ICING_ASSERT_OK_AND_ASSIGN(section_group,
                                 schema_store->ExtractSections(message_doc));
      EXPECT_THAT(section_group.string_sections[0].content,
                  ElementsAre("content"));

      // Verify that our persisted data are ok
      EXPECT_THAT(schema_store->GetSchemaTypeId("email"), IsOkAndHolds(0));
      EXPECT_THAT(schema_store->GetSchemaTypeId("message"), IsOkAndHolds(1));
    }
  }
}

INSTANTIATE_TEST_SUITE_P(
    SchemaStoreTestWithParam, SchemaStoreTestWithParam,
    testing::Values(
        /*version_state_change=*/
        version_util::StateChange::kUndetermined,
        version_util::StateChange::kCompatible,
        version_util::StateChange::kRollForward,
        version_util::StateChange::kRollBack,
        version_util::StateChange::kUpgrade,
        version_util::StateChange::kVersionZeroUpgrade,
        version_util::StateChange::kVersionZeroRollForward));

}  // namespace

}  // namespace lib
}  // namespace icing
