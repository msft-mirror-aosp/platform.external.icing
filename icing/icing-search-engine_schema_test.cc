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

#include "icing/icing-search-engine.h"

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/file/mock-filesystem.h"
#include "icing/jni/jni-cache.h"
#include "icing/portable/endian.h"
#include "icing/portable/equals-proto.h"
#include "icing/portable/platform.h"
#include "icing/proto/debug.pb.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/document_wrapper.pb.h"
#include "icing/proto/initialize.pb.h"
#include "icing/proto/logging.pb.h"
#include "icing/proto/optimize.pb.h"
#include "icing/proto/persist.pb.h"
#include "icing/proto/reset.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/status.pb.h"
#include "icing/proto/storage.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/proto/usage.pb.h"
#include "icing/schema-builder.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/icu-data-file-helper.h"
#include "icing/testing/jni-test-helpers.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::Return;

// For mocking purpose, we allow tests to provide a custom Filesystem.
class TestIcingSearchEngine : public IcingSearchEngine {
 public:
  TestIcingSearchEngine(const IcingSearchEngineOptions& options,
                        std::unique_ptr<const Filesystem> filesystem,
                        std::unique_ptr<const IcingFilesystem> icing_filesystem,
                        std::unique_ptr<Clock> clock,
                        std::unique_ptr<JniCache> jni_cache)
      : IcingSearchEngine(options, std::move(filesystem),
                          std::move(icing_filesystem), std::move(clock),
                          std::move(jni_cache)) {}
};

std::string GetTestBaseDir() { return GetTestTempDir() + "/icing"; }

// This test is meant to cover all tests relating to
// IcingSearchEngine::GetSchema and IcingSearchEngine::SetSchema.
class IcingSearchEngineSchemaTest : public testing::Test {
 protected:
  void SetUp() override {
    if (!IsCfStringTokenization() && !IsReverseJniTokenization()) {
      // If we've specified using the reverse-JNI method for segmentation (i.e.
      // not ICU), then we won't have the ICU data file included to set up.
      // Technically, we could choose to use reverse-JNI for segmentation AND
      // include an ICU data file, but that seems unlikely and our current BUILD
      // setup doesn't do this.
      // File generated via icu_data_file rule in //icing/BUILD.
      std::string icu_data_file_path =
          GetTestFilePath("icing/icu.dat");
      ICING_ASSERT_OK(
          icu_data_file_helper::SetUpICUDataFile(icu_data_file_path));
    }
    filesystem_.CreateDirectoryRecursively(GetTestBaseDir().c_str());
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(GetTestBaseDir().c_str());
  }

  const Filesystem* filesystem() const { return &filesystem_; }

 private:
  Filesystem filesystem_;
};

// Non-zero value so we don't override it to be the current time
constexpr int64_t kDefaultCreationTimestampMs = 1575492852000;

std::string GetSchemaDir() { return GetTestBaseDir() + "/schema_dir"; }

IcingSearchEngineOptions GetDefaultIcingOptions() {
  IcingSearchEngineOptions icing_options;
  icing_options.set_base_dir(GetTestBaseDir());
  return icing_options;
}

DocumentProto CreateMessageDocument(std::string name_space, std::string uri) {
  return DocumentBuilder()
      .SetKey(std::move(name_space), std::move(uri))
      .SetSchema("Message")
      .AddStringProperty("body", "message body")
      .SetCreationTimestampMs(kDefaultCreationTimestampMs)
      .Build();
}

SchemaProto CreateMessageSchema() {
  return SchemaBuilder()
      .AddType(SchemaTypeConfigBuilder().SetType("Message").AddProperty(
          PropertyConfigBuilder()
              .SetName("body")
              .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
              .SetCardinality(CARDINALITY_REQUIRED)))
      .Build();
}

ScoringSpecProto GetDefaultScoringSpec() {
  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  return scoring_spec;
}

TEST_F(IcingSearchEngineSchemaTest,
       CircularReferenceCreateSectionManagerReturnsInvalidArgument) {
  // Create a type config with a circular reference.
  SchemaProto schema;
  auto* type = schema.add_types();
  type->set_schema_type("Message");

  auto* body = type->add_properties();
  body->set_property_name("recipient");
  body->set_schema_type("Person");
  body->set_data_type(PropertyConfigProto::DataType::DOCUMENT);
  body->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);
  body->mutable_document_indexing_config()->set_index_nested_properties(true);

  type = schema.add_types();
  type->set_schema_type("Person");

  body = type->add_properties();
  body->set_property_name("recipient");
  body->set_schema_type("Message");
  body->set_data_type(PropertyConfigProto::DataType::DOCUMENT);
  body->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);
  body->mutable_document_indexing_config()->set_index_nested_properties(true);

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineSchemaTest, FailToReadSchema) {
  IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();

  {
    // Successfully initialize and set a schema
    IcingSearchEngine icing(icing_options, GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
  }

  auto mock_filesystem = std::make_unique<MockFilesystem>();

  // This fails FileBackedProto::Read() when we try to check the schema we
  // had previously set
  ON_CALL(*mock_filesystem,
          OpenForRead(Eq(icing_options.base_dir() + "/schema_dir/schema.pb")))
      .WillByDefault(Return(-1));

  TestIcingSearchEngine test_icing(icing_options, std::move(mock_filesystem),
                                   std::make_unique<IcingFilesystem>(),
                                   std::make_unique<FakeClock>(),
                                   GetTestJniCache());

  InitializeResultProto initialize_result_proto = test_icing.Initialize();
  EXPECT_THAT(initialize_result_proto.status(),
              ProtoStatusIs(StatusProto::INTERNAL));
  EXPECT_THAT(initialize_result_proto.status().message(),
              HasSubstr("Unable to open file for read"));
}

TEST_F(IcingSearchEngineSchemaTest, FailToWriteSchema) {
  IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();

  auto mock_filesystem = std::make_unique<MockFilesystem>();
  // This fails FileBackedProto::Write()
  ON_CALL(*mock_filesystem, OpenForWrite(HasSubstr("schema.pb")))
      .WillByDefault(Return(-1));

  TestIcingSearchEngine icing(icing_options, std::move(mock_filesystem),
                              std::make_unique<IcingFilesystem>(),
                              std::make_unique<FakeClock>(), GetTestJniCache());

  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());

  SetSchemaResultProto set_schema_result_proto =
      icing.SetSchema(CreateMessageSchema());
  EXPECT_THAT(set_schema_result_proto.status(),
              ProtoStatusIs(StatusProto::INTERNAL));
  EXPECT_THAT(set_schema_result_proto.status().message(),
              HasSubstr("Unable to open file for write"));
}

TEST_F(IcingSearchEngineSchemaTest, SetSchemaIncompatibleFails) {
  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    // 1. Create a schema with an Email type with properties { "title", "body"}
    SchemaProto schema;
    SchemaTypeConfigProto* type = schema.add_types();
    type->set_schema_type("Email");
    PropertyConfigProto* property = type->add_properties();
    property->set_property_name("title");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
    property = type->add_properties();
    property->set_property_name("body");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

    EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    // 2. Add an email document
    DocumentProto doc = DocumentBuilder()
                            .SetKey("emails", "email#1")
                            .SetSchema("Email")
                            .AddStringProperty("title", "Hello world.")
                            .AddStringProperty("body", "Goodnight Moon.")
                            .Build();
    EXPECT_THAT(icing.Put(std::move(doc)).status(), ProtoIsOk());
  }

  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    // 3. Set a schema that deletes email. This should fail.
    SchemaProto schema;
    SchemaTypeConfigProto* type = schema.add_types();
    type->set_schema_type("Message");
    PropertyConfigProto* property = type->add_properties();
    property->set_property_name("body");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

    EXPECT_THAT(
        icing.SetSchema(schema, /*ignore_errors_and_delete_documents=*/false)
            .status(),
        ProtoStatusIs(StatusProto::FAILED_PRECONDITION));

    // 4. Try to delete by email type. This should succeed because email wasn't
    // deleted in step 3.
    EXPECT_THAT(icing.DeleteBySchemaType("Email").status(), ProtoIsOk());
  }
}

TEST_F(IcingSearchEngineSchemaTest,
       SetSchemaIncompatibleForceOverrideSucceeds) {
  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    // 1. Create a schema with an Email type with properties { "title", "body"}
    SchemaProto schema;
    SchemaTypeConfigProto* type = schema.add_types();
    type->set_schema_type("Email");
    PropertyConfigProto* property = type->add_properties();
    property->set_property_name("title");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
    property = type->add_properties();
    property->set_property_name("body");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

    EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    // 2. Add an email document
    DocumentProto doc = DocumentBuilder()
                            .SetKey("emails", "email#1")
                            .SetSchema("Email")
                            .AddStringProperty("title", "Hello world.")
                            .AddStringProperty("body", "Goodnight Moon.")
                            .Build();
    EXPECT_THAT(icing.Put(std::move(doc)).status(), ProtoIsOk());
  }

  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    // 3. Set a schema that deletes email with force override. This should
    // succeed and delete the email type.
    SchemaProto schema;
    SchemaTypeConfigProto* type = schema.add_types();
    type->set_schema_type("Message");
    PropertyConfigProto* property = type->add_properties();
    property->set_property_name("body");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

    EXPECT_THAT(icing.SetSchema(schema, true).status(), ProtoIsOk());

    // 4. Try to delete by email type. This should fail because email was
    // already deleted.
    EXPECT_THAT(icing.DeleteBySchemaType("Email").status(),
                ProtoStatusIs(StatusProto::NOT_FOUND));
  }
}

TEST_F(IcingSearchEngineSchemaTest, SetSchemaUnsetVersionIsZero) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  // 1. Create a schema with an Email type with version 1
  SchemaProto schema;
  SchemaTypeConfigProto* type = schema.add_types();
  type->set_schema_type("Email");
  PropertyConfigProto* property = type->add_properties();
  property->set_property_name("title");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  EXPECT_THAT(icing.GetSchema().schema().types(0).version(), Eq(0));
}

TEST_F(IcingSearchEngineSchemaTest, SetSchemaCompatibleVersionUpdateSucceeds) {
  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    // 1. Create a schema with an Email type with version 1
    SchemaProto schema;
    SchemaTypeConfigProto* type = schema.add_types();
    type->set_version(1);
    type->set_schema_type("Email");
    PropertyConfigProto* property = type->add_properties();
    property->set_property_name("title");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

    SetSchemaResultProto set_schema_result = icing.SetSchema(schema);
    // Ignore latency numbers. They're covered elsewhere.
    set_schema_result.clear_latency_ms();
    SetSchemaResultProto expected_set_schema_result;
    expected_set_schema_result.mutable_status()->set_code(StatusProto::OK);
    expected_set_schema_result.mutable_new_schema_types()->Add("Email");
    EXPECT_THAT(set_schema_result, EqualsProto(expected_set_schema_result));

    EXPECT_THAT(icing.GetSchema().schema().types(0).version(), Eq(1));
  }

  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    // 2. Create schema that adds a new optional property and updates version.
    SchemaProto schema;
    SchemaTypeConfigProto* type = schema.add_types();
    type->set_version(2);
    type->set_schema_type("Email");
    PropertyConfigProto* property = type->add_properties();
    property->set_property_name("title");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
    property = type->add_properties();
    property->set_property_name("body");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

    // 3. SetSchema should succeed and the version number should be updated.
    SetSchemaResultProto set_schema_result = icing.SetSchema(schema, true);
    // Ignore latency numbers. They're covered elsewhere.
    set_schema_result.clear_latency_ms();
    SetSchemaResultProto expected_set_schema_result;
    expected_set_schema_result.mutable_status()->set_code(StatusProto::OK);
    expected_set_schema_result.mutable_fully_compatible_changed_schema_types()
        ->Add("Email");
    EXPECT_THAT(set_schema_result, EqualsProto(expected_set_schema_result));

    EXPECT_THAT(icing.GetSchema().schema().types(0).version(), Eq(2));
  }
}

TEST_F(IcingSearchEngineSchemaTest, SetSchemaIncompatibleVersionUpdateFails) {
  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    // 1. Create a schema with an Email type with version 1
    SchemaProto schema;
    SchemaTypeConfigProto* type = schema.add_types();
    type->set_version(1);
    type->set_schema_type("Email");
    PropertyConfigProto* property = type->add_properties();
    property->set_property_name("title");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

    EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    EXPECT_THAT(icing.GetSchema().schema().types(0).version(), Eq(1));
  }

  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    // 2. Create schema that makes an incompatible change (OPTIONAL -> REQUIRED)
    SchemaProto schema;
    SchemaTypeConfigProto* type = schema.add_types();
    type->set_version(2);
    type->set_schema_type("Email");
    PropertyConfigProto* property = type->add_properties();
    property->set_property_name("title");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);

    // 3. SetSchema should fail and the version number should NOT be updated.
    EXPECT_THAT(icing.SetSchema(schema).status(),
                ProtoStatusIs(StatusProto::FAILED_PRECONDITION));

    EXPECT_THAT(icing.GetSchema().schema().types(0).version(), Eq(1));
  }
}

TEST_F(IcingSearchEngineSchemaTest,
       SetSchemaIncompatibleVersionUpdateForceOverrideSucceeds) {
  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    // 1. Create a schema with an Email type with version 1
    SchemaProto schema;
    SchemaTypeConfigProto* type = schema.add_types();
    type->set_version(1);
    type->set_schema_type("Email");
    PropertyConfigProto* property = type->add_properties();
    property->set_property_name("title");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

    EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    EXPECT_THAT(icing.GetSchema().schema().types(0).version(), Eq(1));
  }

  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    // 2. Create schema that makes an incompatible change (OPTIONAL -> REQUIRED)
    // with force override to true.
    SchemaProto schema;
    SchemaTypeConfigProto* type = schema.add_types();
    type->set_version(2);
    type->set_schema_type("Email");
    PropertyConfigProto* property = type->add_properties();
    property->set_property_name("title");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);

    // 3. SetSchema should succeed and the version number should be updated.
    EXPECT_THAT(icing.SetSchema(schema, true).status(), ProtoIsOk());

    EXPECT_THAT(icing.GetSchema().schema().types(0).version(), Eq(2));
  }
}

TEST_F(IcingSearchEngineSchemaTest, SetSchemaNoChangeVersionUpdateSucceeds) {
  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    // 1. Create a schema with an Email type with version 1
    SchemaProto schema;
    SchemaTypeConfigProto* type = schema.add_types();
    type->set_version(1);
    type->set_schema_type("Email");
    PropertyConfigProto* property = type->add_properties();
    property->set_property_name("title");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

    EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    EXPECT_THAT(icing.GetSchema().schema().types(0).version(), Eq(1));
  }

  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    // 2. Create schema that only changes the version.
    SchemaProto schema;
    SchemaTypeConfigProto* type = schema.add_types();
    type->set_version(2);
    type->set_schema_type("Email");
    PropertyConfigProto* property = type->add_properties();
    property->set_property_name("title");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

    // 3. SetSchema should succeed and the version number should be updated.
    EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    EXPECT_THAT(icing.GetSchema().schema().types(0).version(), Eq(2));
  }
}

TEST_F(IcingSearchEngineSchemaTest,
       SetSchemaDuplicateTypesReturnsAlreadyExists) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  // Create a schema with types { "Email", "Message" and "Email" }
  SchemaProto schema;
  SchemaTypeConfigProto* type = schema.add_types();
  type->set_schema_type("Email");
  PropertyConfigProto* property = type->add_properties();
  property->set_property_name("title");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  type = schema.add_types();
  type->set_schema_type("Message");
  property = type->add_properties();
  property->set_property_name("body");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  *schema.add_types() = schema.types(0);

  EXPECT_THAT(icing.SetSchema(schema).status(),
              ProtoStatusIs(StatusProto::ALREADY_EXISTS));
}

TEST_F(IcingSearchEngineSchemaTest,
       SetSchemaDuplicatePropertiesReturnsAlreadyExists) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  // Create a schema with an Email type with properties { "title", "body" and
  // "title" }
  SchemaProto schema;
  SchemaTypeConfigProto* type = schema.add_types();
  type->set_schema_type("Email");
  PropertyConfigProto* property = type->add_properties();
  property->set_property_name("title");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
  property = type->add_properties();
  property->set_property_name("body");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
  property = type->add_properties();
  property->set_property_name("title");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  EXPECT_THAT(icing.SetSchema(schema).status(),
              ProtoStatusIs(StatusProto::ALREADY_EXISTS));
}

TEST_F(IcingSearchEngineSchemaTest, SetSchema) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(1000);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  auto message_document = CreateMessageDocument("namespace", "uri");

  auto schema_with_message = CreateMessageSchema();

  SchemaProto schema_with_email;
  SchemaTypeConfigProto* type = schema_with_email.add_types();
  type->set_schema_type("Email");
  PropertyConfigProto* property = type->add_properties();
  property->set_property_name("title");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  SchemaProto schema_with_email_and_message = schema_with_email;
  type = schema_with_email_and_message.add_types();
  type->set_schema_type("Message");
  property = type->add_properties();
  property->set_property_name("body");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  // Create an arbitrary invalid schema
  SchemaProto invalid_schema;
  SchemaTypeConfigProto* empty_type = invalid_schema.add_types();
  empty_type->set_schema_type("");

  // Make sure we can't set invalid schemas
  SetSchemaResultProto set_schema_result = icing.SetSchema(invalid_schema);
  EXPECT_THAT(set_schema_result.status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
  EXPECT_THAT(set_schema_result.latency_ms(), Eq(1000));

  // Can add an document of a set schema
  set_schema_result = icing.SetSchema(schema_with_message);
  EXPECT_THAT(set_schema_result.status(), ProtoStatusIs(StatusProto::OK));
  EXPECT_THAT(set_schema_result.latency_ms(), Eq(1000));
  EXPECT_THAT(icing.Put(message_document).status(), ProtoIsOk());

  // Schema with Email doesn't have Message, so would result incompatible
  // data
  set_schema_result = icing.SetSchema(schema_with_email);
  EXPECT_THAT(set_schema_result.status(),
              ProtoStatusIs(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(set_schema_result.latency_ms(), Eq(1000));

  // Can expand the set of schema types and add an document of a new
  // schema type
  set_schema_result = icing.SetSchema(schema_with_email_and_message);
  EXPECT_THAT(set_schema_result.status(), ProtoStatusIs(StatusProto::OK));
  EXPECT_THAT(set_schema_result.latency_ms(), Eq(1000));

  EXPECT_THAT(icing.Put(message_document).status(), ProtoIsOk());
  // Can't add an document whose schema isn't set
  auto photo_document = DocumentBuilder()
                            .SetKey("namespace", "uri")
                            .SetSchema("Photo")
                            .AddStringProperty("creator", "icing")
                            .Build();
  PutResultProto put_result_proto = icing.Put(photo_document);
  EXPECT_THAT(put_result_proto.status(), ProtoStatusIs(StatusProto::NOT_FOUND));
  EXPECT_THAT(put_result_proto.status().message(),
              HasSubstr("'Photo' not found"));
}

TEST_F(IcingSearchEngineSchemaTest,
       SetSchemaNewIndexedPropertyTriggersIndexRestorationAndReturnsOk) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  SchemaProto schema_with_no_indexed_property = CreateMessageSchema();
  schema_with_no_indexed_property.mutable_types(0)
      ->mutable_properties(0)
      ->clear_string_indexing_config();

  SetSchemaResultProto set_schema_result =
      icing.SetSchema(schema_with_no_indexed_property);
  // Ignore latency numbers. They're covered elsewhere.
  set_schema_result.clear_latency_ms();
  SetSchemaResultProto expected_set_schema_result;
  expected_set_schema_result.mutable_status()->set_code(StatusProto::OK);
  expected_set_schema_result.mutable_new_schema_types()->Add("Message");
  EXPECT_THAT(set_schema_result, EqualsProto(expected_set_schema_result));

  // Nothing will be index and Search() won't return anything.
  EXPECT_THAT(icing.Put(CreateMessageDocument("namespace", "uri")).status(),
              ProtoIsOk());

  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto empty_result;
  empty_result.mutable_status()->set_code(StatusProto::OK);

  SearchResultProto actual_results =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(actual_results,
              EqualsSearchResultIgnoreStatsAndScores(empty_result));

  SchemaProto schema_with_indexed_property = CreateMessageSchema();
  // Index restoration should be triggered here because new schema requires more
  // properties to be indexed.
  set_schema_result = icing.SetSchema(schema_with_indexed_property);
  // Ignore latency numbers. They're covered elsewhere.
  set_schema_result.clear_latency_ms();
  expected_set_schema_result = SetSchemaResultProto();
  expected_set_schema_result.mutable_status()->set_code(StatusProto::OK);
  expected_set_schema_result.mutable_index_incompatible_changed_schema_types()
      ->Add("Message");
  EXPECT_THAT(set_schema_result, EqualsProto(expected_set_schema_result));

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      CreateMessageDocument("namespace", "uri");
  actual_results = icing.Search(search_spec, GetDefaultScoringSpec(),
                                ResultSpecProto::default_instance());
  EXPECT_THAT(actual_results, EqualsSearchResultIgnoreStatsAndScores(
                                  expected_search_result_proto));
}

TEST_F(IcingSearchEngineSchemaTest,
       SetSchemaChangeNestedPropertiesTriggersIndexRestorationAndReturnsOk) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  SchemaTypeConfigProto person_proto =
      SchemaTypeConfigBuilder()
          .SetType("Person")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  SchemaProto nested_schema =
      SchemaBuilder()
          .AddType(person_proto)
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("sender")
                                        .SetDataTypeDocument(
                                            "Person",
                                            /*index_nested_properties=*/true)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  SetSchemaResultProto set_schema_result = icing.SetSchema(nested_schema);
  // Ignore latency numbers. They're covered elsewhere.
  set_schema_result.clear_latency_ms();
  SetSchemaResultProto expected_set_schema_result;
  expected_set_schema_result.mutable_status()->set_code(StatusProto::OK);
  expected_set_schema_result.mutable_new_schema_types()->Add("Email");
  expected_set_schema_result.mutable_new_schema_types()->Add("Person");
  EXPECT_THAT(set_schema_result, EqualsProto(expected_set_schema_result));

  DocumentProto document =
      DocumentBuilder()
          .SetKey("namespace1", "uri1")
          .SetSchema("Email")
          .SetCreationTimestampMs(1000)
          .AddStringProperty("subject",
                             "Did you get the memo about TPS reports?")
          .AddDocumentProperty("sender",
                               DocumentBuilder()
                                   .SetKey("namespace1", "uri1")
                                   .SetSchema("Person")
                                   .AddStringProperty("name", "Bill Lundbergh")
                                   .Build())
          .Build();

  // "sender.name" should get assigned property id 0 and subject should get
  // property id 1.
  EXPECT_THAT(icing.Put(document).status(), ProtoIsOk());

  // document should match a query for 'Bill' in 'sender.name', but not in
  // 'subject'
  SearchSpecProto search_spec;
  search_spec.set_query("sender.name:Bill");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto result;
  result.mutable_status()->set_code(StatusProto::OK);
  *result.mutable_results()->Add()->mutable_document() = document;

  SearchResultProto actual_results =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(actual_results, EqualsSearchResultIgnoreStatsAndScores(result));

  SearchResultProto empty_result;
  empty_result.mutable_status()->set_code(StatusProto::OK);
  search_spec.set_query("subject:Bill");
  actual_results = icing.Search(search_spec, GetDefaultScoringSpec(),
                                ResultSpecProto::default_instance());
  EXPECT_THAT(actual_results,
              EqualsSearchResultIgnoreStatsAndScores(empty_result));

  // Now update the schema with index_nested_properties=false. This should
  // reassign property ids, lead to an index rebuild and ensure that nothing
  // match a query for "Bill".
  SchemaProto no_nested_schema =
      SchemaBuilder()
          .AddType(person_proto)
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("sender")
                                        .SetDataTypeDocument(
                                            "Person",
                                            /*index_nested_properties=*/false)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  set_schema_result = icing.SetSchema(no_nested_schema);
  // Ignore latency numbers. They're covered elsewhere.
  set_schema_result.clear_latency_ms();
  expected_set_schema_result = SetSchemaResultProto();
  expected_set_schema_result.mutable_status()->set_code(StatusProto::OK);
  expected_set_schema_result.mutable_index_incompatible_changed_schema_types()
      ->Add("Email");
  EXPECT_THAT(set_schema_result, EqualsProto(expected_set_schema_result));

  // document shouldn't match a query for 'Bill' in either 'sender.name' or
  // 'subject'
  search_spec.set_query("sender.name:Bill");
  actual_results = icing.Search(search_spec, GetDefaultScoringSpec(),
                                ResultSpecProto::default_instance());
  EXPECT_THAT(actual_results,
              EqualsSearchResultIgnoreStatsAndScores(empty_result));

  search_spec.set_query("subject:Bill");
  actual_results = icing.Search(search_spec, GetDefaultScoringSpec(),
                                ResultSpecProto::default_instance());
  EXPECT_THAT(actual_results,
              EqualsSearchResultIgnoreStatsAndScores(empty_result));
}

TEST_F(IcingSearchEngineSchemaTest,
       ForceSetSchemaPropertyDeletionTriggersIndexRestorationAndReturnsOk) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  // 'body' should have a property id of 0 and 'subject' should have a property
  // id of 1.
  SchemaProto email_with_body_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  SetSchemaResultProto set_schema_result =
      icing.SetSchema(email_with_body_schema);
  // Ignore latency numbers. They're covered elsewhere.
  set_schema_result.clear_latency_ms();
  SetSchemaResultProto expected_set_schema_result;
  expected_set_schema_result.mutable_new_schema_types()->Add("Email");
  expected_set_schema_result.mutable_status()->set_code(StatusProto::OK);
  EXPECT_THAT(set_schema_result, EqualsProto(expected_set_schema_result));

  // Create a document with only a subject property.
  DocumentProto document =
      DocumentBuilder()
          .SetKey("namespace1", "uri1")
          .SetSchema("Email")
          .SetCreationTimestampMs(1000)
          .AddStringProperty("subject",
                             "Did you get the memo about TPS reports?")
          .Build();
  EXPECT_THAT(icing.Put(document).status(), ProtoIsOk());

  // We should be able to retrieve the document by searching for 'tps' in
  // 'subject'.
  SearchSpecProto search_spec;
  search_spec.set_query("subject:tps");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto result;
  result.mutable_status()->set_code(StatusProto::OK);
  *result.mutable_results()->Add()->mutable_document() = document;

  SearchResultProto actual_results =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(actual_results, EqualsSearchResultIgnoreStatsAndScores(result));

  // Now update the schema to remove the 'body' field. This is backwards
  // incompatible, but document should be preserved because it doesn't contain a
  // 'body' field. If the index is correctly rebuilt, then 'subject' will now
  // have a property id of 0. If not, then the hits in the index will still have
  // have a property id of 1 and therefore it won't be found.
  SchemaProto email_no_body_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Email").AddProperty(
              PropertyConfigBuilder()
                  .SetName("subject")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  set_schema_result = icing.SetSchema(
      email_no_body_schema, /*ignore_errors_and_delete_documents=*/true);
  // Ignore latency numbers. They're covered elsewhere.
  set_schema_result.clear_latency_ms();
  expected_set_schema_result = SetSchemaResultProto();
  expected_set_schema_result.mutable_incompatible_schema_types()->Add("Email");
  expected_set_schema_result.mutable_index_incompatible_changed_schema_types()
      ->Add("Email");
  expected_set_schema_result.mutable_status()->set_code(StatusProto::OK);
  EXPECT_THAT(set_schema_result, EqualsProto(expected_set_schema_result));

  // We should be able to retrieve the document by searching for 'tps' in
  // 'subject'.
  search_spec.set_query("subject:tps");
  actual_results = icing.Search(search_spec, GetDefaultScoringSpec(),
                                ResultSpecProto::default_instance());
  EXPECT_THAT(actual_results, EqualsSearchResultIgnoreStatsAndScores(result));
}

TEST_F(
    IcingSearchEngineSchemaTest,
    ForceSetSchemaPropertyDeletionAndAdditionTriggersIndexRestorationAndReturnsOk) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  // 'body' should have a property id of 0 and 'subject' should have a property
  // id of 1.
  SchemaProto email_with_body_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  SetSchemaResultProto set_schema_result =
      icing.SetSchema(email_with_body_schema);
  // Ignore latency numbers. They're covered elsewhere.
  set_schema_result.clear_latency_ms();
  SetSchemaResultProto expected_set_schema_result;
  expected_set_schema_result.mutable_new_schema_types()->Add("Email");
  expected_set_schema_result.mutable_status()->set_code(StatusProto::OK);
  EXPECT_THAT(set_schema_result, EqualsProto(expected_set_schema_result));

  // Create a document with only a subject property.
  DocumentProto document =
      DocumentBuilder()
          .SetKey("namespace1", "uri1")
          .SetSchema("Email")
          .SetCreationTimestampMs(1000)
          .AddStringProperty("subject",
                             "Did you get the memo about TPS reports?")
          .Build();
  EXPECT_THAT(icing.Put(document).status(), ProtoIsOk());

  // We should be able to retrieve the document by searching for 'tps' in
  // 'subject'.
  SearchSpecProto search_spec;
  search_spec.set_query("subject:tps");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto result;
  result.mutable_status()->set_code(StatusProto::OK);
  *result.mutable_results()->Add()->mutable_document() = document;

  SearchResultProto actual_results =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(actual_results, EqualsSearchResultIgnoreStatsAndScores(result));

  // Now update the schema to remove the 'body' field. This is backwards
  // incompatible, but document should be preserved because it doesn't contain a
  // 'body' field. If the index is correctly rebuilt, then 'subject' and 'to'
  // will now have property ids of 0 and 1 respectively. If not, then the hits
  // in the index will still have have a property id of 1 and therefore it won't
  // be found.
  SchemaProto email_no_body_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("to")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  set_schema_result = icing.SetSchema(
      email_no_body_schema, /*ignore_errors_and_delete_documents=*/true);
  // Ignore latency numbers. They're covered elsewhere.
  set_schema_result.clear_latency_ms();
  expected_set_schema_result = SetSchemaResultProto();
  expected_set_schema_result.mutable_incompatible_schema_types()->Add("Email");
  expected_set_schema_result.mutable_index_incompatible_changed_schema_types()
      ->Add("Email");
  expected_set_schema_result.mutable_status()->set_code(StatusProto::OK);
  EXPECT_THAT(set_schema_result, EqualsProto(expected_set_schema_result));

  // We should be able to retrieve the document by searching for 'tps' in
  // 'subject'.
  search_spec.set_query("subject:tps");
  actual_results = icing.Search(search_spec, GetDefaultScoringSpec(),
                                ResultSpecProto::default_instance());
  EXPECT_THAT(actual_results, EqualsSearchResultIgnoreStatsAndScores(result));
}

TEST_F(IcingSearchEngineSchemaTest,
       ForceSetSchemaIncompatibleNestedDocsAreDeleted) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  SchemaTypeConfigProto email_schema_type =
      SchemaTypeConfigBuilder()
          .SetType("Email")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("sender")
                  .SetDataTypeDocument("Person",
                                       /*index_nested_properties=*/true)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("subject")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  SchemaProto nested_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Person")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("name")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("company")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(email_schema_type)
          .Build();

  SetSchemaResultProto set_schema_result = icing.SetSchema(nested_schema);
  // Ignore latency numbers. They're covered elsewhere.
  set_schema_result.clear_latency_ms();
  SetSchemaResultProto expected_set_schema_result;
  expected_set_schema_result.mutable_new_schema_types()->Add("Email");
  expected_set_schema_result.mutable_new_schema_types()->Add("Person");
  expected_set_schema_result.mutable_status()->set_code(StatusProto::OK);
  EXPECT_THAT(set_schema_result, EqualsProto(expected_set_schema_result));

  // Create two documents - a person document and an email document - both docs
  // should be deleted when we remove the 'company' field from the person type.
  DocumentProto person_document =
      DocumentBuilder()
          .SetKey("namespace1", "uri1")
          .SetSchema("Person")
          .SetCreationTimestampMs(1000)
          .AddStringProperty("name", "Bill Lundbergh")
          .AddStringProperty("company", "Initech Corp.")
          .Build();
  EXPECT_THAT(icing.Put(person_document).status(), ProtoIsOk());

  DocumentProto email_document =
      DocumentBuilder()
          .SetKey("namespace1", "uri2")
          .SetSchema("Email")
          .SetCreationTimestampMs(1000)
          .AddStringProperty("subject",
                             "Did you get the memo about TPS reports?")
          .AddDocumentProperty("sender", person_document)
          .Build();
  EXPECT_THAT(icing.Put(email_document).status(), ProtoIsOk());

  // We should be able to retrieve both documents.
  GetResultProto get_result =
      icing.Get("namespace1", "uri1", GetResultSpecProto::default_instance());
  EXPECT_THAT(get_result.status(), ProtoIsOk());
  EXPECT_THAT(get_result.document(), EqualsProto(person_document));

  get_result =
      icing.Get("namespace1", "uri2", GetResultSpecProto::default_instance());
  EXPECT_THAT(get_result.status(), ProtoIsOk());
  EXPECT_THAT(get_result.document(), EqualsProto(email_document));

  // Now update the schema to remove the 'company' field. This is backwards
  // incompatible, *both* documents should be deleted because both fail
  // validation (they each contain a 'Person' that has a non-existent property).
  nested_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(email_schema_type)
          .Build();

  set_schema_result = icing.SetSchema(
      nested_schema, /*ignore_errors_and_delete_documents=*/true);
  // Ignore latency numbers. They're covered elsewhere.
  set_schema_result.clear_latency_ms();
  expected_set_schema_result = SetSchemaResultProto();
  expected_set_schema_result.mutable_incompatible_schema_types()->Add("Person");
  expected_set_schema_result.mutable_incompatible_schema_types()->Add("Email");
  expected_set_schema_result.mutable_index_incompatible_changed_schema_types()
      ->Add("Email");
  expected_set_schema_result.mutable_index_incompatible_changed_schema_types()
      ->Add("Person");
  expected_set_schema_result.mutable_status()->set_code(StatusProto::OK);
  EXPECT_THAT(set_schema_result, EqualsProto(expected_set_schema_result));

  // Both documents should be deleted now.
  get_result =
      icing.Get("namespace1", "uri1", GetResultSpecProto::default_instance());
  EXPECT_THAT(get_result.status(), ProtoStatusIs(StatusProto::NOT_FOUND));

  get_result =
      icing.Get("namespace1", "uri2", GetResultSpecProto::default_instance());
  EXPECT_THAT(get_result.status(), ProtoStatusIs(StatusProto::NOT_FOUND));
}

// TODO(b/256022027): add unit tests for join incompatible schema change to make
//   sure the joinable cache is rebuilt correctly.

TEST_F(IcingSearchEngineSchemaTest, SetSchemaRevalidatesDocumentsAndReturnsOk) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  SchemaProto schema_with_optional_subject;
  auto type = schema_with_optional_subject.add_types();
  type->set_schema_type("email");

  // Add a OPTIONAL property
  auto property = type->add_properties();
  property->set_property_name("subject");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  EXPECT_THAT(icing.SetSchema(schema_with_optional_subject).status(),
              ProtoIsOk());

  DocumentProto email_document_without_subject =
      DocumentBuilder()
          .SetKey("namespace", "without_subject")
          .SetSchema("email")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto email_document_with_subject =
      DocumentBuilder()
          .SetKey("namespace", "with_subject")
          .SetSchema("email")
          .AddStringProperty("subject", "foo")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  EXPECT_THAT(icing.Put(email_document_without_subject).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(email_document_with_subject).status(), ProtoIsOk());

  SchemaProto schema_with_required_subject;
  type = schema_with_required_subject.add_types();
  type->set_schema_type("email");

  // Add a REQUIRED property
  property = type->add_properties();
  property->set_property_name("subject");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);

  // Can't set the schema since it's incompatible
  SetSchemaResultProto set_schema_result =
      icing.SetSchema(schema_with_required_subject);
  // Ignore latency numbers. They're covered elsewhere.
  set_schema_result.clear_latency_ms();
  SetSchemaResultProto expected_set_schema_result_proto;
  expected_set_schema_result_proto.mutable_status()->set_code(
      StatusProto::FAILED_PRECONDITION);
  expected_set_schema_result_proto.mutable_status()->set_message(
      "Schema is incompatible.");
  expected_set_schema_result_proto.add_incompatible_schema_types("email");

  EXPECT_THAT(set_schema_result, EqualsProto(expected_set_schema_result_proto));

  // Force set it
  set_schema_result =
      icing.SetSchema(schema_with_required_subject,
                      /*ignore_errors_and_delete_documents=*/true);
  // Ignore latency numbers. They're covered elsewhere.
  set_schema_result.clear_latency_ms();
  expected_set_schema_result_proto.mutable_status()->set_code(StatusProto::OK);
  expected_set_schema_result_proto.mutable_status()->clear_message();
  EXPECT_THAT(set_schema_result, EqualsProto(expected_set_schema_result_proto));

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = email_document_with_subject;

  EXPECT_THAT(icing.Get("namespace", "with_subject",
                        GetResultSpecProto::default_instance()),
              EqualsProto(expected_get_result_proto));

  // The document without a subject got deleted because it failed validation
  // against the new schema
  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace, without_subject) not found.");
  expected_get_result_proto.clear_document();

  EXPECT_THAT(icing.Get("namespace", "without_subject",
                        GetResultSpecProto::default_instance()),
              EqualsProto(expected_get_result_proto));
}

TEST_F(IcingSearchEngineSchemaTest, SetSchemaDeletesDocumentsAndReturnsOk) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  SchemaProto schema;
  auto type = schema.add_types();
  type->set_schema_type("email");
  type = schema.add_types();
  type->set_schema_type("message");

  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  DocumentProto email_document =
      DocumentBuilder()
          .SetKey("namespace", "email_uri")
          .SetSchema("email")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message_document =
      DocumentBuilder()
          .SetKey("namespace", "message_uri")
          .SetSchema("message")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  EXPECT_THAT(icing.Put(email_document).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(message_document).status(), ProtoIsOk());

  // Clear the schema and only add the "email" type, essentially deleting the
  // "message" type
  SchemaProto new_schema;
  type = new_schema.add_types();
  type->set_schema_type("email");

  // Can't set the schema since it's incompatible
  SetSchemaResultProto set_schema_result = icing.SetSchema(new_schema);
  // Ignore latency numbers. They're covered elsewhere.
  set_schema_result.clear_latency_ms();
  SetSchemaResultProto expected_result;
  expected_result.mutable_status()->set_code(StatusProto::FAILED_PRECONDITION);
  expected_result.mutable_status()->set_message("Schema is incompatible.");
  expected_result.add_deleted_schema_types("message");

  EXPECT_THAT(set_schema_result, EqualsProto(expected_result));

  // Force set it
  set_schema_result =
      icing.SetSchema(new_schema,
                      /*ignore_errors_and_delete_documents=*/true);
  // Ignore latency numbers. They're covered elsewhere.
  set_schema_result.clear_latency_ms();
  expected_result.mutable_status()->set_code(StatusProto::OK);
  expected_result.mutable_status()->clear_message();
  EXPECT_THAT(set_schema_result, EqualsProto(expected_result));

  // "email" document is still there
  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = email_document;

  EXPECT_THAT(icing.Get("namespace", "email_uri",
                        GetResultSpecProto::default_instance()),
              EqualsProto(expected_get_result_proto));

  // "message" document got deleted
  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace, message_uri) not found.");
  expected_get_result_proto.clear_document();

  EXPECT_THAT(icing.Get("namespace", "message_uri",
                        GetResultSpecProto::default_instance()),
              EqualsProto(expected_get_result_proto));
}

TEST_F(IcingSearchEngineSchemaTest, GetSchemaNotFound) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  EXPECT_THAT(icing.GetSchema().status(),
              ProtoStatusIs(StatusProto::NOT_FOUND));
}

TEST_F(IcingSearchEngineSchemaTest, GetSchemaOk) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  GetSchemaResultProto expected_get_schema_result_proto;
  expected_get_schema_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_schema_result_proto.mutable_schema() = CreateMessageSchema();
  EXPECT_THAT(icing.GetSchema(), EqualsProto(expected_get_schema_result_proto));
}

TEST_F(IcingSearchEngineSchemaTest, GetSchemaTypeFailedPrecondition) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  GetSchemaTypeResultProto get_schema_type_result_proto =
      icing.GetSchemaType("nonexistent_schema");
  EXPECT_THAT(get_schema_type_result_proto.status(),
              ProtoStatusIs(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(get_schema_type_result_proto.status().message(),
              HasSubstr("Schema not set"));
}

TEST_F(IcingSearchEngineSchemaTest, GetSchemaTypeOk) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  GetSchemaTypeResultProto expected_get_schema_type_result_proto;
  expected_get_schema_type_result_proto.mutable_status()->set_code(
      StatusProto::OK);
  *expected_get_schema_type_result_proto.mutable_schema_type_config() =
      CreateMessageSchema().types(0);
  EXPECT_THAT(icing.GetSchemaType(CreateMessageSchema().types(0).schema_type()),
              EqualsProto(expected_get_schema_type_result_proto));
}

TEST_F(IcingSearchEngineSchemaTest,
       SetSchemaCanNotDetectPreviousSchemaWasLostWithoutDocuments) {
  SchemaProto schema;
  auto type = schema.add_types();
  type->set_schema_type("Message");

  auto body = type->add_properties();
  body->set_property_name("body");
  body->set_data_type(PropertyConfigProto::DataType::STRING);
  body->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  // Make an incompatible schema, a previously OPTIONAL field is REQUIRED
  SchemaProto incompatible_schema = schema;
  incompatible_schema.mutable_types(0)->mutable_properties(0)->set_cardinality(
      PropertyConfigProto::Cardinality::REQUIRED);

  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  ASSERT_TRUE(filesystem()->DeleteDirectoryRecursively(GetSchemaDir().c_str()));

  // Since we don't have any documents yet, we can't detect this edge-case.  But
  // it should be fine since there aren't any documents to be invalidated.
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(incompatible_schema).status(), ProtoIsOk());
}

TEST_F(IcingSearchEngineSchemaTest, SetSchemaCanDetectPreviousSchemaWasLost) {
  SchemaProto schema;
  auto type = schema.add_types();
  type->set_schema_type("Message");

  auto body = type->add_properties();
  body->set_property_name("body");
  body->set_data_type(PropertyConfigProto::DataType::STRING);
  body->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
  body->mutable_string_indexing_config()->set_term_match_type(
      TermMatchType::PREFIX);
  body->mutable_string_indexing_config()->set_tokenizer_type(
      StringIndexingConfig::TokenizerType::PLAIN);

  // Make an incompatible schema, a previously OPTIONAL field is REQUIRED
  SchemaProto incompatible_schema = schema;
  incompatible_schema.mutable_types(0)->mutable_properties(0)->set_cardinality(
      PropertyConfigProto::Cardinality::REQUIRED);

  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    DocumentProto document = CreateMessageDocument("namespace", "uri");
    ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

    // Can retrieve by namespace/uri
    GetResultProto expected_get_result_proto;
    expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
    *expected_get_result_proto.mutable_document() = document;

    ASSERT_THAT(
        icing.Get("namespace", "uri", GetResultSpecProto::default_instance()),
        EqualsProto(expected_get_result_proto));

    // Can search for it
    SearchResultProto expected_search_result_proto;
    expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
    *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
        CreateMessageDocument("namespace", "uri");
    SearchResultProto search_result_proto =
        icing.Search(search_spec, GetDefaultScoringSpec(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                         expected_search_result_proto));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  ASSERT_TRUE(filesystem()->DeleteDirectoryRecursively(GetSchemaDir().c_str()));

  // Setting the new, different schema will remove incompatible documents
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(incompatible_schema).status(), ProtoIsOk());

  // Can't retrieve by namespace/uri
  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace, uri) not found.");

  EXPECT_THAT(
      icing.Get("namespace", "uri", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  // Can't search for it
  SearchResultProto empty_result;
  empty_result.mutable_status()->set_code(StatusProto::OK);
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto,
              EqualsSearchResultIgnoreStatsAndScores(empty_result));
}

TEST_F(IcingSearchEngineSchemaTest, IcingShouldWorkFor64Sections) {
  // Create a schema with 64 sections
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       // Person has 4 sections.
                       .SetType("Person")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("firstName")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("lastName")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("emailAddress")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("phoneNumber")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       // Email has 16 sections.
                       .SetType("Email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("date")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("time")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(
                           PropertyConfigBuilder()
                               .SetName("sender")
                               .SetDataTypeDocument(
                                   "Person", /*index_nested_properties=*/true)
                               .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(
                           PropertyConfigBuilder()
                               .SetName("receiver")
                               .SetDataTypeDocument(
                                   "Person", /*index_nested_properties=*/true)
                               .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(
                           PropertyConfigBuilder()
                               .SetName("cc")
                               .SetDataTypeDocument(
                                   "Person", /*index_nested_properties=*/true)
                               .SetCardinality(CARDINALITY_REPEATED)))
          .AddType(SchemaTypeConfigBuilder()
                       // EmailCollection has 64 sections.
                       .SetType("EmailCollection")
                       .AddProperty(
                           PropertyConfigBuilder()
                               .SetName("email1")
                               .SetDataTypeDocument(
                                   "Email", /*index_nested_properties=*/true)
                               .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(
                           PropertyConfigBuilder()
                               .SetName("email2")
                               .SetDataTypeDocument(
                                   "Email", /*index_nested_properties=*/true)
                               .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(
                           PropertyConfigBuilder()
                               .SetName("email3")
                               .SetDataTypeDocument(
                                   "Email", /*index_nested_properties=*/true)
                               .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(
                           PropertyConfigBuilder()
                               .SetName("email4")
                               .SetDataTypeDocument(
                                   "Email", /*index_nested_properties=*/true)
                               .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  DocumentProto person1 =
      DocumentBuilder()
          .SetKey("namespace", "person1")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first1")
          .AddStringProperty("lastName", "last1")
          .AddStringProperty("emailAddress", "email1@gmail.com")
          .AddStringProperty("phoneNumber", "000-000-001")
          .Build();
  DocumentProto person2 =
      DocumentBuilder()
          .SetKey("namespace", "person2")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first2")
          .AddStringProperty("lastName", "last2")
          .AddStringProperty("emailAddress", "email2@gmail.com")
          .AddStringProperty("phoneNumber", "000-000-002")
          .Build();
  DocumentProto person3 =
      DocumentBuilder()
          .SetKey("namespace", "person3")
          .SetSchema("Person")
          .AddStringProperty("firstName", "first3")
          .AddStringProperty("lastName", "last3")
          .AddStringProperty("emailAddress", "email3@gmail.com")
          .AddStringProperty("phoneNumber", "000-000-003")
          .Build();
  DocumentProto email1 = DocumentBuilder()
                             .SetKey("namespace", "email1")
                             .SetSchema("Email")
                             .AddStringProperty("body", "test body")
                             .AddStringProperty("subject", "test subject")
                             .AddStringProperty("date", "2022-08-01")
                             .AddStringProperty("time", "1:00 PM")
                             .AddDocumentProperty("sender", person1)
                             .AddDocumentProperty("receiver", person2)
                             .AddDocumentProperty("cc", person3)
                             .Build();
  DocumentProto email2 = DocumentBuilder()
                             .SetKey("namespace", "email2")
                             .SetSchema("Email")
                             .AddStringProperty("body", "test body")
                             .AddStringProperty("subject", "test subject")
                             .AddStringProperty("date", "2022-08-02")
                             .AddStringProperty("time", "2:00 PM")
                             .AddDocumentProperty("sender", person2)
                             .AddDocumentProperty("receiver", person1)
                             .AddDocumentProperty("cc", person3)
                             .Build();
  DocumentProto email3 = DocumentBuilder()
                             .SetKey("namespace", "email3")
                             .SetSchema("Email")
                             .AddStringProperty("body", "test body")
                             .AddStringProperty("subject", "test subject")
                             .AddStringProperty("date", "2022-08-03")
                             .AddStringProperty("time", "3:00 PM")
                             .AddDocumentProperty("sender", person3)
                             .AddDocumentProperty("receiver", person1)
                             .AddDocumentProperty("cc", person2)
                             .Build();
  DocumentProto email4 = DocumentBuilder()
                             .SetKey("namespace", "email4")
                             .SetSchema("Email")
                             .AddStringProperty("body", "test body")
                             .AddStringProperty("subject", "test subject")
                             .AddStringProperty("date", "2022-08-04")
                             .AddStringProperty("time", "4:00 PM")
                             .AddDocumentProperty("sender", person3)
                             .AddDocumentProperty("receiver", person2)
                             .AddDocumentProperty("cc", person1)
                             .Build();
  DocumentProto email_collection =
      DocumentBuilder()
          .SetKey("namespace", "email_collection")
          .SetSchema("EmailCollection")
          .AddDocumentProperty("email1", email1)
          .AddDocumentProperty("email2", email2)
          .AddDocumentProperty("email3", email3)
          .AddDocumentProperty("email4", email4)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email_collection).status(), ProtoIsOk());

  const std::vector<std::string> query_terms = {
      "first1", "last2",   "email3@gmail.com", "000-000-001",
      "body",   "subject", "2022-08-02",       "3\\:00"};
  SearchResultProto expected_document;
  expected_document.mutable_status()->set_code(StatusProto::OK);
  *expected_document.mutable_results()->Add()->mutable_document() =
      email_collection;
  for (const std::string& query_term : query_terms) {
    SearchSpecProto search_spec;
    search_spec.set_term_match_type(TermMatchType::PREFIX);
    search_spec.set_query(query_term);
    SearchResultProto actual_results =
        icing.Search(search_spec, GetDefaultScoringSpec(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(actual_results,
                EqualsSearchResultIgnoreStatsAndScores(expected_document));
  }

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("foo");
  SearchResultProto expected_no_documents;
  expected_no_documents.mutable_status()->set_code(StatusProto::OK);
  SearchResultProto actual_results =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(actual_results,
              EqualsSearchResultIgnoreStatsAndScores(expected_no_documents));
}

}  // namespace
}  // namespace lib
}  // namespace icing