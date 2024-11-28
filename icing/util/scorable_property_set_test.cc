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

#include "icing/util/scorable_property_set.h"

#include <cstdint>
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
#include "icing/proto/internal/scorable_property_set.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-filter-data.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::Pointee;

ScorablePropertyProto BuildScorablePropertyProtoFromBoolean(
    const std::vector<bool>& boolean_values) {
  ScorablePropertyProto scorable_property;
  scorable_property.mutable_boolean_values()->Add(boolean_values.begin(),
                                                  boolean_values.end());
  return scorable_property;
}

ScorablePropertyProto BuildScorablePropertyProtoFromInt64(
    const std::vector<int64_t>& int64_values) {
  ScorablePropertyProto scorable_property;
  scorable_property.mutable_int64_values()->Add(int64_values.begin(),
                                                int64_values.end());
  return scorable_property;
}

ScorablePropertyProto BuildScorablePropertyProtoFromDouble(
    const std::vector<double>& double_values) {
  ScorablePropertyProto scorable_property;
  scorable_property.mutable_double_values()->Add(double_values.begin(),
                                                 double_values.end());
  return scorable_property;
}

class ScorablePropertySetTest : public ::testing::Test {
 protected:
  ScorablePropertySetTest()
      : schema_store_dir_(GetTestTempDir() + "/schema_store") {}

  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, schema_store_dir_,
                                           &fake_clock_, feature_flags_.get()));

    SchemaProto schema_proto =
        SchemaBuilder()
            .AddType(SchemaTypeConfigBuilder().SetType("dummy").AddProperty(
                PropertyConfigBuilder()
                    .SetName("id")
                    .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                    .SetCardinality(CARDINALITY_REPEATED)))
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("person")
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("id")
                                     .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                     .SetCardinality(CARDINALITY_REPEATED))
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("income")
                            .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                            .SetScorableType(SCORABLE_TYPE_ENABLED)
                            .SetCardinality(CARDINALITY_REPEATED))
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("age")
                            .SetDataType(PropertyConfigProto::DataType::INT64)
                            .SetScorableType(SCORABLE_TYPE_ENABLED)
                            .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("isStarred")
                            .SetDataType(PropertyConfigProto::DataType::BOOLEAN)
                            .SetScorableType(SCORABLE_TYPE_ENABLED)
                            .SetCardinality(CARDINALITY_OPTIONAL)))
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("email")
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("subject")
                                     .SetDataTypeString(TERM_MATCH_EXACT,
                                                        TOKENIZER_PLAIN)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("sender")
                            .SetDataTypeDocument(
                                "person", /*index_nested_properties=*/true)
                            .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("receiver")
                            .SetDataTypeDocument(
                                "person", /*index_nested_properties=*/true)
                            .SetCardinality(CARDINALITY_REPEATED))
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("importanceBoolean")
                            .SetDataType(PropertyConfigProto::DataType::BOOLEAN)
                            .SetScorableType(SCORABLE_TYPE_ENABLED)
                            .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(
                        PropertyConfigBuilder()
                            .SetName("scoreDouble")
                            .SetDataType(PropertyConfigProto::DataType::DOUBLE)
                            .SetScorableType(SCORABLE_TYPE_ENABLED)
                            .SetCardinality(CARDINALITY_REPEATED))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("scoreInt64")
                                     .SetScorableType(SCORABLE_TYPE_ENABLED)
                                     .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                     .SetCardinality(CARDINALITY_REPEATED)))
            .Build();
    ICING_ASSERT_OK(schema_store_->SetSchema(
        schema_proto, /*ignore_errors_and_delete_documents=*/false,
        /*allow_circular_schema_definitions=*/false));
    email_schema_type_id_ =
        schema_store_->GetSchemaTypeId("email").ValueOrDie();
    person_schema_type_id_ =
        schema_store_->GetSchemaTypeId("person").ValueOrDie();
    dummy_schema_type_id_ =
        schema_store_->GetSchemaTypeId("dummy").ValueOrDie();
  }

  void TearDown() override {
    schema_store_.reset();
    filesystem_.DeleteDirectoryRecursively(schema_store_dir_.c_str());
  }

  std::unique_ptr<FeatureFlags> feature_flags_;
  Filesystem filesystem_;
  std::string schema_store_dir_;
  FakeClock fake_clock_;
  SchemaTypeId email_schema_type_id_;
  SchemaTypeId person_schema_type_id_;
  SchemaTypeId dummy_schema_type_id_;
  std::unique_ptr<SchemaStore> schema_store_;
};

TEST_F(ScorablePropertySetTest,
       BuildFromScorablePropertySetProto_GetScorablePropertyProto) {
  ScorablePropertyProto is_starred_scorable_property =
      BuildScorablePropertyProtoFromBoolean({true});
  ScorablePropertyProto income_scorable_property =
      BuildScorablePropertyProtoFromDouble({1.5, 2.5});
  ScorablePropertyProto age_scorable_property =
      BuildScorablePropertyProtoFromInt64({45});

  ScorablePropertySetProto scorable_property_set_proto;
  *scorable_property_set_proto.add_properties() = age_scorable_property;
  *scorable_property_set_proto.add_properties() = income_scorable_property;
  *scorable_property_set_proto.add_properties() = is_starred_scorable_property;

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScorablePropertySet> scorable_property_set,
      ScorablePropertySet::Create(std::move(scorable_property_set_proto),
                                  person_schema_type_id_, schema_store_.get()));
  EXPECT_THAT(scorable_property_set->GetScorablePropertyProto("age"),
              Pointee(EqualsProto(age_scorable_property)));
  EXPECT_THAT(scorable_property_set->GetScorablePropertyProto("income"),
              Pointee(EqualsProto(income_scorable_property)));
  EXPECT_THAT(scorable_property_set->GetScorablePropertyProto("isStarred"),
              Pointee(EqualsProto(is_starred_scorable_property)));

  EXPECT_EQ(
      scorable_property_set->GetScorablePropertyProto("non_exist_property"),
      nullptr);

  EXPECT_EQ(scorable_property_set->GetScorablePropertyProto("subject"),
            nullptr);
}

TEST_F(ScorablePropertySetTest,
       BuildFromScorablePropertySetProto_InconsistentWithSchemaConfig) {
  ScorablePropertyProto scorable_property_proto_boolean =
      BuildScorablePropertyProtoFromBoolean({true});

  ScorablePropertySetProto scorable_property_set_proto;
  *scorable_property_set_proto.add_properties() =
      scorable_property_proto_boolean;
  EXPECT_THAT(
      ScorablePropertySet::Create(std::move(scorable_property_set_proto),
                                  email_schema_type_id_, schema_store_.get()),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(ScorablePropertySetTest,
       BuildFromScorablePropertySetProto_InvalidSchemaTypeId) {
  ScorablePropertyProto scorable_property_proto_boolean =
      BuildScorablePropertyProtoFromBoolean({true});

  ScorablePropertySetProto scorable_property_set_proto;
  *scorable_property_set_proto.add_properties() =
      scorable_property_proto_boolean;
  EXPECT_THAT(
      ScorablePropertySet::Create(std::move(scorable_property_set_proto),
                                  /*schema_type_id=*/1000, schema_store_.get()),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(ScorablePropertySetTest,
       BuildFromScorablePropertySetProto_WithSomePropertiesNotPopulated) {
  ScorablePropertyProto age_scorable_property;
  ScorablePropertyProto is_starred_scorable_property;
  ScorablePropertyProto income_scorable_property =
      BuildScorablePropertyProtoFromInt64({1, 2, 3});

  ScorablePropertySetProto scorable_property_set_proto;
  *scorable_property_set_proto.add_properties() = age_scorable_property;
  *scorable_property_set_proto.add_properties() = income_scorable_property;
  *scorable_property_set_proto.add_properties() = is_starred_scorable_property;

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScorablePropertySet> scorable_property_set,
      ScorablePropertySet::Create(std::move(scorable_property_set_proto),
                                  person_schema_type_id_, schema_store_.get()));
  EXPECT_THAT(scorable_property_set->GetScorablePropertyProto("age"),
              Pointee(EqualsProto(age_scorable_property)));
  EXPECT_THAT(scorable_property_set->GetScorablePropertyProto("isStarred"),
              Pointee(EqualsProto(is_starred_scorable_property)));
  EXPECT_THAT(scorable_property_set->GetScorablePropertyProto("income"),
              Pointee(EqualsProto(income_scorable_property)));
}

TEST_F(ScorablePropertySetTest, BuildFromDocument_InvalidSchemaTypeId) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("foo", "1")
          .SetSchema("email")
          .AddStringProperty("subjectString", "subject foo")
          .SetCreationTimestampMs(0)
          .Build();

  EXPECT_THAT(
      ScorablePropertySet::Create(document,
                                  /*schema_type_id=*/1000, schema_store_.get()),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(ScorablePropertySetTest,
       BuildFromDocument_NoScorablePropertiesFromDocument) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("foo", "1")
          .SetSchema("email")
          .AddStringProperty("subjectString", "subject foo")
          .SetCreationTimestampMs(0)
          .Build();

  ScorablePropertySetProto expected_scorable_property_set;
  expected_scorable_property_set.add_properties();  // importanceBoolean
  expected_scorable_property_set.add_properties();  // receiver.age
  expected_scorable_property_set.add_properties();  // receiver.income
  expected_scorable_property_set.add_properties();  // receiver.isStarred
  expected_scorable_property_set.add_properties();  // scoreDouble
  expected_scorable_property_set.add_properties();  // scoreInt64
  expected_scorable_property_set.add_properties();  // sender.age
  expected_scorable_property_set.add_properties();  // sender.income
  expected_scorable_property_set.add_properties();  // sender.isStarred

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScorablePropertySet> scorable_property_set,
      ScorablePropertySet::Create(document, email_schema_type_id_,
                                  schema_store_.get()));
  EXPECT_THAT(scorable_property_set->GetScorablePropertySetProto(),
              EqualsProto(expected_scorable_property_set));
}

TEST_F(ScorablePropertySetTest,
       BuildFromDocument_NoScorablePropertiesFromSchema) {
  DocumentProto document;
  EXPECT_THAT(ScorablePropertySet::Create(document, dummy_schema_type_id_,
                                          schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(ScorablePropertySetTest, ScorablePropertySetFromNestedDocument) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("foo", "1")
          .SetSchema("email")
          .AddStringProperty("subjectString", "subject foo")
          .AddBooleanProperty("importanceBoolean", true)
          .AddInt64Property("scoreInt64", 1, 2, 3)
          .AddDocumentProperty(
              "receiver",
              DocumentBuilder()
                  .SetKey("namespace", "uri1")
                  .SetSchema("person")
                  .AddInt64Property("age", 30)
                  .AddDoubleProperty("income", 10000, 20000, 30000)
                  .AddBooleanProperty("isStarred", true)
                  .Build(),
              DocumentBuilder()
                  .SetKey("namespace", "uri2")
                  .SetSchema("person")
                  .AddInt64Property("age", 35)
                  .AddDoubleProperty("income", 10001, 20001, 30001)
                  .AddBooleanProperty("isStarred", false)
                  .Build())
          .AddDocumentProperty(
              "sender", DocumentBuilder()
                            .SetKey("namespace", "uri3")
                            .SetSchema("person")
                            .AddInt64Property("age", 50)
                            .AddDoubleProperty("income", 21001, 21002, 21003)
                            .AddBooleanProperty("isStarred", false)
                            .Build())
          .SetCreationTimestampMs(0)
          .Build();

  ScorablePropertySetProto expected_scorable_property_set;
  // importanceBoolean
  *expected_scorable_property_set.add_properties() =
      BuildScorablePropertyProtoFromBoolean({true});
  // receiver.age
  *expected_scorable_property_set.add_properties() =
      BuildScorablePropertyProtoFromInt64({30, 35});
  // receiver.income
  *expected_scorable_property_set.add_properties() =
      BuildScorablePropertyProtoFromDouble(
          {10000, 20000, 30000, 10001, 20001, 30001});
  // receiver.isStarred
  *expected_scorable_property_set.add_properties() =
      BuildScorablePropertyProtoFromBoolean({true, false});
  // scoreDouble
  *expected_scorable_property_set.add_properties() =
      BuildScorablePropertyProtoFromDouble({});
  // scoreInt64
  *expected_scorable_property_set.add_properties() =
      BuildScorablePropertyProtoFromInt64({1, 2, 3});
  // sender.age
  *expected_scorable_property_set.add_properties() =
      BuildScorablePropertyProtoFromInt64({50});
  // sender.income
  *expected_scorable_property_set.add_properties() =
      BuildScorablePropertyProtoFromDouble({21001, 21002, 21003});
  // sender.isStarred
  *expected_scorable_property_set.add_properties() =
      BuildScorablePropertyProtoFromBoolean({false});

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScorablePropertySet> scorable_property_set,
      ScorablePropertySet::Create(document, email_schema_type_id_,
                                  schema_store_.get()));
  EXPECT_THAT(scorable_property_set->GetScorablePropertySetProto(),
              EqualsProto(expected_scorable_property_set));

  // Test GetScorablePropertyProto() for each property path.
  EXPECT_THAT(
      scorable_property_set->GetScorablePropertyProto("importanceBoolean"),
      Pointee(EqualsProto(BuildScorablePropertyProtoFromBoolean({true}))));
  EXPECT_THAT(scorable_property_set->GetScorablePropertyProto("scoreDouble"),
              Pointee(EqualsProto(BuildScorablePropertyProtoFromDouble({}))));
  EXPECT_THAT(
      scorable_property_set->GetScorablePropertyProto("scoreInt64"),
      Pointee(EqualsProto(BuildScorablePropertyProtoFromInt64({1, 2, 3}))));
  EXPECT_THAT(scorable_property_set->GetScorablePropertyProto("sender.age"),
              Pointee(EqualsProto(BuildScorablePropertyProtoFromInt64({50}))));
  EXPECT_THAT(scorable_property_set->GetScorablePropertyProto("sender.income"),
              Pointee(EqualsProto(BuildScorablePropertyProtoFromDouble(
                  {21001, 21002, 21003}))));
  EXPECT_THAT(
      scorable_property_set->GetScorablePropertyProto("sender.isStarred"),
      Pointee(EqualsProto(BuildScorablePropertyProtoFromBoolean({false}))));
  EXPECT_THAT(
      scorable_property_set->GetScorablePropertyProto("receiver.age"),
      Pointee(EqualsProto(BuildScorablePropertyProtoFromInt64({30, 35}))));
  EXPECT_THAT(
      scorable_property_set->GetScorablePropertyProto("receiver.income"),
      Pointee(EqualsProto(BuildScorablePropertyProtoFromDouble(
          {10000, 20000, 30000, 10001, 20001, 30001}))));
  EXPECT_THAT(
      scorable_property_set->GetScorablePropertyProto("receiver.isStarred"),
      Pointee(
          EqualsProto(BuildScorablePropertyProtoFromBoolean({true, false}))));
}

}  // namespace

}  // namespace lib
}  // namespace icing
