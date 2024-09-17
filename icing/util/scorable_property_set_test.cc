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
#include "icing/file/filesystem.h"
#include "icing/proto/internal/scorable_property_set.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/store/document-filter-data.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::Pointee;

ScorablePropertyProto BuildScorablePropertyProtoFromBoolean(
    bool boolean_value) {
  ScorablePropertyProto scorable_property;
  scorable_property.add_boolean_values(boolean_value);
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
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_,
        SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_));

    SchemaProto schema_proto =
        SchemaBuilder()
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
            .AddType(SchemaTypeConfigBuilder().SetType("person").AddProperty(
                PropertyConfigBuilder()
                    .SetName("id")
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
  }

  void TearDown() override {
    schema_store_.reset();
    filesystem_.DeleteDirectoryRecursively(schema_store_dir_.c_str());
  }

  Filesystem filesystem_;
  std::string schema_store_dir_;
  FakeClock fake_clock_;
  SchemaTypeId email_schema_type_id_;
  SchemaTypeId person_schema_type_id_;
  std::unique_ptr<SchemaStore> schema_store_;
};

TEST_F(ScorablePropertySetTest,
       BuildFromScorablePropertySetProto_GetScorablePropertyProto) {
  ScorablePropertyProto scorable_property_proto_boolean =
      BuildScorablePropertyProtoFromBoolean(true);
  ScorablePropertyProto scorable_property_proto_double =
      BuildScorablePropertyProtoFromDouble({1.5, 2.5});
  ScorablePropertyProto scorable_property_proto_int64 =
      BuildScorablePropertyProtoFromInt64({1, 2, 3});

  ScorablePropertySetProto scorable_property_set_proto;
  *scorable_property_set_proto.add_properties() =
      scorable_property_proto_boolean;
  *scorable_property_set_proto.add_properties() =
      scorable_property_proto_double;
  *scorable_property_set_proto.add_properties() = scorable_property_proto_int64;

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScorablePropertySet> scorable_property_set,
      ScorablePropertySet::Create(std::move(scorable_property_set_proto),
                                  email_schema_type_id_, schema_store_.get()));
  ASSERT_THAT(
      scorable_property_set->GetScorablePropertyProto("scoreInt64"),
      IsOkAndHolds(Pointee(EqualsProto(scorable_property_proto_int64))));
  ASSERT_THAT(
      scorable_property_set->GetScorablePropertyProto("scoreDouble"),
      IsOkAndHolds(Pointee(EqualsProto(scorable_property_proto_double))));
  ASSERT_THAT(
      scorable_property_set->GetScorablePropertyProto("importanceBoolean"),
      IsOkAndHolds(Pointee(EqualsProto(scorable_property_proto_boolean))));

  ASSERT_THAT(
      scorable_property_set->GetScorablePropertyProto("non_exist_property"),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  ASSERT_THAT(scorable_property_set->GetScorablePropertyProto("subject"),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(ScorablePropertySetTest,
       BuildFromScorablePropertySetProto_InconsistentWithSchemaConfig) {
  ScorablePropertyProto scorable_property_proto_boolean =
      BuildScorablePropertyProtoFromBoolean(true);

  ScorablePropertySetProto scorable_property_set_proto;
  *scorable_property_set_proto.add_properties() =
      scorable_property_proto_boolean;
  ASSERT_THAT(
      ScorablePropertySet::Create(std::move(scorable_property_set_proto),
                                  email_schema_type_id_, schema_store_.get()),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(ScorablePropertySetTest,
       BuildFromScorablePropertySetProto_InvalidSchemaTypeId) {
  ScorablePropertyProto scorable_property_proto_boolean =
      BuildScorablePropertyProtoFromBoolean(true);

  ScorablePropertySetProto scorable_property_set_proto;
  *scorable_property_set_proto.add_properties() =
      scorable_property_proto_boolean;
  ASSERT_THAT(
      ScorablePropertySet::Create(std::move(scorable_property_set_proto),
                                  /*schema_type_id=*/1000, schema_store_.get()),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(ScorablePropertySetTest,
       BuildFromScorablePropertySetProto_WithSomePropertiesNotPopulated) {
  ScorablePropertyProto scorable_property_proto_boolean;
  ScorablePropertyProto scorable_property_proto_double;
  ScorablePropertyProto scorable_property_proto_int64 =
      BuildScorablePropertyProtoFromInt64({1, 2, 3});

  ScorablePropertySetProto scorable_property_set_proto;
  *scorable_property_set_proto.add_properties() =
      scorable_property_proto_boolean;
  *scorable_property_set_proto.add_properties() =
      scorable_property_proto_double;
  *scorable_property_set_proto.add_properties() = scorable_property_proto_int64;

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScorablePropertySet> scorable_property_set,
      ScorablePropertySet::Create(std::move(scorable_property_set_proto),
                                  email_schema_type_id_, schema_store_.get()));
  ASSERT_THAT(
      scorable_property_set->GetScorablePropertyProto("scoreInt64"),
      IsOkAndHolds(Pointee(EqualsProto(scorable_property_proto_int64))));
  ASSERT_THAT(
      scorable_property_set->GetScorablePropertyProto("scoreDouble"),
      IsOkAndHolds(Pointee(EqualsProto(scorable_property_proto_double))));
  ASSERT_THAT(
      scorable_property_set->GetScorablePropertyProto("importanceBoolean"),
      IsOkAndHolds(Pointee(EqualsProto(scorable_property_proto_boolean))));
}

TEST_F(ScorablePropertySetTest, BuildFromDocument_InvalidSchemaTypeId) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("foo", "1")
          .SetSchema("email")
          .AddStringProperty("subjectString", "subject foo")
          .SetCreationTimestampMs(0)
          .Build();

  ASSERT_THAT(
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
  expected_scorable_property_set.add_properties();
  expected_scorable_property_set.add_properties();
  expected_scorable_property_set.add_properties();

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
  ASSERT_THAT(ScorablePropertySet::Create(document, person_schema_type_id_,
                                          schema_store_.get()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(ScorablePropertySetTest,
       BuildFromDocument_AllScorablePropertiesPopulated) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("foo", "1")
          .SetSchema("email")
          .AddDoubleProperty("scoreDouble", 1.5, 2.5)
          .AddStringProperty("subjectString", "subject foo")
          .AddBooleanProperty("importanceBoolean", true)
          .AddInt64Property("scoreInt64", 1, 2, 3)
          .SetCreationTimestampMs(0)
          .Build();

  ScorablePropertySetProto expected_scorable_property_set;
  *expected_scorable_property_set.add_properties() =
      BuildScorablePropertyProtoFromBoolean(true);
  *expected_scorable_property_set.add_properties() =
      BuildScorablePropertyProtoFromDouble({1.5, 2.5});
  *expected_scorable_property_set.add_properties() =
      BuildScorablePropertyProtoFromInt64({1, 2, 3});

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScorablePropertySet> scorable_property_set,
      ScorablePropertySet::Create(document, email_schema_type_id_,
                                  schema_store_.get()));
  EXPECT_THAT(scorable_property_set->GetScorablePropertySetProto(),
              EqualsProto(expected_scorable_property_set));
}

TEST_F(ScorablePropertySetTest,
       BuildFromDocument_ScorablePropertiesPartiallyPopulated) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("foo", "1")
          .SetSchema("email")
          .AddInt64Property("scoreInt64", 1, 2, 3)
          .AddDoubleProperty("scoreDouble", 1.5, 2.5)
          .AddStringProperty("subjectString", "subject foo")
          .SetCreationTimestampMs(0)
          .Build();

  ScorablePropertySetProto expected_scorable_property_set;
  expected_scorable_property_set.add_properties();
  *expected_scorable_property_set.add_properties() =
      BuildScorablePropertyProtoFromDouble({1.5, 2.5});
  *expected_scorable_property_set.add_properties() =
      BuildScorablePropertyProtoFromInt64({1, 2, 3});

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<ScorablePropertySet> scorable_property_set,
      ScorablePropertySet::Create(document, email_schema_type_id_,
                                  schema_store_.get()));
  EXPECT_THAT(scorable_property_set->GetScorablePropertySetProto(),
              EqualsProto(expected_scorable_property_set));
}

}  // namespace

}  // namespace lib
}  // namespace icing
