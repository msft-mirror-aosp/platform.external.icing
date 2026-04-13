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

#include "icing/schema/scorable_property_manager.h"

#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-util.h"
#include "icing/store/document-filter-data.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::Pointee;

constexpr int kEmailSchemaTypeId = 1;
constexpr int kMessageSchemaTypeId = 2;
constexpr int kPersonSchemaTypeId = 3;

SchemaTypeConfigProto CreateEmailTypeConfig() {
  return SchemaTypeConfigBuilder()
      .SetType("email")
      .AddProperty(PropertyConfigBuilder()
                       .SetName("subject")
                       .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                       .SetCardinality(CARDINALITY_OPTIONAL))
      .AddProperty(PropertyConfigBuilder()
                       .SetName("scorableInt64")
                       .SetDataType(TYPE_INT64)
                       .SetScorableType(SCORABLE_TYPE_ENABLED)
                       .SetCardinality(CARDINALITY_REPEATED))
      .AddProperty(PropertyConfigBuilder()
                       .SetName("scorableDouble")
                       .SetDataType(TYPE_DOUBLE)
                       .SetScorableType(SCORABLE_TYPE_ENABLED)
                       .SetCardinality(CARDINALITY_REPEATED))
      .Build();
}

SchemaTypeConfigProto CreateMessageTypeConfig() {
  return SchemaTypeConfigBuilder()
      .SetType("message")
      .AddProperty(PropertyConfigBuilder()
                       .SetName("message")
                       .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                       .SetCardinality(CARDINALITY_OPTIONAL))
      .AddProperty(PropertyConfigBuilder()
                       .SetName("scorableBoolean")
                       .SetDataType(TYPE_BOOLEAN)
                       .SetScorableType(SCORABLE_TYPE_ENABLED)
                       .SetCardinality(CARDINALITY_REPEATED))
      .AddProperty(PropertyConfigBuilder()
                       .SetName("scorableDouble")
                       .SetDataType(TYPE_DOUBLE)
                       .SetScorableType(SCORABLE_TYPE_ENABLED)
                       .SetCardinality(CARDINALITY_REPEATED))
      .Build();
}

SchemaTypeConfigProto CreatePersonTypeConfig() {
  return SchemaTypeConfigBuilder()
      .SetType("person")
      .AddProperty(PropertyConfigBuilder()
                       .SetName("name")
                       .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                       .SetCardinality(CARDINALITY_OPTIONAL))
      .Build();
}

class ScorablePropertyManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    type_config_map_.emplace("email", CreateEmailTypeConfig());
    type_config_map_.emplace("message", CreateMessageTypeConfig());
    type_config_map_.emplace("person", CreatePersonTypeConfig());
    schema_id_to_type_map_.emplace(kEmailSchemaTypeId, "email");
    schema_id_to_type_map_.emplace(kMessageSchemaTypeId, "message");
    schema_id_to_type_map_.emplace(kPersonSchemaTypeId, "person");
  }

  SchemaUtil::TypeConfigMap type_config_map_;
  std::unordered_map<SchemaTypeId, std::string> schema_id_to_type_map_;
};

TEST_F(ScorablePropertyManagerTest,
       GetScorablePropertyIndex_InvalidSchemaTypeId) {
  ScorablePropertyManager scorable_property_manager;

  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  /*schema_type_id=*/100, "subject", type_config_map_,
                  schema_id_to_type_map_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(ScorablePropertyManagerTest,
       GetScorablePropertyIndex_InvalidPropertyName) {
  ScorablePropertyManager scorable_property_manager;

  // non-existing property
  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  kEmailSchemaTypeId, "non_existing", type_config_map_,
                  schema_id_to_type_map_),
              IsOkAndHolds(Eq(std::nullopt)));

  // non-scorable property
  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  kEmailSchemaTypeId, "subject", type_config_map_,
                  schema_id_to_type_map_),
              IsOkAndHolds(Eq(std::nullopt)));
}

TEST_F(ScorablePropertyManagerTest, GetScorablePropertyIndex_Ok) {
  ScorablePropertyManager scorable_property_manager;

  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  kEmailSchemaTypeId, "scorableDouble", type_config_map_,
                  schema_id_to_type_map_),
              IsOkAndHolds(0));
  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  kEmailSchemaTypeId, "scorableInt64", type_config_map_,
                  schema_id_to_type_map_),
              IsOkAndHolds(1));
  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  kMessageSchemaTypeId, "scorableBoolean", type_config_map_,
                  schema_id_to_type_map_),
              IsOkAndHolds(0));
  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  kMessageSchemaTypeId, "scorableDouble", type_config_map_,
                  schema_id_to_type_map_),
              IsOkAndHolds(1));

  // Repeat those calls to test the cache hits scenarios.
  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  kEmailSchemaTypeId, "scorableDouble", type_config_map_,
                  schema_id_to_type_map_),
              IsOkAndHolds(0));
  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  kEmailSchemaTypeId, "scorableInt64", type_config_map_,
                  schema_id_to_type_map_),
              IsOkAndHolds(1));
  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  kMessageSchemaTypeId, "scorableBoolean", type_config_map_,
                  schema_id_to_type_map_),
              IsOkAndHolds(0));
  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  kMessageSchemaTypeId, "scorableDouble", type_config_map_,
                  schema_id_to_type_map_),
              IsOkAndHolds(1));
}

TEST_F(ScorablePropertyManagerTest,
       GetOrderedScorablePropertyInfo_InvalidSchemaTypeId) {
  ScorablePropertyManager scorable_property_manager;

  EXPECT_THAT(
      scorable_property_manager.GetOrderedScorablePropertyInfo(
          /*schema_type_id=*/100, type_config_map_, schema_id_to_type_map_),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(ScorablePropertyManagerTest,
       GetOrderedScorablePropertyInfo_NoScorablePropertyInSchema) {
  ScorablePropertyManager scorable_property_manager;

  EXPECT_THAT(
      scorable_property_manager.GetOrderedScorablePropertyInfo(
          kPersonSchemaTypeId, type_config_map_, schema_id_to_type_map_),
      IsOkAndHolds(Pointee(ElementsAre())));

  // Repeat the call to test the cache hits scenarios.
  EXPECT_THAT(
      scorable_property_manager.GetOrderedScorablePropertyInfo(
          kPersonSchemaTypeId, type_config_map_, schema_id_to_type_map_),
      IsOkAndHolds(Pointee(ElementsAre())));
}

TEST_F(ScorablePropertyManagerTest, GetOrderedScorablePropertyInfo_Ok) {
  ScorablePropertyManager scorable_property_manager;

  EXPECT_THAT(scorable_property_manager.GetOrderedScorablePropertyInfo(
                  kEmailSchemaTypeId, type_config_map_, schema_id_to_type_map_),
              IsOkAndHolds(Pointee(ElementsAre(
                  EqualsScorablePropertyInfo("scorableDouble", TYPE_DOUBLE),
                  EqualsScorablePropertyInfo("scorableInt64", TYPE_INT64)))));
  EXPECT_THAT(
      scorable_property_manager.GetOrderedScorablePropertyInfo(
          kMessageSchemaTypeId, type_config_map_, schema_id_to_type_map_),
      IsOkAndHolds(Pointee(ElementsAre(
          EqualsScorablePropertyInfo("scorableBoolean", TYPE_BOOLEAN),
          EqualsScorablePropertyInfo("scorableDouble", TYPE_DOUBLE)))));

  // Repeat those calls to test the cache hits scenarios.
  EXPECT_THAT(scorable_property_manager.GetOrderedScorablePropertyInfo(
                  kEmailSchemaTypeId, type_config_map_, schema_id_to_type_map_),
              IsOkAndHolds(Pointee(ElementsAre(
                  EqualsScorablePropertyInfo("scorableDouble", TYPE_DOUBLE),
                  EqualsScorablePropertyInfo("scorableInt64", TYPE_INT64)))));
  EXPECT_THAT(
      scorable_property_manager.GetOrderedScorablePropertyInfo(
          kMessageSchemaTypeId, type_config_map_, schema_id_to_type_map_),
      IsOkAndHolds(Pointee(ElementsAre(
          EqualsScorablePropertyInfo("scorableBoolean", TYPE_BOOLEAN),
          EqualsScorablePropertyInfo("scorableDouble", TYPE_DOUBLE)))));
}

TEST_F(ScorablePropertyManagerTest,
       GetOrderedScorablePropertyInfo_WithNestedSchemas) {
  std::string person_schema_name = "Person";
  std::string gmail_schema_name = "Gmail";
  std::string interaction_log_schema_name = "InteractionLog";

  SchemaTypeConfigProto person_schema_config =
      SchemaTypeConfigBuilder()
          .SetType(person_schema_name)
          .AddProperty(PropertyConfigBuilder()
                           .SetName("name")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("networth")
                           .SetDataType(TYPE_DOUBLE)
                           .SetScorableType(SCORABLE_TYPE_ENABLED)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  SchemaTypeConfigProto gmail_schema_config =
      SchemaTypeConfigBuilder()
          .SetType(gmail_schema_name)
          .AddProperty(
              PropertyConfigBuilder().SetName("subject").SetDataTypeString(
                  TERM_MATCH_UNKNOWN, TOKENIZER_NONE))
          .AddProperty(
              PropertyConfigBuilder().SetName("sender").SetDataTypeDocument(
                  person_schema_name,
                  /*index_nested_properties=*/true))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("recipient")
                  .SetDataTypeDocument(person_schema_name,
                                       /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto interaction_log_schema_config =
      SchemaTypeConfigBuilder()
          .SetType(interaction_log_schema_name)
          .AddProperty(
              PropertyConfigBuilder().SetName("summary").SetDataTypeString(
                  TERM_MATCH_EXACT, TOKENIZER_PLAIN))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("contactGmail")
                  .SetDataTypeDocument(gmail_schema_name,
                                       /*index_nested_properties=*/true))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("mostContactedPerson")
                  .SetDataTypeDocument(person_schema_name,
                                       /*index_nested_properties=*/true))
          .Build();
  SchemaUtil::TypeConfigMap type_config_map = {
      {person_schema_name, person_schema_config},
      {gmail_schema_name, gmail_schema_config},
      {interaction_log_schema_name, interaction_log_schema_config}};
  std::unordered_map<SchemaTypeId, std::string> schema_id_to_type_map = {
      {1, person_schema_name},
      {2, gmail_schema_name},
      {3, interaction_log_schema_name}};
  ScorablePropertyManager scorable_property_manager;

  EXPECT_THAT(scorable_property_manager.GetOrderedScorablePropertyInfo(
                  /*schema_type_id=*/1, type_config_map, schema_id_to_type_map),
              IsOkAndHolds(Pointee(ElementsAre(
                  EqualsScorablePropertyInfo("networth", TYPE_DOUBLE)))));
  EXPECT_THAT(
      scorable_property_manager.GetOrderedScorablePropertyInfo(
          /*schema_type_id=*/2, type_config_map, schema_id_to_type_map),
      IsOkAndHolds(Pointee(ElementsAre(
          EqualsScorablePropertyInfo("recipient.networth", TYPE_DOUBLE),
          EqualsScorablePropertyInfo("sender.networth", TYPE_DOUBLE)))));
  EXPECT_THAT(scorable_property_manager.GetOrderedScorablePropertyInfo(
                  /*schema_type_id=*/3, type_config_map, schema_id_to_type_map),
              IsOkAndHolds(Pointee(ElementsAre(
                  EqualsScorablePropertyInfo("contactGmail.recipient.networth",
                                             TYPE_DOUBLE),
                  EqualsScorablePropertyInfo("contactGmail.sender.networth",
                                             TYPE_DOUBLE),
                  EqualsScorablePropertyInfo("mostContactedPerson.networth",
                                             TYPE_DOUBLE)))));
}

TEST_F(ScorablePropertyManagerTest,
       GetScorablePropertyIndex_WithNestedSchemas) {
  std::string person_schema_name = "Person";
  std::string gmail_schema_name = "Gmail";
  std::string interaction_log_schema_name = "InteractionLog";

  SchemaTypeConfigProto person_schema_config =
      SchemaTypeConfigBuilder()
          .SetType(person_schema_name)
          .AddProperty(PropertyConfigBuilder()
                           .SetName("name")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("networth")
                           .SetDataType(TYPE_DOUBLE)
                           .SetScorableType(SCORABLE_TYPE_ENABLED)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  SchemaTypeConfigProto gmail_schema_config =
      SchemaTypeConfigBuilder()
          .SetType(gmail_schema_name)
          .AddProperty(
              PropertyConfigBuilder().SetName("subject").SetDataTypeString(
                  TERM_MATCH_UNKNOWN, TOKENIZER_NONE))
          .AddProperty(
              PropertyConfigBuilder().SetName("sender").SetDataTypeDocument(
                  person_schema_name,
                  /*index_nested_properties=*/true))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("recipient")
                  .SetDataTypeDocument(person_schema_name,
                                       /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto interaction_log_schema_config =
      SchemaTypeConfigBuilder()
          .SetType(interaction_log_schema_name)
          .AddProperty(
              PropertyConfigBuilder().SetName("summary").SetDataTypeString(
                  TERM_MATCH_EXACT, TOKENIZER_PLAIN))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("contactGmail")
                  .SetDataTypeDocument(gmail_schema_name,
                                       /*index_nested_properties=*/true))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("mostContactedPerson")
                  .SetDataTypeDocument(person_schema_name,
                                       /*index_nested_properties=*/true))
          .Build();
  SchemaUtil::TypeConfigMap type_config_map = {
      {person_schema_name, person_schema_config},
      {gmail_schema_name, gmail_schema_config},
      {interaction_log_schema_name, interaction_log_schema_config}};
  std::unordered_map<SchemaTypeId, std::string> schema_id_to_type_map = {
      {1, person_schema_name},
      {2, gmail_schema_name},
      {3, interaction_log_schema_name}};
  ScorablePropertyManager scorable_property_manager;

  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  /*schema_type_id=*/1, "networth", type_config_map,
                  schema_id_to_type_map),
              IsOkAndHolds(0));
  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  /*schema_type_id=*/2, "recipient.networth", type_config_map,
                  schema_id_to_type_map),
              IsOkAndHolds(0));
  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  /*schema_type_id=*/2, "sender.networth", type_config_map,
                  schema_id_to_type_map),
              IsOkAndHolds(1));
  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  /*schema_type_id=*/3, "contactGmail.recipient.networth",
                  type_config_map, schema_id_to_type_map),
              IsOkAndHolds(0));
  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  /*schema_type_id=*/3, "contactGmail.sender.networth",
                  type_config_map, schema_id_to_type_map),
              IsOkAndHolds(1));
  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  /*schema_type_id=*/3, "mostContactedPerson.networth",
                  type_config_map, schema_id_to_type_map),
              IsOkAndHolds(2));
}

}  // namespace

}  // namespace lib
}  // namespace icing
