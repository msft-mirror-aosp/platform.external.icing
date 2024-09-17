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
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  // non-scorable property
  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  kEmailSchemaTypeId, "subject", type_config_map_,
                  schema_id_to_type_map_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(ScorablePropertyManagerTest, GetScorablePropertyIndex_Ok) {
  ScorablePropertyManager scorable_property_manager;

  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  kEmailSchemaTypeId, "scorableInt64", type_config_map_,
                  schema_id_to_type_map_),
              IsOkAndHolds(0));
  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  kEmailSchemaTypeId, "scorableDouble", type_config_map_,
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
                  kEmailSchemaTypeId, "scorableInt64", type_config_map_,
                  schema_id_to_type_map_),
              IsOkAndHolds(0));
  EXPECT_THAT(scorable_property_manager.GetScorablePropertyIndex(
                  kEmailSchemaTypeId, "scorableDouble", type_config_map_,
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
       GetOrderedScorablePropertyNames_InvalidSchemaTypeId) {
  ScorablePropertyManager scorable_property_manager;

  EXPECT_THAT(
      scorable_property_manager.GetOrderedScorablePropertyNames(
          /*schema_type_id=*/100, type_config_map_, schema_id_to_type_map_),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(ScorablePropertyManagerTest,
       GetOrderedScorablePropertyNames_NoScorablePropertyInSchema) {
  ScorablePropertyManager scorable_property_manager;

  EXPECT_THAT(
      scorable_property_manager.GetOrderedScorablePropertyNames(
          kPersonSchemaTypeId, type_config_map_, schema_id_to_type_map_),
      IsOkAndHolds(Pointee(ElementsAre())));

  // Repeat the call to test the cache hits scenarios.
  EXPECT_THAT(
      scorable_property_manager.GetOrderedScorablePropertyNames(
          kPersonSchemaTypeId, type_config_map_, schema_id_to_type_map_),
      IsOkAndHolds(Pointee(ElementsAre())));
}

TEST_F(ScorablePropertyManagerTest, GetOrderedScorablePropertyNames_Ok) {
  ScorablePropertyManager scorable_property_manager;

  EXPECT_THAT(
      scorable_property_manager.GetOrderedScorablePropertyNames(
          kEmailSchemaTypeId, type_config_map_, schema_id_to_type_map_),
      IsOkAndHolds(Pointee(ElementsAre("scorableInt64", "scorableDouble"))));
  EXPECT_THAT(
      scorable_property_manager.GetOrderedScorablePropertyNames(
          kMessageSchemaTypeId, type_config_map_, schema_id_to_type_map_),
      IsOkAndHolds(Pointee(ElementsAre("scorableBoolean", "scorableDouble"))));

  // Repeat those calls to test the cache hits scenarios.
  EXPECT_THAT(
      scorable_property_manager.GetOrderedScorablePropertyNames(
          kEmailSchemaTypeId, type_config_map_, schema_id_to_type_map_),
      IsOkAndHolds(Pointee(ElementsAre("scorableInt64", "scorableDouble"))));
  EXPECT_THAT(
      scorable_property_manager.GetOrderedScorablePropertyNames(
          kMessageSchemaTypeId, type_config_map_, schema_id_to_type_map_),
      IsOkAndHolds(Pointee(ElementsAre("scorableBoolean", "scorableDouble"))));
}

}  // namespace

}  // namespace lib
}  // namespace icing
