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

#include <string_view>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-util.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {

namespace {

using portable_equals_proto::EqualsProto;
using ::testing::Pointee;

class SchemaUtilTypeConfigInfoCacheTest
    : public ::testing::TestWithParam<bool> {};

TEST_P(SchemaUtilTypeConfigInfoCacheTest, AddAndGetTypeConfigSimple_ok) {
  bool enable_schema_definition_deduping = GetParam();
  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(enable_schema_definition_deduping);

  SchemaTypeConfigProto type_a = SchemaTypeConfigBuilder().SetType("A").Build();
  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("B")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("a")
                  .SetDataTypeDocument("A",
                                       /*index_nested_properties=*/true)
                  .SetCardinality(CARDINALITY_REPEATED))
          .Build();

  // Add config with no properties
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
  // Add config with properties
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_b), IsOk());

  if (enable_schema_definition_deduping) {
    // When deduping is enabled, GetTypeConfigView will populate the
    // properties digest field.
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(
                    SchemaTypeConfigBuilder(type_a)
                        .BuildAndPopulatePropertiesDigest())));
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("B"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(
                    SchemaTypeConfigBuilder(type_b)
                        .BuildAndPopulatePropertiesDigest())));
  } else {
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(type_a)));
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("B"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(type_b)));
  }
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest, GetNonExistingType_returnsNotFound) {
  bool enable_schema_definition_deduping = GetParam();
  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(enable_schema_definition_deduping);

  EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  // Add a config so that cache is no longer empty.
  SchemaTypeConfigProto type_a = SchemaTypeConfigBuilder().SetType("A").Build();
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());

  // Getting non-existing type still returns NOT_FOUND.
  EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("B"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest, AddAndGetAlreadyExistingType_isNoOp) {
  bool enable_schema_definition_deduping = GetParam();
  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(enable_schema_definition_deduping);

  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED))
          .Build();
  SchemaTypeConfigProto type_a_add_property =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop2")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REPEATED))
          .Build();
  SchemaTypeConfigProto type_a_with_digest =
      SchemaTypeConfigBuilder(type_a).BuildAndPopulatePropertiesDigest();

  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a_with_digest), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a_add_property),
              IsOk());

  // Only the first type config is added.
  if (enable_schema_definition_deduping) {
    // When deduping is enabled, GetTypeConfigView will populate the
    // properties digest field.
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(
                    SchemaTypeConfigBuilder(type_a)
                        .BuildAndPopulatePropertiesDigest())));
  } else {
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(type_a)));
  }
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest,
       AddAndGetTypeConfigWithValidPropertiesDigest_ok) {
  bool enable_schema_definition_deduping = GetParam();
  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(enable_schema_definition_deduping);

  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED))
          .BuildAndPopulatePropertiesDigest();
  // Add config with valid properties digest
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
  EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
              IsOkAndHolds(TypeConfigHolderEqualsProto(type_a)));
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest,
       AddFullTypeConfigWithInvalidPropertiesDigest_ok) {
  bool enable_schema_definition_deduping = GetParam();
  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(enable_schema_definition_deduping);

  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop2")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REPEATED))
          .SetPropertiesDigest("invalid_properties_digest")
          .Build();

  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
  if (enable_schema_definition_deduping) {
    EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(
                    SchemaTypeConfigBuilder(type_a)
                        .BuildAndPopulatePropertiesDigest())));
  } else {
    EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(type_a)));
  }
}

TEST_P(
    SchemaUtilTypeConfigInfoCacheTest,
    AddDedupedTypeConfigWithInvalidPropertiesDigest_failsWhenDedupingEnabled) {
  bool enable_schema_definition_deduping = GetParam();
  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(enable_schema_definition_deduping);

  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .SetPropertiesDigest("invalid_properties_digest")
          .Build();

  if (enable_schema_definition_deduping) {
    EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a),
                StatusIs(libtextclassifier3::StatusCode::INTERNAL));
  } else {
    EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(type_a)));
  }
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest,
       AddAndGetTypeConfigWithDuplicatePropertyDefinitions_ok) {
  bool enable_schema_definition_deduping = GetParam();
  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(enable_schema_definition_deduping);

  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED))
          .SetDescription("description")
          .SetDatabase("db/")
          .Build();

  SchemaTypeConfigProto type_a_copy =
      SchemaTypeConfigBuilder(type_a).SetType("A1").Build();

  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a_copy), IsOk());

  // Both type configs are added.
  if (enable_schema_definition_deduping) {
    // When deduping is enabled, GetTypeConfigView will populate the
    // properties digest field.
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(
                    SchemaTypeConfigBuilder(type_a)
                        .BuildAndPopulatePropertiesDigest())));
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A1"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(
                    SchemaTypeConfigBuilder(type_a_copy)
                        .BuildAndPopulatePropertiesDigest())));
  } else {
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(type_a)));
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A1"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(
                    SchemaTypeConfigBuilder(type_a_copy).Build())));
  }
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest,
       AddAndGetDedupedPropertyDefinitions_ok) {
  bool enable_schema_definition_deduping = GetParam();
  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(enable_schema_definition_deduping);

  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop2")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REPEATED))
          .Build();

  // Deduped type config should have a valid properties digest without any
  // property definitions.
  SchemaTypeConfigProto type_a_deduped_copy =
      SchemaTypeConfigBuilder(type_a)
          .SetType("A1")
          .BuildAndPopulatePropertiesDigest();
  type_a_deduped_copy.clear_properties();

  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a_deduped_copy),
              IsOk());

  // GetTypeConfigView
  if (enable_schema_definition_deduping) {
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(
                    SchemaTypeConfigBuilder(type_a)
                        .BuildAndPopulatePropertiesDigest())));
    // When deduping is enabled, properties definitions are populated for
    // GetTypeConfigView.
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A1"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(
                    SchemaTypeConfigBuilder(type_a_deduped_copy)
                        .AddProperty(PropertyConfigBuilder()
                                         .SetName("prop1")
                                         .SetDataTypeString(TERM_MATCH_EXACT,
                                                            TOKENIZER_PLAIN)
                                         .SetCardinality(CARDINALITY_REPEATED))
                        .AddProperty(PropertyConfigBuilder()
                                         .SetName("prop2")
                                         .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                         .SetCardinality(CARDINALITY_REPEATED))
                        .Build())));
  } else {
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(type_a)));
    // When deduping is disabled, properties definitions are not populated for
    // GetTypeConfigView.
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A1"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(type_a_deduped_copy)));
  }
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest, AddDedupedTypeConfigsFirst_ok) {
  bool enable_schema_definition_deduping = GetParam();
  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(enable_schema_definition_deduping);

  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop2")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REPEATED))
          .Build();

  // Deduped type config should have a valid properties digest without any
  // property definitions.
  SchemaTypeConfigProto type_a_deduped_copy1 =
      SchemaTypeConfigBuilder(type_a)
          .SetType("A1")
          .BuildAndPopulatePropertiesDigest();
  type_a_deduped_copy1.clear_properties();

  SchemaTypeConfigProto type_a_deduped_copy2 =
      SchemaTypeConfigBuilder(type_a)
          .SetType("A2")
          .BuildAndPopulatePropertiesDigest();
  type_a_deduped_copy2.clear_properties();

  // Add the deduped type config first. This should not affect the final
  // result.
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a_deduped_copy1),
              IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a_deduped_copy2),
              IsOk());

  // GetTypeConfigView
  if (enable_schema_definition_deduping) {
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(
                    SchemaTypeConfigBuilder(type_a)
                        .BuildAndPopulatePropertiesDigest())));
    // When deduping is enabled, properties definitions are populated for
    // GetTypeConfigView.
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A1"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(
                    SchemaTypeConfigBuilder(type_a_deduped_copy1)
                        .AddProperty(PropertyConfigBuilder()
                                         .SetName("prop1")
                                         .SetDataTypeString(TERM_MATCH_EXACT,
                                                            TOKENIZER_PLAIN)
                                         .SetCardinality(CARDINALITY_REPEATED))
                        .AddProperty(PropertyConfigBuilder()
                                         .SetName("prop2")
                                         .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                         .SetCardinality(CARDINALITY_REPEATED))
                        .Build())));
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A2"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(
                    SchemaTypeConfigBuilder(type_a_deduped_copy2)
                        .AddProperty(PropertyConfigBuilder()
                                         .SetName("prop1")
                                         .SetDataTypeString(TERM_MATCH_EXACT,
                                                            TOKENIZER_PLAIN)
                                         .SetCardinality(CARDINALITY_REPEATED))
                        .AddProperty(PropertyConfigBuilder()
                                         .SetName("prop2")
                                         .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                         .SetCardinality(CARDINALITY_REPEATED))
                        .Build())));
  } else {
    EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
                IsOkAndHolds(TypeConfigHolderEqualsProto(type_a)));
    // When deduping is disabled, properties definitions are not populated for
    // GetTypeConfigView.
    EXPECT_THAT(
        type_config_info_cache.GetFullSchemaTypeConfigHolder("A1"),
        IsOkAndHolds(TypeConfigHolderEqualsProto(type_a_deduped_copy1)));
    EXPECT_THAT(
        type_config_info_cache.GetFullSchemaTypeConfigHolder("A2"),
        IsOkAndHolds(TypeConfigHolderEqualsProto(type_a_deduped_copy2)));
  }
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest,
       AddTypeConfigWithDuplicatePropertyDefinition_dedupesDuplicateDefs) {
  bool enable_schema_definition_deduping = GetParam();
  if (!enable_schema_definition_deduping) {
    GTEST_SKIP() << "This test is only relevant when deduping is enabled.";
  }
  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(enable_schema_definition_deduping);

  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop2")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REPEATED))
          .Build();

  SchemaTypeConfigProto type_a_copy1 =
      SchemaTypeConfigBuilder(type_a).SetType("A1").Build();
  SchemaTypeConfigProto type_a_copy2 =
      SchemaTypeConfigBuilder(type_a).SetType("A2").Build();

  // Add protos with AddTypeConfig
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a_copy1), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a_copy2), IsOk());

  SchemaTypeConfigProto type_a_with_digest =
      SchemaTypeConfigBuilder(type_a).BuildAndPopulatePropertiesDigest();
  SchemaTypeConfigProto type_a_copy1_with_digest =
      SchemaTypeConfigBuilder(type_a_copy1).BuildAndPopulatePropertiesDigest();
  SchemaTypeConfigProto type_a_copy2_with_digest =
      SchemaTypeConfigBuilder(type_a_copy2).BuildAndPopulatePropertiesDigest();

  // GetFullSchemaTypeConfigHolder will always return the fully-defined proto +
  // property digest populated.
  EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
              IsOkAndHolds(TypeConfigHolderEqualsProto(type_a_with_digest)));
  EXPECT_THAT(
      type_config_info_cache.GetFullSchemaTypeConfigHolder("A1"),
      IsOkAndHolds(TypeConfigHolderEqualsProto(type_a_copy1_with_digest)));
  EXPECT_THAT(
      type_config_info_cache.GetFullSchemaTypeConfigHolder("A2"),
      IsOkAndHolds(TypeConfigHolderEqualsProto(type_a_copy2_with_digest)));

  // Check that deduping actually happened using GetRawSchemaTypeConfigPointer.
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("A"),
              IsOkAndHolds(Pointee(EqualsProto(type_a_with_digest))));
  type_a_copy1_with_digest.clear_properties();
  type_a_copy2_with_digest.clear_properties();
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("A1"),
              IsOkAndHolds(Pointee(EqualsProto(type_a_copy1_with_digest))));
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("A2"),
              IsOkAndHolds(Pointee(EqualsProto(type_a_copy2_with_digest))));
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest,
       AddTypeConfigWithDuplicatePropertyDefinition_insertionOrderMatters) {
  bool enable_schema_definition_deduping = GetParam();
  if (!enable_schema_definition_deduping) {
    GTEST_SKIP() << "This test is only relevant when deduping is enabled.";
  }
  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(enable_schema_definition_deduping);

  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop2")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REPEATED))
          .Build();

  SchemaTypeConfigProto type_a_copy1 =
      SchemaTypeConfigBuilder(type_a).SetType("A1").Build();

  // Add protos with AddTypeConfig
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a_copy1), IsOk());

  // GetFullSchemaTypeConfig will always return the fully-defined proto +
  // property digest populated.
  SchemaTypeConfigProto type_a_with_digest =
      SchemaTypeConfigBuilder(type_a).BuildAndPopulatePropertiesDigest();
  SchemaTypeConfigProto type_a_copy1_with_digest =
      SchemaTypeConfigBuilder(type_a_copy1).BuildAndPopulatePropertiesDigest();

  EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
              IsOkAndHolds(TypeConfigHolderEqualsProto(type_a_with_digest)));
  EXPECT_THAT(
      type_config_info_cache.GetFullSchemaTypeConfigHolder("A1"),
      IsOkAndHolds(TypeConfigHolderEqualsProto(type_a_copy1_with_digest)));

  // Check the internal state via GetRawSchemaTypeConfigPointer and verify that
  // type_a_copy1 was deduped.
  SchemaTypeConfigProto deduped_type_a_copy1 = type_a_copy1_with_digest;
  deduped_type_a_copy1.clear_properties();

  // type_a_copy1 should be deduped since it was added later.
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("A"),
              IsOkAndHolds(Pointee(EqualsProto(type_a_with_digest))));
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("A1"),
              IsOkAndHolds(Pointee(EqualsProto(deduped_type_a_copy1))));

  // Clear cache and re-add types in reverse order.
  type_config_info_cache.Clear();
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a_copy1), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());

  // GetFullSchemaTypeConfigHolder results should be the same as before.
  EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
              IsOkAndHolds(TypeConfigHolderEqualsProto(type_a_with_digest)));
  EXPECT_THAT(
      type_config_info_cache.GetFullSchemaTypeConfigHolder("A1"),
      IsOkAndHolds(TypeConfigHolderEqualsProto(type_a_copy1_with_digest)));

  // Check that deduping actually happened using GetRawSchemaTypeConfigPointer.
  SchemaTypeConfigProto deduped_type_a = type_a_with_digest;
  deduped_type_a.clear_properties();

  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("A"),
              IsOkAndHolds(Pointee(EqualsProto(deduped_type_a))));
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("A1"),
              IsOkAndHolds(Pointee(EqualsProto(type_a_copy1_with_digest))));
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest,
       AddTypeConfigWithReorderedProperties_notDeduped) {
  bool enable_schema_definition_deduping = GetParam();
  if (!enable_schema_definition_deduping) {
    GTEST_SKIP() << "This test is only relevant when deduping is enabled.";
  }
  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(enable_schema_definition_deduping);

  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop2")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REPEATED))
          .Build();

  SchemaTypeConfigProto type_a_reordered =
      SchemaTypeConfigBuilder()
          .SetType("AReordered")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop2")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED))
          .Build();

  // Add protos with AddTypeConfig
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a_reordered), IsOk());

  // GetFullSchemaTypeConfig will always return the fully-defined proto with
  // property digest populated.
  SchemaTypeConfigProto type_a_with_digest =
      SchemaTypeConfigBuilder(type_a).BuildAndPopulatePropertiesDigest();
  SchemaTypeConfigProto type_a_reordered_with_digest =
      SchemaTypeConfigBuilder(type_a_reordered)
          .BuildAndPopulatePropertiesDigest();

  EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
              IsOkAndHolds(TypeConfigHolderEqualsProto(type_a_with_digest)));
  EXPECT_THAT(
      type_config_info_cache.GetFullSchemaTypeConfigHolder("AReordered"),
      IsOkAndHolds(TypeConfigHolderEqualsProto(type_a_reordered_with_digest)));

  // Check the raw stored type configs via GetRawSchemaTypeConfigPointer.
  // Neither type should be deduped.
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("A"),
              IsOkAndHolds(Pointee(EqualsProto(type_a_with_digest))));
  EXPECT_THAT(
      type_config_info_cache.GetRawSchemaTypeConfigPointer("AReordered"),
      IsOkAndHolds(Pointee(EqualsProto(type_a_reordered_with_digest))));
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest, AddAndGetNestedDuplicateTypesOk) {
  bool enable_schema_definition_deduping = GetParam();
  if (!enable_schema_definition_deduping) {
    GTEST_SKIP() << "This test is only relevant when deduping is enabled.";
  }
  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(enable_schema_definition_deduping);

  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop2")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REPEATED))
          .Build();

  SchemaTypeConfigProto type_a_copy =
      SchemaTypeConfigBuilder(type_a).SetType("ACopy").Build();

  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("B")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("a")
                           .SetDataTypeDocument(
                               "ACopy", /*indexable_nested_properties=*/true)
                           .SetCardinality(CARDINALITY_REPEATED))
          .Build();
  SchemaTypeConfigProto type_b_copy =
      SchemaTypeConfigBuilder(type_b).SetType("BCopy").Build();

  SchemaTypeConfigProto type_c =
      SchemaTypeConfigBuilder()
          .SetType("C")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("a")
                           .SetDataTypeDocument(
                               "A", /*indexable_nested_properties=*/true)
                           .SetCardinality(CARDINALITY_REPEATED))
          .Build();
  SchemaTypeConfigProto type_c_copy =
      SchemaTypeConfigBuilder(type_c).SetType("CCopy").Build();

  // Add protos with AddTypeConfig
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_b), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_c), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a_copy), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_b_copy), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_c_copy), IsOk());

  // GetFullSchemaTypeConfigHolder will always return the fully-defined proto
  // with property digest populated.
  EXPECT_THAT(
      type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
      IsOkAndHolds(TypeConfigHolderEqualsProto(
          SchemaTypeConfigBuilder(type_a).BuildAndPopulatePropertiesDigest())));
  EXPECT_THAT(
      type_config_info_cache.GetFullSchemaTypeConfigHolder("B"),
      IsOkAndHolds(TypeConfigHolderEqualsProto(
          SchemaTypeConfigBuilder(type_b).BuildAndPopulatePropertiesDigest())));
  EXPECT_THAT(
      type_config_info_cache.GetFullSchemaTypeConfigHolder("C"),
      IsOkAndHolds(TypeConfigHolderEqualsProto(
          SchemaTypeConfigBuilder(type_c).BuildAndPopulatePropertiesDigest())));

  SchemaTypeConfigProto a_copy_with_digest =
      SchemaTypeConfigBuilder(type_a_copy).BuildAndPopulatePropertiesDigest();
  EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("ACopy"),
              IsOkAndHolds(TypeConfigHolderEqualsProto(a_copy_with_digest)));

  SchemaTypeConfigProto b_copy_with_digest =
      SchemaTypeConfigBuilder(type_b_copy).BuildAndPopulatePropertiesDigest();
  EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("BCopy"),
              IsOkAndHolds(TypeConfigHolderEqualsProto(b_copy_with_digest)));

  SchemaTypeConfigProto c_copy_with_digest =
      SchemaTypeConfigBuilder(type_c_copy).BuildAndPopulatePropertiesDigest();
  EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("CCopy"),
              IsOkAndHolds(TypeConfigHolderEqualsProto(c_copy_with_digest)));

  // Check the internal state via GetRawSchemaTypeConfigPointer.
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("A"),
              IsOkAndHolds(Pointee(
                  EqualsProto(SchemaTypeConfigBuilder(type_a)
                                  .BuildAndPopulatePropertiesDigest()))));

  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("B"),
              IsOkAndHolds(Pointee(
                  EqualsProto(SchemaTypeConfigBuilder(type_b)
                                  .BuildAndPopulatePropertiesDigest()))));
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("C"),
              IsOkAndHolds(Pointee(
                  EqualsProto(SchemaTypeConfigBuilder(type_c)
                                  .BuildAndPopulatePropertiesDigest()))));
  // The copy types should be deduped (properties cleared)
  a_copy_with_digest.clear_properties();
  b_copy_with_digest.clear_properties();
  c_copy_with_digest.clear_properties();
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("ACopy"),
              IsOkAndHolds(Pointee(EqualsProto(a_copy_with_digest))));
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("BCopy"),
              IsOkAndHolds(Pointee(EqualsProto(b_copy_with_digest))));
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("CCopy"),
              IsOkAndHolds(Pointee(EqualsProto(c_copy_with_digest))));
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest, RemoveTypeConfig_singleCopyOk) {
  bool enable_schema_definition_deduping = GetParam();
  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(enable_schema_definition_deduping);

  SchemaTypeConfigProto type_a = SchemaTypeConfigBuilder().SetType("A").Build();
  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("B")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("a")
                  .SetDataTypeDocument("A",
                                       /*index_nested_properties=*/true)
                  .SetCardinality(CARDINALITY_REPEATED))
          .Build();

  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_b), IsOk());

  // Check that types are present
  SchemaTypeConfigProto expected_type_a;
  SchemaTypeConfigProto expected_type_b;
  if (enable_schema_definition_deduping) {
    expected_type_a =
        SchemaTypeConfigBuilder(type_a).BuildAndPopulatePropertiesDigest();
    expected_type_b =
        SchemaTypeConfigBuilder(type_b).BuildAndPopulatePropertiesDigest();
  } else {
    expected_type_a = type_a;
    expected_type_b = type_b;
  }
  EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
              IsOkAndHolds(TypeConfigHolderEqualsProto(expected_type_a)));
  EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("B"),
              IsOkAndHolds(TypeConfigHolderEqualsProto(expected_type_b)));

  // Remove types
  EXPECT_THAT(type_config_info_cache.RemoveTypeConfig("A"), IsOk());
  EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("A"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  EXPECT_THAT(type_config_info_cache.RemoveTypeConfig("B"), IsOk());
  EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("B"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("B"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest, RemoveNonExistentTypeConfig_ok) {
  bool enable_schema_definition_deduping = GetParam();
  SchemaUtil::TypeConfigInfoCache empty_info_cache =
      SchemaUtil::TypeConfigInfoCache(enable_schema_definition_deduping);

  EXPECT_THAT(empty_info_cache.GetFullSchemaTypeConfigHolder("A"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(empty_info_cache.RemoveTypeConfig("A"), IsOk());
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest, RemoveDedupedTypeConfig_ok) {
  bool enable_schema_definition_deduping = GetParam();
  if (!enable_schema_definition_deduping) {
    GTEST_SKIP() << "This test is only relevant when deduping is enabled.";
  }
  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(enable_schema_definition_deduping);

  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop2")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REPEATED))
          .Build();

  SchemaTypeConfigProto type_a_copy1 =
      SchemaTypeConfigBuilder(type_a).SetType("A1").Build();
  SchemaTypeConfigProto type_a_copy2 =
      SchemaTypeConfigBuilder(type_a).SetType("A2").Build();

  // Add 3 copies of the same type.
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a_copy1), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a_copy2), IsOk());

  SchemaTypeConfigProto type_a_with_digest =
      SchemaTypeConfigBuilder(type_a).BuildAndPopulatePropertiesDigest();
  SchemaTypeConfigProto deduped_type_a_copy1 =
      SchemaTypeConfigBuilder(type_a)
          .SetType("A1")
          .BuildAndPopulatePropertiesDigest();
  deduped_type_a_copy1.clear_properties();
  SchemaTypeConfigProto deduped_type_a_copy2 =
      SchemaTypeConfigBuilder(type_a)
          .SetType("A2")
          .BuildAndPopulatePropertiesDigest();
  deduped_type_a_copy2.clear_properties();

  // Type A is the canonical type config. It should be the only one with
  // properties populated in the cache's internal storage.
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("A"),
              IsOkAndHolds(Pointee(EqualsProto(type_a_with_digest))));
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("A1"),
              IsOkAndHolds(Pointee(EqualsProto(deduped_type_a_copy1))));
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("A2"),
              IsOkAndHolds(Pointee(EqualsProto(deduped_type_a_copy2))));

  // Remove type A1. Call to GetFullSchemaTypeConfigHolder for A and A2 should
  // still work.
  EXPECT_THAT(type_config_info_cache.RemoveTypeConfig("A1"), IsOk());

  EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A"),
              IsOkAndHolds(TypeConfigHolderEqualsProto(type_a_with_digest)));
  EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A2"),
              IsOkAndHolds(TypeConfigHolderEqualsProto(
                  SchemaTypeConfigBuilder(type_a_copy2)
                      .BuildAndPopulatePropertiesDigest())));
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest, RemoveCanonicalTypeConfig_ok) {
  bool enable_schema_definition_deduping = GetParam();
  if (!enable_schema_definition_deduping) {
    GTEST_SKIP() << "This test is only relevant when deduping is enabled.";
  }
  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(enable_schema_definition_deduping);

  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop2")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_REPEATED))
          .Build();

  SchemaTypeConfigProto type_a_copy1 =
      SchemaTypeConfigBuilder(type_a).SetType("A1").Build();
  SchemaTypeConfigProto type_a_copy2 =
      SchemaTypeConfigBuilder(type_a).SetType("A2").Build();

  // Add 3 copies of the same type.
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a_copy1), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a_copy2), IsOk());

  SchemaTypeConfigProto type_a_with_digest =
      SchemaTypeConfigBuilder(type_a).BuildAndPopulatePropertiesDigest();
  SchemaTypeConfigProto deduped_type_a_copy1 =
      SchemaTypeConfigBuilder(type_a)
          .SetType("A1")
          .BuildAndPopulatePropertiesDigest();
  deduped_type_a_copy1.clear_properties();
  SchemaTypeConfigProto deduped_type_a_copy2 =
      SchemaTypeConfigBuilder(type_a)
          .SetType("A2")
          .BuildAndPopulatePropertiesDigest();
  deduped_type_a_copy2.clear_properties();

  // Type A is the canonical type config. It should be the only one with
  // properties populated in the cache's internal storage.
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("A"),
              IsOkAndHolds(Pointee(EqualsProto(type_a_with_digest))));
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("A1"),
              IsOkAndHolds(Pointee(EqualsProto(deduped_type_a_copy1))));
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("A2"),
              IsOkAndHolds(Pointee(EqualsProto(deduped_type_a_copy2))));

  // Remove type A. Call to GetFullSchemaTypeConfigHolder for A1 and A2 should
  // still work.
  EXPECT_THAT(type_config_info_cache.RemoveTypeConfig("A"), IsOk());
  EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A1"),
              IsOkAndHolds(TypeConfigHolderEqualsProto(
                  SchemaTypeConfigBuilder(type_a_copy1)
                      .BuildAndPopulatePropertiesDigest())));
  EXPECT_THAT(type_config_info_cache.GetFullSchemaTypeConfigHolder("A2"),
              IsOkAndHolds(TypeConfigHolderEqualsProto(
                  SchemaTypeConfigBuilder(type_a_copy2)
                      .BuildAndPopulatePropertiesDigest())));

  // The last inserted deduped type should now be the canonical type config.
  EXPECT_THAT(type_config_info_cache.GetRawSchemaTypeConfigPointer("A2"),
              IsOkAndHolds(Pointee(
                  EqualsProto(SchemaTypeConfigBuilder(type_a_copy2)
                                  .BuildAndPopulatePropertiesDigest()))));
}

INSTANTIATE_TEST_SUITE_P(SchemaUtilTypeConfigInfoCacheTestSuite,
                         SchemaUtilTypeConfigInfoCacheTest,
                         testing::Values(true, false));

}  // namespace

}  // namespace lib
}  // namespace icing
