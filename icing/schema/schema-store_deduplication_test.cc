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

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/feature-flags.h"
#include "icing/file/filesystem.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/schema-util.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/sha256.h"

namespace icing {
namespace lib {

namespace {

using portable_equals_proto::EqualsProto;
using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::IsEmpty;
using ::testing::Ne;
using ::testing::Pair;
using ::testing::Pointee;
using ::testing::UnorderedElementsAre;

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

TEST_P(SchemaUtilTypeConfigInfoCacheTest,
       CalculateSchemaUpdatePlan_addToEmptyCache) {
  bool enable_schema_definition_deduping = GetParam();
  SchemaUtil::TypeConfigInfoCache type_config_info_cache(
      enable_schema_definition_deduping);

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

  SchemaTypeConfigProto expected_type_a =
      enable_schema_definition_deduping
          ? SchemaTypeConfigBuilder(type_a).BuildAndPopulatePropertiesDigest()
          : type_a;
  SchemaTypeConfigProto expected_type_b =
      enable_schema_definition_deduping
          ? SchemaTypeConfigBuilder(type_b).BuildAndPopulatePropertiesDigest()
          : type_b;

  EXPECT_THAT(type_config_info_cache.CalculateSchemaUpdatePlan(
                  /*types_to_add=*/{type_a, type_b}, /*types_to_remove=*/{}),
              IsOkAndHolds(UnorderedElementsAre(
                  Pair("A", EqualsProto(expected_type_a)),
                  Pair("B", EqualsProto(expected_type_b)))));
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest,
       CalculateSchemaUpdatePlan_addToEmptyCacheWithDeduping) {
  bool enable_schema_definition_deduping = GetParam();
  SchemaUtil::TypeConfigInfoCache type_config_info_cache(
      enable_schema_definition_deduping);

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
  SchemaTypeConfigProto type_a1 =
      SchemaTypeConfigBuilder(type_a).SetType("A1").Build();

  SchemaTypeConfigProto expected_type_a =
      enable_schema_definition_deduping
          ? SchemaTypeConfigBuilder(type_a).BuildAndPopulatePropertiesDigest()
          : type_a;
  SchemaTypeConfigProto expected_type_a1 =
      enable_schema_definition_deduping
          ? SchemaTypeConfigBuilder(type_a1).BuildAndPopulatePropertiesDigest()
          : type_a1;
  if (enable_schema_definition_deduping) {
    // type_a1 should be deduped
    expected_type_a1.clear_properties();
  }
  EXPECT_THAT(type_config_info_cache.CalculateSchemaUpdatePlan(
                  /*types_to_add=*/{type_a, type_a1}, /*types_to_remove=*/{}),
              IsOkAndHolds(UnorderedElementsAre(
                  Pair("A", EqualsProto(expected_type_a)),
                  Pair("A1", EqualsProto(expected_type_a1)))));
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest,
       CalculateSchemaUpdatePlan_addToExistingCache) {
  bool enable_schema_definition_deduping = GetParam();
  SchemaUtil::TypeConfigInfoCache type_config_info_cache(
      enable_schema_definition_deduping);

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
  SchemaTypeConfigProto type_a1 =
      SchemaTypeConfigBuilder(type_a).SetType("A1").Build();

  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a1), IsOk());

  SchemaTypeConfigProto type_a2 =
      SchemaTypeConfigBuilder(type_a).SetType("A2").Build();
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
  SchemaTypeConfigProto expected_type_a2 =
      enable_schema_definition_deduping
          ? SchemaTypeConfigBuilder(type_a2).BuildAndPopulatePropertiesDigest()
          : type_a2;
  if (enable_schema_definition_deduping) {
    // type_a2 should be deduped
    expected_type_a2.clear_properties();
  }
  SchemaTypeConfigProto expected_type_b =
      enable_schema_definition_deduping
          ? SchemaTypeConfigBuilder(type_b).BuildAndPopulatePropertiesDigest()
          : type_b;

  EXPECT_THAT(type_config_info_cache.CalculateSchemaUpdatePlan(
                  /*types_to_add=*/{type_a2, type_b}, /*types_to_remove=*/{}),
              IsOkAndHolds(UnorderedElementsAre(
                  Pair("A2", EqualsProto(expected_type_a2)),
                  Pair("B", EqualsProto(expected_type_b)))));
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest,
       CalculateSchemaUpdatePlan_removeDuplicateTypesReturnsEmpty) {
  bool enable_schema_definition_deduping = GetParam();
  SchemaUtil::TypeConfigInfoCache type_config_info_cache(
      enable_schema_definition_deduping);

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
  SchemaTypeConfigProto type_a1 =
      SchemaTypeConfigBuilder(type_a).SetType("A1").Build();
  SchemaTypeConfigProto type_a2 =
      SchemaTypeConfigBuilder(type_a).SetType("A2").Build();
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a1), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a2), IsOk());

  EXPECT_THAT(type_config_info_cache.CalculateSchemaUpdatePlan(
                  /*types_to_add=*/{}, /*types_to_remove=*/{"A1", "A2"}),
              IsOkAndHolds(IsEmpty()));
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest,
       CalculateSchemaUpdatePlan_removeAllTypesReturnsEmpty) {
  bool enable_schema_definition_deduping = GetParam();
  SchemaUtil::TypeConfigInfoCache type_config_info_cache(
      enable_schema_definition_deduping);

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
  SchemaTypeConfigProto type_a1 =
      SchemaTypeConfigBuilder(type_a).SetType("A1").Build();
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
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a1), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_b), IsOk());

  EXPECT_THAT(type_config_info_cache.CalculateSchemaUpdatePlan(
                  /*types_to_add=*/{}, /*types_to_remove=*/{"A", "A1", "B"}),
              IsOkAndHolds(IsEmpty()));
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest,
       CalculateSchemaUpdatePlan_removeCanonicalType) {
  bool enable_schema_definition_deduping = GetParam();
  if (!enable_schema_definition_deduping) {
    GTEST_SKIP() << "This test is only relevant when deduping is enabled.";
  }
  SchemaUtil::TypeConfigInfoCache type_config_info_cache(
      enable_schema_definition_deduping);

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
  SchemaTypeConfigProto type_a1 =
      SchemaTypeConfigBuilder(type_a).SetType("A1").Build();
  SchemaTypeConfigProto type_a2 =
      SchemaTypeConfigBuilder(type_a).SetType("A2").Build();
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
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a1), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a2), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_b), IsOk());

  // type_a1 will be the canonical type after type_a is removed.
  EXPECT_THAT(
      type_config_info_cache.CalculateSchemaUpdatePlan(
          /*types_to_add=*/{}, /*types_to_remove=*/{"A", "A2"}),
      IsOkAndHolds(UnorderedElementsAre(
          Pair("A1", EqualsProto(SchemaTypeConfigBuilder(type_a1)
                                     .BuildAndPopulatePropertiesDigest())))));
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest,
       CalculateSchemaUpdatePlan_removeCanonicalAndAddDuplicateType) {
  bool enable_schema_definition_deduping = GetParam();
  if (!enable_schema_definition_deduping) {
    GTEST_SKIP() << "This test is only relevant when deduping is enabled.";
  }
  SchemaUtil::TypeConfigInfoCache type_config_info_cache(
      enable_schema_definition_deduping);

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
  SchemaTypeConfigProto type_a1 =
      SchemaTypeConfigBuilder(type_a).SetType("A1").Build();
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
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a1), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_b), IsOk());

  // Remove A and add A2. A2 will be the new canonical type while A1 is
  // unchanged.
  SchemaTypeConfigProto type_a2 =
      SchemaTypeConfigBuilder(type_a).SetType("A2").Build();

  EXPECT_THAT(
      type_config_info_cache.CalculateSchemaUpdatePlan(
          /*types_to_add=*/{type_a2}, /*types_to_remove=*/{"A"}),
      IsOkAndHolds(UnorderedElementsAre(
          Pair("A2", EqualsProto(SchemaTypeConfigBuilder(type_a2)
                                     .BuildAndPopulatePropertiesDigest())))));
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest,
       CalculateSchemaUpdatePlan_removeAllAndAddDuplicateType) {
  bool enable_schema_definition_deduping = GetParam();
  if (!enable_schema_definition_deduping) {
    GTEST_SKIP() << "This test is only relevant when deduping is enabled.";
  }
  SchemaUtil::TypeConfigInfoCache type_config_info_cache(
      enable_schema_definition_deduping);

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
  SchemaTypeConfigProto type_a1 =
      SchemaTypeConfigBuilder(type_a).SetType("A1").Build();
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
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_a1), IsOk());
  EXPECT_THAT(type_config_info_cache.AddTypeConfig(type_b), IsOk());

  // Remove A, A1, and add A2. A2 will be the new canonical type.
  SchemaTypeConfigProto type_a2 =
      SchemaTypeConfigBuilder(type_a).SetType("A2").Build();

  EXPECT_THAT(
      type_config_info_cache.CalculateSchemaUpdatePlan(
          /*types_to_add=*/{type_a2}, /*types_to_remove=*/{"A", "A1"}),
      IsOkAndHolds(UnorderedElementsAre(
          Pair("A2", EqualsProto(SchemaTypeConfigBuilder(type_a2)
                                     .BuildAndPopulatePropertiesDigest())))));
}

TEST_P(SchemaUtilTypeConfigInfoCacheTest,
       CalculateSchemaUpdatePlan_removeAndAddSameType) {
  bool enable_schema_definition_deduping = GetParam();
  if (!enable_schema_definition_deduping) {
    GTEST_SKIP() << "This test is only relevant when deduping is enabled.";
  }
  SchemaUtil::TypeConfigInfoCache type_config_info_cache(
      enable_schema_definition_deduping);

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

  // Change A1's definition
  SchemaTypeConfigProto type_a_modified =
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
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop3")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();

  EXPECT_THAT(
      type_config_info_cache.CalculateSchemaUpdatePlan(
          /*types_to_add=*/{type_a_modified}, /*types_to_remove=*/{"A"}),
      IsOkAndHolds(UnorderedElementsAre(
          Pair("A", EqualsProto(SchemaTypeConfigBuilder(type_a_modified)
                                    .BuildAndPopulatePropertiesDigest())))));
}

INSTANTIATE_TEST_SUITE_P(SchemaUtilTypeConfigInfoCacheTestSuite,
                         SchemaUtilTypeConfigInfoCacheTest,
                         testing::Values(true, false));

class SchemaStoreDeduplicationTest : public ::testing::TestWithParam<bool> {
 protected:
  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());
    test_dir_ = GetTestTempDir() + "/icing";
    schema_store_dir_ = test_dir_ + "/schema_store";
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());
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

// Returns true if the schema proto is deduped (i.e. the schema contains exactly
// one copy of each distinct properties definition)
bool IsSchemaProtoDeduped(const SchemaProto& schema) {
  Sha256Digest empty_config_digest =
      SchemaUtil::ComputeSchemaPropertiesSha256Digest(SchemaTypeConfigProto());
  std::unordered_set<Sha256Digest> digests_with_canonical_type;
  std::unordered_set<Sha256Digest> all_seen_digests;

  // Iterate through all types in the schema and check that:
  // - Each type has a valid properties digest.
  // - There is only one canonical type for each digest.
  for (const SchemaTypeConfigProto& type_config : schema.types()) {
    std::optional<Sha256Digest> properties_digest =
        SchemaUtil::GetSchemaPropertiesDigest(type_config);
    if (!properties_digest) {
      // Every type in a deduped schema should have a valid properties digest.
      return false;
    }
    Sha256Digest digest = properties_digest.value();
    if (digest == empty_config_digest) {
      // Skip truly empty types
      continue;
    }
    all_seen_digests.insert(digest);
    if (!type_config.properties().empty()) {
      auto [_, inserted] = digests_with_canonical_type.insert(digest);
      if (!inserted) {
        // We've already seen a canonical type for this digest.
        return false;
      }
    }
  }

  return all_seen_digests.size() == digests_with_canonical_type.size();
}

TEST_P(SchemaStoreDeduplicationTest,
       SetSchemaDedupesCopiedTypes_sameDatabaseOk) {
  if (!feature_flags_->enable_schema_definition_deduping()) {
    GTEST_SKIP() << "This test is only relevant when deduping is enabled.";
  }
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db/message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("email")
                                        .SetDataTypeDocument(
                                            "db/email",
                                            /*index_nested_properties=*/true)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .SetDatabase("db/"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db/messageCopy")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("email")
                                        .SetDataTypeDocument(
                                            "db/email",
                                            /*index_nested_properties=*/true)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .SetDatabase("db/"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db/email")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("title")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("score")
                                        .SetDataType(TYPE_DOUBLE)
                                        .SetScorableType(SCORABLE_TYPE_DISABLED)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .SetDatabase("db/"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("db/emailCopy")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("title")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("score")
                                        .SetDataType(TYPE_DOUBLE)
                                        .SetScorableType(SCORABLE_TYPE_DISABLED)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .SetDatabase("db/"))
          .Build();

  bool db_scoped_set_schema = GetParam();
  SchemaStore::SetSchemaResult expected_result;
  expected_result.success = true;
  expected_result.schema_types_new_by_name.insert("db/message");
  expected_result.schema_types_new_by_name.insert("db/messageCopy");
  expected_result.schema_types_new_by_name.insert("db/email");
  expected_result.schema_types_new_by_name.insert("db/emailCopy");

  if (db_scoped_set_schema) {
    // Set full schema using empty database.
    ICING_ASSERT_OK_AND_ASSIGN(
        SchemaStore::SetSchemaResult actual_result,
        schema_store->SetSchema(CreateSetSchemaRequestProto(
            schema, /*database=*/"db/",
            /*ignore_errors_and_delete_documents=*/false)));
    EXPECT_THAT(actual_result,
                EqualsSetSchemaResultIgnoringStats(expected_result));
    EXPECT_GT(actual_result.schema_proto_byte_size, 0);
  } else {
    // Set full schema using empty database.
    ICING_ASSERT_OK_AND_ASSIGN(
        SchemaStore::SetSchemaResult actual_result,
        schema_store->SetSchema(CreateSetSchemaRequestProto(
            schema, /*database=*/"",
            /*ignore_errors_and_delete_documents=*/false)));
    EXPECT_THAT(actual_result,
                EqualsSetSchemaResultIgnoringStats(expected_result));
    EXPECT_GT(actual_result.schema_proto_byte_size, 0);
  }

  // Check that the file-backed schema proto is deduped.
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* stored_schema,
                             schema_store->GetFileBackedSchemaProto());
  EXPECT_TRUE(IsSchemaProtoDeduped(*stored_schema));

  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_message_type,
      schema_store->GetSchemaTypeConfigPointer("db/message"));
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_message_copy_type,
      schema_store->GetSchemaTypeConfigPointer("db/messageCopy"));
  EXPECT_THAT(stored_message_type->properties_size(),
              Ne(stored_message_copy_type->properties_size()));

  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_email_type,
      schema_store->GetSchemaTypeConfigPointer("db/email"));
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_email_copy_type,
      schema_store->GetSchemaTypeConfigPointer("db/emailCopy"));
  EXPECT_THAT(stored_email_type->properties_size(),
              Ne(stored_email_copy_type->properties_size()));

  // GetFullSchemaProto returns the original schema.
  EXPECT_THAT(schema_store->GetFullSchemaProto(),
              IsOkAndHolds(EqualsProto(schema)));
}

TEST_P(SchemaStoreDeduplicationTest,
       SetSchemaDedupesCopiedTypes_differentDatabasesOk) {
  if (!feature_flags_->enable_schema_definition_deduping()) {
    GTEST_SKIP() << "This test is only relevant when deduping is enabled.";
  }
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));
  SchemaTypeConfigProto db1_message =
      SchemaTypeConfigBuilder()
          .SetType("db1/message")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("text")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .SetDatabase("db1/")
          .Build();
  SchemaTypeConfigProto db1_email =
      SchemaTypeConfigBuilder()
          .SetType("db1/email")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("title")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("score")
                           .SetDataType(TYPE_DOUBLE)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .SetDatabase("db1/")
          .Build();
  SchemaTypeConfigProto db2_message = SchemaTypeConfigBuilder(db1_message)
                                          .SetType("db2/message")
                                          .SetDatabase("db2/")
                                          .Build();
  SchemaTypeConfigProto db2_email = SchemaTypeConfigBuilder(db1_email)
                                        .SetType("db2/email")
                                        .SetDatabase("db2/")
                                        .Build();
  SchemaProto full_schema = SchemaBuilder()
                                .AddType(db1_message)
                                .AddType(db1_email)
                                .AddType(db2_message)
                                .AddType(db2_email)
                                .Build();
  SchemaProto db1_schema =
      SchemaBuilder().AddType(db1_message).AddType(db1_email).Build();
  SchemaProto db2_schema =
      SchemaBuilder().AddType(db2_message).AddType(db2_email).Build();

  bool db_scoped_set_schema = GetParam();
  if (db_scoped_set_schema) {
    // Set schema separately for both databases.
    SchemaStore::SetSchemaResult expected_result;
    expected_result.success = true;
    expected_result.schema_types_new_by_name.insert("db1/message");
    expected_result.schema_types_new_by_name.insert("db1/email");
    ICING_ASSERT_OK_AND_ASSIGN(
        SchemaStore::SetSchemaResult actual_result,
        schema_store->SetSchema(CreateSetSchemaRequestProto(
            db1_schema, /*database=*/"db1/",
            /*ignore_errors_and_delete_documents=*/false)));
    EXPECT_THAT(actual_result,
                EqualsSetSchemaResultIgnoringStats(expected_result));
    EXPECT_GT(actual_result.schema_proto_byte_size, 0);

    expected_result = SchemaStore::SetSchemaResult();
    expected_result.success = true;
    expected_result.schema_types_new_by_name.insert("db2/message");
    expected_result.schema_types_new_by_name.insert("db2/email");
    ICING_ASSERT_OK_AND_ASSIGN(
        actual_result, schema_store->SetSchema(CreateSetSchemaRequestProto(
                           db2_schema, /*database=*/"db2/",
                           /*ignore_errors_and_delete_documents=*/false)));
    EXPECT_THAT(actual_result,
                EqualsSetSchemaResultIgnoringStats(expected_result));
    EXPECT_GT(actual_result.schema_proto_byte_size, 0);
  } else {
    // Set schema for the 2 dbs together using the empty database name.
    SchemaStore::SetSchemaResult expected_result;
    expected_result.success = true;
    expected_result.schema_types_new_by_name.insert("db1/message");
    expected_result.schema_types_new_by_name.insert("db1/email");
    expected_result.schema_types_new_by_name.insert("db2/message");
    expected_result.schema_types_new_by_name.insert("db2/email");
    ICING_ASSERT_OK_AND_ASSIGN(
        SchemaStore::SetSchemaResult actual_result,
        schema_store->SetSchema(CreateSetSchemaRequestProto(
            full_schema,
            /*database=*/"", /*ignore_errors_and_delete_documents=*/false)));
    EXPECT_THAT(actual_result,
                EqualsSetSchemaResultIgnoringStats(expected_result));
    EXPECT_GT(actual_result.schema_proto_byte_size, 0);
  }

  // Check that the file-backed schema proto is deduped.
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* stored_schema,
                             schema_store->GetFileBackedSchemaProto());
  EXPECT_TRUE(IsSchemaProtoDeduped(*stored_schema));

  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db1_message_type,
      schema_store->GetSchemaTypeConfigPointer("db1/message"));
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db2_message_type,
      schema_store->GetSchemaTypeConfigPointer("db2/message"));
  EXPECT_THAT(stored_db1_message_type->properties_size(),
              Ne(stored_db2_message_type->properties_size()));

  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db1_email_type,
      schema_store->GetSchemaTypeConfigPointer("db1/email"));
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db2_email_type,
      schema_store->GetSchemaTypeConfigPointer("db2/email"));
  EXPECT_THAT(stored_db1_email_type->properties_size(),
              Ne(stored_db2_email_type->properties_size()));

  // GetSchema methods returns the db's original schema.
  EXPECT_THAT(schema_store->GetSchema("db1/"),
              IsOkAndHolds(EqualsProto(db1_schema)));
  EXPECT_THAT(schema_store->GetSchema("db2/"),
              IsOkAndHolds(EqualsProto(db2_schema)));

  // GetFullSchemaProto returns the full combined schema
  EXPECT_THAT(schema_store->GetFullSchemaProto(),
              IsOkAndHolds(EqualsProto(full_schema)));
}

TEST_P(SchemaStoreDeduplicationTest, SetSchemaRemoveCanonicalTypes_ok) {
  if (!feature_flags_->enable_schema_definition_deduping()) {
    GTEST_SKIP() << "This test is only relevant when deduping is enabled.";
  }
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaTypeConfigProto db1_message =
      SchemaTypeConfigBuilder()
          .SetType("db1/message")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("text")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .SetDatabase("db1/")
          .Build();
  SchemaTypeConfigProto db1_email =
      SchemaTypeConfigBuilder()
          .SetType("db1/email")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("title")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("score")
                           .SetDataType(TYPE_DOUBLE)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .SetDatabase("db1/")
          .Build();
  SchemaTypeConfigProto db2_message = SchemaTypeConfigBuilder(db1_message)
                                          .SetType("db2/message")
                                          .SetDatabase("db2/")
                                          .Build();
  SchemaTypeConfigProto db2_email = SchemaTypeConfigBuilder(db1_email)
                                        .SetType("db2/email")
                                        .SetDatabase("db2/")
                                        .Build();
  SchemaTypeConfigProto db3_message = SchemaTypeConfigBuilder(db1_message)
                                          .SetType("db3/message")
                                          .SetDatabase("db3/")
                                          .Build();
  SchemaTypeConfigProto db3_email = SchemaTypeConfigBuilder(db1_email)
                                        .SetType("db3/email")
                                        .SetDatabase("db3/")
                                        .Build();
  SchemaProto db1_schema =
      SchemaBuilder().AddType(db1_message).AddType(db1_email).Build();
  SchemaProto db2_schema =
      SchemaBuilder().AddType(db2_message).AddType(db2_email).Build();
  SchemaProto db3_schema =
      SchemaBuilder().AddType(db3_message).AddType(db3_email).Build();

  // Set duplicate schema for all databases.
  SchemaStore::SetSchemaResult expected_result;
  expected_result.success = true;
  expected_result.schema_types_new_by_name.insert("db1/message");
  expected_result.schema_types_new_by_name.insert("db1/email");
  ICING_ASSERT_OK_AND_ASSIGN(
      SchemaStore::SetSchemaResult actual_result,
      schema_store->SetSchema(CreateSetSchemaRequestProto(
          db1_schema,
          /*database=*/"db1/", /*ignore_errors_and_delete_documents=*/false)));
  EXPECT_THAT(actual_result,
              EqualsSetSchemaResultIgnoringStats(expected_result));
  EXPECT_GT(actual_result.schema_proto_byte_size, 0);
  expected_result = SchemaStore::SetSchemaResult();
  expected_result.success = true;
  expected_result.schema_types_new_by_name.insert("db2/message");
  expected_result.schema_types_new_by_name.insert("db2/email");
  ICING_ASSERT_OK_AND_ASSIGN(
      actual_result,
      schema_store->SetSchema(CreateSetSchemaRequestProto(
          db2_schema,
          /*database=*/"db2/", /*ignore_errors_and_delete_documents=*/false)));
  EXPECT_THAT(actual_result,
              EqualsSetSchemaResultIgnoringStats(expected_result));
  EXPECT_GT(actual_result.schema_proto_byte_size, 0);

  expected_result = SchemaStore::SetSchemaResult();
  expected_result.success = true;
  expected_result.schema_types_new_by_name.insert("db3/message");
  expected_result.schema_types_new_by_name.insert("db3/email");
  ICING_ASSERT_OK_AND_ASSIGN(
      actual_result,
      schema_store->SetSchema(CreateSetSchemaRequestProto(
          db3_schema,
          /*database=*/"db3/", /*ignore_errors_and_delete_documents=*/false)));
  EXPECT_THAT(actual_result,
              EqualsSetSchemaResultIgnoringStats(expected_result));
  EXPECT_GT(actual_result.schema_proto_byte_size, 0);

  // Check that the file-backed schema proto is deduped.
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* stored_schema,
                             schema_store->GetFileBackedSchemaProto());
  EXPECT_TRUE(IsSchemaProtoDeduped(*stored_schema));

  // Types in db1 should be the canonical types since they were added first.
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db1_message_type,
      schema_store->GetSchemaTypeConfigPointer("db1/message"));
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db1_email_type,
      schema_store->GetSchemaTypeConfigPointer("db1/email"));
  EXPECT_THAT(stored_db1_message_type->properties_size(), Ne(0));
  EXPECT_THAT(stored_db1_email_type->properties_size(), Ne(0));

  // Remove db1 types.
  expected_result = SchemaStore::SetSchemaResult();
  expected_result.success = true;
  expected_result.schema_types_deleted_by_name.insert("db1/message");
  expected_result.schema_types_deleted_by_name.insert("db1/email");
  expected_result.schema_types_deleted_by_id.insert(0);   // db1/message
  expected_result.schema_types_deleted_by_id.insert(1);   // db1/email
  expected_result.old_schema_type_ids_changed.insert(4);  // db3/message
  expected_result.old_schema_type_ids_changed.insert(5);  // db3/email

  bool db_scoped_set_schema = GetParam();
  if (db_scoped_set_schema) {
    ICING_ASSERT_OK_AND_ASSIGN(
        SchemaStore::SetSchemaResult actual_result,
        schema_store->SetSchema(CreateSetSchemaRequestProto(
            SchemaBuilder().Build(),
            /*database=*/"db1/",
            /*ignore_errors_and_delete_documents=*/true)));
    EXPECT_THAT(actual_result,
                EqualsSetSchemaResultIgnoringStats(expected_result));
    EXPECT_GT(actual_result.schema_proto_byte_size, 0);
  } else {
    SchemaProto full_schema = SchemaBuilder()
                                  .AddType(db2_message)
                                  .AddType(db2_email)
                                  .AddType(db3_message)
                                  .AddType(db3_email)
                                  .Build();
    ICING_ASSERT_OK_AND_ASSIGN(
        SchemaStore::SetSchemaResult actual_result,
        schema_store->SetSchema(CreateSetSchemaRequestProto(
            full_schema, /*database=*/"",
            /*ignore_errors_and_delete_documents=*/true)));
    EXPECT_THAT(actual_result,
                EqualsSetSchemaResultIgnoringStats(expected_result));
    EXPECT_GT(actual_result.schema_proto_byte_size, 0);
  }

  // Remaining db2 and db3 types should still be deduped
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db2_message_type,
      schema_store->GetSchemaTypeConfigPointer("db2/message"));
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db2_email_type,
      schema_store->GetSchemaTypeConfigPointer("db2/email"));
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db3_message_type,
      schema_store->GetSchemaTypeConfigPointer("db3/message"));
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db3_email_type,
      schema_store->GetSchemaTypeConfigPointer("db3/email"));
  EXPECT_THAT(stored_db2_message_type->properties_size(),
              Ne(stored_db3_message_type->properties_size()));
  EXPECT_THAT(stored_db2_email_type->properties_size(),
              Ne(stored_db3_email_type->properties_size()));

  ICING_ASSERT_OK_AND_ASSIGN(stored_schema,
                             schema_store->GetFileBackedSchemaProto());
  EXPECT_TRUE(IsSchemaProtoDeduped(*stored_schema));

  // Check that GetSchema methods return the original full types.
  EXPECT_THAT(schema_store->GetSchema("db1/"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(schema_store->GetSchema("db2/"),
              IsOkAndHolds(EqualsProto(db2_schema)));
  EXPECT_THAT(schema_store->GetSchema("db3/"),
              IsOkAndHolds(EqualsProto(db3_schema)));
  EXPECT_THAT(schema_store->GetFullSchemaProto(),
              IsOkAndHolds(EqualsProto(SchemaBuilder()
                                           .AddType(db3_message)
                                           .AddType(db3_email)
                                           .AddType(db2_message)
                                           .AddType(db2_email)
                                           .Build())));
}

TEST_P(SchemaStoreDeduplicationTest, SetSchemaChangeCanonicalTypes_ok) {
  if (!feature_flags_->enable_schema_definition_deduping()) {
    GTEST_SKIP() << "This test is only relevant when deduping is enabled.";
  }
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaTypeConfigProto db1_message =
      SchemaTypeConfigBuilder()
          .SetType("db1/message")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("text")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .SetDatabase("db1/")
          .Build();
  SchemaTypeConfigProto db1_email =
      SchemaTypeConfigBuilder()
          .SetType("db1/email")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("title")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("score")
                           .SetDataType(TYPE_DOUBLE)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .SetDatabase("db1/")
          .Build();
  SchemaTypeConfigProto db2_message = SchemaTypeConfigBuilder(db1_message)
                                          .SetType("db2/message")
                                          .SetDatabase("db2/")
                                          .Build();
  SchemaTypeConfigProto db2_email = SchemaTypeConfigBuilder(db1_email)
                                        .SetType("db2/email")
                                        .SetDatabase("db2/")
                                        .Build();
  SchemaTypeConfigProto db3_message = SchemaTypeConfigBuilder(db1_message)
                                          .SetType("db3/message")
                                          .SetDatabase("db3/")
                                          .Build();
  SchemaTypeConfigProto db3_email = SchemaTypeConfigBuilder(db1_email)
                                        .SetType("db3/email")
                                        .SetDatabase("db3/")
                                        .Build();
  SchemaProto db1_schema =
      SchemaBuilder().AddType(db1_message).AddType(db1_email).Build();
  SchemaProto db2_schema =
      SchemaBuilder().AddType(db2_message).AddType(db2_email).Build();
  SchemaProto db3_schema =
      SchemaBuilder().AddType(db3_message).AddType(db3_email).Build();

  // Set duplicate schema for all databases.
  SchemaStore::SetSchemaResult expected_result;
  expected_result.success = true;
  expected_result.schema_types_new_by_name.insert("db1/message");
  expected_result.schema_types_new_by_name.insert("db1/email");
  ICING_ASSERT_OK_AND_ASSIGN(
      SchemaStore::SetSchemaResult actual_result,
      schema_store->SetSchema(CreateSetSchemaRequestProto(
          db1_schema, /*database=*/"db1/",
          /*ignore_errors_and_delete_documents=*/false)));
  EXPECT_THAT(actual_result,
              EqualsSetSchemaResultIgnoringStats(expected_result));
  EXPECT_GT(actual_result.schema_proto_byte_size, 0);

  expected_result = SchemaStore::SetSchemaResult();
  expected_result.success = true;
  expected_result.schema_types_new_by_name.insert("db2/message");
  expected_result.schema_types_new_by_name.insert("db2/email");
  ICING_ASSERT_OK_AND_ASSIGN(
      actual_result, schema_store->SetSchema(CreateSetSchemaRequestProto(
                         db2_schema,
                         /*database=*/"db2/",
                         /*ignore_errors_and_delete_documents=*/false)));
  EXPECT_THAT(actual_result,
              EqualsSetSchemaResultIgnoringStats(expected_result));
  EXPECT_GT(actual_result.schema_proto_byte_size, 0);

  expected_result = SchemaStore::SetSchemaResult();
  expected_result.success = true;
  expected_result.schema_types_new_by_name.insert("db3/message");
  expected_result.schema_types_new_by_name.insert("db3/email");
  ICING_ASSERT_OK_AND_ASSIGN(
      actual_result, schema_store->SetSchema(CreateSetSchemaRequestProto(
                         db3_schema,
                         /*database=*/"db3/",
                         /*ignore_errors_and_delete_documents=*/false)));
  EXPECT_THAT(actual_result,
              EqualsSetSchemaResultIgnoringStats(expected_result));
  EXPECT_GT(actual_result.schema_proto_byte_size, 0);

  // Check that the file-backed schema proto is deduped.
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* stored_schema,
                             schema_store->GetFileBackedSchemaProto());
  EXPECT_TRUE(IsSchemaProtoDeduped(*stored_schema));

  // Types in db1 should be the canonical types since they were added first.
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db1_message_type,
      schema_store->GetSchemaTypeConfigPointer("db1/message"));
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db1_email_type,
      schema_store->GetSchemaTypeConfigPointer("db1/email"));
  EXPECT_THAT(stored_db1_message_type->properties_size(), Ne(0));
  EXPECT_THAT(stored_db1_email_type->properties_size(), Ne(0));

  // Reset db1 types.
  SchemaTypeConfigProto db1_message_new =
      SchemaTypeConfigBuilder()
          .SetType("db1/message")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("text")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("text2")
                  .SetDataTypeString(TERM_MATCH_UNKNOWN, TOKENIZER_NONE)
                  .SetCardinality(
                      CARDINALITY_OPTIONAL))  // fully-compatible change
          .SetDatabase("db1/")
          .Build();
  SchemaTypeConfigProto db1_email_new =
      SchemaTypeConfigBuilder()
          .SetType("db1/email")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("title")
                           .SetDataTypeString(
                               TERM_MATCH_PREFIX,
                               TOKENIZER_PLAIN)  // index-incompatible change
                           .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("score")
                           .SetDataType(TYPE_DOUBLE)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .SetDatabase("db1/")
          .Build();
  SchemaProto db1_schema_new =
      SchemaBuilder().AddType(db1_message_new).AddType(db1_email_new).Build();
  SchemaProto full_schema_new = SchemaBuilder()
                                    .AddType(db1_message_new)
                                    .AddType(db1_email_new)
                                    .AddType(db2_message)
                                    .AddType(db2_email)
                                    .AddType(db3_message)
                                    .AddType(db3_email)
                                    .Build();

  bool db_scoped_set_schema = GetParam();
  expected_result = SchemaStore::SetSchemaResult();
  expected_result.success = true;
  expected_result.schema_types_changed_fully_compatible_by_name.insert(
      "db1/message");
  expected_result.schema_types_index_incompatible_by_name.insert("db1/email");
  if (db_scoped_set_schema) {
    ICING_ASSERT_OK_AND_ASSIGN(
        SchemaStore::SetSchemaResult actual_result,
        schema_store->SetSchema(CreateSetSchemaRequestProto(
            db1_schema_new, /*database=*/"db1/",
            /*ignore_errors_and_delete_documents=*/true)));
    EXPECT_THAT(actual_result,
                EqualsSetSchemaResultIgnoringStats(expected_result));
    EXPECT_GT(actual_result.schema_proto_byte_size, 0);
  } else {
    ICING_ASSERT_OK_AND_ASSIGN(
        SchemaStore::SetSchemaResult actual_result,
        schema_store->SetSchema(CreateSetSchemaRequestProto(
            full_schema_new, /*database=*/"",
            /*ignore_errors_and_delete_documents=*/true)));
    EXPECT_THAT(actual_result,
                EqualsSetSchemaResultIgnoringStats(expected_result));
    EXPECT_GT(actual_result.schema_proto_byte_size, 0);
  }

  // Remaining db2 and db3 types should still be deduped
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db2_message_type,
      schema_store->GetSchemaTypeConfigPointer("db2/message"));
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db2_email_type,
      schema_store->GetSchemaTypeConfigPointer("db2/email"));
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db3_message_type,
      schema_store->GetSchemaTypeConfigPointer("db3/message"));
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db3_email_type,
      schema_store->GetSchemaTypeConfigPointer("db3/email"));
  EXPECT_THAT(stored_db2_message_type->properties_size(),
              Ne(stored_db3_message_type->properties_size()));
  EXPECT_THAT(stored_db2_email_type->properties_size(),
              Ne(stored_db3_email_type->properties_size()));

  ICING_ASSERT_OK_AND_ASSIGN(stored_schema,
                             schema_store->GetFileBackedSchemaProto());
  EXPECT_TRUE(IsSchemaProtoDeduped(*stored_schema));

  // Check that GetSchema methods returns the right types.
  EXPECT_THAT(schema_store->GetSchema("db1/"),
              IsOkAndHolds(EqualsProto(db1_schema_new)));
  EXPECT_THAT(schema_store->GetSchema("db2/"),
              IsOkAndHolds(EqualsProto(db2_schema)));
  EXPECT_THAT(schema_store->GetSchema("db3/"),
              IsOkAndHolds(EqualsProto(db3_schema)));
  EXPECT_THAT(schema_store->GetFullSchemaProto(),
              IsOkAndHolds(EqualsProto(full_schema_new)));
}

TEST_P(SchemaStoreDeduplicationTest, SetSchemaChangeOrRemoveDedupedTypes_ok) {
  if (!feature_flags_->enable_schema_definition_deduping()) {
    GTEST_SKIP() << "This test is only relevant when deduping is enabled.";
  }
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaTypeConfigProto db1_message =
      SchemaTypeConfigBuilder()
          .SetType("db1/message")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("text")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .SetDatabase("db1/")
          .Build();
  SchemaTypeConfigProto db1_email =
      SchemaTypeConfigBuilder()
          .SetType("db1/email")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("title")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("score")
                           .SetDataType(TYPE_DOUBLE)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .SetDatabase("db1/")
          .Build();
  SchemaTypeConfigProto db2_message = SchemaTypeConfigBuilder(db1_message)
                                          .SetType("db2/message")
                                          .SetDatabase("db2/")
                                          .Build();
  SchemaTypeConfigProto db2_email = SchemaTypeConfigBuilder(db1_email)
                                        .SetType("db2/email")
                                        .SetDatabase("db2/")
                                        .Build();
  SchemaTypeConfigProto db3_message = SchemaTypeConfigBuilder(db1_message)
                                          .SetType("db3/message")
                                          .SetDatabase("db3/")
                                          .Build();
  SchemaTypeConfigProto db3_email = SchemaTypeConfigBuilder(db1_email)
                                        .SetType("db3/email")
                                        .SetDatabase("db3/")
                                        .Build();
  SchemaProto db1_schema =
      SchemaBuilder().AddType(db1_message).AddType(db1_email).Build();
  SchemaProto db2_schema =
      SchemaBuilder().AddType(db2_message).AddType(db2_email).Build();
  SchemaProto db3_schema =
      SchemaBuilder().AddType(db3_message).AddType(db3_email).Build();

  // Set duplicate schema for all databases.
  SchemaStore::SetSchemaResult expected_result;
  expected_result.success = true;
  expected_result.schema_types_new_by_name.insert("db1/message");
  expected_result.schema_types_new_by_name.insert("db1/email");
  ICING_ASSERT_OK_AND_ASSIGN(
      SchemaStore::SetSchemaResult actual_result,
      schema_store->SetSchema(CreateSetSchemaRequestProto(
          db1_schema, /*database=*/"db1/",
          /*ignore_errors_and_delete_documents=*/false)));
  EXPECT_THAT(actual_result,
              EqualsSetSchemaResultIgnoringStats(expected_result));
  EXPECT_GT(actual_result.schema_proto_byte_size, 0);

  expected_result = SchemaStore::SetSchemaResult();
  expected_result.success = true;
  expected_result.schema_types_new_by_name.insert("db2/message");
  expected_result.schema_types_new_by_name.insert("db2/email");
  ICING_ASSERT_OK_AND_ASSIGN(
      actual_result, schema_store->SetSchema(CreateSetSchemaRequestProto(
                         db2_schema,
                         /*database=*/"db2/",
                         /*ignore_errors_and_delete_documents=*/false)));
  EXPECT_THAT(actual_result,
              EqualsSetSchemaResultIgnoringStats(expected_result));
  EXPECT_GT(actual_result.schema_proto_byte_size, 0);

  expected_result = SchemaStore::SetSchemaResult();
  expected_result.success = true;
  expected_result.schema_types_new_by_name.insert("db3/message");
  expected_result.schema_types_new_by_name.insert("db3/email");
  ICING_ASSERT_OK_AND_ASSIGN(
      actual_result, schema_store->SetSchema(CreateSetSchemaRequestProto(
                         db3_schema,
                         /*database=*/"db3/",
                         /*ignore_errors_and_delete_documents=*/false)));
  EXPECT_THAT(actual_result,
              EqualsSetSchemaResultIgnoringStats(expected_result));
  EXPECT_GT(actual_result.schema_proto_byte_size, 0);

  // Check that the file-backed schema proto is deduped.
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* stored_schema,
                             schema_store->GetFileBackedSchemaProto());
  EXPECT_TRUE(IsSchemaProtoDeduped(*stored_schema));

  // Types in db2 and db3 should be the deduped types.
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db2_message_type,
      schema_store->GetSchemaTypeConfigPointer("db2/message"));
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db2_email_type,
      schema_store->GetSchemaTypeConfigPointer("db2/email"));
  EXPECT_THAT(stored_db2_message_type->properties_size(), Eq(0));
  EXPECT_THAT(stored_db2_email_type->properties_size(), Eq(0));
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db3_message_type,
      schema_store->GetSchemaTypeConfigPointer("db3/message"));
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db3_email_type,
      schema_store->GetSchemaTypeConfigPointer("db3/email"));
  EXPECT_THAT(stored_db3_message_type->properties_size(), Eq(0));
  EXPECT_THAT(stored_db3_email_type->properties_size(), Eq(0));

  // Reset db2 types.
  SchemaTypeConfigProto db2_message_new =
      SchemaTypeConfigBuilder()
          .SetType("db2/message")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("text")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("text2")
                  .SetDataTypeString(TERM_MATCH_UNKNOWN, TOKENIZER_NONE)
                  .SetCardinality(
                      CARDINALITY_OPTIONAL))  // fully-compatible change
          .SetDatabase("db2/")
          .Build();

  // Remove email type.
  SchemaProto db2_schema_new = SchemaBuilder().AddType(db2_message_new).Build();
  SchemaProto full_schema_new = SchemaBuilder()
                                    .AddType(db1_message)
                                    .AddType(db1_email)
                                    .AddType(db2_message_new)
                                    .AddType(db3_email)
                                    .AddType(db3_message)
                                    .Build();

  expected_result = SchemaStore::SetSchemaResult();
  expected_result.success = true;
  expected_result.schema_types_changed_fully_compatible_by_name.insert(
      "db2/message");
  expected_result.schema_types_deleted_by_name.insert("db2/email");
  expected_result.schema_types_deleted_by_id.insert(3);  // db2/email
  // Type id for db3/email. This will backfill the gap for db2/email.
  expected_result.old_schema_type_ids_changed.insert(5);

  bool db_scoped_set_schema = GetParam();
  if (db_scoped_set_schema) {
    ICING_ASSERT_OK_AND_ASSIGN(
        SchemaStore::SetSchemaResult actual_result,
        schema_store->SetSchema(CreateSetSchemaRequestProto(
            db2_schema_new,
            /*database=*/"db2/",
            /*ignore_errors_and_delete_documents=*/true)));
    EXPECT_THAT(actual_result,
                EqualsSetSchemaResultIgnoringStats(expected_result));
    EXPECT_GT(actual_result.schema_proto_byte_size, 0);
  } else {
    ICING_ASSERT_OK_AND_ASSIGN(
        SchemaStore::SetSchemaResult actual_result,
        schema_store->SetSchema(CreateSetSchemaRequestProto(
            full_schema_new,
            /*database=*/"",
            /*ignore_errors_and_delete_documents=*/true)));
    EXPECT_THAT(actual_result,
                EqualsSetSchemaResultIgnoringStats(expected_result));
    EXPECT_GT(actual_result.schema_proto_byte_size, 0);
  }

  // Remaining db1 and db3 types should still be deduped
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db1_message_type,
      schema_store->GetSchemaTypeConfigPointer("db1/message"));
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db1_email_type,
      schema_store->GetSchemaTypeConfigPointer("db1/email"));
  ICING_ASSERT_OK_AND_ASSIGN(
      stored_db3_message_type,
      schema_store->GetSchemaTypeConfigPointer("db3/message"));
  ICING_ASSERT_OK_AND_ASSIGN(
      stored_db3_email_type,
      schema_store->GetSchemaTypeConfigPointer("db3/email"));
  EXPECT_THAT(stored_db1_message_type->properties_size(),
              Ne(stored_db3_message_type->properties_size()));
  EXPECT_THAT(stored_db1_email_type->properties_size(),
              Ne(stored_db3_email_type->properties_size()));

  // db2 should no longer be deduped.
  ICING_ASSERT_OK_AND_ASSIGN(
      stored_db2_message_type,
      schema_store->GetSchemaTypeConfigPointer("db2/message"));
  EXPECT_THAT(stored_db2_message_type->properties_size(), Ne(0));
  EXPECT_THAT(schema_store->GetSchemaTypeConfigPointer("db2/email"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  ICING_ASSERT_OK_AND_ASSIGN(stored_schema,
                             schema_store->GetFileBackedSchemaProto());
  EXPECT_TRUE(IsSchemaProtoDeduped(*stored_schema));

  // Check that GetSchema methods returns the right types.
  EXPECT_THAT(schema_store->GetSchema("db1/"),
              IsOkAndHolds(EqualsProto(db1_schema)));
  EXPECT_THAT(schema_store->GetSchema("db2/"),
              IsOkAndHolds(EqualsProto(db2_schema_new)));
  EXPECT_THAT(
      schema_store->GetSchema("db3/"),
      IsOkAndHolds(EqualsProto(
          SchemaBuilder().AddType(db3_email).AddType(db3_message).Build())));

  EXPECT_THAT(schema_store->GetFullSchemaProto(),
              IsOkAndHolds(EqualsProto(full_schema_new)));
}

TEST_P(SchemaStoreDeduplicationTest,
       SetSchemaUpdateMetadataFields_schemaIsStillDeduped) {
  if (!feature_flags_->enable_schema_definition_deduping()) {
    GTEST_SKIP() << "This test is only relevant when deduping is enabled.";
  }
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, schema_store_dir_, &fake_clock_,
                          feature_flags_.get()));

  SchemaTypeConfigProto db1_message =
      SchemaTypeConfigBuilder()
          .SetType("db1/message")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("text")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED))
          .SetDatabase("db1/")
          .SetDescription("message_description")
          .Build();
  SchemaTypeConfigProto db1_email =
      SchemaTypeConfigBuilder()
          .SetType("db1/email")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("title")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_REQUIRED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("score")
                           .SetDataType(TYPE_DOUBLE)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("account")
                           .SetDataType(TYPE_STRING)
                           .SetCardinality(CARDINALITY_REPEATED))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("account1")
                           .SetDataType(TYPE_STRING)
                           .SetCardinality(CARDINALITY_REPEATED))
          .SetDatabase("db1/")
          .SetDescription("email_description")
          .AddAccountProperty("account")
          .Build();
  SchemaTypeConfigProto db2_message = SchemaTypeConfigBuilder(db1_message)
                                          .SetType("db2/message")
                                          .SetDatabase("db2/")
                                          .Build();
  SchemaTypeConfigProto db2_email = SchemaTypeConfigBuilder(db1_email)
                                        .SetType("db2/email")
                                        .SetDatabase("db2/")
                                        .Build();
  SchemaProto db1_schema =
      SchemaBuilder().AddType(db1_message).AddType(db1_email).Build();
  SchemaProto db2_schema =
      SchemaBuilder().AddType(db2_message).AddType(db2_email).Build();

  // Set duplicate schema
  SchemaStore::SetSchemaResult expected_result;
  expected_result.success = true;
  expected_result.schema_types_new_by_name.insert("db1/message");
  expected_result.schema_types_new_by_name.insert("db1/email");
  ICING_ASSERT_OK_AND_ASSIGN(
      SchemaStore::SetSchemaResult actual_result,
      schema_store->SetSchema(CreateSetSchemaRequestProto(
          db1_schema, /*database=*/"db1/",
          /*ignore_errors_and_delete_documents=*/false)));
  EXPECT_THAT(actual_result,
              EqualsSetSchemaResultIgnoringStats(expected_result));
  EXPECT_GT(actual_result.schema_proto_byte_size, 0);

  expected_result = SchemaStore::SetSchemaResult();
  expected_result.success = true;
  expected_result.schema_types_new_by_name.insert("db2/message");
  expected_result.schema_types_new_by_name.insert("db2/email");
  ICING_ASSERT_OK_AND_ASSIGN(
      actual_result, schema_store->SetSchema(CreateSetSchemaRequestProto(
                         db2_schema,
                         /*database=*/"db2/",
                         /*ignore_errors_and_delete_documents=*/false)));
  EXPECT_THAT(actual_result,
              EqualsSetSchemaResultIgnoringStats(expected_result));
  EXPECT_GT(actual_result.schema_proto_byte_size, 0);

  // Check that the file-backed schema proto is deduped.
  ICING_ASSERT_OK_AND_ASSIGN(const SchemaProto* stored_schema,
                             schema_store->GetFileBackedSchemaProto());
  EXPECT_TRUE(IsSchemaProtoDeduped(*stored_schema));

  // Types in db1 should be the canonical types since they were added first.
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db1_message_type,
      schema_store->GetSchemaTypeConfigPointer("db1/message"));
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db1_email_type,
      schema_store->GetSchemaTypeConfigPointer("db1/email"));
  EXPECT_THAT(stored_db1_message_type->properties_size(), Ne(0));
  EXPECT_THAT(stored_db1_email_type->properties_size(), Ne(0));

  // Reset types and update the description
  SchemaTypeConfigProto db1_message_new =
      SchemaTypeConfigBuilder(db1_message)
          .SetDescription("message_description_new")
          .Build();
  SchemaTypeConfigProto db1_email_new =
      SchemaTypeConfigBuilder(db1_email)
          .SetDescription("email_description_new")
          .AddAccountProperty("account1")
          .Build();
  db1_schema =
      SchemaBuilder().AddType(db1_message_new).AddType(db1_email_new).Build();

  SchemaTypeConfigProto db2_message_new =
      SchemaTypeConfigBuilder(db2_message)
          .SetDescription("db2_message_description_new")
          .Build();
  SchemaTypeConfigProto db2_email_new =
      SchemaTypeConfigBuilder(db2_email)
          .SetDescription("db2_email_description_new")
          .AddAccountProperty("account1")
          .Build();
  db2_schema =
      SchemaBuilder().AddType(db2_message_new).AddType(db2_email_new).Build();

  SchemaProto full_schema_new = SchemaBuilder()
                                    .AddType(db1_message_new)
                                    .AddType(db1_email_new)
                                    .AddType(db2_message_new)
                                    .AddType(db2_email_new)
                                    .Build();

  bool db_scoped_set_schema = GetParam();
  expected_result = SchemaStore::SetSchemaResult();
  expected_result.success = true;
  if (db_scoped_set_schema) {
    ICING_ASSERT_OK_AND_ASSIGN(
        SchemaStore::SetSchemaResult actual_result,
        schema_store->SetSchema(CreateSetSchemaRequestProto(
            db1_schema, /*database=*/"db1/",
            /*ignore_errors_and_delete_documents=*/true)));
    ICING_ASSERT_OK_AND_ASSIGN(
        actual_result, schema_store->SetSchema(CreateSetSchemaRequestProto(
                           db2_schema, /*database=*/"db2/",
                           /*ignore_errors_and_delete_documents=*/true)));
    expected_result.schema_types_incompatible_by_id.insert(3);
    expected_result.schema_types_incompatible_by_name.insert("db2/email");
    EXPECT_THAT(actual_result,
                EqualsSetSchemaResultIgnoringStats(expected_result));
    EXPECT_GT(actual_result.schema_proto_byte_size, 0);
  } else {
    ICING_ASSERT_OK_AND_ASSIGN(
        SchemaStore::SetSchemaResult actual_result,
        schema_store->SetSchema(CreateSetSchemaRequestProto(
            full_schema_new, /*database=*/"",
            /*ignore_errors_and_delete_documents=*/true)));
    expected_result.schema_types_incompatible_by_id.insert(1);
    expected_result.schema_types_incompatible_by_id.insert(3);
    expected_result.schema_types_incompatible_by_name.insert("db1/email");
    expected_result.schema_types_incompatible_by_name.insert("db2/email");
    EXPECT_THAT(actual_result,
                EqualsSetSchemaResultIgnoringStats(expected_result));
    EXPECT_GT(actual_result.schema_proto_byte_size, 0);
  }

  // db2 should still be deduped
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db2_message_type,
      schema_store->GetSchemaTypeConfigPointer("db2/message"));
  ICING_ASSERT_OK_AND_ASSIGN(
      const SchemaTypeConfigProto* stored_db2_email_type,
      schema_store->GetSchemaTypeConfigPointer("db2/email"));
  EXPECT_THAT(stored_db2_message_type->properties_size(), Eq(0));
  EXPECT_THAT(stored_db2_email_type->properties_size(), Eq(0));

  ICING_ASSERT_OK_AND_ASSIGN(stored_schema,
                             schema_store->GetFileBackedSchemaProto());
  EXPECT_TRUE(IsSchemaProtoDeduped(*stored_schema));

  // Check that GetSchema methods returns the right types.
  EXPECT_THAT(schema_store->GetSchema("db1/"),
              IsOkAndHolds(EqualsProto(db1_schema)));
  EXPECT_THAT(schema_store->GetSchema("db2/"),
              IsOkAndHolds(EqualsProto(db2_schema)));
  EXPECT_THAT(schema_store->GetFullSchemaProto(),
              IsOkAndHolds(EqualsProto(full_schema_new)));
}

INSTANTIATE_TEST_SUITE_P(SchemaStoreDeduplicationTest,
                         SchemaStoreDeduplicationTest,
                         testing::Values(
                             /*db_scoped_set_schema=*/true, false));

}  // namespace

}  // namespace lib
}  // namespace icing
