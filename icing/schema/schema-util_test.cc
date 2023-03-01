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

#include "icing/schema/schema-util.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_set>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/schema-builder.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {
namespace {

using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsEmpty;

// Properties/fields in a schema type
constexpr char kEmailType[] = "EmailMessage";
constexpr char kMessageType[] = "Text";
constexpr char kPersonType[] = "Person";

TEST(SchemaUtilTest, DependentGraphAlphabeticalOrder) {
  // Create a schema with the following dependent relation:
  //         C
  //       /   \
  // A - B       E - F
  //       \   /
  //         D
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("b")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("B", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("B")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("c")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("C", /*index_nested_properties=*/true))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("d")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("D", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_c =
      SchemaTypeConfigBuilder()
          .SetType("C")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("e")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("E", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_d =
      SchemaTypeConfigBuilder()
          .SetType("D")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("e")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("E", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_e =
      SchemaTypeConfigBuilder()
          .SetType("E")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("f")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("F", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_f =
      SchemaTypeConfigBuilder()
          .SetType("F")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("text")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN))
          .Build();

  // Provide these in alphabetical order: A, B, C, D, E, F
  SchemaProto schema = SchemaBuilder()
                           .AddType(type_a)
                           .AddType(type_b)
                           .AddType(type_c)
                           .AddType(type_d)
                           .AddType(type_e)
                           .AddType(type_f)
                           .Build();
  ICING_ASSERT_OK_AND_ASSIGN(SchemaUtil::DependentMap d_map,
                             SchemaUtil::Validate(schema));
  EXPECT_THAT(d_map, testing::SizeIs(5));
  EXPECT_THAT(d_map["F"],
              testing::UnorderedElementsAre("A", "B", "C", "D", "E"));
  EXPECT_THAT(d_map["E"], testing::UnorderedElementsAre("A", "B", "C", "D"));
  EXPECT_THAT(d_map["D"], testing::UnorderedElementsAre("A", "B"));
  EXPECT_THAT(d_map["C"], testing::UnorderedElementsAre("A", "B"));
  EXPECT_THAT(d_map["B"], testing::UnorderedElementsAre("A"));
}

TEST(SchemaUtilTest, DependentGraphReverseAlphabeticalOrder) {
  // Create a schema with the following dependent relation:
  //         C
  //       /   \
  // A - B       E - F
  //       \   /
  //         D
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("b")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("B", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("B")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("c")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("C", /*index_nested_properties=*/true))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("d")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("D", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_c =
      SchemaTypeConfigBuilder()
          .SetType("C")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("e")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("E", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_d =
      SchemaTypeConfigBuilder()
          .SetType("D")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("e")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("E", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_e =
      SchemaTypeConfigBuilder()
          .SetType("E")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("f")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("F", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_f =
      SchemaTypeConfigBuilder()
          .SetType("F")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("text")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN))
          .Build();

  // Provide these in reverse alphabetical order:
  //   F, E, D, C, B, A
  SchemaProto schema = SchemaBuilder()
                           .AddType(type_f)
                           .AddType(type_e)
                           .AddType(type_d)
                           .AddType(type_c)
                           .AddType(type_b)
                           .AddType(type_a)
                           .Build();
  ICING_ASSERT_OK_AND_ASSIGN(SchemaUtil::DependentMap d_map,
                             SchemaUtil::Validate(schema));
  EXPECT_THAT(d_map, testing::SizeIs(5));
  EXPECT_THAT(d_map["F"],
              testing::UnorderedElementsAre("A", "B", "C", "D", "E"));
  EXPECT_THAT(d_map["E"], testing::UnorderedElementsAre("A", "B", "C", "D"));
  EXPECT_THAT(d_map["D"], testing::UnorderedElementsAre("A", "B"));
  EXPECT_THAT(d_map["C"], testing::UnorderedElementsAre("A", "B"));
  EXPECT_THAT(d_map["B"], testing::UnorderedElementsAre("A"));
}

TEST(SchemaUtilTest, DependentGraphMixedOrder) {
  // Create a schema with the following dependent relation:
  //         C
  //       /   \
  // A - B       E - F
  //       \   /
  //         D
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("b")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("B", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("B")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("c")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("C", /*index_nested_properties=*/true))
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("d")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("D", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_c =
      SchemaTypeConfigBuilder()
          .SetType("C")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("e")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("E", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_d =
      SchemaTypeConfigBuilder()
          .SetType("D")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("e")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("E", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_e =
      SchemaTypeConfigBuilder()
          .SetType("E")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("f")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("F", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_f =
      SchemaTypeConfigBuilder()
          .SetType("F")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("text")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN))
          .Build();

  // Provide these in a random order: C, E, F, A, B, D
  SchemaProto schema = SchemaBuilder()
                           .AddType(type_c)
                           .AddType(type_e)
                           .AddType(type_f)
                           .AddType(type_a)
                           .AddType(type_b)
                           .AddType(type_d)
                           .Build();
  ICING_ASSERT_OK_AND_ASSIGN(SchemaUtil::DependentMap d_map,
                             SchemaUtil::Validate(schema));
  EXPECT_THAT(d_map, testing::SizeIs(5));
  EXPECT_THAT(d_map["F"],
              testing::UnorderedElementsAre("A", "B", "C", "D", "E"));
  EXPECT_THAT(d_map["E"], testing::UnorderedElementsAre("A", "B", "C", "D"));
  EXPECT_THAT(d_map["D"], testing::UnorderedElementsAre("A", "B"));
  EXPECT_THAT(d_map["C"], testing::UnorderedElementsAre("A", "B"));
  EXPECT_THAT(d_map["B"], testing::UnorderedElementsAre("A"));
}

TEST(SchemaUtilTest, TopLevelCycle) {
  // Create a schema with the following dependent relation:
  // A - B - B - B - B....
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("b")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("B", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("B")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("b")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("B", /*index_nested_properties=*/true))
          .Build();

  SchemaProto schema = SchemaBuilder().AddType(type_a).AddType(type_b).Build();
  EXPECT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("Infinite loop")));
}

TEST(SchemaUtilTest, MultiLevelCycle) {
  // Create a schema with the following dependent relation:
  // A - B - C - A - B - C - A ...
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("b")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("B", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("B")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("c")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("C", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_c =
      SchemaTypeConfigBuilder()
          .SetType("C")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("a")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("A", /*index_nested_properties=*/true))
          .Build();

  SchemaProto schema =
      SchemaBuilder().AddType(type_a).AddType(type_b).AddType(type_c).Build();
  EXPECT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(SchemaUtilTest, NonExistentType) {
  // Create a schema with the following dependent relation:
  // A - B - C - X (does not exist)
  SchemaTypeConfigProto type_a =
      SchemaTypeConfigBuilder()
          .SetType("A")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("b")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("B", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_b =
      SchemaTypeConfigBuilder()
          .SetType("B")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("c")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("C", /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto type_c =
      SchemaTypeConfigBuilder()
          .SetType("C")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("x")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeDocument("X", /*index_nested_properties=*/true))
          .Build();

  SchemaProto schema =
      SchemaBuilder().AddType(type_a).AddType(type_b).AddType(type_c).Build();
  EXPECT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(SchemaUtilTest, EmptySchemaProtoIsValid) {
  SchemaProto schema;
  ICING_ASSERT_OK(SchemaUtil::Validate(schema));
}

TEST(SchemaUtilTest, Valid_Nested) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("subject")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("sender")
                                        .SetDataTypeDocument(
                                            kPersonType,
                                            /*index_nested_properties=*/true)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("name")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  ICING_ASSERT_OK(SchemaUtil::Validate(schema));
}

TEST(SchemaUtilTest, ClearedPropertyConfigsIsValid) {
  // No property fields is technically ok, but probably not realistic.
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType(kEmailType))
          .Build();
  ICING_ASSERT_OK(SchemaUtil::Validate(schema));
}

TEST(SchemaUtilTest, ClearedSchemaTypeIsInvalid) {
  SchemaProto schema =
      SchemaBuilder().AddType(SchemaTypeConfigBuilder()).Build();
  ASSERT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(SchemaUtilTest, EmptySchemaTypeIsInvalid) {
  SchemaProto schema =
      SchemaBuilder().AddType(SchemaTypeConfigBuilder().SetType("")).Build();

  ASSERT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(SchemaUtilTest, AnySchemaTypeOk) {
  SchemaProto schema = SchemaBuilder()
                           .AddType(SchemaTypeConfigBuilder().SetType(
                               "abc123!@#$%^&*()_-+=[{]}|\\;:'\",<.>?你好"))
                           .Build();

  ICING_ASSERT_OK(SchemaUtil::Validate(schema));
}

TEST(SchemaUtilTest, ClearedPropertyNameIsInvalid) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("foo")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();
  schema.mutable_types(0)->mutable_properties(0)->clear_property_name();
  ASSERT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(SchemaUtilTest, EmptyPropertyNameIsInvalid) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  ASSERT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(SchemaUtilTest, NonAlphanumericPropertyNameIsInvalid) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("a_b")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  ASSERT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(SchemaUtilTest, AlphanumericPropertyNameOk) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("abc123")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  ICING_ASSERT_OK(SchemaUtil::Validate(schema));
}

TEST(SchemaUtilTest, DuplicatePropertyNameIsInvalid) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("DuplicatedProperty")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("DuplicatedProperty")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();
  ASSERT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::ALREADY_EXISTS));
}

TEST(SchemaUtilTest, ClearedDataTypeIsInvalid) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("NewProperty")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();
  schema.mutable_types(0)->mutable_properties(0)->clear_data_type();
  ASSERT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(SchemaUtilTest, UnknownDataTypeIsInvalid) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType(kEmailType)
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("NewProperty")
                          .SetDataType(PropertyConfigProto::DataType::UNKNOWN)
                          .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();
  ASSERT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(SchemaUtilTest, ClearedCardinalityIsInvalid) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("NewProperty")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();
  schema.mutable_types(0)->mutable_properties(0)->clear_cardinality();
  ASSERT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(SchemaUtilTest, UnknownCardinalityIsInvalid) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("NewProperty")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_UNKNOWN)))
          .Build();
  ASSERT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(SchemaUtilTest, ClearedPropertySchemaTypeIsInvalid) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("NewProperty")
                                        .SetDataType(TYPE_DOCUMENT)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .Build();
  ASSERT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(SchemaUtilTest, Invalid_EmptyPropertySchemaType) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("NewProperty")
                                        .SetDataTypeDocument(
                                            /*schema_type=*/"",
                                            /*index_nested_properties=*/true)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  ASSERT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(SchemaUtilTest, NoMatchingSchemaTypeIsInvalid) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("NewProperty")
                                        .SetDataTypeDocument(
                                            /*schema_type=*/"NewSchemaType",
                                            /*index_nested_properties=*/true)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  ASSERT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("Undefined 'schema_type'")));
}

TEST(SchemaUtilTest, NewOptionalPropertyIsCompatible) {
  // Configure old schema
  SchemaProto old_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop1")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  // Configure new schema with an optional field, not considered incompatible
  // since it's fine if old data doesn't have this optional field
  SchemaProto new_schema_with_optional =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop1")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("NewOptional")
                                        .SetDataType(TYPE_DOUBLE)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.schema_types_changed_fully_compatible.insert(kEmailType);
  SchemaUtil::DependentMap no_dependents_map;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(
                  old_schema, new_schema_with_optional, no_dependents_map),
              Eq(schema_delta));
}

TEST(SchemaUtilTest, NewRequiredPropertyIsIncompatible) {
  // Configure old schema
  SchemaProto old_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop1")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  // Configure new schema with a required field, considered incompatible since
  // old data won't have this required field
  SchemaProto new_schema_with_required =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop1")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("NewRequired")
                                        .SetDataType(TYPE_DOUBLE)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.schema_types_incompatible.emplace(kEmailType);
  SchemaUtil::DependentMap no_dependents_map;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(
                  old_schema, new_schema_with_required, no_dependents_map),
              Eq(schema_delta));
}

TEST(SchemaUtilTest, NewSchemaMissingPropertyIsIncompatible) {
  // Configure old schema
  SchemaProto old_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop1")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("OldOptional")
                                        .SetDataType(TYPE_INT64)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // Configure new schema, new schema needs to at least have all the
  // previously defined properties
  SchemaProto new_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop1")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.schema_types_incompatible.emplace(kEmailType);
  SchemaUtil::DependentMap no_dependents_map;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema, new_schema,
                                                    no_dependents_map),
              Eq(schema_delta));
}

TEST(SchemaUtilTest, CompatibilityOfDifferentCardinalityOk) {
  // Configure less restrictive schema based on cardinality
  SchemaProto less_restrictive_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataType(TYPE_INT64)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .Build();

  // Configure more restrictive schema based on cardinality
  SchemaProto more_restrictive_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataType(TYPE_INT64)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // We can't have a new schema be more restrictive, REPEATED->OPTIONAL
  SchemaUtil::SchemaDelta incompatible_schema_delta;
  incompatible_schema_delta.schema_types_incompatible.emplace(kEmailType);
  SchemaUtil::DependentMap no_dependents_map;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(
                  /*old_schema=*/less_restrictive_schema,
                  /*new_schema=*/more_restrictive_schema, no_dependents_map),
              Eq(incompatible_schema_delta));

  // We can have the new schema be less restrictive, OPTIONAL->REPEATED;
  SchemaUtil::SchemaDelta compatible_schema_delta;
  compatible_schema_delta.schema_types_changed_fully_compatible.insert(
      kEmailType);
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(
                  /*old_schema=*/more_restrictive_schema,
                  /*new_schema=*/less_restrictive_schema, no_dependents_map),
              Eq(compatible_schema_delta));
}

TEST(SchemaUtilTest, DifferentDataTypeIsIncompatible) {
  // Configure old schema, with an int64_t property
  SchemaProto old_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataType(TYPE_INT64)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .Build();

  // Configure new schema, with a double property
  SchemaProto new_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataType(TYPE_DOUBLE)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .Build();

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.schema_types_incompatible.emplace(kEmailType);
  SchemaUtil::DependentMap no_dependents_map;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema, new_schema,
                                                    no_dependents_map),
              Eq(schema_delta));
}

TEST(SchemaUtilTest, DifferentSchemaTypeIsIncompatible) {
  // Configure old schema, where Property is supposed to be a Person type
  SchemaProto old_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop")
                                        .SetDataType(TYPE_INT64)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kMessageType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop")
                                        .SetDataType(TYPE_INT64)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeDocument(
                                            kPersonType,
                                            /*index_nested_properties=*/true)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .Build();

  // Configure new schema, where Property is supposed to be an Email type
  SchemaProto new_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop")
                                        .SetDataType(TYPE_INT64)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kMessageType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop")
                                        .SetDataType(TYPE_INT64)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeDocument(
                                            kMessageType,
                                            /*index_nested_properties=*/true)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .Build();

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.schema_types_incompatible.emplace(kEmailType);
  // kEmailType depends on kMessageType
  SchemaUtil::DependentMap dependents_map = {{kMessageType, {kEmailType}}};
  SchemaUtil::SchemaDelta actual = SchemaUtil::ComputeCompatibilityDelta(
      old_schema, new_schema, dependents_map);
  EXPECT_THAT(actual, Eq(schema_delta));
  EXPECT_THAT(actual.schema_types_incompatible,
              testing::ElementsAre(kEmailType));
  EXPECT_THAT(actual.schema_types_deleted, testing::IsEmpty());
}

TEST(SchemaUtilTest, ChangingIndexedStringPropertiesMakesIndexIncompatible) {
  // Configure old schema
  SchemaProto schema_with_indexed_property =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // Configure new schema
  SchemaProto schema_with_unindexed_property =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeString(TERM_MATCH_UNKNOWN,
                                                           TOKENIZER_NONE)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.schema_types_index_incompatible.insert(kPersonType);

  // New schema gained a new indexed string property.
  SchemaUtil::DependentMap no_dependents_map;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(
                  schema_with_unindexed_property, schema_with_indexed_property,
                  no_dependents_map),
              Eq(schema_delta));

  // New schema lost an indexed string property.
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(
                  schema_with_indexed_property, schema_with_unindexed_property,
                  no_dependents_map),
              Eq(schema_delta));
}

TEST(SchemaUtilTest, AddingNewIndexedStringPropertyMakesIndexIncompatible) {
  // Configure old schema
  SchemaProto old_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // Configure new schema
  SchemaProto new_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("NewIndexedProperty")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.schema_types_index_incompatible.insert(kPersonType);
  SchemaUtil::DependentMap no_dependents_map;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema, new_schema,
                                                    no_dependents_map),
              Eq(schema_delta));
}

TEST(SchemaUtilTest,
     AddingNewNonIndexedStringPropertyShouldRemainIndexCompatible) {
  // Configure old schema
  SchemaProto old_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // Configure new schema
  SchemaProto new_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("NewProperty")
                                        .SetDataTypeString(TERM_MATCH_UNKNOWN,
                                                           TOKENIZER_NONE)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  SchemaUtil::DependentMap no_dependents_map;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema, new_schema,
                                                    no_dependents_map)
                  .schema_types_index_incompatible,
              IsEmpty());
}

TEST(SchemaUtilTest, ChangingIndexedIntegerPropertiesMakesIndexIncompatible) {
  // Configure old schema
  SchemaProto schema_with_indexed_property =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // Configure new schema
  SchemaProto schema_with_unindexed_property =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeInt64(NUMERIC_MATCH_UNKNOWN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.schema_types_index_incompatible.insert(kPersonType);

  // New schema gained a new indexed integer property.
  SchemaUtil::DependentMap no_dependents_map;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(
                  schema_with_unindexed_property, schema_with_indexed_property,
                  no_dependents_map),
              Eq(schema_delta));

  // New schema lost an indexed integer property.
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(
                  schema_with_indexed_property, schema_with_unindexed_property,
                  no_dependents_map),
              Eq(schema_delta));
}

TEST(SchemaUtilTest, AddingNewIndexedIntegerPropertyMakesIndexIncompatible) {
  // Configure old schema
  SchemaProto old_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // Configure new schema
  SchemaProto new_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("NewIndexedProperty")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.schema_types_index_incompatible.insert(kPersonType);
  SchemaUtil::DependentMap no_dependents_map;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema, new_schema,
                                                    no_dependents_map),
              Eq(schema_delta));
}

TEST(SchemaUtilTest,
     AddingNewNonIndexedIntegerPropertyShouldRemainIndexCompatible) {
  // Configure old schema
  SchemaProto old_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // Configure new schema
  SchemaProto new_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("NewProperty")
                                        .SetDataTypeInt64(NUMERIC_MATCH_UNKNOWN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  SchemaUtil::DependentMap no_dependents_map;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema, new_schema,
                                                    no_dependents_map)
                  .schema_types_index_incompatible,
              IsEmpty());
}

TEST(SchemaUtilTest, ChangingJoinablePropertiesMakesJoinIncompatible) {
  // Configure old schema
  SchemaProto schema_with_joinable_property =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // Configure new schema
  SchemaProto schema_with_non_joinable_property =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_NONE)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  SchemaUtil::SchemaDelta expected_schema_delta;
  expected_schema_delta.schema_types_join_incompatible.insert(kPersonType);

  // New schema gained a new joinable property.
  SchemaUtil::DependentMap no_dependents_map;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(
                  schema_with_non_joinable_property,
                  schema_with_joinable_property, no_dependents_map),
              Eq(expected_schema_delta));

  // New schema lost a joinable property.
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(
                  schema_with_joinable_property,
                  schema_with_non_joinable_property, no_dependents_map),
              Eq(expected_schema_delta));
}

TEST(SchemaUtilTest, AddingNewJoinablePropertyMakesJoinIncompatible) {
  // Configure old schema
  SchemaProto old_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // Configure new schema
  SchemaProto new_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("NewJoinableProperty")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  SchemaUtil::SchemaDelta expected_schema_delta;
  expected_schema_delta.schema_types_join_incompatible.insert(kPersonType);
  SchemaUtil::DependentMap no_dependents_map;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema, new_schema,
                                                    no_dependents_map),
              Eq(expected_schema_delta));
}

TEST(SchemaUtilTest, AddingNewNonJoinablePropertyShouldRemainJoinCompatible) {
  // Configure old schema
  SchemaProto old_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("JoinableProperty")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // Configure new schema
  SchemaProto new_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("JoinableProperty")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("NewProperty")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  SchemaUtil::DependentMap no_dependents_map;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema, new_schema,
                                                    no_dependents_map)
                  .schema_types_join_incompatible,
              IsEmpty());
}

TEST(SchemaUtilTest, AddingTypeIsCompatible) {
  // Can add a new type, existing data isn't incompatible, since none of them
  // are of this new schema type
  SchemaProto old_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  SchemaProto new_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.schema_types_new.insert(kEmailType);
  SchemaUtil::DependentMap no_dependents_map;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema, new_schema,
                                                    no_dependents_map),
              Eq(schema_delta));
}

TEST(SchemaUtilTest, DeletingTypeIsNoted) {
  // Can't remove an old type, new schema needs to at least have all the
  // previously defined schema otherwise the Documents of the missing schema
  // are invalid
  SchemaProto old_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  SchemaProto new_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.schema_types_deleted.emplace(kPersonType);
  SchemaUtil::DependentMap no_dependents_map;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema, new_schema,
                                                    no_dependents_map),
              Eq(schema_delta));
}

TEST(SchemaUtilTest, DeletingPropertyAndChangingProperty) {
  SchemaProto old_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property1")
                                        .SetDataType(TYPE_STRING)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property2")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  // Remove Property2 and make Property1 indexed now. Removing Property2 should
  // be incompatible.
  SchemaProto new_schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kEmailType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("Property1")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.schema_types_incompatible.emplace(kEmailType);
  schema_delta.schema_types_index_incompatible.emplace(kEmailType);
  SchemaUtil::DependentMap no_dependents_map;
  SchemaUtil::SchemaDelta actual = SchemaUtil::ComputeCompatibilityDelta(
      old_schema, new_schema, no_dependents_map);
  EXPECT_THAT(actual, Eq(schema_delta));
}

TEST(SchemaUtilTest, IndexNestedDocumentsIndexIncompatible) {
  // Make two schemas. One that sets index_nested_properties to false and one
  // that sets it to true.
  SchemaTypeConfigProto email_type_config =
      SchemaTypeConfigBuilder()
          .SetType(kEmailType)
          .AddProperty(PropertyConfigBuilder()
                           .SetName("subject")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  SchemaProto no_nested_index_schema =
      SchemaBuilder()
          .AddType(email_type_config)
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("emails")
                                        .SetDataTypeDocument(
                                            kEmailType,
                                            /*index_nested_properties=*/false)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .Build();

  SchemaProto nested_index_schema =
      SchemaBuilder()
          .AddType(email_type_config)
          .AddType(SchemaTypeConfigBuilder()
                       .SetType(kPersonType)
                       .AddProperty(
                           PropertyConfigBuilder()
                               .SetName("emails")
                               .SetDataTypeDocument(
                                   kEmailType, /*index_nested_properties=*/true)
                               .SetCardinality(CARDINALITY_REPEATED)))
          .Build();

  // Going from index_nested_properties=false to index_nested_properties=true
  // should make kPersonType index_incompatible. kEmailType should be
  // unaffected.
  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.schema_types_index_incompatible.emplace(kPersonType);
  SchemaUtil::DependentMap dependents_map = {{kEmailType, {kPersonType}}};
  SchemaUtil::SchemaDelta actual = SchemaUtil::ComputeCompatibilityDelta(
      no_nested_index_schema, nested_index_schema, dependents_map);
  EXPECT_THAT(actual, Eq(schema_delta));

  // Going from index_nested_properties=true to index_nested_properties=false
  // should also make kPersonType index_incompatible. kEmailType should be
  // unaffected.
  actual = SchemaUtil::ComputeCompatibilityDelta(
      nested_index_schema, no_nested_index_schema, dependents_map);
  EXPECT_THAT(actual, Eq(schema_delta));
}

TEST(SchemaUtilTest, ValidateStringIndexingConfigShouldHaveTermMatchType) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("MyType").AddProperty(
              PropertyConfigBuilder()
                  .SetName("Foo")
                  .SetDataTypeString(TERM_MATCH_UNKNOWN, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  // Error if we don't set a term match type
  EXPECT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  // Passes once we set a term match type
  schema = SchemaBuilder()
               .AddType(SchemaTypeConfigBuilder().SetType("MyType").AddProperty(
                   PropertyConfigBuilder()
                       .SetName("Foo")
                       .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                       .SetCardinality(CARDINALITY_REQUIRED)))
               .Build();
  EXPECT_THAT(SchemaUtil::Validate(schema), IsOk());
}

TEST(SchemaUtilTest, ValidateStringIndexingConfigShouldHaveTokenizer) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("MyType").AddProperty(
              PropertyConfigBuilder()
                  .SetName("Foo")
                  .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_NONE)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  // Error if we don't set a tokenizer type
  EXPECT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  // Passes once we set a tokenizer type
  schema = SchemaBuilder()
               .AddType(SchemaTypeConfigBuilder().SetType("MyType").AddProperty(
                   PropertyConfigBuilder()
                       .SetName("Foo")
                       .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                       .SetCardinality(CARDINALITY_REQUIRED)))
               .Build();
  EXPECT_THAT(SchemaUtil::Validate(schema), IsOk());
}

TEST(SchemaUtilTest, MultipleReferencesToSameNestedSchemaOk) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("InnerSchema"))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("OuterSchema")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("InnerProperty1")
                                        .SetDataTypeDocument(
                                            "InnerSchema",
                                            /*index_nested_properties=*/true)
                                        .SetCardinality(CARDINALITY_REPEATED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("InnerProperty2")
                                        .SetDataTypeDocument(
                                            "InnerSchema",
                                            /*index_nested_properties=*/true)
                                        .SetCardinality(CARDINALITY_REPEATED)))
          .Build();

  EXPECT_THAT(SchemaUtil::Validate(schema), IsOk());
}

TEST(SchemaUtilTest, InvalidSelfReference) {
  // Create a schema with a self-reference cycle in it: OwnSchema -> OwnSchema
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("OwnSchema")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("NestedDocument")
                                        .SetDataTypeDocument(
                                            "OwnSchema",
                                            /*index_nested_properties=*/true)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  EXPECT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("Infinite loop")));
}

TEST(SchemaUtilTest, InvalidSelfReferenceEvenWithOtherProperties) {
  // Create a schema with a self-reference cycle in it: OwnSchema -> OwnSchema
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("OwnSchema")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("NestedDocument")
                                        .SetDataTypeDocument(
                                            "OwnSchema",
                                            /*index_nested_properties=*/true)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("SomeString")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  EXPECT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("Infinite loop")));
}

TEST(SchemaUtilTest, InvalidInfiniteLoopTwoDegrees) {
  // Create a schema for the outer schema
  SchemaProto schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("A")
                  // Reference schema B, so far so good
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("NestedDocument")
                                   .SetDataTypeDocument(
                                       "B", /*index_nested_properties=*/true)
                                   .SetCardinality(CARDINALITY_OPTIONAL)))
          // Create the inner schema
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("B")
                  // Reference the schema A, causing an infinite loop of
                  // references.
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("NestedDocument")
                                   .SetDataTypeDocument(
                                       "A", /*index_nested_properties=*/true)
                                   .SetCardinality(CARDINALITY_REPEATED)))
          .Build();

  // Two degrees of referencing: A -> B -> A
  EXPECT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("Infinite loop")));
}

TEST(SchemaUtilTest, InvalidInfiniteLoopThreeDegrees) {
  SchemaProto schema =
      SchemaBuilder()
          // Create a schema for the outer schema
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("A")
                  // Reference schema B, so far so good
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("NestedDocument")
                                   .SetDataTypeDocument(
                                       "B", /*index_nested_properties=*/true)
                                   .SetCardinality(CARDINALITY_OPTIONAL)))
          // Create the inner schema
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("B")
                  // Reference schema C, so far so good
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("NestedDocument")
                                   .SetDataTypeDocument(
                                       "C", /*index_nested_properties=*/true)
                                   .SetCardinality(CARDINALITY_REPEATED)))
          // Create the inner schema
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("C")
                  // Reference schema C, so far so good
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("NestedDocument")
                                   .SetDataTypeDocument(
                                       "A", /*index_nested_properties=*/true)
                                   .SetCardinality(CARDINALITY_REPEATED)))
          .Build();

  // Three degrees of referencing: A -> B -> C -> A
  EXPECT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("Infinite loop")));
}

}  // namespace

}  // namespace lib
}  // namespace icing
