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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {
namespace {

using ::testing::Eq;

// Properties/fields in a schema type
constexpr char kEmailType[] = "EmailMessage";
constexpr char kPersonType[] = "Person";

class SchemaUtilTest : public ::testing::Test {
 protected:
  SchemaProto schema_proto_;

  static SchemaTypeConfigProto CreateSchemaTypeConfig(
      const std::string_view schema_type,
      const std::string_view nested_schema_type = "") {
    SchemaTypeConfigProto type;
    type.set_schema_type(std::string(schema_type));

    auto string_property = type.add_properties();
    string_property->set_property_name("string");
    string_property->set_data_type(PropertyConfigProto::DataType::STRING);
    string_property->set_cardinality(
        PropertyConfigProto::Cardinality::REQUIRED);

    auto int_property = type.add_properties();
    int_property->set_property_name("int");
    int_property->set_data_type(PropertyConfigProto::DataType::INT64);
    int_property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

    auto double_property = type.add_properties();
    double_property->set_property_name("double");
    double_property->set_data_type(PropertyConfigProto::DataType::DOUBLE);
    double_property->set_cardinality(
        PropertyConfigProto::Cardinality::REPEATED);

    auto bool_property = type.add_properties();
    bool_property->set_property_name("boolean");
    bool_property->set_data_type(PropertyConfigProto::DataType::BOOLEAN);
    bool_property->set_cardinality(PropertyConfigProto::Cardinality::REPEATED);

    auto bytes_property = type.add_properties();
    bytes_property->set_property_name("bytes");
    bytes_property->set_data_type(PropertyConfigProto::DataType::BYTES);
    bytes_property->set_cardinality(PropertyConfigProto::Cardinality::REPEATED);

    if (!nested_schema_type.empty()) {
      auto document_property = type.add_properties();
      document_property->set_property_name("document");
      document_property->set_data_type(PropertyConfigProto::DataType::DOCUMENT);
      document_property->set_cardinality(
          PropertyConfigProto::Cardinality::REPEATED);
      document_property->set_schema_type(std::string(nested_schema_type));
    }

    return type;
  }
};

TEST_F(SchemaUtilTest, Valid_Empty) {
  ICING_ASSERT_OK(SchemaUtil::Validate(schema_proto_));
}

TEST_F(SchemaUtilTest, Valid_Nested) {
  auto email_type = schema_proto_.add_types();
  *email_type = CreateSchemaTypeConfig(kEmailType, kPersonType);

  auto person_type = schema_proto_.add_types();
  *person_type = CreateSchemaTypeConfig(kPersonType);

  ICING_ASSERT_OK(SchemaUtil::Validate(schema_proto_));
}

TEST_F(SchemaUtilTest, Valid_ClearedPropertyConfigs) {
  // No property fields is technically ok, but probably not realistic.
  auto type = schema_proto_.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);
  type->clear_properties();

  ICING_ASSERT_OK(SchemaUtil::Validate(schema_proto_));
}

TEST_F(SchemaUtilTest, Invalid_ClearedSchemaType) {
  auto type = schema_proto_.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);
  type->clear_schema_type();

  ASSERT_THAT(SchemaUtil::Validate(schema_proto_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(SchemaUtilTest, Invalid_EmptySchemaType) {
  auto type = schema_proto_.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);
  type->set_schema_type("");

  ASSERT_THAT(SchemaUtil::Validate(schema_proto_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(SchemaUtilTest, AnySchemaTypeOk) {
  auto type = schema_proto_.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);
  type->set_schema_type("abc123!@#$%^&*()_-+=[{]}|\\;:'\",<.>?你好");

  ICING_ASSERT_OK(SchemaUtil::Validate(schema_proto_));
}

TEST_F(SchemaUtilTest, Invalid_ClearedPropertyName) {
  auto type = schema_proto_.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  auto property = type->add_properties();
  property->clear_property_name();
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);

  ASSERT_THAT(SchemaUtil::Validate(schema_proto_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(SchemaUtilTest, Invalid_EmptyPropertyName) {
  auto type = schema_proto_.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  auto property = type->add_properties();
  property->set_property_name("");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);

  ASSERT_THAT(SchemaUtil::Validate(schema_proto_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(SchemaUtilTest, NonAlphanumericPropertyNameIsInvalid) {
  auto type = schema_proto_.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  auto property = type->add_properties();
  property->set_property_name("_");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);

  ASSERT_THAT(SchemaUtil::Validate(schema_proto_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(SchemaUtilTest, AlphanumericPropertyNameOk) {
  auto type = schema_proto_.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  auto property = type->add_properties();
  property->set_property_name("abc123");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);

  ICING_ASSERT_OK(SchemaUtil::Validate(schema_proto_));
}

TEST_F(SchemaUtilTest, Invalid_DuplicatePropertyName) {
  auto type = schema_proto_.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  auto first_property = type->add_properties();
  first_property->set_property_name("DuplicatedProperty");
  first_property->set_data_type(PropertyConfigProto::DataType::STRING);
  first_property->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);

  auto second_property = type->add_properties();
  second_property->set_property_name("DuplicatedProperty");
  second_property->set_data_type(PropertyConfigProto::DataType::STRING);
  second_property->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);

  ASSERT_THAT(SchemaUtil::Validate(schema_proto_),
              StatusIs(libtextclassifier3::StatusCode::ALREADY_EXISTS));
}

TEST_F(SchemaUtilTest, Invalid_ClearedDataType) {
  auto type = schema_proto_.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  auto property = type->add_properties();
  property->set_property_name("NewProperty");
  property->clear_data_type();
  property->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);

  ASSERT_THAT(SchemaUtil::Validate(schema_proto_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(SchemaUtilTest, Invalid_UnknownDataType) {
  auto type = schema_proto_.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  auto property = type->add_properties();
  property->set_property_name("NewProperty");
  property->set_data_type(PropertyConfigProto::DataType::UNKNOWN);
  property->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);

  ASSERT_THAT(SchemaUtil::Validate(schema_proto_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(SchemaUtilTest, Invalid_ClearedCardinality) {
  auto type = schema_proto_.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  auto property = type->add_properties();
  property->set_property_name("NewProperty");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->clear_cardinality();

  ASSERT_THAT(SchemaUtil::Validate(schema_proto_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(SchemaUtilTest, Invalid_UnknownCardinality) {
  auto type = schema_proto_.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  auto property = type->add_properties();
  property->set_property_name("NewProperty");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::UNKNOWN);

  ASSERT_THAT(SchemaUtil::Validate(schema_proto_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(SchemaUtilTest, Invalid_ClearedPropertySchemaType) {
  auto type = schema_proto_.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  auto property = type->add_properties();
  property->set_property_name("NewProperty");
  property->set_data_type(PropertyConfigProto::DataType::DOCUMENT);
  property->set_cardinality(PropertyConfigProto::Cardinality::REPEATED);
  property->clear_schema_type();

  ASSERT_THAT(SchemaUtil::Validate(schema_proto_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(SchemaUtilTest, Invalid_EmptyPropertySchemaType) {
  auto type = schema_proto_.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  auto property = type->add_properties();
  property->set_property_name("NewProperty");
  property->set_data_type(PropertyConfigProto::DataType::DOCUMENT);
  property->set_cardinality(PropertyConfigProto::Cardinality::REPEATED);
  property->set_schema_type("");

  ASSERT_THAT(SchemaUtil::Validate(schema_proto_),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(SchemaUtilTest, Invalid_NoMatchingSchemaType) {
  auto type = schema_proto_.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  auto property = type->add_properties();
  property->set_property_name("NewProperty");
  property->set_data_type(PropertyConfigProto::DataType::DOCUMENT);
  property->set_cardinality(PropertyConfigProto::Cardinality::REPEATED);
  property->set_schema_type("NewSchemaType");

  ASSERT_THAT(SchemaUtil::Validate(schema_proto_),
              StatusIs(libtextclassifier3::StatusCode::UNKNOWN));
}

TEST_F(SchemaUtilTest, NewOptionalPropertyIsCompatible) {
  // Configure old schema
  SchemaProto old_schema;
  auto type = old_schema.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  // Configure new schema with an optional field, not considered incompatible
  // since it's fine if old data doesn't have this optional field
  SchemaProto new_schema_with_optional;
  type = new_schema_with_optional.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  auto property = type->add_properties();
  property->set_property_name("NewOptional");
  property->set_data_type(PropertyConfigProto::DataType::DOUBLE);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  SchemaUtil::SchemaDelta schema_delta;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema,
                                                    new_schema_with_optional),
              Eq(schema_delta));
}

TEST_F(SchemaUtilTest, NewRequiredPropertyIsIncompatible) {
  // Configure old schema
  SchemaProto old_schema;
  auto type = old_schema.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  // Configure new schema with a required field, considered incompatible since
  // old data won't have this required field
  SchemaProto new_schema_with_required;
  type = new_schema_with_required.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  auto property = type->add_properties();
  property->set_property_name("NewRequired");
  property->set_data_type(PropertyConfigProto::DataType::DOUBLE);
  property->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.schema_types_incompatible.emplace(kEmailType);
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema,
                                                    new_schema_with_required),
              Eq(schema_delta));
}

TEST_F(SchemaUtilTest, NewSchemaMissingPropertyIsIncompatible) {
  // Configure old schema
  SchemaProto old_schema;
  auto type = old_schema.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  auto property = type->add_properties();
  property->set_property_name("OldOptional");
  property->set_data_type(PropertyConfigProto::DataType::INT64);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  // Configure new schema, new schema needs to at least have all the
  // previously defined properties
  SchemaProto new_schema;
  type = new_schema.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.schema_types_incompatible.emplace(kEmailType);
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema, new_schema),
              Eq(schema_delta));
}

TEST_F(SchemaUtilTest, CompatibilityOfDifferentCardinalityOk) {
  // Configure less restrictive schema based on cardinality
  SchemaProto less_restrictive_schema;
  auto type = less_restrictive_schema.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  auto property = type->add_properties();
  property->set_property_name("Property");
  property->set_data_type(PropertyConfigProto::DataType::INT64);
  property->set_cardinality(PropertyConfigProto::Cardinality::REPEATED);

  // Configure more restrictive schema based on cardinality
  SchemaProto more_restrictive_schema;
  type = more_restrictive_schema.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  property = type->add_properties();
  property->set_property_name("Property");
  property->set_data_type(PropertyConfigProto::DataType::INT64);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  // We can't have a new schema be less restrictive, REQUIRED->OPTIONAL
  SchemaUtil::SchemaDelta incompatible_schema_delta;
  incompatible_schema_delta.schema_types_incompatible.emplace(kEmailType);
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(
                  /*old_schema=*/less_restrictive_schema,
                  /*new_schema=*/more_restrictive_schema),
              Eq(incompatible_schema_delta));

  // We can have the new schema be more restrictive, OPTIONAL->REPEATED;
  SchemaUtil::SchemaDelta compatible_schema_delta;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(
                  /*old_schema=*/more_restrictive_schema,
                  /*new_schema=*/less_restrictive_schema),
              Eq(compatible_schema_delta));
}

TEST_F(SchemaUtilTest, DifferentDataTypeIsIncompatible) {
  // Configure old schema, with an int64_t property
  SchemaProto old_schema;
  auto type = old_schema.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  auto property = type->add_properties();
  property->set_property_name("Property");
  property->set_data_type(PropertyConfigProto::DataType::INT64);
  property->set_cardinality(PropertyConfigProto::Cardinality::REPEATED);

  // Configure new schema, with a double property
  SchemaProto new_schema;
  type = new_schema.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  property = type->add_properties();
  property->set_property_name("Property");
  property->set_data_type(PropertyConfigProto::DataType::DOUBLE);
  property->set_cardinality(PropertyConfigProto::Cardinality::REPEATED);

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.schema_types_incompatible.emplace(kEmailType);
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema, new_schema),
              Eq(schema_delta));
}

TEST_F(SchemaUtilTest, DifferentSchemaTypeIsIncompatible) {
  // Configure old schema, where Property is supposed to be a Person type
  SchemaProto old_schema;
  auto type = old_schema.add_types();
  *type = CreateSchemaTypeConfig(kPersonType);

  *type = CreateSchemaTypeConfig(kEmailType);
  auto property = type->add_properties();
  property->set_property_name("Property");
  property->set_data_type(PropertyConfigProto::DataType::DOCUMENT);
  property->set_cardinality(PropertyConfigProto::Cardinality::REPEATED);
  property->set_schema_type(kPersonType);

  // Configure new schema, where Property is supposed to be an Email type
  SchemaProto new_schema;
  type = new_schema.add_types();
  *type = CreateSchemaTypeConfig(kPersonType);

  *type = CreateSchemaTypeConfig(kEmailType);
  property = type->add_properties();
  property->set_property_name("Property");
  property->set_data_type(PropertyConfigProto::DataType::DOCUMENT);
  property->set_cardinality(PropertyConfigProto::Cardinality::REPEATED);
  property->set_schema_type(kEmailType);

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.schema_types_incompatible.emplace(kEmailType);
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema, new_schema),
              Eq(schema_delta));
}

TEST_F(SchemaUtilTest, ChangingIndexedPropertiesMakesIndexIncompatible) {
  // Configure old schema
  SchemaProto old_schema;
  auto old_type = old_schema.add_types();
  *old_type = CreateSchemaTypeConfig(kEmailType, kPersonType);

  auto old_property = old_type->add_properties();
  old_property->set_property_name("Property");
  old_property->set_data_type(PropertyConfigProto::DataType::STRING);
  old_property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  // Configure new schema
  SchemaProto new_schema;
  auto new_type = new_schema.add_types();
  *new_type = CreateSchemaTypeConfig(kEmailType, kPersonType);

  auto new_property = new_type->add_properties();
  new_property->set_property_name("Property");
  new_property->set_data_type(PropertyConfigProto::DataType::STRING);
  new_property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.index_incompatible = true;

  // New schema gained a new indexed property.
  old_property->mutable_string_indexing_config()->set_term_match_type(
      TermMatchType::UNKNOWN);
  new_property->mutable_string_indexing_config()->set_term_match_type(
      TermMatchType::EXACT_ONLY);
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema, new_schema),
              Eq(schema_delta));

  // New schema lost an indexed property.
  old_property->mutable_string_indexing_config()->set_term_match_type(
      TermMatchType::EXACT_ONLY);
  new_property->mutable_string_indexing_config()->set_term_match_type(
      TermMatchType::UNKNOWN);
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema, new_schema),
              Eq(schema_delta));
}

TEST_F(SchemaUtilTest, AddingNewIndexedPropertyMakesIndexIncompatible) {
  // Configure old schema
  SchemaProto old_schema;
  auto old_type = old_schema.add_types();
  *old_type = CreateSchemaTypeConfig(kEmailType, kPersonType);

  auto old_property = old_type->add_properties();
  old_property->set_property_name("Property");
  old_property->set_data_type(PropertyConfigProto::DataType::STRING);
  old_property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  // Configure new schema
  SchemaProto new_schema;
  auto new_type = new_schema.add_types();
  *new_type = CreateSchemaTypeConfig(kEmailType, kPersonType);

  auto new_property = new_type->add_properties();
  new_property->set_property_name("Property");
  new_property->set_data_type(PropertyConfigProto::DataType::STRING);
  new_property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  new_property = new_type->add_properties();
  new_property->set_property_name("NewIndexedProperty");
  new_property->set_data_type(PropertyConfigProto::DataType::STRING);
  new_property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
  new_property->mutable_string_indexing_config()->set_term_match_type(
      TermMatchType::EXACT_ONLY);

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.index_incompatible = true;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema, new_schema),
              Eq(schema_delta));
}

TEST_F(SchemaUtilTest, AddingTypeIsCompatible) {
  // Can add a new type, existing data isn't incompatible, since none of them
  // are of this new schema type
  SchemaProto old_schema;
  auto type = old_schema.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  SchemaProto new_schema;
  type = new_schema.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);
  type = new_schema.add_types();
  *type = CreateSchemaTypeConfig(kPersonType);

  SchemaUtil::SchemaDelta schema_delta;
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema, new_schema),
              Eq(schema_delta));
}

TEST_F(SchemaUtilTest, DeletingTypeIsNoted) {
  // Can't remove an old type, new schema needs to at least have all the
  // previously defined schema otherwise the Documents of the missing schema
  // are invalid
  SchemaProto old_schema;
  auto type = old_schema.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);
  type = old_schema.add_types();
  *type = CreateSchemaTypeConfig(kPersonType);

  SchemaProto new_schema;
  type = new_schema.add_types();
  *type = CreateSchemaTypeConfig(kEmailType);

  SchemaUtil::SchemaDelta schema_delta;
  schema_delta.schema_types_deleted.emplace(kPersonType);
  EXPECT_THAT(SchemaUtil::ComputeCompatibilityDelta(old_schema, new_schema),
              Eq(schema_delta));
}

TEST_F(SchemaUtilTest, ValidateStringIndexingConfigShouldHaveTermMatchType) {
  SchemaProto schema;
  auto* type = schema.add_types();
  type->set_schema_type("MyType");

  auto* prop = type->add_properties();
  prop->set_property_name("Foo");
  prop->set_data_type(PropertyConfigProto::DataType::STRING);
  prop->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);
  prop->mutable_string_indexing_config()->set_tokenizer_type(
      StringIndexingConfig::TokenizerType::PLAIN);

  // Error if we don't set a term match type
  EXPECT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  // Passes once we set a term match type
  prop->mutable_string_indexing_config()->set_term_match_type(
      TermMatchType::EXACT_ONLY);
  EXPECT_THAT(SchemaUtil::Validate(schema), IsOk());
}

TEST_F(SchemaUtilTest, ValidateStringIndexingConfigShouldHaveTokenizer) {
  SchemaProto schema;
  auto* type = schema.add_types();
  type->set_schema_type("MyType");

  auto* prop = type->add_properties();
  prop->set_property_name("Foo");
  prop->set_data_type(PropertyConfigProto::DataType::STRING);
  prop->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);
  prop->mutable_string_indexing_config()->set_term_match_type(
      TermMatchType::EXACT_ONLY);

  // Error if we don't set a tokenizer type
  EXPECT_THAT(SchemaUtil::Validate(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  // Passes once we set a tokenizer type
  prop->mutable_string_indexing_config()->set_tokenizer_type(
      StringIndexingConfig::TokenizerType::PLAIN);
  EXPECT_THAT(SchemaUtil::Validate(schema), IsOk());
}

}  // namespace

}  // namespace lib
}  // namespace icing
