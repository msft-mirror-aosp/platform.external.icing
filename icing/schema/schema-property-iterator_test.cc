// Copyright (C) 2023 Google LLC
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

#include "icing/schema/schema-property-iterator.h"

#include <string>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/portable/equals-proto.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-util.h"
#include "icing/testing/common-matchers.h"

namespace icing {
namespace lib {

namespace {

using portable_equals_proto::EqualsProto;
using ::testing::Eq;
using ::testing::IsFalse;
using ::testing::IsTrue;

TEST(SchemaPropertyIteratorTest,
     SingleLevelSchemaTypeConfigShouldIterateInCorrectOrder) {
  std::string schema_type_name = "Schema";

  SchemaTypeConfigProto schema_type_config =
      SchemaTypeConfigBuilder()
          .SetType(schema_type_name)
          .AddProperty(PropertyConfigBuilder().SetName("Google").SetDataType(
              TYPE_STRING))
          .AddProperty(PropertyConfigBuilder().SetName("Youtube").SetDataType(
              TYPE_BYTES))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("Alphabet")
                           .SetDataType(TYPE_INT64))
          .Build();
  SchemaUtil::TypeConfigMap type_config_map = {
      {schema_type_name, schema_type_config}};

  SchemaPropertyIterator iterator(schema_type_config, type_config_map);
  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("Alphabet"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config.properties(2)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsTrue());

  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("Google"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config.properties(0)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsTrue());

  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("Youtube"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config.properties(1)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsTrue());

  EXPECT_THAT(iterator.Advance(),
              StatusIs(libtextclassifier3::StatusCode::OUT_OF_RANGE));
}

TEST(SchemaPropertyIteratorTest,
     NestedSchemaTypeConfigShouldIterateInCorrectOrder) {
  std::string schema_type_name1 = "SchemaOne";
  std::string schema_type_name2 = "SchemaTwo";
  std::string schema_type_name3 = "SchemaThree";

  SchemaTypeConfigProto schema_type_config1 =
      SchemaTypeConfigBuilder()
          .SetType(schema_type_name1)
          .AddProperty(PropertyConfigBuilder().SetName("Google").SetDataType(
              TYPE_STRING))
          .AddProperty(PropertyConfigBuilder().SetName("Youtube").SetDataType(
              TYPE_BYTES))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("Alphabet")
                           .SetDataType(TYPE_INT64))
          .Build();
  SchemaTypeConfigProto schema_type_config2 =
      SchemaTypeConfigBuilder()
          .SetType(schema_type_name2)
          .AddProperty(
              PropertyConfigBuilder().SetName("Foo").SetDataType(TYPE_STRING))
          .AddProperty(
              PropertyConfigBuilder().SetName("Bar").SetDataTypeDocument(
                  schema_type_name1, /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto schema_type_config3 =
      SchemaTypeConfigBuilder()
          .SetType(schema_type_name3)
          .AddProperty(
              PropertyConfigBuilder().SetName("Hello").SetDataType(TYPE_STRING))
          .AddProperty(
              PropertyConfigBuilder().SetName("World").SetDataTypeDocument(
                  schema_type_name1, /*index_nested_properties=*/true))
          .AddProperty(
              PropertyConfigBuilder().SetName("Icing").SetDataTypeDocument(
                  schema_type_name2, /*index_nested_properties=*/true))
          .Build();
  SchemaUtil::TypeConfigMap type_config_map = {
      {schema_type_name1, schema_type_config1},
      {schema_type_name2, schema_type_config2},
      {schema_type_name3, schema_type_config3}};

  // SchemaThree: {
  //   "Hello": TYPE_STRING,
  //   "World": TYPE_DOCUMENT SchemaOne {
  //     "Google": TYPE_STRING,
  //     "Youtube": TYPE_BYTES,
  //     "Alphabet": TYPE_INT64,
  //   },
  //   "Icing": TYPE_DOCUMENT SchemaTwo {
  //     "Foo": TYPE_STRING,
  //     "Bar": TYPE_DOCUMENT SchemaOne {
  //       "Google": TYPE_STRING,
  //       "Youtube": TYPE_BYTES,
  //       "Alphabet": TYPE_INT64,
  //     },
  //   },
  // }
  SchemaPropertyIterator iterator(schema_type_config3, type_config_map);
  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("Hello"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config3.properties(0)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsTrue());

  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("Icing.Bar.Alphabet"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config1.properties(2)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsTrue());

  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("Icing.Bar.Google"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config1.properties(0)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsTrue());

  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("Icing.Bar.Youtube"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config1.properties(1)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsTrue());

  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("Icing.Foo"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config2.properties(0)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsTrue());

  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("World.Alphabet"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config1.properties(2)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsTrue());

  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("World.Google"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config1.properties(0)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsTrue());

  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("World.Youtube"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config1.properties(1)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsTrue());

  EXPECT_THAT(iterator.Advance(),
              StatusIs(libtextclassifier3::StatusCode::OUT_OF_RANGE));
}

TEST(SchemaPropertyIteratorTest,
     NonExistingNestedSchemaTypeConfigShouldGetNotFoundError) {
  std::string schema_type_name1 = "SchemaOne";
  std::string schema_type_name2 = "SchemaTwo";

  SchemaTypeConfigProto schema_type_config1 =
      SchemaTypeConfigBuilder()
          .SetType(schema_type_name1)
          .AddProperty(PropertyConfigBuilder().SetName("Google").SetDataType(
              TYPE_STRING))
          .AddProperty(PropertyConfigBuilder().SetName("Youtube").SetDataType(
              TYPE_BYTES))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("Alphabet")
                           .SetDataType(TYPE_INT64))
          .Build();
  SchemaTypeConfigProto schema_type_config2 =
      SchemaTypeConfigBuilder()
          .SetType(schema_type_name2)
          .AddProperty(
              PropertyConfigBuilder().SetName("Foo").SetDataTypeDocument(
                  schema_type_name1, /*index_nested_properties=*/true))
          .Build();
  // Remove the second level (schema_type_config1) from type_config_map.
  SchemaUtil::TypeConfigMap type_config_map = {
      {schema_type_name2, schema_type_config2}};

  SchemaPropertyIterator iterator(schema_type_config2, type_config_map);
  // Since Foo is a document type property with schema type = "SchemaOne" and
  // "SchemaOne" is not in type_config_map, Advance() should return NOT_FOUND
  // error.
  EXPECT_THAT(iterator.Advance(),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST(SchemaPropertyIteratorTest,
     SchemaTypeConfigWithEmptyPropertyShouldGetOutOfRangeErrorAtFirstAdvance) {
  std::string schema_type_name = "Schema";

  SchemaTypeConfigProto schema_type_config =
      SchemaTypeConfigBuilder().SetType(schema_type_name).Build();
  SchemaUtil::TypeConfigMap type_config_map = {
      {schema_type_name, schema_type_config}};

  SchemaPropertyIterator iterator(schema_type_config, type_config_map);
  EXPECT_THAT(iterator.Advance(),
              StatusIs(libtextclassifier3::StatusCode::OUT_OF_RANGE));
}

TEST(SchemaPropertyIteratorTest,
     SchemaTypeConfigWithCycleDependencyShouldGetInvalidArgumentError) {
  std::string schema_type_name1 = "SchemaOne";
  std::string schema_type_name2 = "SchemaTwo";

  SchemaTypeConfigProto schema_type_config1 =
      SchemaTypeConfigBuilder()
          .SetType(schema_type_name1)
          .AddProperty(
              PropertyConfigBuilder().SetName("Foo").SetDataTypeDocument(
                  schema_type_name2, /*index_nested_properties=*/true))
          .Build();
  SchemaTypeConfigProto schema_type_config2 =
      SchemaTypeConfigBuilder()
          .SetType(schema_type_name2)
          .AddProperty(
              PropertyConfigBuilder().SetName("Bar").SetDataTypeDocument(
                  schema_type_name1, /*index_nested_properties=*/true))
          .Build();
  SchemaUtil::TypeConfigMap type_config_map = {
      {schema_type_name1, schema_type_config1},
      {schema_type_name2, schema_type_config2}};

  SchemaPropertyIterator iterator(schema_type_config1, type_config_map);
  EXPECT_THAT(iterator.Advance(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(SchemaPropertyIteratorTest,
     SchemaTypeConfigWithSelfDependencyShouldGetInvalidArgumentError) {
  std::string schema_type_name = "SchemaOne";

  SchemaTypeConfigProto schema_type_config =
      SchemaTypeConfigBuilder()
          .SetType(schema_type_name)
          .AddProperty(
              PropertyConfigBuilder().SetName("Foo").SetDataTypeDocument(
                  schema_type_name, /*index_nested_properties=*/true))
          .Build();
  SchemaUtil::TypeConfigMap type_config_map = {
      {schema_type_name, schema_type_config}};

  SchemaPropertyIterator iterator(schema_type_config, type_config_map);
  EXPECT_THAT(iterator.Advance(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST(SchemaPropertyIteratorTest, NestedIndexable) {
  std::string schema_type_name1 = "SchemaOne";
  std::string schema_type_name2 = "SchemaTwo";
  std::string schema_type_name3 = "SchemaThree";
  std::string schema_type_name4 = "SchemaFour";

  SchemaTypeConfigProto schema_type_config1 =
      SchemaTypeConfigBuilder()
          .SetType(schema_type_name1)
          .AddProperty(
              PropertyConfigBuilder().SetName("Google").SetDataTypeString(
                  TERM_MATCH_EXACT, TOKENIZER_PLAIN))
          .Build();
  SchemaTypeConfigProto schema_type_config2 =
      SchemaTypeConfigBuilder()
          .SetType(schema_type_name2)
          .AddProperty(
              PropertyConfigBuilder().SetName("Bar").SetDataTypeDocument(
                  schema_type_name1, /*index_nested_properties=*/true))
          .AddProperty(PropertyConfigBuilder().SetName("Foo").SetDataTypeString(
              TERM_MATCH_EXACT, TOKENIZER_PLAIN))
          .Build();
  SchemaTypeConfigProto schema_type_config3 =
      SchemaTypeConfigBuilder()
          .SetType(schema_type_name3)
          .AddProperty(
              PropertyConfigBuilder().SetName("Bar").SetDataTypeDocument(
                  schema_type_name1,
                  /*index_nested_properties=*/false))
          .AddProperty(PropertyConfigBuilder().SetName("Foo").SetDataTypeString(
              TERM_MATCH_EXACT, TOKENIZER_PLAIN))
          .Build();
  SchemaTypeConfigProto schema_type_config4 =
      SchemaTypeConfigBuilder()
          .SetType(schema_type_name4)
          .AddProperty(
              PropertyConfigBuilder().SetName("Baz1").SetDataTypeDocument(
                  schema_type_name2, /*index_nested_properties=*/true))
          .AddProperty(
              PropertyConfigBuilder().SetName("Baz2").SetDataTypeDocument(
                  schema_type_name2, /*index_nested_properties=*/false))
          .AddProperty(
              PropertyConfigBuilder().SetName("Baz3").SetDataTypeDocument(
                  schema_type_name3, /*index_nested_properties=*/true))
          .AddProperty(
              PropertyConfigBuilder().SetName("Baz4").SetDataTypeDocument(
                  schema_type_name3, /*index_nested_properties=*/false))
          .AddProperty(
              PropertyConfigBuilder().SetName("Hello1").SetDataTypeDocument(
                  schema_type_name1, /*index_nested_properties=*/true))
          .AddProperty(
              PropertyConfigBuilder().SetName("Hello2").SetDataTypeDocument(
                  schema_type_name1, /*index_nested_properties=*/false))
          .AddProperty(
              PropertyConfigBuilder().SetName("World").SetDataTypeString(
                  TERM_MATCH_EXACT, TOKENIZER_PLAIN))
          .Build();
  SchemaUtil::TypeConfigMap type_config_map = {
      {schema_type_name1, schema_type_config1},
      {schema_type_name2, schema_type_config2},
      {schema_type_name3, schema_type_config3},
      {schema_type_name4, schema_type_config4}};

  // SchemaFour: {
  //   "Baz1": TYPE_DOCUMENT INDEX_NESTED_PROPERTIES=true SchemaTwo {
  //     "Bar": TYPE_DOCUMENT INDEX_NESTED_PROPERTIES=true SchemaOne {
  //       "Google": TYPE_STRING INDEXABLE,
  //     },
  //     "Foo": TYPE_STRING INDEXABLE,
  //   },
  //   "Baz2": TYPE_DOCUMENT INDEX_NESTED_PROPERTIES=false SchemaTwo {
  //     "Bar": TYPE_DOCUMENT INDEX_NESTED_PROPERTIES=true SchemaOne {
  //       "Google": TYPE_STRING INDEXABLE,
  //     },
  //     "Foo": TYPE_STRING INDEXABLE,
  //   },
  //   "Baz3": TYPE_DOCUMENT INDEX_NESTED_PROPERTIES=true SchemaThree {
  //     "Bar": TYPE_DOCUMENT INDEX_NESTED_PROPERTIES=false SchemaOne {
  //       "Google": TYPE_STRING INDEXABLE,
  //     },
  //     "Foo": TYPE_STRING INDEXABLE,
  //   },
  //   "Baz4": TYPE_DOCUMENT INDEX_NESTED_PROPERTIES=false SchemaThree {
  //     "Bar": TYPE_DOCUMENT INDEX_NESTED_PROPERTIES=false SchemaOne {
  //       "Google": TYPE_STRING INDEXABLE,
  //     },
  //     "Foo": TYPE_STRING INDEXABLE,
  //   },
  //   "Hello": TYPE_DOCUMENT INDEX_NESTED_PROPERTIES=false SchemaOne {
  //     "Google": TYPE_STRING INDEXABLE,
  //   },
  //   "World": TYPE_STRING INDEXABLE,
  // }
  SchemaPropertyIterator iterator(schema_type_config4, type_config_map);

  // Baz1 to Baz4: 2 levels of nested document type property.
  // For Baz1, all levels set index_nested_properties = true, so all leaf
  // properties should be nested indexable.
  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("Baz1.Bar.Google"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config1.properties(0)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsTrue());

  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("Baz1.Foo"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config2.properties(1)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsTrue());

  // For Baz2, the parent level sets index_nested_properties = false, so all
  // leaf properties in child levels should be nested unindexable even if
  // they've set their index_nested_properties = true.
  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("Baz2.Bar.Google"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config1.properties(0)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsFalse());

  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("Baz2.Foo"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config2.properties(1)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsFalse());

  // For Baz3, the parent level sets index_nested_properties = true, but the
  // child level sets index_nested_properties = false.
  // - Leaf properties in the parent level should be nested indexable.
  // - Leaf properties in the child level should be nested unindexable.
  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("Baz3.Bar.Google"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config1.properties(0)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsFalse());

  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("Baz3.Foo"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config2.properties(1)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsTrue());

  // For Baz4, all levels set index_nested_properties = false, so all leaf
  // properties should be nested unindexable.
  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("Baz4.Bar.Google"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config1.properties(0)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsFalse());

  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("Baz4.Foo"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config2.properties(1)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsFalse());

  // Verify 1 and 0 level of nested document type properties.
  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("Hello1.Google"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config1.properties(0)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsTrue());

  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("Hello2.Google"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config1.properties(0)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsFalse());

  EXPECT_THAT(iterator.Advance(), IsOk());
  EXPECT_THAT(iterator.GetCurrentPropertyPath(), Eq("World"));
  EXPECT_THAT(iterator.GetCurrentPropertyConfig(),
              EqualsProto(schema_type_config4.properties(6)));
  EXPECT_THAT(iterator.GetCurrentNestedIndexable(), IsTrue());

  EXPECT_THAT(iterator.Advance(),
              StatusIs(libtextclassifier3::StatusCode::OUT_OF_RANGE));
}

}  // namespace

}  // namespace lib
}  // namespace icing
