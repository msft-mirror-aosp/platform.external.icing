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

#include "icing/schema/backup-schema-producer.h"

#include <memory>
#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/feature-flags.h"
#include "icing/file/filesystem.h"
#include "icing/portable/equals-proto.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-type-manager.h"
#include "icing/schema/schema-util.h"
#include "icing/store/document-filter-data.h"
#include "icing/store/dynamic-trie-key-mapper.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::testing::Eq;
using ::testing::Pointee;
using ::testing::SizeIs;

class BackupSchemaProducerTest : public ::testing::TestWithParam<FeatureFlags> {
 protected:
  void SetUp() override {
    test_dir_ = GetTestTempDir() + "/icing";
    schema_store_dir_ = test_dir_ + "/schema_store";
    filesystem_.CreateDirectoryRecursively(schema_store_dir_.c_str());
    feature_flags_ = std::make_unique<FeatureFlags>(GetParam());
  }

  void TearDown() override {
    ASSERT_TRUE(filesystem_.DeleteDirectoryRecursively(test_dir_.c_str()));
  }

  Filesystem filesystem_;
  std::string test_dir_;
  std::string schema_store_dir_;
  std::unique_ptr<FeatureFlags> feature_flags_;
};

TEST_P(BackupSchemaProducerTest, EmptySchema) {
  SchemaProto empty;
  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(
          /*enable_schema_definition_deduping=*/true);
  SchemaUtil::BuildTypeConfigInfoCache(empty, &type_config_info_cache);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DynamicTrieKeyMapper<SchemaTypeId>> type_id_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, schema_store_dir_,
                                                 /*maximum_size_bytes=*/10000));
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaTypeManager> schema_type_manager,
      SchemaTypeManager::Create(type_config_info_cache, type_id_mapper.get()));

  BackupSchemaProducer backup_producer(feature_flags_.get());
  ICING_ASSERT_OK_AND_ASSIGN(
      BackupSchemaProducer::BackupSchemaResult result,
      backup_producer.Produce(empty, schema_type_manager->section_manager(),
                              type_config_info_cache));
  EXPECT_THAT(result.backup_schema_produced, Eq(false));
}

TEST_P(BackupSchemaProducerTest, NoIndexedPropertySchema) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("TypeA")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop1")
                                        .SetCardinality(CARDINALITY_OPTIONAL)
                                        .SetDataType(TYPE_STRING))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop2")
                                        .SetCardinality(CARDINALITY_REQUIRED)
                                        .SetDataType(TYPE_INT64)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("TypeB")
                       .AddProperty(
                           PropertyConfigBuilder()
                               .SetName("prop3")
                               .SetCardinality(CARDINALITY_OPTIONAL)
                               .SetDataTypeDocument(
                                   "TypeA", /*index_nested_properties=*/false))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop4")
                                        .SetCardinality(CARDINALITY_REPEATED)
                                        .SetDataType(TYPE_STRING)))
          .Build();

  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(
          /*enable_schema_definition_deduping=*/true);
  SchemaUtil::BuildTypeConfigInfoCache(schema, &type_config_info_cache);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DynamicTrieKeyMapper<SchemaTypeId>> type_id_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, schema_store_dir_,
                                                 /*maximum_size_bytes=*/10000));
  ASSERT_THAT(type_id_mapper->Put("TypeA", 0), IsOk());
  ASSERT_THAT(type_id_mapper->Put("TypeB", 1), IsOk());
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaTypeManager> schema_type_manager,
      SchemaTypeManager::Create(type_config_info_cache, type_id_mapper.get()));

  BackupSchemaProducer backup_producer(feature_flags_.get());
  ICING_ASSERT_OK_AND_ASSIGN(
      BackupSchemaProducer::BackupSchemaResult result,
      backup_producer.Produce(schema, schema_type_manager->section_manager(),
                              type_config_info_cache));
  EXPECT_THAT(result.backup_schema_produced, Eq(false));
}

TEST_P(BackupSchemaProducerTest, RollbackCompatibleSchema) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("TypeA")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop1")
                                        .SetCardinality(CARDINALITY_OPTIONAL)
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop2")
                                        .SetCardinality(CARDINALITY_REQUIRED)
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("TypeB")
                       .AddProperty(
                           PropertyConfigBuilder()
                               .SetName("prop3")
                               .SetCardinality(CARDINALITY_OPTIONAL)
                               .SetDataTypeDocument(
                                   "TypeA", /*index_nested_properties=*/true))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("prop4")
                                        .SetCardinality(CARDINALITY_REPEATED)
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_VERBATIM)))
          .Build();

  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(
          /*enable_schema_definition_deduping=*/true);
  SchemaUtil::BuildTypeConfigInfoCache(schema, &type_config_info_cache);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DynamicTrieKeyMapper<SchemaTypeId>> type_id_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, schema_store_dir_,
                                                 /*maximum_size_bytes=*/10000));
  ASSERT_THAT(type_id_mapper->Put("TypeA", 0), IsOk());
  ASSERT_THAT(type_id_mapper->Put("TypeB", 1), IsOk());
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaTypeManager> schema_type_manager,
      SchemaTypeManager::Create(type_config_info_cache, type_id_mapper.get()));

  BackupSchemaProducer backup_producer(feature_flags_.get());
  ICING_ASSERT_OK_AND_ASSIGN(
      BackupSchemaProducer::BackupSchemaResult result,
      backup_producer.Produce(schema, schema_type_manager->section_manager(),
                              type_config_info_cache));
  EXPECT_THAT(result.backup_schema_produced, Eq(false));
}

TEST_P(BackupSchemaProducerTest, RemoveRfc822) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("TypeA").AddProperty(
              PropertyConfigBuilder()
                  .SetName("prop1")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_RFC822)))
          .Build();

  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(
          /*enable_schema_definition_deduping=*/true);
  SchemaUtil::BuildTypeConfigInfoCache(schema, &type_config_info_cache);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DynamicTrieKeyMapper<SchemaTypeId>> type_id_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, schema_store_dir_,
                                                 /*maximum_size_bytes=*/10000));
  ASSERT_THAT(type_id_mapper->Put("TypeA", 0), IsOk());
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaTypeManager> schema_type_manager,
      SchemaTypeManager::Create(type_config_info_cache, type_id_mapper.get()));

  BackupSchemaProducer backup_producer(feature_flags_.get());
  ICING_ASSERT_OK_AND_ASSIGN(
      BackupSchemaProducer::BackupSchemaResult result,
      backup_producer.Produce(schema, schema_type_manager->section_manager(),
                              type_config_info_cache));
  EXPECT_THAT(result.backup_schema_produced, Eq(true));

  SchemaProto expected_backup =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("TypeA").AddProperty(
              PropertyConfigBuilder()
                  .SetName("prop1")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataType(TYPE_STRING)))
          .Build();
  EXPECT_THAT(result.backup_schema,
              portable_equals_proto::EqualsProto(expected_backup));
}

TEST_P(BackupSchemaProducerTest, MakeExtraStringIndexedPropertiesUnindexed) {
  PropertyConfigBuilder indexed_string_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN);
  SchemaTypeConfigProto type =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .AddProperty(indexed_string_property_builder.SetName("prop1"))
          .AddProperty(indexed_string_property_builder.SetName("prop2"))
          .AddProperty(indexed_string_property_builder.SetName("prop3"))
          .AddProperty(indexed_string_property_builder.SetName("prop4"))
          .AddProperty(indexed_string_property_builder.SetName("prop5"))
          .AddProperty(indexed_string_property_builder.SetName("prop6"))
          .AddProperty(indexed_string_property_builder.SetName("prop7"))
          .AddProperty(indexed_string_property_builder.SetName("prop8"))
          .AddProperty(indexed_string_property_builder.SetName("prop9"))
          .AddProperty(indexed_string_property_builder.SetName("prop10"))
          .AddProperty(indexed_string_property_builder.SetName("prop11"))
          .AddProperty(indexed_string_property_builder.SetName("prop12"))
          .AddProperty(indexed_string_property_builder.SetName("prop13"))
          .AddProperty(indexed_string_property_builder.SetName("prop14"))
          .AddProperty(indexed_string_property_builder.SetName("prop15"))
          .AddProperty(indexed_string_property_builder.SetName("prop16"))
          .AddProperty(indexed_string_property_builder.SetName("prop17"))
          .AddProperty(indexed_string_property_builder.SetName("prop18"))
          .AddProperty(indexed_string_property_builder.SetName("prop19"))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type).Build();

  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(
          /*enable_schema_definition_deduping=*/true);
  SchemaUtil::BuildTypeConfigInfoCache(schema, &type_config_info_cache);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DynamicTrieKeyMapper<SchemaTypeId>> type_id_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, schema_store_dir_,
                                                 /*maximum_size_bytes=*/10000));
  ASSERT_THAT(type_id_mapper->Put("TypeA", 0), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaTypeManager> schema_type_manager,
      SchemaTypeManager::Create(type_config_info_cache, type_id_mapper.get()));

  BackupSchemaProducer backup_producer(feature_flags_.get());
  ICING_ASSERT_OK_AND_ASSIGN(
      BackupSchemaProducer::BackupSchemaResult result,
      backup_producer.Produce(schema, schema_type_manager->section_manager(),
                              type_config_info_cache));
  EXPECT_THAT(result.backup_schema_produced, Eq(true));

  PropertyConfigBuilder unindexed_string_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataType(TYPE_STRING);
  SchemaTypeConfigProto expected_type =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .AddProperty(indexed_string_property_builder.SetName("prop1"))
          .AddProperty(indexed_string_property_builder.SetName("prop2"))
          .AddProperty(indexed_string_property_builder.SetName("prop3"))
          .AddProperty(indexed_string_property_builder.SetName("prop4"))
          .AddProperty(indexed_string_property_builder.SetName("prop5"))
          .AddProperty(indexed_string_property_builder.SetName("prop6"))
          .AddProperty(indexed_string_property_builder.SetName("prop7"))
          .AddProperty(indexed_string_property_builder.SetName("prop8"))
          .AddProperty(indexed_string_property_builder.SetName("prop9"))
          .AddProperty(indexed_string_property_builder.SetName("prop10"))
          .AddProperty(indexed_string_property_builder.SetName("prop11"))
          .AddProperty(indexed_string_property_builder.SetName("prop12"))
          .AddProperty(indexed_string_property_builder.SetName("prop13"))
          .AddProperty(indexed_string_property_builder.SetName("prop14"))
          .AddProperty(indexed_string_property_builder.SetName("prop15"))
          .AddProperty(unindexed_string_property_builder.SetName("prop16"))
          .AddProperty(unindexed_string_property_builder.SetName("prop17"))
          .AddProperty(unindexed_string_property_builder.SetName("prop18"))
          .AddProperty(unindexed_string_property_builder.SetName("prop19"))
          .Build();
  SchemaProto expected_backup = SchemaBuilder().AddType(expected_type).Build();
  EXPECT_THAT(result.backup_schema,
              portable_equals_proto::EqualsProto(expected_backup));
}

TEST_P(BackupSchemaProducerTest, MakeExtraIntIndexedPropertiesUnindexed) {
  PropertyConfigBuilder indexed_int_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeInt64(NUMERIC_MATCH_RANGE);
  SchemaTypeConfigProto type =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(indexed_int_property_builder.SetName("prop0"))
          .AddProperty(indexed_int_property_builder.SetName("prop1"))
          .AddProperty(indexed_int_property_builder.SetName("prop2"))
          .AddProperty(indexed_int_property_builder.SetName("prop3"))
          .AddProperty(indexed_int_property_builder.SetName("prop4"))
          .AddProperty(indexed_int_property_builder.SetName("prop5"))
          .AddProperty(indexed_int_property_builder.SetName("prop6"))
          .AddProperty(indexed_int_property_builder.SetName("prop7"))
          .AddProperty(indexed_int_property_builder.SetName("prop8"))
          .AddProperty(indexed_int_property_builder.SetName("prop9"))
          .AddProperty(indexed_int_property_builder.SetName("prop10"))
          .AddProperty(indexed_int_property_builder.SetName("prop11"))
          .AddProperty(indexed_int_property_builder.SetName("prop12"))
          .AddProperty(indexed_int_property_builder.SetName("prop13"))
          .AddProperty(indexed_int_property_builder.SetName("prop14"))
          .AddProperty(indexed_int_property_builder.SetName("prop15"))
          .AddProperty(indexed_int_property_builder.SetName("prop16"))
          .AddProperty(indexed_int_property_builder.SetName("prop17"))
          .AddProperty(indexed_int_property_builder.SetName("prop18"))
          .AddProperty(indexed_int_property_builder.SetName("prop19"))
          .Build();
  SchemaProto schema = SchemaBuilder().AddType(type).Build();

  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(
          /*enable_schema_definition_deduping=*/true);
  SchemaUtil::BuildTypeConfigInfoCache(schema, &type_config_info_cache);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DynamicTrieKeyMapper<SchemaTypeId>> type_id_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, schema_store_dir_,
                                                 /*maximum_size_bytes=*/10000));
  ASSERT_THAT(type_id_mapper->Put("TypeA", 0), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaTypeManager> schema_type_manager,
      SchemaTypeManager::Create(type_config_info_cache, type_id_mapper.get()));

  BackupSchemaProducer backup_producer(feature_flags_.get());
  ICING_ASSERT_OK_AND_ASSIGN(
      BackupSchemaProducer::BackupSchemaResult result,
      backup_producer.Produce(schema, schema_type_manager->section_manager(),
                              type_config_info_cache));
  EXPECT_THAT(result.backup_schema_produced, Eq(true));

  PropertyConfigBuilder unindexed_int_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataType(TYPE_INT64);
  SchemaTypeConfigProto expected_type =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(indexed_int_property_builder.SetName("prop0"))
          .AddProperty(indexed_int_property_builder.SetName("prop1"))
          .AddProperty(indexed_int_property_builder.SetName("prop2"))
          .AddProperty(indexed_int_property_builder.SetName("prop3"))
          .AddProperty(indexed_int_property_builder.SetName("prop4"))
          .AddProperty(indexed_int_property_builder.SetName("prop5"))
          .AddProperty(indexed_int_property_builder.SetName("prop6"))
          .AddProperty(indexed_int_property_builder.SetName("prop7"))
          .AddProperty(indexed_int_property_builder.SetName("prop8"))
          .AddProperty(indexed_int_property_builder.SetName("prop9"))
          .AddProperty(indexed_int_property_builder.SetName("prop10"))
          .AddProperty(indexed_int_property_builder.SetName("prop11"))
          .AddProperty(indexed_int_property_builder.SetName("prop12"))
          .AddProperty(indexed_int_property_builder.SetName("prop13"))
          .AddProperty(indexed_int_property_builder.SetName("prop14"))
          .AddProperty(indexed_int_property_builder.SetName("prop15"))
          .AddProperty(unindexed_int_property_builder.SetName("prop16"))
          .AddProperty(unindexed_int_property_builder.SetName("prop17"))
          .AddProperty(unindexed_int_property_builder.SetName("prop18"))
          .AddProperty(unindexed_int_property_builder.SetName("prop19"))
          .Build();
  SchemaProto expected_backup = SchemaBuilder().AddType(expected_type).Build();
  EXPECT_THAT(result.backup_schema,
              portable_equals_proto::EqualsProto(expected_backup));
}

TEST_P(BackupSchemaProducerTest, MakeExtraDocumentIndexedPropertiesUnindexed) {
  PropertyConfigBuilder indexed_string_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN);
  SchemaTypeConfigProto typeB =
      SchemaTypeConfigBuilder()
          .SetType("TypeB")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .AddProperty(indexed_string_property_builder.SetName("prop1"))
          .AddProperty(indexed_string_property_builder.SetName("prop2"))
          .AddProperty(indexed_string_property_builder.SetName("prop3"))
          .AddProperty(indexed_string_property_builder.SetName("prop4"))
          .AddProperty(indexed_string_property_builder.SetName("prop5"))
          .AddProperty(indexed_string_property_builder.SetName("prop6"))
          .AddProperty(indexed_string_property_builder.SetName("prop7"))
          .AddProperty(indexed_string_property_builder.SetName("prop8"))
          .AddProperty(indexed_string_property_builder.SetName("prop9"))
          .Build();

  PropertyConfigBuilder indexed_document_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeDocument("TypeB", /*index_nested_properties=*/true);
  SchemaTypeConfigProto typeA =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(indexed_document_property_builder.SetName("propA"))
          .AddProperty(indexed_document_property_builder.SetName("propB"))
          .Build();

  SchemaProto schema = SchemaBuilder().AddType(typeA).AddType(typeB).Build();

  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(
          /*enable_schema_definition_deduping=*/true);
  SchemaUtil::BuildTypeConfigInfoCache(schema, &type_config_info_cache);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DynamicTrieKeyMapper<SchemaTypeId>> type_id_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, schema_store_dir_,
                                                 /*maximum_size_bytes=*/10000));
  ASSERT_THAT(type_id_mapper->Put("TypeA", 0), IsOk());
  ASSERT_THAT(type_id_mapper->Put("TypeB", 1), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaTypeManager> schema_type_manager,
      SchemaTypeManager::Create(type_config_info_cache, type_id_mapper.get()));

  BackupSchemaProducer backup_producer(feature_flags_.get());
  ICING_ASSERT_OK_AND_ASSIGN(
      BackupSchemaProducer::BackupSchemaResult result,
      backup_producer.Produce(schema, schema_type_manager->section_manager(),
                              type_config_info_cache));
  EXPECT_THAT(result.backup_schema_produced, Eq(true));

  PropertyConfigProto unindexed_document_property =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataType(TYPE_DOCUMENT)
          .Build();
  unindexed_document_property.set_schema_type("TypeB");
  PropertyConfigBuilder unindexed_document_property_builder(
      unindexed_document_property);
  SchemaTypeConfigProto expected_typeA =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(indexed_document_property_builder.SetName("propA"))
          .AddProperty(unindexed_document_property_builder.SetName("propB"))
          .Build();
  SchemaProto expected_backup =
      SchemaBuilder().AddType(expected_typeA).AddType(typeB).Build();
  EXPECT_THAT(result.backup_schema,
              portable_equals_proto::EqualsProto(expected_backup));
}

TEST_P(
    BackupSchemaProducerTest,
    MakeExtraDocumentIndexedPropertiesWithIndexableNestedPropertiesListUnindexed) {
  PropertyConfigBuilder indexed_string_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN);
  PropertyConfigBuilder indexed_int_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeInt64(NUMERIC_MATCH_RANGE);
  SchemaTypeConfigProto typeB =
      SchemaTypeConfigBuilder()
          .SetType("TypeB")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .AddProperty(indexed_int_property_builder.SetName("prop1"))
          .AddProperty(indexed_string_property_builder.SetName("prop2"))
          .AddProperty(indexed_int_property_builder.SetName("prop3"))
          .AddProperty(indexed_string_property_builder.SetName("prop4"))
          .AddProperty(indexed_int_property_builder.SetName("prop5"))
          .AddProperty(indexed_string_property_builder.SetName("prop6"))
          .AddProperty(indexed_int_property_builder.SetName("prop7"))
          .AddProperty(indexed_string_property_builder.SetName("prop8"))
          .AddProperty(indexed_int_property_builder.SetName("prop9"))
          .Build();

  // Create indexed document property by using indexable nested properties list.
  PropertyConfigBuilder indexed_document_property_with_list_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeDocument(
              "TypeB", /*indexable_nested_properties_list=*/{
                  "prop0", "prop1", "prop2", "prop3", "prop4", "prop5",
                  "unknown1", "unknown2", "unknown3"});
  SchemaTypeConfigProto typeA =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(
              indexed_document_property_with_list_builder.SetName("propA"))
          .AddProperty(
              indexed_document_property_with_list_builder.SetName("propB"))
          .Build();

  SchemaProto schema = SchemaBuilder().AddType(typeA).AddType(typeB).Build();

  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(
          /*enable_schema_definition_deduping=*/true);
  SchemaUtil::BuildTypeConfigInfoCache(schema, &type_config_info_cache);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DynamicTrieKeyMapper<SchemaTypeId>> type_id_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, schema_store_dir_,
                                                 /*maximum_size_bytes=*/10000));
  ASSERT_THAT(type_id_mapper->Put("TypeA", 0), IsOk());
  ASSERT_THAT(type_id_mapper->Put("TypeB", 1), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaTypeManager> schema_type_manager,
      SchemaTypeManager::Create(type_config_info_cache, type_id_mapper.get()));
  ASSERT_THAT(schema_type_manager->section_manager().GetMetadataList("TypeA"),
              IsOkAndHolds(Pointee(SizeIs(18))));

  BackupSchemaProducer backup_producer(feature_flags_.get());
  ICING_ASSERT_OK_AND_ASSIGN(
      BackupSchemaProducer::BackupSchemaResult result,
      backup_producer.Produce(schema, schema_type_manager->section_manager(),
                              type_config_info_cache));
  EXPECT_THAT(result.backup_schema_produced, Eq(true));

  PropertyConfigProto unindexed_document_property =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataType(TYPE_DOCUMENT)
          .Build();
  unindexed_document_property.set_schema_type("TypeB");
  PropertyConfigBuilder unindexed_document_property_builder(
      unindexed_document_property);

  // "propA" and "propB" both have 9 sections respectively, so we have to drop
  // "propB" indexing config to make total # of sections <= 16.
  SchemaTypeConfigProto expected_typeA =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(
              indexed_document_property_with_list_builder.SetName("propA"))
          .AddProperty(unindexed_document_property_builder.SetName("propB"))
          .Build();
  SchemaProto expected_backup =
      SchemaBuilder().AddType(expected_typeA).AddType(typeB).Build();
  EXPECT_THAT(result.backup_schema,
              portable_equals_proto::EqualsProto(expected_backup));
}

TEST_P(BackupSchemaProducerTest, MakeRfcPropertiesUnindexedFirst) {
  PropertyConfigBuilder indexed_string_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN);
  // Create a type with 16 indexed properties, one of which is an RFC822
  // property.
  SchemaTypeConfigProto typeA =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .AddProperty(indexed_string_property_builder.SetName("prop1"))
          .AddProperty(indexed_string_property_builder.SetName("prop2"))
          .AddProperty(indexed_string_property_builder.SetName("prop3"))
          .AddProperty(indexed_string_property_builder.SetName("prop4"))
          // "propRfc" takes the place of "prop5".
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("propRfc")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_RFC822))
          .AddProperty(indexed_string_property_builder.SetName("prop6"))
          .AddProperty(indexed_string_property_builder.SetName("prop7"))
          .AddProperty(indexed_string_property_builder.SetName("prop8"))
          .AddProperty(indexed_string_property_builder.SetName("prop9"))
          .AddProperty(indexed_string_property_builder.SetName("prop10"))
          .AddProperty(indexed_string_property_builder.SetName("prop11"))
          .AddProperty(indexed_string_property_builder.SetName("prop12"))
          .AddProperty(indexed_string_property_builder.SetName("prop13"))
          .AddProperty(indexed_string_property_builder.SetName("prop14"))
          .AddProperty(indexed_string_property_builder.SetName("prop15"))
          .AddProperty(indexed_string_property_builder.SetName("prop16"))
          .Build();

  SchemaProto schema = SchemaBuilder().AddType(typeA).Build();

  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(
          /*enable_schema_definition_deduping=*/true);
  SchemaUtil::BuildTypeConfigInfoCache(schema, &type_config_info_cache);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DynamicTrieKeyMapper<SchemaTypeId>> type_id_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, schema_store_dir_,
                                                 /*maximum_size_bytes=*/10000));
  ASSERT_THAT(type_id_mapper->Put("TypeA", 0), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaTypeManager> schema_type_manager,
      SchemaTypeManager::Create(type_config_info_cache, type_id_mapper.get()));

  BackupSchemaProducer backup_producer(feature_flags_.get());
  ICING_ASSERT_OK_AND_ASSIGN(
      BackupSchemaProducer::BackupSchemaResult result,
      backup_producer.Produce(schema, schema_type_manager->section_manager(),
                              type_config_info_cache));
  EXPECT_THAT(result.backup_schema_produced, Eq(true));

  // The RFC822 property should have been marked as unindexed first. This would
  // leave only 15 indexed properties which is under the old limit of 16.
  SchemaTypeConfigProto expected_typeA =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .AddProperty(indexed_string_property_builder.SetName("prop1"))
          .AddProperty(indexed_string_property_builder.SetName("prop2"))
          .AddProperty(indexed_string_property_builder.SetName("prop3"))
          .AddProperty(indexed_string_property_builder.SetName("prop4"))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("propRfc")
                           .SetCardinality(CARDINALITY_OPTIONAL)
                           .SetDataType(TYPE_STRING))
          .AddProperty(indexed_string_property_builder.SetName("prop6"))
          .AddProperty(indexed_string_property_builder.SetName("prop7"))
          .AddProperty(indexed_string_property_builder.SetName("prop8"))
          .AddProperty(indexed_string_property_builder.SetName("prop9"))
          .AddProperty(indexed_string_property_builder.SetName("prop10"))
          .AddProperty(indexed_string_property_builder.SetName("prop11"))
          .AddProperty(indexed_string_property_builder.SetName("prop12"))
          .AddProperty(indexed_string_property_builder.SetName("prop13"))
          .AddProperty(indexed_string_property_builder.SetName("prop14"))
          .AddProperty(indexed_string_property_builder.SetName("prop15"))
          .AddProperty(indexed_string_property_builder.SetName("prop16"))
          .Build();
  SchemaProto expected_backup = SchemaBuilder().AddType(expected_typeA).Build();
  EXPECT_THAT(result.backup_schema,
              portable_equals_proto::EqualsProto(expected_backup));
}

TEST_P(BackupSchemaProducerTest, MakeExtraPropertiesUnindexedMultipleTypes) {
  PropertyConfigBuilder indexed_string_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN);
  PropertyConfigBuilder indexed_int_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeInt64(NUMERIC_MATCH_RANGE);
  SchemaTypeConfigProto typeB =
      SchemaTypeConfigBuilder()
          .SetType("TypeB")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .AddProperty(indexed_int_property_builder.SetName("prop1"))
          .AddProperty(indexed_string_property_builder.SetName("prop2"))
          .AddProperty(indexed_int_property_builder.SetName("prop3"))
          .AddProperty(indexed_string_property_builder.SetName("prop4"))
          .Build();

  PropertyConfigBuilder indexed_document_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeDocument("TypeB", /*index_nested_properties=*/true);
  PropertyConfigBuilder indexed_document_property_with_list_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeDocument(
              "TypeB", /*indexable_nested_properties_list=*/{
                  "prop0", "prop4", "unknown1", "unknown2", "unknown3"});
  SchemaTypeConfigProto typeA =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(indexed_string_property_builder.SetName("propA"))
          .AddProperty(
              indexed_document_property_with_list_builder.SetName("propB"))
          .AddProperty(indexed_string_property_builder.SetName("propC"))
          .AddProperty(indexed_document_property_builder.SetName("propD"))
          .AddProperty(indexed_string_property_builder.SetName("propE"))
          .AddProperty(indexed_int_property_builder.SetName("propF"))
          .AddProperty(indexed_document_property_builder.SetName("propG"))
          .AddProperty(indexed_string_property_builder.SetName("propH"))
          .AddProperty(indexed_int_property_builder.SetName("propI"))
          .AddProperty(
              indexed_document_property_with_list_builder.SetName("propJ"))
          .Build();

  SchemaProto schema = SchemaBuilder().AddType(typeA).AddType(typeB).Build();

  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(
          /*enable_schema_definition_deduping=*/true);
  SchemaUtil::BuildTypeConfigInfoCache(schema, &type_config_info_cache);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DynamicTrieKeyMapper<SchemaTypeId>> type_id_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, schema_store_dir_,
                                                 /*maximum_size_bytes=*/10000));
  ASSERT_THAT(type_id_mapper->Put("TypeA", 0), IsOk());
  ASSERT_THAT(type_id_mapper->Put("TypeB", 1), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaTypeManager> schema_type_manager,
      SchemaTypeManager::Create(type_config_info_cache, type_id_mapper.get()));
  ASSERT_THAT(schema_type_manager->section_manager().GetMetadataList("TypeA"),
              IsOkAndHolds(Pointee(SizeIs(26))));

  BackupSchemaProducer backup_producer(feature_flags_.get());
  ICING_ASSERT_OK_AND_ASSIGN(
      BackupSchemaProducer::BackupSchemaResult result,
      backup_producer.Produce(schema, schema_type_manager->section_manager(),
                              type_config_info_cache));
  EXPECT_THAT(result.backup_schema_produced, Eq(true));

  PropertyConfigBuilder unindexed_string_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataType(TYPE_STRING);
  PropertyConfigBuilder unindexed_int_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataType(TYPE_INT64);
  PropertyConfigProto unindexed_document_property =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataType(TYPE_DOCUMENT)
          .Build();
  unindexed_document_property.set_schema_type("TypeB");
  PropertyConfigBuilder unindexed_document_property_builder(
      unindexed_document_property);

  // On version 0 (Android T):
  // - Only "propA", "propC", "propD.prop0", "propD.prop1", "propD.prop2",
  //   "propD.prop3", "propD.prop4", "propE", "propF" will be assigned sections.
  // - Unlike version 2, "propB.prop0", "propB.prop4", "propB.unknown1",
  //   "propB.unknown2", "propB.unknown3" will be ignored because version 0
  //   doesn't recognize indexable nested properties list.
  // - So there will be only 9 sections on version 0. We still have potential to
  //   avoid dropping "propG", "propH", "propI" indexing configs on version 0
  //   (in this case it will be 16 sections), but it is ok to make it simple as
  //   long as total # of sections <= 16.
  SchemaTypeConfigProto expected_typeA =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(indexed_string_property_builder.SetName("propA"))
          .AddProperty(
              indexed_document_property_with_list_builder.SetName("propB"))
          .AddProperty(indexed_string_property_builder.SetName("propC"))
          .AddProperty(indexed_document_property_builder.SetName("propD"))
          .AddProperty(indexed_string_property_builder.SetName("propE"))
          .AddProperty(indexed_int_property_builder.SetName("propF"))
          .AddProperty(unindexed_document_property_builder.SetName("propG"))
          .AddProperty(unindexed_string_property_builder.SetName("propH"))
          .AddProperty(unindexed_int_property_builder.SetName("propI"))
          .AddProperty(unindexed_document_property_builder.SetName("propJ"))
          .Build();
  SchemaProto expected_backup =
      SchemaBuilder().AddType(expected_typeA).AddType(typeB).Build();
  EXPECT_THAT(result.backup_schema,
              portable_equals_proto::EqualsProto(expected_backup));
}

TEST_P(BackupSchemaProducerTest,
       EmbeddingBackupDisabledDoesNotRemoveEmbeddingProperty) {
  if (feature_flags_->enable_embedding_backup_generation()) {
    GTEST_SKIP() << "enable_embedding_backup_generation is enabled. Skipping.";
  }
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("TypeA").AddProperty(
              PropertyConfigBuilder()
                  .SetName("prop1")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeVector(EmbeddingIndexingConfig::
                                         EmbeddingIndexingType::LINEAR_SEARCH)))
          .Build();

  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(
          /*enable_schema_definition_deduping=*/true);
  SchemaUtil::BuildTypeConfigInfoCache(schema, &type_config_info_cache);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DynamicTrieKeyMapper<SchemaTypeId>> type_id_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, schema_store_dir_,
                                                 /*maximum_size_bytes=*/10000));
  ASSERT_THAT(type_id_mapper->Put("TypeA", 0), IsOk());
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaTypeManager> schema_type_manager,
      SchemaTypeManager::Create(type_config_info_cache, type_id_mapper.get()));

  BackupSchemaProducer backup_producer(feature_flags_.get());
  ICING_ASSERT_OK_AND_ASSIGN(
      BackupSchemaProducer::BackupSchemaResult result,
      backup_producer.Produce(schema, schema_type_manager->section_manager(),
                              type_config_info_cache));
  EXPECT_THAT(result.backup_schema_produced, Eq(false));
}

TEST_P(BackupSchemaProducerTest, RemoveEmbeddingProperty) {
  if (!feature_flags_->enable_embedding_backup_generation()) {
    GTEST_SKIP() << "enable_embedding_backup_generation is disabled. Skipping.";
  }
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("TypeA").AddProperty(
              PropertyConfigBuilder()
                  .SetName("prop1")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeVector(EmbeddingIndexingConfig::
                                         EmbeddingIndexingType::LINEAR_SEARCH)))
          .Build();

  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(
          /*enable_schema_definition_deduping=*/true);
  SchemaUtil::BuildTypeConfigInfoCache(schema, &type_config_info_cache);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DynamicTrieKeyMapper<SchemaTypeId>> type_id_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, schema_store_dir_,
                                                 /*maximum_size_bytes=*/10000));
  ASSERT_THAT(type_id_mapper->Put("TypeA", 0), IsOk());
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaTypeManager> schema_type_manager,
      SchemaTypeManager::Create(type_config_info_cache, type_id_mapper.get()));

  BackupSchemaProducer backup_producer(feature_flags_.get());
  ICING_ASSERT_OK_AND_ASSIGN(
      BackupSchemaProducer::BackupSchemaResult result,
      backup_producer.Produce(schema, schema_type_manager->section_manager(),
                              type_config_info_cache));
  EXPECT_THAT(result.backup_schema_produced, Eq(true));

  // The Embedding Property should have been removed from the backup schema.
  SchemaProto expected_backup =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("TypeA"))
          .Build();
  EXPECT_THAT(result.backup_schema,
              portable_equals_proto::EqualsProto(expected_backup));
}

// Even REQUIRED embedding properties should be removed.
TEST_P(BackupSchemaProducerTest, RemoveRequiredEmbeddingProperty) {
  if (!feature_flags_->enable_embedding_backup_generation()) {
    GTEST_SKIP() << "enable_embedding_backup_generation is disabled. Skipping.";
  }
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("TypeA").AddProperty(
              PropertyConfigBuilder()
                  .SetName("prop1")
                  .SetCardinality(CARDINALITY_REQUIRED)
                  .SetDataTypeVector(EmbeddingIndexingConfig::
                                         EmbeddingIndexingType::LINEAR_SEARCH)))
          .Build();

  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(
          /*enable_schema_definition_deduping=*/true);
  SchemaUtil::BuildTypeConfigInfoCache(schema, &type_config_info_cache);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DynamicTrieKeyMapper<SchemaTypeId>> type_id_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, schema_store_dir_,
                                                 /*maximum_size_bytes=*/10000));
  ASSERT_THAT(type_id_mapper->Put("TypeA", 0), IsOk());
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaTypeManager> schema_type_manager,
      SchemaTypeManager::Create(type_config_info_cache, type_id_mapper.get()));

  BackupSchemaProducer backup_producer(feature_flags_.get());
  ICING_ASSERT_OK_AND_ASSIGN(
      BackupSchemaProducer::BackupSchemaResult result,
      backup_producer.Produce(schema, schema_type_manager->section_manager(),
                              type_config_info_cache));
  EXPECT_THAT(result.backup_schema_produced, Eq(true));

  // The Embedding Property should have been removed from the backup schema.
  SchemaProto expected_backup =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("TypeA"))
          .Build();
  EXPECT_THAT(result.backup_schema,
              portable_equals_proto::EqualsProto(expected_backup));
}

// Embedding properties consume an indexed property id. We should remove them
// first to minimize the number of indexed properties that we have to mark as
// unindexed.
TEST_P(BackupSchemaProducerTest, RemoveEmbeddingPropertyFirst) {
  if (!feature_flags_->enable_embedding_backup_generation()) {
    GTEST_SKIP() << "enable_embedding_backup_generation is disabled. Skipping.";
  }

  PropertyConfigBuilder indexed_string_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN);
  // Create a type with 16 indexed properties, one of which is an embedding
  // property.
  SchemaTypeConfigProto typeA =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .AddProperty(indexed_string_property_builder.SetName("prop1"))
          .AddProperty(indexed_string_property_builder.SetName("prop2"))
          .AddProperty(indexed_string_property_builder.SetName("prop3"))
          .AddProperty(indexed_string_property_builder.SetName("prop4"))
          // "propEmbed" takes the place of "prop5".
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("propEmbed")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeVector(EmbeddingIndexingConfig::
                                         EmbeddingIndexingType::LINEAR_SEARCH))
          .AddProperty(indexed_string_property_builder.SetName("prop6"))
          .AddProperty(indexed_string_property_builder.SetName("prop7"))
          .AddProperty(indexed_string_property_builder.SetName("prop8"))
          .AddProperty(indexed_string_property_builder.SetName("prop9"))
          .AddProperty(indexed_string_property_builder.SetName("prop10"))
          .AddProperty(indexed_string_property_builder.SetName("prop11"))
          .AddProperty(indexed_string_property_builder.SetName("prop12"))
          .AddProperty(indexed_string_property_builder.SetName("prop13"))
          .AddProperty(indexed_string_property_builder.SetName("prop14"))
          .AddProperty(indexed_string_property_builder.SetName("prop15"))
          .AddProperty(indexed_string_property_builder.SetName("prop16"))
          .Build();

  SchemaProto schema = SchemaBuilder().AddType(typeA).Build();

  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(
          /*enable_schema_definition_deduping=*/true);
  SchemaUtil::BuildTypeConfigInfoCache(schema, &type_config_info_cache);
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DynamicTrieKeyMapper<SchemaTypeId>> type_id_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, schema_store_dir_,
                                                 /*maximum_size_bytes=*/10000));
  ASSERT_THAT(type_id_mapper->Put("TypeA", 0), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaTypeManager> schema_type_manager,
      SchemaTypeManager::Create(type_config_info_cache, type_id_mapper.get()));

  BackupSchemaProducer backup_producer(feature_flags_.get());
  ICING_ASSERT_OK_AND_ASSIGN(
      BackupSchemaProducer::BackupSchemaResult result,
      backup_producer.Produce(schema, schema_type_manager->section_manager(),
                              type_config_info_cache));
  EXPECT_THAT(result.backup_schema_produced, Eq(true));

  // The Embedding Property should have been removed from the backup schema.
  // This would leave only 15 indexed properties which is under the old limit of
  // 16. All remaining properties should remain indexed.
  SchemaTypeConfigProto expected_typeA =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .AddProperty(indexed_string_property_builder.SetName("prop1"))
          .AddProperty(indexed_string_property_builder.SetName("prop2"))
          .AddProperty(indexed_string_property_builder.SetName("prop3"))
          .AddProperty(indexed_string_property_builder.SetName("prop4"))
          .AddProperty(indexed_string_property_builder.SetName("prop6"))
          .AddProperty(indexed_string_property_builder.SetName("prop7"))
          .AddProperty(indexed_string_property_builder.SetName("prop8"))
          .AddProperty(indexed_string_property_builder.SetName("prop9"))
          .AddProperty(indexed_string_property_builder.SetName("prop10"))
          .AddProperty(indexed_string_property_builder.SetName("prop11"))
          .AddProperty(indexed_string_property_builder.SetName("prop12"))
          .AddProperty(indexed_string_property_builder.SetName("prop13"))
          .AddProperty(indexed_string_property_builder.SetName("prop14"))
          .AddProperty(indexed_string_property_builder.SetName("prop15"))
          .AddProperty(indexed_string_property_builder.SetName("prop16"))
          .Build();
  SchemaProto expected_backup = SchemaBuilder().AddType(expected_typeA).Build();
  EXPECT_THAT(result.backup_schema,
              portable_equals_proto::EqualsProto(expected_backup));
}

TEST_P(BackupSchemaProducerTest, RedefineDedupedTypes) {
  SchemaTypeConfigProto typeA =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("prop1")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop2")
                           .SetCardinality(CARDINALITY_REQUIRED)
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE))
          .Build();
  // typeB and typeC are deduped copies of typeA.
  SchemaTypeConfigProto typeB = SchemaTypeConfigBuilder(typeA)
                                    .SetType("TypeB")
                                    .BuildAndPopulatePropertiesDigest();
  typeB.clear_properties();
  SchemaTypeConfigProto typeC = SchemaTypeConfigBuilder(typeA)
                                    .SetType("TypeC")
                                    .BuildAndPopulatePropertiesDigest();
  typeC.clear_properties();

  SchemaProto schema =
      SchemaBuilder().AddType(typeA).AddType(typeB).AddType(typeC).Build();

  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(
          /*enable_schema_definition_deduping=*/true);
  SchemaUtil::BuildTypeConfigInfoCache(schema, &type_config_info_cache);

  // Check that TypeB and TypeC are deduped.
  EXPECT_THAT(type_config_info_cache.IsSchemaTypeConfigDeduped("TypeA"),
              IsOkAndHolds(false));
  EXPECT_THAT(type_config_info_cache.IsSchemaTypeConfigDeduped("TypeB"),
              IsOkAndHolds(true));
  EXPECT_THAT(type_config_info_cache.IsSchemaTypeConfigDeduped("TypeC"),
              IsOkAndHolds(true));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DynamicTrieKeyMapper<SchemaTypeId>> type_id_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, schema_store_dir_,
                                                 /*maximum_size_bytes=*/10000));
  ASSERT_THAT(type_id_mapper->Put("TypeA", 0), IsOk());
  ASSERT_THAT(type_id_mapper->Put("TypeB", 1), IsOk());
  ASSERT_THAT(type_id_mapper->Put("TypeC", 2), IsOk());
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaTypeManager> schema_type_manager,
      SchemaTypeManager::Create(type_config_info_cache, type_id_mapper.get()));

  BackupSchemaProducer backup_producer(feature_flags_.get());
  ICING_ASSERT_OK_AND_ASSIGN(
      BackupSchemaProducer::BackupSchemaResult result,
      backup_producer.Produce(schema, schema_type_manager->section_manager(),
                              type_config_info_cache));
  EXPECT_THAT(result.backup_schema_produced, Eq(true));

  SchemaTypeConfigProto expected_typeB =
      SchemaTypeConfigBuilder(typeA).SetType("TypeB").Build();
  SchemaTypeConfigProto expected_typeC =
      SchemaTypeConfigBuilder(typeA).SetType("TypeC").Build();
  EXPECT_THAT(expected_typeB.properties_size(), Eq(2));
  EXPECT_THAT(expected_typeC.properties_size(), Eq(2));

  SchemaProto expected_backup = SchemaBuilder()
                                    .AddType(typeA)
                                    .AddType(expected_typeB)
                                    .AddType(expected_typeC)
                                    .Build();
  EXPECT_THAT(result.backup_schema,
              EqualsSchemaProtoIgnorePropertiesDigest(expected_backup));
}

TEST_P(BackupSchemaProducerTest, RedefineDedupedTypeWithRfc822Tokenization) {
  SchemaTypeConfigProto typeA =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("prop1")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_RFC822))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop2")
                           .SetCardinality(CARDINALITY_REQUIRED)
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE))
          .Build();
  // typeB and typeC are deduped copies of typeA.
  SchemaTypeConfigProto typeB = SchemaTypeConfigBuilder(typeA)
                                    .SetType("TypeB")
                                    .BuildAndPopulatePropertiesDigest();
  typeB.clear_properties();
  SchemaTypeConfigProto typeC = SchemaTypeConfigBuilder(typeA)
                                    .SetType("TypeC")
                                    .BuildAndPopulatePropertiesDigest();
  typeC.clear_properties();

  SchemaProto schema =
      SchemaBuilder().AddType(typeA).AddType(typeB).AddType(typeC).Build();

  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(
          /*enable_schema_definition_deduping=*/true);
  SchemaUtil::BuildTypeConfigInfoCache(schema, &type_config_info_cache);

  // Check that TypeB and TypeC are deduped.
  EXPECT_THAT(type_config_info_cache.IsSchemaTypeConfigDeduped("TypeA"),
              IsOkAndHolds(false));
  EXPECT_THAT(type_config_info_cache.IsSchemaTypeConfigDeduped("TypeB"),
              IsOkAndHolds(true));
  EXPECT_THAT(type_config_info_cache.IsSchemaTypeConfigDeduped("TypeC"),
              IsOkAndHolds(true));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DynamicTrieKeyMapper<SchemaTypeId>> type_id_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, schema_store_dir_,
                                                 /*maximum_size_bytes=*/10000));
  ASSERT_THAT(type_id_mapper->Put("TypeA", 0), IsOk());
  ASSERT_THAT(type_id_mapper->Put("TypeB", 1), IsOk());
  ASSERT_THAT(type_id_mapper->Put("TypeC", 2), IsOk());
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaTypeManager> schema_type_manager,
      SchemaTypeManager::Create(type_config_info_cache, type_id_mapper.get()));

  BackupSchemaProducer backup_producer(feature_flags_.get());
  ICING_ASSERT_OK_AND_ASSIGN(
      BackupSchemaProducer::BackupSchemaResult result,
      backup_producer.Produce(schema, schema_type_manager->section_manager(),
                              type_config_info_cache));
  EXPECT_THAT(result.backup_schema_produced, Eq(true));

  SchemaTypeConfigProto typeA_no_rfc =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop1")
                           .SetCardinality(CARDINALITY_OPTIONAL)
                           .SetDataType(TYPE_STRING))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("prop2")
                           .SetCardinality(CARDINALITY_REQUIRED)
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE))
          .Build();
  SchemaTypeConfigProto typeB_no_rfc =
      SchemaTypeConfigBuilder(typeA_no_rfc).SetType("TypeB").Build();
  SchemaTypeConfigProto typeC_no_rfc =
      SchemaTypeConfigBuilder(typeA_no_rfc).SetType("TypeC").Build();
  EXPECT_THAT(typeB_no_rfc.properties_size(), Eq(2));
  EXPECT_THAT(typeC_no_rfc.properties_size(), Eq(2));

  SchemaProto expected_backup = SchemaBuilder()
                                    .AddType(typeA_no_rfc)
                                    .AddType(typeB_no_rfc)
                                    .AddType(typeC_no_rfc)
                                    .Build();
  EXPECT_THAT(result.backup_schema,
              EqualsSchemaProtoIgnorePropertiesDigest(expected_backup));
}

TEST_P(BackupSchemaProducerTest, RedefineDedupedTypesWithEmbeddingProperty) {
  if (!feature_flags_->enable_embedding_backup_generation()) {
    GTEST_SKIP() << "enable_embedding_backup_generation is disabled. Skipping.";
  }

  PropertyConfigBuilder indexed_string_property_builder =
      PropertyConfigBuilder()
          .SetCardinality(CARDINALITY_OPTIONAL)
          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN);
  // Create a type with 16 indexed properties, one of which is an embedding
  // property.
  SchemaTypeConfigProto typeA =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .AddProperty(indexed_string_property_builder.SetName("prop1"))
          .AddProperty(indexed_string_property_builder.SetName("prop2"))
          .AddProperty(indexed_string_property_builder.SetName("prop3"))
          .AddProperty(indexed_string_property_builder.SetName("prop4"))
          // "propEmbed" takes the place of "prop5".
          .AddProperty(
              PropertyConfigBuilder()
                  .SetName("propEmbed")
                  .SetCardinality(CARDINALITY_OPTIONAL)
                  .SetDataTypeVector(EmbeddingIndexingConfig::
                                         EmbeddingIndexingType::LINEAR_SEARCH))
          .AddProperty(indexed_string_property_builder.SetName("prop6"))
          .AddProperty(indexed_string_property_builder.SetName("prop7"))
          .AddProperty(indexed_string_property_builder.SetName("prop8"))
          .AddProperty(indexed_string_property_builder.SetName("prop9"))
          .AddProperty(indexed_string_property_builder.SetName("prop10"))
          .AddProperty(indexed_string_property_builder.SetName("prop11"))
          .AddProperty(indexed_string_property_builder.SetName("prop12"))
          .AddProperty(indexed_string_property_builder.SetName("prop13"))
          .AddProperty(indexed_string_property_builder.SetName("prop14"))
          .AddProperty(indexed_string_property_builder.SetName("prop15"))
          .AddProperty(indexed_string_property_builder.SetName("prop16"))
          .Build();

  // typeB is a deduped copy of typeA.
  SchemaTypeConfigProto typeB = SchemaTypeConfigBuilder(typeA)
                                    .SetType("TypeB")
                                    .BuildAndPopulatePropertiesDigest();
  typeB.clear_properties();

  SchemaProto schema = SchemaBuilder().AddType(typeA).AddType(typeB).Build();

  SchemaUtil::TypeConfigInfoCache type_config_info_cache =
      SchemaUtil::TypeConfigInfoCache(
          /*enable_schema_definition_deduping=*/true);
  SchemaUtil::BuildTypeConfigInfoCache(schema, &type_config_info_cache);
  // Check that TypeB is deduped.
  EXPECT_THAT(type_config_info_cache.IsSchemaTypeConfigDeduped("TypeA"),
              IsOkAndHolds(false));
  EXPECT_THAT(type_config_info_cache.IsSchemaTypeConfigDeduped("TypeB"),
              IsOkAndHolds(true));

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<DynamicTrieKeyMapper<SchemaTypeId>> type_id_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, schema_store_dir_,
                                                 /*maximum_size_bytes=*/10000));
  ASSERT_THAT(type_id_mapper->Put("TypeA", 0), IsOk());
  ASSERT_THAT(type_id_mapper->Put("TypeB", 1), IsOk());

  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaTypeManager> schema_type_manager,
      SchemaTypeManager::Create(type_config_info_cache, type_id_mapper.get()));

  BackupSchemaProducer backup_producer(feature_flags_.get());
  ICING_ASSERT_OK_AND_ASSIGN(
      BackupSchemaProducer::BackupSchemaResult result,
      backup_producer.Produce(schema, schema_type_manager->section_manager(),
                              type_config_info_cache));
  EXPECT_THAT(result.backup_schema_produced, Eq(true));

  // The Embedding Property should have been removed from the backup schema.
  // This would leave only 15 indexed properties which is under the old limit of
  // 16. All remaining properties should remain indexed.
  SchemaTypeConfigProto expected_typeA =
      SchemaTypeConfigBuilder()
          .SetType("TypeA")
          .AddProperty(indexed_string_property_builder.SetName("prop0"))
          .AddProperty(indexed_string_property_builder.SetName("prop1"))
          .AddProperty(indexed_string_property_builder.SetName("prop2"))
          .AddProperty(indexed_string_property_builder.SetName("prop3"))
          .AddProperty(indexed_string_property_builder.SetName("prop4"))
          .AddProperty(indexed_string_property_builder.SetName("prop6"))
          .AddProperty(indexed_string_property_builder.SetName("prop7"))
          .AddProperty(indexed_string_property_builder.SetName("prop8"))
          .AddProperty(indexed_string_property_builder.SetName("prop9"))
          .AddProperty(indexed_string_property_builder.SetName("prop10"))
          .AddProperty(indexed_string_property_builder.SetName("prop11"))
          .AddProperty(indexed_string_property_builder.SetName("prop12"))
          .AddProperty(indexed_string_property_builder.SetName("prop13"))
          .AddProperty(indexed_string_property_builder.SetName("prop14"))
          .AddProperty(indexed_string_property_builder.SetName("prop15"))
          .AddProperty(indexed_string_property_builder.SetName("prop16"))
          .Build();
  SchemaTypeConfigProto expected_typeB =
      SchemaTypeConfigBuilder(expected_typeA).SetType("TypeB").Build();
  SchemaProto expected_backup =
      SchemaBuilder().AddType(expected_typeA).AddType(expected_typeB).Build();
  EXPECT_THAT(result.backup_schema,
              EqualsSchemaProtoIgnorePropertiesDigest(expected_backup));
}

INSTANTIATE_TEST_SUITE_P(
    BackupSchemaProducerTest, BackupSchemaProducerTest,
    testing::Values(FeatureFlags(
                        /*allow_circular_schema_definitions=*/true,
                        /*enable_repeated_field_joins=*/true,
                        /*enable_embedding_backup_generation=*/false,
                        /*enable_schema_database=*/true,
                        /*enable_smaller_decompression_buffer_size=*/true,
                        /*enable_passing_filter_to_children=*/true,
                        /*enable_proto_log_new_header_format=*/true,
                        /*enable_reusable_decompression_buffer=*/true,
                        /*enable_schema_type_id_optimization=*/true,
                        /*enable_optimize_improvements=*/true,
                        /*expired_document_purge_threshold_ms=*/0,
                        /*enable_non_existent_qualified_id_join=*/true,
                        /*enable_skip_set_schema_type_equality_check=*/true,
                        /*enable_embed_query_optimization=*/true,
                        /*enable_schema_definition_deduping=*/true),
                    FeatureFlags(
                        /*allow_circular_schema_definitions=*/true,
                        /*enable_repeated_field_joins=*/true,
                        /*enable_embedding_backup_generation=*/true,
                        /*enable_schema_database=*/true,
                        /*enable_smaller_decompression_buffer_size=*/true,
                        /*enable_passing_filter_to_children=*/true,
                        /*enable_proto_log_new_header_format=*/true,
                        /*enable_reusable_decompression_buffer=*/true,
                        /*enable_schema_type_id_optimization=*/true,
                        /*enable_optimize_improvements=*/true,
                        /*expired_document_purge_threshold_ms=*/0,
                        /*enable_non_existent_qualified_id_join=*/true,
                        /*enable_skip_set_schema_type_equality_check=*/true,
                        /*enable_embed_query_optimization=*/true,
                        /*enable_schema_definition_deduping=*/true)));

}  // namespace

}  // namespace lib
}  // namespace icing
