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

#include "icing/schema/section-manager.h"

#include <limits>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-util.h"
#include "icing/store/dynamic-trie-key-mapper.h"
#include "icing/store/key-mapper.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::testing::ElementsAre;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::SizeIs;

// type and property names of EmailMessage
constexpr char kTypeEmail[] = "EmailMessage";
constexpr char kPropertySubject[] = "subject";
constexpr char kPropertyText[] = "text";
constexpr char kPropertyAttachment[] = "attachment";
constexpr char kPropertyRecipients[] = "recipients";
constexpr char kPropertyRecipientIds[] = "recipientIds";
constexpr char kPropertyTimestamp[] = "timestamp";
constexpr char kPropertyNonIndexableInteger[] = "non_indexable_integer";
// type and property names of Conversation
constexpr char kTypeConversation[] = "Conversation";
constexpr char kPropertyName[] = "name";
constexpr char kPropertyEmails[] = "emails";

constexpr int64_t kDefaultTimestamp = 1663274901;

class SectionManagerTest : public ::testing::Test {
 protected:
  SectionManagerTest() : test_dir_(GetTestTempDir() + "/icing") {
    auto email_type = CreateEmailTypeConfig();
    auto conversation_type = CreateConversationTypeConfig();
    type_config_map_.emplace(email_type.schema_type(), email_type);
    type_config_map_.emplace(conversation_type.schema_type(),
                             conversation_type);

    email_document_ =
        DocumentBuilder()
            .SetKey("icing", "email/1")
            .SetSchema(kTypeEmail)
            .AddStringProperty(kPropertySubject, "the subject")
            .AddStringProperty(kPropertyText, "the text")
            .AddBytesProperty(kPropertyAttachment, "attachment bytes")
            .AddStringProperty(kPropertyRecipients, "recipient1", "recipient2",
                               "recipient3")
            .AddInt64Property(kPropertyRecipientIds, 1, 2, 3)
            .AddInt64Property(kPropertyTimestamp, kDefaultTimestamp)
            .AddInt64Property(kPropertyNonIndexableInteger, 100)
            .Build();

    conversation_document_ =
        DocumentBuilder()
            .SetKey("icing", "conversation/1")
            .SetSchema(kTypeConversation)
            .AddDocumentProperty(kPropertyEmails,
                                 DocumentProto(email_document_),
                                 DocumentProto(email_document_))
            .Build();
  }

  void SetUp() override {
    // DynamicTrieKeyMapper uses 3 internal arrays for bookkeeping. Give each
    // one 128KiB so the total DynamicTrieKeyMapper should get 384KiB
    int key_mapper_size = 3 * 128 * 1024;
    ICING_ASSERT_OK_AND_ASSIGN(schema_type_mapper_,
                               DynamicTrieKeyMapper<SchemaTypeId>::Create(
                                   filesystem_, test_dir_, key_mapper_size));
    ICING_ASSERT_OK(schema_type_mapper_->Put(kTypeEmail, 0));
    ICING_ASSERT_OK(schema_type_mapper_->Put(kTypeConversation, 1));
  }

  static SchemaTypeConfigProto CreateEmailTypeConfig() {
    SchemaTypeConfigProto type =
        SchemaTypeConfigBuilder()
            .SetType(kTypeEmail)
            .AddProperty(
                PropertyConfigBuilder()
                    .SetName(kPropertySubject)
                    .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                    .SetCardinality(CARDINALITY_REQUIRED))
            .AddProperty(
                PropertyConfigBuilder()
                    .SetName(kPropertyText)
                    .SetDataTypeString(TERM_MATCH_UNKNOWN, TOKENIZER_NONE)
                    .SetCardinality(CARDINALITY_OPTIONAL))
            .AddProperty(PropertyConfigBuilder()
                             .SetName(kPropertyAttachment)
                             .SetDataType(TYPE_BYTES)
                             .SetCardinality(CARDINALITY_REQUIRED))
            .AddProperty(
                PropertyConfigBuilder()
                    .SetName(kPropertyRecipients)
                    .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                    .SetCardinality(CARDINALITY_REPEATED))
            .AddProperty(PropertyConfigBuilder()
                             .SetName(kPropertyRecipientIds)
                             .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                             .SetCardinality(CARDINALITY_REPEATED))
            .AddProperty(PropertyConfigBuilder()
                             .SetName(kPropertyTimestamp)
                             .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                             .SetCardinality(CARDINALITY_REQUIRED))
            .AddProperty(PropertyConfigBuilder()
                             .SetName(kPropertyNonIndexableInteger)
                             .SetDataType(TYPE_INT64)
                             .SetCardinality(CARDINALITY_REQUIRED))
            .Build();
    return type;
  }

  static SchemaTypeConfigProto CreateConversationTypeConfig() {
    SchemaTypeConfigProto type;
    type.set_schema_type(kTypeConversation);

    auto name = type.add_properties();
    name->set_property_name(kPropertyName);
    name->set_data_type(TYPE_STRING);
    name->set_cardinality(CARDINALITY_OPTIONAL);
    name->mutable_string_indexing_config()->set_term_match_type(
        TERM_MATCH_EXACT);

    auto emails = type.add_properties();
    emails->set_property_name(kPropertyEmails);
    emails->set_data_type(TYPE_DOCUMENT);
    emails->set_cardinality(CARDINALITY_REPEATED);
    emails->set_schema_type(kTypeEmail);
    emails->mutable_document_indexing_config()->set_index_nested_properties(
        true);

    return type;
  }

  Filesystem filesystem_;
  const std::string test_dir_;
  SchemaUtil::TypeConfigMap type_config_map_;
  std::unique_ptr<KeyMapper<SchemaTypeId>> schema_type_mapper_;

  DocumentProto email_document_;
  DocumentProto conversation_document_;
};

TEST_F(SectionManagerTest, CreationWithNullPointerShouldFail) {
  EXPECT_THAT(
      SectionManager::Create(type_config_map_, /*schema_type_mapper=*/nullptr),
      StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
}

TEST_F(SectionManagerTest, CreationWithTooManyPropertiesShouldFail) {
  SchemaTypeConfigProto type_config;
  type_config.set_schema_type("type");
  // Adds more properties than allowed
  int max_num_sections_allowed = kMaxSectionId - kMinSectionId + 1;
  for (int i = 0; i < max_num_sections_allowed + 1; i++) {
    auto property = type_config.add_properties();
    property->set_property_name("property" + std::to_string(i));
    property->set_data_type(TYPE_STRING);
    property->set_cardinality(CARDINALITY_REQUIRED);
    property->mutable_string_indexing_config()->set_term_match_type(
        TERM_MATCH_EXACT);
  }

  SchemaUtil::TypeConfigMap type_config_map;
  type_config_map.emplace("type", type_config);

  EXPECT_THAT(
      SectionManager::Create(type_config_map, schema_type_mapper_.get()),
      StatusIs(libtextclassifier3::StatusCode::OUT_OF_RANGE,
               HasSubstr("Too many properties")));
}

TEST_F(SectionManagerTest, GetSectionContent) {
  ICING_ASSERT_OK_AND_ASSIGN(
      auto section_manager,
      SectionManager::Create(type_config_map_, schema_type_mapper_.get()));

  // Test simple section paths
  EXPECT_THAT(section_manager->GetSectionContent<std::string_view>(
                  email_document_,
                  /*section_path=*/"subject"),
              IsOkAndHolds(ElementsAre("the subject")));
  EXPECT_THAT(section_manager->GetSectionContent<std::string_view>(
                  email_document_,
                  /*section_path=*/"text"),
              IsOkAndHolds(ElementsAre("the text")));
  EXPECT_THAT(
      section_manager->GetSectionContent<int64_t>(email_document_,
                                                  /*section_path=*/"timestamp"),
      IsOkAndHolds(ElementsAre(kDefaultTimestamp)));
}

TEST_F(SectionManagerTest, GetSectionContentRepeatedValues) {
  ICING_ASSERT_OK_AND_ASSIGN(
      auto section_manager,
      SectionManager::Create(type_config_map_, schema_type_mapper_.get()));

  // Test repeated values
  EXPECT_THAT(
      section_manager->GetSectionContent<std::string_view>(
          email_document_,
          /*section_path=*/"recipients"),
      IsOkAndHolds(ElementsAre("recipient1", "recipient2", "recipient3")));
  EXPECT_THAT(section_manager->GetSectionContent<int64_t>(
                  email_document_,
                  /*section_path=*/"recipientIds"),
              IsOkAndHolds(ElementsAre(1, 2, 3)));
}

TEST_F(SectionManagerTest, GetSectionContentConcatenatedSectionPaths) {
  ICING_ASSERT_OK_AND_ASSIGN(
      auto section_manager,
      SectionManager::Create(type_config_map_, schema_type_mapper_.get()));

  // Test concatenated section paths: "property1.property2"
  EXPECT_THAT(section_manager->GetSectionContent<std::string_view>(
                  conversation_document_,
                  /*section_path=*/"emails.subject"),
              IsOkAndHolds(ElementsAre("the subject", "the subject")));
  EXPECT_THAT(section_manager->GetSectionContent<std::string_view>(
                  conversation_document_,
                  /*section_path=*/"emails.text"),
              IsOkAndHolds(ElementsAre("the text", "the text")));
  EXPECT_THAT(section_manager->GetSectionContent<int64_t>(
                  conversation_document_,
                  /*section_path=*/"emails.timestamp"),
              IsOkAndHolds(ElementsAre(kDefaultTimestamp, kDefaultTimestamp)));
  EXPECT_THAT(
      section_manager->GetSectionContent<std::string_view>(
          conversation_document_,
          /*section_path=*/"emails.recipients"),
      IsOkAndHolds(ElementsAre("recipient1", "recipient2", "recipient3",
                               "recipient1", "recipient2", "recipient3")));
  EXPECT_THAT(section_manager->GetSectionContent<int64_t>(
                  conversation_document_,
                  /*section_path=*/"emails.recipientIds"),
              IsOkAndHolds(ElementsAre(1, 2, 3, 1, 2, 3)));
}

TEST_F(SectionManagerTest, GetSectionContentNonExistingPaths) {
  ICING_ASSERT_OK_AND_ASSIGN(
      auto section_manager,
      SectionManager::Create(type_config_map_, schema_type_mapper_.get()));

  // Test non-existing paths
  EXPECT_THAT(section_manager->GetSectionContent<std::string_view>(
                  email_document_,
                  /*section_path=*/"name"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(section_manager->GetSectionContent<std::string_view>(
                  email_document_,
                  /*section_path=*/"invalid"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(section_manager->GetSectionContent<std::string_view>(
                  conversation_document_,
                  /*section_path=*/"emails.invalid"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(SectionManagerTest, GetSectionContentNonIndexableTypes) {
  ICING_ASSERT_OK_AND_ASSIGN(
      auto section_manager,
      SectionManager::Create(type_config_map_, schema_type_mapper_.get()));

  // Test other data types
  // BYTES type can't be indexed, so content won't be returned
  EXPECT_THAT(section_manager->GetSectionContent<std::string_view>(
                  email_document_,
                  /*section_path=*/"attachment"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(SectionManagerTest, GetSectionContentMismatchedType) {
  ICING_ASSERT_OK_AND_ASSIGN(
      auto section_manager,
      SectionManager::Create(type_config_map_, schema_type_mapper_.get()));

  // Use the wrong template type to get the indexable content. GetSectionContent
  // should get empty content from the corresponding proto (repeated) field and
  // return NOT_FOUND error.
  EXPECT_THAT(section_manager->GetSectionContent<std::string_view>(
                  email_document_,
                  /*section_path=*/"recipientIds"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(section_manager->GetSectionContent<int64_t>(
                  email_document_,
                  /*section_path=*/"recipients"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

// The following tests are similar to the ones above but use section ids
// instead of section paths
TEST_F(SectionManagerTest, GetSectionContentBySectionId) {
  ICING_ASSERT_OK_AND_ASSIGN(
      auto section_manager,
      SectionManager::Create(type_config_map_, schema_type_mapper_.get()));

  // EmailMessage (section id -> section path):
  SectionId recipient_ids_section_id = 0;
  SectionId recipients_section_id = 1;
  SectionId subject_section_id = 2;
  SectionId timestamp_section_id = 3;
  SectionId invalid_email_section_id = 4;
  EXPECT_THAT(section_manager->GetSectionContent<int64_t>(
                  email_document_, recipient_ids_section_id),
              IsOkAndHolds(ElementsAre(1, 2, 3)));
  EXPECT_THAT(
      section_manager->GetSectionContent<std::string_view>(
          email_document_, recipients_section_id),
      IsOkAndHolds(ElementsAre("recipient1", "recipient2", "recipient3")));
  EXPECT_THAT(section_manager->GetSectionContent<std::string_view>(
                  email_document_, subject_section_id),
              IsOkAndHolds(ElementsAre("the subject")));
  EXPECT_THAT(section_manager->GetSectionContent<int64_t>(email_document_,
                                                          timestamp_section_id),
              IsOkAndHolds(ElementsAre(kDefaultTimestamp)));

  EXPECT_THAT(section_manager->GetSectionContent<std::string_view>(
                  email_document_, invalid_email_section_id),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  // Conversation (section id -> section path):
  //   0 -> emails.recipientIds
  //   1 -> emails.recipients
  //   2 -> emails.subject
  //   3 -> emails.timestamp
  //   4 -> name
  SectionId emails_recipient_ids_section_id = 0;
  SectionId emails_recipients_section_id = 1;
  SectionId emails_subject_section_id = 2;
  SectionId emails_timestamp_section_id = 3;
  SectionId name_section_id = 4;
  SectionId invalid_conversation_section_id = 5;
  EXPECT_THAT(section_manager->GetSectionContent<int64_t>(
                  conversation_document_, emails_recipient_ids_section_id),
              IsOkAndHolds(ElementsAre(1, 2, 3, 1, 2, 3)));
  EXPECT_THAT(
      section_manager->GetSectionContent<std::string_view>(
          conversation_document_, emails_recipients_section_id),
      IsOkAndHolds(ElementsAre("recipient1", "recipient2", "recipient3",
                               "recipient1", "recipient2", "recipient3")));
  EXPECT_THAT(section_manager->GetSectionContent<std::string_view>(
                  conversation_document_, emails_subject_section_id),
              IsOkAndHolds(ElementsAre("the subject", "the subject")));
  EXPECT_THAT(section_manager->GetSectionContent<int64_t>(
                  conversation_document_, emails_timestamp_section_id),
              IsOkAndHolds(ElementsAre(kDefaultTimestamp, kDefaultTimestamp)));

  EXPECT_THAT(section_manager->GetSectionContent<std::string_view>(
                  conversation_document_, name_section_id),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(section_manager->GetSectionContent<std::string_view>(
                  conversation_document_, invalid_conversation_section_id),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(SectionManagerTest, ExtractSections) {
  ICING_ASSERT_OK_AND_ASSIGN(
      auto section_manager,
      SectionManager::Create(type_config_map_, schema_type_mapper_.get()));

  // Extracts all sections from 'EmailMessage' document
  ICING_ASSERT_OK_AND_ASSIGN(SectionGroup section_group,
                             section_manager->ExtractSections(email_document_));

  // String sections
  EXPECT_THAT(section_group.string_sections, SizeIs(2));

  EXPECT_THAT(section_group.string_sections[0].metadata,
              Eq(SectionMetadata(
                  /*id_in=*/1, TYPE_STRING, TOKENIZER_PLAIN, TERM_MATCH_EXACT,
                  NUMERIC_MATCH_UNKNOWN,
                  /*path_in=*/"recipients")));
  EXPECT_THAT(section_group.string_sections[0].content,
              ElementsAre("recipient1", "recipient2", "recipient3"));

  EXPECT_THAT(section_group.string_sections[1].metadata,
              Eq(SectionMetadata(
                  /*id_in=*/2, TYPE_STRING, TOKENIZER_PLAIN, TERM_MATCH_EXACT,
                  NUMERIC_MATCH_UNKNOWN,
                  /*path_in=*/"subject")));
  EXPECT_THAT(section_group.string_sections[1].content,
              ElementsAre("the subject"));

  // Integer sections
  EXPECT_THAT(section_group.integer_sections, SizeIs(2));

  EXPECT_THAT(section_group.integer_sections[0].metadata,
              Eq(SectionMetadata(/*id_in=*/0, TYPE_INT64, TOKENIZER_NONE,
                                 TERM_MATCH_UNKNOWN, NUMERIC_MATCH_RANGE,
                                 /*path_in=*/"recipientIds")));
  EXPECT_THAT(section_group.integer_sections[0].content, ElementsAre(1, 2, 3));

  EXPECT_THAT(section_group.integer_sections[1].metadata,
              Eq(SectionMetadata(/*id_in=*/3, TYPE_INT64, TOKENIZER_NONE,
                                 TERM_MATCH_UNKNOWN, NUMERIC_MATCH_RANGE,
                                 /*path_in=*/"timestamp")));
  EXPECT_THAT(section_group.integer_sections[1].content,
              ElementsAre(kDefaultTimestamp));
}

TEST_F(SectionManagerTest, ExtractSectionsNested) {
  ICING_ASSERT_OK_AND_ASSIGN(
      auto section_manager,
      SectionManager::Create(type_config_map_, schema_type_mapper_.get()));

  // Extracts all sections from 'Conversation' document
  ICING_ASSERT_OK_AND_ASSIGN(
      SectionGroup section_group,
      section_manager->ExtractSections(conversation_document_));

  // String sections
  EXPECT_THAT(section_group.string_sections, SizeIs(2));

  EXPECT_THAT(section_group.string_sections[0].metadata,
              Eq(SectionMetadata(
                  /*id_in=*/1, TYPE_STRING, TOKENIZER_PLAIN, TERM_MATCH_EXACT,
                  NUMERIC_MATCH_UNKNOWN,
                  /*path_in=*/"emails.recipients")));
  EXPECT_THAT(section_group.string_sections[0].content,
              ElementsAre("recipient1", "recipient2", "recipient3",
                          "recipient1", "recipient2", "recipient3"));

  EXPECT_THAT(section_group.string_sections[1].metadata,
              Eq(SectionMetadata(
                  /*id_in=*/2, TYPE_STRING, TOKENIZER_PLAIN, TERM_MATCH_EXACT,
                  NUMERIC_MATCH_UNKNOWN,
                  /*path_in=*/"emails.subject")));
  EXPECT_THAT(section_group.string_sections[1].content,
              ElementsAre("the subject", "the subject"));

  // Integer sections
  EXPECT_THAT(section_group.integer_sections, SizeIs(2));

  EXPECT_THAT(section_group.integer_sections[0].metadata,
              Eq(SectionMetadata(/*id_in=*/0, TYPE_INT64, TOKENIZER_NONE,
                                 TERM_MATCH_UNKNOWN, NUMERIC_MATCH_RANGE,
                                 /*path_in=*/"emails.recipientIds")));
  EXPECT_THAT(section_group.integer_sections[0].content,
              ElementsAre(1, 2, 3, 1, 2, 3));

  EXPECT_THAT(section_group.integer_sections[1].metadata,
              Eq(SectionMetadata(/*id_in=*/3, TYPE_INT64, TOKENIZER_NONE,
                                 TERM_MATCH_UNKNOWN, NUMERIC_MATCH_RANGE,
                                 /*path_in=*/"emails.timestamp")));
  EXPECT_THAT(section_group.integer_sections[1].content,
              ElementsAre(kDefaultTimestamp, kDefaultTimestamp));
}

TEST_F(SectionManagerTest,
       NonStringFieldsWithStringIndexingConfigDontCreateSections) {
  // Create a schema for an empty document.
  SchemaTypeConfigProto empty_type;
  empty_type.set_schema_type("EmptySchema");

  // Create a schema with all the non-string fields
  SchemaTypeConfigProto type_with_non_string_properties;
  type_with_non_string_properties.set_schema_type("Schema");

  // Create an int property with a string_indexing_config
  auto int_property = type_with_non_string_properties.add_properties();
  int_property->set_property_name("int");
  int_property->set_data_type(TYPE_INT64);
  int_property->set_cardinality(CARDINALITY_REQUIRED);
  int_property->mutable_string_indexing_config()->set_term_match_type(
      TERM_MATCH_EXACT);
  int_property->mutable_string_indexing_config()->set_tokenizer_type(
      TOKENIZER_PLAIN);

  // Create a double property with a string_indexing_config
  auto double_property = type_with_non_string_properties.add_properties();
  double_property->set_property_name("double");
  double_property->set_data_type(TYPE_DOUBLE);
  double_property->set_cardinality(CARDINALITY_REQUIRED);
  double_property->mutable_string_indexing_config()->set_term_match_type(
      TERM_MATCH_EXACT);
  double_property->mutable_string_indexing_config()->set_tokenizer_type(
      TOKENIZER_PLAIN);

  // Create a boolean property with a string_indexing_config
  auto boolean_property = type_with_non_string_properties.add_properties();
  boolean_property->set_property_name("boolean");
  boolean_property->set_data_type(TYPE_BOOLEAN);
  boolean_property->set_cardinality(CARDINALITY_REQUIRED);
  boolean_property->mutable_string_indexing_config()->set_term_match_type(
      TERM_MATCH_EXACT);
  boolean_property->mutable_string_indexing_config()->set_tokenizer_type(
      TOKENIZER_PLAIN);

  // Create a bytes property with a string_indexing_config
  auto bytes_property = type_with_non_string_properties.add_properties();
  bytes_property->set_property_name("bytes");
  bytes_property->set_data_type(TYPE_BYTES);
  bytes_property->set_cardinality(CARDINALITY_REQUIRED);
  bytes_property->mutable_string_indexing_config()->set_term_match_type(
      TERM_MATCH_EXACT);
  bytes_property->mutable_string_indexing_config()->set_tokenizer_type(
      TOKENIZER_PLAIN);

  // Create a document property with a string_indexing_config
  auto document_property = type_with_non_string_properties.add_properties();
  document_property->set_property_name("document");
  document_property->set_data_type(TYPE_DOCUMENT);
  document_property->set_schema_type(empty_type.schema_type());
  document_property->set_cardinality(CARDINALITY_REQUIRED);
  document_property->mutable_string_indexing_config()->set_term_match_type(
      TERM_MATCH_EXACT);
  document_property->mutable_string_indexing_config()->set_tokenizer_type(
      TOKENIZER_PLAIN);

  // Setup classes to create the section manager
  SchemaUtil::TypeConfigMap type_config_map;
  type_config_map.emplace(type_with_non_string_properties.schema_type(),
                          type_with_non_string_properties);
  type_config_map.emplace(empty_type.schema_type(), empty_type);

  // DynamicTrieKeyMapper uses 3 internal arrays for bookkeeping. Give each one
  // 128KiB so the total DynamicTrieKeyMapper should get 384KiB
  int key_mapper_size = 3 * 128 * 1024;
  std::string dir = GetTestTempDir() + "/non_string_fields";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<KeyMapper<SchemaTypeId>> schema_type_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, dir,
                                                 key_mapper_size));
  ICING_ASSERT_OK(schema_type_mapper->Put(
      type_with_non_string_properties.schema_type(), /*schema_type_id=*/0));
  ICING_ASSERT_OK(schema_type_mapper->Put(empty_type.schema_type(),
                                          /*schema_type_id=*/1));

  ICING_ASSERT_OK_AND_ASSIGN(
      auto section_manager,
      SectionManager::Create(type_config_map, schema_type_mapper.get()));

  // Create an empty document to be nested
  DocumentProto empty_document = DocumentBuilder()
                                     .SetKey("icing", "uri1")
                                     .SetSchema(empty_type.schema_type())
                                     .Build();

  // Create a document that follows "Schema"
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "uri2")
          .SetSchema(type_with_non_string_properties.schema_type())
          .AddInt64Property("int", 1)
          .AddDoubleProperty("double", 0.2)
          .AddBooleanProperty("boolean", true)
          .AddBytesProperty("bytes", "attachment bytes")
          .AddDocumentProperty("document", empty_document)
          .Build();

  // Extracts sections from 'Schema' document
  ICING_ASSERT_OK_AND_ASSIGN(SectionGroup section_group,
                             section_manager->ExtractSections(document));
  EXPECT_THAT(section_group.string_sections, IsEmpty());
  EXPECT_THAT(section_group.integer_sections, IsEmpty());
}

TEST_F(SectionManagerTest,
       NonIntegerFieldsWithIntegerIndexingConfigDontCreateSections) {
  // Create a schema for an empty document.
  SchemaTypeConfigProto empty_type;
  empty_type.set_schema_type("EmptySchema");

  // Create a schema with all the non-integer fields
  SchemaTypeConfigProto type_with_non_integer_properties;
  type_with_non_integer_properties.set_schema_type("Schema");

  // Create an string property with a integer_indexing_config
  auto string_property = type_with_non_integer_properties.add_properties();
  string_property->set_property_name("string");
  string_property->set_data_type(TYPE_STRING);
  string_property->set_cardinality(CARDINALITY_REQUIRED);
  string_property->mutable_integer_indexing_config()->set_numeric_match_type(
      NUMERIC_MATCH_RANGE);

  // Create a double property with a integer_indexing_config
  auto double_property = type_with_non_integer_properties.add_properties();
  double_property->set_property_name("double");
  double_property->set_data_type(TYPE_DOUBLE);
  double_property->set_cardinality(CARDINALITY_REQUIRED);
  double_property->mutable_integer_indexing_config()->set_numeric_match_type(
      NUMERIC_MATCH_RANGE);

  // Create a boolean property with a integer_indexing_config
  auto boolean_property = type_with_non_integer_properties.add_properties();
  boolean_property->set_property_name("boolean");
  boolean_property->set_data_type(TYPE_BOOLEAN);
  boolean_property->set_cardinality(CARDINALITY_REQUIRED);
  boolean_property->mutable_integer_indexing_config()->set_numeric_match_type(
      NUMERIC_MATCH_RANGE);

  // Create a bytes property with a integer_indexing_config
  auto bytes_property = type_with_non_integer_properties.add_properties();
  bytes_property->set_property_name("bytes");
  bytes_property->set_data_type(TYPE_BYTES);
  bytes_property->set_cardinality(CARDINALITY_REQUIRED);
  bytes_property->mutable_integer_indexing_config()->set_numeric_match_type(
      NUMERIC_MATCH_RANGE);

  // Create a document property with a integer_indexing_config
  auto document_property = type_with_non_integer_properties.add_properties();
  document_property->set_property_name("document");
  document_property->set_data_type(TYPE_DOCUMENT);
  document_property->set_schema_type(empty_type.schema_type());
  document_property->set_cardinality(CARDINALITY_REQUIRED);
  document_property->mutable_integer_indexing_config()->set_numeric_match_type(
      NUMERIC_MATCH_RANGE);

  // Setup classes to create the section manager
  SchemaUtil::TypeConfigMap type_config_map;
  type_config_map.emplace(type_with_non_integer_properties.schema_type(),
                          type_with_non_integer_properties);
  type_config_map.emplace(empty_type.schema_type(), empty_type);

  // DynamicTrieKeyMapper uses 3 internal arrays for bookkeeping. Give each one
  // 128KiB so the total DynamicTrieKeyMapper should get 384KiB
  int key_mapper_size = 3 * 128 * 1024;
  std::string dir = GetTestTempDir() + "/non_integer_fields";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<KeyMapper<SchemaTypeId>> schema_type_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, dir,
                                                 key_mapper_size));
  ICING_ASSERT_OK(schema_type_mapper->Put(
      type_with_non_integer_properties.schema_type(), /*schema_type_id=*/0));
  ICING_ASSERT_OK(schema_type_mapper->Put(empty_type.schema_type(),
                                          /*schema_type_id=*/1));

  ICING_ASSERT_OK_AND_ASSIGN(
      auto section_manager,
      SectionManager::Create(type_config_map, schema_type_mapper.get()));

  // Create an empty document to be nested
  DocumentProto empty_document = DocumentBuilder()
                                     .SetKey("icing", "uri1")
                                     .SetSchema(empty_type.schema_type())
                                     .Build();

  // Create a document that follows "Schema"
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "uri2")
          .SetSchema(type_with_non_integer_properties.schema_type())
          .AddStringProperty("string", "abc")
          .AddDoubleProperty("double", 0.2)
          .AddBooleanProperty("boolean", true)
          .AddBytesProperty("bytes", "attachment bytes")
          .AddDocumentProperty("document", empty_document)
          .Build();

  // Extracts sections from 'Schema' document
  ICING_ASSERT_OK_AND_ASSIGN(SectionGroup section_group,
                             section_manager->ExtractSections(document));
  EXPECT_THAT(section_group.string_sections, IsEmpty());
  EXPECT_THAT(section_group.integer_sections, IsEmpty());
}

TEST_F(SectionManagerTest, AssignSectionsRecursivelyForDocumentFields) {
  // Create the inner schema that the document property is.
  SchemaTypeConfigProto document_type;
  document_type.set_schema_type("DocumentSchema");

  auto string_property = document_type.add_properties();
  string_property->set_property_name("string");
  string_property->set_data_type(TYPE_STRING);
  string_property->set_cardinality(CARDINALITY_REQUIRED);
  string_property->mutable_string_indexing_config()->set_term_match_type(
      TERM_MATCH_EXACT);
  string_property->mutable_string_indexing_config()->set_tokenizer_type(
      TOKENIZER_PLAIN);

  auto integer_property = document_type.add_properties();
  integer_property->set_property_name("integer");
  integer_property->set_data_type(TYPE_INT64);
  integer_property->set_cardinality(CARDINALITY_REQUIRED);
  integer_property->mutable_integer_indexing_config()->set_numeric_match_type(
      NUMERIC_MATCH_RANGE);

  // Create the outer schema which has the document property.
  SchemaTypeConfigProto type;
  type.set_schema_type("Schema");

  auto document_property = type.add_properties();
  document_property->set_property_name("document");
  document_property->set_data_type(TYPE_DOCUMENT);
  document_property->set_schema_type(document_type.schema_type());
  document_property->set_cardinality(CARDINALITY_REQUIRED);

  // Opt into recursing into the document fields.
  document_property->mutable_document_indexing_config()
      ->set_index_nested_properties(true);

  // Create the inner document.
  DocumentProto inner_document = DocumentBuilder()
                                     .SetKey("icing", "uri1")
                                     .SetSchema(document_type.schema_type())
                                     .AddStringProperty("string", "foo")
                                     .AddInt64Property("integer", 123)
                                     .Build();

  // Create the outer document that holds the inner document
  DocumentProto outer_document =
      DocumentBuilder()
          .SetKey("icing", "uri2")
          .SetSchema(type.schema_type())
          .AddDocumentProperty("document", inner_document)
          .Build();

  // Setup classes to create the section manager
  SchemaUtil::TypeConfigMap type_config_map;
  type_config_map.emplace(type.schema_type(), type);
  type_config_map.emplace(document_type.schema_type(), document_type);

  // DynamicTrieKeyMapper uses 3 internal arrays for bookkeeping. Give each one
  // 128KiB so the total DynamicTrieKeyMapper should get 384KiB
  int key_mapper_size = 3 * 128 * 1024;
  std::string dir = GetTestTempDir() + "/recurse_into_document";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<KeyMapper<SchemaTypeId>> schema_type_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, dir,
                                                 key_mapper_size));
  int type_schema_type_id = 0;
  int document_type_schema_type_id = 1;
  ICING_ASSERT_OK(
      schema_type_mapper->Put(type.schema_type(), type_schema_type_id));
  ICING_ASSERT_OK(schema_type_mapper->Put(document_type.schema_type(),
                                          document_type_schema_type_id));

  ICING_ASSERT_OK_AND_ASSIGN(
      auto section_manager,
      SectionManager::Create(type_config_map, schema_type_mapper.get()));

  // Extracts sections from 'Schema' document; there should be the 1 string
  // property and 1 integer property inside the document.
  ICING_ASSERT_OK_AND_ASSIGN(SectionGroup section_group,
                             section_manager->ExtractSections(outer_document));
  EXPECT_THAT(section_group.string_sections, SizeIs(1));
  EXPECT_THAT(section_group.integer_sections, SizeIs(1));
}

TEST_F(SectionManagerTest, DontAssignSectionsRecursivelyForDocumentFields) {
  // Create the inner schema that the document property is.
  SchemaTypeConfigProto document_type;
  document_type.set_schema_type("DocumentSchema");

  auto string_property = document_type.add_properties();
  string_property->set_property_name("string");
  string_property->set_data_type(TYPE_STRING);
  string_property->set_cardinality(CARDINALITY_REQUIRED);
  string_property->mutable_string_indexing_config()->set_term_match_type(
      TERM_MATCH_EXACT);
  string_property->mutable_string_indexing_config()->set_tokenizer_type(
      TOKENIZER_PLAIN);

  auto integer_property = document_type.add_properties();
  integer_property->set_property_name("integer");
  integer_property->set_data_type(TYPE_INT64);
  integer_property->set_cardinality(CARDINALITY_REQUIRED);
  integer_property->mutable_integer_indexing_config()->set_numeric_match_type(
      NUMERIC_MATCH_RANGE);

  // Create the outer schema which has the document property.
  SchemaTypeConfigProto type;
  type.set_schema_type("Schema");

  auto document_property = type.add_properties();
  document_property->set_property_name("document");
  document_property->set_data_type(TYPE_DOCUMENT);
  document_property->set_schema_type(document_type.schema_type());
  document_property->set_cardinality(CARDINALITY_REQUIRED);

  // Opt into recursing into the document fields.
  document_property->mutable_document_indexing_config()
      ->set_index_nested_properties(false);

  // Create the inner document.
  DocumentProto inner_document = DocumentBuilder()
                                     .SetKey("icing", "uri1")
                                     .SetSchema(document_type.schema_type())
                                     .AddStringProperty("string", "foo")
                                     .AddInt64Property("integer", 123)
                                     .Build();

  // Create the outer document that holds the inner document
  DocumentProto outer_document =
      DocumentBuilder()
          .SetKey("icing", "uri2")
          .SetSchema(type.schema_type())
          .AddDocumentProperty("document", inner_document)
          .Build();

  // Setup classes to create the section manager
  SchemaUtil::TypeConfigMap type_config_map;
  type_config_map.emplace(type.schema_type(), type);
  type_config_map.emplace(document_type.schema_type(), document_type);

  // DynamicTrieKeyMapper uses 3 internal arrays for bookkeeping. Give each one
  // 128KiB so the total DynamicTrieKeyMapper should get 384KiB
  int key_mapper_size = 3 * 128 * 1024;
  std::string dir = GetTestTempDir() + "/recurse_into_document";
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<KeyMapper<SchemaTypeId>> schema_type_mapper,
      DynamicTrieKeyMapper<SchemaTypeId>::Create(filesystem_, dir,
                                                 key_mapper_size));
  int type_schema_type_id = 0;
  int document_type_schema_type_id = 1;
  ICING_ASSERT_OK(
      schema_type_mapper->Put(type.schema_type(), type_schema_type_id));
  ICING_ASSERT_OK(schema_type_mapper->Put(document_type.schema_type(),
                                          document_type_schema_type_id));

  ICING_ASSERT_OK_AND_ASSIGN(
      auto section_manager,
      SectionManager::Create(type_config_map, schema_type_mapper.get()));

  // Extracts sections from 'Schema' document; there won't be any since we
  // didn't recurse into the document to see the inner string property
  ICING_ASSERT_OK_AND_ASSIGN(SectionGroup section_group,
                             section_manager->ExtractSections(outer_document));
  EXPECT_THAT(section_group.string_sections, IsEmpty());
  EXPECT_THAT(section_group.integer_sections, IsEmpty());
}

}  // namespace

}  // namespace lib
}  // namespace icing
