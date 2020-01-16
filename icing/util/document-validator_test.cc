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

#include "icing/util/document-validator.h"

#include <cstdint>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/proto/schema.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {
using ::testing::HasSubstr;

// type and property names of EmailMessage
constexpr char kTypeEmail[] = "EmailMessage";
constexpr char kPropertySubject[] = "subject";
constexpr char kPropertyText[] = "text";
constexpr char kPropertyRecipients[] = "recipients";
// type and property names of Conversation
constexpr char kTypeConversation[] = "Conversation";
constexpr char kPropertyName[] = "name";
constexpr char kPropertyEmails[] = "emails";
// Other values
constexpr char kDefaultNamespace[] = "icing";
constexpr char kDefaultString[] = "This is a string.";

class DocumentValidatorTest : public ::testing::Test {
 protected:
  DocumentValidatorTest() {}

  void SetUp() override {
    SchemaProto schema;
    auto type_config = schema.add_types();
    CreateEmailTypeConfig(type_config);

    type_config = schema.add_types();
    CreateConversationTypeConfig(type_config);

    ICING_ASSERT_OK_AND_ASSIGN(
        schema_store_, SchemaStore::Create(&filesystem_, GetTestTempDir()));
    ASSERT_THAT(schema_store_->SetSchema(schema), IsOk());

    document_validator_ =
        std::make_unique<DocumentValidator>(schema_store_.get());
  }

  static void CreateEmailTypeConfig(SchemaTypeConfigProto* type_config) {
    type_config->set_schema_type(kTypeEmail);

    auto subject = type_config->add_properties();
    subject->set_property_name(kPropertySubject);
    subject->set_data_type(PropertyConfigProto::DataType::STRING);
    subject->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);

    auto text = type_config->add_properties();
    text->set_property_name(kPropertyText);
    text->set_data_type(PropertyConfigProto::DataType::STRING);
    text->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

    auto recipients = type_config->add_properties();
    recipients->set_property_name(kPropertyRecipients);
    recipients->set_data_type(PropertyConfigProto::DataType::STRING);
    recipients->set_cardinality(PropertyConfigProto::Cardinality::REPEATED);
  }

  static DocumentBuilder SimpleEmailBuilder() {
    return DocumentBuilder()
        .SetKey(kDefaultNamespace, "email/1")
        .SetSchema(kTypeEmail)
        .AddStringProperty(kPropertySubject, kDefaultString)
        .AddStringProperty(kPropertyText, kDefaultString)
        .AddStringProperty(kPropertyRecipients, kDefaultString, kDefaultString,
                           kDefaultString);
  }

  static void CreateConversationTypeConfig(SchemaTypeConfigProto* type_config) {
    type_config->set_schema_type(kTypeConversation);

    auto name = type_config->add_properties();
    name->set_property_name(kPropertyName);
    name->set_data_type(PropertyConfigProto::DataType::STRING);
    name->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);

    auto emails = type_config->add_properties();
    emails->set_property_name(kPropertyEmails);
    emails->set_data_type(PropertyConfigProto::DataType::DOCUMENT);
    emails->set_cardinality(PropertyConfigProto::Cardinality::REPEATED);
    emails->set_schema_type(kTypeEmail);
  }

  static DocumentBuilder SimpleConversationBuilder() {
    return DocumentBuilder()
        .SetKey(kDefaultNamespace, "conversation/1")
        .SetSchema(kTypeConversation)
        .AddStringProperty(kPropertyName, kDefaultString)
        .AddDocumentProperty(kPropertyEmails, SimpleEmailBuilder().Build(),
                             SimpleEmailBuilder().Build(),
                             SimpleEmailBuilder().Build());
  }

  std::unique_ptr<DocumentValidator> document_validator_;
  std::unique_ptr<SchemaStore> schema_store_;
  Filesystem filesystem_;
};

TEST_F(DocumentValidatorTest, ValidateSimpleSchemasOk) {
  DocumentProto email = SimpleEmailBuilder().Build();
  EXPECT_THAT(document_validator_->Validate(email), IsOk());

  DocumentProto conversation = SimpleConversationBuilder().Build();
  EXPECT_THAT(document_validator_->Validate(conversation), IsOk());
}

TEST_F(DocumentValidatorTest, ValidateEmptyNamespaceInvalid) {
  DocumentProto email = SimpleEmailBuilder().SetNamespace("").Build();
  EXPECT_THAT(document_validator_->Validate(email),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("'namespace' is empty")));
}

TEST_F(DocumentValidatorTest, ValidateEmptyUriInvalid) {
  DocumentProto email = SimpleEmailBuilder().SetUri("").Build();
  EXPECT_THAT(document_validator_->Validate(email),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("'uri' is empty")));
}

TEST_F(DocumentValidatorTest, ValidateEmptySchemaInvalid) {
  DocumentProto email = SimpleEmailBuilder().SetSchema("").Build();
  EXPECT_THAT(document_validator_->Validate(email),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("'schema' is empty")));
}

TEST_F(DocumentValidatorTest, ValidateNonexistentSchemaNotFound) {
  DocumentProto email =
      SimpleEmailBuilder().SetSchema("WrongEmailType").Build();
  EXPECT_THAT(document_validator_->Validate(email),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND,
                       HasSubstr("'WrongEmailType' not found")));
}

TEST_F(DocumentValidatorTest, ValidateEmptyPropertyInvalid) {
  DocumentProto email =
      SimpleEmailBuilder().AddStringProperty("", kDefaultString).Build();

  EXPECT_THAT(document_validator_->Validate(email),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("'name' is empty")));
}

TEST_F(DocumentValidatorTest, ValidateDuplicatePropertyAlreadyExists) {
  DocumentProto email = SimpleEmailBuilder()
                            .ClearProperties()
                            .AddStringProperty(kPropertySubject, kDefaultString)
                            .AddStringProperty(kPropertySubject, kDefaultString)
                            .Build();

  EXPECT_THAT(document_validator_->Validate(email),
              StatusIs(libtextclassifier3::StatusCode::ALREADY_EXISTS,
                       HasSubstr("'subject' already exists")));
}

TEST_F(DocumentValidatorTest, ValidateNonexistentPropertyNotFound) {
  DocumentProto email =
      SimpleEmailBuilder()
          .AddStringProperty("WrongPropertyName", kDefaultString)
          .Build();

  EXPECT_THAT(document_validator_->Validate(email),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND,
                       HasSubstr("'WrongPropertyName' not found")));
}

TEST_F(DocumentValidatorTest, ValidateAllCustomPropertyOk) {
  DocumentProto email =
      SimpleEmailBuilder()
          // A nonexistent property, would've triggered a NotFound message
          .AddCustomStringProperty("WrongPropertyName", kDefaultString)
          // 'subject' property should've been a string according to the schema
          .AddCustomBooleanProperty(kPropertySubject, false, true)
          .Build();

  EXPECT_THAT(document_validator_->Validate(email), IsOk());
}

TEST_F(DocumentValidatorTest, ValidateExactlyOneRequiredValueOk) {
  // Required property should have exactly 1 value
  DocumentProto email =
      SimpleEmailBuilder()
          .ClearProperties()
          .AddStringProperty(kPropertySubject, kDefaultString)  // 1 value
          .Build();

  EXPECT_THAT(document_validator_->Validate(email), IsOk());
}

TEST_F(DocumentValidatorTest, ValidateInvalidNumberOfRequiredValues) {
  // Required property should have exactly 1 value
  DocumentProto email = SimpleEmailBuilder()
                            .ClearProperties()
                            .AddStringProperty(kPropertySubject)  // 0 values
                            .Build();

  EXPECT_THAT(document_validator_->Validate(email),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("'subject' with only 1 value is required "
                                 "but 0 elements are found")));

  email =
      SimpleEmailBuilder()
          .ClearProperties()
          .AddStringProperty(kPropertySubject, kDefaultString, kDefaultString)
          .Build();

  EXPECT_THAT(document_validator_->Validate(email),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("'subject' with only 1 value is required "
                                 "but 2 elements are found")));
}

TEST_F(DocumentValidatorTest, ValidateZeroOrOneOptionalValueOk) {
  DocumentProto email = SimpleEmailBuilder()
                            .ClearProperties()
                            .AddStringProperty(kPropertySubject, kDefaultString)
                            .AddStringProperty(kPropertyText)  // 0 values
                            .Build();

  EXPECT_THAT(document_validator_->Validate(email), IsOk());

  email = SimpleEmailBuilder()
              .ClearProperties()
              .AddStringProperty(kPropertySubject, kDefaultString)
              .AddStringProperty(kPropertyText, kDefaultString)  // 1 value
              .Build();

  EXPECT_THAT(document_validator_->Validate(email), IsOk());
}

TEST_F(DocumentValidatorTest, ValidateInvalidNumberOfOptionalValues) {
  DocumentProto email =
      SimpleEmailBuilder()
          .ClearProperties()
          .AddStringProperty(kPropertySubject, kDefaultString)
          .AddStringProperty(kPropertyText, kDefaultString, kDefaultString)
          .Build();

  EXPECT_THAT(
      document_validator_->Validate(email),
      StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
               HasSubstr("'text' is optional but 2 elements are found")));
}

TEST_F(DocumentValidatorTest, ValidateMissingRequiredPropertyInvalid) {
  // All required properties should be present in document
  DocumentProto email = SimpleEmailBuilder()
                            .ClearProperties()
                            .AddStringProperty(kPropertyText, kDefaultString)
                            .Build();

  // The required property 'subject' isn't added in email.
  EXPECT_THAT(document_validator_->Validate(email),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("One or more required fields missing")));
}

TEST_F(DocumentValidatorTest,
       ValidateNestedPropertyDoesntMatchSchemaTypeInvalid) {
  // Nested DocumentProto should have the expected schema type
  DocumentProto conversation =
      SimpleConversationBuilder()
          .ClearProperties()
          .AddStringProperty(kPropertyName, kDefaultString)
          .AddDocumentProperty(
              kPropertyEmails, SimpleEmailBuilder().Build(),
              SimpleConversationBuilder().Build(),  // Wrong document type
              SimpleEmailBuilder().Build())
          .Build();

  EXPECT_THAT(document_validator_->Validate(conversation),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("'emails' should have type 'EmailMessage' but "
                                 "actual value has type 'Conversation'")));
}

TEST_F(DocumentValidatorTest, ValidateNestedPropertyInvalid) {
  // Issues in nested DocumentProto should be detected
  DocumentProto conversation =
      SimpleConversationBuilder()
          .ClearProperties()
          .AddStringProperty(kPropertyName, kDefaultString)
          .AddDocumentProperty(kPropertyEmails,
                               SimpleEmailBuilder()
                                   .SetNamespace("")
                                   .Build())  // Bad nested document
          .Build();

  EXPECT_THAT(document_validator_->Validate(conversation),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("'namespace' is empty")));
}

TEST_F(DocumentValidatorTest, HandleTypeConfigMapChangesOk) {
  SchemaProto email_schema;
  auto type_config = email_schema.add_types();
  CreateEmailTypeConfig(type_config);

  // Create a custom directory so we don't collide with the test's preset schema
  // in SetUp
  const std::string custom_schema_dir = GetTestTempDir() + "/custom_schema";
  filesystem_.DeleteDirectoryRecursively(custom_schema_dir.c_str());
  filesystem_.CreateDirectoryRecursively(custom_schema_dir.c_str());

  // Set a schema with only the 'Email' type
  ICING_ASSERT_OK_AND_ASSIGN(
      std::unique_ptr<SchemaStore> schema_store,
      SchemaStore::Create(&filesystem_, custom_schema_dir));
  ASSERT_THAT(schema_store->SetSchema(email_schema), IsOk());

  DocumentValidator document_validator(schema_store.get());

  DocumentProto conversation = SimpleConversationBuilder().Build();

  // Schema doesn't know about the 'Conversation' type yet
  EXPECT_THAT(document_validator.Validate(conversation),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND,
                       HasSubstr("'Conversation' not found")));

  // Add the 'Conversation' type
  SchemaProto email_and_conversation_schema = email_schema;
  type_config = email_and_conversation_schema.add_types();
  CreateConversationTypeConfig(type_config);

  // DocumentValidator should be able to handle the SchemaStore getting updated
  // separately
  ASSERT_THAT(schema_store->SetSchema(email_and_conversation_schema), IsOk());

  ICING_EXPECT_OK(document_validator.Validate(conversation));
}

TEST_F(DocumentValidatorTest, PositiveDocumentScoreOk) {
  DocumentProto email = SimpleEmailBuilder().SetScore(1).Build();
  ICING_EXPECT_OK(document_validator_->Validate(email));

  email = SimpleEmailBuilder()
              .SetScore(std::numeric_limits<int32_t>::max())
              .Build();
  ICING_EXPECT_OK(document_validator_->Validate(email));
}

TEST_F(DocumentValidatorTest, NegativeDocumentScoreInvalid) {
  DocumentProto email = SimpleEmailBuilder().SetScore(-1).Build();
  EXPECT_THAT(document_validator_->Validate(email),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("is negative")));

  email = SimpleEmailBuilder()
              .SetScore(std::numeric_limits<int32_t>::min())
              .Build();
  EXPECT_THAT(document_validator_->Validate(email),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("is negative")));
}

TEST_F(DocumentValidatorTest, PositiveDocumentCreationTimestampMsOk) {
  DocumentProto email = SimpleEmailBuilder().SetCreationTimestampMs(1).Build();
  ICING_EXPECT_OK(document_validator_->Validate(email));

  email = SimpleEmailBuilder()
              .SetCreationTimestampMs(std::numeric_limits<int32_t>::max())
              .Build();
  ICING_EXPECT_OK(document_validator_->Validate(email));
}

TEST_F(DocumentValidatorTest, ZeroDocumentCreationTimestampMsOk) {
  DocumentProto email = SimpleEmailBuilder().SetCreationTimestampMs(0).Build();
  ICING_EXPECT_OK(document_validator_->Validate(email));
}

TEST_F(DocumentValidatorTest, NegativeDocumentCreationTimestampMsInvalid) {
  DocumentProto email = SimpleEmailBuilder().SetCreationTimestampMs(-1).Build();
  EXPECT_THAT(document_validator_->Validate(email),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("is negative")));

  email = SimpleEmailBuilder()
              .SetCreationTimestampMs(std::numeric_limits<int32_t>::min())
              .Build();
  EXPECT_THAT(document_validator_->Validate(email),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("is negative")));
}

TEST_F(DocumentValidatorTest, PositiveDocumentTtlMsOk) {
  DocumentProto email = SimpleEmailBuilder().SetTtlMs(1).Build();
  ICING_EXPECT_OK(document_validator_->Validate(email));

  email = SimpleEmailBuilder()
              .SetTtlMs(std::numeric_limits<int32_t>::max())
              .Build();
  ICING_EXPECT_OK(document_validator_->Validate(email));
}

TEST_F(DocumentValidatorTest, ZeroDocumentTtlMsOk) {
  DocumentProto email = SimpleEmailBuilder().SetTtlMs(0).Build();
  ICING_EXPECT_OK(document_validator_->Validate(email));
}

TEST_F(DocumentValidatorTest, NegativeDocumentTtlMsInvalid) {
  DocumentProto email = SimpleEmailBuilder().SetTtlMs(-1).Build();
  EXPECT_THAT(document_validator_->Validate(email),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("is negative")));

  email = SimpleEmailBuilder()
              .SetTtlMs(std::numeric_limits<int32_t>::min())
              .Build();
  EXPECT_THAT(document_validator_->Validate(email),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("is negative")));
}

}  // namespace

}  // namespace lib
}  // namespace icing
