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

#include "icing/icing-search-engine.h"

#include <cstdint>
#include <ctime>
#include <limits>
#include <memory>
#include <string>
#include <utility>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/file/mock-filesystem.h"
#include "icing/portable/equals-proto.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/icing-search-engine-options.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/snippet-helpers.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {
using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::_;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Lt;
using ::testing::Return;
using ::testing::SizeIs;

// For mocking purpose, we allow tests to provide a custom Filesystem.
class TestIcingSearchEngine : public IcingSearchEngine {
 public:
  TestIcingSearchEngine(const IcingSearchEngineOptions& options,
                        std::unique_ptr<const Filesystem> filesystem,
                        std::unique_ptr<FakeClock> clock)
      : IcingSearchEngine(options, std::move(filesystem), std::move(clock)) {}
};

std::string GetTestBaseDir() { return GetTestTempDir() + "/icing"; }

class IcingSearchEngineTest : public testing::Test {
 protected:
  void SetUp() override {
    ICING_ASSERT_OK(
        // File generated via icu_data_file rule in //icing/BUILD.
        SetUpICUDataFile("icing/icu.dat"));
    filesystem_.CreateDirectoryRecursively(GetTestBaseDir().c_str());
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(GetTestBaseDir().c_str());
  }

  const Filesystem* filesystem() const { return &filesystem_; }

 private:
  Filesystem filesystem_;
};

constexpr int kMaxSupportedDocumentSize = (1u << 24) - 1;

// Non-zero value so we don't override it to be the current time
constexpr std::time_t kDefaultCreationTimestampSecs = 1575492852;

std::string GetDocumentDir() { return GetTestBaseDir() + "/document_dir"; }

std::string GetIndexDir() { return GetTestBaseDir() + "/index_dir"; }

std::string GetSchemaDir() { return GetTestBaseDir() + "/schema_dir"; }

std::string GetHeaderFilename() {
  return GetTestBaseDir() + "/icing_search_engine_header";
}

IcingSearchEngineOptions GetDefaultIcingOptions() {
  IcingSearchEngineOptions icing_options;
  icing_options.set_base_dir(GetTestBaseDir());
  return icing_options;
}

DocumentProto GetDefaultDocument() {
  return DocumentBuilder()
      .SetKey("namespace", "uri")
      .SetSchema("Message")
      .AddStringProperty("body", "message body")
      .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
      .Build();
}

SchemaProto GetDefaultSchema() {
  SchemaProto schema;
  auto type = schema.add_types();
  type->set_schema_type("Message");

  auto body = type->add_properties();
  body->set_property_name("body");
  body->set_data_type(PropertyConfigProto::DataType::STRING);
  body->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);
  body->mutable_indexing_config()->set_term_match_type(TermMatchType::PREFIX);
  body->mutable_indexing_config()->set_tokenizer_type(
      IndexingConfig::TokenizerType::PLAIN);

  return schema;
}

ScoringSpecProto GetDefaultScoringSpec() {
  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  return scoring_spec;
}

TEST_F(IcingSearchEngineTest, SimpleInitialization) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_ASSERT_OK(icing.Initialize());
  ICING_ASSERT_OK(icing.SetSchema(GetDefaultSchema()));

  DocumentProto document = GetDefaultDocument();
  ICING_ASSERT_OK(icing.Put(document));
  ICING_ASSERT_OK(icing.Put(DocumentProto(document)));
}

TEST_F(IcingSearchEngineTest, InitializingAgainSavesNonPersistedData) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_ASSERT_OK(icing.Initialize());
  ICING_ASSERT_OK(icing.SetSchema(GetDefaultSchema()));

  DocumentProto document = GetDefaultDocument();
  ICING_ASSERT_OK(icing.Put(document));
  ASSERT_THAT(icing.Get("namespace", "uri"),
              IsOkAndHolds(EqualsProto(document)));

  ICING_EXPECT_OK(icing.Initialize());
  EXPECT_THAT(icing.Get("namespace", "uri"),
              IsOkAndHolds(EqualsProto(document)));
}

TEST_F(IcingSearchEngineTest, MaxIndexMergeSizeReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_index_merge_size(std::numeric_limits<int32_t>::max());
  IcingSearchEngine icing(options);
  EXPECT_THAT(icing.Initialize(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineTest, NegativeMergeSizeReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_index_merge_size(-1);
  IcingSearchEngine icing(options);
  EXPECT_THAT(icing.Initialize(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineTest, ZeroMergeSizeReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_index_merge_size(0);
  IcingSearchEngine icing(options);
  EXPECT_THAT(icing.Initialize(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineTest, GoodIndexMergeSizeReturnsOk) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  // One is fine, if a bit weird. It just means that the lite index will be
  // smaller and will request a merge any time content is added to it.
  options.set_index_merge_size(1);
  IcingSearchEngine icing(options);
  ICING_EXPECT_OK(icing.Initialize());
}

TEST_F(IcingSearchEngineTest,
       NegativeMaxTokensPerDocSizeReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_max_tokens_per_doc(-1);
  IcingSearchEngine icing(options);
  EXPECT_THAT(icing.Initialize(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineTest, ZeroMaxTokensPerDocSizeReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_max_tokens_per_doc(0);
  IcingSearchEngine icing(options);
  EXPECT_THAT(icing.Initialize(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineTest, GoodMaxTokensPerDocSizeReturnsOk) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  // INT_MAX is valid - it just means that we shouldn't limit the number of
  // tokens per document. It would be pretty inconceivable that anyone would
  // produce such a document - the text being indexed alone would take up at
  // least ~4.3 GiB! - and the document would be rejected before indexing
  // for exceeding max_document_size, but there's no reason to explicitly
  // bar it.
  options.set_max_tokens_per_doc(std::numeric_limits<int32_t>::max());
  IcingSearchEngine icing(options);
  ICING_EXPECT_OK(icing.Initialize());
}

TEST_F(IcingSearchEngineTest, NegativeMaxTokenLenReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_max_token_length(-1);
  IcingSearchEngine icing(options);
  EXPECT_THAT(icing.Initialize(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineTest, ZeroMaxTokenLenReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_max_token_length(0);
  IcingSearchEngine icing(options);
  EXPECT_THAT(icing.Initialize(),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineTest, MaxTokenLenReturnsOkAndTruncatesTokens) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  // A length of 1 is allowed - even though it would be strange to want
  // this.
  options.set_max_token_length(1);
  IcingSearchEngine icing(options);
  ICING_EXPECT_OK(icing.Initialize());
  ICING_EXPECT_OK(icing.SetSchema(GetDefaultSchema()));

  DocumentProto document =
      DocumentBuilder()
          .SetKey("namespace", "uri")
          .SetSchema("Message")
          .AddStringProperty("body", "message")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  ICING_EXPECT_OK(icing.Put(document));

  // "message" should have been truncated to "m"
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  // The indexed tokens were  truncated to length of 1, so "m" will match
  search_spec.set_query("m");

  SearchResultProto exp_result;
  (*exp_result.mutable_results()->Add()->mutable_document()) = document;

  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(exp_result)));

  // The query token is also truncated to length of 1, so "me"->"m" matches "m"
  search_spec.set_query("me");
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(exp_result)));

  // The query token is still truncated to length of 1, so "massage"->"m"
  // matches "m"
  search_spec.set_query("massage");
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(exp_result)));
}

TEST_F(IcingSearchEngineTest,
       MaxIntMaxTokenLenReturnsOkTooLargeTokenReturnsResourceExhausted) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  // Set token length to max. This is allowed (it just means never to
  // truncate tokens). However, this does mean that tokens that exceed the
  // size of the lexicon will cause indexing to fail.
  options.set_max_token_length(std::numeric_limits<int32_t>::max());
  IcingSearchEngine icing(options);
  ICING_EXPECT_OK(icing.Initialize());
  ICING_EXPECT_OK(icing.SetSchema(GetDefaultSchema()));

  // Add a document that just barely fits under the max document limit.
  // This will still fail to index because we won't actually have enough
  // room in the lexicon to fit this content.
  std::string enormous_string(kMaxSupportedDocumentSize - 256, 'p');
  DocumentProto document =
      DocumentBuilder()
          .SetKey("namespace", "uri")
          .SetSchema("Message")
          .AddStringProperty("body", std::move(enormous_string))
          .Build();
  EXPECT_THAT(icing.Put(document),
              StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));

  SearchSpecProto search_spec;
  search_spec.set_query("p");
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(SearchResultProto::default_instance())));
}

TEST_F(IcingSearchEngineTest, FailToCreateDocStore) {
  auto mock_filesystem = std::make_unique<MockFilesystem>();
  // This fails DocumentStore::Create()
  ON_CALL(*mock_filesystem, CreateDirectoryRecursively(_))
      .WillByDefault(Return(false));

  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::move(mock_filesystem),
                              std::make_unique<FakeClock>());

  ASSERT_THAT(icing.Initialize(),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL,
                       HasSubstr("Could not create directory")));
}

TEST_F(IcingSearchEngineTest,
       CircularReferenceCreateSectionManagerReturnsInvalidArgument) {
  // Create a type config with a circular reference.
  SchemaProto schema;
  auto* type = schema.add_types();
  type->set_schema_type("Message");

  auto* body = type->add_properties();
  body->set_property_name("recipient");
  body->set_schema_type("Person");
  body->set_data_type(PropertyConfigProto::DataType::DOCUMENT);
  body->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);
  body->mutable_indexing_config()->set_term_match_type(
      TermMatchType::EXACT_ONLY);
  body->mutable_indexing_config()->set_tokenizer_type(
      IndexingConfig::TokenizerType::PLAIN);

  type = schema.add_types();
  type->set_schema_type("Person");

  body = type->add_properties();
  body->set_property_name("recipient");
  body->set_schema_type("Message");
  body->set_data_type(PropertyConfigProto::DataType::DOCUMENT);
  body->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);
  body->mutable_indexing_config()->set_term_match_type(
      TermMatchType::EXACT_ONLY);
  body->mutable_indexing_config()->set_tokenizer_type(
      IndexingConfig::TokenizerType::PLAIN);

  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_EXPECT_OK(icing.Initialize());
  EXPECT_THAT(icing.SetSchema(schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineTest, NoSchemaSet) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_ASSERT_OK(icing.Initialize());

  DocumentProto document = GetDefaultDocument();
  EXPECT_THAT(icing.Put(document),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND,
                       HasSubstr("'Message' not found")));
  EXPECT_THAT(icing.Put(DocumentProto(document)),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND,
                       HasSubstr("'Message' not found")));
}

TEST_F(IcingSearchEngineTest, FailToReadSchema) {
  IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();

  {
    // Successfully initialize and set a schema
    IcingSearchEngine icing(icing_options);
    ICING_ASSERT_OK(icing.Initialize());
    ICING_ASSERT_OK(icing.SetSchema(GetDefaultSchema()));
  }

  auto mock_filesystem = std::make_unique<MockFilesystem>();

  // This fails FileBackedProto::Read() when we try to check the schema we
  // had previously set
  ON_CALL(*mock_filesystem,
          OpenForRead(Eq(icing_options.base_dir() + "/schema_dir/schema.pb")))
      .WillByDefault(Return(-1));

  TestIcingSearchEngine test_icing(icing_options, std::move(mock_filesystem),
                                   std::make_unique<FakeClock>());

  ASSERT_THAT(test_icing.Initialize(),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL,
                       HasSubstr("Unable to open file for read")));
}

TEST_F(IcingSearchEngineTest, FailToWriteSchema) {
  IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();

  auto mock_filesystem = std::make_unique<MockFilesystem>();
  // This fails FileBackedProto::Write()
  ON_CALL(*mock_filesystem,
          OpenForWrite(Eq(icing_options.base_dir() + "/schema_dir/schema.pb")))
      .WillByDefault(Return(-1));

  TestIcingSearchEngine icing(icing_options, std::move(mock_filesystem),
                              std::make_unique<FakeClock>());

  ICING_ASSERT_OK(icing.Initialize());
  ASSERT_THAT(icing.SetSchema(GetDefaultSchema()),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL,
                       HasSubstr("Unable to open file for write")));
}

TEST_F(IcingSearchEngineTest, SetSchema) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_ASSERT_OK(icing.Initialize());

  auto message_document = GetDefaultDocument();

  auto schema_with_message = GetDefaultSchema();

  SchemaProto schema_with_email;
  SchemaTypeConfigProto* type = schema_with_email.add_types();
  type->set_schema_type("Email");
  PropertyConfigProto* property = type->add_properties();
  property->set_property_name("title");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  SchemaProto schema_with_email_and_message = schema_with_email;
  type = schema_with_email_and_message.add_types();
  type->set_schema_type("Message");
  property = type->add_properties();
  property->set_property_name("body");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  // Create an arbitrary invalid schema
  SchemaProto invalid_schema;
  SchemaTypeConfigProto* empty_type = invalid_schema.add_types();
  empty_type->set_schema_type("");

  // Make sure we can't set invalid schemas
  EXPECT_THAT(icing.SetSchema(invalid_schema),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));

  // Can add an document of a set schema
  ICING_EXPECT_OK(icing.SetSchema(schema_with_message));
  ICING_EXPECT_OK(icing.Put(message_document));

  // Schema with Email doesn't have Message, so would result incompatible
  // data
  EXPECT_THAT(icing.SetSchema(schema_with_email),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));

  // Can expand the set of schema types and add an document of a new
  // schema type
  ICING_EXPECT_OK(icing.SetSchema(SchemaProto(schema_with_email_and_message)));
  ICING_EXPECT_OK(icing.Put(message_document));

  // Can't add an document whose schema isn't set
  auto photo_document = DocumentBuilder()
                            .SetKey("namespace", "uri")
                            .SetSchema("Photo")
                            .AddStringProperty("creator", "icing")
                            .Build();
  EXPECT_THAT(icing.Put(photo_document),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND,
                       HasSubstr("'Photo' not found")));
}

TEST_F(IcingSearchEngineTest, SetSchemaTriggersIndexRestorationAndReturnsOk) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_ASSERT_OK(icing.Initialize());

  SchemaProto schema_with_no_indexed_property = GetDefaultSchema();
  schema_with_no_indexed_property.mutable_types(0)
      ->mutable_properties(0)
      ->clear_indexing_config();

  ICING_EXPECT_OK(icing.SetSchema(schema_with_no_indexed_property));
  // Nothing will be index and Search() won't return anything.
  ICING_EXPECT_OK(icing.Put(GetDefaultDocument()));

  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto empty_result;

  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(empty_result)));

  SchemaProto schema_with_indexed_property = GetDefaultSchema();
  // Index restoration should be triggered here because new schema requires more
  // properties to be indexed.
  ICING_EXPECT_OK(icing.SetSchema(schema_with_indexed_property));
  SearchResultProto expected_result;
  (*expected_result.mutable_results()->Add()->mutable_document()) =
      GetDefaultDocument();
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(expected_result)));
}

TEST_F(IcingSearchEngineTest, SetSchemaRevalidatesDocumentsAndReturnsOk) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_ASSERT_OK(icing.Initialize());

  SchemaProto schema_with_optional_subject;
  auto type = schema_with_optional_subject.add_types();
  type->set_schema_type("email");

  // Add a OPTIONAL property
  auto property = type->add_properties();
  property->set_property_name("subject");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  ICING_EXPECT_OK(icing.SetSchema(schema_with_optional_subject));

  DocumentProto email_document_without_subject =
      DocumentBuilder()
          .SetKey("namespace", "without_subject")
          .SetSchema("email")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  DocumentProto email_document_with_subject =
      DocumentBuilder()
          .SetKey("namespace", "with_subject")
          .SetSchema("email")
          .AddStringProperty("subject", "foo")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();

  ICING_EXPECT_OK(icing.Put(email_document_without_subject));
  ICING_EXPECT_OK(icing.Put(email_document_with_subject));

  SchemaProto schema_with_required_subject;
  type = schema_with_required_subject.add_types();
  type->set_schema_type("email");

  // Add a REQUIRED property
  property = type->add_properties();
  property->set_property_name("subject");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);

  // Can't set the schema since it's incompatible
  EXPECT_THAT(icing.SetSchema(schema_with_required_subject),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  // Force set it
  ICING_EXPECT_OK(icing.SetSchema(schema_with_required_subject,
                                  /*ignore_errors_and_delete_documents=*/true));

  EXPECT_THAT(icing.Get("namespace", "with_subject"),
              IsOkAndHolds(EqualsProto(email_document_with_subject)));

  // The document without a subject got deleted because it failed validation
  // against the new schema
  EXPECT_THAT(icing.Get("namespace", "without_subject"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(IcingSearchEngineTest, SetSchemaDeletesDocumentsAndReturnsOk) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_ASSERT_OK(icing.Initialize());

  SchemaProto schema;
  auto type = schema.add_types();
  type->set_schema_type("email");
  type = schema.add_types();
  type->set_schema_type("message");

  ICING_EXPECT_OK(icing.SetSchema(schema));

  DocumentProto email_document =
      DocumentBuilder()
          .SetKey("namespace", "email_uri")
          .SetSchema("email")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  DocumentProto message_document =
      DocumentBuilder()
          .SetKey("namespace", "message_uri")
          .SetSchema("message")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();

  ICING_EXPECT_OK(icing.Put(email_document));
  ICING_EXPECT_OK(icing.Put(message_document));

  // Clear the schema and only add the "email" type, essentially deleting the
  // "message" type
  SchemaProto new_schema;
  type = new_schema.add_types();
  type->set_schema_type("email");

  // Can't set the schema since it's incompatible
  EXPECT_THAT(icing.SetSchema(new_schema),
              StatusIs(libtextclassifier3::StatusCode::FAILED_PRECONDITION));
  // Force set it
  ICING_EXPECT_OK(icing.SetSchema(new_schema,
                                  /*ignore_errors_and_delete_documents=*/true));

  // "email" document is still there
  EXPECT_THAT(icing.Get("namespace", "email_uri"),
              IsOkAndHolds(EqualsProto(email_document)));

  // "message" document got deleted
  EXPECT_THAT(icing.Get("namespace", "message_uri"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(IcingSearchEngineTest, GetSchemaNotFound) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_ASSERT_OK(icing.Initialize());

  EXPECT_THAT(icing.GetSchema(),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(IcingSearchEngineTest, GetSchemaOk) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_ASSERT_OK(icing.Initialize());

  ICING_EXPECT_OK(icing.SetSchema(GetDefaultSchema()));
  EXPECT_THAT(icing.GetSchema(), IsOkAndHolds(EqualsProto(GetDefaultSchema())));
}

TEST_F(IcingSearchEngineTest, GetSchemaTypeNotFound) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_ASSERT_OK(icing.Initialize());

  EXPECT_THAT(icing.GetSchemaType("nonexistent_schema"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(IcingSearchEngineTest, GetSchemaTypeOk) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_ASSERT_OK(icing.Initialize());

  ICING_EXPECT_OK(icing.SetSchema(GetDefaultSchema()));
  EXPECT_THAT(icing.GetSchemaType(GetDefaultSchema().types(0).schema_type()),
              IsOkAndHolds(EqualsProto(GetDefaultSchema().types(0))));
}

TEST_F(IcingSearchEngineTest, GetDocument) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_ASSERT_OK(icing.Initialize());
  ICING_ASSERT_OK(icing.SetSchema(GetDefaultSchema()));

  // Simple put and get
  ICING_ASSERT_OK(icing.Put(GetDefaultDocument()));
  ASSERT_THAT(icing.Get("namespace", "uri"),
              IsOkAndHolds(EqualsProto(GetDefaultDocument())));

  // Put an invalid document
  ASSERT_THAT(icing.Put(DocumentProto()),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT,
                       HasSubstr("'namespace' is empty")));

  // Get a non-existing key
  ASSERT_THAT(icing.Get("wrong", "uri"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(IcingSearchEngineTest, SearchReturnsValidResults) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_ASSERT_OK(icing.Initialize());
  ICING_ASSERT_OK(icing.SetSchema(GetDefaultSchema()));

  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetSchema("Message")
          .AddStringProperty("body", "message body")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  ICING_ASSERT_OK(icing.Put(document_one));
  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Message")
          .AddStringProperty("body", "message body")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  ICING_ASSERT_OK(icing.Put(document_two));

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("message");

  ResultSpecProto result_spec;
  result_spec.mutable_snippet_spec()->set_max_window_bytes(64);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(1);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(1);

  ICING_ASSERT_OK_AND_ASSIGN(
      SearchResultProto results,
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec));
  EXPECT_THAT(results.results(), SizeIs(2));
  EXPECT_THAT(results.results(0).document(), EqualsProto(document_two));
  EXPECT_THAT(GetMatch(results.results(0).document(),
                       results.results(0).snippet(), "body",
                       /*snippet_index=*/0),
              Eq("message"));
  EXPECT_THAT(
      GetWindow(results.results(0).document(), results.results(0).snippet(),
                "body", /*snippet_index=*/0),
      Eq("message body"));
  EXPECT_THAT(results.results(1).document(), EqualsProto(document_one));
  EXPECT_THAT(
      GetMatch(results.results(1).document(), results.results(1).snippet(),
               "body", /*snippet_index=*/0),
      IsEmpty());
  EXPECT_THAT(
      GetWindow(results.results(1).document(), results.results(1).snippet(),
                "body", /*snippet_index=*/0),
      IsEmpty());

  search_spec.set_query("foo");
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(SearchResultProto::default_instance())));
}

TEST_F(IcingSearchEngineTest, SearchReturnsOneResult) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_ASSERT_OK(icing.Initialize());
  ICING_ASSERT_OK(icing.SetSchema(GetDefaultSchema()));

  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetSchema("Message")
          .AddStringProperty("body", "message body")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  ICING_ASSERT_OK(icing.Put(document_one));
  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Message")
          .AddStringProperty("body", "message body")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  ICING_ASSERT_OK(icing.Put(document_two));

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("message");

  ResultSpecProto result_spec;
  result_spec.set_num_to_retrieve(1);

  SearchResultProto expected_result;
  (*expected_result.mutable_results()->Add()->mutable_document()) =
      document_two;
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(), result_spec),
              IsOkAndHolds(EqualsProto(expected_result)));
}

TEST_F(IcingSearchEngineTest, SearchZeroResultLimitReturnsEmptyResults) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_ASSERT_OK(icing.Initialize());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("");

  ResultSpecProto result_spec;
  result_spec.set_num_to_retrieve(0);

  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(), result_spec),
              IsOkAndHolds(EqualsProto(SearchResultProto::default_instance())));
}

TEST_F(IcingSearchEngineTest, SearchNegativeResultLimitReturnsInvalidArgument) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_ASSERT_OK(icing.Initialize());

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("");

  ResultSpecProto result_spec;
  result_spec.set_num_to_retrieve(-5);

  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(), result_spec),
              StatusIs(libtextclassifier3::StatusCode::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineTest, SearchWithPersistenceReturnsValidResults) {
  IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();

  {
    // Set the schema up beforehand.
    IcingSearchEngine icing(icing_options);
    ICING_ASSERT_OK(icing.Initialize());
    ICING_ASSERT_OK(icing.SetSchema(GetDefaultSchema()));
    // Schema will be persisted to disk when icing goes out of scope.
  }

  {
    // Ensure that icing initializes the schema and section_manager
    // properly from the pre-existing file.
    IcingSearchEngine icing(icing_options);
    ICING_ASSERT_OK(icing.Initialize());

    ICING_EXPECT_OK(icing.Put(GetDefaultDocument()));
    // The index and document store will be persisted to disk when icing goes
    // out of scope.
  }

  {
    // Ensure that the index is brought back up without problems and we
    // can query for the content that we expect.
    IcingSearchEngine icing(icing_options);
    ICING_ASSERT_OK(icing.Initialize());

    SearchSpecProto search_spec;
    search_spec.set_term_match_type(TermMatchType::PREFIX);
    search_spec.set_query("message");

    SearchResultProto expected_result;
    (*expected_result.mutable_results()->Add()->mutable_document()) =
        GetDefaultDocument();

    EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                IsOkAndHolds(EqualsProto(expected_result)));

    search_spec.set_query("foo");
    EXPECT_THAT(
        icing.Search(search_spec, GetDefaultScoringSpec(),
                     ResultSpecProto::default_instance()),
        IsOkAndHolds(EqualsProto(SearchResultProto::default_instance())));
  }
}

TEST_F(IcingSearchEngineTest, OptimizationShouldRemoveDeletedDocs) {
  IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();

  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace", "uri1")
                                .SetSchema("Message")
                                .AddStringProperty("body", "message body1")
                                .Build();
  {
    IcingSearchEngine icing(icing_options);
    ICING_ASSERT_OK(icing.Initialize());
    ICING_ASSERT_OK(icing.SetSchema(GetDefaultSchema()));
    ICING_ASSERT_OK(icing.Put(document1));

    // Deletes document1
    ICING_ASSERT_OK(icing.Delete("namespace", "uri1"));
    const std::string document_log_path =
        icing_options.base_dir() + "/document_dir/document_log";
    int64_t document_log_size_before =
        filesystem()->GetFileSize(document_log_path.c_str());
    ICING_ASSERT_OK(icing.Optimize());
    int64_t document_log_size_after =
        filesystem()->GetFileSize(document_log_path.c_str());

    // Validates that document can't be found right after Optimize()
    EXPECT_THAT(icing.Get("namespace", "uri1"),
                StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
    // Validates that document is actually removed from document log
    EXPECT_THAT(document_log_size_after, Lt(document_log_size_before));
  }  // Destroys IcingSearchEngine to make sure nothing is cached.

  IcingSearchEngine icing(icing_options);
  ICING_EXPECT_OK(icing.Initialize());
  EXPECT_THAT(icing.Get("namespace", "uri1"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(IcingSearchEngineTest, OptimizationShouldDeleteTemporaryDirectory) {
  IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();
  IcingSearchEngine icing(icing_options);
  ICING_ASSERT_OK(icing.Initialize());
  ICING_ASSERT_OK(icing.SetSchema(GetDefaultSchema()));

  // Create a tmp dir that will be used in Optimize() to swap files,
  // this validates that any tmp dirs will be deleted before using.
  const std::string tmp_dir =
      icing_options.base_dir() + "/document_dir_optimize_tmp";

  const std::string tmp_file = tmp_dir + "/file";
  ASSERT_TRUE(filesystem()->CreateDirectory(tmp_dir.c_str()));
  ScopedFd fd(filesystem()->OpenForWrite(tmp_file.c_str()));
  ASSERT_TRUE(fd.is_valid());
  ASSERT_TRUE(filesystem()->Write(fd.get(), "1234", 4));
  fd.reset();

  ICING_EXPECT_OK(icing.Optimize());

  EXPECT_FALSE(filesystem()->DirectoryExists(tmp_dir.c_str()));
  EXPECT_FALSE(filesystem()->FileExists(tmp_file.c_str()));
}

TEST_F(IcingSearchEngineTest, GetAndPutShouldWorkAfterOptimization) {
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetSchema("Message")
          .AddStringProperty("body", "message body1")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Message")
          .AddStringProperty("body", "message body2")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  DocumentProto document3 =
      DocumentBuilder()
          .SetKey("namespace", "uri3")
          .SetSchema("Message")
          .AddStringProperty("body", "message body3")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  {
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ICING_ASSERT_OK(icing.Initialize());
    ICING_ASSERT_OK(icing.SetSchema(GetDefaultSchema()));

    ICING_ASSERT_OK(icing.Put(document1));
    ICING_ASSERT_OK(icing.Optimize());

    // Validates that Get() and Put() are good right after Optimize()
    EXPECT_THAT(icing.Get("namespace", "uri1"),
                IsOkAndHolds(EqualsProto(document1)));
    ICING_EXPECT_OK(icing.Put(document2));
  }  // Destroys IcingSearchEngine to make sure nothing is cached.

  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_EXPECT_OK(icing.Initialize());
  EXPECT_THAT(icing.Get("namespace", "uri1"),
              IsOkAndHolds(EqualsProto(document1)));
  EXPECT_THAT(icing.Get("namespace", "uri2"),
              IsOkAndHolds(EqualsProto(document2)));
  ICING_EXPECT_OK(icing.Put(document3));
}

TEST_F(IcingSearchEngineTest, DeleteShouldWorkAfterOptimization) {
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetSchema("Message")
          .AddStringProperty("body", "message body1")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Message")
          .AddStringProperty("body", "message body2")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  {
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ICING_ASSERT_OK(icing.Initialize());
    ICING_ASSERT_OK(icing.SetSchema(GetDefaultSchema()));
    ICING_ASSERT_OK(icing.Put(document1));
    ICING_ASSERT_OK(icing.Put(document2));
    ICING_ASSERT_OK(icing.Optimize());

    // Validates that Delete() works right after Optimize()
    ICING_EXPECT_OK(icing.Delete("namespace", "uri1"));
    EXPECT_THAT(icing.Get("namespace", "uri1"),
                StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
    EXPECT_THAT(icing.Get("namespace", "uri2"),
                IsOkAndHolds(EqualsProto(document2)));
  }  // Destroys IcingSearchEngine to make sure nothing is cached.

  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_EXPECT_OK(icing.Initialize());
  ICING_EXPECT_OK(icing.Delete("namespace", "uri2"));
  EXPECT_THAT(icing.Get("namespace", "uri1"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
  EXPECT_THAT(icing.Get("namespace", "uri2"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));
}

TEST_F(IcingSearchEngineTest, SetSchemaShouldWorkAfterOptimization) {
  // Creates 3 test schemas
  SchemaProto schema1 = SchemaProto(GetDefaultSchema());

  SchemaProto schema2 = SchemaProto(schema1);
  auto new_property2 = schema2.mutable_types(0)->add_properties();
  new_property2->set_property_name("property2");
  new_property2->set_data_type(PropertyConfigProto::DataType::STRING);
  new_property2->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
  new_property2->mutable_indexing_config()->set_term_match_type(
      TermMatchType::PREFIX);
  new_property2->mutable_indexing_config()->set_tokenizer_type(
      IndexingConfig::TokenizerType::PLAIN);

  SchemaProto schema3 = SchemaProto(schema2);
  auto new_property3 = schema3.mutable_types(0)->add_properties();
  new_property3->set_property_name("property3");
  new_property3->set_data_type(PropertyConfigProto::DataType::STRING);
  new_property3->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
  new_property3->mutable_indexing_config()->set_term_match_type(
      TermMatchType::PREFIX);
  new_property3->mutable_indexing_config()->set_tokenizer_type(
      IndexingConfig::TokenizerType::PLAIN);

  {
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ICING_ASSERT_OK(icing.Initialize());
    ICING_ASSERT_OK(icing.SetSchema(schema1));
    ICING_ASSERT_OK(icing.Optimize());

    // Validates that SetSchema() works right after Optimize()
    ICING_EXPECT_OK(icing.SetSchema(schema2));
  }  // Destroys IcingSearchEngine to make sure nothing is cached.

  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_EXPECT_OK(icing.Initialize());
  ICING_EXPECT_OK(icing.SetSchema(schema3));
}

TEST_F(IcingSearchEngineTest, SearchShouldWorkAfterOptimization) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("namespace", "uri")
          .SetSchema("Message")
          .AddStringProperty("body", "message")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");
  SearchResultProto expected_result;
  (*expected_result.mutable_results()->Add()->mutable_document()) = document;

  {
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ICING_ASSERT_OK(icing.Initialize());
    ICING_ASSERT_OK(icing.SetSchema(GetDefaultSchema()));
    ICING_ASSERT_OK(icing.Put(document));
    ICING_ASSERT_OK(icing.Optimize());

    // Validates that Search() works right after Optimize()
    EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                IsOkAndHolds(EqualsProto(expected_result)));
  }  // Destroys IcingSearchEngine to make sure nothing is cached.

  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_EXPECT_OK(icing.Initialize());
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(expected_result)));
}

TEST_F(IcingSearchEngineTest, SearchIncludesDocumentsBeforeTtl) {
  SchemaProto schema;
  auto type = schema.add_types();
  type->set_schema_type("Message");

  auto body = type->add_properties();
  body->set_property_name("body");
  body->set_data_type(PropertyConfigProto::DataType::STRING);
  body->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);
  body->mutable_indexing_config()->set_term_match_type(TermMatchType::PREFIX);
  body->mutable_indexing_config()->set_tokenizer_type(
      IndexingConfig::TokenizerType::PLAIN);

  DocumentProto document = DocumentBuilder()
                               .SetKey("namespace", "uri")
                               .SetSchema("Message")
                               .AddStringProperty("body", "message body")
                               .SetCreationTimestampSecs(100)
                               .SetTtlSecs(500)
                               .Build();

  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_result;
  (*expected_result.mutable_results()->Add()->mutable_document()) = document;

  // Time just has to be less than the document's creation timestamp (100) + the
  // schema's ttl (500)
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetSeconds(400);

  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::move(fake_clock));

  ICING_EXPECT_OK(icing.Initialize());
  ICING_EXPECT_OK(icing.SetSchema(schema));
  ICING_EXPECT_OK(icing.Put(document));

  // Check that the document is returned as part of search results
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(expected_result)));
}

TEST_F(IcingSearchEngineTest, SearchDoesntIncludeDocumentsPastTtl) {
  SchemaProto schema;
  auto type = schema.add_types();
  type->set_schema_type("Message");

  auto body = type->add_properties();
  body->set_property_name("body");
  body->set_data_type(PropertyConfigProto::DataType::STRING);
  body->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);
  body->mutable_indexing_config()->set_term_match_type(TermMatchType::PREFIX);
  body->mutable_indexing_config()->set_tokenizer_type(
      IndexingConfig::TokenizerType::PLAIN);

  DocumentProto document = DocumentBuilder()
                               .SetKey("namespace", "uri")
                               .SetSchema("Message")
                               .AddStringProperty("body", "message body")
                               .SetCreationTimestampSecs(100)
                               .SetTtlSecs(500)
                               .Build();

  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_result;
  (*expected_result.mutable_results()->Add()->mutable_document()) = document;

  // Time just has to be greater than the document's creation timestamp (100) +
  // the schema's ttl (500)
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetSeconds(700);

  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::move(fake_clock));

  ICING_EXPECT_OK(icing.Initialize());
  ICING_EXPECT_OK(icing.SetSchema(schema));
  ICING_EXPECT_OK(icing.Put(document));

  // Check that the document is returned as part of search results
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(SearchResultProto::default_instance())));
}

TEST_F(IcingSearchEngineTest, SearchWorksAfterSchemaTypesCompatiblyModified) {
  SchemaProto schema;
  auto type_config = schema.add_types();
  type_config->set_schema_type("message");

  auto property = type_config->add_properties();
  property->set_property_name("body");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  DocumentProto message_document =
      DocumentBuilder()
          .SetKey("namespace", "message_uri")
          .SetSchema("message")
          .AddStringProperty("body", "foo")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_ASSERT_OK(icing.Initialize());
  ICING_ASSERT_OK(icing.SetSchema(schema));
  ICING_ASSERT_OK(icing.Put(message_document));

  // Make sure we can search for message document
  SearchSpecProto search_spec;
  search_spec.set_query("foo");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  // The message isn't indexed, so we get nothing
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(SearchResultProto::default_instance())));

  // With just the schema type filter, we can search for the message
  search_spec.Clear();
  search_spec.add_schema_type_filters("message");

  SearchResultProto expected_result;
  (*expected_result.mutable_results()->Add()->mutable_document()) =
      message_document;

  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(expected_result)));

  // Since SchemaTypeIds are assigned based on order in the SchemaProto, this
  // will force a change in the DocumentStore's cached SchemaTypeIds
  schema.clear_types();
  type_config = schema.add_types();
  type_config->set_schema_type("email");

  // Adding a new indexed property will require reindexing
  type_config = schema.add_types();
  type_config->set_schema_type("message");

  property = type_config->add_properties();
  property->set_property_name("body");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
  property->mutable_indexing_config()->set_term_match_type(
      TermMatchType::PREFIX);
  property->mutable_indexing_config()->set_tokenizer_type(
      IndexingConfig::TokenizerType::PLAIN);

  ICING_EXPECT_OK(icing.SetSchema(schema));

  search_spec.Clear();
  search_spec.set_query("foo");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.add_schema_type_filters("message");

  // We can still search for the message document
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(expected_result)));
}

TEST_F(IcingSearchEngineTest, RecoverFromMissingHeaderFile) {
  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_result;
  (*expected_result.mutable_results()->Add()->mutable_document()) =
      GetDefaultDocument();

  {
    // Basic initialization/setup
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ICING_EXPECT_OK(icing.Initialize());
    ICING_EXPECT_OK(icing.SetSchema(GetDefaultSchema()));
    ICING_EXPECT_OK(icing.Put(GetDefaultDocument()));
    EXPECT_THAT(icing.Get("namespace", "uri"),
                IsOkAndHolds(EqualsProto(GetDefaultDocument())));
    EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                IsOkAndHolds(EqualsProto(expected_result)));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  EXPECT_TRUE(filesystem()->DeleteFile(GetHeaderFilename().c_str()));

  // We should be able to recover from this and access all our previous data
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_EXPECT_OK(icing.Initialize());

  // Checks that DocumentLog is still ok
  EXPECT_THAT(icing.Get("namespace", "uri"),
              IsOkAndHolds(EqualsProto(GetDefaultDocument())));

  // Checks that the index is still ok so we can search over it
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(expected_result)));

  // Checks that Schema is still since it'll be needed to validate the document
  ICING_EXPECT_OK(icing.Put(GetDefaultDocument()));
}

TEST_F(IcingSearchEngineTest, RecoverFromInvalidHeaderMagic) {
  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_result;
  (*expected_result.mutable_results()->Add()->mutable_document()) =
      GetDefaultDocument();

  {
    // Basic initialization/setup
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ICING_EXPECT_OK(icing.Initialize());
    ICING_EXPECT_OK(icing.SetSchema(GetDefaultSchema()));
    ICING_EXPECT_OK(icing.Put(GetDefaultDocument()));
    EXPECT_THAT(icing.Get("namespace", "uri"),
                IsOkAndHolds(EqualsProto(GetDefaultDocument())));
    EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                IsOkAndHolds(EqualsProto(expected_result)));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  // Change the header's magic value
  int32_t invalid_magic = 1;  // Anything that's not the actual kMagic value.
  filesystem()->PWrite(GetHeaderFilename().c_str(),
                       offsetof(IcingSearchEngine::Header, magic),
                       &invalid_magic, sizeof(invalid_magic));

  // We should be able to recover from this and access all our previous data
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_EXPECT_OK(icing.Initialize());

  // Checks that DocumentLog is still ok
  EXPECT_THAT(icing.Get("namespace", "uri"),
              IsOkAndHolds(EqualsProto(GetDefaultDocument())));

  // Checks that the index is still ok so we can search over it
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(expected_result)));

  // Checks that Schema is still since it'll be needed to validate the document
  ICING_EXPECT_OK(icing.Put(GetDefaultDocument()));
}

TEST_F(IcingSearchEngineTest, RecoverFromInvalidHeaderChecksum) {
  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_result;
  (*expected_result.mutable_results()->Add()->mutable_document()) =
      GetDefaultDocument();

  {
    // Basic initialization/setup
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ICING_EXPECT_OK(icing.Initialize());
    ICING_EXPECT_OK(icing.SetSchema(GetDefaultSchema()));
    ICING_EXPECT_OK(icing.Put(GetDefaultDocument()));
    EXPECT_THAT(icing.Get("namespace", "uri"),
                IsOkAndHolds(EqualsProto(GetDefaultDocument())));
    EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                IsOkAndHolds(EqualsProto(expected_result)));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  // Change the header's checksum value
  uint32_t invalid_checksum =
      1;  // Anything that's not the actual checksum value
  filesystem()->PWrite(GetHeaderFilename().c_str(),
                       offsetof(IcingSearchEngine::Header, checksum),
                       &invalid_checksum, sizeof(invalid_checksum));

  // We should be able to recover from this and access all our previous data
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_EXPECT_OK(icing.Initialize());

  // Checks that DocumentLog is still ok
  EXPECT_THAT(icing.Get("namespace", "uri"),
              IsOkAndHolds(EqualsProto(GetDefaultDocument())));

  // Checks that the index is still ok so we can search over it
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(expected_result)));

  // Checks that Schema is still since it'll be needed to validate the document
  ICING_EXPECT_OK(icing.Put(GetDefaultDocument()));
}

TEST_F(IcingSearchEngineTest, UnableToRecoverFromCorruptSchema) {
  {
    // Basic initialization/setup
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ICING_EXPECT_OK(icing.Initialize());
    ICING_EXPECT_OK(icing.SetSchema(GetDefaultSchema()));
    ICING_EXPECT_OK(icing.Put(GetDefaultDocument()));
    EXPECT_THAT(icing.Get("namespace", "uri"),
                IsOkAndHolds(EqualsProto(GetDefaultDocument())));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  const std::string schema_file =
      absl_ports::StrCat(GetSchemaDir(), "/schema.pb");
  const std::string corrupt_data = "1234";
  EXPECT_TRUE(filesystem()->Write(schema_file.c_str(), corrupt_data.data(),
                                  corrupt_data.size()));

  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize(),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL));
}

TEST_F(IcingSearchEngineTest, UnableToRecoverFromCorruptDocumentLog) {
  {
    // Basic initialization/setup
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ICING_EXPECT_OK(icing.Initialize());
    ICING_EXPECT_OK(icing.SetSchema(GetDefaultSchema()));
    ICING_EXPECT_OK(icing.Put(GetDefaultDocument()));
    EXPECT_THAT(icing.Get("namespace", "uri"),
                IsOkAndHolds(EqualsProto(GetDefaultDocument())));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  const std::string document_log_file =
      absl_ports::StrCat(GetDocumentDir(), "/document_log");
  const std::string corrupt_data = "1234";
  EXPECT_TRUE(filesystem()->Write(document_log_file.c_str(),
                                  corrupt_data.data(), corrupt_data.size()));

  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize(),
              StatusIs(libtextclassifier3::StatusCode::INTERNAL));
}

TEST_F(IcingSearchEngineTest, RecoverFromInconsistentSchemaStore) {
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Message")
          .AddStringProperty("additional", "content")
          .AddStringProperty("body", "message body")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  {
    // Initializes folder and schema
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ICING_EXPECT_OK(icing.Initialize());

    SchemaProto schema;
    auto type = schema.add_types();
    type->set_schema_type("Message");

    auto property = type->add_properties();
    property->set_property_name("body");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);
    property->mutable_indexing_config()->set_term_match_type(
        TermMatchType::PREFIX);
    property->mutable_indexing_config()->set_tokenizer_type(
        IndexingConfig::TokenizerType::PLAIN);

    property = type->add_properties();
    property->set_property_name("additional");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

    ICING_EXPECT_OK(icing.SetSchema(schema));
    ICING_EXPECT_OK(icing.Put(GetDefaultDocument()));
    ICING_EXPECT_OK(icing.Put(document2));

    // Won't get us anything because "additional" isn't marked as an indexed
    // property in the schema
    SearchSpecProto search_spec;
    search_spec.set_query("additional:content");
    search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

    SearchResultProto expected_result;
    EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                IsOkAndHolds(EqualsProto(expected_result)));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  {
    // This schema will change the SchemaTypeIds from the previous schema_
    // (since SchemaTypeIds are assigned based on order of the types, and this
    // new schema changes the ordering of previous types)
    SchemaProto new_schema;
    auto type = new_schema.add_types();
    type->set_schema_type("Email");

    type = new_schema.add_types();
    type->set_schema_type("Message");

    // Adding a new property changes the SectionIds (since SectionIds are
    // assigned based on alphabetical order of indexed sections, marking
    // "additional" as an indexed property will push the "body" property to a
    // different SectionId)
    auto property = type->add_properties();
    property->set_property_name("body");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);
    property->mutable_indexing_config()->set_term_match_type(
        TermMatchType::PREFIX);
    property->mutable_indexing_config()->set_tokenizer_type(
        IndexingConfig::TokenizerType::PLAIN);

    property = type->add_properties();
    property->set_property_name("additional");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
    property->mutable_indexing_config()->set_term_match_type(
        TermMatchType::PREFIX);
    property->mutable_indexing_config()->set_tokenizer_type(
        IndexingConfig::TokenizerType::PLAIN);

    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(filesystem(), GetSchemaDir()));
    ICING_EXPECT_OK(schema_store->SetSchema(new_schema));
  }  // Will persist new schema

  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_EXPECT_OK(icing.Initialize());

  // We can insert a Email document since we kept the new schema
  DocumentProto email_document =
      DocumentBuilder()
          .SetKey("namespace", "email_uri")
          .SetSchema("Email")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  ICING_EXPECT_OK(icing.Put(email_document));
  EXPECT_THAT(icing.Get("namespace", "email_uri"),
              IsOkAndHolds(EqualsProto(email_document)));

  SearchSpecProto search_spec;

  // The section restrict will ensure we are using the correct, updated
  // SectionId in the Index
  search_spec.set_query("additional:content");

  // Schema type filter will ensure we're using the correct, updated
  // SchemaTypeId in the DocumentStore
  search_spec.add_schema_type_filters("Message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_result;
  (*expected_result.mutable_results()->Add()->mutable_document()) = document2;

  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(expected_result)));
}

TEST_F(IcingSearchEngineTest, RecoverFromInconsistentDocumentStore) {
  {
    // Initializes folder and schema, index one document
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ICING_EXPECT_OK(icing.Initialize());
    ICING_EXPECT_OK(icing.SetSchema(GetDefaultSchema()));
    ICING_EXPECT_OK(icing.Put(GetDefaultDocument()));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Message")
          .AddStringProperty("body", "message body")
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(filesystem(), GetSchemaDir()));
    ICING_EXPECT_OK(schema_store->SetSchema(GetDefaultSchema()));

    // Puts a second document into DocumentStore but doesn't index it.
    FakeClock fake_clock;
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DocumentStore> document_store,
        DocumentStore::Create(filesystem(), GetDocumentDir(), &fake_clock,
                              schema_store.get()));
    ICING_EXPECT_OK(document_store->Put(document2));
  }

  IcingSearchEngine icing(GetDefaultIcingOptions());
  // Index Restoration should be triggered here and document2 should be
  // indexed.
  ICING_EXPECT_OK(icing.Initialize());

  // DocumentStore kept the additional document
  EXPECT_THAT(icing.Get("namespace", "uri"),
              IsOkAndHolds(EqualsProto(GetDefaultDocument())));
  EXPECT_THAT(icing.Get("namespace", "uri2"),
              IsOkAndHolds(EqualsProto(document2)));

  // We indexed the additional document
  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_result;
  (*expected_result.mutable_results()->Add()->mutable_document()) = document2;
  (*expected_result.mutable_results()->Add()->mutable_document()) =
      GetDefaultDocument();

  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(expected_result)));
}

TEST_F(IcingSearchEngineTest, RecoverFromInconsistentIndex) {
  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_result;
  (*expected_result.mutable_results()->Add()->mutable_document()) =
      GetDefaultDocument();

  {
    // Initializes folder and schema, index one document
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ICING_EXPECT_OK(icing.Initialize());
    ICING_EXPECT_OK(icing.SetSchema(GetDefaultSchema()));
    ICING_EXPECT_OK(icing.Put(GetDefaultDocument()));
    EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                IsOkAndHolds(EqualsProto(expected_result)));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  // Pretend we lost the entire index
  EXPECT_TRUE(filesystem()->DeleteDirectoryRecursively(
      absl_ports::StrCat(GetIndexDir(), "/idx/lite.").c_str()));

  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_EXPECT_OK(icing.Initialize());

  // Check that our index is ok by searching over the restored index
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(expected_result)));
}

TEST_F(IcingSearchEngineTest, RecoverFromCorruptIndex) {
  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_result;
  (*expected_result.mutable_results()->Add()->mutable_document()) =
      GetDefaultDocument();

  {
    // Initializes folder and schema, index one document
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ICING_EXPECT_OK(icing.Initialize());
    ICING_EXPECT_OK(icing.SetSchema(GetDefaultSchema()));
    ICING_EXPECT_OK(icing.Put(GetDefaultDocument()));
    EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                IsOkAndHolds(EqualsProto(expected_result)));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  // Pretend index is corrupted
  const std::string index_hit_buffer_file = GetIndexDir() + "/idx/lite.hb";
  ScopedFd fd(filesystem()->OpenForWrite(index_hit_buffer_file.c_str()));
  ASSERT_TRUE(fd.is_valid());
  ASSERT_TRUE(filesystem()->Write(fd.get(), "1234", 4));

  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_EXPECT_OK(icing.Initialize());

  // Check that our index is ok by searching over the restored index
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(expected_result)));
}

TEST_F(IcingSearchEngineTest, SearchResultShouldBeRankedByDocumentScore) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_EXPECT_OK(icing.Initialize());
  ICING_EXPECT_OK(icing.SetSchema(GetDefaultSchema()));

  // Creates 3 documents and ensures the relationship in terms of document
  // score is: document1 < document2 < document3
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace", "uri/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetScore(1)
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace", "uri/2")
          .SetSchema("Message")
          .AddStringProperty("body", "message2")
          .SetScore(2)
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  DocumentProto document3 =
      DocumentBuilder()
          .SetKey("namespace", "uri/3")
          .SetSchema("Message")
          .AddStringProperty("body", "message3")
          .SetScore(3)
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();

  // Intentionally inserts the documents in the order that is different than
  // their score order
  ICING_ASSERT_OK(icing.Put(document2));
  ICING_ASSERT_OK(icing.Put(document3));
  ICING_ASSERT_OK(icing.Put(document1));

  // "m" will match all 3 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  // Result should be in descending score order
  SearchResultProto exp_result;
  (*exp_result.mutable_results()->Add()->mutable_document()) = document3;
  (*exp_result.mutable_results()->Add()->mutable_document()) = document2;
  (*exp_result.mutable_results()->Add()->mutable_document()) = document1;

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  EXPECT_THAT(icing.Search(search_spec, scoring_spec,
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(exp_result)));
}

TEST_F(IcingSearchEngineTest, SearchShouldAllowNoScoring) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_EXPECT_OK(icing.Initialize());
  ICING_EXPECT_OK(icing.SetSchema(GetDefaultSchema()));

  // Creates 3 documents and ensures the relationship of them is:
  // document1 < document2 < document3
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace", "uri/1")
                                .SetSchema("Message")
                                .AddStringProperty("body", "message1")
                                .SetScore(1)
                                .SetCreationTimestampSecs(1571111111)
                                .Build();
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace", "uri/2")
                                .SetSchema("Message")
                                .AddStringProperty("body", "message2")
                                .SetScore(2)
                                .SetCreationTimestampSecs(1572222222)
                                .Build();
  DocumentProto document3 = DocumentBuilder()
                                .SetKey("namespace", "uri/3")
                                .SetSchema("Message")
                                .AddStringProperty("body", "message3")
                                .SetScore(3)
                                .SetCreationTimestampSecs(1573333333)
                                .Build();

  // Intentionally inserts the documents in the order that is different than
  // their score order
  ICING_ASSERT_OK(icing.Put(document3));
  ICING_ASSERT_OK(icing.Put(document1));
  ICING_ASSERT_OK(icing.Put(document2));

  // "m" will match all 3 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  SearchResultProto exp_result;
  (*exp_result.mutable_results()->Add()->mutable_document()) = document2;
  (*exp_result.mutable_results()->Add()->mutable_document()) = document1;
  (*exp_result.mutable_results()->Add()->mutable_document()) = document3;

  // Results should not be ranked by score but returned in reverse insertion
  // order.
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::NONE);
  EXPECT_THAT(icing.Search(search_spec, scoring_spec,
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(exp_result)));
}

TEST_F(IcingSearchEngineTest, SearchResultShouldBeRankedByCreationTimestamp) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_EXPECT_OK(icing.Initialize());
  ICING_EXPECT_OK(icing.SetSchema(GetDefaultSchema()));

  // Creates 3 documents and ensures the relationship in terms of creation
  // timestamp score is: document1 < document2 < document3
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace", "uri/1")
                                .SetSchema("Message")
                                .AddStringProperty("body", "message1")
                                .SetCreationTimestampSecs(1571111111)
                                .Build();
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace", "uri/2")
                                .SetSchema("Message")
                                .AddStringProperty("body", "message2")
                                .SetCreationTimestampSecs(1572222222)
                                .Build();
  DocumentProto document3 = DocumentBuilder()
                                .SetKey("namespace", "uri/3")
                                .SetSchema("Message")
                                .AddStringProperty("body", "message3")
                                .SetCreationTimestampSecs(1573333333)
                                .Build();

  // Intentionally inserts the documents in the order that is different than
  // their score order
  ICING_ASSERT_OK(icing.Put(document3));
  ICING_ASSERT_OK(icing.Put(document1));
  ICING_ASSERT_OK(icing.Put(document2));

  // "m" will match all 3 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  // Result should be in descending timestamp order
  SearchResultProto exp_result;
  (*exp_result.mutable_results()->Add()->mutable_document()) = document3;
  (*exp_result.mutable_results()->Add()->mutable_document()) = document2;
  (*exp_result.mutable_results()->Add()->mutable_document()) = document1;

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::CREATION_TIMESTAMP);
  EXPECT_THAT(icing.Search(search_spec, scoring_spec,
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(exp_result)));
}

TEST_F(IcingSearchEngineTest, SearchResultShouldBeRankedAscendingly) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_EXPECT_OK(icing.Initialize());
  ICING_EXPECT_OK(icing.SetSchema(GetDefaultSchema()));

  // Creates 3 documents and ensures the relationship in terms of document
  // score is: document1 < document2 < document3
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace", "uri/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetScore(1)
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace", "uri/2")
          .SetSchema("Message")
          .AddStringProperty("body", "message2")
          .SetScore(2)
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();
  DocumentProto document3 =
      DocumentBuilder()
          .SetKey("namespace", "uri/3")
          .SetSchema("Message")
          .AddStringProperty("body", "message3")
          .SetScore(3)
          .SetCreationTimestampSecs(kDefaultCreationTimestampSecs)
          .Build();

  // Intentionally inserts the documents in the order that is different than
  // their score order
  ICING_ASSERT_OK(icing.Put(document2));
  ICING_ASSERT_OK(icing.Put(document3));
  ICING_ASSERT_OK(icing.Put(document1));

  // "m" will match all 3 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  // Result should be in ascending score order
  SearchResultProto exp_result;
  (*exp_result.mutable_results()->Add()->mutable_document()) = document1;
  (*exp_result.mutable_results()->Add()->mutable_document()) = document2;
  (*exp_result.mutable_results()->Add()->mutable_document()) = document3;

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  scoring_spec.set_order_by(ScoringSpecProto::Order::ASC);
  EXPECT_THAT(icing.Search(search_spec, scoring_spec,
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(exp_result)));
}

TEST_F(IcingSearchEngineTest,
       SetSchemaCanNotDetectPreviousSchemaWasLostWithoutDocuments) {
  SchemaProto schema;
  auto type = schema.add_types();
  type->set_schema_type("Message");

  auto body = type->add_properties();
  body->set_property_name("body");
  body->set_data_type(PropertyConfigProto::DataType::STRING);
  body->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  // Make an incompatible schema, a previously OPTIONAL field is REQUIRED
  SchemaProto incompatible_schema = schema;
  incompatible_schema.mutable_types(0)->mutable_properties(0)->set_cardinality(
      PropertyConfigProto::Cardinality::REQUIRED);

  {
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ICING_ASSERT_OK(icing.Initialize());
    ICING_ASSERT_OK(icing.SetSchema(schema));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  ASSERT_TRUE(filesystem()->DeleteDirectoryRecursively(GetSchemaDir().c_str()));

  // Since we don't have any documents yet, we can't detect this edge-case. But
  // it should be fine since there aren't any documents to be invalidated.
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_EXPECT_OK(icing.Initialize());
  ICING_EXPECT_OK(icing.SetSchema(incompatible_schema));
}

TEST_F(IcingSearchEngineTest, SetSchemaCanDetectPreviousSchemaWasLost) {
  SchemaProto schema;
  auto type = schema.add_types();
  type->set_schema_type("Message");

  auto body = type->add_properties();
  body->set_property_name("body");
  body->set_data_type(PropertyConfigProto::DataType::STRING);
  body->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
  body->mutable_indexing_config()->set_term_match_type(TermMatchType::PREFIX);
  body->mutable_indexing_config()->set_tokenizer_type(
      IndexingConfig::TokenizerType::PLAIN);

  // Make an incompatible schema, a previously OPTIONAL field is REQUIRED
  SchemaProto incompatible_schema = schema;
  incompatible_schema.mutable_types(0)->mutable_properties(0)->set_cardinality(
      PropertyConfigProto::Cardinality::REQUIRED);

  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  {
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ICING_ASSERT_OK(icing.Initialize());
    ICING_ASSERT_OK(icing.SetSchema(schema));

    DocumentProto document = GetDefaultDocument();
    ICING_ASSERT_OK(icing.Put(document));

    // Can retrieve by namespace/uri
    ASSERT_THAT(icing.Get("namespace", "uri"),
                IsOkAndHolds(EqualsProto(document)));

    // Can search for it
    SearchResultProto expected_result;
    (*expected_result.mutable_results()->Add()->mutable_document()) =
        GetDefaultDocument();
    ASSERT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                IsOkAndHolds(EqualsProto(expected_result)));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  ASSERT_TRUE(filesystem()->DeleteDirectoryRecursively(GetSchemaDir().c_str()));

  // Setting the new, different schema will remove incompatible documents
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ICING_EXPECT_OK(icing.Initialize());
  ICING_EXPECT_OK(icing.SetSchema(incompatible_schema));

  // Can't retrieve by namespace/uri
  EXPECT_THAT(icing.Get("namespace", "uri"),
              StatusIs(libtextclassifier3::StatusCode::NOT_FOUND));

  // Can't search for it
  SearchResultProto empty_result;
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              IsOkAndHolds(EqualsProto(empty_result)));
}

}  // namespace
}  // namespace lib
}  // namespace icing
