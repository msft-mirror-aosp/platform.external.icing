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
#include "icing/helpers/icu/icu-data-file-helper.h"
#include "icing/portable/equals-proto.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/initialize.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/status.pb.h"
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
using ::testing::Gt;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Lt;
using ::testing::Matcher;
using ::testing::Return;
using ::testing::SizeIs;
using ::testing::StrEq;

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
    // File generated via icu_data_file rule in //icing/BUILD.
    std::string icu_data_file_path =
        GetTestFilePath("icing/icu.dat");
    ICING_ASSERT_OK(icu_data_file_helper::SetUpICUDataFile(icu_data_file_path));
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
constexpr int64_t kDefaultCreationTimestampMs = 1575492852000;

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

DocumentProto CreateMessageDocument(std::string name_space, std::string uri) {
  return DocumentBuilder()
      .SetKey(std::move(name_space), std::move(uri))
      .SetSchema("Message")
      .AddStringProperty("body", "message body")
      .SetCreationTimestampMs(kDefaultCreationTimestampMs)
      .Build();
}

SchemaProto CreateMessageSchema() {
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

SchemaProto CreateEmailSchema() {
  SchemaProto schema;
  auto* type = schema.add_types();
  type->set_schema_type("Email");

  auto* body = type->add_properties();
  body->set_property_name("body");
  body->set_data_type(PropertyConfigProto::DataType::STRING);
  body->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);
  body->mutable_indexing_config()->set_term_match_type(TermMatchType::PREFIX);
  body->mutable_indexing_config()->set_tokenizer_type(
      IndexingConfig::TokenizerType::PLAIN);
  auto* subj = type->add_properties();
  subj->set_property_name("subject");
  subj->set_data_type(PropertyConfigProto::DataType::STRING);
  subj->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);
  subj->mutable_indexing_config()->set_term_match_type(TermMatchType::PREFIX);
  subj->mutable_indexing_config()->set_tokenizer_type(
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
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  DocumentProto document = CreateMessageDocument("namespace", "uri");
  ASSERT_THAT(icing.Put(document).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(DocumentProto(document)).status().code(),
              Eq(StatusProto::OK));
}

TEST_F(IcingSearchEngineTest, InitializingAgainSavesNonPersistedData) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  DocumentProto document = CreateMessageDocument("namespace", "uri");
  ASSERT_THAT(icing.Put(document).status().code(), Eq(StatusProto::OK));

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = document;

  ASSERT_THAT(icing.Get("namespace", "uri"),
              EqualsProto(expected_get_result_proto));

  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.Get("namespace", "uri"),
              EqualsProto(expected_get_result_proto));
}

TEST_F(IcingSearchEngineTest, MaxIndexMergeSizeReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_index_merge_size(std::numeric_limits<int32_t>::max());
  IcingSearchEngine icing(options);
  EXPECT_THAT(icing.Initialize().status().code(),
              Eq(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineTest, NegativeMergeSizeReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_index_merge_size(-1);
  IcingSearchEngine icing(options);
  EXPECT_THAT(icing.Initialize().status().code(),
              Eq(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineTest, ZeroMergeSizeReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_index_merge_size(0);
  IcingSearchEngine icing(options);
  EXPECT_THAT(icing.Initialize().status().code(),
              Eq(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineTest, GoodIndexMergeSizeReturnsOk) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  // One is fine, if a bit weird. It just means that the lite index will be
  // smaller and will request a merge any time content is added to it.
  options.set_index_merge_size(1);
  IcingSearchEngine icing(options);
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
}

TEST_F(IcingSearchEngineTest,
       NegativeMaxTokensPerDocSizeReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_max_tokens_per_doc(-1);
  IcingSearchEngine icing(options);
  EXPECT_THAT(icing.Initialize().status().code(),
              Eq(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineTest, ZeroMaxTokensPerDocSizeReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_max_tokens_per_doc(0);
  IcingSearchEngine icing(options);
  EXPECT_THAT(icing.Initialize().status().code(),
              Eq(StatusProto::INVALID_ARGUMENT));
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
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
}

TEST_F(IcingSearchEngineTest, NegativeMaxTokenLenReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_max_token_length(-1);
  IcingSearchEngine icing(options);
  EXPECT_THAT(icing.Initialize().status().code(),
              Eq(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineTest, ZeroMaxTokenLenReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_max_token_length(0);
  IcingSearchEngine icing(options);
  EXPECT_THAT(icing.Initialize().status().code(),
              Eq(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineTest, MaxTokenLenReturnsOkAndTruncatesTokens) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  // A length of 1 is allowed - even though it would be strange to want
  // this.
  options.set_max_token_length(1);
  IcingSearchEngine icing(options);
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  DocumentProto document = CreateMessageDocument("namespace", "uri");
  EXPECT_THAT(icing.Put(document).status().code(), Eq(StatusProto::OK));

  // "message" should have been truncated to "m"
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  // The indexed tokens were  truncated to length of 1, so "m" will match
  search_spec.set_query("m");

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document;

  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));

  // The query token is also truncated to length of 1, so "me"->"m" matches "m"
  search_spec.set_query("me");
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));

  // The query token is still truncated to length of 1, so "massage"->"m"
  // matches "m"
  search_spec.set_query("massage");
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest,
       MaxIntMaxTokenLenReturnsOkTooLargeTokenReturnsResourceExhausted) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  // Set token length to max. This is allowed (it just means never to
  // truncate tokens). However, this does mean that tokens that exceed the
  // size of the lexicon will cause indexing to fail.
  options.set_max_token_length(std::numeric_limits<int32_t>::max());
  IcingSearchEngine icing(options);
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

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
  EXPECT_THAT(icing.Put(document).status().code(),
              Eq(StatusProto::OUT_OF_SPACE));

  SearchSpecProto search_spec;
  search_spec.set_query("p");
  search_spec.set_term_match_type(TermMatchType::PREFIX);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, FailToCreateDocStore) {
  auto mock_filesystem = std::make_unique<MockFilesystem>();
  // This fails DocumentStore::Create()
  ON_CALL(*mock_filesystem, CreateDirectoryRecursively(_))
      .WillByDefault(Return(false));

  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::move(mock_filesystem),
                              std::make_unique<FakeClock>());

  InitializeResultProto initialize_result_proto = icing.Initialize();
  EXPECT_THAT(initialize_result_proto.status().code(),
              Eq(StatusProto::INTERNAL));
  EXPECT_THAT(initialize_result_proto.status().message(),
              HasSubstr("Could not create directory"));
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
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.SetSchema(schema).status().code(),
              Eq(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineTest, PutWithoutSchemaFailedPrecondition) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  DocumentProto document = CreateMessageDocument("namespace", "uri");
  PutResultProto put_result_proto = icing.Put(document);
  EXPECT_THAT(put_result_proto.status().code(),
              Eq(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(put_result_proto.status().message(), HasSubstr("Schema not set"));
}

TEST_F(IcingSearchEngineTest, FailToReadSchema) {
  IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();

  {
    // Successfully initialize and set a schema
    IcingSearchEngine icing(icing_options);
    ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
                Eq(StatusProto::OK));
  }

  auto mock_filesystem = std::make_unique<MockFilesystem>();

  // This fails FileBackedProto::Read() when we try to check the schema we
  // had previously set
  ON_CALL(*mock_filesystem,
          OpenForRead(Eq(icing_options.base_dir() + "/schema_dir/schema.pb")))
      .WillByDefault(Return(-1));

  TestIcingSearchEngine test_icing(icing_options, std::move(mock_filesystem),
                                   std::make_unique<FakeClock>());

  InitializeResultProto initialize_result_proto = test_icing.Initialize();
  EXPECT_THAT(initialize_result_proto.status().code(),
              Eq(StatusProto::INTERNAL));
  EXPECT_THAT(initialize_result_proto.status().message(),
              HasSubstr("Unable to open file for read"));
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

  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  SetSchemaResultProto set_schema_result_proto =
      icing.SetSchema(CreateMessageSchema());
  EXPECT_THAT(set_schema_result_proto.status().code(),
              Eq(StatusProto::INTERNAL));
  EXPECT_THAT(set_schema_result_proto.status().message(),
              HasSubstr("Unable to open file for write"));
}

TEST_F(IcingSearchEngineTest, SetSchema) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  auto message_document = CreateMessageDocument("namespace", "uri");

  auto schema_with_message = CreateMessageSchema();

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
  EXPECT_THAT(icing.SetSchema(invalid_schema).status().code(),
              Eq(StatusProto::INVALID_ARGUMENT));

  // Can add an document of a set schema
  EXPECT_THAT(icing.SetSchema(schema_with_message).status().code(),
              Eq(StatusProto::OK));
  EXPECT_THAT(icing.Put(message_document).status().code(), Eq(StatusProto::OK));

  // Schema with Email doesn't have Message, so would result incompatible
  // data
  EXPECT_THAT(icing.SetSchema(schema_with_email).status().code(),
              Eq(StatusProto::FAILED_PRECONDITION));

  // Can expand the set of schema types and add an document of a new
  // schema type
  EXPECT_THAT(icing.SetSchema(SchemaProto(schema_with_email_and_message))
                  .status()
                  .code(),
              Eq(StatusProto::OK));
  EXPECT_THAT(icing.Put(message_document).status().code(), Eq(StatusProto::OK));

  // Can't add an document whose schema isn't set
  auto photo_document = DocumentBuilder()
                            .SetKey("namespace", "uri")
                            .SetSchema("Photo")
                            .AddStringProperty("creator", "icing")
                            .Build();
  PutResultProto put_result_proto = icing.Put(photo_document);
  EXPECT_THAT(put_result_proto.status().code(), Eq(StatusProto::NOT_FOUND));
  EXPECT_THAT(put_result_proto.status().message(),
              HasSubstr("'Photo' not found"));
}

TEST_F(IcingSearchEngineTest, SetSchemaTriggersIndexRestorationAndReturnsOk) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  SchemaProto schema_with_no_indexed_property = CreateMessageSchema();
  schema_with_no_indexed_property.mutable_types(0)
      ->mutable_properties(0)
      ->clear_indexing_config();

  EXPECT_THAT(icing.SetSchema(schema_with_no_indexed_property).status().code(),
              Eq(StatusProto::OK));
  // Nothing will be index and Search() won't return anything.
  EXPECT_THAT(
      icing.Put(CreateMessageDocument("namespace", "uri")).status().code(),
      Eq(StatusProto::OK));

  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto empty_result;
  empty_result.mutable_status()->set_code(StatusProto::OK);

  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(empty_result));

  SchemaProto schema_with_indexed_property = CreateMessageSchema();
  // Index restoration should be triggered here because new schema requires more
  // properties to be indexed.
  EXPECT_THAT(icing.SetSchema(schema_with_indexed_property).status().code(),
              Eq(StatusProto::OK));

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      CreateMessageDocument("namespace", "uri");
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, SetSchemaRevalidatesDocumentsAndReturnsOk) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  SchemaProto schema_with_optional_subject;
  auto type = schema_with_optional_subject.add_types();
  type->set_schema_type("email");

  // Add a OPTIONAL property
  auto property = type->add_properties();
  property->set_property_name("subject");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

  EXPECT_THAT(icing.SetSchema(schema_with_optional_subject).status().code(),
              Eq(StatusProto::OK));

  DocumentProto email_document_without_subject =
      DocumentBuilder()
          .SetKey("namespace", "without_subject")
          .SetSchema("email")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto email_document_with_subject =
      DocumentBuilder()
          .SetKey("namespace", "with_subject")
          .SetSchema("email")
          .AddStringProperty("subject", "foo")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  EXPECT_THAT(icing.Put(email_document_without_subject).status().code(),
              Eq(StatusProto::OK));
  EXPECT_THAT(icing.Put(email_document_with_subject).status().code(),
              Eq(StatusProto::OK));

  SchemaProto schema_with_required_subject;
  type = schema_with_required_subject.add_types();
  type->set_schema_type("email");

  // Add a REQUIRED property
  property = type->add_properties();
  property->set_property_name("subject");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);

  // Can't set the schema since it's incompatible
  SetSchemaResultProto expected_set_schema_result_proto;
  expected_set_schema_result_proto.mutable_status()->set_code(
      StatusProto::FAILED_PRECONDITION);
  expected_set_schema_result_proto.mutable_status()->set_message(
      "Schema is incompatible.");
  expected_set_schema_result_proto.add_incompatible_schema_types("email");

  EXPECT_THAT(icing.SetSchema(schema_with_required_subject),
              EqualsProto(expected_set_schema_result_proto));

  // Force set it
  expected_set_schema_result_proto.mutable_status()->set_code(StatusProto::OK);
  expected_set_schema_result_proto.mutable_status()->clear_message();
  EXPECT_THAT(icing.SetSchema(schema_with_required_subject,
                              /*ignore_errors_and_delete_documents=*/true),
              EqualsProto(expected_set_schema_result_proto));

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = email_document_with_subject;

  EXPECT_THAT(icing.Get("namespace", "with_subject"),
              EqualsProto(expected_get_result_proto));

  // The document without a subject got deleted because it failed validation
  // against the new schema
  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace, without_subject) not found.");
  expected_get_result_proto.clear_document();

  EXPECT_THAT(icing.Get("namespace", "without_subject"),
              EqualsProto(expected_get_result_proto));
}

TEST_F(IcingSearchEngineTest, SetSchemaDeletesDocumentsAndReturnsOk) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  SchemaProto schema;
  auto type = schema.add_types();
  type->set_schema_type("email");
  type = schema.add_types();
  type->set_schema_type("message");

  EXPECT_THAT(icing.SetSchema(schema).status().code(), Eq(StatusProto::OK));

  DocumentProto email_document =
      DocumentBuilder()
          .SetKey("namespace", "email_uri")
          .SetSchema("email")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message_document =
      DocumentBuilder()
          .SetKey("namespace", "message_uri")
          .SetSchema("message")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  EXPECT_THAT(icing.Put(email_document).status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.Put(message_document).status().code(), Eq(StatusProto::OK));

  // Clear the schema and only add the "email" type, essentially deleting the
  // "message" type
  SchemaProto new_schema;
  type = new_schema.add_types();
  type->set_schema_type("email");

  // Can't set the schema since it's incompatible
  SetSchemaResultProto expected_result;
  expected_result.mutable_status()->set_code(StatusProto::FAILED_PRECONDITION);
  expected_result.mutable_status()->set_message("Schema is incompatible.");
  expected_result.add_deleted_schema_types("message");

  EXPECT_THAT(icing.SetSchema(new_schema), EqualsProto(expected_result));

  // Force set it
  expected_result.mutable_status()->set_code(StatusProto::OK);
  expected_result.mutable_status()->clear_message();
  EXPECT_THAT(icing.SetSchema(new_schema,
                              /*ignore_errors_and_delete_documents=*/true),
              EqualsProto(expected_result));

  // "email" document is still there
  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = email_document;

  EXPECT_THAT(icing.Get("namespace", "email_uri"),
              EqualsProto(expected_get_result_proto));

  // "message" document got deleted
  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace, message_uri) not found.");
  expected_get_result_proto.clear_document();

  EXPECT_THAT(icing.Get("namespace", "message_uri"),
              EqualsProto(expected_get_result_proto));
}

TEST_F(IcingSearchEngineTest, GetSchemaNotFound) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  EXPECT_THAT(icing.GetSchema().status().code(), Eq(StatusProto::NOT_FOUND));
}

TEST_F(IcingSearchEngineTest, GetSchemaOk) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  GetSchemaResultProto expected_get_schema_result_proto;
  expected_get_schema_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_schema_result_proto.mutable_schema() = CreateMessageSchema();
  EXPECT_THAT(icing.GetSchema(), EqualsProto(expected_get_schema_result_proto));
}

TEST_F(IcingSearchEngineTest, GetSchemaTypeFailedPrecondition) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  GetSchemaTypeResultProto get_schema_type_result_proto =
      icing.GetSchemaType("nonexistent_schema");
  EXPECT_THAT(get_schema_type_result_proto.status().code(),
              Eq(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(get_schema_type_result_proto.status().message(),
              HasSubstr("Schema not set"));
}

TEST_F(IcingSearchEngineTest, GetSchemaTypeOk) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  GetSchemaTypeResultProto expected_get_schema_type_result_proto;
  expected_get_schema_type_result_proto.mutable_status()->set_code(
      StatusProto::OK);
  *expected_get_schema_type_result_proto.mutable_schema_type_config() =
      CreateMessageSchema().types(0);
  EXPECT_THAT(icing.GetSchemaType(CreateMessageSchema().types(0).schema_type()),
              EqualsProto(expected_get_schema_type_result_proto));
}

TEST_F(IcingSearchEngineTest, GetDocument) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  // Simple put and get
  ASSERT_THAT(
      icing.Put(CreateMessageDocument("namespace", "uri")).status().code(),
      Eq(StatusProto::OK));

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() =
      CreateMessageDocument("namespace", "uri");
  ASSERT_THAT(icing.Get("namespace", "uri"),
              EqualsProto(expected_get_result_proto));

  // Put an invalid document
  PutResultProto put_result_proto = icing.Put(DocumentProto());
  EXPECT_THAT(put_result_proto.status().code(),
              Eq(StatusProto::INVALID_ARGUMENT));
  EXPECT_THAT(put_result_proto.status().message(),
              HasSubstr("'namespace' is empty"));

  // Get a non-existing key
  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (wrong, uri) not found.");
  expected_get_result_proto.clear_document();
  ASSERT_THAT(icing.Get("wrong", "uri"),
              EqualsProto(expected_get_result_proto));
}

TEST_F(IcingSearchEngineTest, SearchReturnsValidResults) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  DocumentProto document_one = CreateMessageDocument("namespace", "uri1");
  ASSERT_THAT(icing.Put(document_one).status().code(), Eq(StatusProto::OK));

  DocumentProto document_two = CreateMessageDocument("namespace", "uri2");
  ASSERT_THAT(icing.Put(document_two).status().code(), Eq(StatusProto::OK));

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("message");

  ResultSpecProto result_spec;
  result_spec.mutable_snippet_spec()->set_max_window_bytes(64);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(1);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(1);

  SearchResultProto results =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(results.status().code(), Eq(StatusProto::OK));
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

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, SearchReturnsOneResult) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  DocumentProto document_one = CreateMessageDocument("namespace", "uri1");
  ASSERT_THAT(icing.Put(document_one).status().code(), Eq(StatusProto::OK));

  DocumentProto document_two = CreateMessageDocument("namespace", "uri2");
  ASSERT_THAT(icing.Put(document_two).status().code(), Eq(StatusProto::OK));

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("message");

  ResultSpecProto result_spec;
  result_spec.set_num_per_page(1);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document_two;

  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(search_result_proto.status().code(), Eq(StatusProto::OK));
  // The token is a random number so we don't verify it.
  expected_search_result_proto.set_next_page_token(
      search_result_proto.next_page_token());
  EXPECT_THAT(search_result_proto, EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, SearchZeroResultLimitReturnsEmptyResults) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("");

  ResultSpecProto result_spec;
  result_spec.set_num_per_page(0);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(), result_spec),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, SearchNegativeResultLimitReturnsInvalidArgument) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("");

  ResultSpecProto result_spec;
  result_spec.set_num_per_page(-5);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(
      StatusProto::INVALID_ARGUMENT);
  expected_search_result_proto.mutable_status()->set_message(
      "ResultSpecProto.num_per_page cannot be negative.");
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(), result_spec),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, SearchWithPersistenceReturnsValidResults) {
  IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();

  {
    // Set the schema up beforehand.
    IcingSearchEngine icing(icing_options);
    ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
                Eq(StatusProto::OK));
    // Schema will be persisted to disk when icing goes out of scope.
  }

  {
    // Ensure that icing initializes the schema and section_manager
    // properly from the pre-existing file.
    IcingSearchEngine icing(icing_options);
    ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

    EXPECT_THAT(
        icing.Put(CreateMessageDocument("namespace", "uri")).status().code(),
        Eq(StatusProto::OK));
    // The index and document store will be persisted to disk when icing goes
    // out of scope.
  }

  {
    // Ensure that the index is brought back up without problems and we
    // can query for the content that we expect.
    IcingSearchEngine icing(icing_options);
    ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

    SearchSpecProto search_spec;
    search_spec.set_term_match_type(TermMatchType::PREFIX);
    search_spec.set_query("message");

    SearchResultProto expected_search_result_proto;
    expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
    *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
        CreateMessageDocument("namespace", "uri");

    EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                EqualsProto(expected_search_result_proto));

    search_spec.set_query("foo");

    SearchResultProto empty_result;
    empty_result.mutable_status()->set_code(StatusProto::OK);
    EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                EqualsProto(empty_result));
  }
}

TEST_F(IcingSearchEngineTest, SearchShouldReturnEmpty) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("message");

  // Empty result, no next-page token
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);

  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());

  EXPECT_THAT(search_result_proto, EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, SearchShouldReturnMultiplePages) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  // Creates and inserts 5 documents
  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  DocumentProto document2 = CreateMessageDocument("namespace", "uri2");
  DocumentProto document3 = CreateMessageDocument("namespace", "uri3");
  DocumentProto document4 = CreateMessageDocument("namespace", "uri4");
  DocumentProto document5 = CreateMessageDocument("namespace", "uri5");
  ASSERT_THAT(icing.Put(document1).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document2).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document3).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document4).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document5).status().code(), Eq(StatusProto::OK));

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("message");

  ResultSpecProto result_spec;
  result_spec.set_num_per_page(2);

  // Searches and gets the first page, 2 results
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document5;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document4;
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(search_result_proto.next_page_token(), Gt(kInvalidNextPageToken));
  uint64_t next_page_token = search_result_proto.next_page_token();
  // Since the token is a random number, we don't need to verify
  expected_search_result_proto.set_next_page_token(next_page_token);
  EXPECT_THAT(search_result_proto, EqualsProto(expected_search_result_proto));

  // Second page, 2 results
  expected_search_result_proto.clear_results();
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document3;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  EXPECT_THAT(icing.GetNextPage(next_page_token),
              EqualsProto(expected_search_result_proto));

  // Third page, 1 result
  expected_search_result_proto.clear_results();
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document1;
  EXPECT_THAT(icing.GetNextPage(next_page_token),
              EqualsProto(expected_search_result_proto));

  // No more results
  expected_search_result_proto.clear_results();
  expected_search_result_proto.clear_next_page_token();
  EXPECT_THAT(icing.GetNextPage(next_page_token),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, SearchWithNoScoringShouldReturnMultiplePages) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  // Creates and inserts 5 documents
  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  DocumentProto document2 = CreateMessageDocument("namespace", "uri2");
  DocumentProto document3 = CreateMessageDocument("namespace", "uri3");
  DocumentProto document4 = CreateMessageDocument("namespace", "uri4");
  DocumentProto document5 = CreateMessageDocument("namespace", "uri5");
  ASSERT_THAT(icing.Put(document1).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document2).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document3).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document4).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document5).status().code(), Eq(StatusProto::OK));

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("message");

  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::NONE);

  ResultSpecProto result_spec;
  result_spec.set_num_per_page(2);

  // Searches and gets the first page, 2 results
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document5;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document4;
  SearchResultProto search_result_proto =
      icing.Search(search_spec, scoring_spec, result_spec);
  EXPECT_THAT(search_result_proto.next_page_token(), Gt(kInvalidNextPageToken));
  uint64_t next_page_token = search_result_proto.next_page_token();
  // Since the token is a random number, we don't need to verify
  expected_search_result_proto.set_next_page_token(next_page_token);
  EXPECT_THAT(search_result_proto, EqualsProto(expected_search_result_proto));

  // Second page, 2 results
  expected_search_result_proto.clear_results();
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document3;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  EXPECT_THAT(icing.GetNextPage(next_page_token),
              EqualsProto(expected_search_result_proto));

  // Third page, 1 result
  expected_search_result_proto.clear_results();
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document1;
  EXPECT_THAT(icing.GetNextPage(next_page_token),
              EqualsProto(expected_search_result_proto));

  // No more results
  expected_search_result_proto.clear_results();
  expected_search_result_proto.clear_next_page_token();
  EXPECT_THAT(icing.GetNextPage(next_page_token),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, ShouldReturnMultiplePagesWithSnippets) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  // Creates and inserts 5 documents
  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  DocumentProto document2 = CreateMessageDocument("namespace", "uri2");
  DocumentProto document3 = CreateMessageDocument("namespace", "uri3");
  DocumentProto document4 = CreateMessageDocument("namespace", "uri4");
  DocumentProto document5 = CreateMessageDocument("namespace", "uri5");
  ASSERT_THAT(icing.Put(document1).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document2).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document3).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document4).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document5).status().code(), Eq(StatusProto::OK));

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("message");

  ResultSpecProto result_spec;
  result_spec.set_num_per_page(2);
  result_spec.mutable_snippet_spec()->set_max_window_bytes(64);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(1);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(3);

  // Searches and gets the first page, 2 results with 2 snippets
  SearchResultProto search_result =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  ASSERT_THAT(search_result.status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(search_result.results(), SizeIs(2));
  ASSERT_THAT(search_result.next_page_token(), Gt(kInvalidNextPageToken));

  EXPECT_THAT(search_result.results(0).document(), EqualsProto(document5));
  EXPECT_THAT(GetMatch(search_result.results(0).document(),
                       search_result.results(0).snippet(), "body",
                       /*snippet_index=*/0),
              Eq("message"));
  EXPECT_THAT(GetWindow(search_result.results(0).document(),
                        search_result.results(0).snippet(), "body",
                        /*snippet_index=*/0),
              Eq("message body"));
  EXPECT_THAT(search_result.results(1).document(), EqualsProto(document4));
  EXPECT_THAT(GetMatch(search_result.results(1).document(),
                       search_result.results(1).snippet(), "body",
                       /*snippet_index=*/0),
              Eq("message"));
  EXPECT_THAT(GetWindow(search_result.results(1).document(),
                        search_result.results(1).snippet(), "body",
                        /*snippet_index=*/0),
              Eq("message body"));

  // Second page, 2 result with 1 snippet
  search_result = icing.GetNextPage(search_result.next_page_token());
  ASSERT_THAT(search_result.status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(search_result.results(), SizeIs(2));
  ASSERT_THAT(search_result.next_page_token(), Gt(kInvalidNextPageToken));

  EXPECT_THAT(search_result.results(0).document(), EqualsProto(document3));
  EXPECT_THAT(GetMatch(search_result.results(0).document(),
                       search_result.results(0).snippet(), "body",
                       /*snippet_index=*/0),
              Eq("message"));
  EXPECT_THAT(GetWindow(search_result.results(0).document(),
                        search_result.results(0).snippet(), "body",
                        /*snippet_index=*/0),
              Eq("message body"));
  EXPECT_THAT(search_result.results(1).document(), EqualsProto(document2));
  EXPECT_THAT(search_result.results(1).snippet().entries_size(), Eq(0));

  // Third page, 1 result with 0 snippets
  search_result = icing.GetNextPage(search_result.next_page_token());
  ASSERT_THAT(search_result.status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(search_result.results(), SizeIs(1));
  ASSERT_THAT(search_result.next_page_token(), Gt(kInvalidNextPageToken));

  EXPECT_THAT(search_result.results(0).document(), EqualsProto(document1));
  EXPECT_THAT(search_result.results(0).snippet().entries_size(), Eq(0));
}

TEST_F(IcingSearchEngineTest, ShouldInvalidateNextPageToken) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  DocumentProto document2 = CreateMessageDocument("namespace", "uri2");
  ASSERT_THAT(icing.Put(document1).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document2).status().code(), Eq(StatusProto::OK));

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("message");

  ResultSpecProto result_spec;
  result_spec.set_num_per_page(1);

  // Searches and gets the first page, 1 result
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(search_result_proto.next_page_token(), Gt(kInvalidNextPageToken));
  uint64_t next_page_token = search_result_proto.next_page_token();
  // Since the token is a random number, we don't need to verify
  expected_search_result_proto.set_next_page_token(next_page_token);
  EXPECT_THAT(search_result_proto, EqualsProto(expected_search_result_proto));
  // Now document1 is still to be fetched.

  // Invalidates token
  icing.InvalidateNextPageToken(next_page_token);

  // Tries to fetch the second page, no result since it's invalidated
  expected_search_result_proto.clear_results();
  expected_search_result_proto.clear_next_page_token();
  EXPECT_THAT(icing.GetNextPage(next_page_token),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest,
       AllPageTokensShouldBeInvalidatedAfterOptimization) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  DocumentProto document2 = CreateMessageDocument("namespace", "uri2");
  ASSERT_THAT(icing.Put(document1).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document2).status().code(), Eq(StatusProto::OK));

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("message");

  ResultSpecProto result_spec;
  result_spec.set_num_per_page(1);

  // Searches and gets the first page, 1 result
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(search_result_proto.next_page_token(), Gt(kInvalidNextPageToken));
  uint64_t next_page_token = search_result_proto.next_page_token();
  // Since the token is a random number, we don't need to verify
  expected_search_result_proto.set_next_page_token(next_page_token);
  EXPECT_THAT(search_result_proto, EqualsProto(expected_search_result_proto));
  // Now document1 is still to be fetched.

  OptimizeResultProto optimize_result_proto;
  optimize_result_proto.mutable_status()->set_code(StatusProto::OK);
  optimize_result_proto.mutable_status()->set_message("");
  ASSERT_THAT(icing.Optimize(), EqualsProto(optimize_result_proto));

  // Tries to fetch the second page, no results since all tokens have been
  // invalidated during Optimize()
  expected_search_result_proto.clear_results();
  expected_search_result_proto.clear_next_page_token();
  EXPECT_THAT(icing.GetNextPage(next_page_token),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, OptimizationShouldRemoveDeletedDocs) {
  IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();

  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace, uri1) not found.");
  {
    IcingSearchEngine icing(icing_options);
    ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
                Eq(StatusProto::OK));
    ASSERT_THAT(icing.Put(document1).status().code(), Eq(StatusProto::OK));

    // Deletes document1
    ASSERT_THAT(icing.Delete("namespace", "uri1").status().code(),
                Eq(StatusProto::OK));
    const std::string document_log_path =
        icing_options.base_dir() + "/document_dir/document_log";
    int64_t document_log_size_before =
        filesystem()->GetFileSize(document_log_path.c_str());
    ASSERT_THAT(icing.Optimize().status().code(), Eq(StatusProto::OK));
    int64_t document_log_size_after =
        filesystem()->GetFileSize(document_log_path.c_str());

    // Validates that document can't be found right after Optimize()
    EXPECT_THAT(icing.Get("namespace", "uri1"),
                EqualsProto(expected_get_result_proto));
    // Validates that document is actually removed from document log
    EXPECT_THAT(document_log_size_after, Lt(document_log_size_before));
  }  // Destroys IcingSearchEngine to make sure nothing is cached.

  IcingSearchEngine icing(icing_options);
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.Get("namespace", "uri1"),
              EqualsProto(expected_get_result_proto));
}

TEST_F(IcingSearchEngineTest, OptimizationShouldDeleteTemporaryDirectory) {
  IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();
  IcingSearchEngine icing(icing_options);
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

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

  EXPECT_THAT(icing.Optimize().status().code(), Eq(StatusProto::OK));

  EXPECT_FALSE(filesystem()->DirectoryExists(tmp_dir.c_str()));
  EXPECT_FALSE(filesystem()->FileExists(tmp_file.c_str()));
}

TEST_F(IcingSearchEngineTest, GetOptimizeInfoHasCorrectStats) {
  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace", "uri2")
                                .SetSchema("Message")
                                .AddStringProperty("body", "message body")
                                .SetCreationTimestampMs(100)
                                .SetTtlMs(500)
                                .Build();

  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetSystemTimeMilliseconds(1000);

  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::move(fake_clock));
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  // Just initialized, nothing is optimizable yet.
  GetOptimizeInfoResultProto optimize_info = icing.GetOptimizeInfo();
  EXPECT_THAT(optimize_info.status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(optimize_info.optimizable_docs(), Eq(0));
  EXPECT_THAT(optimize_info.estimated_optimizable_bytes(), Eq(0));

  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document1).status().code(), Eq(StatusProto::OK));

  // Only have active documents, nothing is optimizable yet.
  optimize_info = icing.GetOptimizeInfo();
  EXPECT_THAT(optimize_info.status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(optimize_info.optimizable_docs(), Eq(0));
  EXPECT_THAT(optimize_info.estimated_optimizable_bytes(), Eq(0));

  // Deletes document1
  ASSERT_THAT(icing.Delete("namespace", "uri1").status().code(),
              Eq(StatusProto::OK));

  optimize_info = icing.GetOptimizeInfo();
  EXPECT_THAT(optimize_info.status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(optimize_info.optimizable_docs(), Eq(1));
  EXPECT_THAT(optimize_info.estimated_optimizable_bytes(), Gt(0));
  int64_t first_estimated_optimizable_bytes =
      optimize_info.estimated_optimizable_bytes();

  // Add a second document, but it'll be expired since the time (1000) is
  // greater than the document's creation timestamp (100) + the document's ttl
  // (500)
  ASSERT_THAT(icing.Put(document2).status().code(), Eq(StatusProto::OK));

  optimize_info = icing.GetOptimizeInfo();
  EXPECT_THAT(optimize_info.status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(optimize_info.optimizable_docs(), Eq(2));
  EXPECT_THAT(optimize_info.estimated_optimizable_bytes(),
              Gt(first_estimated_optimizable_bytes));

  // Optimize
  ASSERT_THAT(icing.Optimize().status().code(), Eq(StatusProto::OK));

  // Nothing is optimizable now that everything has been optimized away.
  optimize_info = icing.GetOptimizeInfo();
  EXPECT_THAT(optimize_info.status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(optimize_info.optimizable_docs(), Eq(0));
  EXPECT_THAT(optimize_info.estimated_optimizable_bytes(), Eq(0));
}

TEST_F(IcingSearchEngineTest, GetAndPutShouldWorkAfterOptimization) {
  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  DocumentProto document2 = CreateMessageDocument("namespace", "uri2");
  DocumentProto document3 = CreateMessageDocument("namespace", "uri3");

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = document1;

  {
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
                Eq(StatusProto::OK));

    ASSERT_THAT(icing.Put(document1).status().code(), Eq(StatusProto::OK));
    ASSERT_THAT(icing.Optimize().status().code(), Eq(StatusProto::OK));

    // Validates that Get() and Put() are good right after Optimize()
    EXPECT_THAT(icing.Get("namespace", "uri1"),
                EqualsProto(expected_get_result_proto));
    EXPECT_THAT(icing.Put(document2).status().code(), Eq(StatusProto::OK));
  }  // Destroys IcingSearchEngine to make sure nothing is cached.

  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.Get("namespace", "uri1"),
              EqualsProto(expected_get_result_proto));

  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(icing.Get("namespace", "uri2"),
              EqualsProto(expected_get_result_proto));

  EXPECT_THAT(icing.Put(document3).status().code(), Eq(StatusProto::OK));
}

TEST_F(IcingSearchEngineTest, DeleteShouldWorkAfterOptimization) {
  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  DocumentProto document2 = CreateMessageDocument("namespace", "uri2");
  {
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
                Eq(StatusProto::OK));
    ASSERT_THAT(icing.Put(document1).status().code(), Eq(StatusProto::OK));
    ASSERT_THAT(icing.Put(document2).status().code(), Eq(StatusProto::OK));
    ASSERT_THAT(icing.Optimize().status().code(), Eq(StatusProto::OK));

    // Validates that Delete() works right after Optimize()
    EXPECT_THAT(icing.Delete("namespace", "uri1").status().code(),
                Eq(StatusProto::OK));

    GetResultProto expected_get_result_proto;
    expected_get_result_proto.mutable_status()->set_code(
        StatusProto::NOT_FOUND);
    expected_get_result_proto.mutable_status()->set_message(
        "Document (namespace, uri1) not found.");
    EXPECT_THAT(icing.Get("namespace", "uri1"),
                EqualsProto(expected_get_result_proto));

    expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
    expected_get_result_proto.mutable_status()->clear_message();
    *expected_get_result_proto.mutable_document() = document2;
    EXPECT_THAT(icing.Get("namespace", "uri2"),
                EqualsProto(expected_get_result_proto));
  }  // Destroys IcingSearchEngine to make sure nothing is cached.

  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.Delete("namespace", "uri2").status().code(),
              Eq(StatusProto::OK));

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace, uri1) not found.");
  EXPECT_THAT(icing.Get("namespace", "uri1"),
              EqualsProto(expected_get_result_proto));

  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace, uri2) not found.");
  EXPECT_THAT(icing.Get("namespace", "uri2"),
              EqualsProto(expected_get_result_proto));
}

TEST_F(IcingSearchEngineTest, DeleteBySchemaType) {
  SchemaProto schema;
  // Add an email type
  auto type = schema.add_types();
  type->set_schema_type("email");
  auto property = type->add_properties();
  property->set_property_name("subject");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
  // Add an message type
  type = schema.add_types();
  type->set_schema_type("message");
  property = type->add_properties();
  property->set_property_name("body");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace1", "uri1")
          .SetSchema("message")
          .AddStringProperty("body", "message body1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace2", "uri2")
          .SetSchema("email")
          .AddStringProperty("subject", "message body2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.SetSchema(schema).status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.Put(document1).status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.Put(document2).status().code(), Eq(StatusProto::OK));

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = document1;
  EXPECT_THAT(icing.Get("namespace1", "uri1"),
              EqualsProto(expected_get_result_proto));

  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(icing.Get("namespace2", "uri2"),
              EqualsProto(expected_get_result_proto));

  // Delete the first type. The first doc should be irretrievable. The
  // second should still be present.
  EXPECT_THAT(icing.DeleteBySchemaType("message").status().code(),
              Eq(StatusProto::OK));

  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace1, uri1) not found.");
  expected_get_result_proto.clear_document();
  EXPECT_THAT(icing.Get("namespace1", "uri1"),
              EqualsProto(expected_get_result_proto));

  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  expected_get_result_proto.mutable_status()->clear_message();
  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(icing.Get("namespace2", "uri2"),
              EqualsProto(expected_get_result_proto));
}

TEST_F(IcingSearchEngineTest, DeleteByNamespace) {
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace1", "uri1")
          .SetSchema("Message")
          .AddStringProperty("body", "message body1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace2", "uri2")
          .SetSchema("Message")
          .AddStringProperty("body", "message body2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));
  EXPECT_THAT(icing.Put(document1).status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.Put(document2).status().code(), Eq(StatusProto::OK));

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = document1;
  EXPECT_THAT(icing.Get("namespace1", "uri1"),
              EqualsProto(expected_get_result_proto));

  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(icing.Get("namespace2", "uri2"),
              EqualsProto(expected_get_result_proto));

  // Delete the first namespace. The first doc should be irretrievable. The
  // second should still be present.
  EXPECT_THAT(icing.DeleteByNamespace("namespace1").status().code(),
              Eq(StatusProto::OK));

  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace1, uri1) not found.");
  expected_get_result_proto.clear_document();
  EXPECT_THAT(icing.Get("namespace1", "uri1"),
              EqualsProto(expected_get_result_proto));

  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  expected_get_result_proto.mutable_status()->clear_message();
  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(icing.Get("namespace2", "uri2"),
              EqualsProto(expected_get_result_proto));
}

TEST_F(IcingSearchEngineTest, SetSchemaShouldWorkAfterOptimization) {
  // Creates 3 test schemas
  SchemaProto schema1 = SchemaProto(CreateMessageSchema());

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
    ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    ASSERT_THAT(icing.SetSchema(schema1).status().code(), Eq(StatusProto::OK));
    ASSERT_THAT(icing.Optimize().status().code(), Eq(StatusProto::OK));

    // Validates that SetSchema() works right after Optimize()
    EXPECT_THAT(icing.SetSchema(schema2).status().code(), Eq(StatusProto::OK));
  }  // Destroys IcingSearchEngine to make sure nothing is cached.

  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.SetSchema(schema3).status().code(), Eq(StatusProto::OK));
}

TEST_F(IcingSearchEngineTest, SearchShouldWorkAfterOptimization) {
  DocumentProto document = CreateMessageDocument("namespace", "uri");
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document;

  {
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
                Eq(StatusProto::OK));
    ASSERT_THAT(icing.Put(document).status().code(), Eq(StatusProto::OK));
    ASSERT_THAT(icing.Optimize().status().code(), Eq(StatusProto::OK));

    // Validates that Search() works right after Optimize()
    EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                EqualsProto(expected_search_result_proto));
  }  // Destroys IcingSearchEngine to make sure nothing is cached.

  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, IcingShouldWorkFineIfOptimizationIsAborted) {
  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  {
    // Initializes a normal icing to create files needed
    IcingSearchEngine icing(GetDefaultIcingOptions());
    ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
                Eq(StatusProto::OK));
    ASSERT_THAT(icing.Put(document1).status().code(), Eq(StatusProto::OK));
  }

  // Creates a mock filesystem in which DeleteDirectoryRecursively() always
  // fails. This will fail IcingSearchEngine::OptimizeDocumentStore() and makes
  // it return ABORTED_ERROR.
  auto mock_filesystem = std::make_unique<MockFilesystem>();
  ON_CALL(*mock_filesystem, DeleteDirectoryRecursively)
      .WillByDefault(Return(false));

  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::move(mock_filesystem),
                              std::make_unique<FakeClock>());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.Optimize().status().code(), Eq(StatusProto::ABORTED));

  // Now optimization is aborted, we verify that document-related functions
  // still work as expected.

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = document1;
  EXPECT_THAT(icing.Get("namespace", "uri1"),
              EqualsProto(expected_get_result_proto));

  DocumentProto document2 = CreateMessageDocument("namespace", "uri2");

  EXPECT_THAT(icing.Put(document2).status().code(), Eq(StatusProto::OK));

  SearchSpecProto search_spec;
  search_spec.set_query("m");
  search_spec.set_term_match_type(TermMatchType::PREFIX);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document1;

  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest,
       OptimizationShouldRecoverIfFileDirectoriesAreMissing) {
  // Creates a mock filesystem in which SwapFiles() always fails and deletes the
  // directories. This will fail IcingSearchEngine::OptimizeDocumentStore().
  auto mock_filesystem = std::make_unique<MockFilesystem>();
  ON_CALL(*mock_filesystem, SwapFiles)
      .WillByDefault([this](const char* one, const char* two) {
        filesystem()->DeleteDirectoryRecursively(one);
        filesystem()->DeleteDirectoryRecursively(two);
        return false;
      });

  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::move(mock_filesystem),
                              std::make_unique<FakeClock>());

  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));
  ASSERT_THAT(
      icing.Put(CreateMessageDocument("namespace", "uri")).status().code(),
      Eq(StatusProto::OK));

  // Optimize() fails due to filesystem error
  EXPECT_THAT(icing.Optimize().status().code(),
              Eq(StatusProto::WARNING_DATA_LOSS));

  // Document is not found because original file directory is missing
  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace, uri) not found.");
  EXPECT_THAT(icing.Get("namespace", "uri"),
              EqualsProto(expected_get_result_proto));

  DocumentProto new_document =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Message")
          .AddStringProperty("body", "new body")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  EXPECT_THAT(icing.Put(new_document).status().code(), Eq(StatusProto::OK));

  SearchSpecProto search_spec;
  search_spec.set_query("m");
  search_spec.set_term_match_type(TermMatchType::PREFIX);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);

  // Searching old content returns nothing because original file directory is
  // missing
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));

  search_spec.set_query("n");

  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      new_document;

  // Searching new content returns the new document
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, OptimizationShouldRecoverIfDataFilesAreMissing) {
  // Creates a mock filesystem in which SwapFiles() always fails and empties the
  // directories. This will fail IcingSearchEngine::OptimizeDocumentStore().
  auto mock_filesystem = std::make_unique<MockFilesystem>();
  ON_CALL(*mock_filesystem, SwapFiles)
      .WillByDefault([this](const char* one, const char* two) {
        filesystem()->DeleteDirectoryRecursively(one);
        filesystem()->CreateDirectoryRecursively(one);
        filesystem()->DeleteDirectoryRecursively(two);
        filesystem()->CreateDirectoryRecursively(two);
        return false;
      });

  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::move(mock_filesystem),
                              std::make_unique<FakeClock>());

  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));
  ASSERT_THAT(
      icing.Put(CreateMessageDocument("namespace", "uri")).status().code(),
      Eq(StatusProto::OK));

  // Optimize() fails due to filesystem error
  EXPECT_THAT(icing.Optimize().status().code(),
              Eq(StatusProto::WARNING_DATA_LOSS));

  // Document is not found because original files are missing
  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace, uri) not found.");
  EXPECT_THAT(icing.Get("namespace", "uri"),
              EqualsProto(expected_get_result_proto));

  DocumentProto new_document =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Message")
          .AddStringProperty("body", "new body")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  EXPECT_THAT(icing.Put(new_document).status().code(), Eq(StatusProto::OK));

  SearchSpecProto search_spec;
  search_spec.set_query("m");
  search_spec.set_term_match_type(TermMatchType::PREFIX);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);

  // Searching old content returns nothing because original files are missing
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));

  search_spec.set_query("n");

  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      new_document;

  // Searching new content returns the new document
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
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
                               .SetCreationTimestampMs(100)
                               .SetTtlMs(500)
                               .Build();

  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document;

  // Time just has to be less than the document's creation timestamp (100) + the
  // document's ttl (500)
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetSystemTimeMilliseconds(400);

  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::move(fake_clock));

  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.SetSchema(schema).status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.Put(document).status().code(), Eq(StatusProto::OK));

  // Check that the document is returned as part of search results
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
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
                               .SetCreationTimestampMs(100)
                               .SetTtlMs(500)
                               .Build();

  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);

  // Time just has to be greater than the document's creation timestamp (100) +
  // the document's ttl (500)
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetSystemTimeMilliseconds(700);

  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::move(fake_clock));

  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.SetSchema(schema).status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.Put(document).status().code(), Eq(StatusProto::OK));

  // Check that the document is not returned as part of search results
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
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
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.SetSchema(schema).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(message_document).status().code(), Eq(StatusProto::OK));

  // Make sure we can search for message document
  SearchSpecProto search_spec;
  search_spec.set_query("foo");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);

  // The message isn't indexed, so we get nothing
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));

  // With just the schema type filter, we can search for the message
  search_spec.Clear();
  search_spec.add_schema_type_filters("message");

  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      message_document;

  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));

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

  EXPECT_THAT(icing.SetSchema(schema).status().code(), Eq(StatusProto::OK));

  search_spec.Clear();
  search_spec.set_query("foo");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.add_schema_type_filters("message");

  // We can still search for the message document
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, RecoverFromMissingHeaderFile) {
  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      CreateMessageDocument("namespace", "uri");

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() =
      CreateMessageDocument("namespace", "uri");

  {
    // Basic initialization/setup
    IcingSearchEngine icing(GetDefaultIcingOptions());
    EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
                Eq(StatusProto::OK));
    EXPECT_THAT(
        icing.Put(CreateMessageDocument("namespace", "uri")).status().code(),
        Eq(StatusProto::OK));
    EXPECT_THAT(icing.Get("namespace", "uri"),
                EqualsProto(expected_get_result_proto));
    EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                EqualsProto(expected_search_result_proto));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  EXPECT_TRUE(filesystem()->DeleteFile(GetHeaderFilename().c_str()));

  // We should be able to recover from this and access all our previous data
  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  // Checks that DocumentLog is still ok
  EXPECT_THAT(icing.Get("namespace", "uri"),
              EqualsProto(expected_get_result_proto));

  // Checks that the index is still ok so we can search over it
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));

  // Checks that Schema is still since it'll be needed to validate the document
  EXPECT_THAT(
      icing.Put(CreateMessageDocument("namespace", "uri")).status().code(),
      Eq(StatusProto::OK));
}

TEST_F(IcingSearchEngineTest, RecoverFromInvalidHeaderMagic) {
  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      CreateMessageDocument("namespace", "uri");

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() =
      CreateMessageDocument("namespace", "uri");

  {
    // Basic initialization/setup
    IcingSearchEngine icing(GetDefaultIcingOptions());
    EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
                Eq(StatusProto::OK));
    EXPECT_THAT(
        icing.Put(CreateMessageDocument("namespace", "uri")).status().code(),
        Eq(StatusProto::OK));
    EXPECT_THAT(icing.Get("namespace", "uri"),
                EqualsProto(expected_get_result_proto));
    EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                EqualsProto(expected_search_result_proto));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  // Change the header's magic value
  int32_t invalid_magic = 1;  // Anything that's not the actual kMagic value.
  filesystem()->PWrite(GetHeaderFilename().c_str(),
                       offsetof(IcingSearchEngine::Header, magic),
                       &invalid_magic, sizeof(invalid_magic));

  // We should be able to recover from this and access all our previous data
  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  // Checks that DocumentLog is still ok
  EXPECT_THAT(icing.Get("namespace", "uri"),
              EqualsProto(expected_get_result_proto));

  // Checks that the index is still ok so we can search over it
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));

  // Checks that Schema is still since it'll be needed to validate the document
  EXPECT_THAT(
      icing.Put(CreateMessageDocument("namespace", "uri")).status().code(),
      Eq(StatusProto::OK));
}

TEST_F(IcingSearchEngineTest, RecoverFromInvalidHeaderChecksum) {
  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      CreateMessageDocument("namespace", "uri");

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() =
      CreateMessageDocument("namespace", "uri");

  {
    // Basic initialization/setup
    IcingSearchEngine icing(GetDefaultIcingOptions());
    EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
                Eq(StatusProto::OK));
    EXPECT_THAT(
        icing.Put(CreateMessageDocument("namespace", "uri")).status().code(),
        Eq(StatusProto::OK));
    EXPECT_THAT(icing.Get("namespace", "uri"),
                EqualsProto(expected_get_result_proto));
    EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                EqualsProto(expected_search_result_proto));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  // Change the header's checksum value
  uint32_t invalid_checksum =
      1;  // Anything that's not the actual checksum value
  filesystem()->PWrite(GetHeaderFilename().c_str(),
                       offsetof(IcingSearchEngine::Header, checksum),
                       &invalid_checksum, sizeof(invalid_checksum));

  // We should be able to recover from this and access all our previous data
  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  // Checks that DocumentLog is still ok
  EXPECT_THAT(icing.Get("namespace", "uri"),
              EqualsProto(expected_get_result_proto));

  // Checks that the index is still ok so we can search over it
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));

  // Checks that Schema is still since it'll be needed to validate the document
  EXPECT_THAT(
      icing.Put(CreateMessageDocument("namespace", "uri")).status().code(),
      Eq(StatusProto::OK));
}

TEST_F(IcingSearchEngineTest, UnableToRecoverFromCorruptSchema) {
  {
    // Basic initialization/setup
    IcingSearchEngine icing(GetDefaultIcingOptions());
    EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
                Eq(StatusProto::OK));
    EXPECT_THAT(
        icing.Put(CreateMessageDocument("namespace", "uri")).status().code(),
        Eq(StatusProto::OK));

    GetResultProto expected_get_result_proto;
    expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
    *expected_get_result_proto.mutable_document() =
        CreateMessageDocument("namespace", "uri");

    EXPECT_THAT(icing.Get("namespace", "uri"),
                EqualsProto(expected_get_result_proto));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  const std::string schema_file =
      absl_ports::StrCat(GetSchemaDir(), "/schema.pb");
  const std::string corrupt_data = "1234";
  EXPECT_TRUE(filesystem()->Write(schema_file.c_str(), corrupt_data.data(),
                                  corrupt_data.size()));

  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::INTERNAL));
}

TEST_F(IcingSearchEngineTest, UnableToRecoverFromCorruptDocumentLog) {
  {
    // Basic initialization/setup
    IcingSearchEngine icing(GetDefaultIcingOptions());
    EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
                Eq(StatusProto::OK));
    EXPECT_THAT(
        icing.Put(CreateMessageDocument("namespace", "uri")).status().code(),
        Eq(StatusProto::OK));

    GetResultProto expected_get_result_proto;
    expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
    *expected_get_result_proto.mutable_document() =
        CreateMessageDocument("namespace", "uri");

    EXPECT_THAT(icing.Get("namespace", "uri"),
                EqualsProto(expected_get_result_proto));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  const std::string document_log_file =
      absl_ports::StrCat(GetDocumentDir(), "/document_log");
  const std::string corrupt_data = "1234";
  EXPECT_TRUE(filesystem()->Write(document_log_file.c_str(),
                                  corrupt_data.data(), corrupt_data.size()));

  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::INTERNAL));
}

TEST_F(IcingSearchEngineTest, RecoverFromInconsistentSchemaStore) {
  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  DocumentProto document2_with_additional_property =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Message")
          .AddStringProperty("additional", "content")
          .AddStringProperty("body", "message body")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  {
    // Initializes folder and schema
    IcingSearchEngine icing(GetDefaultIcingOptions());
    EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

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

    EXPECT_THAT(icing.SetSchema(schema).status().code(), Eq(StatusProto::OK));
    EXPECT_THAT(icing.Put(document1).status().code(), Eq(StatusProto::OK));
    EXPECT_THAT(icing.Put(document2_with_additional_property).status().code(),
                Eq(StatusProto::OK));

    // Won't get us anything because "additional" isn't marked as an indexed
    // property in the schema
    SearchSpecProto search_spec;
    search_spec.set_query("additional:content");
    search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

    SearchResultProto expected_search_result_proto;
    expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
    EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                EqualsProto(expected_search_result_proto));
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
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  // We can insert a Email document since we kept the new schema
  DocumentProto email_document =
      DocumentBuilder()
          .SetKey("namespace", "email_uri")
          .SetSchema("Email")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  EXPECT_THAT(icing.Put(email_document).status().code(), Eq(StatusProto::OK));

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = email_document;

  EXPECT_THAT(icing.Get("namespace", "email_uri"),
              EqualsProto(expected_get_result_proto));

  SearchSpecProto search_spec;

  // The section restrict will ensure we are using the correct, updated
  // SectionId in the Index
  search_spec.set_query("additional:content");

  // Schema type filter will ensure we're using the correct, updated
  // SchemaTypeId in the DocumentStore
  search_spec.add_schema_type_filters("Message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2_with_additional_property;

  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, RecoverFromInconsistentDocumentStore) {
  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  DocumentProto document2 = CreateMessageDocument("namespace", "uri2");

  {
    // Initializes folder and schema, index one document
    IcingSearchEngine icing(GetDefaultIcingOptions());
    EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
                Eq(StatusProto::OK));
    EXPECT_THAT(icing.Put(document1).status().code(), Eq(StatusProto::OK));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(filesystem(), GetSchemaDir()));
    ICING_EXPECT_OK(schema_store->SetSchema(CreateMessageSchema()));

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
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = document1;

  // DocumentStore kept the additional document
  EXPECT_THAT(icing.Get("namespace", "uri1"),
              EqualsProto(expected_get_result_proto));

  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(icing.Get("namespace", "uri2"),
              EqualsProto(expected_get_result_proto));

  // We indexed the additional document
  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;

  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document1;

  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, RecoverFromInconsistentIndex) {
  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      CreateMessageDocument("namespace", "uri");

  {
    // Initializes folder and schema, index one document
    IcingSearchEngine icing(GetDefaultIcingOptions());
    EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
                Eq(StatusProto::OK));
    EXPECT_THAT(
        icing.Put(CreateMessageDocument("namespace", "uri")).status().code(),
        Eq(StatusProto::OK));
    EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                EqualsProto(expected_search_result_proto));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  // Pretend we lost the entire index
  EXPECT_TRUE(filesystem()->DeleteDirectoryRecursively(
      absl_ports::StrCat(GetIndexDir(), "/idx/lite.").c_str()));

  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  // Check that our index is ok by searching over the restored index
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, RecoverFromCorruptIndex) {
  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      CreateMessageDocument("namespace", "uri");

  {
    // Initializes folder and schema, index one document
    IcingSearchEngine icing(GetDefaultIcingOptions());
    EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
                Eq(StatusProto::OK));
    EXPECT_THAT(
        icing.Put(CreateMessageDocument("namespace", "uri")).status().code(),
        Eq(StatusProto::OK));
    EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                EqualsProto(expected_search_result_proto));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  // Pretend index is corrupted
  const std::string index_hit_buffer_file = GetIndexDir() + "/idx/lite.hb";
  ScopedFd fd(filesystem()->OpenForWrite(index_hit_buffer_file.c_str()));
  ASSERT_TRUE(fd.is_valid());
  ASSERT_TRUE(filesystem()->Write(fd.get(), "1234", 4));

  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));

  // Check that our index is ok by searching over the restored index
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, SearchResultShouldBeRankedByDocumentScore) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  // Creates 3 documents and ensures the relationship in terms of document
  // score is: document1 < document2 < document3
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace", "uri/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetScore(1)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace", "uri/2")
          .SetSchema("Message")
          .AddStringProperty("body", "message2")
          .SetScore(2)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document3 =
      DocumentBuilder()
          .SetKey("namespace", "uri/3")
          .SetSchema("Message")
          .AddStringProperty("body", "message3")
          .SetScore(3)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  // Intentionally inserts the documents in the order that is different than
  // their score order
  ASSERT_THAT(icing.Put(document2).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document3).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document1).status().code(), Eq(StatusProto::OK));

  // "m" will match all 3 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  // Result should be in descending score order
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document3;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document1;

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  EXPECT_THAT(icing.Search(search_spec, scoring_spec,
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, SearchShouldAllowNoScoring) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  // Creates 3 documents and ensures the relationship of them is:
  // document1 < document2 < document3
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace", "uri/1")
                                .SetSchema("Message")
                                .AddStringProperty("body", "message1")
                                .SetScore(1)
                                .SetCreationTimestampMs(1571111111111)
                                .Build();
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace", "uri/2")
                                .SetSchema("Message")
                                .AddStringProperty("body", "message2")
                                .SetScore(2)
                                .SetCreationTimestampMs(1572222222222)
                                .Build();
  DocumentProto document3 = DocumentBuilder()
                                .SetKey("namespace", "uri/3")
                                .SetSchema("Message")
                                .AddStringProperty("body", "message3")
                                .SetScore(3)
                                .SetCreationTimestampMs(1573333333333)
                                .Build();

  // Intentionally inserts the documents in the order that is different than
  // their score order
  ASSERT_THAT(icing.Put(document3).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document1).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document2).status().code(), Eq(StatusProto::OK));

  // "m" will match all 3 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document1;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document3;

  // Results should not be ranked by score but returned in reverse insertion
  // order.
  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::NONE);
  EXPECT_THAT(icing.Search(search_spec, scoring_spec,
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, SearchResultShouldBeRankedByCreationTimestamp) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  // Creates 3 documents and ensures the relationship in terms of creation
  // timestamp score is: document1 < document2 < document3
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("namespace", "uri/1")
                                .SetSchema("Message")
                                .AddStringProperty("body", "message1")
                                .SetCreationTimestampMs(1571111111111)
                                .Build();
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("namespace", "uri/2")
                                .SetSchema("Message")
                                .AddStringProperty("body", "message2")
                                .SetCreationTimestampMs(1572222222222)
                                .Build();
  DocumentProto document3 = DocumentBuilder()
                                .SetKey("namespace", "uri/3")
                                .SetSchema("Message")
                                .AddStringProperty("body", "message3")
                                .SetCreationTimestampMs(1573333333333)
                                .Build();

  // Intentionally inserts the documents in the order that is different than
  // their score order
  ASSERT_THAT(icing.Put(document3).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document1).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document2).status().code(), Eq(StatusProto::OK));

  // "m" will match all 3 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  // Result should be in descending timestamp order
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document3;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document1;

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(
      ScoringSpecProto::RankingStrategy::CREATION_TIMESTAMP);
  EXPECT_THAT(icing.Search(search_spec, scoring_spec,
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
}

TEST_F(IcingSearchEngineTest, SearchResultShouldBeRankedAscendingly) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  // Creates 3 documents and ensures the relationship in terms of document
  // score is: document1 < document2 < document3
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace", "uri/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message1")
          .SetScore(1)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace", "uri/2")
          .SetSchema("Message")
          .AddStringProperty("body", "message2")
          .SetScore(2)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document3 =
      DocumentBuilder()
          .SetKey("namespace", "uri/3")
          .SetSchema("Message")
          .AddStringProperty("body", "message3")
          .SetScore(3)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  // Intentionally inserts the documents in the order that is different than
  // their score order
  ASSERT_THAT(icing.Put(document2).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document3).status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.Put(document1).status().code(), Eq(StatusProto::OK));

  // "m" will match all 3 documents
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("m");

  // Result should be in ascending score order
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document1;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document3;

  ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  scoring_spec.set_order_by(ScoringSpecProto::Order::ASC);
  EXPECT_THAT(icing.Search(search_spec, scoring_spec,
                           ResultSpecProto::default_instance()),
              EqualsProto(expected_search_result_proto));
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
    ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    ASSERT_THAT(icing.SetSchema(schema).status().code(), Eq(StatusProto::OK));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  ASSERT_TRUE(filesystem()->DeleteDirectoryRecursively(GetSchemaDir().c_str()));

  // Since we don't have any documents yet, we can't detect this edge-case.  But
  // it should be fine since there aren't any documents to be invalidated.
  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.SetSchema(incompatible_schema).status().code(),
              Eq(StatusProto::OK));
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
    ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    ASSERT_THAT(icing.SetSchema(schema).status().code(), Eq(StatusProto::OK));

    DocumentProto document = CreateMessageDocument("namespace", "uri");
    ASSERT_THAT(icing.Put(document).status().code(), Eq(StatusProto::OK));

    // Can retrieve by namespace/uri
    GetResultProto expected_get_result_proto;
    expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
    *expected_get_result_proto.mutable_document() = document;

    ASSERT_THAT(icing.Get("namespace", "uri"),
                EqualsProto(expected_get_result_proto));

    // Can search for it
    SearchResultProto expected_search_result_proto;
    expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
    *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
        CreateMessageDocument("namespace", "uri");
    ASSERT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                             ResultSpecProto::default_instance()),
                EqualsProto(expected_search_result_proto));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  ASSERT_TRUE(filesystem()->DeleteDirectoryRecursively(GetSchemaDir().c_str()));

  // Setting the new, different schema will remove incompatible documents
  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.SetSchema(incompatible_schema).status().code(),
              Eq(StatusProto::OK));

  // Can't retrieve by namespace/uri
  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace, uri) not found.");

  EXPECT_THAT(icing.Get("namespace", "uri"),
              EqualsProto(expected_get_result_proto));

  // Can't search for it
  SearchResultProto empty_result;
  empty_result.mutable_status()->set_code(StatusProto::OK);
  EXPECT_THAT(icing.Search(search_spec, GetDefaultScoringSpec(),
                           ResultSpecProto::default_instance()),
              EqualsProto(empty_result));
}

TEST_F(IcingSearchEngineTest, PersistToDisk) {
  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() =
      CreateMessageDocument("namespace", "uri");

  {
    IcingSearchEngine icing(GetDefaultIcingOptions());
    EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
    EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
                Eq(StatusProto::OK));
    EXPECT_THAT(
        icing.Put(CreateMessageDocument("namespace", "uri")).status().code(),
        Eq(StatusProto::OK));

    // Persisting shouldn't affect anything
    EXPECT_THAT(icing.PersistToDisk().status().code(), Eq(StatusProto::OK));

    EXPECT_THAT(icing.Get("namespace", "uri"),
                EqualsProto(expected_get_result_proto));
  }  // Destructing persists as well

  IcingSearchEngine icing(GetDefaultIcingOptions());
  EXPECT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.Get("namespace", "uri"),
              EqualsProto(expected_get_result_proto));
}

TEST_F(IcingSearchEngineTest, ResetOk) {
  SchemaProto message_schema = CreateMessageSchema();
  SchemaProto empty_schema = SchemaProto(message_schema);
  empty_schema.clear_types();

  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(message_schema).status().code(),
              Eq(StatusProto::OK));

  int64_t empty_state_size =
      filesystem()->GetFileDiskUsage(GetTestBaseDir().c_str());

  DocumentProto document = CreateMessageDocument("namespace", "uri");
  ASSERT_THAT(icing.Put(document).status().code(), Eq(StatusProto::OK));

  // Check that things have been added
  EXPECT_THAT(filesystem()->GetDiskUsage(GetTestBaseDir().c_str()),
              Gt(empty_state_size));

  EXPECT_THAT(icing.Reset().status().code(), Eq(StatusProto::OK));

  // Check that we're back to an empty state
  EXPECT_EQ(filesystem()->GetFileDiskUsage(GetTestBaseDir().c_str()),
            empty_state_size);

  // Sanity check that we can still call other APIs. If things aren't cleared,
  // then this should raise an error since the empty schema is incompatible with
  // the old message_schema.
  EXPECT_THAT(icing.SetSchema(empty_schema).status().code(),
              Eq(StatusProto::OK));
}

TEST_F(IcingSearchEngineTest, ResetAbortedError) {
  auto mock_filesystem = std::make_unique<MockFilesystem>();

  // This fails IcingSearchEngine::Reset(). But since we didn't actually delete
  // anything, we'll be able to consider this just an ABORTED call.
  ON_CALL(*mock_filesystem,
          DeleteDirectoryRecursively(StrEq(GetTestBaseDir().c_str())))
      .WillByDefault(Return(false));

  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::move(mock_filesystem),
                              std::make_unique<FakeClock>());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  DocumentProto document = CreateMessageDocument("namespace", "uri");
  ASSERT_THAT(icing.Put(document).status().code(), Eq(StatusProto::OK));
  EXPECT_THAT(icing.Reset().status().code(), Eq(StatusProto::ABORTED));

  // Everything is still intact.
  // Can get old data.
  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = document;
  EXPECT_THAT(icing.Get(document.namespace_(), document.uri()),
              EqualsProto(expected_get_result_proto));

  // Can add new data.
  EXPECT_THAT(
      icing.Put(CreateMessageDocument("namespace", "uri")).status().code(),
      Eq(StatusProto::OK));
}

TEST_F(IcingSearchEngineTest, ResetInternalError) {
  auto mock_filesystem = std::make_unique<MockFilesystem>();

  // Let all other calls succeed.
  EXPECT_CALL(*mock_filesystem, Write(Matcher<const char*>(_), _, _))
      .WillRepeatedly(Return(true));

  // This prevents IcingSearchEngine from creating a DocumentStore instance on
  // reinitialization
  const std::string document_log_path =
      GetTestBaseDir() + "/document_dir/document_log";
  EXPECT_CALL(
      *mock_filesystem,
      Write(Matcher<const char*>(StrEq(document_log_path.c_str())), _, _))
      .WillOnce(Return(true))
      .WillOnce(Return(false));

  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::move(mock_filesystem),
                              std::make_unique<FakeClock>());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));
  EXPECT_THAT(icing.Reset().status().code(), Eq(StatusProto::INTERNAL));
}

TEST_F(IcingSearchEngineTest, SnippetNormalization) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetSchema("Message")
          .AddStringProperty("body", "MDI zurich Team Meeting")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_one).status().code(), Eq(StatusProto::OK));

  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Message")
          .AddStringProperty("body", "mdi Zürich Team Meeting")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_two).status().code(), Eq(StatusProto::OK));

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_query("mdi Zürich");

  ResultSpecProto result_spec;
  result_spec.mutable_snippet_spec()->set_max_window_bytes(64);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(2);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(2);

  SearchResultProto results =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(results.status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(results.results(), SizeIs(2));
  const DocumentProto& result_document_1 = results.results(0).document();
  const SnippetProto& result_snippet_1 = results.results(0).snippet();
  EXPECT_THAT(result_document_1, EqualsProto(document_two));
  EXPECT_THAT(GetMatch(result_document_1, result_snippet_1, "body",
                       /*snippet_index=*/0),
              Eq("mdi"));
  EXPECT_THAT(GetWindow(result_document_1, result_snippet_1, "body",
                        /*snippet_index=*/0),
              Eq("mdi Zürich Team Meeting"));
  EXPECT_THAT(GetMatch(result_document_1, result_snippet_1, "body",
                       /*snippet_index=*/1),
              Eq("Zürich"));
  EXPECT_THAT(GetWindow(result_document_1, result_snippet_1, "body",
                        /*snippet_index=*/1),
              Eq("mdi Zürich Team Meeting"));

  const DocumentProto& result_document_2 = results.results(1).document();
  const SnippetProto& result_snippet_2 = results.results(1).snippet();
  EXPECT_THAT(result_document_2, EqualsProto(document_one));
  EXPECT_THAT(GetMatch(result_document_2, result_snippet_2, "body",
                       /*snippet_index=*/0),
              Eq("MDI"));
  EXPECT_THAT(GetWindow(result_document_2, result_snippet_2, "body",
                        /*snippet_index=*/0),
              Eq("MDI zurich Team Meeting"));
  EXPECT_THAT(GetMatch(result_document_2, result_snippet_2, "body",
                       /*snippet_index=*/1),
              Eq("zurich"));
  EXPECT_THAT(GetWindow(result_document_2, result_snippet_2, "body",
                        /*snippet_index=*/1),
              Eq("MDI zurich Team Meeting"));
}

TEST_F(IcingSearchEngineTest, SnippetNormalizationPrefix) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status().code(),
              Eq(StatusProto::OK));

  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetSchema("Message")
          .AddStringProperty("body", "MDI zurich Team Meeting")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_one).status().code(), Eq(StatusProto::OK));

  DocumentProto document_two =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Message")
          .AddStringProperty("body", "mdi Zürich Team Meeting")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_two).status().code(), Eq(StatusProto::OK));

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("md Zür");

  ResultSpecProto result_spec;
  result_spec.mutable_snippet_spec()->set_max_window_bytes(64);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(2);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(2);

  SearchResultProto results =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(results.status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(results.results(), SizeIs(2));
  const DocumentProto& result_document_1 = results.results(0).document();
  const SnippetProto& result_snippet_1 = results.results(0).snippet();
  EXPECT_THAT(result_document_1, EqualsProto(document_two));
  EXPECT_THAT(GetMatch(result_document_1, result_snippet_1, "body",
                       /*snippet_index=*/0),
              Eq("mdi"));
  EXPECT_THAT(GetWindow(result_document_1, result_snippet_1, "body",
                        /*snippet_index=*/0),
              Eq("mdi Zürich Team Meeting"));
  EXPECT_THAT(GetMatch(result_document_1, result_snippet_1, "body",
                       /*snippet_index=*/1),
              Eq("Zürich"));
  EXPECT_THAT(GetWindow(result_document_1, result_snippet_1, "body",
                        /*snippet_index=*/1),
              Eq("mdi Zürich Team Meeting"));

  const DocumentProto& result_document_2 = results.results(1).document();
  const SnippetProto& result_snippet_2 = results.results(1).snippet();
  EXPECT_THAT(result_document_2, EqualsProto(document_one));
  EXPECT_THAT(GetMatch(result_document_2, result_snippet_2, "body",
                       /*snippet_index=*/0),
              Eq("MDI"));
  EXPECT_THAT(GetWindow(result_document_2, result_snippet_2, "body",
                        /*snippet_index=*/0),
              Eq("MDI zurich Team Meeting"));
  EXPECT_THAT(GetMatch(result_document_2, result_snippet_2, "body",
                       /*snippet_index=*/1),
              Eq("zurich"));
  EXPECT_THAT(GetWindow(result_document_2, result_snippet_2, "body",
                        /*snippet_index=*/1),
              Eq("MDI zurich Team Meeting"));
}

TEST_F(IcingSearchEngineTest, SnippetSectionRestrict) {
  IcingSearchEngine icing(GetDefaultIcingOptions());
  ASSERT_THAT(icing.Initialize().status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(icing.SetSchema(CreateEmailSchema()).status().code(),
              Eq(StatusProto::OK));

  DocumentProto document_one =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetSchema("Email")
          .AddStringProperty("subject", "MDI zurich Team Meeting")
          .AddStringProperty("body", "MDI zurich Team Meeting")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document_one).status().code(), Eq(StatusProto::OK));

  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  search_spec.set_query("body:Zür");

  ResultSpecProto result_spec;
  result_spec.mutable_snippet_spec()->set_max_window_bytes(64);
  result_spec.mutable_snippet_spec()->set_num_matches_per_property(10);
  result_spec.mutable_snippet_spec()->set_num_to_snippet(10);

  SearchResultProto results =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(results.status().code(), Eq(StatusProto::OK));
  ASSERT_THAT(results.results(), SizeIs(1));
  const DocumentProto& result_document = results.results(0).document();
  const SnippetProto& result_snippet = results.results(0).snippet();
  EXPECT_THAT(result_document, EqualsProto(document_one));
  EXPECT_THAT(
      GetMatch(result_document, result_snippet, "body", /*snippet_index=*/0),
      Eq("zurich"));
  EXPECT_THAT(
      GetWindow(result_document, result_snippet, "body", /*snippet_index=*/0),
      Eq("MDI zurich Team Meeting"));
  EXPECT_THAT(
      GetMatch(result_document, result_snippet, "subject", /*snippet_index=*/0),
      IsEmpty());
  EXPECT_THAT(GetWindow(result_document, result_snippet, "subject",
                        /*snippet_index=*/0),
              IsEmpty());
}

TEST_F(IcingSearchEngineTest, UninitializedInstanceFailsSafely) {
  IcingSearchEngine icing(GetDefaultIcingOptions());

  SchemaProto email_schema = CreateMessageSchema();
  EXPECT_THAT(icing.SetSchema(email_schema).status().code(),
              Eq(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(icing.GetSchema().status().code(),
              Eq(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(
      icing.GetSchemaType(email_schema.types(0).schema_type()).status().code(),
      Eq(StatusProto::FAILED_PRECONDITION));

  DocumentProto doc = CreateMessageDocument("namespace", "uri");
  EXPECT_THAT(icing.Put(doc).status().code(),
              Eq(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(icing.Get(doc.namespace_(), doc.uri()).status().code(),
              Eq(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(icing.Delete(doc.namespace_(), doc.uri()).status().code(),
              Eq(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(icing.DeleteByNamespace(doc.namespace_()).status().code(),
              Eq(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(icing.DeleteBySchemaType(email_schema.types(0).schema_type())
                  .status()
                  .code(),
              Eq(StatusProto::FAILED_PRECONDITION));

  SearchSpecProto search_spec = SearchSpecProto::default_instance();
  ScoringSpecProto scoring_spec = ScoringSpecProto::default_instance();
  ResultSpecProto result_spec = ResultSpecProto::default_instance();
  EXPECT_THAT(
      icing.Search(search_spec, scoring_spec, result_spec).status().code(),
      Eq(StatusProto::FAILED_PRECONDITION));
  constexpr int kSomePageToken = 12;
  EXPECT_THAT(icing.GetNextPage(kSomePageToken).status().code(),
              Eq(StatusProto::FAILED_PRECONDITION));
  icing.InvalidateNextPageToken(kSomePageToken);  // Verify this doesn't crash.

  EXPECT_THAT(icing.PersistToDisk().status().code(),
              Eq(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(icing.Optimize().status().code(),
              Eq(StatusProto::FAILED_PRECONDITION));
}

}  // namespace
}  // namespace lib
}  // namespace icing
