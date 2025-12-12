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

#include <chrono>  // NOLINT
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <thread>  // NOLINT
#include <utility>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/document-builder.h"
#include "icing/file/filesystem.h"
#include "icing/file/mock-filesystem.h"
#include "icing/icing-search-engine.h"
#include "icing/jni/jni-cache.h"
#include "icing/portable/endian.h"
#include "icing/portable/equals-proto.h"
#include "icing/portable/platform.h"
#include "icing/proto/debug.pb.h"
#include "icing/proto/document.pb.h"
#include "icing/proto/document_wrapper.pb.h"
#include "icing/proto/initialize.pb.h"
#include "icing/proto/logging.pb.h"
#include "icing/proto/optimize.pb.h"
#include "icing/proto/persist.pb.h"
#include "icing/proto/reset.pb.h"
#include "icing/proto/schema.pb.h"
#include "icing/proto/scoring.pb.h"
#include "icing/proto/search.pb.h"
#include "icing/proto/status.pb.h"
#include "icing/proto/storage.pb.h"
#include "icing/proto/term.pb.h"
#include "icing/proto/usage.pb.h"
#include "icing/schema-builder.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/jni-test-helpers.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/icu-data-file-helper.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::AllOf;
using ::testing::Eq;
using ::testing::Ge;
using ::testing::Gt;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Property;
using ::testing::Return;
using ::testing::SizeIs;
using ::testing::StrEq;
using ::testing::UnorderedElementsAre;

// For mocking purpose, we allow tests to provide a custom Filesystem.
class TestIcingSearchEngine : public IcingSearchEngine {
 public:
  TestIcingSearchEngine(const IcingSearchEngineOptions& options,
                        std::unique_ptr<const Filesystem> filesystem,
                        std::unique_ptr<const IcingFilesystem> icing_filesystem,
                        std::unique_ptr<Clock> clock,
                        std::unique_ptr<JniCache> jni_cache)
      : IcingSearchEngine(options, std::move(filesystem),
                          std::move(icing_filesystem), std::move(clock),
                          std::move(jni_cache)) {}
};

std::string GetTestBaseDir() { return GetTestTempDir() + "/icing"; }

// This test is meant to cover all tests relating to IcingSearchEngine::Delete*.
class IcingSearchEngineDeleteTest : public testing::Test {
 protected:
  void SetUp() override {
    if (!IsCfStringTokenization() && !IsReverseJniTokenization()) {
      // If we've specified using the reverse-JNI method for segmentation (i.e.
      // not ICU), then we won't have the ICU data file included to set up.
      // Technically, we could choose to use reverse-JNI for segmentation AND
      // include an ICU data file, but that seems unlikely and our current BUILD
      // setup doesn't do this.
      // File generated via icu_data_file rule in //icing/BUILD.
      std::string icu_data_file_path =
          GetTestFilePath("icing/icu.dat");
      ICING_ASSERT_OK(
          icu_data_file_helper::SetUpIcuDataFile(icu_data_file_path));
    }
    filesystem_.CreateDirectoryRecursively(GetTestBaseDir().c_str());
  }

  void TearDown() override {
    filesystem_.DeleteDirectoryRecursively(GetTestBaseDir().c_str());
  }

  const Filesystem* filesystem() const { return &filesystem_; }

 private:
  Filesystem filesystem_;
};

// Non-zero value so we don't override it to be the current time
constexpr int64_t kDefaultCreationTimestampMs = 1575492852000;

IcingSearchEngineOptions GetDefaultIcingOptions() {
  IcingSearchEngineOptions icing_options;
  icing_options.set_base_dir(GetTestBaseDir());
  icing_options.set_enable_qualified_id_join_index_v3(true);
  icing_options.set_enable_soft_index_restoration(true);
  icing_options.set_enable_delete_propagation_from(false);
  return icing_options;
}

SchemaTypeConfigProto CreateMessageSchemaTypeConfig() {
  return SchemaTypeConfigBuilder()
      .SetType("Message")
      .AddProperty(PropertyConfigBuilder()
                       .SetName("body")
                       .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                       .SetCardinality(CARDINALITY_REQUIRED))
      .Build();
}

SchemaTypeConfigProto CreateEmailSchemaTypeConfig() {
  return SchemaTypeConfigBuilder()
      .SetType("Email")
      .AddProperty(PropertyConfigBuilder()
                       .SetName("body")
                       .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                       .SetCardinality(CARDINALITY_REQUIRED))
      .AddProperty(PropertyConfigBuilder()
                       .SetName("subject")
                       .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                       .SetCardinality(CARDINALITY_REQUIRED))
      .Build();
}

ScoringSpecProto GetDefaultScoringSpec() {
  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  return scoring_spec;
}

TEST_F(IcingSearchEngineDeleteTest, Delete) {
  SchemaProto schema =
      SchemaBuilder().AddType(CreateMessageSchemaTypeConfig()).Build();

  DocumentProto document =
      DocumentBuilder()
          .SetKey("namespace", "uri")
          .SetSchema("Message")
          .AddStringProperty("body", "message body1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  // Sanity check that the document is present.
  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = document;
  ASSERT_THAT(
      icing.Get("namespace", "uri", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  // Delete "namespace", "uri".
  EXPECT_THAT(icing.Delete("namespace", "uri").status(), ProtoIsOk());

  // Get again.
  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace, uri) not found.");
  expected_get_result_proto.clear_document();
  EXPECT_THAT(
      icing.Get("namespace", "uri", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));
}

TEST_F(IcingSearchEngineDeleteTest, DeleteWithDeletePropagation) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Email")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("subject")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("sender")
                                   .SetDataTypeJoinableString(
                                       JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                       DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                                   .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("sender")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                            DELETE_PROPAGATION_TYPE_NONE)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  DocumentProto person1 =
      DocumentBuilder()
          .SetKey("namespace", "person1")
          .SetSchema("Person")
          .AddStringProperty("name", "Alice")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto person2 =
      DocumentBuilder()
          .SetKey("namespace", "person2")
          .SetSchema("Person")
          .AddStringProperty("name", "Bob")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("subject", "test")
          .AddStringProperty("sender", "namespace#person1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto email2 =
      DocumentBuilder()
          .SetKey("namespace", "email2")
          .SetSchema("Email")
          .AddStringProperty("subject", "test")
          .AddStringProperty("sender", "namespace#person2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message1 =
      DocumentBuilder()
          .SetKey("namespace", "message1")
          .SetSchema("Message")
          .AddStringProperty("body", "test")
          .AddStringProperty("sender", "namespace#person1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message2 =
      DocumentBuilder()
          .SetKey("namespace", "message2")
          .SetSchema("Message")
          .AddStringProperty("body", "test")
          .AddStringProperty("sender", "namespace#person2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_enable_delete_propagation_from(true);

  IcingSearchEngine icing(options, GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(message1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(message2).status(), ProtoIsOk());

  // Delete person1.
  DeleteResultProto delete_result = icing.Delete("namespace", "person1");
  EXPECT_THAT(delete_result.status(), ProtoIsOk());
  // Person1 and email1 should be deleted.
  EXPECT_THAT(delete_result.delete_stats().num_documents_deleted(), Eq(2));

  // Verify Get API for email and message documents.
  // Email1 should be deleted. The joinable property "sender" in schema type
  // "Email" has delete propagation type PROPAGATE_FROM and the referenced
  // document "person1" is deleted.
  GetResultProto expected_get_result_proto1;
  expected_get_result_proto1.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto1.mutable_status()->set_message(
      "Document (namespace, email1) not found.");
  EXPECT_THAT(
      icing.Get("namespace", "email1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto1));

  // Email2 should still exist. The joinable property "sender" in schema type
  // "Email" has delete propagation type PROPAGATE_FROM but the referenced
  // document "person2" is not deleted.
  GetResultProto expected_get_result_google::protobuf;
  expected_get_result_google::protobuf.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_google::protobuf.mutable_document() = email2;
  EXPECT_THAT(
      icing.Get("namespace", "email2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_google::protobuf));

  // Message1 should still exist. The joinable property "sender" in schema type
  // "Message" has delete propagation type NONE.
  GetResultProto expected_get_result_proto3;
  expected_get_result_proto3.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto3.mutable_document() = message1;
  EXPECT_THAT(icing.Get("namespace", "message1",
                        GetResultSpecProto::default_instance()),
              EqualsProto(expected_get_result_proto3));

  // Message2 should still exist. The joinable property "sender" in schema type
  // "Message" has delete propagation type NONE, and the referenced document
  // "person2" is not deleted.
  GetResultProto expected_get_result_proto4;
  expected_get_result_proto4.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto4.mutable_document() = message2;
  EXPECT_THAT(icing.Get("namespace", "message2",
                        GetResultSpecProto::default_instance()),
              EqualsProto(expected_get_result_proto4));
}

TEST_F(IcingSearchEngineDeleteTest, DeleteBySchemaType) {
  SchemaProto schema;
  // Add an email type
  auto type = schema.add_types();
  type->set_schema_type("email");
  auto property = type->add_properties();
  property->set_property_name("subject");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
  property->mutable_string_indexing_config()->set_term_match_type(
      TermMatchType::EXACT_ONLY);
  property->mutable_string_indexing_config()->set_tokenizer_type(
      StringIndexingConfig::TokenizerType::PLAIN);
  // Add an message type
  type = schema.add_types();
  type->set_schema_type("message");
  property = type->add_properties();
  property->set_property_name("body");
  property->set_data_type(PropertyConfigProto::DataType::STRING);
  property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
  property->mutable_string_indexing_config()->set_term_match_type(
      TermMatchType::EXACT_ONLY);
  property->mutable_string_indexing_config()->set_tokenizer_type(
      StringIndexingConfig::TokenizerType::PLAIN);
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

  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(7);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = document1;
  EXPECT_THAT(
      icing.Get("namespace1", "uri1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(
      icing.Get("namespace2", "uri2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  // Delete the first type. The first doc should be irretrievable. The
  // second should still be present.
  DeleteBySchemaTypeResultProto result_proto =
      icing.DeleteBySchemaType("message");
  EXPECT_THAT(result_proto.status(), ProtoIsOk());
  DeleteStatsProto exp_stats;
  exp_stats.set_delete_type(DeleteStatsProto::DeleteType::SCHEMA_TYPE);
  exp_stats.set_latency_ms(7);
  exp_stats.set_num_documents_deleted(1);
  EXPECT_THAT(result_proto.delete_stats(), EqualsProto(exp_stats));

  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace1, uri1) not found.");
  expected_get_result_proto.clear_document();
  EXPECT_THAT(
      icing.Get("namespace1", "uri1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  expected_get_result_proto.mutable_status()->clear_message();
  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(
      icing.Get("namespace2", "uri2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  // Search for "message", only document2 should show up.
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_query("message");
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineDeleteTest, DeleteSchemaTypeByQuery) {
  SchemaProto schema = SchemaBuilder()
                           .AddType(CreateMessageSchemaTypeConfig())
                           .AddType(CreateEmailSchemaTypeConfig())
                           .Build();

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
          .SetSchema("Email")
          .AddStringProperty("subject", "subject subject2")
          .AddStringProperty("body", "message body2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document1).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document2).status(), ProtoIsOk());

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = document1;
  EXPECT_THAT(
      icing.Get("namespace1", "uri1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(
      icing.Get("namespace2", "uri2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  // Delete the first type. The first doc should be irretrievable. The
  // second should still be present.
  SearchSpecProto search_spec;
  search_spec.add_schema_type_filters("Message");
  EXPECT_THAT(icing.DeleteByQuery(search_spec).status(), ProtoIsOk());

  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace1, uri1) not found.");
  expected_get_result_proto.clear_document();
  EXPECT_THAT(
      icing.Get("namespace1", "uri1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  expected_get_result_proto.mutable_status()->clear_message();
  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(
      icing.Get("namespace2", "uri2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  search_spec = SearchSpecProto::default_instance();
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineDeleteTest, DeleteByNamespace) {
  SchemaProto schema =
      SchemaBuilder().AddType(CreateMessageSchemaTypeConfig()).Build();

  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace1", "uri1")
          .SetSchema("Message")
          .AddStringProperty("body", "message body1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace1", "uri2")
          .SetSchema("Message")
          .AddStringProperty("body", "message body2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto document3 =
      DocumentBuilder()
          .SetKey("namespace3", "uri3")
          .SetSchema("Message")
          .AddStringProperty("body", "message body2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(7);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = document1;
  EXPECT_THAT(
      icing.Get("namespace1", "uri1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(
      icing.Get("namespace1", "uri2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  *expected_get_result_proto.mutable_document() = document3;
  EXPECT_THAT(
      icing.Get("namespace3", "uri3", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  // Delete namespace1. Document1 and document2 should be irretrievable.
  // Document3 should still be present.
  DeleteByNamespaceResultProto result_proto =
      icing.DeleteByNamespace("namespace1");
  EXPECT_THAT(result_proto.status(), ProtoIsOk());
  DeleteStatsProto exp_stats;
  exp_stats.set_delete_type(DeleteStatsProto::DeleteType::NAMESPACE);
  exp_stats.set_latency_ms(7);
  exp_stats.set_num_documents_deleted(2);
  EXPECT_THAT(result_proto.delete_stats(), EqualsProto(exp_stats));

  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace1, uri1) not found.");
  expected_get_result_proto.clear_document();
  EXPECT_THAT(
      icing.Get("namespace1", "uri1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace1, uri2) not found.");
  expected_get_result_proto.clear_document();
  EXPECT_THAT(
      icing.Get("namespace1", "uri2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  expected_get_result_proto.mutable_status()->clear_message();
  *expected_get_result_proto.mutable_document() = document3;
  EXPECT_THAT(
      icing.Get("namespace3", "uri3", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  // Search for "message", only document3 should show up.
  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document3;
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_query("message");
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineDeleteTest, DeleteNamespaceByQuery) {
  SchemaProto schema =
      SchemaBuilder().AddType(CreateMessageSchemaTypeConfig()).Build();

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

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document1).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document2).status(), ProtoIsOk());

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = document1;
  EXPECT_THAT(
      icing.Get("namespace1", "uri1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(
      icing.Get("namespace2", "uri2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  // Delete the first namespace. The first doc should be irretrievable. The
  // second should still be present.
  SearchSpecProto search_spec;
  search_spec.add_namespace_filters("namespace1");
  EXPECT_THAT(icing.DeleteByQuery(search_spec).status(), ProtoIsOk());

  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace1, uri1) not found.");
  expected_get_result_proto.clear_document();
  EXPECT_THAT(
      icing.Get("namespace1", "uri1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  expected_get_result_proto.mutable_status()->clear_message();
  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(
      icing.Get("namespace2", "uri2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  search_spec = SearchSpecProto::default_instance();
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineDeleteTest, DeleteByQuery) {
  SchemaProto schema =
      SchemaBuilder().AddType(CreateMessageSchemaTypeConfig()).Build();

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

  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(7);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document1).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document2).status(), ProtoIsOk());

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = document1;
  EXPECT_THAT(
      icing.Get("namespace1", "uri1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(
      icing.Get("namespace2", "uri2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  // Delete all docs containing 'body1'. The first doc should be irretrievable.
  // The second should still be present.
  SearchSpecProto search_spec;
  search_spec.set_query("body1");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  DeleteByQueryResultProto result_proto = icing.DeleteByQuery(search_spec);
  EXPECT_THAT(result_proto.status(), ProtoIsOk());
  DeleteByQueryStatsProto exp_stats;
  exp_stats.set_latency_ms(7);
  exp_stats.set_num_documents_deleted(1);
  exp_stats.set_query_length(search_spec.query().length());
  exp_stats.set_num_terms(1);
  exp_stats.set_num_namespaces_filtered(0);
  exp_stats.set_num_schema_types_filtered(0);
  exp_stats.set_parse_query_latency_ms(7);
  exp_stats.set_document_removal_latency_ms(7);
  EXPECT_THAT(result_proto.delete_by_query_stats(), EqualsProto(exp_stats));

  expected_get_result_proto.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto.mutable_status()->set_message(
      "Document (namespace1, uri1) not found.");
  expected_get_result_proto.clear_document();
  EXPECT_THAT(
      icing.Get("namespace1", "uri1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  expected_get_result_proto.mutable_status()->clear_message();
  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(
      icing.Get("namespace2", "uri2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  search_spec = SearchSpecProto::default_instance();
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineDeleteTest, DeleteByQueryReturnInfo) {
  SchemaProto schema =
      SchemaBuilder().AddType(CreateMessageSchemaTypeConfig()).Build();

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
  DocumentProto document3 =
      DocumentBuilder()
          .SetKey("namespace2", "uri3")
          .SetSchema("Message")
          .AddStringProperty("body", "message body3")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(7);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document3).status(), ProtoIsOk());

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = document1;
  EXPECT_THAT(
      icing.Get("namespace1", "uri1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(
      icing.Get("namespace2", "uri2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  *expected_get_result_proto.mutable_document() = document3;
  EXPECT_THAT(
      icing.Get("namespace2", "uri3", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  // Delete all docs to test the information is correctly grouped.
  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  DeleteByQueryResultProto result_proto =
      icing.DeleteByQuery(search_spec, true);
  EXPECT_THAT(result_proto.status(), ProtoIsOk());
  DeleteByQueryStatsProto exp_stats;
  exp_stats.set_latency_ms(7);
  exp_stats.set_num_documents_deleted(3);
  exp_stats.set_query_length(search_spec.query().length());
  exp_stats.set_num_terms(1);
  exp_stats.set_num_namespaces_filtered(0);
  exp_stats.set_num_schema_types_filtered(0);
  exp_stats.set_parse_query_latency_ms(7);
  exp_stats.set_document_removal_latency_ms(7);
  EXPECT_THAT(result_proto.delete_by_query_stats(), EqualsProto(exp_stats));

  // Check that DeleteByQuery can return information for deleted documents.
  DeleteByQueryResultProto::DocumentGroupInfo info1, info2;
  info1.set_namespace_("namespace1");
  info1.set_schema("Message");
  info1.add_uris("uri1");
  info2.set_namespace_("namespace2");
  info2.set_schema("Message");
  info2.add_uris("uri3");
  info2.add_uris("uri2");
  EXPECT_THAT(result_proto.deleted_documents(),
              UnorderedElementsAre(EqualsProto(info1), EqualsProto(info2)));

  EXPECT_THAT(
      icing.Get("namespace1", "uri1", GetResultSpecProto::default_instance())
          .status()
          .code(),
      Eq(StatusProto::NOT_FOUND));
  EXPECT_THAT(
      icing.Get("namespace2", "uri2", GetResultSpecProto::default_instance())
          .status()
          .code(),
      Eq(StatusProto::NOT_FOUND));
  EXPECT_THAT(
      icing.Get("namespace2", "uri3", GetResultSpecProto::default_instance())
          .status()
          .code(),
      Eq(StatusProto::NOT_FOUND));
}

TEST_F(IcingSearchEngineDeleteTest, DeleteByQueryNotFound) {
  SchemaProto schema =
      SchemaBuilder().AddType(CreateMessageSchemaTypeConfig()).Build();

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

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document1).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document2).status(), ProtoIsOk());

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = document1;
  EXPECT_THAT(
      icing.Get("namespace1", "uri1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(
      icing.Get("namespace2", "uri2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  // Delete all docs containing 'foo', which should be none of them. Both docs
  // should still be present.
  SearchSpecProto search_spec;
  search_spec.set_query("foo");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  EXPECT_THAT(icing.DeleteByQuery(search_spec).status(),
              ProtoStatusIs(StatusProto::NOT_FOUND));

  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  expected_get_result_proto.mutable_status()->clear_message();
  *expected_get_result_proto.mutable_document() = document1;
  EXPECT_THAT(
      icing.Get("namespace1", "uri1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  expected_get_result_proto.mutable_status()->clear_message();
  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(
      icing.Get("namespace2", "uri2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  search_spec = SearchSpecProto::default_instance();
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document2;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document1;
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineDeleteTest, DeleteByQueryWithDeletePropagation) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Email")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("subject")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("sender")
                                   .SetDataTypeJoinableString(
                                       JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                       DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                                   .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("sender")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                            DELETE_PROPAGATION_TYPE_NONE)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  DocumentProto person1 =
      DocumentBuilder()
          .SetKey("namespace", "person1")
          .SetSchema("Person")
          .AddStringProperty("name", "Alice")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto person2 =
      DocumentBuilder()
          .SetKey("namespace", "person2")
          .SetSchema("Person")
          .AddStringProperty("name", "Bob")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto person3 =
      DocumentBuilder()
          .SetKey("namespace", "person3")
          .SetSchema("Person")
          .AddStringProperty("name", "Alice in Wonderland")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto email1 =
      DocumentBuilder()
          .SetKey("namespace", "email1")
          .SetSchema("Email")
          .AddStringProperty("subject", "test")
          .AddStringProperty("sender", "namespace#person1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto email2 =
      DocumentBuilder()
          .SetKey("namespace", "email2")
          .SetSchema("Email")
          .AddStringProperty("subject", "test")
          .AddStringProperty("sender", "namespace#person2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto email3 =
      DocumentBuilder()
          .SetKey("namespace", "email3")
          .SetSchema("Email")
          .AddStringProperty("subject", "test")
          .AddStringProperty("sender", "namespace#person3")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message1 =
      DocumentBuilder()
          .SetKey("namespace", "message1")
          .SetSchema("Message")
          .AddStringProperty("body", "test")
          .AddStringProperty("sender", "namespace#person1")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message2 =
      DocumentBuilder()
          .SetKey("namespace", "message2")
          .SetSchema("Message")
          .AddStringProperty("body", "test")
          .AddStringProperty("sender", "namespace#person2")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message3 =
      DocumentBuilder()
          .SetKey("namespace", "message3")
          .SetSchema("Message")
          .AddStringProperty("body", "test")
          .AddStringProperty("sender", "namespace#person3")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_enable_delete_propagation_from(true);

  IcingSearchEngine icing(options, GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(message1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(message2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(message3).status(), ProtoIsOk());

  // Delete by query "alice".
  SearchSpecProto search_spec;
  search_spec.set_query("alice");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  DeleteByQueryResultProto result_proto = icing.DeleteByQuery(search_spec);
  EXPECT_THAT(result_proto.status(), ProtoIsOk());
  // Person1, person3, email1, and email3 should be deleted.
  EXPECT_THAT(result_proto.delete_by_query_stats().num_documents_deleted(),
              Eq(4));

  // Verify Get API for email and message documents.
  // Email1 should be deleted. The joinable property "sender" in schema type
  // "Email" has delete propagation type PROPAGATE_FROM and the referenced
  // document "person1" is deleted.
  GetResultProto expected_get_result_proto1;
  expected_get_result_proto1.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto1.mutable_status()->set_message(
      "Document (namespace, email1) not found.");
  EXPECT_THAT(
      icing.Get("namespace", "email1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto1));

  // Email2 should still exist. The joinable property "sender" in schema type
  // "Email" has delete propagation type PROPAGATE_FROM but the referenced
  // document "person2" is not deleted.
  GetResultProto expected_get_result_google::protobuf;
  expected_get_result_google::protobuf.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_google::protobuf.mutable_document() = email2;
  EXPECT_THAT(
      icing.Get("namespace", "email2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_google::protobuf));

  // Email3 should be deleted. The joinable property "sender" in schema type
  // "Email" has delete propagation type PROPAGATE_FROM and the referenced
  // document "person3" is deleted.
  GetResultProto expected_get_result_proto3;
  expected_get_result_proto3.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto3.mutable_status()->set_message(
      "Document (namespace, email3) not found.");
  EXPECT_THAT(
      icing.Get("namespace", "email3", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto3));

  // Message1 should still exist. The joinable property "sender" in schema type
  // "Message" has delete propagation type NONE.
  GetResultProto expected_get_result_proto4;
  expected_get_result_proto4.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto4.mutable_document() = message1;
  EXPECT_THAT(icing.Get("namespace", "message1",
                        GetResultSpecProto::default_instance()),
              EqualsProto(expected_get_result_proto4));

  // Message2 should still exist. The joinable property "sender" in schema type
  // "Message" has delete propagation type NONE, and the referenced document
  // "person2" is not deleted.
  GetResultProto expected_get_result_proto5;
  expected_get_result_proto5.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto5.mutable_document() = message2;
  EXPECT_THAT(icing.Get("namespace", "message2",
                        GetResultSpecProto::default_instance()),
              EqualsProto(expected_get_result_proto5));

  // Message3 should still exist. The joinable property "sender" in schema type
  // "Message" has delete propagation type NONE.
  GetResultProto expected_get_result_proto6;
  expected_get_result_proto6.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto6.mutable_document() = message3;
  EXPECT_THAT(icing.Get("namespace", "message3",
                        GetResultSpecProto::default_instance()),
              EqualsProto(expected_get_result_proto6));
}

TEST_F(IcingSearchEngineDeleteTest,
       HandleExpiredDocuments_taskSchedulerDisabled) {
  SchemaProto schema =
      SchemaBuilder().AddType(CreateMessageSchemaTypeConfig()).Build();

  DocumentProto document = DocumentBuilder()
                               .SetKey("namespace", "uri")
                               .SetSchema("Message")
                               .SetCreationTimestampMs(10)
                               .SetTtlMs(1000)  // Expired at 1010 ms.
                               .AddStringProperty("body", "message body1")
                               .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_enable_background_task_scheduler(false);
  options.set_enable_delete_propagation_from(true);
  options.set_expired_document_purge_threshold_ms(0);

  auto fake_clock = std::make_unique<FakeClock>();
  FakeClock* fake_clock_ptr = fake_clock.get();
  TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  // Sanity check that the document is present.
  GetResultProto expected_get_result_proto1;
  expected_get_result_proto1.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto1.mutable_document() = document;
  ASSERT_THAT(
      icing.Get("namespace", "uri", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto1));

  // Adjust the clock to 500 ms and call HandleExpiredDocuments. The document
  // should still be present.
  fake_clock_ptr->SetSystemTimeMilliseconds(500);
  HandleExpiredDocumentsResultProto result_proto1 =
      icing.HandleExpiredDocuments();
  EXPECT_THAT(result_proto1.status(), ProtoIsOk());
  EXPECT_THAT(result_proto1.num_expired_documents(), Eq(0));
  EXPECT_THAT(result_proto1.num_propagated_deleted_documents(), Eq(0));
  EXPECT_THAT(result_proto1.deleted_documents(), IsEmpty());
  EXPECT_THAT(
      icing.Get("namespace", "uri", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto1));

  // Adjust the clock to 1010 ms and call HandleExpiredDocuments. The document
  // should be deleted.
  fake_clock_ptr->SetSystemTimeMilliseconds(1010);
  HandleExpiredDocumentsResultProto result_google::protobuf =
      icing.HandleExpiredDocuments();
  EXPECT_THAT(result_google::protobuf.status(), ProtoIsOk());
  EXPECT_THAT(result_google::protobuf.num_expired_documents(), Eq(1));
  EXPECT_THAT(result_google::protobuf.num_propagated_deleted_documents(), Eq(0));
  EXPECT_THAT(
      result_google::protobuf.deleted_documents(),
      UnorderedElementsAre(AllOf(
          Property(
              &HandleExpiredDocumentsResultProto::DocumentGroupInfo::name_space,
              Eq("namespace")),
          Property(
              &HandleExpiredDocumentsResultProto::DocumentGroupInfo::schema,
              Eq("Message")),
          Property(&HandleExpiredDocumentsResultProto::DocumentGroupInfo::uris,
                   UnorderedElementsAre("uri")))));

  GetResultProto expected_get_result_google::protobuf;
  expected_get_result_google::protobuf.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_google::protobuf.mutable_status()->set_message(
      "Document (namespace, uri) not found.");
  EXPECT_THAT(
      icing.Get("namespace", "uri", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_google::protobuf));

  // Adjust the clock back to 500 ms and get the document again. Should get
  // NOT_FOUND error.
  // This is a hack to make sure that the document is "purged" when we handle
  // expired documents instead of just marking deleted.
  fake_clock_ptr->SetSystemTimeMilliseconds(500);
  EXPECT_THAT(
      icing.Get("namespace", "uri", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_google::protobuf));
}

TEST_F(
    IcingSearchEngineDeleteTest,
    HandleExpiredDocuments_taskSchedulerDisabled_propagateToChildrenWithDeletePropagationEnabled) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Email")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("subject")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("sender")
                                   .SetDataTypeJoinableString(
                                       JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                       DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                                   .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("sender")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                            DELETE_PROPAGATION_TYPE_NONE)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Label")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("name")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_REQUIRED))
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("target")
                                   .SetDataTypeJoinableString(
                                       JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                       DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                                   .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  DocumentProto person1 = DocumentBuilder()
                              .SetKey("namespace", "person1")
                              .SetSchema("Person")
                              .SetCreationTimestampMs(10)
                              .SetTtlMs(1000)  // Expired at 1010 ms.
                              .AddStringProperty("name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey("namespace", "person2")
                              .SetSchema("Person")
                              .SetCreationTimestampMs(10)
                              .SetTtlMs(3000)  // Expired at 3010 ms.
                              .AddStringProperty("name", "Bob")
                              .Build();
  DocumentProto email1 = DocumentBuilder()
                             .SetKey("namespace", "email1")
                             .SetSchema("Email")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(0)  // Never expire.
                             .AddStringProperty("subject", "test")
                             .AddStringProperty("sender", "namespace#person1")
                             .Build();
  DocumentProto email2 = DocumentBuilder()
                             .SetKey("namespace", "email2")
                             .SetSchema("Email")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(0)  // Never expire.
                             .AddStringProperty("subject", "test")
                             .AddStringProperty("sender", "namespace#person2")
                             .Build();
  DocumentProto message1 = DocumentBuilder()
                               .SetKey("namespace", "message1")
                               .SetSchema("Message")
                               .SetCreationTimestampMs(10)
                               .SetTtlMs(0)  // Never expire.
                               .AddStringProperty("body", "test")
                               .AddStringProperty("sender", "namespace#person1")
                               .Build();
  DocumentProto message2 = DocumentBuilder()
                               .SetKey("namespace", "message2")
                               .SetSchema("Message")
                               .SetCreationTimestampMs(10)
                               .SetTtlMs(0)  // Never expire.
                               .AddStringProperty("body", "test")
                               .AddStringProperty("sender", "namespace#person2")
                               .Build();
  DocumentProto label1 = DocumentBuilder()
                             .SetKey("namespace", "label1")
                             .SetSchema("Label")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(0)  // Never expire.
                             .AddStringProperty("name", "label1")
                             .AddStringProperty("target", "namespace#email1")
                             .Build();
  DocumentProto label2 = DocumentBuilder()
                             .SetKey("namespace", "label2")
                             .SetSchema("Label")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(0)  // Never expire.
                             .AddStringProperty("name", "label2")
                             .AddStringProperty("target", "namespace#email2")
                             .Build();
  DocumentProto label3 = DocumentBuilder()
                             .SetKey("namespace", "label3")
                             .SetSchema("Label")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(0)  // Never expire.
                             .AddStringProperty("name", "label3")
                             .AddStringProperty("target", "namespace#message1")
                             .Build();
  DocumentProto label4 = DocumentBuilder()
                             .SetKey("namespace", "label4")
                             .SetSchema("Label")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(0)  // Never expire.
                             .AddStringProperty("name", "label4")
                             .AddStringProperty("target", "namespace#message2")
                             .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_enable_background_task_scheduler(false);
  options.set_enable_delete_propagation_from(true);
  options.set_expired_document_purge_threshold_ms(0);

  auto fake_clock = std::make_unique<FakeClock>();
  FakeClock* fake_clock_ptr = fake_clock.get();
  TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(message1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(message2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(label1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(label2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(label3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(label4).status(), ProtoIsOk());

  // Adjust the clock to 1010 ms and call HandleExpiredDocuments. person1 should
  // be deleted and delete propagation should be triggered for email1 (child)
  // and label1 (grandchild).
  fake_clock_ptr->SetSystemTimeMilliseconds(1010);
  HandleExpiredDocumentsResultProto result_proto =
      icing.HandleExpiredDocuments();
  EXPECT_THAT(result_proto.status(), ProtoIsOk());
  EXPECT_THAT(result_proto.num_expired_documents(), Eq(1));
  EXPECT_THAT(result_proto.num_propagated_deleted_documents(), Eq(2));
  EXPECT_THAT(
      result_proto.deleted_documents(),
      UnorderedElementsAre(
          AllOf(
              Property(&HandleExpiredDocumentsResultProto::DocumentGroupInfo::
                           name_space,
                       Eq("namespace")),
              Property(
                  &HandleExpiredDocumentsResultProto::DocumentGroupInfo::schema,
                  Eq("Person")),
              Property(
                  &HandleExpiredDocumentsResultProto::DocumentGroupInfo::uris,
                  UnorderedElementsAre("person1"))),
          AllOf(
              Property(&HandleExpiredDocumentsResultProto::DocumentGroupInfo::
                           name_space,
                       Eq("namespace")),
              Property(
                  &HandleExpiredDocumentsResultProto::DocumentGroupInfo::schema,
                  Eq("Email")),
              Property(
                  &HandleExpiredDocumentsResultProto::DocumentGroupInfo::uris,
                  UnorderedElementsAre("email1"))),
          AllOf(
              Property(&HandleExpiredDocumentsResultProto::DocumentGroupInfo::
                           name_space,
                       Eq("namespace")),
              Property(
                  &HandleExpiredDocumentsResultProto::DocumentGroupInfo::schema,
                  Eq("Label")),
              Property(
                  &HandleExpiredDocumentsResultProto::DocumentGroupInfo::uris,
                  UnorderedElementsAre("label1")))));

  // Adjust the clock back to 500 ms and verify Get API for email, message and
  // label documents.
  // This is a hack to make sure that the document is "purged" when we handle
  // expired documents instead of just marking deleted.
  fake_clock_ptr->SetSystemTimeMilliseconds(500);

  // Email1 should be deleted. The joinable property "sender" in schema type
  // "Email" has delete propagation type PROPAGATE_FROM and the referenced
  // document "person1" is deleted.
  GetResultProto expected_get_result_proto1;
  expected_get_result_proto1.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto1.mutable_status()->set_message(
      "Document (namespace, email1) not found.");
  EXPECT_THAT(
      icing.Get("namespace", "email1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto1));

  // Email2 should still exist. The joinable property "sender" in schema type
  // "Email" has delete propagation type PROPAGATE_FROM but the referenced
  // document "person2" is not deleted.
  GetResultProto expected_get_result_google::protobuf;
  expected_get_result_google::protobuf.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_google::protobuf.mutable_document() = email2;
  EXPECT_THAT(
      icing.Get("namespace", "email2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_google::protobuf));

  // Message1 should still exist. The joinable property "sender" in schema type
  // "Message" has delete propagation type NONE.
  GetResultProto expected_get_result_proto3;
  expected_get_result_proto3.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto3.mutable_document() = message1;
  EXPECT_THAT(icing.Get("namespace", "message1",
                        GetResultSpecProto::default_instance()),
              EqualsProto(expected_get_result_proto3));

  // Message2 should still exist. The joinable property "sender" in schema type
  // "Message" has delete propagation type NONE, and the referenced document
  // "person2" is not deleted.
  GetResultProto expected_get_result_proto4;
  expected_get_result_proto4.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto4.mutable_document() = message2;
  EXPECT_THAT(icing.Get("namespace", "message2",
                        GetResultSpecProto::default_instance()),
              EqualsProto(expected_get_result_proto4));

  // Label1 should be deleted (propagated from email1).
  GetResultProto expected_get_result_proto5;
  expected_get_result_proto5.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto5.mutable_status()->set_message(
      "Document (namespace, label1) not found.");
  EXPECT_THAT(
      icing.Get("namespace", "label1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto5));

  // Label2 should still exist.
  GetResultProto expected_get_result_proto6;
  expected_get_result_proto6.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto6.mutable_document() = label2;
  EXPECT_THAT(
      icing.Get("namespace", "label2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto6));

  // Label3 should still exist.
  GetResultProto expected_get_result_proto7;
  expected_get_result_proto7.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto7.mutable_document() = label3;
  EXPECT_THAT(
      icing.Get("namespace", "label3", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto7));

  // Label4 should still exist.
  GetResultProto expected_get_result_proto8;
  expected_get_result_proto8.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto8.mutable_document() = label4;
  EXPECT_THAT(
      icing.Get("namespace", "label4", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto8));
}

TEST_F(
    IcingSearchEngineDeleteTest,
    HandleExpiredDocuments_taskSchedulerDisabled_shouldPurgeDocumentsThatExpireWithinThreshold) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Email")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("subject")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("sender")
                                   .SetDataTypeJoinableString(
                                       JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                       DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                                   .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  DocumentProto person1 = DocumentBuilder()
                              .SetKey("namespace", "person1")
                              .SetSchema("Person")
                              .SetCreationTimestampMs(10)
                              .SetTtlMs(1000)  // Expired at 1010 ms.
                              .AddStringProperty("name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey("namespace", "person2")
                              .SetSchema("Person")
                              .SetCreationTimestampMs(10)
                              .SetTtlMs(1101)  // Expired at 1111 ms.
                              .AddStringProperty("name", "Bob")
                              .Build();
  DocumentProto person3 = DocumentBuilder()
                              .SetKey("namespace", "person3")
                              .SetSchema("Person")
                              .SetCreationTimestampMs(10)
                              .SetTtlMs(1100)  // Expired at 1110 ms.
                              .AddStringProperty("name", "Bob")
                              .Build();
  DocumentProto email1 = DocumentBuilder()
                             .SetKey("namespace", "email1")
                             .SetSchema("Email")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(0)  // Never expire.
                             .AddStringProperty("subject", "test")
                             .AddStringProperty("sender", "namespace#person1")
                             .Build();
  DocumentProto email2 = DocumentBuilder()
                             .SetKey("namespace", "email2")
                             .SetSchema("Email")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(0)  // Never expire.
                             .AddStringProperty("subject", "test")
                             .AddStringProperty("sender", "namespace#person2")
                             .Build();
  DocumentProto email3 = DocumentBuilder()
                             .SetKey("namespace", "email3")
                             .SetSchema("Email")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(0)  // Never expire.
                             .AddStringProperty("subject", "test")
                             .AddStringProperty("sender", "namespace#person3")
                             .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_enable_background_task_scheduler(false);
  options.set_enable_delete_propagation_from(true);
  options.set_expired_document_purge_threshold_ms(100);

  auto fake_clock = std::make_unique<FakeClock>();
  FakeClock* fake_clock_ptr = fake_clock.get();
  TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person3).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email3).status(), ProtoIsOk());

  // Adjust the clock to 1010 ms and call HandleExpiredDocuments. person1 and
  // person3 should be deleted, and delete propagation should be triggered for
  // email1 and email3.
  fake_clock_ptr->SetSystemTimeMilliseconds(1010);
  HandleExpiredDocumentsResultProto result_proto =
      icing.HandleExpiredDocuments();
  EXPECT_THAT(result_proto.status(), ProtoIsOk());
  EXPECT_THAT(result_proto.num_expired_documents(), Eq(2));
  EXPECT_THAT(result_proto.num_propagated_deleted_documents(), Eq(2));
  EXPECT_THAT(
      result_proto.deleted_documents(),
      UnorderedElementsAre(
          AllOf(
              Property(&HandleExpiredDocumentsResultProto::DocumentGroupInfo::
                           name_space,
                       Eq("namespace")),
              Property(
                  &HandleExpiredDocumentsResultProto::DocumentGroupInfo::schema,
                  Eq("Person")),
              Property(
                  &HandleExpiredDocumentsResultProto::DocumentGroupInfo::uris,
                  UnorderedElementsAre("person1", "person3"))),
          AllOf(
              Property(&HandleExpiredDocumentsResultProto::DocumentGroupInfo::
                           name_space,
                       Eq("namespace")),
              Property(
                  &HandleExpiredDocumentsResultProto::DocumentGroupInfo::schema,
                  Eq("Email")),
              Property(
                  &HandleExpiredDocumentsResultProto::DocumentGroupInfo::uris,
                  UnorderedElementsAre("email1", "email3")))));
}

TEST_F(
    IcingSearchEngineDeleteTest,
    HandleExpiredDocuments_taskSchedulerEnabled_shouldScheduleNextPurgingExpirationTask) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_OPTIONAL)))
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Email")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("subject")
                          .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("sender")
                                   .SetDataTypeJoinableString(
                                       JOINABLE_VALUE_TYPE_QUALIFIED_ID,
                                       DELETE_PROPAGATION_TYPE_PROPAGATE_FROM)
                                   .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  DocumentProto person1 = DocumentBuilder()
                              .SetKey("namespace", "person1")
                              .SetSchema("Person")
                              .SetCreationTimestampMs(10)
                              .SetTtlMs(5000)  // Expired at 5010 ms.
                              .AddStringProperty("name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey("namespace", "person2")
                              .SetSchema("Person")
                              .SetCreationTimestampMs(10)
                              .SetTtlMs(5400)  // Expired at 5410 ms.
                              .AddStringProperty("name", "Bob")
                              .Build();
  DocumentProto email1 = DocumentBuilder()
                             .SetKey("namespace", "email1")
                             .SetSchema("Email")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(0)  // Never expire.
                             .AddStringProperty("subject", "test")
                             .AddStringProperty("sender", "namespace#person1")
                             .Build();
  DocumentProto email2 = DocumentBuilder()
                             .SetKey("namespace", "email2")
                             .SetSchema("Email")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(0)  // Never expire.
                             .AddStringProperty("subject", "test")
                             .AddStringProperty("sender", "namespace#person2")
                             .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_enable_background_task_scheduler(true);
  options.set_enable_delete_propagation_from(true);
  options.set_expired_document_purge_threshold_ms(100);

  {
    // Initialize Icing and put all documents. Destruct Icing.
    TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(person1).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(person2).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(email1).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(email2).status(), ProtoIsOk());
  }

  // Initialize Icing again with a fake clock and t = 3500 ms. Initialization
  // should schedule the purging expiration task at t = 5010 ms.
  auto fake_clock = std::make_unique<FakeClock>();
  FakeClock* fake_clock_ptr = fake_clock.get();
  fake_clock->SetSystemTimeMilliseconds(3500);
  TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  // Sanity check that person1, person2, email1 and email2 are present.
  GetResultProto expected_get_result_proto1;
  expected_get_result_proto1.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto1.mutable_document() = person1;
  EXPECT_THAT(
      icing.Get("namespace", "person1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto1));

  GetResultProto expected_get_result_google::protobuf;
  expected_get_result_google::protobuf.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_google::protobuf.mutable_document() = person2;
  EXPECT_THAT(
      icing.Get("namespace", "person2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_google::protobuf));

  GetResultProto expected_get_result_proto3;
  expected_get_result_proto3.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto3.mutable_document() = email1;
  EXPECT_THAT(
      icing.Get("namespace", "email1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto3));

  GetResultProto expected_get_result_proto4;
  expected_get_result_proto4.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto4.mutable_document() = email2;
  EXPECT_THAT(
      icing.Get("namespace", "email2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto4));

  // 1. Adjust the clock from 3500 to 5010 ms and sleep for 1550 ms. person1 and
  //    email1 should be purged by the scheduled task, but person2 and email2
  //    should still be alive.
  fake_clock_ptr->SetSystemTimeMilliseconds(5010);
  std::this_thread::sleep_for(std::chrono::milliseconds(1550));

  GetResultProto expected_get_result_proto5;
  expected_get_result_proto5.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto5.mutable_status()->set_message(
      "Document (namespace, person1) not found.");
  EXPECT_THAT(
      icing.Get("namespace", "person1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto5));

  GetResultProto expected_get_result_proto6;
  expected_get_result_proto6.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto6.mutable_status()->set_message(
      "Document (namespace, email1) not found.");
  EXPECT_THAT(
      icing.Get("namespace", "email1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto6));

  EXPECT_THAT(
      icing.Get("namespace", "person2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_google::protobuf));
  EXPECT_THAT(
      icing.Get("namespace", "email2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto4));

  // 2. Adjust the clock from 5010 to 5410 ms and sleep for 400 ms. person2 and
  //    email2 should be purged by the scheduled task.
  fake_clock_ptr->SetSystemTimeMilliseconds(5410);
  std::this_thread::sleep_for(std::chrono::milliseconds(400));

  GetResultProto expected_get_result_proto7;
  expected_get_result_proto7.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto7.mutable_status()->set_message(
      "Document (namespace, person2) not found.");
  EXPECT_THAT(
      icing.Get("namespace", "person2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto7));

  GetResultProto expected_get_result_proto8;
  expected_get_result_proto8.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto8.mutable_status()->set_message(
      "Document (namespace, email2) not found.");
  EXPECT_THAT(
      icing.Get("namespace", "email2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto8));
}

}  // namespace
}  // namespace lib
}  // namespace icing
