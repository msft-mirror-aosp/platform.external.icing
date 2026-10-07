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
#include "icing/icing-search-engine.h"
#include "icing/jni/jni-cache.h"
#include "icing/legacy/index/icing-mock-filesystem.h"
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
#include "icing/store/document-log-creator.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/jni-test-helpers.h"
#include "icing/testing/random-string.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"
#include "icing/util/icu-data-file-helper.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::Eq;
using ::testing::Ge;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Le;
using ::testing::SizeIs;

constexpr std::string_view kIpsumText =
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla convallis "
    "scelerisque orci quis hendrerit. Sed augue turpis, sodales eu gravida "
    "nec, scelerisque nec leo. Maecenas accumsan interdum commodo. Aliquam "
    "mattis sapien est, sit amet interdum risus dapibus sed. Maecenas leo "
    "erat, fringilla in nisl a, venenatis gravida metus. Phasellus venenatis, "
    "orci in aliquet mattis, lectus sapien volutpat arcu, sed hendrerit ligula "
    "arcu nec mauris. Integer dolor mi, rhoncus eget gravida et, pulvinar et "
    "nunc. Aliquam ac sollicitudin nisi. Vivamus sit amet urna vestibulum, "
    "tincidunt eros sed, efficitur nisl. Fusce non neque accumsan, sagittis "
    "nisi eget, sagittis turpis. Ut pulvinar nibh eu purus feugiat faucibus. "
    "Donec tellus nulla, tincidunt vel lacus id, bibendum fermentum turpis. "
    "Nullam ultrices sed nibh vitae aliquet. Ut risus neque, consectetur "
    "vehicula posuere vitae, convallis eu lorem. Donec semper augue eu nibh "
    "placerat semper.";

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

// This test is meant to cover all tests relating to IcingSearchEngine::Put.
class IcingSearchEnginePutTest : public testing::Test {
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

constexpr int kMaxSupportedDocumentSize = (1u << 24) - 1;

// Non-zero value so we don't override it to be the current time
constexpr int64_t kDefaultCreationTimestampMs = 1575492852000;

std::string GetIndexDir() { return GetTestBaseDir() + "/index_dir"; }

IcingSearchEngineOptions GetDefaultIcingOptions() {
  IcingSearchEngineOptions icing_options;
  icing_options.set_base_dir(GetTestBaseDir());
  icing_options.set_enable_repeated_field_joins(true);
  icing_options.set_enable_delete_propagation_from(true);
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
  return SchemaBuilder()
      .AddType(SchemaTypeConfigBuilder().SetType("Message").AddProperty(
          PropertyConfigBuilder()
              .SetName("body")
              .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
              .SetCardinality(CARDINALITY_REQUIRED)))
      .Build();
}

ScoringSpecProto GetDefaultScoringSpec() {
  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  return scoring_spec;
}

TEST_F(IcingSearchEnginePutTest, MaxTokenLenReturnsOkAndTruncatesTokens) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  // A length of 1 is allowed - even though it would be strange to want
  // this.
  options.set_max_token_length(1);
  IcingSearchEngine icing(options, GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  DocumentProto document = CreateMessageDocument("namespace", "uri");
  EXPECT_THAT(icing.Put(document).status(), ProtoIsOk());

  // "message" should have been truncated to "m"
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::PREFIX);
  // The indexed tokens were  truncated to length of 1, so "m" will match
  search_spec.set_query("m");

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      document;

  SearchResultProto actual_results =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(actual_results, EqualsSearchResultIgnoreStatsAndScores(
                                  expected_search_result_proto));

  // The query token is also truncated to length of 1, so "me"->"m" matches "m"
  search_spec.set_query("me");
  actual_results = icing.Search(search_spec, GetDefaultScoringSpec(),
                                ResultSpecProto::default_instance());
  EXPECT_THAT(actual_results, EqualsSearchResultIgnoreStatsAndScores(
                                  expected_search_result_proto));

  // The query token is still truncated to length of 1, so "massage"->"m"
  // matches "m"
  search_spec.set_query("massage");
  actual_results = icing.Search(search_spec, GetDefaultScoringSpec(),
                                ResultSpecProto::default_instance());
  EXPECT_THAT(actual_results, EqualsSearchResultIgnoreStatsAndScores(
                                  expected_search_result_proto));
}

TEST_F(IcingSearchEnginePutTest,
       MaxIntMaxTokenLenReturnsOkTooLargeTokenReturnsResourceExhausted) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  // Set token length to max. This is allowed (it just means never to
  // truncate tokens). However, this does mean that tokens that exceed the
  // size of the lexicon will cause indexing to fail.
  options.set_max_token_length(std::numeric_limits<int32_t>::max());
  IcingSearchEngine icing(options, GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

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
  EXPECT_THAT(icing.Put(document).status(),
              ProtoStatusIs(StatusProto::OUT_OF_SPACE));

  SearchSpecProto search_spec;
  search_spec.set_query("p");
  search_spec.set_term_match_type(TermMatchType::PREFIX);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  SearchResultProto actual_results =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(actual_results, EqualsSearchResultIgnoreStatsAndScores(
                                  expected_search_result_proto));
}

TEST_F(IcingSearchEnginePutTest, PutWithoutSchemaFailedPrecondition) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  DocumentProto document = CreateMessageDocument("namespace", "uri");
  PutResultProto put_result_proto = icing.Put(document);
  EXPECT_THAT(put_result_proto.status(),
              ProtoStatusIs(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(put_result_proto.status().message(), HasSubstr("Schema not set"));
}

TEST_F(IcingSearchEnginePutTest, IndexingDocMergeFailureResets) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("icing", "fake_type/0")
                               .SetSchema("Message")
                               .AddStringProperty("body", kIpsumText)
                               .Build();
  // 1. Create an index with a LiteIndex that will only allow one document
  // before needing a merge.
  {
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_index_merge_size(document.ByteSizeLong());
    IcingSearchEngine icing(options, GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

    // Add two documents. These should get merged into the main index.
    EXPECT_THAT(icing.Put(document).status(), ProtoIsOk());
    document = DocumentBuilder(document).SetUri("fake_type/1").Build();
    EXPECT_THAT(icing.Put(document).status(), ProtoIsOk());
    // Add one document. This one should get remain in the lite index.
    document = DocumentBuilder(document).SetUri("fake_type/2").Build();
    EXPECT_THAT(icing.Put(document).status(), ProtoIsOk());
  }

  // 2. Delete the index file to trigger RestoreIndexIfNeeded.
  std::string idx_subdir = GetIndexDir() + "/idx";
  filesystem()->DeleteDirectoryRecursively(idx_subdir.c_str());

  // 3. Setup a mock filesystem to fail to grow the main index once.
  bool has_failed_already = false;
  auto open_write_lambda = [this, &has_failed_already](const char* filename) {
    std::string main_lexicon_suffix = "/main-lexicon.prop.2";
    std::string filename_string(filename);
    if (!has_failed_already &&
        filename_string.length() >= main_lexicon_suffix.length() &&
        filename_string.substr(
            filename_string.length() - main_lexicon_suffix.length(),
            main_lexicon_suffix.length()) == main_lexicon_suffix) {
      has_failed_already = true;
      return -1;
    }
    return this->filesystem()->OpenForWrite(filename);
  };
  auto mock_icing_filesystem = std::make_unique<IcingMockFilesystem>();
  ON_CALL(*mock_icing_filesystem, OpenForWrite)
      .WillByDefault(open_write_lambda);

  // 4. Create the index again. This should trigger index restoration.
  {
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_index_merge_size(document.ByteSizeLong());
    TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                                std::move(mock_icing_filesystem),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(),
                ProtoStatusIs(StatusProto::WARNING_DATA_LOSS));

    SearchSpecProto search_spec;
    search_spec.set_query("consectetur");
    search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
    SearchResultProto results =
        icing.Search(search_spec, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(results.status(), ProtoIsOk());
    EXPECT_THAT(results.next_page_token(), Eq(0));
    // Only the last document that was added should still be retrievable.
    ASSERT_THAT(results.results(), SizeIs(1));
    EXPECT_THAT(results.results(0).document().uri(), Eq("fake_type/2"));
  }
}

TEST_F(IcingSearchEnginePutTest, PutDocumentUriReturnedInResult) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("icing", "fake_type/0")
                               .SetSchema("Message")
                               .AddStringProperty("body", "message body")
                               .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_index_merge_size(document.ByteSizeLong());
  IcingSearchEngine icing(options, GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  PutResultProto put_result_proto = icing.Put(document);
  EXPECT_THAT(put_result_proto.status(), ProtoIsOk());
  EXPECT_THAT(put_result_proto.uri(), document.uri());
  EXPECT_FALSE(put_result_proto.was_replacement());
}

TEST_F(IcingSearchEnginePutTest, PutDocumentReplacementSucceeds) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("icing", "fake_type/0")
                               .SetSchema("Message")
                               .AddStringProperty("body", "message body")
                               .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_index_merge_size(document.ByteSizeLong());
  IcingSearchEngine icing(options, GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  PutResultProto put_result_proto = icing.Put(document);
  EXPECT_THAT(put_result_proto.status(), ProtoIsOk());
  EXPECT_FALSE(put_result_proto.was_replacement());

  // Putting the document again should succeed.
  put_result_proto = icing.Put(document);
  EXPECT_THAT(put_result_proto.status(), ProtoIsOk());
  EXPECT_TRUE(put_result_proto.was_replacement());
}

TEST_F(IcingSearchEnginePutTest, PutDocumentShouldLogFunctionLatency) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("icing", "fake_type/0")
                               .SetSchema("Message")
                               .AddStringProperty("body", "message body")
                               .Build();

  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(10);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  PutResultProto put_result_proto = icing.Put(document);
  EXPECT_THAT(put_result_proto.status(), ProtoIsOk());
  EXPECT_FALSE(put_result_proto.was_replacement());
  EXPECT_THAT(put_result_proto.put_document_stats().latency_ms(), Eq(10));
}

TEST_F(IcingSearchEnginePutTest, PutDocumentShouldLogDocumentStoreStats) {
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "fake_type/0")
          .SetSchema("Message")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .AddStringProperty("body", "message body")
          .Build();

  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(10);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  PutResultProto put_result_proto = icing.Put(document);
  EXPECT_THAT(put_result_proto.status(), ProtoIsOk());
  EXPECT_FALSE(put_result_proto.was_replacement());
  EXPECT_THAT(put_result_proto.put_document_stats().document_store_latency_ms(),
              Eq(10));
  size_t document_size = put_result_proto.put_document_stats().document_size();
  EXPECT_THAT(document_size, Ge(document.ByteSizeLong()));
  EXPECT_THAT(document_size, Le(document.ByteSizeLong() +
                                sizeof(DocumentProto::InternalFields)));
}

TEST_F(IcingSearchEnginePutTest, PutDocumentShouldLogIndexingStats) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("icing", "fake_type/0")
                               .SetSchema("Message")
                               .AddStringProperty("body", "message body")
                               .Build();

  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(10);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  PutResultProto put_result_proto = icing.Put(document);
  EXPECT_THAT(put_result_proto.status(), ProtoIsOk());
  EXPECT_FALSE(put_result_proto.was_replacement());
  EXPECT_THAT(put_result_proto.put_document_stats().index_latency_ms(), Eq(10));
  // No merge should happen.
  EXPECT_THAT(put_result_proto.put_document_stats().index_merge_latency_ms(),
              Eq(0));
  // The input document has 2 tokens.
  EXPECT_THAT(put_result_proto.put_document_stats()
                  .tokenization_stats()
                  .num_tokens_indexed(),
              Eq(2));
}

TEST_F(IcingSearchEnginePutTest, PutDocumentShouldLogIndexMergeLatency) {
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("icing", "fake_type/1")
                                .SetSchema("Message")
                                .AddStringProperty("body", kIpsumText)
                                .Build();
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("icing", "fake_type/2")
                                .SetSchema("Message")
                                .AddStringProperty("body", kIpsumText)
                                .Build();

  // Create an icing instance with index_merge_size = document1's size.
  IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();
  icing_options.set_index_merge_size(document1.ByteSizeLong());

  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(10);
  TestIcingSearchEngine icing(icing_options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(document1).status(), ProtoIsOk());

  // Putting document2 should trigger an index merge.
  PutResultProto put_result_proto = icing.Put(document2);
  EXPECT_FALSE(put_result_proto.was_replacement());
  EXPECT_THAT(put_result_proto.status(), ProtoIsOk());
  EXPECT_THAT(put_result_proto.put_document_stats().index_merge_latency_ms(),
              Eq(10));
}

TEST_F(IcingSearchEnginePutTest, PutDocumentIndexFailureDeletion) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  // Testing has shown that adding ~600,000 terms generated this way will
  // fill up the hit buffer.
  std::vector<std::string> terms = GenerateUniqueTerms(600000);
  std::string content = absl_ports::StrJoin(terms, " ");
  DocumentProto document = DocumentBuilder()
                               .SetKey("namespace", "uri1")
                               .SetSchema("Message")
                               .AddStringProperty("body", "foo " + content)
                               .Build();
  // We failed to add the document to the index fully. This means that we should
  // reject the document from Icing entirely.
  ASSERT_THAT(icing.Put(document).status(),
              ProtoStatusIs(StatusProto::OUT_OF_SPACE));

  // Make sure that the document isn't searchable.
  SearchSpecProto search_spec;
  search_spec.set_query("foo");
  search_spec.set_term_match_type(TERM_MATCH_PREFIX);

  SearchResultProto search_results =
      icing.Search(search_spec, ScoringSpecProto::default_instance(),
                   ResultSpecProto::default_instance());
  ASSERT_THAT(search_results.status(), ProtoIsOk());
  ASSERT_THAT(search_results.results(), IsEmpty());

  // Make sure that the document isn't retrievable.
  GetResultProto get_result =
      icing.Get("namespace", "uri1", GetResultSpecProto::default_instance());
  ASSERT_THAT(get_result.status(), ProtoStatusIs(StatusProto::NOT_FOUND));
}

TEST_F(IcingSearchEnginePutTest, PutAndGetDocumentWithBlobHandle) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(1000);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(
      icing
          .SetSchema(
              SchemaBuilder()
                  .AddType(SchemaTypeConfigBuilder()
                               .SetType("SchemaType")
                               .AddProperty(
                                   PropertyConfigBuilder()
                                       .SetName("blobHandle")
                                       .SetDataType(TYPE_BLOB_HANDLE)
                                       .SetCardinality(CARDINALITY_REQUIRED)))
                  .Build())
          .status(),
      ProtoIsOk());

  PropertyProto::BlobHandleProto blob_handle;
  blob_handle.set_digest(std::string(32, ' '));
  blob_handle.set_namespace_("namespace");

  DocumentProto document =
      DocumentBuilder()
          .SetKey("namespace", "uri")
          .SetSchema("SchemaType")
          .AddBlobHandleProperty("blobHandle", blob_handle)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  GetResultProto get_result =
      icing.Get("namespace", "uri", GetResultSpecProto::default_instance());
  EXPECT_THAT(get_result.status(), ProtoIsOk());
  EXPECT_THAT(get_result.document(), EqualsProto(document));
}

TEST_F(IcingSearchEnginePutTest, PutDocumentWithInvalidBlobHandle) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(1000);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(
      icing
          .SetSchema(
              SchemaBuilder()
                  .AddType(SchemaTypeConfigBuilder()
                               .SetType("SchemaType")
                               .AddProperty(
                                   PropertyConfigBuilder()
                                       .SetName("blobHandle")
                                       .SetDataType(TYPE_BLOB_HANDLE)
                                       .SetCardinality(CARDINALITY_REQUIRED)))
                  .Build())
          .status(),
      ProtoIsOk());

  PropertyProto::BlobHandleProto blob_handle;
  blob_handle.set_digest("invalid digest");
  blob_handle.set_namespace_("namespace");

  DocumentProto document =
      DocumentBuilder()
          .SetKey("namespace", "uri")
          .SetSchema("SchemaType")
          .AddBlobHandleProperty("blobHandle", blob_handle)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(icing.Put(document).status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEnginePutTest, DocumentCompressionThreshold) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Message").AddProperty(
              PropertyConfigBuilder()
                  .SetName("body")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  // Create document1 with size of 1024 bytes.
  int64_t document1_size = 1024;
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetSchema("Message")
          .AddStringProperty("body", std::string(979, 'a'))
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(document1.ByteSizeLong(), Eq(document1_size));

  // Create document2 with size of 900 bytes.
  int64_t document2_size = 900;
  DocumentProto document2 =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Message")
          .AddStringProperty("body", std::string(855, 'a'))
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  ASSERT_THAT(document2.ByteSizeLong(), Eq(document2_size));

  // Create icing instance with compression threshold of 1000 bytes.
  IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();
  icing_options.set_compression_level(3);

  int64_t document_log_size_full_compression;
  int64_t document_log_size_part_compression;
  int64_t document_log_size_no_compression;
  const std::string document_log_path =
      icing_options.base_dir() + "/document_dir/" +
      DocumentLogCreator::GetDocumentLogFilename();

  // Set the compression threshold to 1000 bytes to only compress document1.
  {
    icing_options.set_compression_threshold_bytes(1000);
    IcingSearchEngine icing(icing_options, GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
    ASSERT_THAT(icing.PersistToDisk(PersistType::FULL).status(), ProtoIsOk());

    document_log_size_part_compression =
        filesystem()->GetFileSize(document_log_path.c_str());

    // Calling Optimize() will not change the size of the document log since
    // the compression setting is the same.
    ASSERT_THAT(icing.Optimize().status(), ProtoIsOk());
    ASSERT_EQ(filesystem()->GetFileSize(document_log_path.c_str()),
              document_log_size_part_compression);
  }

  // Set the compression threshold to 900 bytes to compress both documents.
  {
    icing_options.set_compression_threshold_bytes(900);
    IcingSearchEngine icing(icing_options, GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    // Call Optimize() to refresh the document log with the new compression
    // threshold.
    ASSERT_THAT(icing.Optimize().status(), ProtoIsOk());
    document_log_size_full_compression =
        filesystem()->GetFileSize(document_log_path.c_str());

    // Calling Optimize() again will not change the size of the document log
    // since the compression setting is the same.
    ASSERT_THAT(icing.Optimize().status(), ProtoIsOk());
    ASSERT_EQ(filesystem()->GetFileSize(document_log_path.c_str()),
              document_log_size_full_compression);
  }

  // Set the compression threshold to 10000 bytes will turn off compression.
  {
    icing_options.set_compression_threshold_bytes(10000);
    IcingSearchEngine icing(icing_options, GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    // Call Optimize() to refresh the document log with the new compression
    // threshold.
    ASSERT_THAT(icing.Optimize().status(), ProtoIsOk());
    document_log_size_no_compression =
        filesystem()->GetFileSize(document_log_path.c_str());

    // Calling Optimize() again will not change the size of the document log
    // since the compression setting is the same.
    ASSERT_THAT(icing.Optimize().status(), ProtoIsOk());
    ASSERT_EQ(filesystem()->GetFileSize(document_log_path.c_str()),
              document_log_size_no_compression);
  }

  // Check that no_compression > part_compression > full_compression
  ASSERT_GT(document_log_size_part_compression,
            document_log_size_full_compression);
  ASSERT_GT(document_log_size_no_compression,
            document_log_size_part_compression);
}

TEST_F(
    IcingSearchEnginePutTest,
    PutDocument_taskSchedulerDisabled_shouldNotReschedulePurgingExpirationTask) {
  // This test verifies that when enable_delete_propagation_from is true but
  // enable_background_task_scheduler is false, then:
  // - No background tasks are scheduled.
  // - Since task_scheduler_ is null, APIs attempt to (re)schedule a task should
  //   check the nullness of task_scheduler_ before using it.
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

  DocumentProto person = DocumentBuilder()
                             .SetKey("namespace", "person")
                             .SetSchema("Person")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(1000)  // Expire at 1010 ms.
                             .AddStringProperty("name", "Alice")
                             .Build();
  DocumentProto email = DocumentBuilder()
                            .SetKey("namespace", "email")
                            .SetSchema("Email")
                            .SetCreationTimestampMs(10)
                            .SetTtlMs(0)  // Never expire.
                            .AddStringProperty("subject", "test")
                            .AddStringProperty("sender", "namespace#person")
                            .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_enable_background_task_scheduler(false);
  options.set_enable_delete_propagation_from(true);
  options.set_expired_document_purge_threshold_ms(0);

  // Initialize Icing with a fake clock and t = 10 ms.
  auto fake_clock = std::make_unique<FakeClock>();
  FakeClock* fake_clock_ptr = fake_clock.get();
  fake_clock->SetSystemTimeMilliseconds(10);
  TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  // Put person and email. Since enable_background_task_scheduler is false,
  // there should be no background tasks scheduled.
  ASSERT_THAT(icing.Put(person).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email).status(), ProtoIsOk());

  // Sanity check that person and email are present.
  GetResultProto expected_get_result_proto1;
  expected_get_result_proto1.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto1.mutable_document() = person;
  EXPECT_THAT(
      icing.Get("namespace", "person", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto1));

  GetResultProto expected_get_result_google::protobuf;
  expected_get_result_google::protobuf.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_google::protobuf.mutable_document() = email;
  EXPECT_THAT(
      icing.Get("namespace", "email", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_google::protobuf));

  // Adjust the clock to 1010 ms and sleep for 1010 ms. Email document should
  // still be present since no background tasks were and therefore, expiration
  // propagation and purging did not run for the child document.
  fake_clock_ptr->SetSystemTimeMilliseconds(1010);
  std::this_thread::sleep_for(std::chrono::milliseconds(1010));

  GetResultProto expected_get_result_proto4;
  expected_get_result_proto4.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto4.mutable_document() = email;
  EXPECT_THAT(
      icing.Get("namespace", "email", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto4));
}

TEST_F(IcingSearchEnginePutTest,
       PutDocument_shouldReschedulePurgingExpirationTaskForNewExpTs) {
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

  DocumentProto person = DocumentBuilder()
                             .SetKey("namespace", "person")
                             .SetSchema("Person")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(1000)  // Expire at 1010 ms.
                             .AddStringProperty("name", "Alice")
                             .Build();
  DocumentProto email = DocumentBuilder()
                            .SetKey("namespace", "email")
                            .SetSchema("Email")
                            .SetCreationTimestampMs(10)
                            .SetTtlMs(0)  // Never expire.
                            .AddStringProperty("subject", "test")
                            .AddStringProperty("sender", "namespace#person")
                            .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_enable_background_task_scheduler(true);
  options.set_enable_delete_propagation_from(true);
  options.set_expired_document_purge_threshold_ms(0);

  // Initialize Icing with a fake clock and t = 10 ms. At this moment, there is
  // no expiration document so the purging expired document task is not
  // scheduled.
  auto fake_clock = std::make_unique<FakeClock>();
  FakeClock* fake_clock_ptr = fake_clock.get();
  fake_clock->SetSystemTimeMilliseconds(10);
  TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  // Put person and email. The scheduler should schedule the purging expiration
  // task at t = 1010 ms.
  ASSERT_THAT(icing.Put(person).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email).status(), ProtoIsOk());

  // Sanity check that person and email are present.
  GetResultProto expected_get_result_proto1;
  expected_get_result_proto1.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto1.mutable_document() = person;
  EXPECT_THAT(
      icing.Get("namespace", "person", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto1));

  GetResultProto expected_get_result_google::protobuf;
  expected_get_result_google::protobuf.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_google::protobuf.mutable_document() = email;
  EXPECT_THAT(
      icing.Get("namespace", "email", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_google::protobuf));

  // Adjust the clock to 1010 ms and sleep for 1010 ms. The purging expiration
  // task should execute and purge person and email.
  // - At t = 1010 ms, person is expired, but DocumentFilterData has already
  //   filtered it out, so we're unsure whether it is purged or not.
  // - But the child document email never expires and DocumentFilterData
  //   doesn't filter it out. If the scheduled task had not worked correctly,
  //   then expiration propagation would have not been triggered and email
  //   would've been alive. Therefore, we can verify it by checking email.
  fake_clock_ptr->SetSystemTimeMilliseconds(1010);
  std::this_thread::sleep_for(std::chrono::milliseconds(1010));

  GetResultProto expected_get_result_proto3;
  expected_get_result_proto3.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto3.mutable_status()->set_message(
      "Document (namespace, person) not found.");
  EXPECT_THAT(
      icing.Get("namespace", "person", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto3));

  GetResultProto expected_get_result_proto4;
  expected_get_result_proto4.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto4.mutable_status()->set_message(
      "Document (namespace, email) not found.");
  EXPECT_THAT(
      icing.Get("namespace", "email", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto4));
}

TEST_F(IcingSearchEnginePutTest,
       PutDocument_shouldReschedulePurgingExpirationTaskForSmallerExpTs) {
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
                              .SetTtlMs(2000000)  // Expire at 2000010 ms.
                              .AddStringProperty("name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey("namespace", "person2")
                              .SetSchema("Person")
                              .SetCreationTimestampMs(10)
                              .SetTtlMs(1000)  // Expire at 1010 ms.
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
  options.set_expired_document_purge_threshold_ms(0);

  {
    // Initialize Icing and put person1 and email1. Destruct Icing.
    TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(person1).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(email1).status(), ProtoIsOk());
  }

  // Initialize Icing again with a fake clock and t = 10 ms. Initialization
  // should schedule the purging expiration task at t = 2000010 ms.
  auto fake_clock = std::make_unique<FakeClock>();
  FakeClock* fake_clock_ptr = fake_clock.get();
  fake_clock->SetSystemTimeMilliseconds(10);
  TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  // Put person2 and email2. It should reschedule the purging expiration task at
  // t = 1010 ms.
  ASSERT_THAT(icing.Put(person2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email2).status(), ProtoIsOk());

  // Sanity check that person2 and email2 are present.
  GetResultProto expected_get_result_proto1;
  expected_get_result_proto1.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto1.mutable_document() = person2;
  EXPECT_THAT(
      icing.Get("namespace", "person2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto1));

  GetResultProto expected_get_result_google::protobuf;
  expected_get_result_google::protobuf.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_google::protobuf.mutable_document() = email2;
  EXPECT_THAT(
      icing.Get("namespace", "email2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_google::protobuf));

  // Adjust the clock to 1010 ms and sleep for 1010 ms. The purging expiration
  // task should execute and purge person2 and email2.
  // - At t = 1010 ms, person2 is expired, but DocumentFilterData has already
  //   filtered it out, so we're unsure whether it is purged or not.
  // - But the child document email2 never expires and DocumentFilterData
  //   doesn't filter it out. If the scheduled task had not worked correctly,
  //   then expiration propagation would have not been triggered and email2
  //   would've been alive. Therefore, we can verify it by checking email2.
  fake_clock_ptr->SetSystemTimeMilliseconds(1010);
  std::this_thread::sleep_for(std::chrono::milliseconds(1010));

  GetResultProto expected_get_result_proto3;
  expected_get_result_proto3.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto3.mutable_status()->set_message(
      "Document (namespace, person2) not found.");
  EXPECT_THAT(
      icing.Get("namespace", "person2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto3));

  GetResultProto expected_get_result_proto4;
  expected_get_result_proto4.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto4.mutable_status()->set_message(
      "Document (namespace, email2) not found.");
  EXPECT_THAT(
      icing.Get("namespace", "email2", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto4));
}

TEST_F(IcingSearchEnginePutTest,
       PutDocument_shouldNotReschedulePurgingExpirationTaskForGreaterExpTs) {
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
                              .SetTtlMs(10000)  // Expire at 10010 ms.
                              .AddStringProperty("name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey("namespace", "person2")
                              .SetSchema("Person")
                              .SetCreationTimestampMs(10)
                              .SetTtlMs(2000000)  // Expire at 2000010 ms.
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
  options.set_expired_document_purge_threshold_ms(0);

  {
    // Initialize Icing and put person1 and email1. Destruct Icing.
    TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(person1).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(email1).status(), ProtoIsOk());
  }

  // Initialize Icing again with a fake clock and t = 9010 ms. Initialization
  // should schedule the purging expiration task at t = 10010 ms.
  auto fake_clock = std::make_unique<FakeClock>();
  FakeClock* fake_clock_ptr = fake_clock.get();
  fake_clock->SetSystemTimeMilliseconds(9010);
  TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

  // Put person2 and email2. It should NOT reschedule the purging expiration
  // task since the new expiration timestamp is greater than the current
  // scheduled one.
  ASSERT_THAT(icing.Put(person2).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(email2).status(), ProtoIsOk());

  // Sanity check that person1 and email1 are present.
  GetResultProto expected_get_result_proto1;
  expected_get_result_proto1.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto1.mutable_document() = person1;
  EXPECT_THAT(
      icing.Get("namespace", "person1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto1));

  GetResultProto expected_get_result_google::protobuf;
  expected_get_result_google::protobuf.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_google::protobuf.mutable_document() = email1;
  EXPECT_THAT(
      icing.Get("namespace", "email1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_google::protobuf));

  // Adjust the clock to 10010 ms and sleep for 1100 ms. The purging expiration
  // task should execute and purge person1 and email1.
  // - If Put API had falsely rescheduled the task, then at t = 10010 ms person1
  //   and email1 would've been alive. We need to verify that the task was not
  //   rescheduled with the greater expiration timestamp.
  // - At t = 10010 ms, person1 is expired, but DocumentFilterData has already
  //   filtered it out, so we're unsure whether it is purged or not.
  // - But the child document email1 never expires and DocumentFilterData
  //   doesn't filter it out. If the scheduled task had not worked correctly,
  //   then expiration propagation would have not been triggered and email1
  //   would've been alive. Therefore, we can verify it by checking email1.
  fake_clock_ptr->SetSystemTimeMilliseconds(10010);
  std::this_thread::sleep_for(std::chrono::milliseconds(1100));

  GetResultProto expected_get_result_proto3;
  expected_get_result_proto3.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto3.mutable_status()->set_message(
      "Document (namespace, person1) not found.");
  EXPECT_THAT(
      icing.Get("namespace", "person1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto3));

  GetResultProto expected_get_result_proto4;
  expected_get_result_proto4.mutable_status()->set_code(StatusProto::NOT_FOUND);
  expected_get_result_proto4.mutable_status()->set_message(
      "Document (namespace, email1) not found.");
  EXPECT_THAT(
      icing.Get("namespace", "email1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto4));
}

TEST_F(IcingSearchEnginePutTest,
       PutDocument_shouldSucceedWithDeletePropagationEnabledAndValidParent) {
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

  DocumentProto person = DocumentBuilder()
                             .SetKey("namespace", "person")
                             .SetSchema("Person")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(10000)  // Expire at 10010 ms.
                             .AddStringProperty("name", "Alice")
                             .Build();
  DocumentProto email = DocumentBuilder()
                            .SetKey("namespace", "email")
                            .SetSchema("Email")
                            .SetCreationTimestampMs(10)
                            .SetTtlMs(0)  // Never expire.
                            .AddStringProperty("subject", "test")
                            .AddStringProperty("sender", "namespace#person")
                            .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_enable_delete_propagation_from(true);
  options.set_expired_document_purge_threshold_ms(0);

  // Initialize Icing.
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetSystemTimeMilliseconds(10);
  TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person).status(), ProtoIsOk());

  // Put email document should succeed.
  EXPECT_THAT(icing.Put(email).status(), ProtoIsOk());

  // Can get email document.
  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = email;
  EXPECT_THAT(
      icing.Get("namespace", "email", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));
}

TEST_F(
    IcingSearchEnginePutTest,
    PutDocument_shouldSucceedWithDeletePropagationEnabledAndEmptyQualifiedId) {
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

  DocumentProto email =
      DocumentBuilder()
          .SetKey("namespace", "email")
          .SetSchema("Email")
          .SetCreationTimestampMs(10)
          .SetTtlMs(0)  // Never expire.
          .AddStringProperty("subject", "test")
          .AddStringProperty("sender", "")  // Empty qualified id.
          .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_enable_delete_propagation_from(true);
  options.set_expired_document_purge_threshold_ms(0);

  // Initialize Icing.
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetSystemTimeMilliseconds(10);
  TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  // Put email document should succeed.
  EXPECT_THAT(icing.Put(email).status(), ProtoIsOk());

  // Can get email document.
  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = email;
  EXPECT_THAT(
      icing.Get("namespace", "email", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));
}

TEST_F(
    IcingSearchEnginePutTest,
    PutDocument_shouldFailWithDeletePropagationEnabledAndInvalidQualifiedId) {
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

  DocumentProto email = DocumentBuilder()
                            .SetKey("namespace", "email")
                            .SetSchema("Email")
                            .SetCreationTimestampMs(10)
                            .SetTtlMs(0)  // Never expire.
                            .AddStringProperty("subject", "test")
                            .AddStringProperty("sender", "InvalidQualifiedId")
                            .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_enable_delete_propagation_from(true);
  options.set_expired_document_purge_threshold_ms(0);

  // Initialize Icing.
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetSystemTimeMilliseconds(10);
  TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  // Put email document should fail.
  EXPECT_THAT(icing.Put(email).status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEnginePutTest,
       PutDocument_shouldFailWithDeletePropagationEnabledAndNonExistentParent) {
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

  DocumentProto email =
      DocumentBuilder()
          .SetKey("namespace", "email")
          .SetSchema("Email")
          .SetCreationTimestampMs(10)
          .SetTtlMs(0)  // Never expire.
          .AddStringProperty("subject", "test")
          .AddStringProperty("sender",
                             "namespace#person")  // Non-existent parent.
          .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_enable_delete_propagation_from(true);
  options.set_expired_document_purge_threshold_ms(0);

  // Initialize Icing.
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetSystemTimeMilliseconds(10);
  TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

  // Put email document should fail.
  EXPECT_THAT(icing.Put(email).status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEnginePutTest,
       PutDocument_shouldFailWithDeletePropagationEnabledAndExpiredParent) {
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

  DocumentProto person = DocumentBuilder()
                             .SetKey("namespace", "person")
                             .SetSchema("Person")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(10000)  // Expire at 10010 ms.
                             .AddStringProperty("name", "Alice")
                             .Build();
  DocumentProto email = DocumentBuilder()
                            .SetKey("namespace", "email")
                            .SetSchema("Email")
                            .SetCreationTimestampMs(10)
                            .SetTtlMs(0)  // Never expire.
                            .AddStringProperty("subject", "test")
                            .AddStringProperty("sender", "namespace#person")
                            .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_enable_delete_propagation_from(true);
  options.set_expired_document_purge_threshold_ms(0);

  // Initialize Icing.
  auto fake_clock = std::make_unique<FakeClock>();
  FakeClock* fake_clock_ptr = fake_clock.get();
  fake_clock->SetSystemTimeMilliseconds(10);
  TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person).status(), ProtoIsOk());

  // Adjust the clock to 20000 ms and put email document. Since person document
  // is expired, put email document should fail.
  fake_clock_ptr->SetSystemTimeMilliseconds(20000);
  EXPECT_THAT(icing.Put(email).status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEnginePutTest,
       PutDocument_shouldFailWithDeletePropagationEnabledAndDeletedParent) {
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

  DocumentProto person = DocumentBuilder()
                             .SetKey("namespace", "person")
                             .SetSchema("Person")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(0)  // Never expire.
                             .AddStringProperty("name", "Alice")
                             .Build();
  DocumentProto email = DocumentBuilder()
                            .SetKey("namespace", "email")
                            .SetSchema("Email")
                            .SetCreationTimestampMs(10)
                            .SetTtlMs(0)  // Never expire.
                            .AddStringProperty("subject", "test")
                            .AddStringProperty("sender", "namespace#person")
                            .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_enable_delete_propagation_from(true);
  options.set_expired_document_purge_threshold_ms(0);

  // Initialize Icing.
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetSystemTimeMilliseconds(10);
  TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person).status(), ProtoIsOk());

  // Delete person document. Put email document should fail afterwards.
  ASSERT_THAT(icing.Delete("namespace", "person").status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(email).status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(
    IcingSearchEnginePutTest,
    PutDocument_shouldSucceedWithDeletePropagationDisabledAndNonAliveParents) {
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
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("subject")
                                   .SetDataTypeString(TERM_MATCH_PREFIX,
                                                      TOKENIZER_PLAIN)
                                   .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("sender")
                          .SetDataTypeJoinableString(
                              JOINABLE_VALUE_TYPE_QUALIFIED_ID)  // No delete
                                                                 // propagation.
                          .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  DocumentProto person1 = DocumentBuilder()
                              .SetKey("namespace", "person/1")
                              .SetSchema("Person")
                              .SetCreationTimestampMs(10)
                              .SetTtlMs(10000)  // Expire at 10010 ms.
                              .AddStringProperty("name", "Alice")
                              .Build();
  DocumentProto person2 = DocumentBuilder()
                              .SetKey("namespace", "person/2")
                              .SetSchema("Person")
                              .SetCreationTimestampMs(10)
                              .SetTtlMs(0)  // Never expire.
                              .AddStringProperty("name", "Bob")
                              .Build();
  DocumentProto email1 = DocumentBuilder()
                             .SetKey("namespace", "email/1")
                             .SetSchema("Email")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(0)  // Never expire.
                             .AddStringProperty("subject", "test")
                             .AddStringProperty("sender", "InvalidQualifiedId")
                             .Build();
  DocumentProto email2 =
      DocumentBuilder()
          .SetKey("namespace", "email/2")
          .SetSchema("Email")
          .SetCreationTimestampMs(10)
          .SetTtlMs(0)  // Never expire.
          .AddStringProperty("subject", "test")
          .AddStringProperty(
              "sender", "namespace#NonExistentParent")  // Non-existent parent.
          .Build();
  DocumentProto email3 = DocumentBuilder()
                             .SetKey("namespace", "email/3")
                             .SetSchema("Email")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(0)  // Never expire.
                             .AddStringProperty("subject", "test")
                             .AddStringProperty("sender", "namespace#person/1")
                             .Build();
  DocumentProto email4 = DocumentBuilder()
                             .SetKey("namespace", "email/4")
                             .SetSchema("Email")
                             .SetCreationTimestampMs(10)
                             .SetTtlMs(0)  // Never expire.
                             .AddStringProperty("subject", "test")
                             .AddStringProperty("sender", "namespace#person/2")
                             .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_enable_delete_propagation_from(true);
  options.set_expired_document_purge_threshold_ms(0);

  // Initialize Icing.
  auto fake_clock = std::make_unique<FakeClock>();
  FakeClock* fake_clock_ptr = fake_clock.get();
  fake_clock->SetSystemTimeMilliseconds(10);
  TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person1).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(person2).status(), ProtoIsOk());

  // Put email1, email2 should succeed, even though they contain invalid
  // qualified id and non-existent parent respectively.
  EXPECT_THAT(icing.Put(email1).status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(email2).status(), ProtoIsOk());

  // Adjust the clock to 20000 ms and put email3. Although person1 is expired
  // and email3 contains a joinable qualified id to person1, putting email3
  // should succeed since delete propagation is disabled.
  fake_clock_ptr->SetSystemTimeMilliseconds(20000);
  EXPECT_THAT(icing.Put(email3).status(), ProtoIsOk());

  // Delete person2. Although email4 contains a joinable qualified id to
  // person2, putting email4 should succeed since delete propagation is
  // disabled.
  ASSERT_THAT(icing.Delete("namespace", "person/2").status(), ProtoIsOk());
  EXPECT_THAT(icing.Put(email4).status(), ProtoIsOk());
}

TEST_F(IcingSearchEnginePutTest,
       PutDocument_shouldSetExpirationTimestampInPutResultProto) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetSystemTimeMilliseconds(10);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  DocumentProto document1 = DocumentBuilder()
                                .SetKey("icing", "message/1")
                                .SetSchema("Message")
                                .SetCreationTimestampMs(10)
                                .SetTtlMs(10000)  // Expire at 10010 ms.
                                .AddStringProperty("body", "message body")
                                .Build();
  PutResultProto put_result_proto1 = icing.Put(document1);
  ASSERT_THAT(put_result_proto1.status(), ProtoIsOk());
  EXPECT_THAT(put_result_proto1.document_expiration_timestamp_ms(), Eq(10010));

  DocumentProto document2 = DocumentBuilder()
                                .SetKey("icing", "message/2")
                                .SetSchema("Message")
                                .SetCreationTimestampMs(10)
                                .SetTtlMs(0)  // Never expire.
                                .AddStringProperty("body", "message body")
                                .Build();
  PutResultProto put_result_google::protobuf = icing.Put(document2);
  ASSERT_THAT(put_result_google::protobuf.status(), ProtoIsOk());
  EXPECT_THAT(put_result_google::protobuf.document_expiration_timestamp_ms(),
              Eq(std::numeric_limits<int64_t>::max()));
}

}  // namespace
}  // namespace lib
}  // namespace icing
