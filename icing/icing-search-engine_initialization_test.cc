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

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "icing/text_classifier/lib3/utils/base/status.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "icing/absl_ports/str_cat.h"
#include "icing/document-builder.h"
#include "icing/feature-flags.h"
#include "icing/file/file-backed-vector.h"
#include "icing/file/filesystem.h"
#include "icing/file/memory-mapped-file.h"
#include "icing/file/mock-filesystem.h"
#include "icing/file/portable-file-backed-proto-log.h"
#include "icing/file/version-util.h"
#include "icing/icing-search-engine.h"
#include "icing/index/data-indexing-handler.h"
#include "icing/index/embed/embedding-index.h"
#include "icing/index/embedding-indexing-handler.h"
#include "icing/index/index-processor.h"
#include "icing/index/index.h"
#include "icing/index/integer-section-indexing-handler.h"
#include "icing/index/iterator/doc-hit-info-iterator.h"
#include "icing/index/numeric/integer-index.h"
#include "icing/index/numeric/numeric-index.h"
#include "icing/index/term-indexing-handler.h"
#include "icing/jni/jni-cache.h"
#include "icing/join/document-join-id-pair.h"
#include "icing/join/join-processor.h"
#include "icing/join/qualified-id-join-index-impl-v3.h"
#include "icing/join/qualified-id-join-index.h"
#include "icing/join/qualified-id-join-indexing-handler.h"
#include "icing/legacy/index/icing-filesystem.h"
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
#include "icing/query/query-features.h"
#include "icing/schema-builder.h"
#include "icing/schema/schema-store.h"
#include "icing/schema/section.h"
#include "icing/store/blob-store.h"
#include "icing/store/document-associated-score-data.h"
#include "icing/store/document-id.h"
#include "icing/store/document-log-creator.h"
#include "icing/store/document-store.h"
#include "icing/store/namespace-id-fingerprint.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/embedding-test-utils.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/jni-test-helpers.h"
#include "icing/testing/test-data.h"
#include "icing/testing/test-feature-flags.h"
#include "icing/testing/tmp-directory.h"
#include "icing/tokenization/language-segmenter-factory.h"
#include "icing/tokenization/language-segmenter.h"
#include "icing/transform/normalizer-factory.h"
#include "icing/transform/normalizer-options.h"
#include "icing/transform/normalizer.h"
#include "icing/util/clock.h"
#include "icing/util/icu-data-file-helper.h"
#include "icing/util/tokenized-document.h"
#include "unicode/uloc.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoDefault;
using ::testing::EndsWith;
using ::testing::Eq;
using ::testing::HasSubstr;
using ::testing::IsEmpty;
using ::testing::Matcher;
using ::testing::Ne;
using ::testing::Pointee;
using ::testing::Return;
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

PortableFileBackedProtoLog<DocumentWrapper>::Header ReadDocumentLogHeader(
    Filesystem filesystem, const std::string& file_path) {
  PortableFileBackedProtoLog<DocumentWrapper>::Header header;
  filesystem.PRead(file_path.c_str(), &header,
                   sizeof(PortableFileBackedProtoLog<DocumentWrapper>::Header),
                   /*offset=*/0);
  return header;
}

void WriteDocumentLogHeader(
    Filesystem filesystem, const std::string& file_path,
    PortableFileBackedProtoLog<DocumentWrapper>::Header& header) {
  filesystem.Write(file_path.c_str(), &header,
                   sizeof(PortableFileBackedProtoLog<DocumentWrapper>::Header));
}

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

// This test is meant to cover all tests relating to
// IcingSearchEngine::Initialize.
class IcingSearchEngineInitializationTest : public testing::Test {
 protected:
  void SetUp() override {
    feature_flags_ = std::make_unique<FeatureFlags>(GetTestFeatureFlags());

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

    language_segmenter_factory::SegmenterOptions segmenter_options(ULOC_US);
    ICING_ASSERT_OK_AND_ASSIGN(
        lang_segmenter_,
        language_segmenter_factory::Create(std::move(segmenter_options)));

    NormalizerOptions normalizer_options(
        /*max_term_byte_size=*/std::numeric_limits<int32_t>::max());
    ICING_ASSERT_OK_AND_ASSIGN(normalizer_,
                               normalizer_factory::Create(normalizer_options));
  }

  void TearDown() override {
    normalizer_.reset();
    lang_segmenter_.reset();
    filesystem_.DeleteDirectoryRecursively(GetTestBaseDir().c_str());
  }

  const Filesystem* filesystem() const { return &filesystem_; }

  const IcingFilesystem* icing_filesystem() const { return &icing_filesystem_; }

  std::unique_ptr<FeatureFlags> feature_flags_;
  Filesystem filesystem_;
  IcingFilesystem icing_filesystem_;
  std::unique_ptr<LanguageSegmenter> lang_segmenter_;
  std::unique_ptr<Normalizer> normalizer_;
};

// Non-zero value so we don't override it to be the current time
constexpr int64_t kDefaultCreationTimestampMs = 1575492852000;

std::string GetVersionFileDir() { return GetTestBaseDir(); }

std::string GetDocumentDir() { return GetTestBaseDir() + "/document_dir"; }

std::string GetIndexDir() { return GetTestBaseDir() + "/index_dir"; }

std::string GetIntegerIndexDir() {
  return GetTestBaseDir() + "/integer_index_dir";
}

std::string GetQualifiedIdJoinIndexDir() {
  return GetTestBaseDir() + "/qualified_id_join_index_dir";
}

std::string GetEmbeddingIndexDir() {
  return GetTestBaseDir() + "/embedding_index_dir";
}

std::string GetSchemaDir() { return GetTestBaseDir() + "/schema_dir"; }

std::string GetBlobDir() { return GetTestBaseDir() + "/blob_dir"; }

std::string GetHeaderFilename() {
  return GetTestBaseDir() + "/icing_search_engine_header";
}

IcingSearchEngineOptions GetDefaultIcingOptions() {
  IcingSearchEngineOptions icing_options;
  icing_options.set_base_dir(GetTestBaseDir());
  icing_options.set_document_store_namespace_id_fingerprint(true);
  icing_options.set_enable_embedding_index(true);
  icing_options.set_enable_embedding_quantization(true);
  icing_options.set_enable_blob_store(true);
  icing_options.set_enable_qualified_id_join_index_v3_and_delete_propagate_from(
      true);
  return icing_options;
}

DocumentProto CreateMessageDocument(std::string name_space, std::string uri) {
  return DocumentBuilder()
      .SetKey(std::move(name_space), std::move(uri))
      .SetSchema("Message")
      .AddStringProperty("body", "message body")
      .AddInt64Property("indexableInteger", 123)
      .SetCreationTimestampMs(kDefaultCreationTimestampMs)
      .Build();
}

DocumentProto CreateEmailDocument(const std::string& name_space,
                                  const std::string& uri, int score,
                                  const std::string& subject_content,
                                  const std::string& body_content) {
  return DocumentBuilder()
      .SetKey(name_space, uri)
      .SetSchema("Email")
      .SetScore(score)
      .AddStringProperty("subject", subject_content)
      .AddStringProperty("body", body_content)
      .Build();
}

SchemaTypeConfigProto CreateMessageSchemaTypeConfig() {
  return SchemaTypeConfigBuilder()
      .SetType("Message")
      .AddProperty(PropertyConfigBuilder()
                       .SetName("body")
                       .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                       .SetCardinality(CARDINALITY_REQUIRED))
      .AddProperty(PropertyConfigBuilder()
                       .SetName("indexableInteger")
                       .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
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

SchemaProto CreateMessageSchema() {
  return SchemaBuilder().AddType(CreateMessageSchemaTypeConfig()).Build();
}

SchemaProto CreateEmailSchema() {
  return SchemaBuilder().AddType(CreateEmailSchemaTypeConfig()).Build();
}

ScoringSpecProto GetDefaultScoringSpec() {
  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  return scoring_spec;
}

// TODO(b/272145329): create SearchSpecBuilder, JoinSpecBuilder,
// SearchResultProtoBuilder and ResultProtoBuilder for unit tests and build all
// instances by them.

TEST_F(IcingSearchEngineInitializationTest, UninitializedInstanceFailsSafely) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());

  SchemaProto email_schema = CreateMessageSchema();
  EXPECT_THAT(icing.SetSchema(email_schema).status(),
              ProtoStatusIs(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(icing.GetSchema().status(),
              ProtoStatusIs(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(icing.GetSchemaType(email_schema.types(0).schema_type()).status(),
              ProtoStatusIs(StatusProto::FAILED_PRECONDITION));

  DocumentProto doc = CreateMessageDocument("namespace", "uri");
  EXPECT_THAT(icing.Put(doc).status(),
              ProtoStatusIs(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(icing
                  .Get(doc.namespace_(), doc.uri(),
                       GetResultSpecProto::default_instance())
                  .status(),
              ProtoStatusIs(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(icing.Delete(doc.namespace_(), doc.uri()).status(),
              ProtoStatusIs(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(icing.DeleteByNamespace(doc.namespace_()).status(),
              ProtoStatusIs(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(icing.DeleteBySchemaType(email_schema.types(0).schema_type())
                  .status()
                  .code(),
              Eq(StatusProto::FAILED_PRECONDITION));

  SearchSpecProto search_spec = SearchSpecProto::default_instance();
  ScoringSpecProto scoring_spec = ScoringSpecProto::default_instance();
  ResultSpecProto result_spec = ResultSpecProto::default_instance();
  EXPECT_THAT(icing.Search(search_spec, scoring_spec, result_spec).status(),
              ProtoStatusIs(StatusProto::FAILED_PRECONDITION));
  constexpr int kSomePageToken = 12;
  EXPECT_THAT(icing.GetNextPage(kSomePageToken).status(),
              ProtoStatusIs(StatusProto::FAILED_PRECONDITION));
  icing.InvalidateNextPageToken(kSomePageToken);  // Verify this doesn't crash.

  EXPECT_THAT(icing.PersistToDisk(PersistType::FULL).status(),
              ProtoStatusIs(StatusProto::FAILED_PRECONDITION));
  EXPECT_THAT(icing.Optimize().status(),
              ProtoStatusIs(StatusProto::FAILED_PRECONDITION));
}

TEST_F(IcingSearchEngineInitializationTest, SimpleInitialization) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  DocumentProto document = CreateMessageDocument("namespace", "uri");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  ASSERT_THAT(icing.Put(DocumentProto(document)).status(), ProtoIsOk());
}

TEST_F(IcingSearchEngineInitializationTest,
       InitializingAgainSavesNonPersistedData) {
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
  ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

  DocumentProto document = CreateMessageDocument("namespace", "uri");
  ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = document;

  ASSERT_THAT(
      icing.Get("namespace", "uri", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  EXPECT_THAT(
      icing.Get("namespace", "uri", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));
}

TEST_F(IcingSearchEngineInitializationTest,
       MaxIndexMergeSizeReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_index_merge_size(std::numeric_limits<int32_t>::max());
  IcingSearchEngine icing(options, GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineInitializationTest,
       NegativeMergeSizeReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_index_merge_size(-1);
  IcingSearchEngine icing(options, GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineInitializationTest,
       ZeroMergeSizeReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_index_merge_size(0);
  IcingSearchEngine icing(options, GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineInitializationTest, GoodIndexMergeSizeReturnsOk) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  // One is fine, if a bit weird. It just means that the lite index will be
  // smaller and will request a merge any time content is added to it.
  options.set_index_merge_size(1);
  IcingSearchEngine icing(options, GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
}

TEST_F(IcingSearchEngineInitializationTest,
       NegativeMaxTokenLenReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_max_token_length(-1);
  IcingSearchEngine icing(options, GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineInitializationTest,
       ZeroMaxTokenLenReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_max_token_length(0);
  IcingSearchEngine icing(options, GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineInitializationTest,
       NegativeCompressionLevelReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_compression_level(-1);
  IcingSearchEngine icing(options, GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineInitializationTest,
       GreaterThanMaxCompressionLevelReturnsInvalidArgument) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_compression_level(10);
  IcingSearchEngine icing(options, GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(),
              ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
}

TEST_F(IcingSearchEngineInitializationTest, GoodCompressionLevelReturnsOk) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_compression_level(0);
  IcingSearchEngine icing(options, GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
}

TEST_F(IcingSearchEngineInitializationTest,
       ReinitializingWithDifferentCompressionLevelReturnsOk) {
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_compression_level(3);
  {
    IcingSearchEngine icing(options, GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

    DocumentProto document = CreateMessageDocument("namespace", "uri");
    ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
    ASSERT_THAT(icing.PersistToDisk(PersistType::FULL).status(), ProtoIsOk());
  }
  options.set_compression_level(9);
  {
    IcingSearchEngine icing(options, GetTestJniCache());
    EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  }
  options.set_compression_level(0);
  {
    IcingSearchEngine icing(options, GetTestJniCache());
    EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
  }
}

TEST_F(IcingSearchEngineInitializationTest, FailToCreateDocStore) {
  auto mock_filesystem = std::make_unique<MockFilesystem>();
  // This fails DocumentStore::Create()
  ON_CALL(*mock_filesystem, CreateDirectoryRecursively(_))
      .WillByDefault(Return(false));

  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::move(mock_filesystem),
                              std::make_unique<IcingFilesystem>(),
                              std::make_unique<FakeClock>(), GetTestJniCache());

  InitializeResultProto initialize_result_proto = icing.Initialize();
  EXPECT_THAT(initialize_result_proto.status(),
              ProtoStatusIs(StatusProto::INTERNAL));
  EXPECT_THAT(initialize_result_proto.status().message(),
              HasSubstr("Could not create directory"));
}

TEST_F(IcingSearchEngineInitializationTest,
       InitMarkerFilePreviousFailuresAtThreshold) {
  Filesystem filesystem;
  DocumentProto email1 =
      CreateEmailDocument("namespace", "uri1", 100, "subject1", "body1");
  email1.set_creation_timestamp_ms(10000);
  DocumentProto email2 =
      CreateEmailDocument("namespace", "uri2", 50, "subject2", "body2");
  email2.set_creation_timestamp_ms(10000);

  {
    // Create an index with a few documents.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    InitializeResultProto init_result = icing.Initialize();
    ASSERT_THAT(init_result.status(), ProtoIsOk());
    ASSERT_THAT(init_result.initialize_stats().num_previous_init_failures(),
                Eq(0));
    ASSERT_THAT(icing.SetSchema(CreateEmailSchema()).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(email1).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(email2).status(), ProtoIsOk());
  }

  // Write an init marker file with 5 previously failed attempts.
  std::string marker_filepath = GetTestBaseDir() + "/init_marker";

  {
    ScopedFd marker_file_fd(filesystem.OpenForWrite(marker_filepath.c_str()));
    int network_init_attempts = GHostToNetworkL(5);
    // Write the updated number of attempts before we get started.
    ASSERT_TRUE(filesystem.PWrite(marker_file_fd.get(), 0,
                                  &network_init_attempts,
                                  sizeof(network_init_attempts)));
    ASSERT_TRUE(filesystem.DataSync(marker_file_fd.get()));
  }

  {
    // Create the index again and verify that initialization succeeds and no
    // data is thrown out.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    InitializeResultProto init_result = icing.Initialize();
    ASSERT_THAT(init_result.status(), ProtoIsOk());
    ASSERT_THAT(init_result.initialize_stats().num_previous_init_failures(),
                Eq(5));
    EXPECT_THAT(
        icing.Get("namespace", "uri1", GetResultSpecProto::default_instance())
            .document(),
        EqualsProto(email1));
    EXPECT_THAT(
        icing.Get("namespace", "uri2", GetResultSpecProto::default_instance())
            .document(),
        EqualsProto(email2));
  }

  // The successful init should have thrown out the marker file.
  ASSERT_FALSE(filesystem.FileExists(marker_filepath.c_str()));
}

TEST_F(IcingSearchEngineInitializationTest,
       InitMarkerFilePreviousFailuresBeyondThreshold) {
  Filesystem filesystem;
  DocumentProto email1 =
      CreateEmailDocument("namespace", "uri1", 100, "subject1", "body1");
  DocumentProto email2 =
      CreateEmailDocument("namespace", "uri2", 50, "subject2", "body2");

  {
    // Create an index with a few documents.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    InitializeResultProto init_result = icing.Initialize();
    ASSERT_THAT(init_result.status(), ProtoIsOk());
    ASSERT_THAT(init_result.initialize_stats().num_previous_init_failures(),
                Eq(0));
    ASSERT_THAT(icing.SetSchema(CreateEmailSchema()).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(email1).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(email2).status(), ProtoIsOk());
  }

  // Write an init marker file with 6 previously failed attempts.
  std::string marker_filepath = GetTestBaseDir() + "/init_marker";

  {
    ScopedFd marker_file_fd(filesystem.OpenForWrite(marker_filepath.c_str()));
    int network_init_attempts = GHostToNetworkL(6);
    // Write the updated number of attempts before we get started.
    ASSERT_TRUE(filesystem.PWrite(marker_file_fd.get(), 0,
                                  &network_init_attempts,
                                  sizeof(network_init_attempts)));
    ASSERT_TRUE(filesystem.DataSync(marker_file_fd.get()));
  }

  {
    // Create the index again and verify that initialization succeeds and all
    // data is thrown out.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    InitializeResultProto init_result = icing.Initialize();
    ASSERT_THAT(init_result.status(),
                ProtoStatusIs(StatusProto::WARNING_DATA_LOSS));
    ASSERT_THAT(init_result.initialize_stats().num_previous_init_failures(),
                Eq(6));
    EXPECT_THAT(
        icing.Get("namespace", "uri1", GetResultSpecProto::default_instance())
            .status(),
        ProtoStatusIs(StatusProto::NOT_FOUND));
    EXPECT_THAT(
        icing.Get("namespace", "uri2", GetResultSpecProto::default_instance())
            .status(),
        ProtoStatusIs(StatusProto::NOT_FOUND));
  }

  // The successful init should have thrown out the marker file.
  ASSERT_FALSE(filesystem.FileExists(marker_filepath.c_str()));
}

TEST_F(IcingSearchEngineInitializationTest,
       SuccessiveInitFailuresIncrementsInitMarker) {
  Filesystem filesystem;
  DocumentProto email1 =
      CreateEmailDocument("namespace", "uri1", 100, "subject1", "body1");
  DocumentProto email2 =
      CreateEmailDocument("namespace", "uri2", 50, "subject2", "body2");

  {
    // 1. Create an index with a few documents.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    InitializeResultProto init_result = icing.Initialize();
    ASSERT_THAT(init_result.status(), ProtoIsOk());
    ASSERT_THAT(init_result.initialize_stats().num_previous_init_failures(),
                Eq(0));
    ASSERT_THAT(icing.SetSchema(CreateEmailSchema()).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(email1).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(email2).status(), ProtoIsOk());
  }

  {
    // 2. Create an index that will encounter an IO failure when trying to
    // create the document log.
    IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();

    auto mock_filesystem = std::make_unique<MockFilesystem>();
    std::string document_log_filepath =
        icing_options.base_dir() + "/document_dir/document_log_v1";
    ON_CALL(*mock_filesystem,
            GetFileSize(Matcher<const char*>(Eq(document_log_filepath))))
        .WillByDefault(Return(Filesystem::kBadFileSize));

    TestIcingSearchEngine icing(icing_options, std::move(mock_filesystem),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());

    // Fail to initialize six times in a row.
    InitializeResultProto init_result = icing.Initialize();
    ASSERT_THAT(init_result.status(), ProtoStatusIs(StatusProto::INTERNAL));
    ASSERT_THAT(init_result.initialize_stats().num_previous_init_failures(),
                Eq(0));

    init_result = icing.Initialize();
    ASSERT_THAT(init_result.status(), ProtoStatusIs(StatusProto::INTERNAL));
    ASSERT_THAT(init_result.initialize_stats().num_previous_init_failures(),
                Eq(1));

    init_result = icing.Initialize();
    ASSERT_THAT(init_result.status(), ProtoStatusIs(StatusProto::INTERNAL));
    ASSERT_THAT(init_result.initialize_stats().num_previous_init_failures(),
                Eq(2));

    init_result = icing.Initialize();
    ASSERT_THAT(init_result.status(), ProtoStatusIs(StatusProto::INTERNAL));
    ASSERT_THAT(init_result.initialize_stats().num_previous_init_failures(),
                Eq(3));

    init_result = icing.Initialize();
    ASSERT_THAT(init_result.status(), ProtoStatusIs(StatusProto::INTERNAL));
    ASSERT_THAT(init_result.initialize_stats().num_previous_init_failures(),
                Eq(4));

    init_result = icing.Initialize();
    ASSERT_THAT(init_result.status(), ProtoStatusIs(StatusProto::INTERNAL));
    ASSERT_THAT(init_result.initialize_stats().num_previous_init_failures(),
                Eq(5));
  }

  {
    // 3. Create the index again and verify that initialization succeeds and all
    // data is thrown out.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    InitializeResultProto init_result = icing.Initialize();
    ASSERT_THAT(init_result.status(),
                ProtoStatusIs(StatusProto::WARNING_DATA_LOSS));
    ASSERT_THAT(init_result.initialize_stats().num_previous_init_failures(),
                Eq(6));

    EXPECT_THAT(
        icing.Get("namespace", "uri1", GetResultSpecProto::default_instance())
            .status(),
        ProtoStatusIs(StatusProto::NOT_FOUND));
    EXPECT_THAT(
        icing.Get("namespace", "uri2", GetResultSpecProto::default_instance())
            .status(),
        ProtoStatusIs(StatusProto::NOT_FOUND));
  }

  // The successful init should have thrown out the marker file.
  std::string marker_filepath = GetTestBaseDir() + "/init_marker";
  ASSERT_FALSE(filesystem.FileExists(marker_filepath.c_str()));
}

TEST_F(IcingSearchEngineInitializationTest, RecoverFromMissingHeaderFile) {
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
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
    EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(CreateMessageDocument("namespace", "uri")).status(),
                ProtoIsOk());
    EXPECT_THAT(
        icing.Get("namespace", "uri", GetResultSpecProto::default_instance()),
        EqualsProto(expected_get_result_proto));
    SearchResultProto search_result_proto =
        icing.Search(search_spec, GetDefaultScoringSpec(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                         expected_search_result_proto));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  EXPECT_TRUE(filesystem()->DeleteFile(GetHeaderFilename().c_str()));

  // We should be able to recover from this and access all our previous data
  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());

  // Checks that DocumentLog is still ok
  EXPECT_THAT(
      icing.Get("namespace", "uri", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  // Checks that the term index is still ok so we can search over it
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));

  // Checks that the integer index is still ok so we can search over it
  SearchSpecProto search_spec2;
  search_spec2.set_query("indexableInteger == 123");
  search_spec2.add_enabled_features(std::string(kNumericSearchFeature));

  SearchResultProto search_result_google::protobuf =
      icing.Search(search_spec2, ScoringSpecProto::default_instance(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_google::protobuf, EqualsSearchResultIgnoreStatsAndScores(
                                        expected_search_result_proto));

  // Checks that Schema is still since it'll be needed to validate the document
  EXPECT_THAT(icing.Put(CreateMessageDocument("namespace", "uri")).status(),
              ProtoIsOk());
}

TEST_F(IcingSearchEngineInitializationTest, UnableToRecoverFromCorruptSchema) {
  {
    // Basic initialization/setup
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
    EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(CreateMessageDocument("namespace", "uri")).status(),
                ProtoIsOk());

    GetResultProto expected_get_result_proto;
    expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
    *expected_get_result_proto.mutable_document() =
        CreateMessageDocument("namespace", "uri");

    EXPECT_THAT(
        icing.Get("namespace", "uri", GetResultSpecProto::default_instance()),
        EqualsProto(expected_get_result_proto));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  const std::string schema_file =
      absl_ports::StrCat(GetSchemaDir(), "/schema.pb");
  const std::string corrupt_data = "1234";
  EXPECT_TRUE(filesystem()->Write(schema_file.c_str(), corrupt_data.data(),
                                  corrupt_data.size()));

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(),
              ProtoStatusIs(StatusProto::INTERNAL));
}

TEST_F(IcingSearchEngineInitializationTest,
       UnableToRecoverFromCorruptDocumentLog) {
  {
    // Basic initialization/setup
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
    EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(CreateMessageDocument("namespace", "uri")).status(),
                ProtoIsOk());

    GetResultProto expected_get_result_proto;
    expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
    *expected_get_result_proto.mutable_document() =
        CreateMessageDocument("namespace", "uri");

    EXPECT_THAT(
        icing.Get("namespace", "uri", GetResultSpecProto::default_instance()),
        EqualsProto(expected_get_result_proto));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  const std::string document_log_file = absl_ports::StrCat(
      GetDocumentDir(), "/", DocumentLogCreator::GetDocumentLogFilename());
  const std::string corrupt_data = "1234";
  EXPECT_TRUE(filesystem()->Write(document_log_file.c_str(),
                                  corrupt_data.data(), corrupt_data.size()));

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(),
              ProtoStatusIs(StatusProto::INTERNAL));
}

TEST_F(IcingSearchEngineInitializationTest,
       RecoverFromInconsistentSchemaStore) {
  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  DocumentProto document2_with_additional_property =
      DocumentBuilder()
          .SetKey("namespace", "uri2")
          .SetSchema("Message")
          .AddStringProperty("additional", "content")
          .AddStringProperty("body", "message body")
          .AddInt64Property("indexableInteger", 123)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  {
    // Initializes folder and schema
    IcingSearchEngine icing(options, GetTestJniCache());
    EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());

    SchemaProto schema =
        SchemaBuilder()
            .AddType(
                SchemaTypeConfigBuilder(CreateMessageSchemaTypeConfig())
                    // Add non-indexable property "additional"
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("additional")
                                     .SetDataType(TYPE_STRING)
                                     .SetCardinality(CARDINALITY_OPTIONAL)))
            .Build();

    EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(document1).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(document2_with_additional_property).status(),
                ProtoIsOk());

    // Won't get us anything because "additional" isn't marked as an indexed
    // property in the schema
    SearchSpecProto search_spec;
    search_spec.set_query("additional:content");
    search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

    SearchResultProto expected_search_result_proto;
    expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
    SearchResultProto search_result_proto =
        icing.Search(search_spec, GetDefaultScoringSpec(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                         expected_search_result_proto));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  {
    // This schema will change the SchemaTypeIds from the previous schema_
    // (since SchemaTypeIds are assigned based on order of the types, and this
    // new schema changes the ordering of previous types)
    SchemaProto new_schema;
    auto type = new_schema.add_types();
    type->set_schema_type("Email");

    // Switching a non-indexable property to indexable changes the SectionIds
    // (since SectionIds are assigned based on alphabetical order of indexed
    // sections, marking "additional" as an indexed property will push the
    // "body" and "indexableInteger" property to different SectionIds)
    *new_schema.add_types() =
        SchemaTypeConfigBuilder(CreateMessageSchemaTypeConfig())
            .AddProperty(
                PropertyConfigBuilder()
                    .SetName("additional")
                    .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                    .SetCardinality(CARDINALITY_OPTIONAL))
            .Build();

    // Write the marker file
    std::string marker_filepath =
        absl_ports::StrCat(options.base_dir(), "/set_schema_marker");
    ScopedFd sfd(filesystem()->OpenForWrite(marker_filepath.c_str()));
    ASSERT_TRUE(sfd.is_valid());

    // Write the new schema
    FakeClock fake_clock;
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(filesystem(), GetSchemaDir(), &fake_clock,
                            feature_flags_.get()));
    ICING_EXPECT_OK(schema_store->SetSchema(
        new_schema, /*ignore_errors_and_delete_documents=*/false));
  }  // Will persist new schema

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());

  // We can insert a Email document since we kept the new schema
  DocumentProto email_document =
      DocumentBuilder()
          .SetKey("namespace", "email_uri")
          .SetSchema("Email")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  EXPECT_THAT(icing.Put(email_document).status(), ProtoIsOk());

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = email_document;

  EXPECT_THAT(icing.Get("namespace", "email_uri",
                        GetResultSpecProto::default_instance()),
              EqualsProto(expected_get_result_proto));

  // Verify term search
  SearchSpecProto search_spec1;

  // The section restrict will ensure we are using the correct, updated
  // SectionId in the Index
  search_spec1.set_query("additional:content");

  // Schema type filter will ensure we're using the correct, updated
  // SchemaTypeId in the DocumentStore
  search_spec1.add_schema_type_filters("Message");
  search_spec1.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto1;
  expected_search_result_proto1.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto1.mutable_results()->Add()->mutable_document() =
      document2_with_additional_property;

  SearchResultProto search_result_proto1 =
      icing.Search(search_spec1, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto1, EqualsSearchResultIgnoreStatsAndScores(
                                        expected_search_result_proto1));

  // Verify numeric (integer) search
  SearchSpecProto search_spec2;
  search_spec2.set_query("indexableInteger == 123");
  search_spec1.add_schema_type_filters("Message");
  search_spec2.add_enabled_features(std::string(kNumericSearchFeature));

  SearchResultProto expected_search_result_google::protobuf;
  expected_search_result_google::protobuf.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_google::protobuf.mutable_results()->Add()->mutable_document() =
      document2_with_additional_property;
  *expected_search_result_google::protobuf.mutable_results()->Add()->mutable_document() =
      document1;

  SearchResultProto search_result_google::protobuf =
      icing.Search(search_spec2, ScoringSpecProto::default_instance(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_google::protobuf, EqualsSearchResultIgnoreStatsAndScores(
                                        expected_search_result_google::protobuf));
}

TEST_F(IcingSearchEngineInitializationTest,
       RecoverFromInconsistentDocumentStore) {
  // Test the following scenario: document store is ahead of term, integer and
  // qualified id join index. IcingSearchEngine should be able to recover all
  // indices. Several additional behaviors are also tested:
  // - Index directory handling:
  //   - Term index directory should be unaffected.
  //   - Integer index directory should be unaffected.
  //   - Qualified id join index directory should be unaffected.
  // - Truncate indices:
  //   - "TruncateTo()" for term index shouldn't take effect.
  //   - "Clear()" shouldn't be called for integer index, i.e. no integer index
  //     storage sub directories (path_expr = "*/integer_index_dir/*") should be
  //     discarded.
  //   - "Clear()" shouldn't be called for qualified id join index, i.e. no
  //     underlying storage sub directory (path_expr =
  //     "*/qualified_id_join_index_dir/*") should be discarded.
  // - Still, we need to replay and reindex documents.

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("indexableInteger")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("senderQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  DocumentProto person =
      DocumentBuilder()
          .SetKey("namespace", "person")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message1 =
      DocumentBuilder()
          .SetKey("namespace", "message/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message body one")
          .AddInt64Property("indexableInteger", 123)
          .AddStringProperty("senderQualifiedId", "namespace#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message2 =
      DocumentBuilder()
          .SetKey("namespace", "message/2")
          .SetSchema("Message")
          .AddStringProperty("body", "message body two")
          .AddInt64Property("indexableInteger", 123)
          .AddStringProperty("senderQualifiedId", "namespace#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();

  {
    // Initializes folder and schema, index one document
    TestIcingSearchEngine icing(icing_options, std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());
    EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
    EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(person).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(message1).status(), ProtoIsOk());
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  {
    FakeClock fake_clock;
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(filesystem(), GetSchemaDir(), &fake_clock,
                            feature_flags_.get()));

    ICING_ASSERT_OK_AND_ASSIGN(
        BlobStore blob_store,
        BlobStore::Create(filesystem(), GetBlobDir(), &fake_clock,
                          /*orphan_blob_time_to_live_ms=*/0,
                          PortableFileBackedProtoLog<
                              BlobInfoProto>::kDefaultCompressionLevel));

    // Puts message2 into DocumentStore but doesn't index it.
    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(filesystem(), GetDocumentDir(), &fake_clock,
                              schema_store.get(), feature_flags_.get(),
                              /*force_recovery_and_revalidate_documents=*/false,
                              /*pre_mapping_fbv=*/false,
                              /*use_persistent_hash_map=*/true,
                              PortableFileBackedProtoLog<
                                  DocumentWrapper>::kDefaultCompressionLevel,
                              /*initialize_stats=*/nullptr));
    std::unique_ptr<DocumentStore> document_store =
        std::move(create_result.document_store);

    ICING_EXPECT_OK(document_store->Put(message2));
  }

  // Mock filesystem to observe and check the behavior of all indices.
  auto mock_filesystem = std::make_unique<MockFilesystem>();
  EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
      .WillRepeatedly(DoDefault());
  // Ensure term index directory should never be discarded.
  EXPECT_CALL(*mock_filesystem,
              DeleteDirectoryRecursively(EndsWith("/index_dir")))
      .Times(0);
  // Ensure integer index directory should never be discarded, and Clear()
  // should never be called (i.e. storage sub directory
  // "*/integer_index_dir/*" should never be discarded).
  EXPECT_CALL(*mock_filesystem,
              DeleteDirectoryRecursively(EndsWith("/integer_index_dir")))
      .Times(0);
  EXPECT_CALL(*mock_filesystem,
              DeleteDirectoryRecursively(HasSubstr("/integer_index_dir/")))
      .Times(0);
  // Ensure qualified id join index directory should never be discarded, and
  // Clear() should never be called (i.e. storage sub directory
  // "*/qualified_id_join_index_dir/*" should never be discarded).
  EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(
                                    EndsWith("/qualified_id_join_index_dir")))
      .Times(0);
  EXPECT_CALL(*mock_filesystem, DeleteFile(_)).WillRepeatedly(DoDefault());
  EXPECT_CALL(*mock_filesystem,
              DeleteFile(HasSubstr("/qualified_id_join_index_dir/")))
      .Times(0);

  TestIcingSearchEngine icing(icing_options, std::move(mock_filesystem),
                              std::make_unique<IcingFilesystem>(),
                              std::make_unique<FakeClock>(), GetTestJniCache());
  InitializeResultProto initialize_result = icing.Initialize();
  EXPECT_THAT(initialize_result.status(), ProtoIsOk());
  // Index Restoration should be triggered here and document2 should be
  // indexed.
  EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
              Eq(InitializeStatsProto::INCONSISTENT_WITH_GROUND_TRUTH));
  EXPECT_THAT(
      initialize_result.initialize_stats().integer_index_restoration_cause(),
      Eq(InitializeStatsProto::INCONSISTENT_WITH_GROUND_TRUTH));
  EXPECT_THAT(initialize_result.initialize_stats()
                  .qualified_id_join_index_restoration_cause(),
              Eq(InitializeStatsProto::INCONSISTENT_WITH_GROUND_TRUTH));

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = message1;

  // DocumentStore kept the additional document
  EXPECT_THAT(icing.Get("namespace", "message/1",
                        GetResultSpecProto::default_instance()),
              EqualsProto(expected_get_result_proto));

  *expected_get_result_proto.mutable_document() = message2;
  EXPECT_THAT(icing.Get("namespace", "message/2",
                        GetResultSpecProto::default_instance()),
              EqualsProto(expected_get_result_proto));

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      message2;
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      message1;

  // We indexed the additional document in all indices.
  // Verify term search
  SearchSpecProto search_spec1;
  search_spec1.set_query("message");
  search_spec1.set_term_match_type(TermMatchType::EXACT_ONLY);
  SearchResultProto search_result_proto1 =
      icing.Search(search_spec1, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto1, EqualsSearchResultIgnoreStatsAndScores(
                                        expected_search_result_proto));

  // Verify numeric (integer) search
  SearchSpecProto search_spec2;
  search_spec2.set_query("indexableInteger == 123");
  search_spec2.add_enabled_features(std::string(kNumericSearchFeature));

  SearchResultProto search_result_google::protobuf =
      icing.Search(search_spec2, ScoringSpecProto::default_instance(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_google::protobuf, EqualsSearchResultIgnoreStatsAndScores(
                                        expected_search_result_proto));

  // Verify join search: join a query for `name:person` with a child query for
  // `body:message` based on the child's `senderQualifiedId` field.
  SearchSpecProto search_spec3;
  search_spec3.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec3.set_query("name:person");
  JoinSpecProto* join_spec = search_spec3.mutable_join_spec();
  join_spec->set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec->set_child_property_expression("senderQualifiedId");
  join_spec->set_aggregation_scoring_strategy(
      JoinSpecProto::AggregationScoringStrategy::COUNT);
  JoinSpecProto::NestedSpecProto* nested_spec =
      join_spec->mutable_nested_spec();
  SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
  nested_search_spec->set_term_match_type(TermMatchType::EXACT_ONLY);
  nested_search_spec->set_query("body:message");
  *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();
  *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

  ResultSpecProto result_spec3 = ResultSpecProto::default_instance();
  result_spec3.set_max_joined_children_per_parent_to_return(
      std::numeric_limits<int32_t>::max());

  SearchResultProto expected_join_search_result_proto;
  expected_join_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  SearchResultProto::ResultProto* result_proto =
      expected_join_search_result_proto.mutable_results()->Add();
  *result_proto->mutable_document() = person;
  *result_proto->mutable_joined_results()->Add()->mutable_document() = message2;
  *result_proto->mutable_joined_results()->Add()->mutable_document() = message1;

  SearchResultProto search_result_proto3 = icing.Search(
      search_spec3, ScoringSpecProto::default_instance(), result_spec3);
  EXPECT_THAT(search_result_proto3, EqualsSearchResultIgnoreStatsAndScores(
                                        expected_join_search_result_proto));
}

TEST_F(IcingSearchEngineInitializationTest, RecoverFromCorruptedDocumentStore) {
  // Test the following scenario: some document store derived files are
  // corrupted. IcingSearchEngine should be able to recover the document store.
  // Several additional behaviors are also tested:
  // - Index directory handling:
  //   - Term index directory should be unaffected.
  //   - Integer index directory should be unaffected.
  //   - Qualified id join index directory should be unaffected.
  // - Truncate indices:
  //   - "TruncateTo()" for term index shouldn't take effect.
  //   - "Clear()" shouldn't be called for integer index, i.e. no integer index
  //     storage sub directories (path_expr = "*/integer_index_dir/*") should be
  //     discarded.
  //   - "Clear()" shouldn't be called for qualified id join index, i.e. no
  //     underlying storage sub directory (path_expr =
  //     "*/qualified_id_join_index_dir/*") should be discarded.
  // - Still, we need to replay and reindex documents (for qualified id join
  //   index).

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("indexableInteger")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("senderQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  DocumentProto personDummy =
      DocumentBuilder()
          .SetKey("namespace2", "personDummy")
          .SetSchema("Person")
          .AddStringProperty("name", "personDummy")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto person1 =
      DocumentBuilder()
          .SetKey("namespace1", "person")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto person2 =
      DocumentBuilder()
          .SetKey("namespace2", "person")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message =
      DocumentBuilder()
          .SetKey("namespace2", "message/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message body one")
          .AddInt64Property("indexableInteger", 123)
          .AddStringProperty("senderQualifiedId", "namespace2#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();

  {
    // Initializes folder and schema, index one document
    TestIcingSearchEngine icing(icing_options, std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());
    EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
    EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    // "namespace2" (in personDummy) will be assigned NamespaceId = 0.
    EXPECT_THAT(icing.Put(personDummy).status(), ProtoIsOk());
    // "namespace1" (in person1) will be assigned NamespaceId = 1.
    EXPECT_THAT(icing.Put(person1).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(person2).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());

    // Now delete personDummy.
    EXPECT_THAT(
        icing.Delete(personDummy.namespace_(), personDummy.uri()).status(),
        ProtoIsOk());
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  {
    FakeClock fake_clock;
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(filesystem(), GetSchemaDir(), &fake_clock,
                            feature_flags_.get()));

    // Manually corrupt one of the derived files of DocumentStore without
    // updating checksum in DocumentStore header.
    std::string score_cache_filename = GetDocumentDir() + "/score_cache";
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<FileBackedVector<DocumentAssociatedScoreData>>
            score_cache,
        FileBackedVector<DocumentAssociatedScoreData>::Create(
            *filesystem(), std::move(score_cache_filename),
            MemoryMappedFile::READ_WRITE_AUTO_SYNC));
    ICING_ASSERT_OK_AND_ASSIGN(const DocumentAssociatedScoreData* score_data,
                               score_cache->Get(/*idx=*/0));
    ICING_ASSERT_OK(score_cache->Set(
        /*idx=*/0,
        DocumentAssociatedScoreData(score_data->corpus_id(),
                                    score_data->document_score() + 1,
                                    score_data->creation_timestamp_ms(),
                                    score_data->length_in_tokens())));
    ICING_ASSERT_OK(score_cache->PersistToDisk());
  }

  // Mock filesystem to observe and check the behavior of all indices.
  auto mock_filesystem = std::make_unique<MockFilesystem>();
  EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
      .WillRepeatedly(DoDefault());
  // Ensure term index directory should never be discarded.
  EXPECT_CALL(*mock_filesystem,
              DeleteDirectoryRecursively(EndsWith("/index_dir")))
      .Times(0);
  // Ensure integer index directory should never be discarded, and Clear()
  // should never be called (i.e. storage sub directory
  // "*/integer_index_dir/*" should never be discarded).
  EXPECT_CALL(*mock_filesystem,
              DeleteDirectoryRecursively(EndsWith("/integer_index_dir")))
      .Times(0);
  EXPECT_CALL(*mock_filesystem,
              DeleteDirectoryRecursively(HasSubstr("/integer_index_dir/")))
      .Times(0);
  // Ensure qualified id join index directory should never be discarded, and
  // Clear() should never be called (i.e. storage sub directory
  // "*/qualified_id_join_index_dir/*" should never be discarded).
  EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(
                                    EndsWith("/qualified_id_join_index_dir")))
      .Times(0);
  EXPECT_CALL(*mock_filesystem, DeleteFile(_)).WillRepeatedly(DoDefault());
  EXPECT_CALL(*mock_filesystem,
              DeleteFile(HasSubstr("/qualified_id_join_index_dir/")))
      .Times(0);

  TestIcingSearchEngine icing(icing_options, std::move(mock_filesystem),
                              std::make_unique<IcingFilesystem>(),
                              std::make_unique<FakeClock>(), GetTestJniCache());
  InitializeResultProto initialize_result = icing.Initialize();
  EXPECT_THAT(initialize_result.status(), ProtoIsOk());
  // DocumentStore should be recovered. When reassigning NamespaceId, the order
  // will be the document traversal order: [person1, person2, message].
  // Therefore, "namespace1" will have id = 0 and "namespace2" will have id = 1.
  EXPECT_THAT(
      initialize_result.initialize_stats().document_store_recovery_cause(),
      Eq(InitializeStatsProto::IO_ERROR));
  // Term, integer index and qualified id join index should be unaffected.
  EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
              Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(
      initialize_result.initialize_stats().integer_index_restoration_cause(),
      Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result.initialize_stats()
                  .qualified_id_join_index_restoration_cause(),
              Eq(InitializeStatsProto::NONE));

  // Verify join search: join a query for `name:person` with a child query for
  // `body:message` based on the child's `senderQualifiedId` field. message2
  // should be joined to person2 correctly.
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_query("name:person");
  JoinSpecProto* join_spec = search_spec.mutable_join_spec();
  join_spec->set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec->set_child_property_expression("senderQualifiedId");
  join_spec->set_aggregation_scoring_strategy(
      JoinSpecProto::AggregationScoringStrategy::COUNT);
  JoinSpecProto::NestedSpecProto* nested_spec =
      join_spec->mutable_nested_spec();
  SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
  nested_search_spec->set_term_match_type(TermMatchType::EXACT_ONLY);
  nested_search_spec->set_query("body:message");
  *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();
  *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

  ResultSpecProto result_spec = ResultSpecProto::default_instance();
  result_spec.set_max_joined_children_per_parent_to_return(
      std::numeric_limits<int32_t>::max());

  SearchResultProto expected_join_search_result_proto;
  expected_join_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  SearchResultProto::ResultProto* result_proto =
      expected_join_search_result_proto.mutable_results()->Add();
  *result_proto->mutable_document() = person2;
  *result_proto->mutable_joined_results()->Add()->mutable_document() = message;

  *expected_join_search_result_proto.mutable_results()
       ->Add()
       ->mutable_document() = person1;

  SearchResultProto search_result_proto = icing.Search(
      search_spec, ScoringSpecProto::default_instance(), result_spec);
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_join_search_result_proto));
}

TEST_F(IcingSearchEngineInitializationTest, RecoverFromCorruptIndex) {
  // Test the following scenario: term index is corrupted (e.g. checksum doesn't
  // match). IcingSearchEngine should be able to recover term index. Several
  // additional behaviors are also tested:
  // - Index directory handling:
  //   - Should discard the entire term index directory and start it from
  //     scratch.
  //   - Integer index directory should be unaffected.
  //   - Qualified id join index directory should be unaffected.
  // - Truncate indices:
  //   - "TruncateTo()" for term index shouldn't take effect since we start it
  //     from scratch.
  //   - "Clear()" shouldn't be called for integer index, i.e. no integer index
  //     storage sub directories (path_expr = "*/integer_index_dir/*") should be
  //     discarded.
  //   - "Clear()" shouldn't be called for qualified id join index, i.e. no
  //     underlying storage sub directory (path_expr =
  //     "*/qualified_id_join_index_dir/*") should be discarded.

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("indexableInteger")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("senderQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  DocumentProto person =
      DocumentBuilder()
          .SetKey("namespace", "person")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message =
      DocumentBuilder()
          .SetKey("namespace", "message/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message body")
          .AddInt64Property("indexableInteger", 123)
          .AddStringProperty("senderQualifiedId", "namespace#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  SearchSpecProto search_spec;
  search_spec.set_query("body:message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      message;

  {
    // Initializes folder and schema, index one document
    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::make_unique<Filesystem>(),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());
    EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
    EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(person).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
    SearchResultProto search_result_proto =
        icing.Search(search_spec, GetDefaultScoringSpec(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                         expected_search_result_proto));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  // Manually corrupt term index
  {
    const std::string index_hit_buffer_file = GetIndexDir() + "/idx/lite.hb";
    ScopedFd fd(filesystem()->OpenForWrite(index_hit_buffer_file.c_str()));
    ASSERT_TRUE(fd.is_valid());
    ASSERT_TRUE(filesystem()->Write(fd.get(), "1234", 4));
  }

  // Mock filesystem to observe and check the behavior of all indices.
  auto mock_filesystem = std::make_unique<MockFilesystem>();
  EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
      .WillRepeatedly(DoDefault());
  // Ensure term index directory should be discarded once.
  EXPECT_CALL(*mock_filesystem,
              DeleteDirectoryRecursively(EndsWith("/index_dir")))
      .Times(1);
  // Ensure integer index directory should never be discarded, and Clear()
  // should never be called (i.e. storage sub directory "*/integer_index_dir/*"
  // should never be discarded).
  EXPECT_CALL(*mock_filesystem,
              DeleteDirectoryRecursively(EndsWith("/integer_index_dir")))
      .Times(0);
  EXPECT_CALL(*mock_filesystem,
              DeleteDirectoryRecursively(HasSubstr("/integer_index_dir/")))
      .Times(0);
  // Ensure qualified id join index directory should never be discarded, and
  // Clear() should never be called (i.e. storage sub directory
  // "*/qualified_id_join_index_dir/*" should never be discarded).
  EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(
                                    EndsWith("/qualified_id_join_index_dir")))
      .Times(0);
  EXPECT_CALL(*mock_filesystem, DeleteFile(_)).WillRepeatedly(DoDefault());
  EXPECT_CALL(*mock_filesystem,
              DeleteFile(HasSubstr("/qualified_id_join_index_dir/")))
      .Times(0);

  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::move(mock_filesystem),
                              std::make_unique<IcingFilesystem>(),
                              std::make_unique<FakeClock>(), GetTestJniCache());
  InitializeResultProto initialize_result = icing.Initialize();
  EXPECT_THAT(initialize_result.status(), ProtoIsOk());
  EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
              Eq(InitializeStatsProto::IO_ERROR));
  EXPECT_THAT(
      initialize_result.initialize_stats().integer_index_restoration_cause(),
      Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result.initialize_stats()
                  .qualified_id_join_index_restoration_cause(),
              Eq(InitializeStatsProto::NONE));

  // Check that our index is ok by searching over the restored index
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineInitializationTest, RecoverFromCorruptIntegerIndex) {
  // Test the following scenario: integer index is corrupted (e.g. checksum
  // doesn't match). IcingSearchEngine should be able to recover integer index.
  // Several additional behaviors are also tested:
  // - Index directory handling:
  //   - Term index directory should be unaffected.
  //   - Should discard the entire integer index directory and start it from
  //     scratch.
  //   - Qualified id join index directory should be unaffected.
  // - Truncate indices:
  //   - "TruncateTo()" for term index shouldn't take effect.
  //   - "Clear()" shouldn't be called for integer index, i.e. no integer index
  //     storage sub directories (path_expr = "*/integer_index_dir/*") should be
  //     discarded, since we start it from scratch.
  //   - "Clear()" shouldn't be called for qualified id join index, i.e. no
  //     underlying storage sub directory (path_expr =
  //     "*/qualified_id_join_index_dir/*") should be discarded.

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("indexableInteger")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("senderQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  DocumentProto person =
      DocumentBuilder()
          .SetKey("namespace", "person")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message =
      DocumentBuilder()
          .SetKey("namespace", "message/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message body")
          .AddInt64Property("indexableInteger", 123)
          .AddStringProperty("senderQualifiedId", "namespace#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  SearchSpecProto search_spec;
  search_spec.set_query("indexableInteger == 123");
  search_spec.add_enabled_features(std::string(kNumericSearchFeature));

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      message;

  {
    // Initializes folder and schema, index one document
    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::make_unique<Filesystem>(),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());
    EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
    EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(person).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
    SearchResultProto search_result_proto =
        icing.Search(search_spec, GetDefaultScoringSpec(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                         expected_search_result_proto));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  // Manually corrupt integer index
  {
    const std::string integer_index_metadata_file =
        GetIntegerIndexDir() + "/integer_index.m";
    ScopedFd fd(
        filesystem()->OpenForWrite(integer_index_metadata_file.c_str()));
    ASSERT_TRUE(fd.is_valid());
    ASSERT_TRUE(filesystem()->Write(fd.get(), "1234", 4));
  }

  // Mock filesystem to observe and check the behavior of all indices.
  auto mock_filesystem = std::make_unique<MockFilesystem>();
  EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
      .WillRepeatedly(DoDefault());
  // Ensure term index directory should never be discarded.
  EXPECT_CALL(*mock_filesystem,
              DeleteDirectoryRecursively(EndsWith("/index_dir")))
      .Times(0);
  // Ensure integer index directory should be discarded once, and Clear()
  // should never be called (i.e. storage sub directory "*/integer_index_dir/*"
  // should never be discarded) since we start it from scratch.
  EXPECT_CALL(*mock_filesystem,
              DeleteDirectoryRecursively(EndsWith("/integer_index_dir")))
      .Times(1);
  EXPECT_CALL(*mock_filesystem,
              DeleteDirectoryRecursively(HasSubstr("/integer_index_dir/")))
      .Times(0);
  // Ensure qualified id join index directory should never be discarded, and
  // Clear() should never be called (i.e. storage sub directory
  // "*/qualified_id_join_index_dir/*" should never be discarded).
  EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(
                                    EndsWith("/qualified_id_join_index_dir")))
      .Times(0);
  EXPECT_CALL(*mock_filesystem, DeleteFile(_)).WillRepeatedly(DoDefault());
  EXPECT_CALL(*mock_filesystem,
              DeleteFile(HasSubstr("/qualified_id_join_index_dir/")))
      .Times(0);

  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::move(mock_filesystem),
                              std::make_unique<IcingFilesystem>(),
                              std::make_unique<FakeClock>(), GetTestJniCache());
  InitializeResultProto initialize_result = icing.Initialize();
  EXPECT_THAT(initialize_result.status(), ProtoIsOk());
  EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
              Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(
      initialize_result.initialize_stats().integer_index_restoration_cause(),
      Eq(InitializeStatsProto::IO_ERROR));
  EXPECT_THAT(initialize_result.initialize_stats()
                  .qualified_id_join_index_restoration_cause(),
              Eq(InitializeStatsProto::NONE));

  // Check that our index is ok by searching over the restored index
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineInitializationTest,
       RecoverFromIntegerIndexBucketSplitThresholdChange) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Message").AddProperty(
              PropertyConfigBuilder()
                  .SetName("indexableInteger")
                  .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  DocumentProto message =
      DocumentBuilder()
          .SetKey("namespace", "message/1")
          .SetSchema("Message")
          .AddInt64Property("indexableInteger", 123)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  // 1. Create an index with a message document.
  {
    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::make_unique<Filesystem>(),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
  }

  // 2. Create the index again with different
  //    integer_index_bucket_split_threshold. This should trigger index
  //    restoration.
  {
    // Mock filesystem to observe and check the behavior of all indices.
    auto mock_filesystem = std::make_unique<MockFilesystem>();
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
        .WillRepeatedly(DoDefault());
    // Ensure term index directory should never be discarded.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/index_dir")))
        .Times(0);
    // Ensure integer index directory should be discarded once, and Clear()
    // should never be called (i.e. storage sub directory
    // "*/integer_index_dir/*" should never be discarded) since we start it from
    // scratch.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/integer_index_dir")))
        .Times(1);
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(HasSubstr("/integer_index_dir/")))
        .Times(0);
    // Ensure qualified id join index directory should never be discarded, and
    // Clear() should never be called (i.e. storage sub directory
    // "*/qualified_id_join_index_dir/*" should never be discarded).
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(
                                      EndsWith("/qualified_id_join_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem, DeleteFile(_)).WillRepeatedly(DoDefault());
    EXPECT_CALL(*mock_filesystem,
                DeleteFile(HasSubstr("/qualified_id_join_index_dir/")))
        .Times(0);

    static constexpr int32_t kNewIntegerIndexBucketSplitThreshold = 1000;
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    ASSERT_THAT(kNewIntegerIndexBucketSplitThreshold,
                Ne(options.integer_index_bucket_split_threshold()));
    options.set_integer_index_bucket_split_threshold(
        kNewIntegerIndexBucketSplitThreshold);

    TestIcingSearchEngine icing(options, std::move(mock_filesystem),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());
    InitializeResultProto initialize_result = icing.Initialize();
    ASSERT_THAT(initialize_result.status(), ProtoIsOk());
    EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(
        initialize_result.initialize_stats().integer_index_restoration_cause(),
        Eq(InitializeStatsProto::IO_ERROR));
    EXPECT_THAT(initialize_result.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));

    // Verify integer index works normally
    SearchSpecProto search_spec;
    search_spec.set_query("indexableInteger == 123");
    search_spec.add_enabled_features(std::string(kNumericSearchFeature));

    SearchResultProto results =
        icing.Search(search_spec, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    ASSERT_THAT(results.results(), SizeIs(1));
    EXPECT_THAT(results.results(0).document().uri(), Eq("message/1"));
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       RecoverFromCorruptQualifiedIdJoinIndex) {
  // Test the following scenario: qualified id join index is corrupted (e.g.
  // checksum doesn't match). IcingSearchEngine should be able to recover
  // qualified id join index. Several additional behaviors are also tested:
  // - Index directory handling:
  //   - Term index directory should be unaffected.
  //   - Integer index directory should be unaffected.
  //   - Should discard the entire qualified id join index directory and start
  //     it from scratch.
  // - Truncate indices:
  //   - "TruncateTo()" for term index shouldn't take effect.
  //   - "Clear()" shouldn't be called for integer index, i.e. no integer index
  //     storage sub directories (path_expr = "*/integer_index_dir/*") should be
  //     discarded.
  //   - "Clear()" shouldn't be called for qualified id join index, i.e. no
  //     underlying storage sub directory (path_expr =
  //     "*/qualified_id_join_index_dir/*") should be discarded, since we start
  //     it from scratch.

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("indexableInteger")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("senderQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  DocumentProto person =
      DocumentBuilder()
          .SetKey("namespace", "person")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message =
      DocumentBuilder()
          .SetKey("namespace", "message/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message body")
          .AddInt64Property("indexableInteger", 123)
          .AddStringProperty("senderQualifiedId", "namespace#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  // Prepare join search spec to join a query for `name:person` with a child
  // query for `body:message` based on the child's `senderQualifiedId` field.
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.set_query("name:person");
  JoinSpecProto* join_spec = search_spec.mutable_join_spec();
  join_spec->set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec->set_child_property_expression("senderQualifiedId");
  join_spec->set_aggregation_scoring_strategy(
      JoinSpecProto::AggregationScoringStrategy::COUNT);
  JoinSpecProto::NestedSpecProto* nested_spec =
      join_spec->mutable_nested_spec();
  SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
  nested_search_spec->set_term_match_type(TermMatchType::EXACT_ONLY);
  nested_search_spec->set_query("body:message");
  *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();
  *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

  ResultSpecProto result_spec = ResultSpecProto::default_instance();
  result_spec.set_max_joined_children_per_parent_to_return(
      std::numeric_limits<int32_t>::max());

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  SearchResultProto::ResultProto* result_proto =
      expected_search_result_proto.mutable_results()->Add();
  *result_proto->mutable_document() = person;
  *result_proto->mutable_joined_results()->Add()->mutable_document() = message;

  {
    // Initializes folder and schema, index one document
    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::make_unique<Filesystem>(),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());
    EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
    EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(person).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
    SearchResultProto search_result_proto =
        icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
    EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                         expected_search_result_proto));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  // Manually corrupt qualified id join index
  {
    const std::string qualified_id_join_index_metadata_file =
        GetQualifiedIdJoinIndexDir() + "/metadata";
    ScopedFd fd(filesystem()->OpenForWrite(
        qualified_id_join_index_metadata_file.c_str()));
    ASSERT_TRUE(fd.is_valid());
    ASSERT_TRUE(filesystem()->Write(fd.get(), "1234", 4));
  }

  // Mock filesystem to observe and check the behavior of all indices.
  auto mock_filesystem = std::make_unique<MockFilesystem>();
  EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
      .WillRepeatedly(DoDefault());
  // Ensure term index directory should never be discarded.
  EXPECT_CALL(*mock_filesystem,
              DeleteDirectoryRecursively(EndsWith("/index_dir")))
      .Times(0);
  // Ensure integer index directory should never be discarded, and Clear()
  // should never be called (i.e. storage sub directory "*/integer_index_dir/*"
  // should never be discarded).
  EXPECT_CALL(*mock_filesystem,
              DeleteDirectoryRecursively(EndsWith("/integer_index_dir")))
      .Times(0);
  EXPECT_CALL(*mock_filesystem,
              DeleteDirectoryRecursively(HasSubstr("/integer_index_dir/")))
      .Times(0);
  // Ensure qualified id join index directory should be discarded once, and
  // Clear() should never be called (i.e. storage sub directory
  // "*/qualified_id_join_index_dir/*" should never be discarded).
  EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(
                                    EndsWith("/qualified_id_join_index_dir")))
      .Times(1);
  EXPECT_CALL(*mock_filesystem, DeleteFile(_)).WillRepeatedly(DoDefault());
  EXPECT_CALL(*mock_filesystem,
              DeleteFile(HasSubstr("/qualified_id_join_index_dir/")))
      .Times(0);

  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::move(mock_filesystem),
                              std::make_unique<IcingFilesystem>(),
                              std::make_unique<FakeClock>(), GetTestJniCache());
  InitializeResultProto initialize_result = icing.Initialize();
  EXPECT_THAT(initialize_result.status(), ProtoIsOk());
  EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
              Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(
      initialize_result.initialize_stats().integer_index_restoration_cause(),
      Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result.initialize_stats()
                  .qualified_id_join_index_restoration_cause(),
              Eq(InitializeStatsProto::IO_ERROR));

  // Check that our index is ok by searching over the restored index
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineInitializationTest, RestoreIndexLoseTermIndex) {
  // Test the following scenario: losing the entire term index. Since we need
  // flash index magic to determine the version, in this test we will throw out
  // the entire term index and re-initialize an empty one, to bypass
  // undetermined version state change and correctly trigger "lose term index"
  // scenario.
  // IcingSearchEngine should be able to recover term index. Several additional
  // behaviors are also tested:
  // - Index directory handling:
  //   - Term index directory should not be discarded (but instead just being
  //     rebuilt by replaying all docs).
  //   - Integer index directory should be unaffected.
  //   - Qualified id join index directory should be unaffected.
  // - Truncate indices:
  //   - "TruncateTo()" for term index shouldn't take effect since it is empty.
  //   - "Clear()" shouldn't be called for integer index, i.e. no integer index
  //     storage sub directories (path_expr = "*/integer_index_dir/*") should be
  //     discarded.
  //   - "Clear()" shouldn't be called for qualified id join index, i.e. no
  //     underlying storage sub directory (path_expr =
  //     "*/qualified_id_join_index_dir/*") should be discarded.

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("indexableInteger")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("senderQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  DocumentProto person =
      DocumentBuilder()
          .SetKey("namespace", "person")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message =
      DocumentBuilder()
          .SetKey("namespace", "message/1")
          .SetSchema("Message")
          .AddStringProperty("body", kIpsumText)
          .AddInt64Property("indexableInteger", 123)
          .AddStringProperty("senderQualifiedId", "namespace#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  // 1. Create an index with 3 message documents.
  {
    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::make_unique<Filesystem>(),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    EXPECT_THAT(icing.Put(person).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
    message = DocumentBuilder(message).SetUri("message/2").Build();
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
    message = DocumentBuilder(message).SetUri("message/3").Build();
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
  }

  // 2. Delete and re-initialize an empty term index to trigger
  // RestoreIndexIfNeeded.
  {
    std::string idx_subdir = GetIndexDir() + "/idx";
    ASSERT_TRUE(filesystem()->DeleteDirectoryRecursively(idx_subdir.c_str()));
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Index> index,
        Index::Create(Index::Options(GetIndexDir(),
                                     /*index_merge_size=*/100,
                                     /*lite_index_sort_at_indexing=*/true,
                                     /*lite_index_sort_size=*/50),
                      filesystem(), icing_filesystem()));
    ICING_ASSERT_OK(index->PersistToDisk());
  }

  // 3. Create the index again. This should trigger index restoration.
  {
    // Mock filesystem to observe and check the behavior of all indices.
    auto mock_filesystem = std::make_unique<MockFilesystem>();
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
        .WillRepeatedly(DoDefault());
    // Ensure term index directory should never be discarded since we've already
    // lost it.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/index_dir")))
        .Times(0);
    // Ensure integer index directory should never be discarded, and Clear()
    // should never be called (i.e. storage sub directory
    // "*/integer_index_dir/*" should never be discarded).
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/integer_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(HasSubstr("/integer_index_dir/")))
        .Times(0);
    // Ensure qualified id join index directory should never be discarded, and
    // Clear() should never be called (i.e. storage sub directory
    // "*/qualified_id_join_index_dir/*" should never be discarded).
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(
                                      EndsWith("/qualified_id_join_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem, DeleteFile(_)).WillRepeatedly(DoDefault());
    EXPECT_CALL(*mock_filesystem,
                DeleteFile(HasSubstr("/qualified_id_join_index_dir/")))
        .Times(0);

    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::move(mock_filesystem),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());
    InitializeResultProto initialize_result = icing.Initialize();
    ASSERT_THAT(initialize_result.status(), ProtoIsOk());
    EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
                Eq(InitializeStatsProto::INCONSISTENT_WITH_GROUND_TRUTH));
    EXPECT_THAT(
        initialize_result.initialize_stats().integer_index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));

    // Verify term index works normally
    SearchSpecProto search_spec1;
    search_spec1.set_query("body:consectetur");
    search_spec1.set_term_match_type(TermMatchType::EXACT_ONLY);
    SearchResultProto results1 =
        icing.Search(search_spec1, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(results1.status(), ProtoIsOk());
    EXPECT_THAT(results1.next_page_token(), Eq(0));
    // All documents should be retrievable.
    ASSERT_THAT(results1.results(), SizeIs(3));
    EXPECT_THAT(results1.results(0).document().uri(), Eq("message/3"));
    EXPECT_THAT(results1.results(1).document().uri(), Eq("message/2"));
    EXPECT_THAT(results1.results(2).document().uri(), Eq("message/1"));

    // Verify integer index works normally
    SearchSpecProto search_spec2;
    search_spec2.set_query("indexableInteger == 123");
    search_spec2.add_enabled_features(std::string(kNumericSearchFeature));

    SearchResultProto results2 =
        icing.Search(search_spec2, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    ASSERT_THAT(results2.results(), SizeIs(3));
    EXPECT_THAT(results2.results(0).document().uri(), Eq("message/3"));
    EXPECT_THAT(results2.results(1).document().uri(), Eq("message/2"));
    EXPECT_THAT(results2.results(2).document().uri(), Eq("message/1"));

    // Verify qualified id join index works normally: join a query for
    // `name:person` with a child query for `body:consectetur` based on the
    // child's `senderQualifiedId` field.
    SearchSpecProto search_spec3;
    search_spec3.set_term_match_type(TermMatchType::EXACT_ONLY);
    search_spec3.set_query("name:person");
    JoinSpecProto* join_spec = search_spec3.mutable_join_spec();
    join_spec->set_parent_property_expression(
        std::string(JoinProcessor::kQualifiedIdExpr));
    join_spec->set_child_property_expression("senderQualifiedId");
    join_spec->set_aggregation_scoring_strategy(
        JoinSpecProto::AggregationScoringStrategy::COUNT);
    JoinSpecProto::NestedSpecProto* nested_spec =
        join_spec->mutable_nested_spec();
    SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
    nested_search_spec->set_term_match_type(TermMatchType::EXACT_ONLY);
    nested_search_spec->set_query("body:consectetur");
    *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();
    *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

    ResultSpecProto result_spec3 = ResultSpecProto::default_instance();
    result_spec3.set_max_joined_children_per_parent_to_return(
        std::numeric_limits<int32_t>::max());

    SearchResultProto results3 = icing.Search(
        search_spec3, ScoringSpecProto::default_instance(), result_spec3);
    ASSERT_THAT(results3.results(), SizeIs(1));
    EXPECT_THAT(results3.results(0).document().uri(), Eq("person"));
    EXPECT_THAT(results3.results(0).joined_results(), SizeIs(3));
    EXPECT_THAT(results3.results(0).joined_results(0).document().uri(),
                Eq("message/3"));
    EXPECT_THAT(results3.results(0).joined_results(1).document().uri(),
                Eq("message/2"));
    EXPECT_THAT(results3.results(0).joined_results(2).document().uri(),
                Eq("message/1"));
  }
}

TEST_F(IcingSearchEngineInitializationTest, RestoreIndexLoseIntegerIndex) {
  // Test the following scenario: losing the entire integer index directory.
  // IcingSearchEngine should be able to recover integer index. Several
  // additional behaviors are also tested:
  // - Index directory handling:
  //   - Term index directory should be unaffected.
  //   - Integer index directory should not be discarded since we've already
  //     lost it. Start it from scratch.
  //   - Qualified id join index directory should be unaffected.
  // - Truncate indices:
  //   - "TruncateTo()" for term index shouldn't take effect.
  //   - "Clear()" shouldn't be called for integer index, i.e. no integer index
  //     storage sub directories (path_expr = "*/integer_index_dir/*") should be
  //     discarded, since we start it from scratch.
  //   - "Clear()" shouldn't be called for qualified id join index, i.e. no
  //     underlying storage sub directory (path_expr =
  //     "*/qualified_id_join_index_dir/*") should be discarded.

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("indexableInteger")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("senderQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  DocumentProto person =
      DocumentBuilder()
          .SetKey("namespace", "person")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message =
      DocumentBuilder()
          .SetKey("namespace", "message/1")
          .SetSchema("Message")
          .AddStringProperty("body", kIpsumText)
          .AddInt64Property("indexableInteger", 123)
          .AddStringProperty("senderQualifiedId", "namespace#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  // 1. Create an index with 3 message documents.
  {
    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::make_unique<Filesystem>(),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    EXPECT_THAT(icing.Put(person).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
    message = DocumentBuilder(message).SetUri("message/2").Build();
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
    message = DocumentBuilder(message).SetUri("message/3").Build();
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
  }

  // 2. Delete the integer index file to trigger RestoreIndexIfNeeded.
  std::string integer_index_dir = GetIntegerIndexDir();
  filesystem()->DeleteDirectoryRecursively(integer_index_dir.c_str());

  // 3. Create the index again. This should trigger index restoration.
  {
    // Mock filesystem to observe and check the behavior of all indices.
    auto mock_filesystem = std::make_unique<MockFilesystem>();
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
        .WillRepeatedly(DoDefault());
    // Ensure term index directory should never be discarded.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/index_dir")))
        .Times(0);
    // Ensure integer index directory should never be discarded since we've
    // already lost it, and Clear() should never be called (i.e. storage sub
    // directory "*/integer_index_dir/*" should never be discarded) since we
    // start it from scratch.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/integer_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(HasSubstr("/integer_index_dir/")))
        .Times(0);
    // Ensure qualified id join index directory should never be discarded, and
    // Clear() should never be called (i.e. storage sub directory
    // "*/qualified_id_join_index_dir/*" should never be discarded).
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(
                                      EndsWith("/qualified_id_join_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem, DeleteFile(_)).WillRepeatedly(DoDefault());
    EXPECT_CALL(*mock_filesystem,
                DeleteFile(HasSubstr("/qualified_id_join_index_dir/")))
        .Times(0);

    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::move(mock_filesystem),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());
    InitializeResultProto initialize_result = icing.Initialize();
    ASSERT_THAT(initialize_result.status(), ProtoIsOk());
    EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(
        initialize_result.initialize_stats().integer_index_restoration_cause(),
        Eq(InitializeStatsProto::INCONSISTENT_WITH_GROUND_TRUTH));
    EXPECT_THAT(initialize_result.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));

    // Verify term index works normally
    SearchSpecProto search_spec1;
    search_spec1.set_query("body:consectetur");
    search_spec1.set_term_match_type(TermMatchType::EXACT_ONLY);
    SearchResultProto results1 =
        icing.Search(search_spec1, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(results1.status(), ProtoIsOk());
    EXPECT_THAT(results1.next_page_token(), Eq(0));
    // All documents should be retrievable.
    ASSERT_THAT(results1.results(), SizeIs(3));
    EXPECT_THAT(results1.results(0).document().uri(), Eq("message/3"));
    EXPECT_THAT(results1.results(1).document().uri(), Eq("message/2"));
    EXPECT_THAT(results1.results(2).document().uri(), Eq("message/1"));

    // Verify integer index works normally
    SearchSpecProto search_spec2;
    search_spec2.set_query("indexableInteger == 123");
    search_spec2.add_enabled_features(std::string(kNumericSearchFeature));

    SearchResultProto results2 =
        icing.Search(search_spec2, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    ASSERT_THAT(results2.results(), SizeIs(3));
    EXPECT_THAT(results2.results(0).document().uri(), Eq("message/3"));
    EXPECT_THAT(results2.results(1).document().uri(), Eq("message/2"));
    EXPECT_THAT(results2.results(2).document().uri(), Eq("message/1"));

    // Verify qualified id join index works normally: join a query for
    // `name:person` with a child query for `body:consectetur` based on the
    // child's `senderQualifiedId` field.
    SearchSpecProto search_spec3;
    search_spec3.set_term_match_type(TermMatchType::EXACT_ONLY);
    search_spec3.set_query("name:person");
    JoinSpecProto* join_spec = search_spec3.mutable_join_spec();
    join_spec->set_parent_property_expression(
        std::string(JoinProcessor::kQualifiedIdExpr));
    join_spec->set_child_property_expression("senderQualifiedId");
    join_spec->set_aggregation_scoring_strategy(
        JoinSpecProto::AggregationScoringStrategy::COUNT);
    JoinSpecProto::NestedSpecProto* nested_spec =
        join_spec->mutable_nested_spec();
    SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
    nested_search_spec->set_term_match_type(TermMatchType::EXACT_ONLY);
    nested_search_spec->set_query("body:consectetur");
    *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();
    *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

    ResultSpecProto result_spec3 = ResultSpecProto::default_instance();
    result_spec3.set_max_joined_children_per_parent_to_return(
        std::numeric_limits<int32_t>::max());

    SearchResultProto results3 = icing.Search(
        search_spec3, ScoringSpecProto::default_instance(), result_spec3);
    ASSERT_THAT(results3.results(), SizeIs(1));
    EXPECT_THAT(results3.results(0).document().uri(), Eq("person"));
    EXPECT_THAT(results3.results(0).joined_results(), SizeIs(3));
    EXPECT_THAT(results3.results(0).joined_results(0).document().uri(),
                Eq("message/3"));
    EXPECT_THAT(results3.results(0).joined_results(1).document().uri(),
                Eq("message/2"));
    EXPECT_THAT(results3.results(0).joined_results(2).document().uri(),
                Eq("message/1"));
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       RestoreIndexLoseQualifiedIdJoinIndex) {
  // Test the following scenario: losing the entire qualified id join index
  // directory. IcingSearchEngine should be able to recover qualified id join
  // index. Several additional behaviors are also tested:
  // - Index directory handling:
  //   - Term index directory should be unaffected.
  //   - Integer index directory should be unaffected.
  //   - Qualified id join index directory should not be discarded since we've
  //     already lost it. Start it from scratch.
  // - Truncate indices:
  //   - "TruncateTo()" for term index shouldn't take effect.
  //   - "Clear()" shouldn't be called for integer index, i.e. no integer index
  //     storage sub directories (path_expr = "*/integer_index_dir/*") should be
  //     discarded.
  //   - "Clear()" shouldn't be called for qualified id join index, i.e. no
  //     underlying storage sub directory (path_expr =
  //     "*/qualified_id_join_index_dir/*") should be discarded, since we start
  //     it from scratch.

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("indexableInteger")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("senderQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  DocumentProto person =
      DocumentBuilder()
          .SetKey("namespace", "person")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message =
      DocumentBuilder()
          .SetKey("namespace", "message/1")
          .SetSchema("Message")
          .AddStringProperty("body", kIpsumText)
          .AddInt64Property("indexableInteger", 123)
          .AddStringProperty("senderQualifiedId", "namespace#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  // 1. Create an index with 3 message documents.
  {
    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::make_unique<Filesystem>(),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    EXPECT_THAT(icing.Put(person).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
    message = DocumentBuilder(message).SetUri("message/2").Build();
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
    message = DocumentBuilder(message).SetUri("message/3").Build();
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
  }

  // 2. Delete the qualified id join index file to trigger RestoreIndexIfNeeded.
  std::string qualified_id_join_index_dir = GetQualifiedIdJoinIndexDir();
  filesystem()->DeleteDirectoryRecursively(qualified_id_join_index_dir.c_str());

  // 3. Create the index again. This should trigger index restoration.
  {
    // Mock filesystem to observe and check the behavior of all indices.
    auto mock_filesystem = std::make_unique<MockFilesystem>();
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
        .WillRepeatedly(DoDefault());
    // Ensure term index directory should never be discarded.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/index_dir")))
        .Times(0);
    // Ensure integer index directory should never be discarded since we've
    // already lost it, and Clear() should never be called (i.e. storage sub
    // directory "*/integer_index_dir/*" should never be discarded).
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/integer_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(HasSubstr("/integer_index_dir/")))
        .Times(0);
    // Ensure qualified id join index directory should never be discarded, and
    // Clear() should never be called (i.e. storage sub directory
    // "*/qualified_id_join_index_dir/*" should never be discarded)
    // since we start it from scratch.
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(
                                      EndsWith("/qualified_id_join_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem, DeleteFile(_)).WillRepeatedly(DoDefault());
    EXPECT_CALL(*mock_filesystem,
                DeleteFile(HasSubstr("/qualified_id_join_index_dir/")))
        .Times(0);

    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::move(mock_filesystem),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());
    InitializeResultProto initialize_result = icing.Initialize();
    ASSERT_THAT(initialize_result.status(), ProtoIsOk());
    EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(
        initialize_result.initialize_stats().integer_index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::INCONSISTENT_WITH_GROUND_TRUTH));

    // Verify term index works normally
    SearchSpecProto search_spec1;
    search_spec1.set_query("body:consectetur");
    search_spec1.set_term_match_type(TermMatchType::EXACT_ONLY);
    SearchResultProto results1 =
        icing.Search(search_spec1, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(results1.status(), ProtoIsOk());
    EXPECT_THAT(results1.next_page_token(), Eq(0));
    // All documents should be retrievable.
    ASSERT_THAT(results1.results(), SizeIs(3));
    EXPECT_THAT(results1.results(0).document().uri(), Eq("message/3"));
    EXPECT_THAT(results1.results(1).document().uri(), Eq("message/2"));
    EXPECT_THAT(results1.results(2).document().uri(), Eq("message/1"));

    // Verify integer index works normally
    SearchSpecProto search_spec2;
    search_spec2.set_query("indexableInteger == 123");
    search_spec2.add_enabled_features(std::string(kNumericSearchFeature));

    SearchResultProto results2 =
        icing.Search(search_spec2, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    ASSERT_THAT(results2.results(), SizeIs(3));
    EXPECT_THAT(results2.results(0).document().uri(), Eq("message/3"));
    EXPECT_THAT(results2.results(1).document().uri(), Eq("message/2"));
    EXPECT_THAT(results2.results(2).document().uri(), Eq("message/1"));

    // Verify qualified id join index works normally: join a query for
    // `name:person` with a child query for `body:consectetur` based on the
    // child's `senderQualifiedId` field.
    SearchSpecProto search_spec3;
    search_spec3.set_term_match_type(TermMatchType::EXACT_ONLY);
    search_spec3.set_query("name:person");
    JoinSpecProto* join_spec = search_spec3.mutable_join_spec();
    join_spec->set_parent_property_expression(
        std::string(JoinProcessor::kQualifiedIdExpr));
    join_spec->set_child_property_expression("senderQualifiedId");
    join_spec->set_aggregation_scoring_strategy(
        JoinSpecProto::AggregationScoringStrategy::COUNT);
    JoinSpecProto::NestedSpecProto* nested_spec =
        join_spec->mutable_nested_spec();
    SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
    nested_search_spec->set_term_match_type(TermMatchType::EXACT_ONLY);
    nested_search_spec->set_query("body:consectetur");
    *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();
    *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

    ResultSpecProto result_spec3 = ResultSpecProto::default_instance();
    result_spec3.set_max_joined_children_per_parent_to_return(
        std::numeric_limits<int32_t>::max());

    SearchResultProto results3 = icing.Search(
        search_spec3, ScoringSpecProto::default_instance(), result_spec3);
    ASSERT_THAT(results3.results(), SizeIs(1));
    EXPECT_THAT(results3.results(0).document().uri(), Eq("person"));
    EXPECT_THAT(results3.results(0).joined_results(), SizeIs(3));
    EXPECT_THAT(results3.results(0).joined_results(0).document().uri(),
                Eq("message/3"));
    EXPECT_THAT(results3.results(0).joined_results(1).document().uri(),
                Eq("message/2"));
    EXPECT_THAT(results3.results(0).joined_results(2).document().uri(),
                Eq("message/1"));
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       RestoreIndexTruncateLiteIndexWithoutReindexing) {
  // Test the following scenario: term lite index is *completely* ahead of
  // document store. IcingSearchEngine should be able to recover term index.
  // Several additional behaviors are also tested:
  // - Index directory handling:
  //   - Term index directory should be unaffected.
  //   - Integer index directory should be unaffected.
  //   - Qualified id join index directory should be unaffected.
  // - Truncate indices:
  //   - "TruncateTo()" for term index should take effect and throw out the
  //     entire lite index. This should be sufficient to make term index
  //     consistent with document store, so reindexing should not take place.
  //   - "Clear()" shouldn't be called for integer index, i.e. no integer index
  //     storage sub directories (path_expr = "*/integer_index_dir/*") should be
  //     discarded.
  //   - "Clear()" shouldn't be called for qualified id join index, i.e. no
  //     underlying storage sub directory (path_expr =
  //     "*/qualified_id_join_index_dir/*") should be discarded.

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("indexableInteger")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("senderQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  DocumentProto person =
      DocumentBuilder()
          .SetKey("namespace", "person")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message =
      DocumentBuilder()
          .SetKey("namespace", "message/1")
          .SetSchema("Message")
          .AddStringProperty("body", kIpsumText)
          .AddInt64Property("indexableInteger", 123)
          .AddStringProperty("senderQualifiedId", "namespace#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  // 1. Create an index with a LiteIndex that will only allow a person and a
  //    message document before needing a merge.
  {
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_index_merge_size(person.ByteSizeLong() +
                                 message.ByteSizeLong());
    TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    EXPECT_THAT(icing.Put(person).status(), ProtoIsOk());
    // Add two message documents. These should get merged into the main index.
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
    message = DocumentBuilder(message).SetUri("message/2").Build();
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
  }

  // 2. Manually add some data into term lite index and increment
  // last_added_document_id, but don't merge into the main index. This will
  // cause mismatched last_added_document_id with term index.
  //   - Document store: [0, 1, 2]
  //   - Term index
  //     - Main index: [0, 1, 2]
  //     - Lite index: [3]
  //   - Integer index: [0, 1, 2]
  //   - Qualified id join index: [0, 1, 2]
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Index> index,
        Index::Create(
            Index::Options(GetIndexDir(),
                           /*index_merge_size=*/message.ByteSizeLong(),
                           /*lite_index_sort_at_indexing=*/true,
                           /*lite_index_sort_size=*/8),
            filesystem(), icing_filesystem()));
    DocumentId original_last_added_doc_id = index->last_added_document_id();
    index->set_last_added_document_id(original_last_added_doc_id + 1);
    Index::Editor editor =
        index->Edit(original_last_added_doc_id + 1, /*section_id=*/0,
                    /*namespace_id=*/0);
    ICING_ASSERT_OK(editor.BufferTerm("foo", TermMatchType::EXACT_ONLY));
    ICING_ASSERT_OK(editor.IndexAllBufferedTerms());
  }

  // 3. Create the index again.
  {
    // Mock filesystem to observe and check the behavior of all indices.
    auto mock_filesystem = std::make_unique<MockFilesystem>();
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
        .WillRepeatedly(DoDefault());
    // Ensure term index directory should never be discarded. since we only call
    // TruncateTo for term index.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/index_dir")))
        .Times(0);
    // Ensure integer index directory should never be discarded, and Clear()
    // should never be called (i.e. storage sub directory
    // "*/integer_index_dir/*" should never be discarded).
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/integer_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(HasSubstr("/integer_index_dir/")))
        .Times(0);
    // Ensure qualified id join index directory should never be discarded, and
    // Clear() should never be called (i.e. storage sub directory
    // "*/qualified_id_join_index_dir/*" should never be discarded).
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(
                                      EndsWith("/qualified_id_join_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem, DeleteFile(_)).WillRepeatedly(DoDefault());
    EXPECT_CALL(*mock_filesystem,
                DeleteFile(HasSubstr("/qualified_id_join_index_dir/")))
        .Times(0);

    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_index_merge_size(message.ByteSizeLong());
    TestIcingSearchEngine icing(options, std::move(mock_filesystem),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());
    InitializeResultProto initialize_result = icing.Initialize();
    ASSERT_THAT(initialize_result.status(), ProtoIsOk());
    // Since truncating lite index is sufficient to make term index consistent
    // with document store, replaying documents or reindex shouldn't take place.
    EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(
        initialize_result.initialize_stats().integer_index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));

    // Verify term index works normally
    SearchSpecProto search_spec1;
    search_spec1.set_query("body:consectetur");
    search_spec1.set_term_match_type(TermMatchType::EXACT_ONLY);
    SearchResultProto results1 =
        icing.Search(search_spec1, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(results1.status(), ProtoIsOk());
    EXPECT_THAT(results1.next_page_token(), Eq(0));
    // Only the documents that were in the main index should be retrievable.
    ASSERT_THAT(results1.results(), SizeIs(2));
    EXPECT_THAT(results1.results(0).document().uri(), Eq("message/2"));
    EXPECT_THAT(results1.results(1).document().uri(), Eq("message/1"));

    // Verify integer index works normally
    SearchSpecProto search_spec2;
    search_spec2.set_query("indexableInteger == 123");
    search_spec2.add_enabled_features(std::string(kNumericSearchFeature));

    SearchResultProto results2 =
        icing.Search(search_spec2, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    ASSERT_THAT(results2.results(), SizeIs(2));
    EXPECT_THAT(results2.results(0).document().uri(), Eq("message/2"));
    EXPECT_THAT(results2.results(1).document().uri(), Eq("message/1"));

    // Verify qualified id join index works normally: join a query for
    // `name:person` with a child query for `body:consectetur` based on the
    // child's `senderQualifiedId` field.
    SearchSpecProto search_spec3;
    search_spec3.set_term_match_type(TermMatchType::EXACT_ONLY);
    search_spec3.set_query("name:person");
    JoinSpecProto* join_spec = search_spec3.mutable_join_spec();
    join_spec->set_parent_property_expression(
        std::string(JoinProcessor::kQualifiedIdExpr));
    join_spec->set_child_property_expression("senderQualifiedId");
    join_spec->set_aggregation_scoring_strategy(
        JoinSpecProto::AggregationScoringStrategy::COUNT);
    JoinSpecProto::NestedSpecProto* nested_spec =
        join_spec->mutable_nested_spec();
    SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
    nested_search_spec->set_term_match_type(TermMatchType::EXACT_ONLY);
    nested_search_spec->set_query("body:consectetur");
    *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();
    *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

    ResultSpecProto result_spec3 = ResultSpecProto::default_instance();
    result_spec3.set_max_joined_children_per_parent_to_return(
        std::numeric_limits<int32_t>::max());

    SearchResultProto results3 = icing.Search(
        search_spec3, ScoringSpecProto::default_instance(), result_spec3);
    ASSERT_THAT(results3.results(), SizeIs(1));
    EXPECT_THAT(results3.results(0).document().uri(), Eq("person"));
    EXPECT_THAT(results3.results(0).joined_results(), SizeIs(2));
    EXPECT_THAT(results3.results(0).joined_results(0).document().uri(),
                Eq("message/2"));
    EXPECT_THAT(results3.results(0).joined_results(1).document().uri(),
                Eq("message/1"));
  }

  // 4. Since document 3 doesn't exist, testing query = "foo" is not enough to
  // verify the correctness of term index restoration. Instead, we have to check
  // hits for "foo" should not be found in term index.
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Index> index,
        Index::Create(
            Index::Options(GetIndexDir(),
                           /*index_merge_size=*/message.ByteSizeLong(),
                           /*lite_index_sort_at_indexing=*/true,
                           /*lite_index_sort_size=*/8),
            filesystem(), icing_filesystem()));
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DocHitInfoIterator> doc_hit_info_iter,
        index->GetIterator("foo", /*term_start_index=*/0,
                           /*unnormalized_term_length=*/0, kSectionIdMaskAll,
                           TermMatchType::EXACT_ONLY));
    EXPECT_THAT(doc_hit_info_iter->Advance(),
                StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       RestoreIndexTruncateLiteIndexWithReindexing) {
  // Test the following scenario: term lite index is *partially* ahead of
  // document store. IcingSearchEngine should be able to recover term index.
  // Several additional behaviors are also tested:
  // - Index directory handling:
  //   - Term index directory should be unaffected.
  //   - Integer index directory should be unaffected.
  //   - Qualified id join index directory should be unaffected.
  // - Truncate indices:
  //   - "TruncateTo()" for term index should take effect and throw out the
  //     entire lite index. However, some valid data in term lite index were
  //     discarded together, so reindexing should still take place to recover
  //     them after truncating.
  //   - "Clear()" shouldn't be called for integer index, i.e. no integer index
  //     storage sub directories (path_expr = "*/integer_index_dir/*") should be
  //     discarded.
  //   - "Clear()" shouldn't be called for qualified id join index, i.e. no
  //     underlying storage sub directory (path_expr =
  //     "*/qualified_id_join_index_dir/*") should be discarded.

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("indexableInteger")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("senderQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  DocumentProto person =
      DocumentBuilder()
          .SetKey("namespace", "person")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message =
      DocumentBuilder()
          .SetKey("namespace", "message/1")
          .SetSchema("Message")
          .AddStringProperty("body", kIpsumText)
          .AddInt64Property("indexableInteger", 123)
          .AddStringProperty("senderQualifiedId", "namespace#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  // 1. Create an index with a LiteIndex that will only allow a person and a
  //    message document before needing a merge.
  {
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_index_merge_size(message.ByteSizeLong());
    TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    EXPECT_THAT(icing.Put(person).status(), ProtoIsOk());
    // Add two message documents. These should get merged into the main index.
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
    message = DocumentBuilder(message).SetUri("message/2").Build();
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
    // Add one document. This one should get remain in the lite index.
    message = DocumentBuilder(message).SetUri("message/3").Build();
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
  }

  // 2. Manually add some data into term lite index and increment
  //    last_added_document_id, but don't merge into the main index. This will
  //    cause mismatched last_added_document_id with term index.
  //   - Document store: [0, 1, 2, 3]
  //   - Term index
  //     - Main index: [0, 1, 2]
  //     - Lite index: [3, 4]
  //   - Integer index: [0, 1, 2, 3]
  //   - Qualified id join index: [0, 1, 2, 3]
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Index> index,
        Index::Create(
            Index::Options(GetIndexDir(),
                           /*index_merge_size=*/message.ByteSizeLong(),
                           /*lite_index_sort_at_indexing=*/true,
                           /*lite_index_sort_size=*/8),
            filesystem(), icing_filesystem()));
    DocumentId original_last_added_doc_id = index->last_added_document_id();
    index->set_last_added_document_id(original_last_added_doc_id + 1);
    Index::Editor editor =
        index->Edit(original_last_added_doc_id + 1, /*section_id=*/0,
                    /*namespace_id=*/0);
    ICING_ASSERT_OK(editor.BufferTerm("foo", TermMatchType::EXACT_ONLY));
    ICING_ASSERT_OK(editor.IndexAllBufferedTerms());
  }

  // 3. Create the index again.
  {
    // Mock filesystem to observe and check the behavior of all indices.
    auto mock_filesystem = std::make_unique<MockFilesystem>();
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
        .WillRepeatedly(DoDefault());
    // Ensure term index directory should never be discarded. since we only call
    // TruncateTo for term index.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/index_dir")))
        .Times(0);
    // Ensure integer index directory should never be discarded, and Clear()
    // should never be called (i.e. storage sub directory
    // "*/integer_index_dir/*" should never be discarded).
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/integer_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(HasSubstr("/integer_index_dir/")))
        .Times(0);
    // Ensure qualified id join index directory should never be discarded, and
    // Clear() should never be called (i.e. storage sub directory
    // "*/qualified_id_join_index_dir/*" should never be discarded).
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(
                                      EndsWith("/qualified_id_join_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem, DeleteFile(_)).WillRepeatedly(DoDefault());
    EXPECT_CALL(*mock_filesystem,
                DeleteFile(HasSubstr("/qualified_id_join_index_dir/")))
        .Times(0);

    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_index_merge_size(message.ByteSizeLong());
    TestIcingSearchEngine icing(options, std::move(mock_filesystem),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());
    InitializeResultProto initialize_result = icing.Initialize();
    ASSERT_THAT(initialize_result.status(), ProtoIsOk());
    // Truncating lite index not only deletes data ahead document store, but
    // also deletes valid data. Therefore, we still have to replay documents and
    // reindex.
    EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
                Eq(InitializeStatsProto::INCONSISTENT_WITH_GROUND_TRUTH));
    EXPECT_THAT(
        initialize_result.initialize_stats().integer_index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));

    // Verify term index works normally
    SearchSpecProto search_spec1;
    search_spec1.set_query("body:consectetur");
    search_spec1.set_term_match_type(TermMatchType::EXACT_ONLY);
    SearchResultProto results1 =
        icing.Search(search_spec1, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(results1.status(), ProtoIsOk());
    EXPECT_THAT(results1.next_page_token(), Eq(0));
    // Only the documents that were in the main index should be retrievable.
    ASSERT_THAT(results1.results(), SizeIs(3));
    EXPECT_THAT(results1.results(0).document().uri(), Eq("message/3"));
    EXPECT_THAT(results1.results(1).document().uri(), Eq("message/2"));
    EXPECT_THAT(results1.results(2).document().uri(), Eq("message/1"));

    // Verify integer index works normally
    SearchSpecProto search_spec2;
    search_spec2.set_query("indexableInteger == 123");
    search_spec2.add_enabled_features(std::string(kNumericSearchFeature));

    SearchResultProto results2 =
        icing.Search(search_spec2, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    ASSERT_THAT(results2.results(), SizeIs(3));
    EXPECT_THAT(results2.results(0).document().uri(), Eq("message/3"));
    EXPECT_THAT(results2.results(1).document().uri(), Eq("message/2"));
    EXPECT_THAT(results2.results(2).document().uri(), Eq("message/1"));

    // Verify qualified id join index works normally: join a query for
    // `name:person` with a child query for `body:consectetur` based on the
    // child's `senderQualifiedId` field.
    SearchSpecProto search_spec3;
    search_spec3.set_term_match_type(TermMatchType::EXACT_ONLY);
    search_spec3.set_query("name:person");
    JoinSpecProto* join_spec = search_spec3.mutable_join_spec();
    join_spec->set_parent_property_expression(
        std::string(JoinProcessor::kQualifiedIdExpr));
    join_spec->set_child_property_expression("senderQualifiedId");
    join_spec->set_aggregation_scoring_strategy(
        JoinSpecProto::AggregationScoringStrategy::COUNT);
    JoinSpecProto::NestedSpecProto* nested_spec =
        join_spec->mutable_nested_spec();
    SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
    nested_search_spec->set_term_match_type(TermMatchType::EXACT_ONLY);
    nested_search_spec->set_query("body:consectetur");
    *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();
    *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

    ResultSpecProto result_spec3 = ResultSpecProto::default_instance();
    result_spec3.set_max_joined_children_per_parent_to_return(
        std::numeric_limits<int32_t>::max());

    SearchResultProto results3 = icing.Search(
        search_spec3, ScoringSpecProto::default_instance(), result_spec3);
    ASSERT_THAT(results3.results(), SizeIs(1));
    EXPECT_THAT(results3.results(0).document().uri(), Eq("person"));
    EXPECT_THAT(results3.results(0).joined_results(), SizeIs(3));
    EXPECT_THAT(results3.results(0).joined_results(0).document().uri(),
                Eq("message/3"));
    EXPECT_THAT(results3.results(0).joined_results(1).document().uri(),
                Eq("message/2"));
    EXPECT_THAT(results3.results(0).joined_results(2).document().uri(),
                Eq("message/1"));
  }

  // 4. Since document 4 doesn't exist, testing query = "foo" is not enough to
  // verify the correctness of term index restoration. Instead, we have to check
  // hits for "foo" should not be found in term index.
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Index> index,
        Index::Create(
            Index::Options(GetIndexDir(),
                           /*index_merge_size=*/message.ByteSizeLong(),
                           /*lite_index_sort_at_indexing=*/true,
                           /*lite_index_sort_size=*/8),
            filesystem(), icing_filesystem()));
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DocHitInfoIterator> doc_hit_info_iter,
        index->GetIterator("foo", /*term_start_index=*/0,
                           /*unnormalized_term_length=*/0, kSectionIdMaskAll,
                           TermMatchType::EXACT_ONLY));
    EXPECT_THAT(doc_hit_info_iter->Advance(),
                StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       RestoreIndexTruncateMainIndexWithoutReindexing) {
  // Test the following scenario: term main index is *completely* ahead of
  // document store. IcingSearchEngine should be able to recover term index.
  // Several additional behaviors are also tested:
  // - Index directory handling:
  //   - Term index directory should be unaffected.
  //   - Integer index directory should be unaffected.
  //   - Qualified id join index directory should be unaffected.
  // - Truncate indices:
  //   - "TruncateTo()" for term index should take effect and throw out the
  //     entire lite and main index. This should be sufficient to make term
  //     index consistent with document store (in this case, document store is
  //     empty as well), so reindexing should not take place.
  //   - "Clear()" should be called for integer index. It is a special case when
  //     document store has no document. Since there is no integer index storage
  //     sub directories (path_expr = "*/integer_index_dir/*"), nothing will be
  //     discarded.
  //   - "Clear()" should be called for qualified id join index. It is a special
  //     case when document store has no document.

  // 1. Create an index with no document.
  {
    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::make_unique<Filesystem>(),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
  }

  // 2. Manually add some data into term lite index and increment
  //    last_added_document_id. Merge some of them into the main index and keep
  //    others in the lite index. This will cause mismatched document id with
  //    document store.
  //   - Document store: []
  //   - Term index
  //     - Main index: [0]
  //     - Lite index: [1]
  //   - Integer index: []
  //   - Qualified id join index: []
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Index> index,
        Index::Create(
            // index merge size is not important here because we will manually
            // invoke merge below.
            Index::Options(GetIndexDir(), /*index_merge_size=*/100,
                           /*lite_index_sort_at_indexing=*/true,
                           /*lite_index_sort_size=*/50),
            filesystem(), icing_filesystem()));
    // Add hits for document 0 and merge.
    ASSERT_THAT(index->last_added_document_id(), kInvalidDocumentId);
    index->set_last_added_document_id(0);
    Index::Editor editor = index->Edit(/*document_id=*/0, /*section_id=*/0,
                                       /*namespace_id=*/0);
    ICING_ASSERT_OK(editor.BufferTerm("foo", TermMatchType::EXACT_ONLY));
    ICING_ASSERT_OK(editor.IndexAllBufferedTerms());
    ICING_ASSERT_OK(index->Merge());

    // Add hits for document 1 and don't merge.
    index->set_last_added_document_id(1);
    editor = index->Edit(/*document_id=*/1, /*section_id=*/0,
                         /*namespace_id=*/0);
    ICING_ASSERT_OK(editor.BufferTerm("bar", TermMatchType::EXACT_ONLY));
    ICING_ASSERT_OK(editor.IndexAllBufferedTerms());
  }

  // 3. Create the index again. This should throw out the lite and main index.
  {
    // Mock filesystem to observe and check the behavior of all indices.
    auto mock_filesystem = std::make_unique<MockFilesystem>();
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
        .WillRepeatedly(DoDefault());
    // Ensure term index directory should never be discarded. since we only call
    // TruncateTo for term index.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/index_dir")))
        .Times(0);
    // Ensure integer index directory should never be discarded. Even though
    // Clear() was called, it shouldn't take effect since there is no storage
    // sub directory ("*/integer_index_dir/*") and nothing will be discarded.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/integer_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(HasSubstr("/integer_index_dir/")))
        .Times(0);
    // Ensure qualified id join index directory should never be discarded.
    // Clear() was called and should discard and reinitialize the underlying
    // mapper.
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(
                                      EndsWith("/qualified_id_join_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem, DeleteFile(_)).WillRepeatedly(DoDefault());
    EXPECT_CALL(*mock_filesystem,
                DeleteFile(HasSubstr("/qualified_id_join_index_dir/")))
        .Times(AtLeast(1));

    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::move(mock_filesystem),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());
    InitializeResultProto initialize_result = icing.Initialize();
    ASSERT_THAT(initialize_result.status(), ProtoIsOk());
    // Since truncating main index is sufficient to make term index consistent
    // with document store, replaying documents or reindexing shouldn't take
    // place.
    EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(
        initialize_result.initialize_stats().integer_index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
  }

  // 4. Since document 0, 1 don't exist, testing queries = "foo", "bar" are not
  // enough to verify the correctness of term index restoration. Instead, we
  // have to check hits for "foo", "bar" should not be found in term index.
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Index> index,
        Index::Create(Index::Options(GetIndexDir(), /*index_merge_size=*/100,
                                     /*lite_index_sort_at_indexing=*/true,
                                     /*lite_index_sort_size=*/50),
                      filesystem(), icing_filesystem()));
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DocHitInfoIterator> doc_hit_info_iter,
        index->GetIterator("foo", /*term_start_index=*/0,
                           /*unnormalized_term_length=*/0, kSectionIdMaskAll,
                           TermMatchType::EXACT_ONLY));
    EXPECT_THAT(doc_hit_info_iter->Advance(),
                StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));

    ICING_ASSERT_OK_AND_ASSIGN(
        doc_hit_info_iter,
        index->GetIterator("bar", /*term_start_index=*/0,
                           /*unnormalized_term_length=*/0, kSectionIdMaskAll,
                           TermMatchType::EXACT_ONLY));
    EXPECT_THAT(doc_hit_info_iter->Advance(),
                StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       RestoreIndexTruncateMainIndexWithReindexing) {
  // Test the following scenario: term main index is *partially* ahead of
  // document store. IcingSearchEngine should be able to recover term index.
  // Several additional behaviors are also tested:
  // - Index directory handling:
  //   - Term index directory should be unaffected.
  //   - Integer index directory should be unaffected.
  //   - Qualified id join index directory should be unaffected.
  // - In RestoreIndexIfNecessary():
  //   - "TruncateTo()" for term index should take effect and throw out the
  //     entire lite and main index. However, some valid data in term main index
  //     were discarded together, so reindexing should still take place to
  //     recover them after truncating.
  //   - "Clear()" shouldn't be called for integer index, i.e. no integer index
  //     storage sub directories (path_expr = "*/integer_index_dir/*") should be
  //     discarded.
  //   - "Clear()" shouldn't be called for qualified id join index, i.e. no
  //     underlying storage sub directory (path_expr =
  //     "*/qualified_id_join_index_dir/*") should be discarded.

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("indexableInteger")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("senderQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  DocumentProto person =
      DocumentBuilder()
          .SetKey("namespace", "person")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message =
      DocumentBuilder()
          .SetKey("namespace", "message/1")
          .SetSchema("Message")
          .AddStringProperty("body", kIpsumText)
          .AddInt64Property("indexableInteger", 123)
          .AddStringProperty("senderQualifiedId", "namespace#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  // 1. Create an index with 3 message documents.
  {
    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::make_unique<Filesystem>(),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    EXPECT_THAT(icing.Put(person).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
    message = DocumentBuilder(message).SetUri("message/2").Build();
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
    message = DocumentBuilder(message).SetUri("message/3").Build();
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
  }

  // 2. Manually add some data into term lite index and increment
  //    last_added_document_id. Merge some of them into the main index and keep
  //    others in the lite index. This will cause mismatched document id with
  //    document store.
  //   - Document store: [0, 1, 2, 3]
  //   - Term index
  //     - Main index: [0, 1, 2, 3, 4]
  //     - Lite index: [5]
  //   - Integer index: [0, 1, 2, 3]
  //   - Qualified id join index: [0, 1, 2, 3]
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Index> index,
        Index::Create(
            Index::Options(GetIndexDir(),
                           /*index_merge_size=*/message.ByteSizeLong(),
                           /*lite_index_sort_at_indexing=*/true,
                           /*lite_index_sort_size=*/8),
            filesystem(), icing_filesystem()));
    // Add hits for document 4 and merge.
    DocumentId original_last_added_doc_id = index->last_added_document_id();
    index->set_last_added_document_id(original_last_added_doc_id + 1);
    Index::Editor editor =
        index->Edit(original_last_added_doc_id + 1, /*section_id=*/0,
                    /*namespace_id=*/0);
    ICING_ASSERT_OK(editor.BufferTerm("foo", TermMatchType::EXACT_ONLY));
    ICING_ASSERT_OK(editor.IndexAllBufferedTerms());
    ICING_ASSERT_OK(index->Merge());

    // Add hits for document 5 and don't merge.
    index->set_last_added_document_id(original_last_added_doc_id + 2);
    editor = index->Edit(original_last_added_doc_id + 2, /*section_id=*/0,
                         /*namespace_id=*/0);
    ICING_ASSERT_OK(editor.BufferTerm("bar", TermMatchType::EXACT_ONLY));
    ICING_ASSERT_OK(editor.IndexAllBufferedTerms());
  }

  // 3. Create the index again. This should throw out the lite and main index
  // and trigger index restoration.
  {
    // Mock filesystem to observe and check the behavior of all indices.
    auto mock_filesystem = std::make_unique<MockFilesystem>();
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
        .WillRepeatedly(DoDefault());
    // Ensure term index directory should never be discarded. since we only call
    // TruncateTo for term index.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/index_dir")))
        .Times(0);
    // Ensure integer index directory should never be discarded, and Clear()
    // should never be called (i.e. storage sub directory
    // "*/integer_index_dir/*" should never be discarded).
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/integer_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(HasSubstr("/integer_index_dir/")))
        .Times(0);
    // Ensure qualified id join index directory should never be discarded, and
    // Clear() should never be called (i.e. storage sub directory
    // "*/qualified_id_join_index_dir/*" should never be discarded).
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(
                                      EndsWith("/qualified_id_join_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem, DeleteFile(_)).WillRepeatedly(DoDefault());
    EXPECT_CALL(*mock_filesystem,
                DeleteFile(HasSubstr("/qualified_id_join_index_dir/")))
        .Times(0);

    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::move(mock_filesystem),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());
    InitializeResultProto initialize_result = icing.Initialize();
    ASSERT_THAT(initialize_result.status(), ProtoIsOk());
    // Truncating main index not only deletes data ahead document store, but
    // also deletes valid data. Therefore, we still have to replay documents and
    // reindex.
    EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
                Eq(InitializeStatsProto::INCONSISTENT_WITH_GROUND_TRUTH));
    EXPECT_THAT(
        initialize_result.initialize_stats().integer_index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));

    // Verify term index works normally
    SearchSpecProto search_spec1;
    search_spec1.set_query("body:consectetur");
    search_spec1.set_term_match_type(TermMatchType::EXACT_ONLY);
    SearchResultProto results1 =
        icing.Search(search_spec1, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(results1.status(), ProtoIsOk());
    EXPECT_THAT(results1.next_page_token(), Eq(0));
    // Only the first document should be retrievable.
    ASSERT_THAT(results1.results(), SizeIs(3));
    EXPECT_THAT(results1.results(0).document().uri(), Eq("message/3"));
    EXPECT_THAT(results1.results(1).document().uri(), Eq("message/2"));
    EXPECT_THAT(results1.results(2).document().uri(), Eq("message/1"));

    // Verify integer index works normally
    SearchSpecProto search_spec2;
    search_spec2.set_query("indexableInteger == 123");
    search_spec2.add_enabled_features(std::string(kNumericSearchFeature));

    SearchResultProto results2 =
        icing.Search(search_spec2, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    ASSERT_THAT(results2.results(), SizeIs(3));
    EXPECT_THAT(results2.results(0).document().uri(), Eq("message/3"));
    EXPECT_THAT(results2.results(1).document().uri(), Eq("message/2"));
    EXPECT_THAT(results2.results(2).document().uri(), Eq("message/1"));

    // Verify qualified id join index works normally: join a query for
    // `name:person` with a child query for `body:consectetur` based on the
    // child's `senderQualifiedId` field.
    SearchSpecProto search_spec3;
    search_spec3.set_term_match_type(TermMatchType::EXACT_ONLY);
    search_spec3.set_query("name:person");
    JoinSpecProto* join_spec = search_spec3.mutable_join_spec();
    join_spec->set_parent_property_expression(
        std::string(JoinProcessor::kQualifiedIdExpr));
    join_spec->set_child_property_expression("senderQualifiedId");
    join_spec->set_aggregation_scoring_strategy(
        JoinSpecProto::AggregationScoringStrategy::COUNT);
    JoinSpecProto::NestedSpecProto* nested_spec =
        join_spec->mutable_nested_spec();
    SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
    nested_search_spec->set_term_match_type(TermMatchType::EXACT_ONLY);
    nested_search_spec->set_query("body:consectetur");
    *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();
    *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

    ResultSpecProto result_spec3 = ResultSpecProto::default_instance();
    result_spec3.set_max_joined_children_per_parent_to_return(
        std::numeric_limits<int32_t>::max());

    SearchResultProto results3 = icing.Search(
        search_spec3, ScoringSpecProto::default_instance(), result_spec3);
    ASSERT_THAT(results3.results(), SizeIs(1));
    EXPECT_THAT(results3.results(0).document().uri(), Eq("person"));
    EXPECT_THAT(results3.results(0).joined_results(), SizeIs(3));
    EXPECT_THAT(results3.results(0).joined_results(0).document().uri(),
                Eq("message/3"));
    EXPECT_THAT(results3.results(0).joined_results(1).document().uri(),
                Eq("message/2"));
    EXPECT_THAT(results3.results(0).joined_results(2).document().uri(),
                Eq("message/1"));
  }

  // 4. Since document 4, 5 don't exist, testing queries = "foo", "bar" are not
  // enough to verify the correctness of term index restoration. Instead, we
  // have to check hits for "foo", "bar" should not be found in term index.
  {
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Index> index,
        Index::Create(Index::Options(GetIndexDir(), /*index_merge_size=*/100,
                                     /*lite_index_sort_at_indexing=*/true,
                                     /*lite_index_sort_size=*/50),
                      filesystem(), icing_filesystem()));
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<DocHitInfoIterator> doc_hit_info_iter,
        index->GetIterator("foo", /*term_start_index=*/0,
                           /*unnormalized_term_length=*/0, kSectionIdMaskAll,
                           TermMatchType::EXACT_ONLY));
    EXPECT_THAT(doc_hit_info_iter->Advance(),
                StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));

    ICING_ASSERT_OK_AND_ASSIGN(
        doc_hit_info_iter,
        index->GetIterator("bar", /*term_start_index=*/0,
                           /*unnormalized_term_length=*/0, kSectionIdMaskAll,
                           TermMatchType::EXACT_ONLY));
    EXPECT_THAT(doc_hit_info_iter->Advance(),
                StatusIs(libtextclassifier3::StatusCode::RESOURCE_EXHAUSTED));
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       RestoreIndexTruncateIntegerIndexWithoutReindexing) {
  // Test the following scenario: integer index is *completely* ahead of
  // document store. IcingSearchEngine should be able to recover integer index.
  // Several additional behaviors are also tested:
  // - Index directory handling:
  //   - Term index directory should be unaffected.
  //   - Integer index directory should be unaffected.
  //   - Qualified id join index directory should be unaffected.
  // - Truncate indices:
  //   - "TruncateTo()" for term index shouldn't take effect.
  //   - "Clear()" should be called for integer index and throw out all integer
  //     index storages, i.e. all storage sub directories (path_expr =
  //     "*/integer_index_dir/*") should be discarded. This should be sufficient
  //     to make integer index consistent with document store (in this case,
  //     document store is empty as well), so reindexing should not take place.
  //   - "Clear()" should be called for qualified id join index. It is a special
  //     case when document store has no document.

  // 1. Create an index with no document.
  {
    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::make_unique<Filesystem>(),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
  }

  // 2. Manually add some data into integer index and increment
  //    last_added_document_id. This will cause mismatched document id with
  //    document store.
  //   - Document store: []
  //   - Term index: []
  //   - Integer index: [0]
  //   - Qualified id join index: []
  {
    Filesystem filesystem;
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<IntegerIndex> integer_index,
        IntegerIndex::Create(filesystem, GetIntegerIndexDir(),
                             /*num_data_threshold_for_bucket_split=*/65536,
                             /*pre_mapping_fbv=*/false));
    // Add hits for document 0.
    ASSERT_THAT(integer_index->last_added_document_id(), kInvalidDocumentId);
    integer_index->set_last_added_document_id(0);
    std::unique_ptr<NumericIndex<int64_t>::Editor> editor = integer_index->Edit(
        /*property_path=*/"indexableInteger", /*document_id=*/0,
        /*section_id=*/0);
    ICING_ASSERT_OK(editor->BufferKey(123));
    ICING_ASSERT_OK(std::move(*editor).IndexAllBufferedKeys());
  }

  // 3. Create the index again. This should trigger index restoration.
  {
    // Mock filesystem to observe and check the behavior of all indices.
    auto mock_filesystem = std::make_unique<MockFilesystem>();
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
        .WillRepeatedly(DoDefault());
    // Ensure term index directory should never be discarded.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/index_dir")))
        .Times(0);
    // Ensure integer index directory should never be discarded.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/integer_index_dir")))
        .Times(0);
    // Clear() should be called to truncate integer index and thus storage sub
    // directory (path_expr = "*/integer_index_dir/*") should be discarded.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(HasSubstr("/integer_index_dir/")))
        .Times(1);
    // Ensure qualified id join index directory should never be discarded.
    // Clear() was called and should discard and reinitialize the underlying
    // mapper.
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(
                                      EndsWith("/qualified_id_join_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem, DeleteFile(_)).WillRepeatedly(DoDefault());
    EXPECT_CALL(*mock_filesystem,
                DeleteFile(HasSubstr("/qualified_id_join_index_dir/")))
        .Times(AtLeast(1));

    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::move(mock_filesystem),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());
    InitializeResultProto initialize_result = icing.Initialize();
    ASSERT_THAT(initialize_result.status(), ProtoIsOk());
    EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    // Since truncating integer index is sufficient to make it consistent with
    // document store, replaying documents or reindexing shouldn't take place.
    EXPECT_THAT(
        initialize_result.initialize_stats().integer_index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));

    // Verify that numeric query safely wiped out the pre-existing hit for
    // 'indexableInteger' == 123. Add a new document without that value for
    // 'indexableInteger' that will take docid=0. If the integer index was not
    // rebuilt correctly, then it will still have the previously added hit for
    // 'indexableInteger' == 123 for docid 0 and incorrectly return this new
    // doc in a query.
    DocumentProto another_message =
        DocumentBuilder()
            .SetKey("namespace", "message/1")
            .SetSchema("Message")
            .AddStringProperty("body", kIpsumText)
            .AddInt64Property("indexableInteger", 456)
            .SetCreationTimestampMs(kDefaultCreationTimestampMs)
            .Build();
    EXPECT_THAT(icing.Put(another_message).status(), ProtoIsOk());
    // Verify integer index works normally
    SearchSpecProto search_spec;
    search_spec.set_query("indexableInteger == 123");
    search_spec.add_enabled_features(std::string(kNumericSearchFeature));

    SearchResultProto results =
        icing.Search(search_spec, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(results.results(), IsEmpty());
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       RestoreIndexTruncateIntegerIndexWithReindexing) {
  // Test the following scenario: integer index is *partially* ahead of document
  // store. IcingSearchEngine should be able to recover integer index. Several
  // additional behaviors are also tested:
  // - Index directory handling:
  //   - Term index directory should be unaffected.
  //   - Integer index directory should be unaffected.
  //   - Qualified id join index directory should be unaffected.
  // - Truncate indices:
  //   - "TruncateTo()" for term index shouldn't take effect.
  //   - "Clear()" should be called for integer index and throw out all integer
  //     index storages, i.e. all storage sub directories (path_expr =
  //     "*/integer_index_dir/*") should be discarded. However, some valid data
  //     in integer index were discarded together, so reindexing should still
  //     take place to recover them after clearing.
  //   - "Clear()" shouldn't be called for qualified id join index, i.e. no
  //     underlying storage sub directory (path_expr =
  //     "*/qualified_id_join_index_dir/*") should be discarded.

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("indexableInteger")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("senderQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  DocumentProto person =
      DocumentBuilder()
          .SetKey("namespace", "person")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message =
      DocumentBuilder()
          .SetKey("namespace", "message/1")
          .SetSchema("Message")
          .AddStringProperty("body", kIpsumText)
          .AddInt64Property("indexableInteger", 123)
          .AddStringProperty("senderQualifiedId", "namespace#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  // 1. Create an index with message 3 documents.
  {
    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::make_unique<Filesystem>(),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    EXPECT_THAT(icing.Put(person).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
    message = DocumentBuilder(message).SetUri("message/2").Build();
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
    message = DocumentBuilder(message).SetUri("message/3").Build();
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
  }

  // 2. Manually add some data into integer index and increment
  //    last_added_document_id. This will cause mismatched document id with
  //    document store.
  //   - Document store: [0, 1, 2, 3]
  //   - Term index: [0, 1, 2, 3]
  //   - Integer index: [0, 1, 2, 3, 4]
  //   - Qualified id join index: [0, 1, 2, 3]
  {
    Filesystem filesystem;
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<IntegerIndex> integer_index,
        IntegerIndex::Create(filesystem, GetIntegerIndexDir(),
                             /*num_data_threshold_for_bucket_split=*/65536,
                             /*pre_mapping_fbv=*/false));
    // Add hits for document 4.
    DocumentId original_last_added_doc_id =
        integer_index->last_added_document_id();
    integer_index->set_last_added_document_id(original_last_added_doc_id + 1);
    std::unique_ptr<NumericIndex<int64_t>::Editor> editor = integer_index->Edit(
        /*property_path=*/"indexableInteger",
        /*document_id=*/original_last_added_doc_id + 1, /*section_id=*/0);
    ICING_ASSERT_OK(editor->BufferKey(456));
    ICING_ASSERT_OK(std::move(*editor).IndexAllBufferedKeys());
  }

  // 3. Create the index again. This should trigger index restoration.
  {
    // Mock filesystem to observe and check the behavior of all indices.
    auto mock_filesystem = std::make_unique<MockFilesystem>();
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
        .WillRepeatedly(DoDefault());
    // Ensure term index directory should never be discarded.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/index_dir")))
        .Times(0);
    // Ensure integer index directory should never be discarded.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/integer_index_dir")))
        .Times(0);
    // Clear() should be called to truncate integer index and thus storage sub
    // directory (path_expr = "*/integer_index_dir/*") should be discarded.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(HasSubstr("/integer_index_dir/")))
        .Times(1);
    // Ensure qualified id join index directory should never be discarded, and
    // Clear() should never be called (i.e. storage sub directory
    // "*/qualified_id_join_index_dir/*" should never be discarded).
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(
                                      EndsWith("/qualified_id_join_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem, DeleteFile(_)).WillRepeatedly(DoDefault());
    EXPECT_CALL(*mock_filesystem,
                DeleteFile(HasSubstr("/qualified_id_join_index_dir/")))
        .Times(0);

    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::move(mock_filesystem),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());
    InitializeResultProto initialize_result = icing.Initialize();
    ASSERT_THAT(initialize_result.status(), ProtoIsOk());
    EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(
        initialize_result.initialize_stats().integer_index_restoration_cause(),
        Eq(InitializeStatsProto::INCONSISTENT_WITH_GROUND_TRUTH));
    EXPECT_THAT(initialize_result.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));

    // Verify term index works normally
    SearchSpecProto search_spec1;
    search_spec1.set_query("body:consectetur");
    search_spec1.set_term_match_type(TermMatchType::EXACT_ONLY);
    SearchResultProto results1 =
        icing.Search(search_spec1, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(results1.status(), ProtoIsOk());
    EXPECT_THAT(results1.next_page_token(), Eq(0));
    // All documents should be retrievable.
    ASSERT_THAT(results1.results(), SizeIs(3));
    EXPECT_THAT(results1.results(0).document().uri(), Eq("message/3"));
    EXPECT_THAT(results1.results(1).document().uri(), Eq("message/2"));
    EXPECT_THAT(results1.results(2).document().uri(), Eq("message/1"));

    // Verify integer index works normally
    SearchSpecProto search_spec2;
    search_spec2.set_query("indexableInteger == 123");
    search_spec2.add_enabled_features(std::string(kNumericSearchFeature));

    SearchResultProto results2 =
        icing.Search(search_spec2, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    ASSERT_THAT(results2.results(), SizeIs(3));
    EXPECT_THAT(results2.results(0).document().uri(), Eq("message/3"));
    EXPECT_THAT(results2.results(1).document().uri(), Eq("message/2"));
    EXPECT_THAT(results2.results(2).document().uri(), Eq("message/1"));

    // Verify qualified id join index works normally: join a query for
    // `name:person` with a child query for `body:consectetur` based on the
    // child's `senderQualifiedId` field.
    SearchSpecProto search_spec3;
    search_spec3.set_term_match_type(TermMatchType::EXACT_ONLY);
    search_spec3.set_query("name:person");
    JoinSpecProto* join_spec = search_spec3.mutable_join_spec();
    join_spec->set_parent_property_expression(
        std::string(JoinProcessor::kQualifiedIdExpr));
    join_spec->set_child_property_expression("senderQualifiedId");
    join_spec->set_aggregation_scoring_strategy(
        JoinSpecProto::AggregationScoringStrategy::COUNT);
    JoinSpecProto::NestedSpecProto* nested_spec =
        join_spec->mutable_nested_spec();
    SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
    nested_search_spec->set_term_match_type(TermMatchType::EXACT_ONLY);
    nested_search_spec->set_query("body:consectetur");
    *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();
    *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

    ResultSpecProto result_spec3 = ResultSpecProto::default_instance();
    result_spec3.set_max_joined_children_per_parent_to_return(
        std::numeric_limits<int32_t>::max());

    SearchResultProto results3 = icing.Search(
        search_spec3, ScoringSpecProto::default_instance(), result_spec3);
    ASSERT_THAT(results3.results(), SizeIs(1));
    EXPECT_THAT(results3.results(0).document().uri(), Eq("person"));
    EXPECT_THAT(results3.results(0).joined_results(), SizeIs(3));
    EXPECT_THAT(results3.results(0).joined_results(0).document().uri(),
                Eq("message/3"));
    EXPECT_THAT(results3.results(0).joined_results(1).document().uri(),
                Eq("message/2"));
    EXPECT_THAT(results3.results(0).joined_results(2).document().uri(),
                Eq("message/1"));

    // Verify that numeric index safely wiped out the pre-existing hit for
    // 'indexableInteger' == 456. Add a new document without that value for
    // 'indexableInteger' that will take docid=0. If the integer index was not
    // rebuilt correctly, then it will still have the previously added hit for
    // 'indexableInteger' == 456 for docid 0 and incorrectly return this new
    // doc in a query.
    DocumentProto another_message =
        DocumentBuilder()
            .SetKey("namespace", "message/4")
            .SetSchema("Message")
            .AddStringProperty("body", kIpsumText)
            .AddStringProperty("senderQualifiedId", "namespace#person")
            .SetCreationTimestampMs(kDefaultCreationTimestampMs)
            .Build();
    EXPECT_THAT(icing.Put(another_message).status(), ProtoIsOk());
    // Verify integer index works normally
    SearchSpecProto search_spec;
    search_spec.set_query("indexableInteger == 456");
    search_spec.add_enabled_features(std::string(kNumericSearchFeature));

    SearchResultProto results =
        icing.Search(search_spec, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(results.results(), IsEmpty());
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       RestoreIndexTruncateQualifiedIdJoinIndexWithoutReindexing) {
  // Test the following scenario: qualified id join index is *completely* ahead
  // of document store. IcingSearchEngine should be able to recover qualified id
  // join index. Several additional behaviors are also tested:
  // - Index directory handling:
  //   - Term index directory should be unaffected.
  //   - Integer index directory should be unaffected.
  //   - Qualified id join index directory should be unaffected.
  // - Truncate indices:
  //   - "TruncateTo()" for term index shouldn't take effect.
  //   - "Clear()" should be called for integer index. It is a special case when
  //     document store has no document. Since there is no integer index storage
  //     sub directories (path_expr = "*/integer_index_dir/*"), nothing will be
  //     discarded.
  //   - "Clear()" should be called for qualified id join index and throw out
  //     all data, i.e. discarding the underlying mapper (path_expr =
  //     "*/qualified_id_join_index_dir/*") and reinitialize. This should be
  //     sufficient to make qualified id join index consistent with document
  //     store (in this case, document store is empty as well), so reindexing
  //     should not take place.

  // 1. Create an index with no document.
  {
    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::make_unique<Filesystem>(),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
  }

  // 2. Manually add some data into integer index and increment
  //    last_added_document_id. This will cause mismatched document id with
  //    document store.
  //   - Document store: []
  //   - Term index: []
  //   - Integer index: []
  //   - Qualified id join index: [0]
  {
    Filesystem filesystem;
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdJoinIndex> qualified_id_join_index,
        QualifiedIdJoinIndexImplV3::Create(
            filesystem, GetQualifiedIdJoinIndexDir(), *feature_flags_));
    // Add data for document 0.
    ASSERT_THAT(qualified_id_join_index->last_added_document_id(),
                kInvalidDocumentId);
    qualified_id_join_index->set_last_added_document_id(0);
    ICING_ASSERT_OK(qualified_id_join_index->Put(
        DocumentJoinIdPair(/*document_id=*/0, /*joinable_property_id=*/0),
        /*parent_document_ids=*/std::vector<DocumentId>{0}));
  }

  // 3. Create the index again. This should trigger index restoration.
  {
    // Mock filesystem to observe and check the behavior of all indices.
    auto mock_filesystem = std::make_unique<MockFilesystem>();
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
        .WillRepeatedly(DoDefault());
    // Ensure term index directory should never be discarded.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/index_dir")))
        .Times(0);
    // Ensure integer index directory should never be discarded. Even though
    // Clear() was called, it shouldn't take effect since there is no storage
    // sub directory ("*/integer_index_dir/*") and nothing will be discarded.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/integer_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(HasSubstr("/integer_index_dir/")))
        .Times(0);
    // Ensure qualified id join index directory should never be discarded.
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(
                                      EndsWith("/qualified_id_join_index_dir")))
        .Times(0);
    // Clear() should be called to truncate qualified id join index and thus
    // underlying storage sub directory (path_expr =
    // "*/qualified_id_join_index_dir/*") should be discarded.
    EXPECT_CALL(*mock_filesystem, DeleteFile(_)).WillRepeatedly(DoDefault());
    EXPECT_CALL(*mock_filesystem,
                DeleteFile(HasSubstr("/qualified_id_join_index_dir/")))
        .Times(AtLeast(1));

    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::move(mock_filesystem),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());
    InitializeResultProto initialize_result = icing.Initialize();
    ASSERT_THAT(initialize_result.status(), ProtoIsOk());
    EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(
        initialize_result.initialize_stats().integer_index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    // Since truncating qualified id join index is sufficient to make it
    // consistent with document store, replaying documents or reindexing
    // shouldn't take place.
    EXPECT_THAT(initialize_result.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
  }

  // 4. Since document 0 doesn't exist, testing join query is not enough to
  // verify the correctness of qualified id join index restoration. Instead, we
  // have to check the previously added data should not be found in qualified id
  // join index.
  {
    Filesystem filesystem;
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdJoinIndex> qualified_id_join_index,
        QualifiedIdJoinIndexImplV3::Create(
            filesystem, GetQualifiedIdJoinIndexDir(), *feature_flags_));
    EXPECT_THAT(qualified_id_join_index, Pointee(IsEmpty()));
    EXPECT_THAT(qualified_id_join_index->Get(/*parent_document_id=*/0),
                IsOkAndHolds(IsEmpty()));
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       RestoreIndexTruncateQualifiedIdJoinIndexWithReindexing) {
  // Test the following scenario: qualified id join index is *partially* ahead
  // of document store. IcingSearchEngine should be able to recover qualified id
  // join index. Several additional behaviors are also tested:
  // - Index directory handling:
  //   - Term index directory should be unaffected.
  //   - Integer index directory should be unaffected.
  //   - Qualified id join index directory should be unaffected.
  // - Truncate indices:
  //   - "TruncateTo()" for term index shouldn't take effect.
  //   - "Clear()" shouldn't be called for integer index, i.e. no integer index
  //     storage sub directories (path_expr = "*/integer_index_dir/*") should be
  //     discarded.
  //   - "Clear()" should be called for qualified id join index and throw out
  //     all data, i.e. discarding the underlying mapper (path_expr =
  //     "*/qualified_id_join_index_dir/*") and reinitialize. However, some
  //     valid data in qualified id join index were discarded together, so
  //     reindexing should still take place to recover them after clearing.

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("indexableInteger")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("senderQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  DocumentProto person =
      DocumentBuilder()
          .SetKey("namespace", "person")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message =
      DocumentBuilder()
          .SetKey("namespace", "message/1")
          .SetSchema("Message")
          .AddStringProperty("body", kIpsumText)
          .AddInt64Property("indexableInteger", 123)
          .AddStringProperty("senderQualifiedId", "namespace#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  // 1. Create an index with message 3 documents.
  {
    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::make_unique<Filesystem>(),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    EXPECT_THAT(icing.Put(person).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
    message = DocumentBuilder(message).SetUri("message/2").Build();
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
    message = DocumentBuilder(message).SetUri("message/3").Build();
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
  }

  // 2. Manually add some data into qualified id join index and increment
  //    last_added_document_id. This will cause mismatched document id with
  //    document store.
  //   - Document store: [0, 1, 2, 3]
  //   - Term index: [0, 1, 2, 3]
  //   - Integer index: [0, 1, 2, 3]
  //   - Qualified id join index: [0, 1, 2, 3, 4]
  {
    Filesystem filesystem;
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdJoinIndex> qualified_id_join_index,
        QualifiedIdJoinIndexImplV3::Create(
            filesystem, GetQualifiedIdJoinIndexDir(), *feature_flags_));
    // Add data for document 4.
    DocumentId original_last_added_doc_id =
        qualified_id_join_index->last_added_document_id();
    qualified_id_join_index->set_last_added_document_id(
        original_last_added_doc_id + 1);
    ICING_ASSERT_OK(qualified_id_join_index->Put(
        DocumentJoinIdPair(/*document_id=*/4, /*joinable_property_id=*/0),
        /*parent_document_ids=*/std::vector<DocumentId>{0}));
  }

  // 3. Create the index again. This should trigger index restoration.
  {
    // Mock filesystem to observe and check the behavior of all indices.
    auto mock_filesystem = std::make_unique<MockFilesystem>();
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
        .WillRepeatedly(DoDefault());
    // Ensure term index directory should never be discarded.
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/index_dir")))
        .Times(0);
    // Ensure integer index directory should never be discarded, and Clear()
    // should never be called (i.e. storage sub directory
    // "*/integer_index_dir/*" should never be discarded).
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/integer_index_dir")))
        .Times(0);
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(HasSubstr("/integer_index_dir/")))
        .Times(0);
    // Ensure qualified id join index directory should never be discarded.
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(
                                      EndsWith("/qualified_id_join_index_dir")))
        .Times(0);
    // Clear() should be called to truncate qualified id join index and thus
    // underlying storage sub directory (path_expr =
    // "*/qualified_id_join_index_dir/*") should be discarded.
    EXPECT_CALL(*mock_filesystem, DeleteFile(_)).WillRepeatedly(DoDefault());
    EXPECT_CALL(*mock_filesystem,
                DeleteFile(HasSubstr("/qualified_id_join_index_dir/")))
        .Times(AtLeast(1));

    TestIcingSearchEngine icing(
        GetDefaultIcingOptions(), std::move(mock_filesystem),
        std::make_unique<IcingFilesystem>(), std::make_unique<FakeClock>(),
        GetTestJniCache());
    InitializeResultProto initialize_result = icing.Initialize();
    ASSERT_THAT(initialize_result.status(), ProtoIsOk());
    EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(
        initialize_result.initialize_stats().integer_index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::INCONSISTENT_WITH_GROUND_TRUTH));

    // Verify term index works normally
    SearchSpecProto search_spec1;
    search_spec1.set_query("body:consectetur");
    search_spec1.set_term_match_type(TermMatchType::EXACT_ONLY);
    SearchResultProto results1 =
        icing.Search(search_spec1, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(results1.status(), ProtoIsOk());
    EXPECT_THAT(results1.next_page_token(), Eq(0));
    // All documents should be retrievable.
    ASSERT_THAT(results1.results(), SizeIs(3));
    EXPECT_THAT(results1.results(0).document().uri(), Eq("message/3"));
    EXPECT_THAT(results1.results(1).document().uri(), Eq("message/2"));
    EXPECT_THAT(results1.results(2).document().uri(), Eq("message/1"));

    // Verify integer index works normally
    SearchSpecProto search_spec2;
    search_spec2.set_query("indexableInteger == 123");
    search_spec2.add_enabled_features(std::string(kNumericSearchFeature));

    SearchResultProto results2 =
        icing.Search(search_spec2, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    ASSERT_THAT(results2.results(), SizeIs(3));
    EXPECT_THAT(results2.results(0).document().uri(), Eq("message/3"));
    EXPECT_THAT(results2.results(1).document().uri(), Eq("message/2"));
    EXPECT_THAT(results2.results(2).document().uri(), Eq("message/1"));

    // Verify qualified id join index works normally: join a query for
    // `name:person` with a child query for `body:consectetur` based on the
    // child's `senderQualifiedId` field.

    // Add document 4 without "senderQualifiedId". If join index is not rebuilt
    // correctly, then it will still have the previously added senderQualifiedId
    // for document 4 and include document 4 incorrectly in the right side.
    DocumentProto another_message =
        DocumentBuilder()
            .SetKey("namespace", "message/4")
            .SetSchema("Message")
            .AddStringProperty("body", kIpsumText)
            .AddInt64Property("indexableInteger", 123)
            .SetCreationTimestampMs(kDefaultCreationTimestampMs)
            .Build();
    EXPECT_THAT(icing.Put(another_message).status(), ProtoIsOk());

    SearchSpecProto search_spec3;
    search_spec3.set_term_match_type(TermMatchType::EXACT_ONLY);
    search_spec3.set_query("name:person");
    JoinSpecProto* join_spec = search_spec3.mutable_join_spec();
    join_spec->set_parent_property_expression(
        std::string(JoinProcessor::kQualifiedIdExpr));
    join_spec->set_child_property_expression("senderQualifiedId");
    join_spec->set_aggregation_scoring_strategy(
        JoinSpecProto::AggregationScoringStrategy::COUNT);
    JoinSpecProto::NestedSpecProto* nested_spec =
        join_spec->mutable_nested_spec();
    SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
    nested_search_spec->set_term_match_type(TermMatchType::EXACT_ONLY);
    nested_search_spec->set_query("body:consectetur");
    *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();
    *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

    ResultSpecProto result_spec3 = ResultSpecProto::default_instance();
    result_spec3.set_max_joined_children_per_parent_to_return(
        std::numeric_limits<int32_t>::max());

    SearchResultProto results3 = icing.Search(
        search_spec3, ScoringSpecProto::default_instance(), result_spec3);
    ASSERT_THAT(results3.results(), SizeIs(1));
    EXPECT_THAT(results3.results(0).document().uri(), Eq("person"));
    EXPECT_THAT(results3.results(0).joined_results(), SizeIs(3));
    EXPECT_THAT(results3.results(0).joined_results(0).document().uri(),
                Eq("message/3"));
    EXPECT_THAT(results3.results(0).joined_results(1).document().uri(),
                Eq("message/2"));
    EXPECT_THAT(results3.results(0).joined_results(2).document().uri(),
                Eq("message/1"));
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       DocumentWithNoIndexedPropertyDoesntCauseRestoreIndex) {
  // 1. Create an index with a single document in it that has no indexed
  // content.
  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    // Set a schema for a single type that has no indexed properties.
    SchemaProto schema =
        SchemaBuilder()
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("Message")
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("unindexedField")
                                     .SetDataTypeString(TERM_MATCH_UNKNOWN,
                                                        TOKENIZER_NONE)
                                     .SetCardinality(CARDINALITY_REQUIRED))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("unindexedInteger")
                                     .SetDataTypeInt64(NUMERIC_MATCH_UNKNOWN)
                                     .SetCardinality(CARDINALITY_REQUIRED)))
            .Build();
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    // Add a document that contains no indexed properties.
    DocumentProto document =
        DocumentBuilder()
            .SetKey("icing", "fake_type/0")
            .SetSchema("Message")
            .AddStringProperty("unindexedField",
                               "Don't you dare search over this!")
            .AddInt64Property("unindexedInteger", -123)
            .Build();
    EXPECT_THAT(icing.Put(document).status(), ProtoIsOk());
  }

  // 2. Create the index again. This should NOT trigger a recovery of any kind.
  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    InitializeResultProto init_result = icing.Initialize();
    EXPECT_THAT(init_result.status(), ProtoIsOk());
    EXPECT_THAT(init_result.initialize_stats().document_store_data_status(),
                Eq(InitializeStatsProto::NO_DATA_LOSS));
    EXPECT_THAT(init_result.initialize_stats().document_store_recovery_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(init_result.initialize_stats().schema_store_recovery_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(init_result.initialize_stats().index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(
        init_result.initialize_stats().integer_index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(init_result.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       DocumentWithNoValidIndexedContentDoesntCauseRestoreIndex) {
  // 1. Create an index with a single document in it that has no valid indexed
  // tokens in its content.
  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    SchemaProto schema =
        SchemaBuilder()
            .AddType(
                SchemaTypeConfigBuilder()
                    .SetType("Message")
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("body")
                                     .SetDataTypeString(TERM_MATCH_PREFIX,
                                                        TOKENIZER_PLAIN)
                                     .SetCardinality(CARDINALITY_REQUIRED))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("indexableInteger")
                                     .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                     .SetCardinality(CARDINALITY_OPTIONAL))
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("senderQualifiedId")
                                     .SetDataTypeJoinableString(
                                         JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                     .SetCardinality(CARDINALITY_OPTIONAL)))
            .Build();
    // Set a schema for a single type that has no term, integer, join indexed
    // contents.
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    // Add a document that contains:
    // - No valid indexed string content - just punctuation
    // - No integer content - since it is an optional property
    // - No qualified id content - since it is an optional property
    DocumentProto document = DocumentBuilder()
                                 .SetKey("icing", "fake_type/0")
                                 .SetSchema("Message")
                                 .AddStringProperty("body", "?...!")
                                 .Build();
    EXPECT_THAT(icing.Put(document).status(), ProtoIsOk());
  }

  // 2. Create the index again. This should NOT trigger a recovery of any kind.
  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    InitializeResultProto init_result = icing.Initialize();
    EXPECT_THAT(init_result.status(), ProtoIsOk());
    EXPECT_THAT(init_result.initialize_stats().document_store_data_status(),
                Eq(InitializeStatsProto::NO_DATA_LOSS));
    EXPECT_THAT(init_result.initialize_stats().document_store_recovery_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(init_result.initialize_stats().schema_store_recovery_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(init_result.initialize_stats().index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(
        init_result.initialize_stats().integer_index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(init_result.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       InitializeShouldLogFunctionLatency) {
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(10);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  InitializeResultProto initialize_result_proto = icing.Initialize();
  EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
  EXPECT_THAT(initialize_result_proto.initialize_stats().latency_ms(), Eq(10));
}

TEST_F(IcingSearchEngineInitializationTest,
       InitializeShouldLogNumberOfDocuments) {
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("icing", "fake_type/1")
                                .SetSchema("Message")
                                .AddStringProperty("body", "message body")
                                .AddInt64Property("indexableInteger", 123)
                                .Build();
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("icing", "fake_type/2")
                                .SetSchema("Message")
                                .AddStringProperty("body", "message body")
                                .AddInt64Property("indexableInteger", 456)
                                .Build();

  {
    // Initialize and put a document.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    InitializeResultProto initialize_result_proto = icing.Initialize();
    EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
    EXPECT_THAT(initialize_result_proto.initialize_stats().num_documents(),
                Eq(0));

    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
  }

  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    InitializeResultProto initialize_result_proto = icing.Initialize();
    EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
    EXPECT_THAT(initialize_result_proto.initialize_stats().num_documents(),
                Eq(1));

    // Put another document.
    ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  }

  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    InitializeResultProto initialize_result_proto = icing.Initialize();
    EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
    EXPECT_THAT(initialize_result_proto.initialize_stats().num_documents(),
                Eq(2));
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       InitializeShouldNotLogRecoveryCauseForFirstTimeInitialize) {
  // Even though the fake timer will return 10, all the latency numbers related
  // to recovery / restoration should be 0 during the first-time initialization.
  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(10);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());
  InitializeResultProto initialize_result_proto = icing.Initialize();
  EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .document_store_recovery_cause(),
              Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .document_store_recovery_latency_ms(),
              Eq(0));
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().document_store_data_status(),
      Eq(InitializeStatsProto::NO_DATA_LOSS));
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().index_restoration_cause(),
      Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .integer_index_restoration_cause(),
              Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .qualified_id_join_index_restoration_cause(),
              Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().index_restoration_latency_ms(),
      Eq(0));
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().schema_store_recovery_cause(),
      Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .schema_store_recovery_latency_ms(),
              Eq(0));
}

TEST_F(IcingSearchEngineInitializationTest,
       InitializeShouldLogRecoveryCausePartialDataLoss) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("icing", "fake_type/0")
                               .SetSchema("Message")
                               .AddStringProperty("body", "message body")
                               .AddInt64Property("indexableInteger", 123)
                               .Build();

  {
    // Initialize and put a document.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(document).status(), ProtoIsOk());
  }

  {
    // Append a non-checksummed document. This will mess up the checksum of the
    // proto log, forcing it to rewind and later return a DATA_LOSS error.
    const std::string serialized_document = document.SerializeAsString();
    const std::string document_log_file = absl_ports::StrCat(
        GetDocumentDir(), "/", DocumentLogCreator::GetDocumentLogFilename());

    int64_t file_size = filesystem()->GetFileSize(document_log_file.c_str());
    filesystem()->PWrite(document_log_file.c_str(), file_size,
                         serialized_document.data(),
                         serialized_document.size());
  }

  {
    // Document store will rewind to previous checkpoint. The cause should be
    // DATA_LOSS and the data status should be PARTIAL_LOSS.
    auto fake_clock = std::make_unique<FakeClock>();
    fake_clock->SetTimerElapsedMilliseconds(10);
    TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                                std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::move(fake_clock), GetTestJniCache());
    InitializeResultProto initialize_result_proto = icing.Initialize();
    EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .document_store_recovery_cause(),
                Eq(InitializeStatsProto::DATA_LOSS));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .document_store_recovery_latency_ms(),
                Eq(10));
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().document_store_data_status(),
        Eq(InitializeStatsProto::PARTIAL_LOSS));
    // Document store rewinds to previous checkpoint and all derived files were
    // regenerated.
    // - Last stored doc id will be consistent with last added document ids in
    //   term/integer/join indices, so there will be no index restoration.
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .integer_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .index_restoration_latency_ms(),
                Eq(0));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .schema_store_recovery_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .schema_store_recovery_latency_ms(),
                Eq(0));
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       InitializeShouldLogRecoveryCauseCompleteDataLoss) {
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("icing", "fake_type/1")
                                .SetSchema("Message")
                                .AddStringProperty("body", "message body")
                                .AddInt64Property("indexableInteger", 123)
                                .Build();

  const std::string document_log_file = absl_ports::StrCat(
      GetDocumentDir(), "/", DocumentLogCreator::GetDocumentLogFilename());
  int64_t corruptible_offset;

  {
    // Initialize and put a document.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    // There's some space at the beginning of the file (e.g. header, kmagic,
    // etc) that is necessary to initialize the FileBackedProtoLog. We can't
    // corrupt that region, so we need to figure out the offset at which
    // documents will be written to - which is the file size after
    // initialization.
    corruptible_offset = filesystem()->GetFileSize(document_log_file.c_str());

    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(document1).status(), ProtoIsOk());
  }

  {
    // "Corrupt" the content written in the log. Make the corrupt document
    // smaller than our original one so we don't accidentally write past our
    // file.
    DocumentProto document =
        DocumentBuilder().SetKey("invalid_namespace", "invalid_uri").Build();
    std::string serialized_document = document.SerializeAsString();
    ASSERT_TRUE(filesystem()->PWrite(
        document_log_file.c_str(), corruptible_offset,
        serialized_document.data(), serialized_document.size()));

    PortableFileBackedProtoLog<DocumentWrapper>::Header header =
        ReadDocumentLogHeader(*filesystem(), document_log_file);

    // Set dirty bit to true to reflect that something changed in the log.
    header.SetDirtyFlag(true);
    header.SetHeaderChecksum(header.CalculateHeaderChecksum());

    WriteDocumentLogHeader(*filesystem(), document_log_file, header);
  }

  {
    // Document store will completely rewind. The cause should be DATA_LOSS and
    // the data status should be COMPLETE_LOSS.
    auto fake_clock = std::make_unique<FakeClock>();
    fake_clock->SetTimerElapsedMilliseconds(10);
    TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                                std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::move(fake_clock), GetTestJniCache());
    InitializeResultProto initialize_result_proto = icing.Initialize();
    EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .document_store_recovery_cause(),
                Eq(InitializeStatsProto::DATA_LOSS));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .document_store_recovery_latency_ms(),
                Eq(10));
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().document_store_data_status(),
        Eq(InitializeStatsProto::COMPLETE_LOSS));
    // The complete rewind of ground truth causes us to clear the index, but
    // that's not considered a restoration.
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .integer_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .index_restoration_latency_ms(),
                Eq(0));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .schema_store_recovery_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .schema_store_recovery_latency_ms(),
                Eq(0));
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       InitializeShouldLogRecoveryCauseIndexInconsistentWithGroundTruth) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("icing", "fake_type/0")
                               .SetSchema("Message")
                               .AddStringProperty("body", "message body")
                               .AddInt64Property("indexableInteger", 123)
                               .Build();
  {
    // Initialize and put a document.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(document).status(), ProtoIsOk());
  }

  {
    // Delete and re-initialize an empty index file to trigger
    // RestoreIndexIfNeeded.
    std::string idx_subdir = GetIndexDir() + "/idx";
    ASSERT_TRUE(filesystem()->DeleteDirectoryRecursively(idx_subdir.c_str()));
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Index> index,
        Index::Create(Index::Options(GetIndexDir(),
                                     /*index_merge_size=*/100,
                                     /*lite_index_sort_at_indexing=*/true,
                                     /*lite_index_sort_size=*/50),
                      filesystem(), icing_filesystem()));
    ICING_ASSERT_OK(index->PersistToDisk());
  }

  {
    // Index is empty but ground truth is not. Index should be restored due to
    // the inconsistency.
    auto fake_clock = std::make_unique<FakeClock>();
    fake_clock->SetTimerElapsedMilliseconds(10);
    TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                                std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::move(fake_clock), GetTestJniCache());
    InitializeResultProto initialize_result_proto = icing.Initialize();
    EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().index_restoration_cause(),
        Eq(InitializeStatsProto::INCONSISTENT_WITH_GROUND_TRUTH));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .integer_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .index_restoration_latency_ms(),
                Eq(10));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .document_store_recovery_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .document_store_recovery_latency_ms(),
                Eq(0));
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().document_store_data_status(),
        Eq(InitializeStatsProto::NO_DATA_LOSS));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .schema_store_recovery_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .schema_store_recovery_latency_ms(),
                Eq(0));
  }
}

TEST_F(
    IcingSearchEngineInitializationTest,
    InitializeShouldLogRecoveryCauseIntegerIndexInconsistentWithGroundTruth) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("icing", "fake_type/0")
                               .SetSchema("Message")
                               .AddStringProperty("body", "message body")
                               .AddInt64Property("indexableInteger", 123)
                               .Build();
  {
    // Initialize and put a document.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(document).status(), ProtoIsOk());
  }

  {
    // Delete the integer index file to trigger RestoreIndexIfNeeded.
    std::string integer_index_dir = GetIntegerIndexDir();
    filesystem()->DeleteDirectoryRecursively(integer_index_dir.c_str());
  }

  {
    // Index is empty but ground truth is not. Index should be restored due to
    // the inconsistency.
    auto fake_clock = std::make_unique<FakeClock>();
    fake_clock->SetTimerElapsedMilliseconds(10);
    TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                                std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::move(fake_clock), GetTestJniCache());
    InitializeResultProto initialize_result_proto = icing.Initialize();
    EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .integer_index_restoration_cause(),
                Eq(InitializeStatsProto::INCONSISTENT_WITH_GROUND_TRUTH));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .index_restoration_latency_ms(),
                Eq(10));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .document_store_recovery_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .document_store_recovery_latency_ms(),
                Eq(0));
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().document_store_data_status(),
        Eq(InitializeStatsProto::NO_DATA_LOSS));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .schema_store_recovery_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .schema_store_recovery_latency_ms(),
                Eq(0));
  }
}

TEST_F(
    IcingSearchEngineInitializationTest,
    InitializeShouldLogRecoveryCauseQualifiedIdJoinIndexInconsistentWithGroundTruth) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("indexableInteger")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("senderQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  DocumentProto person =
      DocumentBuilder()
          .SetKey("namespace", "person")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message =
      DocumentBuilder()
          .SetKey("namespace", "message/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message body")
          .AddInt64Property("indexableInteger", 123)
          .AddStringProperty("senderQualifiedId", "namespace#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  {
    // Initialize and put documents.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(person).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(message).status(), ProtoIsOk());
  }

  {
    // Delete the qualified id join index file to trigger RestoreIndexIfNeeded.
    std::string qualified_id_join_index_dir = GetQualifiedIdJoinIndexDir();
    filesystem()->DeleteDirectoryRecursively(
        qualified_id_join_index_dir.c_str());
  }

  {
    // Index is empty but ground truth is not. Index should be restored due to
    // the inconsistency.
    auto fake_clock = std::make_unique<FakeClock>();
    fake_clock->SetTimerElapsedMilliseconds(10);
    TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                                std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::move(fake_clock), GetTestJniCache());
    InitializeResultProto initialize_result_proto = icing.Initialize();
    EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .integer_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::INCONSISTENT_WITH_GROUND_TRUTH));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .index_restoration_latency_ms(),
                Eq(10));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .document_store_recovery_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .document_store_recovery_latency_ms(),
                Eq(0));
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().document_store_data_status(),
        Eq(InitializeStatsProto::NO_DATA_LOSS));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .schema_store_recovery_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .schema_store_recovery_latency_ms(),
                Eq(0));
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       InitializeShouldLogRecoveryCauseSchemaChangesOutOfSync) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("icing", "fake_type/0")
                               .SetSchema("Message")
                               .AddStringProperty("body", "message body")
                               .AddInt64Property("indexableInteger", 123)
                               .Build();
  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  {
    // Initialize and put one document.
    IcingSearchEngine icing(options, GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  }

  {
    // Simulate a schema change where power is lost after the schema is written.
    SchemaProto new_schema =
        SchemaBuilder()
            .AddType(
                SchemaTypeConfigBuilder(CreateMessageSchemaTypeConfig())
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("subject")
                                     .SetDataTypeString(TERM_MATCH_PREFIX,
                                                        TOKENIZER_PLAIN)
                                     .SetCardinality(CARDINALITY_OPTIONAL)))
            .Build();
    // Write the marker file
    std::string marker_filepath =
        absl_ports::StrCat(options.base_dir(), "/set_schema_marker");
    ScopedFd sfd(filesystem()->OpenForWrite(marker_filepath.c_str()));
    ASSERT_TRUE(sfd.is_valid());

    // Write the new schema
    FakeClock fake_clock;
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(filesystem(), GetSchemaDir(), &fake_clock,
                            feature_flags_.get()));
    ICING_EXPECT_OK(schema_store->SetSchema(
        new_schema, /*ignore_errors_and_delete_documents=*/false));
  }

  {
    // Both document store and index should be recovered from checksum mismatch.
    auto fake_clock = std::make_unique<FakeClock>();
    fake_clock->SetTimerElapsedMilliseconds(10);
    TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                                std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::move(fake_clock), GetTestJniCache());
    InitializeResultProto initialize_result_proto = icing.Initialize();
    EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().index_restoration_cause(),
        Eq(InitializeStatsProto::SCHEMA_CHANGES_OUT_OF_SYNC));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .integer_index_restoration_cause(),
                Eq(InitializeStatsProto::SCHEMA_CHANGES_OUT_OF_SYNC));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::SCHEMA_CHANGES_OUT_OF_SYNC));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .index_restoration_latency_ms(),
                Eq(10));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .document_store_recovery_cause(),
                Eq(InitializeStatsProto::SCHEMA_CHANGES_OUT_OF_SYNC));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .document_store_recovery_latency_ms(),
                Eq(10));
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().document_store_data_status(),
        Eq(InitializeStatsProto::NO_DATA_LOSS));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .schema_store_recovery_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .schema_store_recovery_latency_ms(),
                Eq(0));
  }

  {
    // No recovery should be needed.
    auto fake_clock = std::make_unique<FakeClock>();
    fake_clock->SetTimerElapsedMilliseconds(10);
    TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                                std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::move(fake_clock), GetTestJniCache());
    InitializeResultProto initialize_result_proto = icing.Initialize();
    EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .integer_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .index_restoration_latency_ms(),
                Eq(0));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .document_store_recovery_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .document_store_recovery_latency_ms(),
                Eq(0));
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().document_store_data_status(),
        Eq(InitializeStatsProto::NO_DATA_LOSS));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .schema_store_recovery_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .schema_store_recovery_latency_ms(),
                Eq(0));
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       InitializeShouldLogRecoveryCauseIndexIOError) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("icing", "fake_type/0")
                               .SetSchema("Message")
                               .AddStringProperty("body", "message body")
                               .AddInt64Property("indexableInteger", 123)
                               .Build();
  {
    // Initialize and put one document.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  }

  std::string lite_index_buffer_file_path =
      absl_ports::StrCat(GetIndexDir(), "/idx/lite.hb");
  auto mock_icing_filesystem = std::make_unique<IcingMockFilesystem>();
  EXPECT_CALL(*mock_icing_filesystem, OpenForWrite(_))
      .WillRepeatedly(DoDefault());
  // This fails Index::Create() once.
  EXPECT_CALL(*mock_icing_filesystem,
              OpenForWrite(Eq(lite_index_buffer_file_path)))
      .WillOnce(Return(-1))
      .WillRepeatedly(DoDefault());

  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(10);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::move(mock_icing_filesystem),
                              std::move(fake_clock), GetTestJniCache());

  InitializeResultProto initialize_result_proto = icing.Initialize();
  EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().index_restoration_cause(),
      Eq(InitializeStatsProto::IO_ERROR));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .integer_index_restoration_cause(),
              Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .qualified_id_join_index_restoration_cause(),
              Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().index_restoration_latency_ms(),
      Eq(10));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .document_store_recovery_cause(),
              Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .document_store_recovery_latency_ms(),
              Eq(0));
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().document_store_data_status(),
      Eq(InitializeStatsProto::NO_DATA_LOSS));
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().schema_store_recovery_cause(),
      Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .schema_store_recovery_latency_ms(),
              Eq(0));
}

TEST_F(IcingSearchEngineInitializationTest,
       InitializeShouldLogRecoveryCauseIntegerIndexIOError) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("icing", "fake_type/0")
                               .SetSchema("Message")
                               .AddStringProperty("body", "message body")
                               .AddInt64Property("indexableInteger", 123)
                               .Build();
  {
    // Initialize and put one document.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  }

  std::string integer_index_metadata_file =
      absl_ports::StrCat(GetIntegerIndexDir(), "/integer_index.m");
  auto mock_filesystem = std::make_unique<MockFilesystem>();
  EXPECT_CALL(*mock_filesystem, OpenForWrite(_)).WillRepeatedly(DoDefault());
  // This fails IntegerIndex::Create() once.
  EXPECT_CALL(*mock_filesystem, OpenForWrite(Eq(integer_index_metadata_file)))
      .WillOnce(Return(-1))
      .WillRepeatedly(DoDefault());

  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(10);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::move(mock_filesystem),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());

  InitializeResultProto initialize_result_proto = icing.Initialize();
  EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().index_restoration_cause(),
      Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .integer_index_restoration_cause(),
              Eq(InitializeStatsProto::IO_ERROR));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .qualified_id_join_index_restoration_cause(),
              Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().index_restoration_latency_ms(),
      Eq(10));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .document_store_recovery_cause(),
              Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .document_store_recovery_latency_ms(),
              Eq(0));
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().document_store_data_status(),
      Eq(InitializeStatsProto::NO_DATA_LOSS));
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().schema_store_recovery_cause(),
      Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .schema_store_recovery_latency_ms(),
              Eq(0));
}

TEST_F(IcingSearchEngineInitializationTest,
       InitializeShouldLogRecoveryCauseQualifiedIdJoinIndexIOError) {
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("indexableInteger")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("senderQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  DocumentProto person =
      DocumentBuilder()
          .SetKey("namespace", "person")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message =
      DocumentBuilder()
          .SetKey("namespace", "message/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message body")
          .AddInt64Property("indexableInteger", 123)
          .AddStringProperty("senderQualifiedId", "namespace#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  {
    // Initialize and put documents.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(person).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(message).status(), ProtoIsOk());
  }

  std::string qualified_id_join_index_metadata_file =
      absl_ports::StrCat(GetQualifiedIdJoinIndexDir(), "/metadata");
  auto mock_filesystem = std::make_unique<MockFilesystem>();
  EXPECT_CALL(*mock_filesystem, PRead(A<const char*>(), _, _, _))
      .WillRepeatedly(DoDefault());
  // This fails QualifiedIdJoinIndexImplV3::Create() once.
  EXPECT_CALL(*mock_filesystem, OpenForWrite(_)).WillRepeatedly(DoDefault());
  EXPECT_CALL(*mock_filesystem, OpenForWrite(Matcher<const char*>(
                                    Eq(qualified_id_join_index_metadata_file))))
      .WillOnce(Return(-1))
      .WillRepeatedly(DoDefault());

  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(10);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::move(mock_filesystem),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());

  InitializeResultProto initialize_result_proto = icing.Initialize();
  EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().index_restoration_cause(),
      Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .integer_index_restoration_cause(),
              Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .qualified_id_join_index_restoration_cause(),
              Eq(InitializeStatsProto::IO_ERROR));
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().index_restoration_latency_ms(),
      Eq(10));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .document_store_recovery_cause(),
              Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .document_store_recovery_latency_ms(),
              Eq(0));
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().document_store_data_status(),
      Eq(InitializeStatsProto::NO_DATA_LOSS));
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().schema_store_recovery_cause(),
      Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .schema_store_recovery_latency_ms(),
              Eq(0));
}

TEST_F(IcingSearchEngineInitializationTest,
       InitializeShouldLogRecoveryCauseDocStoreIOError) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("icing", "fake_type/0")
                               .SetSchema("Message")
                               .AddStringProperty("body", "message body")
                               .AddInt64Property("indexableInteger", 123)
                               .Build();
  {
    // Initialize and put one document.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  }

  std::string document_store_header_file_path =
      absl_ports::StrCat(GetDocumentDir(), "/document_store_header");
  auto mock_filesystem = std::make_unique<MockFilesystem>();
  EXPECT_CALL(*mock_filesystem, Read(A<const char*>(), _, _))
      .WillRepeatedly(DoDefault());
  // This fails DocumentStore::InitializeDerivedFiles() once.
  EXPECT_CALL(
      *mock_filesystem,
      Read(Matcher<const char*>(Eq(document_store_header_file_path)), _, _))
      .WillOnce(Return(false))
      .WillRepeatedly(DoDefault());

  auto fake_clock = std::make_unique<FakeClock>();
  fake_clock->SetTimerElapsedMilliseconds(10);
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::move(mock_filesystem),
                              std::make_unique<IcingFilesystem>(),
                              std::move(fake_clock), GetTestJniCache());

  InitializeResultProto initialize_result_proto = icing.Initialize();
  EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .document_store_recovery_cause(),
              Eq(InitializeStatsProto::IO_ERROR));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .document_store_recovery_latency_ms(),
              Eq(10));
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().document_store_data_status(),
      Eq(InitializeStatsProto::NO_DATA_LOSS));
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().index_restoration_cause(),
      Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .integer_index_restoration_cause(),
              Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .qualified_id_join_index_restoration_cause(),
              Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().index_restoration_latency_ms(),
      Eq(0));
  EXPECT_THAT(
      initialize_result_proto.initialize_stats().schema_store_recovery_cause(),
      Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result_proto.initialize_stats()
                  .schema_store_recovery_latency_ms(),
              Eq(0));
}

TEST_F(IcingSearchEngineInitializationTest,
       InitializeShouldLogRecoveryCauseSchemaStoreIOError) {
  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
  }

  {
    // Delete the schema store type mapper to trigger an I/O error.
    std::string schema_store_header_file_path =
        GetSchemaDir() + "/schema_type_mapper";
    ASSERT_TRUE(filesystem()->DeleteDirectoryRecursively(
        schema_store_header_file_path.c_str()));
  }

  {
    auto fake_clock = std::make_unique<FakeClock>();
    fake_clock->SetTimerElapsedMilliseconds(10);
    TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                                std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::move(fake_clock), GetTestJniCache());
    InitializeResultProto initialize_result_proto = icing.Initialize();
    EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .schema_store_recovery_cause(),
                Eq(InitializeStatsProto::IO_ERROR));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .schema_store_recovery_latency_ms(),
                Eq(10));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .document_store_recovery_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .document_store_recovery_latency_ms(),
                Eq(0));
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().document_store_data_status(),
        Eq(InitializeStatsProto::NO_DATA_LOSS));
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .integer_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result_proto.initialize_stats()
                    .index_restoration_latency_ms(),
                Eq(0));
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       InitializeShouldLogNumberOfSchemaTypes) {
  {
    // Initialize an empty storage.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    InitializeResultProto initialize_result_proto = icing.Initialize();
    EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
    // There should be 0 schema types.
    EXPECT_THAT(initialize_result_proto.initialize_stats().num_schema_types(),
                Eq(0));

    // Set a schema with one type config.
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
  }

  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    InitializeResultProto initialize_result_proto = icing.Initialize();
    EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
    // There should be 1 schema type.
    EXPECT_THAT(initialize_result_proto.initialize_stats().num_schema_types(),
                Eq(1));

    // Create and set a schema with two type configs: Email and Message.
    SchemaProto schema = CreateEmailSchema();
    *schema.add_types() = CreateMessageSchemaTypeConfig();

    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
  }

  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    InitializeResultProto initialize_result_proto = icing.Initialize();
    EXPECT_THAT(initialize_result_proto.status(), ProtoIsOk());
    EXPECT_THAT(initialize_result_proto.initialize_stats().num_schema_types(),
                Eq(2));
  }
}

struct IcingSearchEngineInitializationVersionChangeTestParam {
  version_util::VersionInfo existing_version_info;
  std::unordered_set<IcingSearchEngineFeatureInfoProto::FlaggedFeatureType>
      existing_enabled_features;

  explicit IcingSearchEngineInitializationVersionChangeTestParam(
      version_util::VersionInfo version_info_in,
      std::unordered_set<IcingSearchEngineFeatureInfoProto::FlaggedFeatureType>
          existing_enabled_features_in)
      : existing_version_info(std::move(version_info_in)),
        existing_enabled_features(std::move(existing_enabled_features_in)) {}
};

class IcingSearchEngineInitializationVersionChangeTest
    : public IcingSearchEngineInitializationTest,
      public ::testing::WithParamInterface<
          IcingSearchEngineInitializationVersionChangeTestParam> {};

TEST_P(IcingSearchEngineInitializationVersionChangeTest,
       RecoverFromVersionChangeOrUnknownFlagChange) {
  // TODO(b/280697513): test backup schema migration
  // Test the following scenario: version change. All derived data should be
  // rebuilt. We test this by manually adding some invalid derived data and
  // verifying they're removed due to rebuild.
  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("indexableInteger")
                                        .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("senderQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("embedding")
                                        .SetDataTypeVector(
                                            EMBEDDING_INDEXING_LINEAR_SEARCH)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  DocumentProto person1 =
      DocumentBuilder()
          .SetKey("namespace", "person/1")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto person2 =
      DocumentBuilder()
          .SetKey("namespace", "person/2")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message =
      DocumentBuilder()
          .SetKey("namespace", "message")
          .SetSchema("Message")
          .AddStringProperty("body", "correct message")
          .AddInt64Property("indexableInteger", 123)
          .AddStringProperty("senderQualifiedId", "namespace#person/1")
          .AddVectorProperty(
              "embedding", CreateVector("my_model", {0.1, 0.2, 0.3, 0.4, 0.5}))
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  IcingSearchEngineOptions icing_options = GetDefaultIcingOptions();

  {
    // Initializes folder and schema, index person1 and person2
    TestIcingSearchEngine icing(icing_options, std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());
    EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
    EXPECT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(person1).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(person2).status(), ProtoIsOk());
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  {
    // Manually:
    // - Put message into DocumentStore
    // - But add some incorrect data for message into 3 indices
    // - Change version file
    //
    // These will make sure last_added_document_id is consistent with
    // last_stored_document_id, so if Icing didn't handle version change
    // correctly, then the index won't be rebuilt.
    FakeClock fake_clock;
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(filesystem(), GetSchemaDir(), &fake_clock,
                            feature_flags_.get()));

    ICING_ASSERT_OK_AND_ASSIGN(
        BlobStore blob_store,
        BlobStore::Create(filesystem(), GetBlobDir(), &fake_clock,
                          /*orphan_blob_time_to_live_ms=*/0,
                          PortableFileBackedProtoLog<
                              BlobInfoProto>::kDefaultCompressionLevel));

    // Put message into DocumentStore
    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(filesystem(), GetDocumentDir(), &fake_clock,
                              schema_store.get(), feature_flags_.get(),
                              /*force_recovery_and_revalidate_documents=*/false,
                              /*pre_mapping_fbv=*/false,
                              /*use_persistent_hash_map=*/true,
                              PortableFileBackedProtoLog<
                                  DocumentWrapper>::kDefaultCompressionLevel,
                              /*initialize_stats=*/nullptr));
    std::unique_ptr<DocumentStore> document_store =
        std::move(create_result.document_store);
    ICING_ASSERT_OK_AND_ASSIGN(DocumentStore::PutResult put_result,
                               document_store->Put(message));
    DocumentId doc_id = put_result.new_document_id;

    // Index doc_id with incorrect data
    Index::Options options(GetIndexDir(), /*index_merge_size=*/1024 * 1024,
                           /*lite_index_sort_at_indexing=*/true,
                           /*lite_index_sort_size=*/1024 * 8);
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<Index> index,
        Index::Create(options, filesystem(), icing_filesystem()));

    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<IntegerIndex> integer_index,
        IntegerIndex::Create(*filesystem(), GetIntegerIndexDir(),
                             /*num_data_threshold_for_bucket_split=*/65536,
                             /*pre_mapping_fbv=*/false));

    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdJoinIndex> qualified_id_join_index,
        QualifiedIdJoinIndexImplV3::Create(
            *filesystem(), GetQualifiedIdJoinIndexDir(), *feature_flags_));

    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<EmbeddingIndex> embedding_index,
        EmbeddingIndex::Create(filesystem(), GetEmbeddingIndexDir(),
                               &fake_clock, feature_flags_.get()));

    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<TermIndexingHandler> term_indexing_handler,
        TermIndexingHandler::Create(
            &fake_clock, normalizer_.get(), index.get(),
            /*build_property_existence_metadata_hits=*/true));
    ICING_ASSERT_OK_AND_ASSIGN(std::unique_ptr<IntegerSectionIndexingHandler>
                                   integer_section_indexing_handler,
                               IntegerSectionIndexingHandler::Create(
                                   &fake_clock, integer_index.get()));
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<QualifiedIdJoinIndexingHandler>
            qualified_id_join_indexing_handler,
        QualifiedIdJoinIndexingHandler::Create(
            &fake_clock, document_store.get(), qualified_id_join_index.get()));
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<EmbeddingIndexingHandler> embedding_indexing_handler,
        EmbeddingIndexingHandler::Create(&fake_clock, embedding_index.get(),
                                         /*enable_embedding_index=*/true));
    std::vector<std::unique_ptr<DataIndexingHandler>> handlers;
    handlers.push_back(std::move(term_indexing_handler));
    handlers.push_back(std::move(integer_section_indexing_handler));
    handlers.push_back(std::move(qualified_id_join_indexing_handler));
    handlers.push_back(std::move(embedding_indexing_handler));
    IndexProcessor index_processor(std::move(handlers), &fake_clock);

    DocumentProto incorrect_message =
        DocumentBuilder()
            .SetKey("namespace", "message")
            .SetSchema("Message")
            .AddStringProperty("body", "wrong message")
            .AddInt64Property("indexableInteger", 456)
            .AddStringProperty("senderQualifiedId", "namespace#person/2")
            .AddVectorProperty(
                "embedding",
                CreateVector("my_model", {-0.1, -0.2, -0.3, -0.4, -0.5}))
            .SetCreationTimestampMs(kDefaultCreationTimestampMs)
            .Build();
    ICING_ASSERT_OK_AND_ASSIGN(
        TokenizedDocument tokenized_document,
        TokenizedDocument::Create(schema_store.get(), lang_segmenter_.get(),
                                  std::move(incorrect_message)));
    ICING_ASSERT_OK(index_processor.IndexDocument(tokenized_document, doc_id,
                                                  put_result.old_document_id));

    // Rewrite existing data's version files
    ICING_ASSERT_OK(
        version_util::DiscardVersionFiles(*filesystem(), GetVersionFileDir()));
    const version_util::VersionInfo& existing_version_info =
        GetParam().existing_version_info;
    ICING_ASSERT_OK(version_util::WriteV1Version(
        *filesystem(), GetVersionFileDir(), existing_version_info));

    if (existing_version_info.version >= version_util::kFirstV2Version) {
      IcingSearchEngineVersionProto version_proto;
      version_proto.set_version(existing_version_info.version);
      version_proto.set_max_version(existing_version_info.max_version);
      auto* enabled_features = version_proto.mutable_enabled_features();
      for (const auto& feature : GetParam().existing_enabled_features) {
        enabled_features->Add(version_util::GetFeatureInfoProto(feature));
      }
      version_util::WriteV2Version(
          *filesystem(), GetVersionFileDir(),
          std::make_unique<IcingSearchEngineVersionProto>(
              std::move(version_proto)));
    }
  }

  // Mock filesystem to observe and check the behavior of all indices.
  TestIcingSearchEngine icing(GetDefaultIcingOptions(),
                              std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::make_unique<FakeClock>(), GetTestJniCache());
  InitializeResultProto initialize_result = icing.Initialize();
  EXPECT_THAT(initialize_result.status(), ProtoIsOk());

  // Derived files restoration should be triggered here. Incorrect data should
  // be deleted and correct data of message should be indexed.
  // Here we're recovering from a version change or a flag change that requires
  // rebuilding all derived files.
  //
  // TODO(b/314816301): test individual derived files rebuilds due to change
  // in trunk stable feature flags.
  // i.e. Test individual rebuilding for each of:
  //  - document store
  //  - schema store
  //  - term index
  //  - numeric index
  //  - qualified id join index
  //  - embedding index
  InitializeStatsProto::RecoveryCause expected_recovery_cause =
      GetParam().existing_version_info.version != version_util::kVersion
          ? InitializeStatsProto::VERSION_CHANGED
          : InitializeStatsProto::FEATURE_FLAG_CHANGED;
  EXPECT_THAT(
      initialize_result.initialize_stats().document_store_recovery_cause(),
      Eq(expected_recovery_cause));
  EXPECT_THAT(
      initialize_result.initialize_stats().schema_store_recovery_cause(),
      Eq(expected_recovery_cause));
  EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
              Eq(expected_recovery_cause));
  EXPECT_THAT(
      initialize_result.initialize_stats().integer_index_restoration_cause(),
      Eq(expected_recovery_cause));
  EXPECT_THAT(initialize_result.initialize_stats()
                  .qualified_id_join_index_restoration_cause(),
              Eq(expected_recovery_cause));
  EXPECT_THAT(
      initialize_result.initialize_stats().embedding_index_restoration_cause(),
      Eq(expected_recovery_cause));

  // Manually check version file
  ICING_ASSERT_OK_AND_ASSIGN(
      IcingSearchEngineVersionProto version_proto_after_init,
      version_util::ReadVersion(*filesystem(), GetVersionFileDir(),
                                GetIndexDir()));
  EXPECT_THAT(version_proto_after_init.version(), Eq(version_util::kVersion));
  EXPECT_THAT(version_proto_after_init.max_version(),
              Eq(std::max(version_util::kVersion,
                          GetParam().existing_version_info.max_version)));

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      message;

  // Verify term search
  SearchSpecProto search_spec1;
  search_spec1.set_query("body:correct");
  search_spec1.set_term_match_type(TermMatchType::EXACT_ONLY);
  SearchResultProto search_result_proto1 =
      icing.Search(search_spec1, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto1, EqualsSearchResultIgnoreStatsAndScores(
                                        expected_search_result_proto));

  // Verify numeric (integer) search
  SearchSpecProto search_spec2;
  search_spec2.set_query("indexableInteger == 123");
  search_spec2.add_enabled_features(std::string(kNumericSearchFeature));

  SearchResultProto search_result_google::protobuf =
      icing.Search(search_spec2, ScoringSpecProto::default_instance(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_google::protobuf, EqualsSearchResultIgnoreStatsAndScores(
                                        expected_search_result_proto));

  // Verify join search: join a query for `name:person` with a child query for
  // `body:message` based on the child's `senderQualifiedId` field.
  SearchSpecProto search_spec3;
  search_spec3.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec3.set_query("name:person");
  JoinSpecProto* join_spec = search_spec3.mutable_join_spec();
  join_spec->set_parent_property_expression(
      std::string(JoinProcessor::kQualifiedIdExpr));
  join_spec->set_child_property_expression("senderQualifiedId");
  join_spec->set_aggregation_scoring_strategy(
      JoinSpecProto::AggregationScoringStrategy::COUNT);
  JoinSpecProto::NestedSpecProto* nested_spec =
      join_spec->mutable_nested_spec();
  SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
  nested_search_spec->set_term_match_type(TermMatchType::EXACT_ONLY);
  nested_search_spec->set_query("body:message");
  *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();
  *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

  ResultSpecProto result_spec3 = ResultSpecProto::default_instance();
  result_spec3.set_max_joined_children_per_parent_to_return(
      std::numeric_limits<int32_t>::max());

  SearchResultProto expected_join_search_result_proto;
  expected_join_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  // Person 1 with message
  SearchResultProto::ResultProto* result_proto =
      expected_join_search_result_proto.mutable_results()->Add();
  *result_proto->mutable_document() = person1;
  *result_proto->mutable_joined_results()->Add()->mutable_document() = message;
  // Person 2 without children
  *expected_join_search_result_proto.mutable_results()
       ->Add()
       ->mutable_document() = person2;

  SearchResultProto search_result_proto3 = icing.Search(
      search_spec3, ScoringSpecProto::default_instance(), result_spec3);
  EXPECT_THAT(search_result_proto3, EqualsSearchResultIgnoreStatsAndScores(
                                        expected_join_search_result_proto));

  // Verify embedding search
  SearchSpecProto search_spec4;
  search_spec4.set_query("semanticSearch(getEmbeddingParameter(0), 0)");
  *search_spec4.add_embedding_query_vectors() =
      CreateVector("my_model", {1, 1, 1, 1, 1});
  search_spec4.set_embedding_query_metric_type(
      SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT);
  search_spec4.add_enabled_features(
      std::string(kListFilterQueryLanguageFeature));

  SearchResultProto search_result_proto4 =
      icing.Search(search_spec4, ScoringSpecProto::default_instance(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto4, EqualsSearchResultIgnoreStatsAndScores(
                                        expected_search_result_proto));
}

INSTANTIATE_TEST_SUITE_P(
    IcingSearchEngineInitializationVersionChangeTest,
    IcingSearchEngineInitializationVersionChangeTest,
    testing::Values(
        // Manually change existing data set's version to kVersion + 1. When
        // initializing, it will detect "rollback".
        IcingSearchEngineInitializationVersionChangeTestParam(
            version_util::VersionInfo(
                /*version_in=*/version_util::kVersion + 1,
                /*max_version_in=*/version_util::kVersion + 1),
            /*existing_enabled_features_in=*/{}),

        // Currently we don't have any "upgrade" that requires rebuild derived
        // files, so skip this case until we have a case for it.

        // Manually change existing data set's version to kVersion - 1 and
        // max_version to kVersion. When initializing, it will detect "roll
        // forward".
        IcingSearchEngineInitializationVersionChangeTestParam(
            version_util::VersionInfo(
                /*version_in=*/version_util::kVersion - 1,
                /*max_version_in=*/version_util::kVersion),
            /*existing_enabled_features_in=*/{}),

        // Manually change existing data set's version to 0 and max_version to
        // 0. When initializing, it will detect "version 0 upgrade".
        //
        // Note: in reality, version 0 won't be written into version file, but
        // it is ok here since it is hack to simulate version 0 situation.
        IcingSearchEngineInitializationVersionChangeTestParam(
            version_util::VersionInfo(
                /*version_in=*/0,
                /*max_version_in=*/0),
            /*existing_enabled_features_in=*/{}),

        // Manually change existing data set's version to 0 and max_version to
        // kVersion. When initializing, it will detect "version 0 roll forward".
        //
        // Note: in reality, version 0 won't be written into version file, but
        // it is ok here since it is hack to simulate version 0 situation.
        IcingSearchEngineInitializationVersionChangeTestParam(
            version_util::VersionInfo(
                /*version_in=*/0,
                /*max_version_in=*/version_util::kVersion),
            /*existing_enabled_features_in=*/{}),

        // Manually write an unknown feature in the version proto while keeping
        // version the same as kVersion.
        //
        // Result: this will rebuild all derived files with restoration cause
        // FEATURE_FLAG_CHANGED
        IcingSearchEngineInitializationVersionChangeTestParam(
            version_util::VersionInfo(
                /*version_in=*/version_util::kVersion,
                /*max_version_in=*/version_util::kVersion),
            /*existing_enabled_features_in=*/{
                IcingSearchEngineFeatureInfoProto::UNKNOWN})));

class IcingSearchEngineInitializationChangePropertyExistenceHitsFlagTest
    : public IcingSearchEngineInitializationTest,
      public ::testing::WithParamInterface<std::tuple<bool, bool>> {};
TEST_P(IcingSearchEngineInitializationChangePropertyExistenceHitsFlagTest,
       ChangePropertyExistenceHitsFlagTest) {
  bool before_build_property_existence_metadata_hits = std::get<0>(GetParam());
  bool after_build_property_existence_metadata_hits = std::get<1>(GetParam());
  bool flag_changed = before_build_property_existence_metadata_hits !=
                      after_build_property_existence_metadata_hits;

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Value")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REPEATED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("timestamp")
                                        .SetDataType(TYPE_INT64)
                                        .SetCardinality(CARDINALITY_OPTIONAL))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("score")
                                        .SetDataType(TYPE_DOUBLE)
                                        .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // Create a document with every property.
  DocumentProto document0 = DocumentBuilder()
                                .SetKey("icing", "uri0")
                                .SetSchema("Value")
                                .SetCreationTimestampMs(1)
                                .AddStringProperty("body", "foo")
                                .AddInt64Property("timestamp", 123)
                                .AddDoubleProperty("score", 456.789)
                                .Build();
  // Create a document with missing body.
  DocumentProto document1 = DocumentBuilder()
                                .SetKey("icing", "uri1")
                                .SetSchema("Value")
                                .SetCreationTimestampMs(1)
                                .AddInt64Property("timestamp", 123)
                                .AddDoubleProperty("score", 456.789)
                                .Build();
  // Create a document with missing timestamp.
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("icing", "uri2")
                                .SetSchema("Value")
                                .SetCreationTimestampMs(1)
                                .AddStringProperty("body", "foo")
                                .AddDoubleProperty("score", 456.789)
                                .Build();

  // 1. Create an index with the 3 documents.
  {
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_build_property_existence_metadata_hits(
        before_build_property_existence_metadata_hits);
    TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document0).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  }

  // 2. Create the index again with
  // after_build_property_existence_metadata_hits.
  //
  // Mock filesystem to observe and check the behavior of all indices.
  auto mock_filesystem = std::make_unique<MockFilesystem>();
  EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
      .WillRepeatedly(DoDefault());
  // Ensure that the term index is rebuilt if the flag is changed.
  EXPECT_CALL(*mock_filesystem,
              DeleteDirectoryRecursively(EndsWith("/index_dir")))
      .Times(flag_changed ? 1 : 0);

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_build_property_existence_metadata_hits(
      after_build_property_existence_metadata_hits);
  TestIcingSearchEngine icing(options, std::move(mock_filesystem),
                              std::make_unique<IcingFilesystem>(),
                              std::make_unique<FakeClock>(), GetTestJniCache());
  InitializeResultProto initialize_result = icing.Initialize();
  ASSERT_THAT(initialize_result.status(), ProtoIsOk());
  // Ensure that the term index is rebuilt if the flag is changed.
  EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
              Eq(flag_changed ? InitializeStatsProto::FEATURE_FLAG_CHANGED
                              : InitializeStatsProto::NONE));
  EXPECT_THAT(
      initialize_result.initialize_stats().integer_index_restoration_cause(),
      Eq(InitializeStatsProto::NONE));
  EXPECT_THAT(initialize_result.initialize_stats()
                  .qualified_id_join_index_restoration_cause(),
              Eq(InitializeStatsProto::NONE));

  // Get all documents that have "body".
  SearchSpecProto search_spec;
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
  search_spec.add_enabled_features(std::string(kHasPropertyFunctionFeature));
  search_spec.add_enabled_features(
      std::string(kListFilterQueryLanguageFeature));
  search_spec.set_query("hasProperty(\"body\")");
  SearchResultProto results = icing.Search(search_spec, GetDefaultScoringSpec(),
                                           ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  if (after_build_property_existence_metadata_hits) {
    EXPECT_THAT(results.results(), SizeIs(2));
    EXPECT_THAT(results.results(0).document(), EqualsProto(document2));
    EXPECT_THAT(results.results(1).document(), EqualsProto(document0));
  } else {
    EXPECT_THAT(results.results(), IsEmpty());
  }

  // Get all documents that have "timestamp".
  search_spec.set_query("hasProperty(\"timestamp\")");
  results = icing.Search(search_spec, GetDefaultScoringSpec(),
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  if (after_build_property_existence_metadata_hits) {
    EXPECT_THAT(results.results(), SizeIs(2));
    EXPECT_THAT(results.results(0).document(), EqualsProto(document1));
    EXPECT_THAT(results.results(1).document(), EqualsProto(document0));
  } else {
    EXPECT_THAT(results.results(), IsEmpty());
  }

  // Get all documents that have "score".
  search_spec.set_query("hasProperty(\"score\")");
  results = icing.Search(search_spec, GetDefaultScoringSpec(),
                         ResultSpecProto::default_instance());
  EXPECT_THAT(results.status(), ProtoIsOk());
  if (after_build_property_existence_metadata_hits) {
    EXPECT_THAT(results.results(), SizeIs(3));
    EXPECT_THAT(results.results(0).document(), EqualsProto(document2));
    EXPECT_THAT(results.results(1).document(), EqualsProto(document1));
    EXPECT_THAT(results.results(2).document(), EqualsProto(document0));
  } else {
    EXPECT_THAT(results.results(), IsEmpty());
  }
}

INSTANTIATE_TEST_SUITE_P(
    IcingSearchEngineInitializationChangePropertyExistenceHitsFlagTest,
    IcingSearchEngineInitializationChangePropertyExistenceHitsFlagTest,
    testing::Values(std::make_tuple(false, false), std::make_tuple(false, true),
                    std::make_tuple(true, false), std::make_tuple(true, true)));

class IcingSearchEngineInitializationChangeEmbeddingFlagTest
    : public IcingSearchEngineInitializationTest,
      public ::testing::WithParamInterface<std::vector<bool>> {};
TEST_P(IcingSearchEngineInitializationChangeEmbeddingFlagTest,
       ChangeEnableEmbeddingIndexFlagTest) {
  std::vector<bool> enable_embedding_index_flags = GetParam();

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Message").AddProperty(
              PropertyConfigBuilder()
                  .SetName("embedding")
                  .SetDataTypeVector(EMBEDDING_INDEXING_LINEAR_SEARCH)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  // Create a document with an embedding.
  DocumentProto document =
      DocumentBuilder()
          .SetKey("icing", "uri")
          .SetSchema("Message")
          .SetCreationTimestampMs(1)
          .AddVectorProperty(
              "embedding", CreateVector("my_model", {0.1, 0.2, 0.3, 0.4, 0.5}))
          .Build();

  // Create icing with a document that has an embedding.
  {
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_enable_embedding_index(enable_embedding_index_flags[0]);
    TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  }

  // Create icing multiple times with different enable_embedding_index flags.
  for (int i = 1; i < enable_embedding_index_flags.size(); ++i) {
    bool flag_changed =
        enable_embedding_index_flags[i] != enable_embedding_index_flags[i - 1];

    // Ensure that the embedding index is rebuilt if the flag is changed.
    auto mock_filesystem = std::make_unique<MockFilesystem>();
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
        .WillRepeatedly(DoDefault());
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/embedding_index_dir")))
        .Times(flag_changed ? 1 : 0);

    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_enable_embedding_index(enable_embedding_index_flags[i]);
    TestIcingSearchEngine icing(options, std::move(mock_filesystem),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());
    InitializeResultProto initialize_result = icing.Initialize();
    ASSERT_THAT(initialize_result.status(), ProtoIsOk());
    // Ensure that the embedding index is rebuilt if the flag is changed.
    EXPECT_THAT(initialize_result.initialize_stats()
                    .embedding_index_restoration_cause(),
                Eq(flag_changed ? InitializeStatsProto::FEATURE_FLAG_CHANGED
                                : InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(
        initialize_result.initialize_stats().integer_index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));

    // Write a embedding query that should match the document if the embedding
    // index is enabled.
    SearchSpecProto search_spec;
    search_spec.set_query("semanticSearch(getEmbeddingParameter(0), 0)");
    *search_spec.add_embedding_query_vectors() =
        CreateVector("my_model", {1, 1, 1, 1, 1});
    search_spec.set_embedding_query_metric_type(
        SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT);
    search_spec.add_enabled_features(
        std::string(kListFilterQueryLanguageFeature));

    SearchResultProto results =
        icing.Search(search_spec, GetDefaultScoringSpec(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(results.status(), ProtoIsOk());
    // The document should be returned if the embedding index is enabled.
    if (enable_embedding_index_flags[i]) {
      EXPECT_THAT(results.results(), SizeIs(1));
      EXPECT_THAT(results.results(0).document(), EqualsProto(document));
    } else {
      EXPECT_THAT(results.results(), IsEmpty());
    }
  }
}

TEST_P(IcingSearchEngineInitializationChangeEmbeddingFlagTest,
       ChangeEnableEmbeddingQuantizationFlagTest) {
  std::vector<bool> enable_embedding_quantization_flags = GetParam();

  SchemaProto schema =
      SchemaBuilder()
          .AddType(
              SchemaTypeConfigBuilder()
                  .SetType("Message")
                  .AddProperty(
                      PropertyConfigBuilder()
                          .SetName("embeddingUnquantized")
                          .SetDataTypeVector(
                              EMBEDDING_INDEXING_LINEAR_SEARCH,
                              EmbeddingIndexingConfig::QuantizationType::NONE)
                          .SetCardinality(CARDINALITY_OPTIONAL))
                  .AddProperty(PropertyConfigBuilder()
                                   .SetName("embeddingQuantized")
                                   .SetDataTypeVector(
                                       EMBEDDING_INDEXING_LINEAR_SEARCH,
                                       EmbeddingIndexingConfig::
                                           QuantizationType::QUANTIZE_8_BIT)
                                   .SetCardinality(CARDINALITY_OPTIONAL)))
          .Build();

  // If quantization is enabled, this vector will be quantized to {0, 1, 255}.
  PropertyProto::VectorProto vector = CreateVector("my_model", {0, 1.45, 255});
  // Create two documents with different quantization types. If quantization is
  // enabled, then only document2's embedding will be quantized. Otherwise, both
  // will be unquantized.
  DocumentProto document1 =
      DocumentBuilder()
          .SetKey("icing", "uri1")
          .SetSchema("Message")
          .SetCreationTimestampMs(1)
          .AddVectorProperty("embeddingUnquantized", vector)
          .Build();
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("icing", "uri2")
                                .SetSchema("Message")
                                .SetCreationTimestampMs(1)
                                .AddVectorProperty("embeddingQuantized", vector)
                                .Build();

  // Create icing with a document that has an embedding.
  {
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_enable_embedding_index(true);
    options.set_enable_embedding_quantization(
        enable_embedding_quantization_flags[0]);
    TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document1).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document2).status(), ProtoIsOk());
  }

  // Create icing multiple times with different enable_embedding_index flags.
  for (int i = 1; i < enable_embedding_quantization_flags.size(); ++i) {
    bool flag_changed = enable_embedding_quantization_flags[i] !=
                        enable_embedding_quantization_flags[i - 1];

    // Ensure that the embedding index is rebuilt if the flag is changed.
    auto mock_filesystem = std::make_unique<MockFilesystem>();
    EXPECT_CALL(*mock_filesystem, DeleteDirectoryRecursively(_))
        .WillRepeatedly(DoDefault());
    EXPECT_CALL(*mock_filesystem,
                DeleteDirectoryRecursively(EndsWith("/embedding_index_dir")))
        .Times(flag_changed ? 1 : 0);

    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_enable_embedding_index(true);
    options.set_enable_embedding_quantization(
        enable_embedding_quantization_flags[i]);
    TestIcingSearchEngine icing(options, std::move(mock_filesystem),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());
    InitializeResultProto initialize_result = icing.Initialize();
    ASSERT_THAT(initialize_result.status(), ProtoIsOk());
    // Ensure that the embedding index is rebuilt if the flag is changed.
    EXPECT_THAT(initialize_result.initialize_stats()
                    .embedding_index_restoration_cause(),
                Eq(flag_changed ? InitializeStatsProto::FEATURE_FLAG_CHANGED
                                : InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(
        initialize_result.initialize_stats().integer_index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));

    // Write an embedding query that always matches all the documents. We will
    // check quantization by checking the scores.
    //
    // This query will assign a dot product score that is equal to the sum of
    // all dimensions of the matched embedding. As a result, if quantization is
    // enabled, the score will be 0 + 1 + 255 = 256. Otherwise, the score will
    // be 0 + 1.45 + 255 = 256.45.
    SearchSpecProto search_spec;
    search_spec.set_query("semanticSearch(getEmbeddingParameter(0))");
    *search_spec.add_embedding_query_vectors() =
        CreateVector("my_model", {1, 1, 1});
    search_spec.set_embedding_query_metric_type(
        SearchSpecProto::EmbeddingQueryMetricType::DOT_PRODUCT);
    search_spec.add_enabled_features(
        std::string(kListFilterQueryLanguageFeature));
    ScoringSpecProto scoring_spec = GetDefaultScoringSpec();
    scoring_spec.set_rank_by(
        ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
    // Set the advanced scoring expression to a constant so that the documents
    // will be returned according to their order they were added.
    scoring_spec.set_advanced_scoring_expression("0");
    // Set the additional advanced scoring expression to check the embedding
    // scores.
    scoring_spec.add_additional_advanced_scoring_expressions(
        "sum(this.matchedSemanticScores(getEmbeddingParameter(0)))");

    SearchResultProto results = icing.Search(
        search_spec, scoring_spec, ResultSpecProto::default_instance());
    EXPECT_THAT(results.status(), ProtoIsOk());
    EXPECT_THAT(results.results(), SizeIs(2));

    constexpr float eps = 0.0001f;
    constexpr float unquantized_score = 256.45f;
    constexpr float quantized_score = 256.0f;
    EXPECT_THAT(results.results(0).document(), EqualsProto(document2));
    if (enable_embedding_quantization_flags[i]) {
      // When quantization is enabled, only the embedding that is configured
      // with quantization is quantized. So only document2's embedding is
      // quantized.
      EXPECT_NEAR(results.results(0).additional_scores(0), quantized_score,
                  eps);
    } else {
      // When quantization is disabled, all embeddings are unquantized.
      EXPECT_NEAR(results.results(0).additional_scores(0), unquantized_score,
                  eps);
    }
    // document1's embedding is always unquantized, since it is not configured
    // to be quantized.
    EXPECT_THAT(results.results(1).document(), EqualsProto(document1));
    EXPECT_NEAR(results.results(1).additional_scores(0), unquantized_score,
                eps);
  }
}

INSTANTIATE_TEST_SUITE_P(
    IcingSearchEngineInitializationChangeEmbeddingFlagTest,
    IcingSearchEngineInitializationChangeEmbeddingFlagTest,
    testing::Values(std::vector<bool>{false, true, false, true, false, true},
                    std::vector<bool>{true, false, true, false, true, false},
                    std::vector<bool>{false, true, true, true, false, true},
                    std::vector<bool>{true, false, false, false, true, false},
                    std::vector<bool>{true, true, true, true},
                    std::vector<bool>{false, false, false, false}));

class IcingSearchEngineInitializationChangeEnableScorablePropertiesFlagTest
    : public IcingSearchEngineInitializationTest,
      public ::testing::WithParamInterface<std::vector<bool>> {};
TEST_P(IcingSearchEngineInitializationChangeEnableScorablePropertiesFlagTest,
       ChangeEnableScorablePropertiesFlagTest) {
  std::vector<bool> enable_scorable_properties_flags = GetParam();

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_EXACT,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("score")
                                        .SetDataTypeInt64(NUMERIC_MATCH_UNKNOWN)
                                        .SetScorableType(SCORABLE_TYPE_ENABLED)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  // Create a document with an embedding.
  int scorable_prop_value = 10;
  DocumentProto document = DocumentBuilder()
                               .SetKey("icing", "uri")
                               .SetSchema("Message")
                               .SetCreationTimestampMs(1)
                               .AddStringProperty("body", "foo bar")
                               .AddInt64Property("score", scorable_prop_value)
                               .Build();

  // Create icing with a document that has a scorable property.
  {
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_enable_scorable_properties(
        enable_scorable_properties_flags.at(0));
    TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  }

  // Create icing multiple times with different enable_scorable_properties
  // flags.
  for (int i = 1; i < enable_scorable_properties_flags.size(); ++i) {
    bool flag_changed = enable_scorable_properties_flags[i] !=
                        enable_scorable_properties_flags[i - 1];

    // Ensure that the document store derived files are rebuilt if the flag is
    // changed.
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_enable_scorable_properties(enable_scorable_properties_flags[i]);
    TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());
    InitializeResultProto initialize_result = icing.Initialize();
    ASSERT_THAT(initialize_result.status(), ProtoIsOk());

    // Document store recovery cause should be FEATURE_FLAG_CHANGED if the flag
    // is changed.
    EXPECT_THAT(
        initialize_result.initialize_stats().document_store_recovery_cause(),
        Eq(flag_changed ? InitializeStatsProto::FEATURE_FLAG_CHANGED
                        : InitializeStatsProto::NONE));

    // Schema store and all indices should be unaffected.
    EXPECT_THAT(
        initialize_result.initialize_stats().schema_store_recovery_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(
        initialize_result.initialize_stats().integer_index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result.initialize_stats()
                    .embedding_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));

    // Write normal query that should retrieve the document.
    SearchSpecProto search_spec;
    search_spec.set_query("bar");
    search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
    search_spec.add_enabled_features(
        std::string(kListFilterQueryLanguageFeature));

    ScoringSpecProto scoring_spec;
    scoring_spec.add_scoring_feature_types_enabled(
        ScoringFeatureType::SCORABLE_PROPERTY_RANKING);
    scoring_spec.set_rank_by(
        ScoringSpecProto::RankingStrategy::ADVANCED_SCORING_EXPRESSION);
    scoring_spec.set_advanced_scoring_expression("this.creationTimestamp()");
    SearchResultProto results = icing.Search(
        search_spec, scoring_spec, ResultSpecProto::default_instance());
    EXPECT_THAT(results.status(), ProtoIsOk());
    EXPECT_THAT(results.results(), SizeIs(1));
    EXPECT_THAT(results.results(0).document(), EqualsProto(document));
    EXPECT_THAT(results.results(0).score(), Eq(1));

    // Now write a query that tries to access the scorable property.
    scoring_spec.set_advanced_scoring_expression(
        "this.creationTimestamp() + sum(getScorableProperty(\"Message\", "
        "\"score\"))");
    SchemaTypeAliasMapProto* alias_map_proto =
        scoring_spec.add_schema_type_alias_map_protos();
    alias_map_proto->set_alias_schema_type("Message");
    alias_map_proto->mutable_schema_types()->Add("Message");

    results = icing.Search(search_spec, scoring_spec,
                           ResultSpecProto::default_instance());
    if (options.enable_scorable_properties()) {
      EXPECT_THAT(results.status(), ProtoIsOk());
      EXPECT_THAT(results.results(), SizeIs(1));
      EXPECT_THAT(results.results(0).document(), EqualsProto(document));
      EXPECT_THAT(results.results(0).score(), Eq(1 + scorable_prop_value));
    } else {
      EXPECT_THAT(results.status(),
                  ProtoStatusIs(StatusProto::INVALID_ARGUMENT));
    }

    // Reindex the doc with a new scorable property value, regardless of whether
    // scorable properties are enabled. This way we can confirm that Initialize
    // is actually rebuilding the scorable property cache with new values.
    scorable_prop_value += 10;
    document = DocumentBuilder(document)
                   .ClearProperties()
                   .AddStringProperty("body", "foo bar")
                   .AddInt64Property("score", scorable_prop_value)
                   .Build();
    ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  }
}

INSTANTIATE_TEST_SUITE_P(
    IcingSearchEngineInitializationChangeEnableScorablePropertiesFlagTest,
    IcingSearchEngineInitializationChangeEnableScorablePropertiesFlagTest,
    testing::Values(std::vector<bool>{false, true, false, true, false, true},
                    std::vector<bool>{true, false, true, false, true, false},
                    std::vector<bool>{false, true, true, true, false, true},
                    std::vector<bool>{true, false, false, false, true, false},
                    std::vector<bool>{true, true, true, true},
                    std::vector<bool>{false, false, false, false}));

class IcingSearchEngineInitializationSchemaDatabaseMigrationTest
    : public IcingSearchEngineInitializationTest,
      public ::testing::WithParamInterface<std::tuple<int32_t, bool, bool>> {};

TEST_P(IcingSearchEngineInitializationSchemaDatabaseMigrationTest,
       InitializeWithSchemaDatabaseMigration) {
  int32_t existing_version = std::get<0>(GetParam());
  bool previous_version_has_schema_database_enabled = std::get<1>(GetParam());
  bool enable_schema_database = std::get<2>(GetParam());

  IcingSearchEngineVersionProto previous_version_proto;
  previous_version_proto.set_version(existing_version);
  previous_version_proto.set_max_version(existing_version);
  if (previous_version_has_schema_database_enabled) {
    previous_version_proto.add_enabled_features()->set_feature_type(
        IcingSearchEngineFeatureInfoProto::FEATURE_SCHEMA_DATABASE);
  }

  SchemaTypeConfigProto db1_email_type =
      SchemaTypeConfigBuilder()
          .SetType("db1/email")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("db1Subject")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();
  SchemaTypeConfigProto db2_email_type =
      SchemaTypeConfigBuilder()
          .SetType("db2/email")
          .AddProperty(PropertyConfigBuilder()
                           .SetName("db2Subject")
                           .SetDataTypeString(TERM_MATCH_EXACT, TOKENIZER_PLAIN)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .AddProperty(PropertyConfigBuilder()
                           .SetName("db2Id")
                           .SetDataTypeInt64(NUMERIC_MATCH_RANGE)
                           .SetCardinality(CARDINALITY_OPTIONAL))
          .Build();

  if (previous_version_has_schema_database_enabled) {
    // Populate the database field for the db1/email type.
    db1_email_type =
        SchemaTypeConfigBuilder(db1_email_type).SetDatabase("db1").Build();
    if (existing_version >= version_util::kSchemaDatabaseVersion) {
      // Populate the database field for the db2/email type only if previous
      // version is a post schema-database version.
      db2_email_type =
          SchemaTypeConfigBuilder(db2_email_type).SetDatabase("db2").Build();
    }
    // Otherwise, the database field is not populated for db2/email type. This
    // is to simulate the following situation:
    // 1. Icing is initialized on a version>kSchemaDatabaseVersion with schema
    //    database enabled, and db1/email is set with the database field
    //    populated.
    // 2. Icing gets rolled back to pre-schema database version, db2/email is
    //    set during this time so the database field is not populated.
  }
  SchemaProto previous_version_schema =
      SchemaBuilder().AddType(db1_email_type).AddType(db2_email_type).Build();

  DocumentProto db1_email_doc =
      DocumentBuilder()
          .SetKey("namespace", "uri1")
          .SetSchema("db1/email")
          .AddStringProperty("db1Subject", "subject")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto db2_email_doc =
      DocumentBuilder()
          .SetKey("namespace", "uri3")
          .SetSchema("db2/email")
          .AddStringProperty("db2Subject", "subject")
          .AddInt64Property("db2Id", 123)
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  {  // Initialize IcingSearchEngine
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_enable_schema_database(
        previous_version_has_schema_database_enabled);
    TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());
    EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
    // 1. Set schema.
    if (options.enable_schema_database()) {
      // Need to set schemas with a single database field at a time.
      ASSERT_THAT(
          icing.SetSchema(SchemaBuilder().AddType(db1_email_type).Build())
              .status(),
          ProtoIsOk());
      ASSERT_THAT(
          icing.SetSchema(SchemaBuilder().AddType(db2_email_type).Build())
              .status(),
          ProtoIsOk());
    } else {
      ASSERT_THAT(icing.SetSchema(previous_version_schema).status(),
                  ProtoIsOk());
    }
    // 2. Put two documents
    ASSERT_THAT(icing.Put(db1_email_doc).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(db2_email_doc).status(), ProtoIsOk());
    // 3. Rewrite version files
    //    - Only need to rewrite v1 version file to write an older version
    //      number.
    //    - FeatureInfo rewritting (v2 version file) is not needed as it should
    //      be handled by IcingSearchEngine.
    ICING_ASSERT_OK(version_util::WriteV1Version(
        *filesystem(), GetVersionFileDir(),
        version_util::VersionInfo(existing_version, existing_version)));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  options.set_enable_schema_database(enable_schema_database);

  TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                              std::make_unique<IcingFilesystem>(),
                              std::make_unique<FakeClock>(), GetTestJniCache());
  InitializeResultProto initialize_result = icing.Initialize();
  ASSERT_THAT(initialize_result.status(), ProtoIsOk());

  SearchResultProto db1_email_search_result_proto;
  db1_email_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *db1_email_search_result_proto.mutable_results()->Add()->mutable_document() =
      db1_email_doc;

  SearchResultProto db2_email_search_result_proto;
  db2_email_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *db2_email_search_result_proto.mutable_results()->Add()->mutable_document() =
      db2_email_doc;

  SearchResultProto all_email_search_result_proto;
  all_email_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *all_email_search_result_proto.mutable_results()->Add()->mutable_document() =
      db2_email_doc;
  *all_email_search_result_proto.mutable_results()->Add()->mutable_document() =
      db1_email_doc;

  // Verify term search
  SearchSpecProto search_spec1;
  search_spec1.set_query("db1Subject:subject");
  search_spec1.set_term_match_type(TermMatchType::EXACT_ONLY);
  SearchResultProto search_result_proto1 =
      icing.Search(search_spec1, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto1, EqualsSearchResultIgnoreStatsAndScores(
                                        db1_email_search_result_proto));

  SearchSpecProto search_spec2;
  search_spec2.set_query("subject");
  search_spec2.set_term_match_type(TermMatchType::EXACT_ONLY);
  SearchResultProto search_result_google::protobuf =
      icing.Search(search_spec2, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_google::protobuf, EqualsSearchResultIgnoreStatsAndScores(
                                        all_email_search_result_proto));

  // Verify numeric (integer) search
  SearchSpecProto search_spec3;
  search_spec3.set_query("db2Id == 123");
  search_spec3.add_enabled_features(std::string(kNumericSearchFeature));

  SearchResultProto search_result_proto3 =
      icing.Search(search_spec3, ScoringSpecProto::default_instance(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto3, EqualsSearchResultIgnoreStatsAndScores(
                                        db2_email_search_result_proto));

  // Verify GetSchema
  if (enable_schema_database) {
    SchemaTypeConfigProto db1_email_type_with_db =
        SchemaTypeConfigBuilder(db1_email_type).SetDatabase("db1").Build();
    SchemaTypeConfigProto db2_email_type_with_db =
        SchemaTypeConfigBuilder(db2_email_type).SetDatabase("db2").Build();
    SchemaProto full_schema_with_database = SchemaBuilder()
                                                .AddType(db1_email_type_with_db)
                                                .AddType(db2_email_type_with_db)
                                                .Build();
    SchemaProto db1_schema =
        SchemaBuilder().AddType(db1_email_type_with_db).Build();
    SchemaProto db2_schema =
        SchemaBuilder().AddType(db2_email_type_with_db).Build();

    GetSchemaResultProto expected_get_schema_result_proto_full;
    expected_get_schema_result_proto_full.mutable_status()->set_code(
        StatusProto::OK);
    *expected_get_schema_result_proto_full.mutable_schema() =
        full_schema_with_database;
    EXPECT_THAT(icing.GetSchema(),
                EqualsProto(expected_get_schema_result_proto_full));

    GetSchemaResultProto expected_get_schema_result_proto_db1;
    expected_get_schema_result_proto_db1.mutable_status()->set_code(
        StatusProto::OK);
    *expected_get_schema_result_proto_db1.mutable_schema() = db1_schema;
    EXPECT_THAT(icing.GetSchema("db1"),
                EqualsProto(expected_get_schema_result_proto_db1));

    GetSchemaResultProto expected_get_schema_result_proto_db2;
    expected_get_schema_result_proto_db2.mutable_status()->set_code(
        StatusProto::OK);
    *expected_get_schema_result_proto_db2.mutable_schema() = db2_schema;
    EXPECT_THAT(icing.GetSchema("db2"),
                EqualsProto(expected_get_schema_result_proto_db2));
  } else {
    GetSchemaResultProto expected_get_schema_result_proto;
    expected_get_schema_result_proto.mutable_status()->set_code(
        StatusProto::OK);
    *expected_get_schema_result_proto.mutable_schema() =
        previous_version_schema;
    EXPECT_THAT(icing.GetSchema(),
                EqualsProto(expected_get_schema_result_proto));
  }
}

INSTANTIATE_TEST_SUITE_P(
    IcingSearchEngineInitializationSchemaDatabaseMigrationTest,
    IcingSearchEngineInitializationSchemaDatabaseMigrationTest,
    testing::Values(
        std::make_tuple(
            /*previous_version=*/version_util::kSchemaDatabaseVersion - 1,
            /*prev_version_schema_database_enabled=*/false,
            /*enable_schema_database=*/false),
        std::make_tuple(
            /*previous_version=*/version_util::kSchemaDatabaseVersion - 1,
            /*prev_version_schema_database_enabled=*/false,
            /*enable_schema_database=*/true),
        // The next two cases simulate the following scenario:
        // 1. Icing is initialized on a version>kSchemaDatabaseVersion for
        //    sometime, and schemas are set with the database field populated.
        // 2. Icing gets rolled back to pre-schema database version, so new
        //    schema types no longer populate the database field.
        // 3. Icing gets rolled forward to post-schema database version again,
        //    and we should verify that database migration happens correctly.
        std::make_tuple(
            /*previous_version=*/version_util::kSchemaDatabaseVersion - 1,
            /*prev_version_schema_database_enabled=*/true,
            /*enable_schema_database=*/false),
        std::make_tuple(
            /*previous_version=*/version_util::kSchemaDatabaseVersion - 1,
            /*prev_version_schema_database_enabled=*/true,
            /*enable_schema_database=*/true),
        std::make_tuple(
            /*previous_version=*/version_util::kSchemaDatabaseVersion,
            /*prev_version_schema_database_enabled=*/false,
            /*enable_schema_database=*/false),
        std::make_tuple(
            /*previous_version=*/version_util::kSchemaDatabaseVersion,
            /*prev_version_schema_database_enabled=*/false,
            /*enable_schema_database=*/true),
        std::make_tuple(
            /*previous_version=*/version_util::kSchemaDatabaseVersion,
            /*prev_version_schema_database_enabled=*/true,
            /*enable_schema_database=*/false),
        std::make_tuple(
            /*previous_version=*/version_util::kSchemaDatabaseVersion,
            /*prev_version_schema_database_enabled=*/true,
            /*enable_schema_database=*/true)));

class IcingSearchEngineInitializationChangeEnableJoinIndexV3FlagTest
    : public IcingSearchEngineInitializationTest,
      public ::testing::WithParamInterface<std::vector<bool>> {};
TEST_P(IcingSearchEngineInitializationChangeEnableJoinIndexV3FlagTest,
       ChangeEnableJoinIndexV3FlagTest) {
  std::vector<bool> enable_join_index_v3_flags = GetParam();

  SchemaProto schema =
      SchemaBuilder()
          .AddType(SchemaTypeConfigBuilder().SetType("Person").AddProperty(
              PropertyConfigBuilder()
                  .SetName("name")
                  .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
                  .SetCardinality(CARDINALITY_REQUIRED)))
          .AddType(SchemaTypeConfigBuilder()
                       .SetType("Message")
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("body")
                                        .SetDataTypeString(TERM_MATCH_PREFIX,
                                                           TOKENIZER_PLAIN)
                                        .SetCardinality(CARDINALITY_REQUIRED))
                       .AddProperty(PropertyConfigBuilder()
                                        .SetName("senderQualifiedId")
                                        .SetDataTypeJoinableString(
                                            JOINABLE_VALUE_TYPE_QUALIFIED_ID)
                                        .SetCardinality(CARDINALITY_REQUIRED)))
          .Build();

  DocumentProto person =
      DocumentBuilder()
          .SetKey("namespace", "person")
          .SetSchema("Person")
          .AddStringProperty("name", "person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();
  DocumentProto message =
      DocumentBuilder()
          .SetKey("namespace", "message/1")
          .SetSchema("Message")
          .AddStringProperty("body", "message body")
          .AddStringProperty("senderQualifiedId", "namespace#person")
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  {
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_enable_qualified_id_join_index_v3_and_delete_propagate_from(
        enable_join_index_v3_flags.at(0));
    TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(person).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(message).status(), ProtoIsOk());
  }

  // Create icing multiple times with different
  // enable_qualified_id_join_index_v3_and_propagate_delete flags.
  for (int i = 1; i < enable_join_index_v3_flags.size(); ++i) {
    bool flag_changed =
        enable_join_index_v3_flags[i] != enable_join_index_v3_flags[i - 1];

    // Ensure that the qualified id join index is rebuilt if the flag is
    // changed.
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_enable_qualified_id_join_index_v3_and_delete_propagate_from(
        enable_join_index_v3_flags[i]);
    TestIcingSearchEngine icing(options, std::make_unique<Filesystem>(),
                                std::make_unique<IcingFilesystem>(),
                                std::make_unique<FakeClock>(),
                                GetTestJniCache());
    InitializeResultProto initialize_result = icing.Initialize();
    ASSERT_THAT(initialize_result.status(), ProtoIsOk());

    // Qualified id join index recovery cause should be FEATURE_FLAG_CHANGED if
    // flag is changed.
    EXPECT_THAT(initialize_result.initialize_stats()
                    .qualified_id_join_index_restoration_cause(),
                Eq(flag_changed ? InitializeStatsProto::FEATURE_FLAG_CHANGED
                                : InitializeStatsProto::NONE));

    // Schema store, document store and all other indices should be unaffected.
    EXPECT_THAT(
        initialize_result.initialize_stats().schema_store_recovery_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(
        initialize_result.initialize_stats().document_store_recovery_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result.initialize_stats().index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(
        initialize_result.initialize_stats().integer_index_restoration_cause(),
        Eq(InitializeStatsProto::NONE));
    EXPECT_THAT(initialize_result.initialize_stats()
                    .embedding_index_restoration_cause(),
                Eq(InitializeStatsProto::NONE));

    // Prepare join search spec to join a query for `name:person` with a child
    // query for `body:message` based on the child's `senderQualifiedId` field.
    //
    // No matter what the flag value is, the join API should always return the
    // expected result.
    SearchSpecProto search_spec;
    search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
    search_spec.set_query("name:person");
    JoinSpecProto* join_spec = search_spec.mutable_join_spec();
    join_spec->set_parent_property_expression(
        std::string(JoinProcessor::kQualifiedIdExpr));
    join_spec->set_child_property_expression("senderQualifiedId");
    join_spec->set_aggregation_scoring_strategy(
        JoinSpecProto::AggregationScoringStrategy::COUNT);
    JoinSpecProto::NestedSpecProto* nested_spec =
        join_spec->mutable_nested_spec();
    SearchSpecProto* nested_search_spec = nested_spec->mutable_search_spec();
    nested_search_spec->set_term_match_type(TermMatchType::EXACT_ONLY);
    nested_search_spec->set_query("body:message");
    *nested_spec->mutable_scoring_spec() = GetDefaultScoringSpec();
    *nested_spec->mutable_result_spec() = ResultSpecProto::default_instance();

    ResultSpecProto result_spec = ResultSpecProto::default_instance();
    result_spec.set_max_joined_children_per_parent_to_return(
        std::numeric_limits<int32_t>::max());

    SearchResultProto expected_search_result_proto;
    expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
    SearchResultProto::ResultProto* result_proto =
        expected_search_result_proto.mutable_results()->Add();
    *result_proto->mutable_document() = person;
    *result_proto->mutable_joined_results()->Add()->mutable_document() =
        message;

    SearchResultProto search_result_proto =
        icing.Search(search_spec, GetDefaultScoringSpec(), result_spec);
    EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                         expected_search_result_proto));
  }
}

INSTANTIATE_TEST_SUITE_P(
    IcingSearchEngineInitializationChangeEnableJoinIndexV3FlagTest,
    IcingSearchEngineInitializationChangeEnableJoinIndexV3FlagTest,
    testing::Values(std::vector<bool>{false, true, false, true, false, true},
                    std::vector<bool>{true, false, true, false, true, false},
                    std::vector<bool>{false, true, true, true, false, true},
                    std::vector<bool>{true, false, false, false, true, false},
                    std::vector<bool>{true, true, true, true},
                    std::vector<bool>{false, false, false, false}));

}  // namespace
}  // namespace lib
}  // namespace icing
