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
#include "icing/schema/schema-store.h"
#include "icing/store/document-log-creator.h"
#include "icing/testing/common-matchers.h"
#include "icing/testing/fake-clock.h"
#include "icing/testing/icu-data-file-helper.h"
#include "icing/testing/jni-test-helpers.h"
#include "icing/testing/test-data.h"
#include "icing/testing/tmp-directory.h"

namespace icing {
namespace lib {

namespace {

using ::icing::lib::portable_equals_proto::EqualsProto;
using ::testing::_;
using ::testing::Eq;
using ::testing::HasSubstr;
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
          icu_data_file_helper::SetUpICUDataFile(icu_data_file_path));
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

SchemaProto CreateMessageSchema() {
  return SchemaBuilder()
      .AddType(SchemaTypeConfigBuilder().SetType("Message").AddProperty(
          PropertyConfigBuilder()
              .SetName("body")
              .SetDataTypeString(TERM_MATCH_PREFIX, TOKENIZER_PLAIN)
              .SetCardinality(CARDINALITY_REQUIRED)))
      .Build();
}

SchemaProto CreateEmailSchema() {
  return SchemaBuilder()
      .AddType(SchemaTypeConfigBuilder()
                   .SetType("Email")
                   .AddProperty(PropertyConfigBuilder()
                                    .SetName("body")
                                    .SetDataTypeString(TERM_MATCH_PREFIX,
                                                       TOKENIZER_PLAIN)
                                    .SetCardinality(CARDINALITY_REQUIRED))
                   .AddProperty(PropertyConfigBuilder()
                                    .SetName("subject")
                                    .SetDataTypeString(TERM_MATCH_PREFIX,
                                                       TOKENIZER_PLAIN)
                                    .SetCardinality(CARDINALITY_REQUIRED)))
      .Build();
}

ScoringSpecProto GetDefaultScoringSpec() {
  ScoringSpecProto scoring_spec;
  scoring_spec.set_rank_by(ScoringSpecProto::RankingStrategy::DOCUMENT_SCORE);
  return scoring_spec;
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
    auto get_filesize_lambda = [this,
                                &document_log_filepath](const char* filename) {
      if (strncmp(document_log_filepath.c_str(), filename,
                  document_log_filepath.length()) == 0) {
        return Filesystem::kBadFileSize;
      }
      return this->filesystem()->GetFileSize(filename);
    };
    ON_CALL(*mock_filesystem, GetFileSize(A<const char*>()))
        .WillByDefault(get_filesize_lambda);

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

  // Checks that the index is still ok so we can search over it
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
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
          .SetCreationTimestampMs(kDefaultCreationTimestampMs)
          .Build();

  IcingSearchEngineOptions options = GetDefaultIcingOptions();
  {
    // Initializes folder and schema
    IcingSearchEngine icing(options, GetTestJniCache());
    EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());

    SchemaProto schema;
    auto type = schema.add_types();
    type->set_schema_type("Message");

    auto property = type->add_properties();
    property->set_property_name("body");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);
    property->mutable_string_indexing_config()->set_term_match_type(
        TermMatchType::PREFIX);
    property->mutable_string_indexing_config()->set_tokenizer_type(
        StringIndexingConfig::TokenizerType::PLAIN);

    property = type->add_properties();
    property->set_property_name("additional");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);

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
    property->mutable_string_indexing_config()->set_term_match_type(
        TermMatchType::PREFIX);
    property->mutable_string_indexing_config()->set_tokenizer_type(
        StringIndexingConfig::TokenizerType::PLAIN);

    property = type->add_properties();
    property->set_property_name("additional");
    property->set_data_type(PropertyConfigProto::DataType::STRING);
    property->set_cardinality(PropertyConfigProto::Cardinality::OPTIONAL);
    property->mutable_string_indexing_config()->set_term_match_type(
        TermMatchType::PREFIX);
    property->mutable_string_indexing_config()->set_tokenizer_type(
        StringIndexingConfig::TokenizerType::PLAIN);

    // Write the marker file
    std::string marker_filepath =
        absl_ports::StrCat(options.base_dir(), "/set_schema_marker");
    ScopedFd sfd(filesystem()->OpenForWrite(marker_filepath.c_str()));
    ASSERT_TRUE(sfd.is_valid());

    // Write the new schema
    FakeClock fake_clock;
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(filesystem(), GetSchemaDir(), &fake_clock));
    ICING_EXPECT_OK(schema_store->SetSchema(new_schema));
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

  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineInitializationTest,
       RecoverFromInconsistentDocumentStore) {
  DocumentProto document1 = CreateMessageDocument("namespace", "uri1");
  DocumentProto document2 = CreateMessageDocument("namespace", "uri2");

  {
    // Initializes folder and schema, index one document
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
    EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(document1).status(), ProtoIsOk());
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  {
    FakeClock fake_clock;
    ICING_ASSERT_OK_AND_ASSIGN(
        std::unique_ptr<SchemaStore> schema_store,
        SchemaStore::Create(filesystem(), GetSchemaDir(), &fake_clock));
    ICING_EXPECT_OK(schema_store->SetSchema(CreateMessageSchema()));

    // Puts a second document into DocumentStore but doesn't index it.
    ICING_ASSERT_OK_AND_ASSIGN(
        DocumentStore::CreateResult create_result,
        DocumentStore::Create(filesystem(), GetDocumentDir(), &fake_clock,
                              schema_store.get()));
    std::unique_ptr<DocumentStore> document_store =
        std::move(create_result.document_store);

    ICING_EXPECT_OK(document_store->Put(document2));
  }

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  // Index Restoration should be triggered here and document2 should be
  // indexed.
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());

  GetResultProto expected_get_result_proto;
  expected_get_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_get_result_proto.mutable_document() = document1;

  // DocumentStore kept the additional document
  EXPECT_THAT(
      icing.Get("namespace", "uri1", GetResultSpecProto::default_instance()),
      EqualsProto(expected_get_result_proto));

  *expected_get_result_proto.mutable_document() = document2;
  EXPECT_THAT(
      icing.Get("namespace", "uri2", GetResultSpecProto::default_instance()),
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

  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineInitializationTest, RecoverFromInconsistentIndex) {
  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      CreateMessageDocument("namespace", "uri");

  {
    // Initializes folder and schema, index one document
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
    EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(CreateMessageDocument("namespace", "uri")).status(),
                ProtoIsOk());
    SearchResultProto search_result_proto =
        icing.Search(search_spec, GetDefaultScoringSpec(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                         expected_search_result_proto));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  // Pretend we lost the entire index
  EXPECT_TRUE(filesystem()->DeleteDirectoryRecursively(
      absl_ports::StrCat(GetIndexDir(), "/idx/lite.").c_str()));

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());

  // Check that our index is ok by searching over the restored index
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

TEST_F(IcingSearchEngineInitializationTest, RecoverFromCorruptIndex) {
  SearchSpecProto search_spec;
  search_spec.set_query("message");
  search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);

  SearchResultProto expected_search_result_proto;
  expected_search_result_proto.mutable_status()->set_code(StatusProto::OK);
  *expected_search_result_proto.mutable_results()->Add()->mutable_document() =
      CreateMessageDocument("namespace", "uri");

  {
    // Initializes folder and schema, index one document
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());
    EXPECT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(CreateMessageDocument("namespace", "uri")).status(),
                ProtoIsOk());
    SearchResultProto search_result_proto =
        icing.Search(search_spec, GetDefaultScoringSpec(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                         expected_search_result_proto));
  }  // This should shut down IcingSearchEngine and persist anything it needs to

  // Pretend index is corrupted
  const std::string index_hit_buffer_file = GetIndexDir() + "/idx/lite.hb";
  ScopedFd fd(filesystem()->OpenForWrite(index_hit_buffer_file.c_str()));
  ASSERT_TRUE(fd.is_valid());
  ASSERT_TRUE(filesystem()->Write(fd.get(), "1234", 4));

  IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
  EXPECT_THAT(icing.Initialize().status(), ProtoIsOk());

  // Check that our index is ok by searching over the restored index
  SearchResultProto search_result_proto =
      icing.Search(search_spec, GetDefaultScoringSpec(),
                   ResultSpecProto::default_instance());
  EXPECT_THAT(search_result_proto, EqualsSearchResultIgnoreStatsAndScores(
                                       expected_search_result_proto));
}

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

TEST_F(IcingSearchEngineInitializationTest, RestoreIndex) {
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

  // 3. Create the index again. This should trigger index restoration.
  {
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_index_merge_size(document.ByteSizeLong());
    IcingSearchEngine icing(options, GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    SearchSpecProto search_spec;
    search_spec.set_query("consectetur");
    search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
    SearchResultProto results =
        icing.Search(search_spec, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(results.status(), ProtoIsOk());
    EXPECT_THAT(results.next_page_token(), Eq(0));
    // All documents should be retrievable.
    ASSERT_THAT(results.results(), SizeIs(3));
    EXPECT_THAT(results.results(0).document().uri(), Eq("fake_type/2"));
    EXPECT_THAT(results.results(1).document().uri(), Eq("fake_type/1"));
    EXPECT_THAT(results.results(2).document().uri(), Eq("fake_type/0"));
  }
}

TEST_F(IcingSearchEngineInitializationTest, RestoreIndexLoseLiteIndex) {
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

  // 2. Delete the last document from the document log
  {
    const std::string document_log_file = absl_ports::StrCat(
        GetDocumentDir(), "/", DocumentLogCreator::GetDocumentLogFilename());
    filesystem()->DeleteFile(document_log_file.c_str());
    ICING_ASSERT_OK_AND_ASSIGN(
        auto create_result,
        PortableFileBackedProtoLog<DocumentWrapper>::Create(
            filesystem(), document_log_file.c_str(),
            PortableFileBackedProtoLog<DocumentWrapper>::Options(
                /*compress_in=*/true)));
    std::unique_ptr<PortableFileBackedProtoLog<DocumentWrapper>> document_log =
        std::move(create_result.proto_log);

    document = DocumentBuilder(document).SetUri("fake_type/0").Build();
    DocumentWrapper wrapper;
    *wrapper.mutable_document() = document;
    ASSERT_THAT(document_log->WriteProto(wrapper), IsOk());

    document = DocumentBuilder(document).SetUri("fake_type/1").Build();
    *wrapper.mutable_document() = document;
    ASSERT_THAT(document_log->WriteProto(wrapper), IsOk());
  }

  // 3. Create the index again. This should throw out the lite index and trigger
  // index restoration which will only restore the two documents in the main
  // index.
  {
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_index_merge_size(document.ByteSizeLong());
    IcingSearchEngine icing(options, GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    SearchSpecProto search_spec;
    search_spec.set_query("consectetur");
    search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
    SearchResultProto results =
        icing.Search(search_spec, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(results.status(), ProtoIsOk());
    EXPECT_THAT(results.next_page_token(), Eq(0));
    // Only the documents that were in the main index should be retrievable.
    ASSERT_THAT(results.results(), SizeIs(2));
    EXPECT_THAT(results.results(0).document().uri(), Eq("fake_type/1"));
    EXPECT_THAT(results.results(1).document().uri(), Eq("fake_type/0"));
  }
}

TEST_F(IcingSearchEngineInitializationTest, RestoreIndexLoseIndex) {
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

  // 2. Delete the last two documents from the document log.
  {
    const std::string document_log_file = absl_ports::StrCat(
        GetDocumentDir(), "/", DocumentLogCreator::GetDocumentLogFilename());
    filesystem()->DeleteFile(document_log_file.c_str());
    ICING_ASSERT_OK_AND_ASSIGN(
        auto create_result,
        PortableFileBackedProtoLog<DocumentWrapper>::Create(
            filesystem(), document_log_file.c_str(),
            PortableFileBackedProtoLog<DocumentWrapper>::Options(
                /*compress_in=*/true)));
    std::unique_ptr<PortableFileBackedProtoLog<DocumentWrapper>> document_log =
        std::move(create_result.proto_log);

    document = DocumentBuilder(document).SetUri("fake_type/0").Build();
    DocumentWrapper wrapper;
    *wrapper.mutable_document() = document;
    ASSERT_THAT(document_log->WriteProto(wrapper), IsOk());
  }

  // 3. Create the index again. This should throw out the lite and main index
  // and trigger index restoration.
  {
    IcingSearchEngineOptions options = GetDefaultIcingOptions();
    options.set_index_merge_size(document.ByteSizeLong());
    IcingSearchEngine icing(options, GetTestJniCache());

    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    SearchSpecProto search_spec;
    search_spec.set_query("consectetur");
    search_spec.set_term_match_type(TermMatchType::EXACT_ONLY);
    SearchResultProto results =
        icing.Search(search_spec, ScoringSpecProto::default_instance(),
                     ResultSpecProto::default_instance());
    EXPECT_THAT(results.status(), ProtoIsOk());
    EXPECT_THAT(results.next_page_token(), Eq(0));
    // Only the first document should be retrievable.
    ASSERT_THAT(results.results(), SizeIs(1));
    EXPECT_THAT(results.results(0).document().uri(), Eq("fake_type/0"));
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
            .AddType(SchemaTypeConfigBuilder().SetType("Message").AddProperty(
                PropertyConfigBuilder()
                    .SetName("unindexedField")
                    .SetDataTypeString(TERM_MATCH_UNKNOWN, TOKENIZER_NONE)
                    .SetCardinality(CARDINALITY_REQUIRED)))
            .Build();
    ASSERT_THAT(icing.SetSchema(schema).status(), ProtoIsOk());

    // Add a document that contains no indexed content.
    DocumentProto document =
        DocumentBuilder()
            .SetKey("icing", "fake_type/0")
            .SetSchema("Message")
            .AddStringProperty("unindexedField",
                               "Don't you dare search over this!")
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
  }
}

TEST_F(IcingSearchEngineInitializationTest,
       DocumentWithNoValidIndexedContentDoesntCauseRestoreIndex) {
  // 1. Create an index with a single document in it that has no valid indexed
  // tokens in its content.
  {
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());

    // Set a schema for a single type that has no indexed properties.
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());

    // Add a document that contains no valid indexed content - just punctuation.
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
                                .Build();
  DocumentProto document2 = DocumentBuilder()
                                .SetKey("icing", "fake_type/2")
                                .SetSchema("Message")
                                .AddStringProperty("body", "message body")
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
    EXPECT_THAT(
        initialize_result_proto.initialize_stats().index_restoration_cause(),
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
       InitializeShouldLogRecoveryCauseInconsistentWithGroundTruth) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("icing", "fake_type/0")
                               .SetSchema("Message")
                               .AddStringProperty("body", "message body")
                               .Build();
  {
    // Initialize and put a document.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    EXPECT_THAT(icing.Put(document).status(), ProtoIsOk());
  }

  {
    // Delete the index file to trigger RestoreIndexIfNeeded.
    std::string idx_subdir = GetIndexDir() + "/idx";
    filesystem()->DeleteDirectoryRecursively(idx_subdir.c_str());
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
       InitializeShouldLogRecoveryCauseSchemaChangesOutofSync) {
  DocumentProto document = DocumentBuilder()
                               .SetKey("icing", "fake_type/0")
                               .SetSchema("Message")
                               .AddStringProperty("body", "message body")
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
                SchemaTypeConfigBuilder()
                    .SetType("Message")
                    .AddProperty(PropertyConfigBuilder()
                                     .SetName("body")
                                     .SetDataTypeString(TERM_MATCH_PREFIX,
                                                        TOKENIZER_PLAIN)
                                     .SetCardinality(CARDINALITY_REQUIRED))
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
        SchemaStore::Create(filesystem(), GetSchemaDir(), &fake_clock));
    ICING_EXPECT_OK(schema_store->SetSchema(new_schema));
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
                               .Build();
  {
    // Initialize and put one document.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  }

  // lambda to fail OpenForWrite on lite index hit buffer once.
  bool has_failed_already = false;
  auto open_write_lambda = [this, &has_failed_already](const char* filename) {
    std::string lite_index_buffer_file_path =
        absl_ports::StrCat(GetIndexDir(), "/idx/lite.hb");
    std::string filename_string(filename);
    if (!has_failed_already && filename_string == lite_index_buffer_file_path) {
      has_failed_already = true;
      return -1;
    }
    return this->filesystem()->OpenForWrite(filename);
  };

  auto mock_icing_filesystem = std::make_unique<IcingMockFilesystem>();
  // This fails Index::Create() once.
  ON_CALL(*mock_icing_filesystem, OpenForWrite)
      .WillByDefault(open_write_lambda);

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
                               .Build();
  {
    // Initialize and put one document.
    IcingSearchEngine icing(GetDefaultIcingOptions(), GetTestJniCache());
    ASSERT_THAT(icing.Initialize().status(), ProtoIsOk());
    ASSERT_THAT(icing.SetSchema(CreateMessageSchema()).status(), ProtoIsOk());
    ASSERT_THAT(icing.Put(document).status(), ProtoIsOk());
  }

  // lambda to fail Read on document store header once.
  bool has_failed_already = false;
  auto read_lambda = [this, &has_failed_already](const char* filename,
                                                 void* buf, size_t buf_size) {
    std::string document_store_header_file_path =
        absl_ports::StrCat(GetDocumentDir(), "/document_store_header");
    std::string filename_string(filename);
    if (!has_failed_already &&
        filename_string == document_store_header_file_path) {
      has_failed_already = true;
      return false;
    }
    return this->filesystem()->Read(filename, buf, buf_size);
  };

  auto mock_filesystem = std::make_unique<MockFilesystem>();
  // This fails DocumentStore::InitializeDerivedFiles() once.
  ON_CALL(*mock_filesystem, Read(A<const char*>(), _, _))
      .WillByDefault(read_lambda);

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
    // Delete the schema store header file to trigger an I/O error.
    std::string schema_store_header_file_path =
        GetSchemaDir() + "/schema_store_header";
    filesystem()->DeleteFile(schema_store_header_file_path.c_str());
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

    auto type = schema.add_types();
    type->set_schema_type("Message");
    auto body = type->add_properties();
    body->set_property_name("body");
    body->set_data_type(PropertyConfigProto::DataType::STRING);
    body->set_cardinality(PropertyConfigProto::Cardinality::REQUIRED);
    body->mutable_string_indexing_config()->set_term_match_type(
        TermMatchType::PREFIX);
    body->mutable_string_indexing_config()->set_tokenizer_type(
        StringIndexingConfig::TokenizerType::PLAIN);

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

}  // namespace
}  // namespace lib
}  // namespace icing
